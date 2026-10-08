"""HTTP surface for the issuer-processor. Thin wrappers around the tested
functions in authorize.py, capture.py, void_refund.py — no business logic
lives here, only request/response handling and idempotency-key extraction."""
from contextlib import asynccontextmanager
import anyio.to_thread
from fastapi import FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import psycopg
from psycopg_pool import ConnectionPool
import os

from app.authorize import authorize
from app.capture import capture, CaptureError
from app.void_refund import void, refund, VoidError, RefundError
from app.events import flush_events, event_failure_counts

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5433/cards")

@asynccontextmanager
async def lifespan(_app):
    # Sync endpoints run in worker threads; the default limit is 40, which caps
    # concurrent requests regardless of pool size. Configurable for load tests.
    anyio.to_thread.current_default_thread_limiter().total_tokens = int(os.environ.get("THREAD_LIMIT", "40"))
    yield
    # Shutdown: wait for queued events to be delivered, then close the DB pool.
    flush_events(10.0)
    pool.close()


app = FastAPI(title="card-issuer-processor", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# One pool for the whole process instead of a new connection per request.
# max_size stays under Postgres's default limit of 100 connections; requests
# beyond it wait up to `timeout` seconds for a free connection, then fail with
# an error instead of hanging.
POOL_MIN = int(os.environ.get("DB_POOL_MIN", "10"))
POOL_MAX = int(os.environ.get("DB_POOL_MAX", "60"))
pool = ConnectionPool(DATABASE_URL, min_size=POOL_MIN, max_size=POOL_MAX, timeout=30, open=True)


def get_conn():
    return pool.connection()


class AuthorizeRequest(BaseModel):
    card_token: str
    amount_minor: int
    merchant_id: str
    mcc: str


class CaptureRequest(BaseModel):
    auth_id: str
    amount_minor: int


class VoidRequest(BaseModel):
    auth_id: str


class RefundRequest(BaseModel):
    auth_id: str
    amount_minor: int


@app.post("/authorize")
def post_authorize(req: AuthorizeRequest, idempotency_key: str = Header(..., alias="Idempotency-Key")):
    with get_conn() as conn:
        result = authorize(conn, idempotency_key, req.card_token, req.amount_minor, req.merchant_id, req.mcc)
        return {"auth_id": str(result.auth_id), "status": result.status, "decline_code": result.decline_code}


@app.post("/capture")
def post_capture(req: CaptureRequest, idempotency_key: str = Header(..., alias="Idempotency-Key")):
    with get_conn() as conn:
        try:
            result = capture(conn, idempotency_key, req.auth_id, req.amount_minor)
        except CaptureError as e:
            raise HTTPException(status_code=409, detail=str(e))
        return {"auth_id": result.auth_id, "entry_id": result.entry_id, "captured_amount_minor": result.captured_amount_minor}


@app.post("/void")
def post_void(req: VoidRequest, idempotency_key: str = Header(..., alias="Idempotency-Key")):
    with get_conn() as conn:
        try:
            result = void(conn, idempotency_key, req.auth_id)
        except VoidError as e:
            raise HTTPException(status_code=409, detail=str(e))
        return {"auth_id": result.auth_id, "status": result.status}


@app.post("/refund")
def post_refund(req: RefundRequest, idempotency_key: str = Header(..., alias="Idempotency-Key")):
    with get_conn() as conn:
        try:
            result = refund(conn, idempotency_key, req.auth_id, req.amount_minor)
        except RefundError as e:
            raise HTTPException(status_code=409, detail=str(e))
        return {"auth_id": result.auth_id, "entry_id": result.entry_id, "refunded_amount_minor": result.refunded_amount_minor}


@app.get("/accounts/{account_id}/balance")
def get_balance(account_id: int):
    with get_conn() as conn:
        row = conn.execute(
            "SELECT posted_minor, held_minor, available_minor FROM account_balances WHERE account_id = %s",
            (account_id,),
        ).fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="account not found")
        posted, held, available = row
        # ledger_balance_minor is kept as an alias of posted_minor for existing clients.
        return {"account_id": account_id, "posted_minor": posted, "held_minor": held,
                "available_minor": available, "ledger_balance_minor": posted}


@app.get("/health")
async def health():
    """Event delivery failures since the process started. Read after a load test to confirm nothing was dropped."""
    return {
        "status": "ok",
        "event_failures": event_failure_counts(),
        "thread_limit": anyio.to_thread.current_default_thread_limiter().total_tokens,
        "db_pool_max": POOL_MAX,
    }
