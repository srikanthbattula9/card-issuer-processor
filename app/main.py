"""HTTP surface for the issuer-processor. Thin wrappers around the tested
functions in authorize.py, capture.py, void_refund.py — no business logic
lives here, only request/response handling and idempotency-key extraction."""
from fastapi import FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import psycopg
import os

from app.authorize import authorize
from app.capture import capture, CaptureError
from app.void_refund import void, refund, VoidError, RefundError

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5433/cards")

app = FastAPI(title="card-issuer-processor")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_conn():
    return psycopg.connect(DATABASE_URL)


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
            "SELECT ledger_balance_minor FROM account_ledger_balance WHERE account_id = %s",
            (account_id,),
        ).fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="account not found")
        return {"account_id": account_id, "ledger_balance_minor": row[0]}