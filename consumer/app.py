"""Reference webhook consumer.

Verifies the signature, then processes each event exactly once by inserting
its event_id into a table with a primary key, inside the same transaction as
the processing. A duplicate delivery (the processor is at-least-once) hits
the key, does nothing, and is acknowledged with 200 so the sender stops
retrying. FAIL_FIRST_N=<n> rejects the first n requests with 503 to exercise
the sender's retry path.

Run:  WEBHOOK_SECRET=whsec_... uvicorn consumer.app:app --port 9000
"""
import os
import sqlite3
import threading

from fastapi import FastAPI, Request, Response

from app.webhooks import SIGNATURE_HEADER, verify

SECRET = os.environ.get("WEBHOOK_SECRET", "")
DB_PATH = os.environ.get("CONSUMER_DB", "consumer/received.sqlite")
FAIL_FIRST_N = int(os.environ.get("FAIL_FIRST_N", "0"))

app = FastAPI(title="webhook-consumer")
_lock = threading.Lock()
_stats = {"received": 0, "processed": 0, "duplicate": 0, "bad_signature": 0, "forced_failure": 0}


def _db():
    conn = sqlite3.connect(DB_PATH, isolation_level=None)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS received ("
        " event_id TEXT PRIMARY KEY, event_type TEXT NOT NULL, body TEXT NOT NULL,"
        " received_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)")
    return conn


@app.post("/webhook")
async def webhook(request: Request):
    body = await request.body()
    with _lock:
        _stats["received"] += 1
        if _stats["received"] <= FAIL_FIRST_N:
            _stats["forced_failure"] += 1
            return Response(status_code=503, content="simulated outage")
    if not verify(SECRET, body, request.headers.get(SIGNATURE_HEADER, "")):
        _stats["bad_signature"] += 1
        return Response(status_code=400, content="bad signature")
    doc = await request.json()
    conn = _db()
    try:
        conn.execute("BEGIN IMMEDIATE")
        cur = conn.execute(
            "INSERT OR IGNORE INTO received (event_id, event_type, body) VALUES (?,?,?)",
            (doc["event_id"], doc["event_type"], body.decode()))
        if cur.rowcount == 0:
            conn.execute("ROLLBACK")
            _stats["duplicate"] += 1
            return {"status": "duplicate", "event_id": doc["event_id"]}
        # "Processing" would go here, in the same transaction as the dedupe insert.
        conn.execute("COMMIT")
    finally:
        conn.close()
    _stats["processed"] += 1
    return {"status": "processed", "event_id": doc["event_id"]}


@app.get("/stats")
def stats():
    conn = _db()
    try:
        stored = conn.execute("SELECT count(*) FROM received").fetchone()[0]
    finally:
        conn.close()
    return {**_stats, "stored": stored}


@app.get("/received")
def received(limit: int = 50):
    conn = _db()
    try:
        rows = conn.execute(
            "SELECT event_id, event_type, received_at FROM received ORDER BY received_at DESC LIMIT ?", (limit,)
        ).fetchall()
    finally:
        conn.close()
    return [{"event_id": e, "event_type": t, "received_at": r} for e, t, r in rows]
