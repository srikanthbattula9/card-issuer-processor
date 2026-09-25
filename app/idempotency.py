"""Idempotency with recovery points: a retried request resumes from the last
completed phase instead of restarting, and concurrent retries are safely
rejected via row locking. Pattern: Stripe/Brandur Leach, "Implementing
Stripe-like Idempotency Keys in Postgres."
"""
import hashlib
import json
from dataclasses import dataclass

import psycopg


class RequestMismatch(Exception):
    """Same idempotency key, different request body — a client bug."""


class RequestInProgress(Exception):
    """Same idempotency key is currently being processed by another request."""


def _hash_request(body: dict) -> str:
    return hashlib.sha256(json.dumps(body, sort_keys=True).encode()).hexdigest()


@dataclass
class IdempotentRequest:
    key: str
    recovery_point: str
    is_new: bool


def begin(conn: psycopg.Connection, idempotency_key: str, path: str, body: dict) -> IdempotentRequest:
    """Call at the start of a request. Either creates a fresh key at
    'started', or recovers an in-flight one at its last completed phase.
    Raises RequestMismatch or RequestInProgress when retry isn't safe.
    """
    request_hash = _hash_request(body)

    row = conn.execute(
        "SELECT request_hash, recovery_point, locked_at, response_status, response_body "
        "FROM idempotency_keys WHERE idempotency_key = %s FOR UPDATE",
        (idempotency_key,),
    ).fetchone()

    if row is None:
        conn.execute(
            "INSERT INTO idempotency_keys (idempotency_key, request_path, request_hash, "
            "recovery_point, locked_at) VALUES (%s, %s, %s, 'started', now())",
            (idempotency_key, path, request_hash),
        )
        return IdempotentRequest(key=idempotency_key, recovery_point="started", is_new=True)

    stored_hash, recovery_point, locked_at, status, body_out = row

    if stored_hash != request_hash:
        raise RequestMismatch(f"idempotency key {idempotency_key} reused with a different request body")

    if locked_at is not None and recovery_point != "finished":
        raise RequestInProgress(f"idempotency key {idempotency_key} is already being processed")

    conn.execute(
        "UPDATE idempotency_keys SET locked_at = now(), last_run_at = now() WHERE idempotency_key = %s",
        (idempotency_key,),
    )
    return IdempotentRequest(key=idempotency_key, recovery_point=recovery_point, is_new=False)


def advance(conn: psycopg.Connection, idempotency_key: str, recovery_point: str) -> None:
    """Commit progress to the next phase, still locked."""
    conn.execute(
        "UPDATE idempotency_keys SET recovery_point = %s WHERE idempotency_key = %s",
        (recovery_point, idempotency_key),
    )


def finish(conn: psycopg.Connection, idempotency_key: str, status: int, body: dict) -> None:
    """Mark the request complete and unlock it. Future retries with this key
    short-circuit straight to this stored response."""
    conn.execute(
        "UPDATE idempotency_keys SET recovery_point = 'finished', locked_at = NULL, "
        "response_status = %s, response_body = %s WHERE idempotency_key = %s",
        (status, json.dumps(body), idempotency_key),
    )