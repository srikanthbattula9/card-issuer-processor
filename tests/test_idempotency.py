import os
import uuid
import psycopg
import pytest

from app.idempotency import begin, advance, finish, RequestMismatch, RequestInProgress

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5433/cards")


@pytest.fixture
def conn():
    with psycopg.connect(DATABASE_URL, autocommit=True) as c:
        yield c
        c.execute("DELETE FROM idempotency_keys")


def test_new_key_starts_fresh(conn):
    req = begin(conn, str(uuid.uuid4()), "/authorize", {"amount": 1000})
    assert req.is_new is True
    assert req.recovery_point == "started"


def test_retry_with_same_body_resumes_from_recovery_point(conn):
    key = str(uuid.uuid4())
    body = {"amount": 1000}

    req1 = begin(conn, key, "/authorize", body)
    advance(conn, key, "hold_placed")
    finish(conn, key, 200, {"status": "approved"})  # simulate crash right after this in a real flow

    # A retry with the same key and body should recover the finished state.
    req2 = begin(conn, key, "/authorize", body)
    assert req2.is_new is False
    assert req2.recovery_point == "finished"


def test_retry_with_different_body_is_rejected(conn):
    key = str(uuid.uuid4())
    begin(conn, key, "/authorize", {"amount": 1000})
    finish(conn, key, 200, {"status": "approved"})

    with pytest.raises(RequestMismatch):
        begin(conn, key, "/authorize", {"amount": 2000})


def test_concurrent_retry_while_locked_is_rejected(conn):
    key = str(uuid.uuid4())
    body = {"amount": 1000}

    # First request starts and does NOT finish (still "in flight").
    with psycopg.connect(DATABASE_URL) as conn1:
        begin(conn1, key, "/authorize", body)
        advance(conn1, key, "hold_placed")
        conn1.commit()

        # A concurrent retry, before the first one finishes or unlocks, must be rejected.
        with pytest.raises(RequestInProgress):
            begin(conn, key, "/authorize", body)