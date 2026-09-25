"""First test: a journal entry whose legs do not sum to zero must be rejected.

Run with a local Postgres (docker compose up -d) and DATABASE_URL set.
"""
import os
import uuid
import psycopg
import pytest

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/cards")


@pytest.fixture
def conn():
    with psycopg.connect(DATABASE_URL) as c:
        yield c
        c.rollback()


def _new_account(conn, account_type):
    return conn.execute(
        "INSERT INTO accounts(account_type) VALUES (%s) RETURNING account_id", (account_type,)
    ).fetchone()[0]


def test_balanced_entry_is_accepted(conn):
    cust = _new_account(conn, "customer")
    merch = _new_account(conn, "merchant")
    entry = conn.execute(
        "INSERT INTO journal_entries(entry_type, transaction_id, idempotency_key) "
        "VALUES ('capture', %s, %s) RETURNING entry_id",
        (uuid.uuid4(), f"idem-{uuid.uuid4()}"),
    ).fetchone()[0]
    conn.execute("INSERT INTO journal_lines(entry_id, account_id, amount_minor) VALUES (%s,%s,%s)", (entry, cust, -1000))
    conn.execute("INSERT INTO journal_lines(entry_id, account_id, amount_minor) VALUES (%s,%s,%s)", (entry, merch, 1000))
    conn.commit()  # deferred trigger fires here and passes


def test_unbalanced_entry_is_rejected(conn):
    cust = _new_account(conn, "customer")
    merch = _new_account(conn, "merchant")
    entry = conn.execute(
        "INSERT INTO journal_entries(entry_type, transaction_id, idempotency_key) "
        "VALUES ('capture', %s, %s) RETURNING entry_id",
        (uuid.uuid4(), f"idem-{uuid.uuid4()}"),
    ).fetchone()[0]
    conn.execute("INSERT INTO journal_lines(entry_id, account_id, amount_minor) VALUES (%s,%s,%s)", (entry, cust, -1000))
    conn.execute("INSERT INTO journal_lines(entry_id, account_id, amount_minor) VALUES (%s,%s,%s)", (entry, merch, 999))
    with pytest.raises(psycopg.errors.RaiseException):
        conn.commit()  # deferred trigger fires and must fail
