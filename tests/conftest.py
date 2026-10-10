"""Shared fixtures: a connection, and a helper to create a funded test card."""
import os
import uuid
from datetime import date, timedelta

import psycopg
import pytest

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5433/cards")


@pytest.fixture
def conn():
    with psycopg.connect(DATABASE_URL, autocommit=False) as c:
        yield c
        c.rollback()


def make_card(conn, *, balance_minor=0, status="active", expires_on=None):
    """Creates an account, funds it via a journal entry, and issues a card.
    Returns the card_token."""
    expires_on = expires_on or (date.today() + timedelta(days=365))

    account_id = conn.execute(
        "INSERT INTO accounts(account_type) VALUES ('customer') RETURNING account_id"
    ).fetchone()[0]

    if balance_minor:
        funding_account = conn.execute(
            "INSERT INTO accounts(account_type) VALUES ('suspense') RETURNING account_id"
        ).fetchone()[0]
        entry_id = conn.execute(
            "INSERT INTO journal_entries(entry_type, transaction_id, idempotency_key) "
            "VALUES ('adjustment', %s, %s) RETURNING entry_id",
            (uuid.uuid4(), f"fund-{uuid.uuid4()}"),
        ).fetchone()[0]
        conn.execute(
            "INSERT INTO journal_lines(entry_id, account_id, amount_minor) VALUES (%s,%s,%s)",
            (entry_id, funding_account, -balance_minor),
        )
        conn.execute(
            "INSERT INTO journal_lines(entry_id, account_id, amount_minor) VALUES (%s,%s,%s)",
            (entry_id, account_id, balance_minor),
        )

    card_token = uuid.uuid4()
    conn.execute(
        "INSERT INTO card_vault(card_token, pan_encrypted, pan_last4) VALUES (%s, %s, %s)",
        (card_token, b"fake-encrypted-pan", "4242"),
    )
    conn.execute(
        "INSERT INTO cards(card_token, account_id, card_type, status, expires_on) "
        "VALUES (%s,%s,'virtual',%s,%s)",
        (card_token, account_id, status, expires_on),
    )
    conn.commit()
    return str(card_token)

def make_account(conn, account_type="customer"):
    return conn.execute(
        "INSERT INTO accounts (account_type) VALUES (%s) RETURNING account_id", (account_type,)
    ).fetchone()[0]
