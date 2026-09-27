"""Void and refund. Void releases an uncaptured hold with no ledger impact.
Refund reverses a captured amount with a new opposing ledger entry — the
original entry is never modified, only offset. This is what lets an auditor
reconstruct history from an append-only log."""
import uuid
from dataclasses import dataclass

import psycopg

from app.idempotency import begin, advance, finish


class VoidError(Exception):
    pass


class RefundError(Exception):
    pass


@dataclass
class VoidResult:
    auth_id: str
    status: str


@dataclass
class RefundResult:
    auth_id: str
    entry_id: int
    refunded_amount_minor: int


def void(conn: psycopg.Connection, idempotency_key: str, auth_id: str) -> VoidResult:
    """Release an authorization hold that was never captured."""
    req = begin(conn, idempotency_key, "/void", {"auth_id": auth_id})

    if req.recovery_point == "finished":
        return VoidResult(auth_id=auth_id, status="voided")

    row = conn.execute(
        "SELECT status, captured_amount_minor FROM authorizations WHERE auth_id = %s FOR UPDATE",
        (auth_id,),
    ).fetchone()
    if row is None:
        raise VoidError("authorization not found")
    status, captured = row
    if captured > 0:
        raise VoidError("cannot void an authorization that has already been captured — use refund instead")
    if status not in ("approved",):
        raise VoidError(f"cannot void an authorization with status '{status}'")

    conn.execute("UPDATE authorizations SET status = 'voided' WHERE auth_id = %s", (auth_id,))
    finish(conn, idempotency_key, 200, {"auth_id": auth_id, "status": "voided"})
    conn.commit()
    return VoidResult(auth_id=auth_id, status="voided")


def refund(
    conn: psycopg.Connection,
    idempotency_key: str,
    auth_id: str,
    amount_minor: int,
) -> RefundResult:
    """Reverse some or all of a captured amount with a new opposing entry."""
    req = begin(conn, idempotency_key, "/refund", {"auth_id": auth_id, "amount_minor": amount_minor})

    if req.recovery_point == "finished":
        row = conn.execute(
            "SELECT response_body FROM idempotency_keys WHERE idempotency_key = %s",
            (idempotency_key,),
        ).fetchone()
        stored = row[0]
        return RefundResult(auth_id=stored["auth_id"], entry_id=stored["entry_id"],
                             refunded_amount_minor=stored["refunded_amount_minor"])

    row = conn.execute(
        "SELECT account_id, captured_amount_minor FROM authorizations WHERE auth_id = %s FOR UPDATE",
        (auth_id,),
    ).fetchone()
    if row is None:
        raise RefundError("authorization not found")
    account_id, captured = row
    if captured == 0:
        raise RefundError("cannot refund an authorization with nothing captured")
    if amount_minor > captured:
        raise RefundError(f"refund amount {amount_minor} exceeds captured amount {captured}")

    merchant_account_id = conn.execute(
        "SELECT account_id FROM accounts WHERE account_type = 'merchant' LIMIT 1"
    ).fetchone()[0]

    entry_id = conn.execute(
        "INSERT INTO journal_entries(entry_type, transaction_id, idempotency_key) "
        "VALUES ('refund', %s, %s) RETURNING entry_id",
        (uuid.uuid4(), idempotency_key),
    ).fetchone()[0]
    # Opposite direction of capture: credit customer, debit merchant.
    conn.execute(
        "INSERT INTO journal_lines(entry_id, account_id, amount_minor) VALUES (%s,%s,%s)",
        (entry_id, account_id, amount_minor),
    )
    conn.execute(
        "INSERT INTO journal_lines(entry_id, account_id, amount_minor) VALUES (%s,%s,%s)",
        (entry_id, merchant_account_id, -amount_minor),
    )

    result = RefundResult(auth_id=auth_id, entry_id=entry_id, refunded_amount_minor=amount_minor)
    finish(conn, idempotency_key, 200, {
        "auth_id": auth_id, "entry_id": entry_id, "refunded_amount_minor": amount_minor,
    })
    conn.commit()
    return result
