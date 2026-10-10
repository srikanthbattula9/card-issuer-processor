"""ACH credit funding with simulated return behavior.

A funding transfer is pending -- not in posted_minor, not spendable --
until its R01 window closes with no return, at which point post_due()
posts it to the ledger. It can return:

  R01 (insufficient funds): only while still pending. In this simulated
  model the R01 window closes exactly when posting would happen, so R01
  and "already posted" are mutually exclusive by construction.

  R10 (unauthorized): a much longer window, and can hit a transfer that
  has already posted -- meaning the money may already be posted and
  spent. That reverses the original journal entry, which can take the
  account negative. That is real ACH float risk, not a bug.

Windows are simulated values, not NACHA's actual business-day rules (which
also exclude weekends and holidays).

Events publish straight to Kafka (publish_event), not through the webhook
outbox: the outbox's record_event resolves a merchant via auth_id, and ACH
funding has no merchant or authorization. Routing ACH events to merchant
webhooks is a real gap, left for whenever platform-level (non-merchant)
webhook subscribers are built.
"""
import uuid
from dataclasses import dataclass
from datetime import timedelta

import psycopg

from app.clock import Clock
from app.events import publish_event

RETURN_WINDOWS = {
    "R01": timedelta(days=2),
    "R10": timedelta(days=60),
}
POST_WINDOW = RETURN_WINDOWS["R01"]
BATCH = 1000


class ACHError(Exception):
    pass


@dataclass
class ACHTransfer:
    transfer_id: str
    account_id: int
    amount_minor: int
    status: str


def initiate_credit(conn: psycopg.Connection, clock: Clock, account_id: int, amount_minor: int) -> str:
    """Start an ACH credit to account_id. Returns the transfer_id. The
    credit is pending -- not spendable -- until post_due() or apply_return()
    resolves it. Caller commits."""
    if amount_minor <= 0:
        raise ACHError("amount_minor must be positive")
    transfer_id = str(uuid.uuid4())
    conn.execute(
        "INSERT INTO ach_transfers (transfer_id, account_id, amount_minor, status, initiated_at) "
        "VALUES (%s,%s,%s,'pending',%s)",
        (transfer_id, account_id, amount_minor, clock.now()),
    )
    return transfer_id


def post_due(conn: psycopg.Connection, clock: Clock, clearing_account_id: int, limit: int = BATCH) -> list[str]:
    """Post every pending transfer whose R01 window has closed with no
    return. Returns the posted transfer_ids. Caller commits."""
    cutoff = clock.now() - POST_WINDOW
    rows = conn.execute(
        "SELECT transfer_id, account_id, amount_minor FROM ach_transfers "
        "WHERE status = 'pending' AND initiated_at <= %s "
        "ORDER BY initiated_at LIMIT %s FOR UPDATE SKIP LOCKED",
        (cutoff, limit),
    ).fetchall()
    posted = []
    for transfer_id, account_id, amount_minor in rows:
        entry_id = conn.execute(
            "INSERT INTO journal_entries (entry_type, transaction_id, idempotency_key) "
            "VALUES ('ach_credit', %s, %s) RETURNING entry_id",
            (transfer_id, f"ach_post_{transfer_id}"),
        ).fetchone()[0]
        conn.execute("INSERT INTO journal_lines (entry_id, account_id, amount_minor) VALUES (%s,%s,%s)",
                    (entry_id, account_id, amount_minor))
        conn.execute("INSERT INTO journal_lines (entry_id, account_id, amount_minor) VALUES (%s,%s,%s)",
                    (entry_id, clearing_account_id, -amount_minor))
        conn.execute(
            "UPDATE ach_transfers SET status='posted', posted_at=%s, entry_id=%s WHERE transfer_id=%s",
            (clock.now(), entry_id, transfer_id))
        posted.append(str(transfer_id))
        publish_event("ach.posted", {"transfer_id": transfer_id, "account_id": account_id,
                                    "amount_minor": amount_minor})
    return posted


def apply_return(conn: psycopg.Connection, clock: Clock, clearing_account_id: int,
                 transfer_id: str, return_code: str) -> None:
    """Apply an R01 or R10 return. Caller commits."""
    if return_code not in RETURN_WINDOWS:
        raise ACHError(f"unknown return_code {return_code!r}")

    row = conn.execute(
        "SELECT account_id, amount_minor, status, initiated_at, entry_id FROM ach_transfers "
        "WHERE transfer_id = %s FOR UPDATE", (transfer_id,)).fetchone()
    if row is None:
        raise ACHError(f"no such transfer {transfer_id!r}")
    account_id, amount_minor, status, initiated_at, entry_id = row

    if status == "returned":
        raise ACHError(f"transfer {transfer_id!r} was already returned")
    if clock.now() > initiated_at + RETURN_WINDOWS[return_code]:
        raise ACHError(f"{return_code} window has closed for transfer {transfer_id!r}")
    if return_code == "R01" and status != "pending":
        raise ACHError("R01 can only return a still-pending transfer")

    reversal_entry_id = None
    if status == "posted":
        reversal_entry_id = conn.execute(
            "INSERT INTO journal_entries (entry_type, transaction_id, idempotency_key) "
            "VALUES ('ach_reversal', %s, %s) RETURNING entry_id",
            (transfer_id, f"ach_reversal_{transfer_id}"),
        ).fetchone()[0]
        conn.execute("INSERT INTO journal_lines (entry_id, account_id, amount_minor) VALUES (%s,%s,%s)",
                    (reversal_entry_id, account_id, -amount_minor))
        conn.execute("INSERT INTO journal_lines (entry_id, account_id, amount_minor) VALUES (%s,%s,%s)",
                    (reversal_entry_id, clearing_account_id, amount_minor))

    conn.execute(
        "UPDATE ach_transfers SET status='returned', returned_at=%s, return_code=%s, reversal_entry_id=%s "
        "WHERE transfer_id=%s",
        (clock.now(), return_code, reversal_entry_id, transfer_id))

    publish_event("ach.returned", {
        "transfer_id": transfer_id, "account_id": account_id, "amount_minor": amount_minor,
        "return_code": return_code, "was_posted": status == "posted",
    })
