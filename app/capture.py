"""Capture: confirms an authorization hold and posts the real ledger entry.
A hold can be captured once, for up to the held amount. Capturing more than
the hold, or capturing an expired/already-captured/declined hold, is rejected."""
import uuid
from app.events import publish_event
from dataclasses import dataclass
from datetime import datetime, timezone

import psycopg

from app.idempotency import begin, advance, finish


class CaptureError(Exception):
    def __init__(self, reason: str):
        self.reason = reason
        super().__init__(reason)


@dataclass
class CaptureResult:
    auth_id: str
    entry_id: int
    captured_amount_minor: int


def capture(
    conn: psycopg.Connection,
    idempotency_key: str,
    auth_id: str,
    amount_minor: int,
) -> CaptureResult:
    """Capture an approved, unexpired, uncaptured (or partially captured)
    authorization for up to its remaining held amount."""
    body = {"auth_id": auth_id, "amount_minor": amount_minor}
    req = begin(conn, idempotency_key, "/capture", body)

    if req.recovery_point == "finished":
        row = conn.execute(
            "SELECT response_body FROM idempotency_keys WHERE idempotency_key = %s",
            (idempotency_key,),
        ).fetchone()
        stored = row[0]
        return CaptureResult(
            auth_id=stored["auth_id"],
            entry_id=stored["entry_id"],
            captured_amount_minor=stored["captured_amount_minor"],
        )

    # Lock the authorization row so a concurrent capture of the same auth can't race.
    auth = conn.execute(
        "SELECT account_id, amount_minor, status, expires_at, captured_amount_minor, merchant_id "
        "FROM authorizations WHERE auth_id = %s FOR UPDATE",
        (auth_id,),
    ).fetchone()

    if auth is None:
        raise CaptureError("authorization not found")

    account_id, held_amount, status, expires_at, captured_so_far, merchant_id = auth
    now = datetime.now(timezone.utc)

    if status != "approved":
        raise CaptureError(f"cannot capture an authorization with status '{status}'")
    if expires_at < now:
        raise CaptureError("authorization hold has expired")
    remaining = held_amount - captured_so_far
    if amount_minor > remaining:
        raise CaptureError(f"capture amount {amount_minor} exceeds remaining held amount {remaining}")

    advance(conn, idempotency_key, "hold_placed")  # reusing this phase name to mean "validated, about to post"

    # Post the ledger entry: debit customer, credit a merchant clearing account
    # keyed by merchant_id (created on first use).
       # Simplification for this step: one shared merchant clearing account.
    # Real per-merchant accounts arrive when the fee split module lands.
    row = conn.execute(
        "SELECT account_id FROM accounts WHERE account_type = 'merchant' ORDER BY account_id LIMIT 1"
    ).fetchone()
    if row is None:
        row = conn.execute(
            "INSERT INTO accounts(account_type) VALUES ('merchant') RETURNING account_id"
        ).fetchone()
    merchant_account_id = row[0]

    entry_id = conn.execute(
        "INSERT INTO journal_entries(entry_type, transaction_id, idempotency_key) "
        "VALUES ('capture', %s, %s) RETURNING entry_id",
        (uuid.uuid4(), idempotency_key),
    ).fetchone()[0]
    conn.execute(
        "INSERT INTO journal_lines(entry_id, account_id, amount_minor) VALUES (%s,%s,%s)",
        (entry_id, account_id, -amount_minor),
    )
    conn.execute(
        "INSERT INTO journal_lines(entry_id, account_id, amount_minor) VALUES (%s,%s,%s)",
        (entry_id, merchant_account_id, amount_minor),
    )

    new_captured = captured_so_far + amount_minor
    new_status = "captured" if new_captured == held_amount else "approved"  # still open if partial
    conn.execute(
        "UPDATE authorizations SET captured_amount_minor = %s, status = %s WHERE auth_id = %s",
        (new_captured, new_status, auth_id),
    )

    result = CaptureResult(auth_id=auth_id, entry_id=entry_id, captured_amount_minor=amount_minor)
    finish(conn, idempotency_key, 200, {
        "auth_id": auth_id, "entry_id": entry_id, "captured_amount_minor": amount_minor,
    })
    conn.commit()
    publish_event("transaction.captured", {
        "auth_id": auth_id, "entry_id": entry_id, "captured_amount_minor": amount_minor,
    })
    return result