"""Authorization engine: checks a card and account, places a hold, and
returns approve or a specific decline code. Wired through the idempotency
module so a retried authorize request is safe.

Rule order (cheapest first): card status -> MCC blocklist -> velocity -> balance.
The account row is locked before the MCC/velocity/balance checks, because
velocity and balance are both check-then-act and must be serialized per account.
"""
import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import psycopg

from app.events import publish_event
from app.outbox import record_event
from app.idempotency import begin, advance, finish

# Velocity: at most N non-declined authorizations per card per window.
# Configurable so load tests can raise it.
# Known gap: only approvals are counted. Real issuers often also count declined
# attempts to catch card-testing; that is a documented follow-up.
VELOCITY_MAX_AUTHS = int(os.environ.get("VELOCITY_MAX_AUTHS", "5"))
VELOCITY_WINDOW_SECONDS = int(os.environ.get("VELOCITY_WINDOW_SECONDS", "60"))

HOLD_DURATION = timedelta(hours=24)


@dataclass
class AuthResult:
    auth_id: str
    status: str          # 'approved' or 'declined'
    decline_code: str | None
    amount_minor: int


def authorize(
    conn: psycopg.Connection,
    idempotency_key: str,
    card_token: str,
    amount_minor: int,
    merchant_id: str,
    mcc: str,
) -> AuthResult:
    body = {
        "card_token": card_token,
        "amount_minor": amount_minor,
        "merchant_id": merchant_id,
        "mcc": mcc,
    }
    req = begin(conn, idempotency_key, "/authorize", body)

    if req.recovery_point == "finished":
        row = conn.execute(
            "SELECT auth_id, status, decline_code, amount_minor FROM authorizations "
            "WHERE idempotency_key = %s", (idempotency_key,),
        ).fetchone()
        return AuthResult(*row)

    card = conn.execute(
        "SELECT c.status, c.account_id, c.expires_on FROM cards c WHERE c.card_token = %s",
        (card_token,),
    ).fetchone()

    auth_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc)
    decline_code = None
    account_id = None

    if card is None:
        # No matching card: record the attempt for audit/fraud-signal purposes
        # rather than rejecting before we can log it. card_token and account_id
        # stay NULL; the raw value received is preserved separately.
        decline_code = "14"
    else:
        card_status, account_id, expires_on = card
        if card_status == "frozen":
            decline_code = "62"
        elif card_status == "closed":
            decline_code = "14"
        elif expires_on < now.date():
            decline_code = "54"
        else:
            # Everything below reads state and then writes a hold, so serialize per account.
            conn.execute("SELECT account_id FROM accounts WHERE account_id = %s FOR UPDATE", (account_id,))

            if conn.execute("SELECT 1 FROM blocked_mccs WHERE mcc = %s", (mcc,)).fetchone():
                decline_code = "57"
            else:
                recent = conn.execute(
                    "SELECT count(*) FROM authorizations "
                    "WHERE card_token = %s AND status <> 'declined' "
                    "AND approved_at > now() - make_interval(secs => %s::double precision)",
                    (card_token, VELOCITY_WINDOW_SECONDS),
                ).fetchone()[0]
                if recent >= VELOCITY_MAX_AUTHS:
                    decline_code = "61"
                else:
                    available = conn.execute(
                        "SELECT available_balance_minor FROM account_available_balance WHERE account_id = %s",
                        (account_id,),
                    ).fetchone()
                    available_minor = available[0] if available else 0
                    if available_minor < amount_minor:
                        decline_code = "51"

    advance(conn, idempotency_key, "hold_placed")

    status = "declined" if decline_code else "approved"
    expires_at = now + HOLD_DURATION

    db_card_token = card_token if card is not None else None

    conn.execute(
        "INSERT INTO authorizations (auth_id, card_token, account_id, amount_minor, merchant_id, "
        "mcc, status, decline_code, idempotency_key, expires_at, attempted_card_token) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (auth_id, db_card_token, account_id, amount_minor, merchant_id, mcc, status,
         decline_code, idempotency_key, expires_at, card_token),
    )

    result = AuthResult(auth_id=auth_id, status=status, decline_code=decline_code, amount_minor=amount_minor)
    finish(conn, idempotency_key, 200, {
        "auth_id": auth_id, "status": status, "decline_code": decline_code,
    })
    event = {"auth_id": auth_id, "status": status, "decline_code": decline_code, "amount_minor": amount_minor}
    event["event_id"] = record_event(conn, "authorization.decided", event)
    conn.commit()
    publish_event("authorization.decided", event)
    return result