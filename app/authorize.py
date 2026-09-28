"""Authorization engine: checks a card and account, places a hold, and
returns approve or a specific decline code. Wired through the idempotency
module so a retried authorize request is safe."""
import uuid
from app.events import publish_event
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import psycopg

from app.idempotency import begin, advance, finish


class DeclineError(Exception):
    def __init__(self, code: str, reason: str):
        self.code = code
        self.reason = reason
        super().__init__(f"{code}: {reason}")


@dataclass
class AuthResult:
    auth_id: str
    status: str          # 'approved' or 'declined'
    decline_code: str | None
    amount_minor: int


HOLD_DURATION = timedelta(hours=24)


def authorize(
    conn: psycopg.Connection,
    idempotency_key: str,
    card_token: str,
    amount_minor: int,
    merchant_id: str,
    mcc: str,
) -> AuthResult:
    """Run an authorization as atomic phases against the idempotency key.
    Phases: started -> checked -> hold_placed -> finished.
    """
    body = {
        "card_token": card_token,
        "amount_minor": amount_minor,
        "merchant_id": merchant_id,
        "mcc": mcc,
    }
    req = begin(conn, idempotency_key, "/authorize", body)

    if req.recovery_point == "finished":
        # Already resolved by a prior attempt — return the same outcome.
        row = conn.execute(
            "SELECT auth_id, status, decline_code, amount_minor FROM authorizations "
            "WHERE idempotency_key = %s", (idempotency_key,),
        ).fetchone()
        return AuthResult(*row)

    # Phase: check card and account status, available balance.
    card = conn.execute(
        "SELECT c.status, c.account_id, c.expires_on FROM cards c WHERE c.card_token = %s",
        (card_token,),
    ).fetchone()

    if card is None:
        raise DeclineError("14", "invalid card number")

    card_status, account_id, expires_on = card

    auth_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc)

    decline_code = None
    if card_status == "frozen":
        decline_code = "62"
    elif card_status == "closed":
        decline_code = "14"
    elif expires_on < now.date():
        decline_code = "54"
    else:
        conn.execute("SELECT account_id FROM accounts WHERE account_id = %s FOR UPDATE", (account_id,))
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

    # Phase: record the authorization (hold), whether approved or declined.
    conn.execute(
        "INSERT INTO authorizations (auth_id, card_token, account_id, amount_minor, merchant_id, "
        "mcc, status, decline_code, idempotency_key, expires_at) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (auth_id, card_token, account_id, amount_minor, merchant_id, mcc, status,
         decline_code, idempotency_key, expires_at),
    )
    advance(conn, idempotency_key, "hold_placed")

    result = AuthResult(auth_id=auth_id, status=status, decline_code=decline_code, amount_minor=amount_minor)
    finish(conn, idempotency_key, 200, {
        "auth_id": auth_id, "status": status, "decline_code": decline_code,
    })
    conn.commit()
    publish_event("authorization.decided", {
        "auth_id": auth_id, "status": status, "decline_code": decline_code, "amount_minor": amount_minor,
    })
    return result