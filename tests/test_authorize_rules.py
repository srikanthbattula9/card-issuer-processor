import threading
import uuid

import psycopg

from app.authorize import authorize, VELOCITY_MAX_AUTHS
from tests.conftest import make_card, DATABASE_URL


def _auth(conn, card, amount=100, mcc="5812"):
    return authorize(conn, str(uuid.uuid4()), card, amount, "merchant_1", mcc)


def test_blocked_mcc_is_declined_57(conn):
    card = make_card(conn, balance_minor=5000)
    result = _auth(conn, card, mcc="7995")
    assert result.status == "declined"
    assert result.decline_code == "57"


def test_mcc_rule_runs_before_balance_rule(conn):
    # Rule order is part of the contract (the Java port must match):
    # a blocked category on an underfunded card is 57, not 51.
    card = make_card(conn, balance_minor=500)
    result = _auth(conn, card, amount=1000, mcc="6051")
    assert result.decline_code == "57"


def test_authorization_over_the_velocity_limit_is_declined_61(conn):
    card = make_card(conn, balance_minor=1_000_000)
    for _ in range(VELOCITY_MAX_AUTHS):
        assert _auth(conn, card).status == "approved"
    over = _auth(conn, card)
    assert over.status == "declined"
    assert over.decline_code == "61"


def test_balance_rule_still_applies_under_the_velocity_limit(conn):
    card = make_card(conn, balance_minor=500)
    result = _auth(conn, card, amount=1000)
    assert result.decline_code == "51"


def test_simultaneous_authorizations_cannot_exceed_the_velocity_limit(conn):
    """Twice the limit fired at once against one card: exactly the limit may
    be approved. Velocity is check-then-act, so this only holds if the
    account row lock serializes the count and the hold insert."""
    card = make_card(conn, balance_minor=1_000_000)
    n_threads = VELOCITY_MAX_AUTHS * 2
    results, errors = [], []

    def run():
        try:
            with psycopg.connect(DATABASE_URL) as c:
                results.append(_auth(c, card).status)
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=run) for _ in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors, errors
    assert results.count("approved") == VELOCITY_MAX_AUTHS
    assert results.count("declined") == n_threads - VELOCITY_MAX_AUTHS
