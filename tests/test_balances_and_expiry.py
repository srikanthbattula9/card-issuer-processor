"""Balances (posted, held, available) and explicit hold expiry."""
import uuid

import psycopg
import pytest

from app import expire
from app.authorize import authorize
from app.capture import capture, CaptureError
from tests.conftest import DATABASE_URL, make_card

SWEEP_ALL = 1_000_000


@pytest.fixture
def published(monkeypatch):
    events = []
    monkeypatch.setattr(expire, "publish_event", lambda event_type, payload: events.append((event_type, payload)))
    return events


def _balances(conn, card_token):
    account_id = conn.execute("SELECT account_id FROM cards WHERE card_token = %s", (card_token,)).fetchone()[0]
    return conn.execute(
        "SELECT posted_minor, held_minor, available_minor FROM account_balances WHERE account_id = %s",
        (account_id,),
    ).fetchone()


def _authorized(conn, card, amount_minor=1000):
    result = authorize(conn, str(uuid.uuid4()), card, amount_minor, "merchant_1", "5812")
    assert result.status == "approved"
    return str(result.auth_id)


def _lapse(conn, auth_id):
    conn.execute("UPDATE authorizations SET expires_at = now() - interval '1 minute' WHERE auth_id = %s", (auth_id,))
    conn.commit()


def _row(conn, auth_id):
    return conn.execute(
        "SELECT status, captured_amount_minor, expired_at FROM authorizations WHERE auth_id = %s", (auth_id,)
    ).fetchone()


def test_authorize_raises_held_but_not_posted(conn):
    card = make_card(conn, balance_minor=5000)
    assert _balances(conn, card) == (5000, 0, 5000)
    _authorized(conn, card, 1000)
    assert _balances(conn, card) == (5000, 1000, 4000)


def test_full_capture_moves_the_hold_into_posted(conn):
    card = make_card(conn, balance_minor=5000)
    auth_id = _authorized(conn, card, 1000)
    capture(conn, str(uuid.uuid4()), auth_id, 1000)
    assert _balances(conn, card) == (4000, 0, 4000)


def test_partial_capture_leaves_the_remainder_held(conn):
    card = make_card(conn, balance_minor=5000)
    auth_id = _authorized(conn, card, 1000)
    capture(conn, str(uuid.uuid4()), auth_id, 400)
    assert _balances(conn, card) == (4600, 600, 4000)


def test_a_lapsed_hold_frees_available_before_the_sweeper_runs(conn):
    card = make_card(conn, balance_minor=5000)
    auth_id = _authorized(conn, card, 1000)
    _lapse(conn, auth_id)
    assert _balances(conn, card) == (5000, 0, 5000)
    assert _row(conn, auth_id)[0] == "approved"  # status is only updated by the sweeper


def test_sweeper_expires_a_lapsed_hold_and_publishes_an_event(conn, published):
    card = make_card(conn, balance_minor=5000)
    auth_id = _authorized(conn, card, 1000)
    _lapse(conn, auth_id)
    assert auth_id in expire.expire_holds(conn, SWEEP_ALL)
    status, captured, expired_at = _row(conn, auth_id)
    assert status == "expired" and captured == 0 and expired_at is not None
    assert ("authorization.expired", {"auth_id": auth_id, "released_amount_minor": 1000}) in published
    assert _balances(conn, card) == (5000, 0, 5000)


def test_expiry_after_a_partial_capture_releases_only_the_remainder(conn, published):
    card = make_card(conn, balance_minor=5000)
    auth_id = _authorized(conn, card, 1000)
    capture(conn, str(uuid.uuid4()), auth_id, 400)
    _lapse(conn, auth_id)
    assert auth_id in expire.expire_holds(conn, SWEEP_ALL)
    status, captured, _ = _row(conn, auth_id)
    assert status == "expired" and captured == 400
    assert ("authorization.expired", {"auth_id": auth_id, "released_amount_minor": 600}) in published
    assert _balances(conn, card) == (4600, 0, 4600)


def test_sweeper_is_idempotent(conn, published):
    card = make_card(conn, balance_minor=5000)
    auth_id = _authorized(conn, card, 1000)
    _lapse(conn, auth_id)
    assert auth_id in expire.expire_holds(conn, SWEEP_ALL)
    first_expired_at = _row(conn, auth_id)[2]
    assert auth_id not in expire.expire_holds(conn, SWEEP_ALL)
    assert _row(conn, auth_id)[2] == first_expired_at
    assert sum(1 for t, p in published if p["auth_id"] == auth_id) == 1


def test_sweeper_skips_a_row_locked_by_an_in_flight_capture(conn, published):
    card = make_card(conn, balance_minor=5000)
    auth_id = _authorized(conn, card, 1000)
    _lapse(conn, auth_id)
    with psycopg.connect(DATABASE_URL) as other:
        other.execute("SELECT auth_id FROM authorizations WHERE auth_id = %s FOR UPDATE", (auth_id,))
        assert auth_id not in expire.expire_holds(conn, SWEEP_ALL)  # locked, so skipped, not blocked
        other.rollback()
    assert auth_id in expire.expire_holds(conn, SWEEP_ALL)  # picked up once the lock is gone


def test_capture_is_rejected_after_the_hold_is_swept(conn, published):
    card = make_card(conn, balance_minor=5000)
    auth_id = _authorized(conn, card, 1000)
    _lapse(conn, auth_id)
    expire.expire_holds(conn, SWEEP_ALL)
    with pytest.raises(CaptureError):
        capture(conn, str(uuid.uuid4()), auth_id, 1000)
