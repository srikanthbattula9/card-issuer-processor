import uuid

import pytest

from app.authorize import authorize
from app.capture import capture, CaptureError
from tests.conftest import make_card


def _approved_auth(conn, balance_minor=5000, amount_minor=1000):
    card = make_card(conn, balance_minor=balance_minor)
    result = authorize(conn, str(uuid.uuid4()), card, amount_minor, "merchant_1", "5812")
    assert result.status == "approved"
    return result.auth_id


def test_full_capture_moves_the_ledger(conn):
    auth_id = _approved_auth(conn, amount_minor=1000)
    result = capture(conn, str(uuid.uuid4()), auth_id, 1000)
    assert result.captured_amount_minor == 1000

    status = conn.execute(
        "SELECT status, captured_amount_minor FROM authorizations WHERE auth_id = %s", (auth_id,)
    ).fetchone()
    assert status == ("captured", 1000)


def test_capture_more_than_held_amount_is_rejected(conn):
    auth_id = _approved_auth(conn, amount_minor=1000)
    with pytest.raises(CaptureError):
        capture(conn, str(uuid.uuid4()), auth_id, 5000)


def test_capturing_a_declined_authorization_is_rejected(conn):
    card = make_card(conn, balance_minor=100)  # too little to cover 1000
    result = authorize(conn, str(uuid.uuid4()), card, 1000, "merchant_1", "5812")
    assert result.status == "declined"

    with pytest.raises(CaptureError):
        capture(conn, str(uuid.uuid4()), result.auth_id, 1000)


def test_partial_capture_leaves_authorization_open(conn):
    auth_id = _approved_auth(conn, amount_minor=1000)
    capture(conn, str(uuid.uuid4()), auth_id, 400)

    status = conn.execute(
        "SELECT status, captured_amount_minor FROM authorizations WHERE auth_id = %s", (auth_id,)
    ).fetchone()
    assert status == ("approved", 400)  # still open, partially captured


def test_capturing_twice_for_the_full_remaining_amount_works(conn):
    auth_id = _approved_auth(conn, amount_minor=1000)
    capture(conn, str(uuid.uuid4()), auth_id, 400)
    capture(conn, str(uuid.uuid4()), auth_id, 600)

    status = conn.execute(
        "SELECT status, captured_amount_minor FROM authorizations WHERE auth_id = %s", (auth_id,)
    ).fetchone()
    assert status == ("captured", 1000)


def test_retrying_same_capture_key_returns_same_result(conn):
    auth_id = _approved_auth(conn, amount_minor=1000)
    key = str(uuid.uuid4())
    r1 = capture(conn, key, auth_id, 1000)
    r2 = capture(conn, key, auth_id, 1000)
    assert r1.entry_id == r2.entry_id