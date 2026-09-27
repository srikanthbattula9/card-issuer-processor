import uuid

import pytest

from app.authorize import authorize
from app.capture import capture
from app.void_refund import void, refund, VoidError, RefundError
from tests.conftest import make_card


def test_void_releases_an_uncaptured_hold(conn):
    card = make_card(conn, balance_minor=5000)
    auth = authorize(conn, str(uuid.uuid4()), card, 1000, "merchant_1", "5812")

    result = void(conn, str(uuid.uuid4()), auth.auth_id)
    assert result.status == "voided"

    status = conn.execute(
        "SELECT status FROM authorizations WHERE auth_id = %s", (auth.auth_id,)
    ).fetchone()[0]
    assert status == "voided"


def test_voiding_a_captured_authorization_is_rejected(conn):
    card = make_card(conn, balance_minor=5000)
    auth = authorize(conn, str(uuid.uuid4()), card, 1000, "merchant_1", "5812")
    capture(conn, str(uuid.uuid4()), auth.auth_id, 1000)

    with pytest.raises(VoidError):
        void(conn, str(uuid.uuid4()), auth.auth_id)


def test_refund_reverses_a_captured_amount(conn):
    card = make_card(conn, balance_minor=5000)
    auth = authorize(conn, str(uuid.uuid4()), card, 1000, "merchant_1", "5812")
    capture(conn, str(uuid.uuid4()), auth.auth_id, 1000)

    result = refund(conn, str(uuid.uuid4()), auth.auth_id, 1000)
    assert result.refunded_amount_minor == 1000

    balance = conn.execute(
        "SELECT ledger_balance_minor FROM account_ledger_balance WHERE account_id = %s",
        (auth.account_id if hasattr(auth, "account_id") else conn.execute(
            "SELECT account_id FROM authorizations WHERE auth_id = %s", (auth.auth_id,)
        ).fetchone()[0],),
    ).fetchone()[0]
    # customer started with 5000, spent 1000 (capture), got 1000 back (refund) = 5000 again
    assert balance == 5000


def test_refund_more_than_captured_is_rejected(conn):
    card = make_card(conn, balance_minor=5000)
    auth = authorize(conn, str(uuid.uuid4()), card, 1000, "merchant_1", "5812")
    capture(conn, str(uuid.uuid4()), auth.auth_id, 1000)

    with pytest.raises(RefundError):
        refund(conn, str(uuid.uuid4()), auth.auth_id, 5000)


def test_refunding_an_uncaptured_authorization_is_rejected(conn):
    card = make_card(conn, balance_minor=5000)
    auth = authorize(conn, str(uuid.uuid4()), card, 1000, "merchant_1", "5812")

    with pytest.raises(RefundError):
        refund(conn, str(uuid.uuid4()), auth.auth_id, 1000)


def test_original_capture_entry_is_never_modified_only_offset(conn):
    """The audit principle: history is append-only. After a refund, the
    original capture's journal_lines still exist unchanged; the refund is a
    separate entry."""
    card = make_card(conn, balance_minor=5000)
    auth = authorize(conn, str(uuid.uuid4()), card, 1000, "merchant_1", "5812")
    cap = capture(conn, str(uuid.uuid4()), auth.auth_id, 1000)
    refund(conn, str(uuid.uuid4()), auth.auth_id, 1000)

    original_lines = conn.execute(
        "SELECT amount_minor FROM journal_lines WHERE entry_id = %s ORDER BY amount_minor",
        (cap.entry_id,),
    ).fetchall()
    assert [l[0] for l in original_lines] == [-1000, 1000]  # untouched``