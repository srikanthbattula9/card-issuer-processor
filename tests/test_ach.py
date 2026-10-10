"""ACH credit funding: pending -> posted via a simulated clock, R01/R10
returns, and the float risk of an R10 after the money has posted and
been spent."""
from datetime import datetime, timedelta, timezone

import psycopg
import pytest

from app.ach import ACHError, apply_return, initiate_credit, post_due
from app.clock import SimulatedClock
from tests.conftest import make_account

T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)


@pytest.fixture
def clock():
    return SimulatedClock(start=T0)


def _balances(conn, account_id):
    return conn.execute(
        "SELECT posted_minor, held_minor, pending_minor, available_minor FROM account_balances "
        "WHERE account_id = %s", (account_id,)).fetchone()


def _transfer(conn, transfer_id):
    return conn.execute(
        "SELECT status, return_code, entry_id, reversal_entry_id FROM ach_transfers "
        "WHERE transfer_id = %s", (transfer_id,)).fetchone()


def test_initiate_credit_is_pending_and_not_spendable(conn, clock):
    acct = make_account(conn)
    initiate_credit(conn, clock, acct, 1000)
    assert _balances(conn, acct) == (0, 0, 1000, 0)


def test_post_due_does_nothing_before_the_window_closes(conn, clock):
    acct = make_account(conn)
    initiate_credit(conn, clock, acct, 1000)
    clock.advance(timedelta(days=1))               # R01 window is 2 days
    posted = post_due(conn, clock, make_account(conn, "ach_clearing"))
    assert posted == []
    assert _balances(conn, acct) == (0, 0, 1000, 0)


def test_post_due_posts_once_the_window_closes(conn, clock):
    acct = make_account(conn)
    clearing = make_account(conn, "ach_clearing")
    transfer_id = initiate_credit(conn, clock, acct, 1000)
    clock.advance(timedelta(days=3))
    posted = post_due(conn, clock, clearing)
    assert posted == [transfer_id]
    assert _balances(conn, acct) == (1000, 0, 0, 1000)
    status, return_code, entry_id, reversal_id = _transfer(conn, transfer_id)
    assert status == "posted" and entry_id is not None and reversal_id is None
    line_sum = conn.execute("SELECT sum(amount_minor) FROM journal_lines WHERE entry_id = %s",
                           (entry_id,)).fetchone()[0]
    assert line_sum == 0


def test_double_post_is_blocked_by_the_idempotency_key(conn, clock):
    """Guards against a hypothetical bug re-posting an already-posted
    transfer: journal_entries.idempotency_key is UNIQUE, so a repeat insert
    for the same transfer_id fails at the database, not silently double-pays."""
    acct = make_account(conn)
    clearing = make_account(conn, "ach_clearing")
    transfer_id = initiate_credit(conn, clock, acct, 1000)
    clock.advance(timedelta(days=3))
    post_due(conn, clock, clearing)
    conn.execute("UPDATE ach_transfers SET status = 'pending' WHERE transfer_id = %s", (transfer_id,))
    with pytest.raises(psycopg.errors.UniqueViolation):
        post_due(conn, clock, clearing)


def test_r01_before_posting_leaves_no_journal_entries(conn, clock):
    acct = make_account(conn)
    transfer_id = initiate_credit(conn, clock, acct, 1000)
    clock.advance(timedelta(hours=12))
    apply_return(conn, clock, make_account(conn, "ach_clearing"), transfer_id, "R01")
    status, return_code, entry_id, reversal_id = _transfer(conn, transfer_id)
    assert (status, return_code, entry_id, reversal_id) == ("returned", "R01", None, None)
    assert _balances(conn, acct) == (0, 0, 0, 0)


def test_r01_cannot_return_a_transfer_that_already_posted(conn, clock):
    acct = make_account(conn)
    clearing = make_account(conn, "ach_clearing")
    transfer_id = initiate_credit(conn, clock, acct, 1000)
    clock.advance(timedelta(days=3))
    post_due(conn, clock, clearing)
    with pytest.raises(ACHError, match="window"):
        apply_return(conn, clock, clearing, transfer_id, "R01")


def test_r10_before_posting_leaves_no_journal_entries(conn, clock):
    acct = make_account(conn)
    transfer_id = initiate_credit(conn, clock, acct, 1000)
    apply_return(conn, clock, make_account(conn, "ach_clearing"), transfer_id, "R10")
    status, return_code, entry_id, reversal_id = _transfer(conn, transfer_id)
    assert (status, return_code, entry_id, reversal_id) == ("returned", "R10", None, None)


def test_r10_after_posting_reverses_and_can_take_the_account_negative(conn, clock):
    """The customer has already spent the posted credit by the time the
    R10 lands -- the reversal still happens, and the balance goes negative.
    That is real ACH float risk, not a bug."""
    acct = make_account(conn)
    clearing = make_account(conn, "ach_clearing")
    transfer_id = initiate_credit(conn, clock, acct, 1000)
    clock.advance(timedelta(days=3))
    post_due(conn, clock, clearing)
    assert _balances(conn, acct)[0] == 1000                   # posted

    # simulate the customer spending it: a plain debit journal entry
    entry_id = conn.execute(
        "INSERT INTO journal_entries (entry_type, transaction_id, idempotency_key) "
        "VALUES ('capture', gen_random_uuid(), 'spend_test') RETURNING entry_id").fetchone()[0]
    conn.execute("INSERT INTO journal_lines (entry_id, account_id, amount_minor) VALUES (%s,%s,-1000)",
                (entry_id, acct))
    conn.execute("INSERT INTO journal_lines (entry_id, account_id, amount_minor) VALUES (%s,%s,1000)",
                (entry_id, clearing))
    assert _balances(conn, acct)[0] == 0                       # spent

    clock.advance(timedelta(days=10))
    apply_return(conn, clock, clearing, transfer_id, "R10")
    status, return_code, entry_id2, reversal_id = _transfer(conn, transfer_id)
    assert status == "returned" and return_code == "R10" and reversal_id is not None
    assert _balances(conn, acct)[0] == -1000                   # negative: real float risk


def test_r10_window_eventually_closes(conn, clock):
    acct = make_account(conn)
    transfer_id = initiate_credit(conn, clock, acct, 1000)
    clock.advance(timedelta(days=61))                          # R10 window is 60 days
    with pytest.raises(ACHError, match="window"):
        apply_return(conn, clock, make_account(conn, "ach_clearing"), transfer_id, "R10")


def test_unknown_return_code_rejected(conn, clock):
    acct = make_account(conn)
    transfer_id = initiate_credit(conn, clock, acct, 1000)
    with pytest.raises(ACHError, match="unknown return_code"):
        apply_return(conn, clock, make_account(conn, "ach_clearing"), transfer_id, "R99")


def test_already_returned_transfer_cannot_be_returned_again(conn, clock):
    acct = make_account(conn)
    clearing = make_account(conn, "ach_clearing")
    transfer_id = initiate_credit(conn, clock, acct, 1000)
    apply_return(conn, clock, clearing, transfer_id, "R01")
    with pytest.raises(ACHError, match="already returned"):
        apply_return(conn, clock, clearing, transfer_id, "R01")


def test_negative_amount_rejected(conn, clock):
    acct = make_account(conn)
    with pytest.raises(ACHError, match="positive"):
        initiate_credit(conn, clock, acct, -500)


def test_simulated_clock_advances_independently_of_real_time():
    c = SimulatedClock(start=T0)
    assert c.now() == T0
    c.advance(timedelta(days=5))
    assert c.now() == T0 + timedelta(days=5)
    c.set(T0 + timedelta(days=100))
    assert c.now() == T0 + timedelta(days=100)


def test_balance_endpoint_reports_pending_ach_credits(conn, clock, monkeypatch):
    """GET /accounts/{id}/balance must surface pending_minor, not just the
    three balances from before ACH existed."""
    from app.main import get_balance
    acct = make_account(conn)
    initiate_credit(conn, clock, acct, 1500)
    # No commit: get_conn is monkeypatched to reuse this same connection, so
    # get_balance sees this transaction's own uncommitted write directly.

    import contextlib
    import app.main

    @contextlib.contextmanager
    def _reuse_conn():
        yield conn                                  # don't close the test's own connection

    monkeypatch.setattr(app.main, "get_conn", _reuse_conn)
    result = get_balance(acct)
    assert result["pending_minor"] == 1500
    assert result["available_minor"] == 0
