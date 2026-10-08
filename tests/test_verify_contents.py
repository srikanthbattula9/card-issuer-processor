"""Unit tests for the detection logic in scripts/verify_contents.py (no Kafka or database needed)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
import verify_contents as vc  # noqa: E402


def decided(aid, **over):
    return {"auth_id": aid, "status": "approved", "decline_code": None, "amount_minor": 1000, **over}


def expected(**over):
    return {"status": "approved", "decline_code": None, "amount_minor": 1000, **over}


def run(events, db):
    grouped = {}
    for e in events:
        grouped.setdefault(e["auth_id"], []).append(e)
    return vc.compare("test", grouped, db, vc.auth_diff)


def test_clean_run_has_no_problems():
    assert run([decided("A")], {"A": expected()}) == 0


def test_missing_event_is_detected():
    assert run([], {"A": expected()}) == 1


def test_duplicate_event_is_detected():
    assert run([decided("A"), decided("A")], {"A": expected()}) == 1


def test_duplicate_cannot_mask_a_loss():
    events = [decided("A"), decided("A")]
    db = {"A": expected(), "B": expected()}
    assert len(events) == len(db)  # totals match, which is all a count check compares
    assert run(events, db) == 2    # one missing event plus one extra copy


def test_amount_mismatch_is_detected():
    assert run([decided("A", amount_minor=1001)], {"A": expected()}) == 1


def test_event_without_database_row_is_detected():
    assert run([decided("A"), decided("C")], {"A": expected()}) == 1


def test_entry_diff_checks_ledger_amount_and_type():
    diff = vc.entry_diff("captured_amount_minor", "capture")
    good = {"auth_id": "A", "response_amount": 400, "ledger_amount": 400, "entry_type": "capture"}
    event = {"auth_id": "A", "captured_amount_minor": 400}
    assert diff(event, good) == []
    assert diff({"auth_id": "A", "captured_amount_minor": 401}, good)
    assert diff(event, {**good, "ledger_amount": 401})
    assert diff(event, {**good, "entry_type": "refund"})
