from datetime import date, timedelta
import uuid

from app.authorize import authorize
from tests.conftest import make_card


def test_sufficient_balance_is_approved(conn):
    card = make_card(conn, balance_minor=5000)
    result = authorize(conn, str(uuid.uuid4()), card, 1000, "merchant_1", "5812")
    assert result.status == "approved"
    assert result.decline_code is None


def test_insufficient_balance_is_declined_51(conn):
    card = make_card(conn, balance_minor=500)
    result = authorize(conn, str(uuid.uuid4()), card, 1000, "merchant_1", "5812")
    assert result.status == "declined"
    assert result.decline_code == "51"


def test_frozen_card_is_declined_62(conn):
    card = make_card(conn, balance_minor=5000, status="frozen")
    result = authorize(conn, str(uuid.uuid4()), card, 1000, "merchant_1", "5812")
    assert result.decline_code == "62"


def test_expired_card_is_declined_54(conn):
    card = make_card(conn, balance_minor=5000, expires_on=date.today() - timedelta(days=1))
    result = authorize(conn, str(uuid.uuid4()), card, 1000, "merchant_1", "5812")
    assert result.decline_code == "54"


def test_two_holds_that_exceed_balance_cannot_both_approve(conn):
    """The race condition test: $60 balance, two $50 auths at once — at most
    one can be approved, never both."""
    card = make_card(conn, balance_minor=6000)

    import threading
    results = []

    def run():
        with __import__("psycopg").connect(
            __import__("os").environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5433/cards")
        ) as c:
            r = authorize(c, str(uuid.uuid4()), card, 5000, "merchant_1", "5812")
            results.append(r.status)

    t1, t2 = threading.Thread(target=run), threading.Thread(target=run)
    t1.start(); t2.start()
    t1.join(); t2.join()

    assert results.count("approved") <= 1, f"race condition: both approved: {results}"


def test_retrying_same_key_returns_same_result(conn):
    card = make_card(conn, balance_minor=5000)
    key = str(uuid.uuid4())
    r1 = authorize(conn, key, card, 1000, "merchant_1", "5812")
    r2 = authorize(conn, key, card, 1000, "merchant_1", "5812")
    assert str(r1.auth_id) == str(r2.auth_id)
