"""Webhook signing, outbox rows, and at-least-once delivery with backoff."""
import random
import uuid
from datetime import datetime, timezone

import psycopg

from app import webhooks
from app.authorize import authorize
from app.webhooks import register_endpoint, deliver_due, sign, verify, backoff
from tests.conftest import DATABASE_URL, make_card


class FakePost:
    """Transport stub. status_for maps url -> status code (or a list consumed in order)."""
    def __init__(self, status_for=None):
        self.status_for = dict(status_for or {})
        self.calls = []

    def __call__(self, url, body, headers):
        self.calls.append((url, body, headers))
        s = self.status_for.get(url, 200)
        if isinstance(s, list):
            s = s.pop(0)
        return (s, None if 200 <= s < 300 else f"status {s}")


import pytest


@pytest.fixture(autouse=True)
def _cleanup_endpoints():
    """authorize() commits, so endpoints and deliveries made here would outlive
    the test and the real worker would try to POST to them. Remove them."""
    yield
    with psycopg.connect(DATABASE_URL) as c:
        c.execute("DELETE FROM webhook_deliveries WHERE endpoint_id IN "
                  "(SELECT endpoint_id FROM webhook_endpoints WHERE url LIKE 'http://test.invalid/%')")
        c.execute("DELETE FROM webhook_endpoints WHERE url LIKE 'http://test.invalid/%'")
        c.commit()


def _setup(conn, url, merchant=None):
    merchant = merchant or f"m_{uuid.uuid4().hex[:8]}"
    endpoint_id, secret = register_endpoint(conn, merchant, url)
    card = make_card(conn, balance_minor=5000)
    result = authorize(conn, str(uuid.uuid4()), card, 1000, merchant, "5812")
    assert result.status == "approved"
    return endpoint_id, secret, str(result.auth_id)


def _delivery(conn, endpoint_id):
    return conn.execute(
        "SELECT status, attempts, next_attempt_at, last_status_code FROM webhook_deliveries "
        "WHERE endpoint_id = %s ORDER BY delivery_id", (endpoint_id,)).fetchall()


def _make_due(conn, endpoint_id):
    conn.execute("UPDATE webhook_deliveries SET next_attempt_at = now() WHERE endpoint_id = %s", (endpoint_id,))
    conn.commit()


# ---- signing ---------------------------------------------------------------

def test_signature_roundtrip_tamper_and_stale():
    body = b'{"a":1}'
    h = sign("sec", body, ts=1_000)
    assert verify("sec", body, h, now=1_100)
    assert not verify("sec", body + b" ", h, now=1_100)          # body changed
    assert not verify("other", body, h, now=1_100)               # wrong secret
    assert not verify("sec", body, h, now=1_000 + 301)           # outside tolerance
    assert not verify("sec", body, "garbage", now=1_100)         # malformed header
    assert not verify("sec", body, h.replace("t=1000", "t=1050"), now=1_100)  # timestamp edited


def test_backoff_is_bounded_and_capped():
    rng = random.Random(1)
    for attempt in range(1, 12):
        for _ in range(50):
            d = backoff(attempt, rng)
            assert 0 <= d <= min(webhooks.MAX_DELAY, webhooks.BASE_DELAY * 2 ** attempt)


# ---- outbox ----------------------------------------------------------------

def test_state_change_creates_one_delivery_per_active_endpoint(conn):
    merchant = f"m_{uuid.uuid4().hex[:8]}"
    url = f"http://test.invalid/{uuid.uuid4()}"
    active_id, _ = register_endpoint(conn, merchant, url)
    inactive_id, _ = register_endpoint(conn, merchant, url + "/off")
    conn.execute("UPDATE webhook_endpoints SET active = false WHERE endpoint_id = %s", (inactive_id,))
    _, _, auth_id = _setup(conn, url + "/other-merchant")       # different merchant: no rows for us
    card = make_card(conn, balance_minor=5000)
    result = authorize(conn, str(uuid.uuid4()), card, 1000, merchant, "5812")
    assert _delivery(conn, active_id) == [("pending", 0, _delivery(conn, active_id)[0][2], None)]
    assert _delivery(conn, inactive_id) == []
    ev = conn.execute("SELECT event_type, payload->>'auth_id' FROM events WHERE merchant_id = %s", (merchant,)).fetchall()
    assert ev == [("authorization.decided", str(result.auth_id))]


# ---- delivery --------------------------------------------------------------

def test_successful_delivery_is_signed_and_marked(conn):
    url = f"http://test.invalid/{uuid.uuid4()}"
    endpoint_id, secret, auth_id = _setup(conn, url)
    post = FakePost()
    stats = deliver_due(conn, post=post)
    assert stats["delivered"] >= 1
    mine = [c for c in post.calls if c[0] == url]
    assert len(mine) == 1
    _, body, headers = mine[0]
    assert verify(secret, body, headers[webhooks.SIGNATURE_HEADER])
    assert headers[webhooks.ID_HEADER]
    import json
    doc = json.loads(body)
    assert doc["event_type"] == "authorization.decided"
    assert doc["data"]["auth_id"] == auth_id
    assert doc["event_id"] == headers[webhooks.ID_HEADER]
    (status, attempts, _, code), = _delivery(conn, endpoint_id)
    assert (status, attempts, code) == ("delivered", 1, 200)


def test_failure_schedules_retry_then_succeeds(conn):
    url = f"http://test.invalid/{uuid.uuid4()}"
    endpoint_id, _, _ = _setup(conn, url)
    post = FakePost({url: [503, 200]})
    deliver_due(conn, post=post, rng=random.Random(0))
    (status, attempts, next_at, code), = _delivery(conn, endpoint_id)
    assert (status, attempts, code) == ("pending", 1, 503)
    assert next_at > datetime.now(timezone.utc)
    deliver_due(conn, post=post)                                  # not due yet: nothing happens
    assert _delivery(conn, endpoint_id)[0][1] == 1
    _make_due(conn, endpoint_id)
    deliver_due(conn, post=post)
    (status, attempts, _, code), = _delivery(conn, endpoint_id)
    assert (status, attempts, code) == ("delivered", 2, 200)


def test_gives_up_after_max_attempts(conn):
    url = f"http://test.invalid/{uuid.uuid4()}"
    endpoint_id, _, _ = _setup(conn, url)
    post = FakePost({url: 500})
    for _ in range(webhooks.MAX_ATTEMPTS):
        _make_due(conn, endpoint_id)
        deliver_due(conn, post=post)
    (status, attempts, _, _), = _delivery(conn, endpoint_id)
    assert (status, attempts) == ("failed", webhooks.MAX_ATTEMPTS)
    _make_due(conn, endpoint_id)
    n = len(post.calls)
    deliver_due(conn, post=post)
    assert len([c for c in post.calls[n:] if c[0] == url]) == 0  # failed rows are never retried


def test_no_response_counts_as_an_attempt(conn):
    url = f"http://test.invalid/{uuid.uuid4()}"
    endpoint_id, _, _ = _setup(conn, url)
    post = FakePost({url: 0})
    deliver_due(conn, post=post)
    (status, attempts, _, code), = _delivery(conn, endpoint_id)
    assert (status, attempts, code) == ("pending", 1, 0)


def test_row_locked_by_another_worker_is_skipped(conn):
    url = f"http://test.invalid/{uuid.uuid4()}"
    endpoint_id, _, _ = _setup(conn, url)
    post = FakePost()
    with psycopg.connect(DATABASE_URL) as other:
        other.execute("SELECT 1 FROM webhook_deliveries WHERE endpoint_id = %s FOR UPDATE", (endpoint_id,))
        deliver_due(conn, post=post)
        assert [c for c in post.calls if c[0] == url] == []
        other.rollback()
    deliver_due(conn, post=post)
    assert len([c for c in post.calls if c[0] == url]) == 1
