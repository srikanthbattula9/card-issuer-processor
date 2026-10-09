"""Webhook signing and delivery.

Delivery is at-least-once: a row is marked delivered only after the endpoint
returns 2xx, so a crash after the POST but before the UPDATE re-sends it.
Consumers must dedupe on event_id (see consumer/). Signing follows the
Stripe scheme: header "t=<unix>,v1=<hex>", where v1 = HMAC-SHA256 over
"<t>.<raw body>" with the endpoint secret. Signing the timestamp bounds replay.
"""
import argparse
import hashlib
import hmac
import json
import os
import random
import secrets
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone

import psycopg

SIGNATURE_HEADER = "Webhook-Signature"
ID_HEADER = "Webhook-Id"
TOLERANCE_SECONDS = 300          # reject signatures older than this
MAX_ATTEMPTS = 8
BASE_DELAY = 2.0                 # seconds; attempt n waits ~BASE_DELAY * 2**n
MAX_DELAY = 300.0
BATCH = 100
HTTP_TIMEOUT = 5.0


# ---- signing ---------------------------------------------------------------

def sign(secret: str, body: bytes, ts: int | None = None) -> str:
    ts = int(time.time()) if ts is None else ts
    mac = hmac.new(secret.encode(), f"{ts}.".encode() + body, hashlib.sha256).hexdigest()
    return f"t={ts},v1={mac}"


def verify(secret: str, body: bytes, header: str, now: int | None = None,
           tolerance: int = TOLERANCE_SECONDS) -> bool:
    """True only if the signature matches and the timestamp is within tolerance."""
    try:
        parts = dict(p.split("=", 1) for p in header.split(","))
        ts = int(parts["t"])
        given = parts["v1"]
    except (ValueError, KeyError, AttributeError):
        return False
    now = int(time.time()) if now is None else now
    if abs(now - ts) > tolerance:
        return False
    expected = hmac.new(secret.encode(), f"{ts}.".encode() + body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, given)


# ---- endpoints -------------------------------------------------------------

def register_endpoint(conn: psycopg.Connection, merchant_id: str, url: str,
                      secret: str | None = None) -> tuple[str, str]:
    """Create an endpoint. Returns (endpoint_id, secret). Caller commits."""
    secret = secret or ("whsec_" + secrets.token_hex(24))
    endpoint_id = conn.execute(
        "INSERT INTO webhook_endpoints (merchant_id, url, secret) VALUES (%s,%s,%s) RETURNING endpoint_id::text",
        (merchant_id, url, secret),
    ).fetchone()[0]
    return endpoint_id, secret


# ---- delivery --------------------------------------------------------------

def backoff(attempt: int, rng: random.Random | None = None) -> float:
    """Full-jitter exponential backoff. attempt is the number already made (>=1)."""
    cap = min(MAX_DELAY, BASE_DELAY * (2 ** attempt))
    return (rng or random).uniform(0, cap)


def build_body(event_id: str, event_type: str, occurred_at: datetime, payload: dict) -> bytes:
    body = {"event_id": event_id, "event_type": event_type,
            "occurred_at": occurred_at.isoformat(), "data": payload}
    return json.dumps(body, separators=(",", ":"), sort_keys=True, default=str).encode()


def http_post(url: str, body: bytes, headers: dict) -> tuple[int, str | None]:
    """Default transport. Returns (status_code, error). status 0 means no response."""
    req = urllib.request.Request(url, data=body, method="POST", headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
            return resp.status, None
    except urllib.error.HTTPError as e:
        return e.code, str(e)
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        return 0, str(e)


def deliver_due(conn: psycopg.Connection, post=http_post, limit: int = BATCH,
                rng: random.Random | None = None) -> dict:
    """Attempt every due pending delivery once. Returns counts by outcome."""
    rows = conn.execute(
        "SELECT d.delivery_id, d.attempts, e.event_id::text, e.event_type, e.occurred_at, e.payload, "
        "       w.url, w.secret "
        "FROM webhook_deliveries d "
        "JOIN events e ON e.event_id = d.event_id "
        "JOIN webhook_endpoints w ON w.endpoint_id = d.endpoint_id "
        "WHERE d.status = 'pending' AND d.next_attempt_at <= now() "
        "ORDER BY d.next_attempt_at LIMIT %s FOR UPDATE OF d SKIP LOCKED",
        (limit,),
    ).fetchall()
    stats = {"delivered": 0, "retry": 0, "failed": 0}
    for delivery_id, attempts, event_id, event_type, occurred_at, payload, url, secret in rows:
        body = build_body(event_id, event_type, occurred_at, payload)
        headers = {"Content-Type": "application/json", ID_HEADER: event_id,
                   SIGNATURE_HEADER: sign(secret, body)}
        status, error = post(url, body, headers)
        attempts += 1
        if 200 <= status < 300:
            conn.execute(
                "UPDATE webhook_deliveries SET status='delivered', attempts=%s, last_status_code=%s, "
                "last_error=NULL, delivered_at=now() WHERE delivery_id=%s",
                (attempts, status, delivery_id))
            stats["delivered"] += 1
        elif attempts >= MAX_ATTEMPTS:
            conn.execute(
                "UPDATE webhook_deliveries SET status='failed', attempts=%s, last_status_code=%s, "
                "last_error=%s WHERE delivery_id=%s",
                (attempts, status, error, delivery_id))
            stats["failed"] += 1
        else:
            delay = backoff(attempts, rng)
            conn.execute(
                "UPDATE webhook_deliveries SET attempts=%s, last_status_code=%s, last_error=%s, "
                "next_attempt_at=now() + %s WHERE delivery_id=%s",
                (attempts, status, error, timedelta(seconds=delay), delivery_id))
            stats["retry"] += 1
    conn.commit()
    return stats


def main() -> None:
    ap = argparse.ArgumentParser(description="Deliver pending webhooks.")
    ap.add_argument("--interval", type=float, default=None, help="loop every N seconds; default: one pass")
    args = ap.parse_args()
    dsn = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5433/cards")
    with psycopg.connect(dsn) as conn:
        while True:
            stats = deliver_due(conn)
            print(f"{datetime.now(timezone.utc).isoformat()} {stats}", flush=True)
            if args.interval is None:
                break
            time.sleep(args.interval)


if __name__ == "__main__":
    main()
