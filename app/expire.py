"""Marks lapsed authorization holds as expired and publishes an event for each.

The available balance already ignores a hold once expires_at passes, so this does
not change what a customer can spend. It makes the state explicit: the row's status
becomes 'expired', expired_at records when it was swept, and consumers get an
authorization.expired event. Safe to run repeatedly and from several processes:
rows locked by an in-flight capture are skipped and picked up on the next pass.

  python -m app.expire                  # sweep once
  python -m app.expire --interval 30    # sweep every 30 seconds
"""
import argparse
import os
import time

import psycopg

from app.events import flush_events, publish_event
from app.outbox import record_event

DEFAULT_DATABASE_URL = "postgresql://postgres:postgres@localhost:5433/cards"
BATCH = 1000


def expire_holds(conn: psycopg.Connection, limit: int = BATCH) -> list[str]:
    """Expire up to `limit` lapsed holds. Returns the auth_ids it expired."""
    rows = conn.execute(
        "WITH due AS ("
        "    SELECT auth_id FROM authorizations "
        "    WHERE status = 'approved' AND expires_at <= now() "
        "    ORDER BY expires_at LIMIT %s FOR UPDATE SKIP LOCKED"
        ") "
        "UPDATE authorizations a SET status = 'expired', expired_at = now() "
        "FROM due WHERE a.auth_id = due.auth_id "
        "RETURNING a.auth_id::text, a.amount_minor - a.captured_amount_minor",
        (limit,),
    ).fetchall()
    events = []
    for auth_id, released in rows:
        payload = {"auth_id": auth_id, "released_amount_minor": released}
        payload["event_id"] = record_event(conn, "authorization.expired", payload)
        events.append(payload)
    conn.commit()
    # Kafka publish is after commit and best-effort; the outbox row above is the durable record.
    for payload in events:
        publish_event("authorization.expired", payload)
    return [auth_id for auth_id, _ in rows]


def sweep_all(conn: psycopg.Connection) -> int:
    total = 0
    while True:
        batch = expire_holds(conn)
        total += len(batch)
        if len(batch) < BATCH:
            return total


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--interval", type=float, default=0, help="seconds between sweeps; 0 sweeps once")
    args = parser.parse_args()
    with psycopg.connect(os.environ.get("DATABASE_URL", DEFAULT_DATABASE_URL)) as conn:
        while True:
            print(f"expired {sweep_all(conn)} lapsed holds", flush=True)
            if not args.interval:
                break
            time.sleep(args.interval)
    flush_events()


if __name__ == "__main__":
    main()
