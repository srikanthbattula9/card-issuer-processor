"""Transactional outbox for webhooks.

record_event() writes one row to `events` and one row to `webhook_deliveries`
per active endpoint of the merchant, inside the caller's open transaction.
The caller commits. So an event row exists if and only if the state change
it describes committed, and a crash at any point can't leave one without
the other. Delivery itself is done by app/webhooks.py (the worker).

Kafka publishing stays where it was (after commit, best-effort), but now
carries the same event_id so the two paths can be cross-checked.
"""
import json

import psycopg


def record_event(conn: psycopg.Connection, event_type: str, payload: dict) -> str:
    """Insert the event and its pending deliveries. Returns the event_id.

    The merchant is looked up from payload["auth_id"], which every event type
    carries, so call sites don't need merchant_id in scope.
    """
    row = conn.execute(
        "INSERT INTO events (event_type, merchant_id, payload) "
        "SELECT %s, a.merchant_id, %s::jsonb FROM authorizations a WHERE a.auth_id = %s "
        "RETURNING event_id::text, merchant_id",
        (event_type, json.dumps(payload, default=str), payload["auth_id"]),
    ).fetchone()
    if row is None:
        raise RuntimeError(f"record_event: authorization {payload['auth_id']} not found")
    event_id, merchant_id = row
    conn.execute(
        "INSERT INTO webhook_deliveries (event_id, endpoint_id) "
        "SELECT %s, endpoint_id FROM webhook_endpoints WHERE merchant_id = %s AND active",
        (event_id, merchant_id),
    )
    return event_id
