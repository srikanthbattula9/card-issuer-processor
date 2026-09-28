"""Publishes state-change events to Kafka. Every authorize, capture, void,
and refund emits an event, so reconciliation and other consumers can react
without querying the core tables directly. A failed publish must not fail
the request that triggered it — see the note on foreign state mutations
below."""
import json
import os
from datetime import datetime, timezone

from confluent_kafka import Producer

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
TOPIC = "card.transactions"

_producer = None


def get_producer() -> Producer:
    global _producer
    if _producer is None:
        _producer = Producer({"bootstrap.servers": KAFKA_BROKER})
    return _producer


def publish_event(event_type: str, payload: dict) -> None:
    """Fire-and-forget publish. As noted in Stripe's idempotency-keys post:
    a call to Kafka is a foreign state mutation like any other and can fail —
    it should not be treated as free just because it's usually fast. For now
    we publish best-effort after the local transaction commits; a stronger
    guarantee (transactional outbox) is a documented next step, not silently
    assumed to be solved."""
    if os.environ.get("EVENTS_DISABLED") == "1":
        return
    event = {
        "event_type": event_type,
        "occurred_at": datetime.now(timezone.utc).isoformat(),
        **payload,
    }
    producer = get_producer()
    producer.produce(TOPIC, value=json.dumps(event).encode("utf-8"))
    producer.flush(timeout=5)