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
        # linger.ms lets the client batch messages; delivery happens in the
        # background, and failures are reported through _on_delivery.
        _producer = Producer({"bootstrap.servers": KAFKA_BROKER, "linger.ms": 5})
    return _producer


NONBLOCKING_PUBLISH = os.environ.get("EVENTS_NONBLOCKING") == "1"
_failures = {"delivery": 0, "queue_full": 0}


def _on_delivery(err, msg):
    """Runs when the broker acknowledges or rejects a message."""
    if err is not None:
        _failures["delivery"] += 1


def event_failure_counts() -> dict:
    return dict(_failures)


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
    try:
        producer.produce(TOPIC, value=json.dumps(event).encode("utf-8"), on_delivery=_on_delivery)
    except BufferError:
        # Local queue is full: drop and count it rather than block the request.
        _failures["queue_full"] += 1
        return
    if NONBLOCKING_PUBLISH:
        # Non-blocking: services delivery callbacks for earlier messages and returns.
        # Weaker guarantee: an event can be lost if the process dies before delivery.
        producer.poll(0)
    else:
        # Default: wait for the broker to acknowledge before the request returns.
        producer.flush(timeout=5)


def flush_events(timeout: float = 10.0) -> int:
    """Wait for queued messages to be delivered. Call at shutdown. Returns how many are still undelivered."""
    if _producer is None:
        return 0
    return _producer.flush(timeout)