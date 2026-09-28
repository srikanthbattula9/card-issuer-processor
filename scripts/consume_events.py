"""Reads events off card.transactions and prints them. Run this in a
separate terminal while hitting the API to watch events arrive live."""
import json
from confluent_kafka import Consumer

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "demo-consumer",
    "auto.offset.reset": "earliest",
})
consumer.subscribe(["card.transactions"])

print("Listening on card.transactions... (Ctrl+C to stop)")
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("error:", msg.error())
            continue
        event = json.loads(msg.value())
        print(f"[{event['occurred_at']}] {event['event_type']}: {event}")
except KeyboardInterrupt:
    pass
finally:
    consumer.close()