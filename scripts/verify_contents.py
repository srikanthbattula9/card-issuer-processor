#!/usr/bin/env python3
"""Contents-based event integrity check.

Usage:
  python3 scripts/verify_contents.py snapshot   # before the run: record topic offset + DB time
  ... run load_test.py / simulate_network.py ...
  python3 scripts/verify_contents.py check      # after the run: compare events to database rows

Unlike verify_events.sh, which compares counts, this matches every event to its
database row by key and compares the contents. Per event type it reports:
  missing           in the database, no event on the topic
  extra_copies      events beyond the first for the same key (duplicates)
  unexpected        event with no database row in the window
  mismatched        event whose fields differ from the database
A duplicate can no longer hide a loss, because they are counted separately.
"""
import json
import os
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

import psycopg
from confluent_kafka import Consumer, TopicPartition

BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
DB_URL = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5433/cards")
TOPIC = "card.transactions"
BASELINE = Path(__file__).with_name("verify_baseline.json")


def make_consumer():
    return Consumer({"bootstrap.servers": BROKER, "group.id": "verify-contents",
                     "enable.auto.commit": False, "auto.offset.reset": "earliest"})


def high_watermark(consumer):
    _, high = consumer.get_watermark_offsets(TopicPartition(TOPIC, 0), timeout=10, cached=False)
    return high


def snapshot():
    c = make_consumer()
    offset = high_watermark(c)
    c.close()
    with psycopg.connect(DB_URL) as conn:
        db_now = conn.execute("SELECT now()").fetchone()[0].isoformat()
    BASELINE.write_text(json.dumps({"offset": offset, "db_now": db_now}))
    print(f"baseline saved: topic offset {offset}, database time {db_now}")


def read_events(start, end):
    c = make_consumer()
    c.assign([TopicPartition(TOPIC, 0, start)])
    events, unparseable, idle, pos = [], 0, 0, start
    while pos < end:
        msg = c.poll(1.0)
        if msg is None or msg.error():
            idle += 1
            if idle >= 15:
                break
            continue
        idle = 0
        pos = msg.offset() + 1
        try:
            events.append(json.loads(msg.value()))
        except ValueError:
            unparseable += 1
    c.close()
    if pos < end:
        print(f"WARNING: stopped at offset {pos}, before {end}; results are incomplete")
    return events, unparseable


def group(events, event_type, key):
    out = defaultdict(list)
    for e in events:
        if e.get("event_type") == event_type:
            out[e.get(key)].append(e)
    return out


def load_db(conn, since):
    auth = {}
    for aid, status, code, amt in conn.execute(
        "SELECT auth_id::text, status, decline_code, amount_minor "
        "FROM authorizations WHERE approved_at >= %s", (since,)
    ).fetchall():
        # The event records the decision; the column later becomes captured/voided/expired.
        auth[aid] = {"status": "declined" if status == "declined" else "approved",
                     "decline_code": code, "amount_minor": amt}
    caps, refs, voids = {}, {}, {}
    rows = conn.execute(
        "SELECT request_path, response_body FROM idempotency_keys "
        "WHERE response_status = 200 AND created_at >= %s "
        "AND request_path IN ('/capture', '/refund', '/void')", (since,)
    ).fetchall()
    for path, body in rows:
        if path == "/void":
            voids[body["auth_id"]] = {}
        elif path == "/capture":
            caps[body["entry_id"]] = {"auth_id": body["auth_id"],
                                      "response_amount": body["captured_amount_minor"]}
        elif path == "/refund":
            refs[body["entry_id"]] = {"auth_id": body["auth_id"],
                                      "response_amount": body["refunded_amount_minor"]}
    ids = list(caps) + list(refs)
    ledger, types = {}, {}
    if ids:
        ledger = dict(conn.execute(
            "SELECT entry_id, SUM(amount_minor) FILTER (WHERE amount_minor > 0) "
            "FROM journal_lines WHERE entry_id = ANY(%s) GROUP BY entry_id", (ids,)).fetchall())
        types = dict(conn.execute(
            "SELECT entry_id, entry_type FROM journal_entries WHERE entry_id = ANY(%s)", (ids,)).fetchall())
    for d in (caps, refs):
        for k, x in d.items():
            x["ledger_amount"] = int(ledger[k]) if ledger.get(k) is not None else None
            x["entry_type"] = types.get(k)
    return auth, caps, refs, voids


def auth_diff(e, x):
    return [f"{f}: event={e.get(f)!r} db={x[f]!r}"
            for f in ("status", "decline_code", "amount_minor") if e.get(f) != x[f]]


def entry_diff(amount_field, entry_type):
    def diff(e, x):
        out = []
        if e.get("auth_id") != x["auth_id"]:
            out.append(f"auth_id: event={e.get('auth_id')!r} db={x['auth_id']!r}")
        if e.get(amount_field) != x["ledger_amount"]:
            out.append(f"{amount_field}: event={e.get(amount_field)!r} ledger={x['ledger_amount']!r}")
        if x["response_amount"] != x["ledger_amount"]:
            out.append(f"stored response amount {x['response_amount']!r} differs from ledger {x['ledger_amount']!r}")
        if x["entry_type"] != entry_type:
            out.append(f"entry_type: expected {entry_type!r} got {x['entry_type']!r}")
        return out
    return diff


def compare(name, ev, db, diff):
    missing = [k for k in db if k not in ev]
    unexpected = [k for k in ev if k not in db]
    extra = sum(len(v) - 1 for v in ev.values())
    duplicated = [k for k, v in ev.items() if len(v) > 1]
    mismatched = []
    for k, exp in db.items():
        for e in ev.get(k, []):
            d = diff(e, exp)
            if d:
                mismatched.append((k, d))
                break
    total = sum(len(v) for v in ev.values())
    print(f"{name:<22} db={len(db):>7} events={total:>7} missing={len(missing):>5} "
          f"extra_copies={extra:>5} unexpected={len(unexpected):>5} mismatched={len(mismatched):>5}")
    for label, items in (("missing", missing), ("unexpected", unexpected), ("duplicated", duplicated)):
        for k in items[:3]:
            print(f"    {label}: {k}")
    for k, d in mismatched[:3]:
        print(f"    mismatched: {k}: {'; '.join(d)}")
    return len(missing) + extra + len(unexpected) + len(mismatched)


def check():
    if not BASELINE.exists():
        sys.exit("no baseline found: run `snapshot` first")
    base = json.loads(BASELINE.read_text())
    c = make_consumer()
    end = high_watermark(c)
    c.close()
    events, unparseable = read_events(base["offset"], end)
    since = datetime.fromisoformat(base["db_now"])
    with psycopg.connect(DB_URL) as conn:
        auth, caps, refs, voids = load_db(conn, since)
    print(f"window: topic offsets {base['offset']}..{end} ({len(events)} events), database since {base['db_now']}\n")
    problems = unparseable
    problems += compare("authorization.decided", group(events, "authorization.decided", "auth_id"), auth, auth_diff)
    problems += compare("authorization.voided", group(events, "authorization.voided", "auth_id"), voids, lambda e, x: [])
    problems += compare("transaction.captured", group(events, "transaction.captured", "entry_id"), caps,
                        entry_diff("captured_amount_minor", "capture"))
    problems += compare("transaction.refunded", group(events, "transaction.refunded", "entry_id"), refs,
                        entry_diff("refunded_amount_minor", "refund"))
    known = {"authorization.decided", "authorization.voided", "transaction.captured", "transaction.refunded"}
    other = Counter(e.get("event_type") for e in events if e.get("event_type") not in known)
    if other:
        print(f"\nevents of unknown type: {dict(other)}")
        problems += sum(other.values())
    if unparseable:
        print(f"unparseable messages: {unparseable}")
    print("\nRESULT: " + ("CLEAN" if problems == 0 else f"{problems} PROBLEM(S) FOUND"))
    sys.exit(0 if problems == 0 else 1)


if __name__ == "__main__":
    if len(sys.argv) != 2 or sys.argv[1] not in ("snapshot", "check"):
        sys.exit(__doc__)
    snapshot() if sys.argv[1] == "snapshot" else check()
