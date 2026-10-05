"""Open-loop load generator for POST /authorize.

A scheduler releases one job every 1/RATE seconds into a queue; worker threads
send them. If the workers fall behind, the queue grows and the backlog is
reported, so an unachievable rate shows up as backlog, not as a quietly lower
rate. Latency is measured from just before the send to the response, and queue
wait is recorded separately.

Usage: python3 scripts/load_test.py --rate 100 --duration 30 --workers 32
"""
import argparse
import json
import queue
import random
import threading
import time
import uuid

import requests

MERCHANTS = [("coffee_shop", "5812"), ("grocery_store", "5411"), ("online_retailer", "5999")]


def build_job(pool, low_balance, frozen):
    """Pick an outcome class and build one request. Returns (kind, token, amount, merchant, mcc)."""
    r = random.random()
    amount = random.randint(500, 5000)
    merchant, mcc = random.choice(MERCHANTS)
    if r < 0.85:
        return ("approve", random.choice(pool), amount, merchant, mcc)
    if r < 0.90:
        return ("low_balance", low_balance, 100000, merchant, mcc)
    if r < 0.94:
        return ("unknown_card", str(uuid.uuid4()), amount, merchant, mcc)
    return ("frozen", frozen, amount, merchant, mcc)


class Stats:
    def __init__(self):
        self.lock = threading.Lock()
        self.latencies = []
        self.queue_waits = []
        self.outcomes = {}
        self.status_codes = {}
        self.malformed = 0
        self.dup_checked = 0
        self.dup_matched = 0
        self.peak_backlog = 0

    def record(self, latency_ms, queue_wait_ms, kind, code, malformed=False):
        with self.lock:
            self.latencies.append(latency_ms)
            self.queue_waits.append(queue_wait_ms)
            self.outcomes[kind] = self.outcomes.get(kind, 0) + 1
            self.status_codes[code] = self.status_codes.get(code, 0) + 1
            if malformed:
                self.malformed += 1


def post_authorize(session, base_url, key, token, amount, merchant, mcc):
    return session.post(
        f"{base_url}/authorize",
        headers={"Idempotency-Key": key},
        json={"card_token": token, "amount_minor": amount, "merchant_id": merchant, "mcc": mcc},
        timeout=30,
    )


def worker(jobs, stats, base_url):
    session = requests.Session()
    while True:
        item = jobs.get()
        if item is None:
            return
        due, job = item
        kind, token, amount, merchant, mcc = job
        key = str(uuid.uuid4())

        t0 = time.perf_counter()
        queue_wait_ms = (t0 - due) * 1000
        try:
            resp = post_authorize(session, base_url, key, token, amount, merchant, mcc)
            latency_ms = (time.perf_counter() - t0) * 1000
        except requests.RequestException:
            stats.record((time.perf_counter() - t0) * 1000, queue_wait_ms, kind, "conn_error")
            continue

        malformed = False
        body = None
        if resp.status_code == 200:
            try:
                body = resp.json()
                if "auth_id" not in body or "status" not in body:
                    malformed = True
            except ValueError:
                malformed = True
        stats.record(latency_ms, queue_wait_ms, kind, resp.status_code, malformed)

        # 10% of approvals are immediately resent with the same key: must return the same auth_id.
        if body and body.get("status") == "approved" and random.random() < 0.10:
            try:
                retry = post_authorize(session, base_url, key, token, amount, merchant, mcc)
                with stats.lock:
                    stats.dup_checked += 1
                    if retry.status_code == 200 and retry.json().get("auth_id") == body["auth_id"]:
                        stats.dup_matched += 1
            except (requests.RequestException, ValueError):
                with stats.lock:
                    stats.dup_checked += 1


def percentile(sorted_values, p):
    if not sorted_values:
        return 0.0
    idx = min(len(sorted_values) - 1, int(len(sorted_values) * p))
    return sorted_values[idx]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rate", type=float, default=100, help="target requests per second")
    parser.add_argument("--duration", type=float, default=30, help="seconds to send for")
    parser.add_argument("--workers", type=int, default=32)
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--pool-file", default="scripts/pool.json")
    args = parser.parse_args()

    with open(args.pool_file) as f:
        cfg = json.load(f)
    pool, low_balance, frozen = cfg["pool"], cfg["low_balance"], cfg["frozen"]

    jobs = queue.Queue()
    stats = Stats()
    threads = [threading.Thread(target=worker, args=(jobs, stats, args.base_url), daemon=True)
               for _ in range(args.workers)]
    for t in threads:
        t.start()

    total = int(args.rate * args.duration)
    start = time.perf_counter()
    for i in range(total):
        due = start + i / args.rate
        delay = due - time.perf_counter()
        if delay > 0:
            time.sleep(delay)
        jobs.put((due, build_job(pool, low_balance, frozen)))
        stats.peak_backlog = max(stats.peak_backlog, jobs.qsize())

    send_elapsed = time.perf_counter() - start
    for _ in threads:
        jobs.put(None)
    for t in threads:
        t.join()
    total_elapsed = time.perf_counter() - start

    lat = sorted(stats.latencies)
    qw = sorted(stats.queue_waits)
    n = len(lat)
    five_xx = sum(v for k, v in stats.status_codes.items() if isinstance(k, int) and k >= 500)
    conn_err = stats.status_codes.get("conn_error", 0)

    print("\n--- load test ---")
    print(f"target rate: {args.rate:.0f}/s for {args.duration:.0f}s ({total} requests), {args.workers} workers")
    print(f"scheduled over {send_elapsed:.1f}s, all responses in {total_elapsed:.1f}s")
    print(f"achieved throughput: {n / total_elapsed:.1f} responses/s")
    print(f"latency ms: p50={percentile(lat, .5):.1f} p95={percentile(lat, .95):.1f} "
          f"p99={percentile(lat, .99):.1f} max={lat[-1] if lat else 0:.1f}")
    print(f"queue wait ms (generator, not service): p50={percentile(qw, .5):.1f} "
          f"p95={percentile(qw, .95):.1f} max={qw[-1] if qw else 0:.1f}; peak backlog={stats.peak_backlog}")
    print(f"outcomes sent: {dict(sorted(stats.outcomes.items()))}")
    print(f"HTTP status codes: {dict(sorted(stats.status_codes.items(), key=lambda kv: str(kv[0])))}")
    print(f"5xx={five_xx} conn_errors={conn_err} malformed_200={stats.malformed}")
    print(f"duplicate resends: {stats.dup_matched}/{stats.dup_checked} returned the same auth_id")
    raise SystemExit(1 if (five_xx or conn_err or stats.malformed) else 0)


if __name__ == "__main__":
    main()
