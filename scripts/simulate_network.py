"""Plays the role of the card network: fires a stream of realistic
authorize + capture requests at the live API. The mix includes normal
purchases, expected declines, and duplicate retries with the same
idempotency key (the same class of duplicate a real network resend
would produce). Reports authorize latency percentiles at the end."""
import json
import random
import time
import uuid

import requests

BASE_URL = "http://127.0.0.1:8000"

with open("scripts/cards.json") as f:
    CARDS = json.load(f)

MERCHANTS = [("coffee_shop", "5812"), ("grocery_store", "5411"), ("online_retailer", "5999")]


def authorize(card_token, amount_minor, idempotency_key, merchant_id, mcc):
    return requests.post(
        f"{BASE_URL}/authorize",
        headers={"Idempotency-Key": idempotency_key},
        json={"card_token": card_token, "amount_minor": amount_minor,
              "merchant_id": merchant_id, "mcc": mcc},
    )


def capture(auth_id, amount_minor, idempotency_key):
    return requests.post(
        f"{BASE_URL}/capture",
        headers={"Idempotency-Key": idempotency_key},
        json={"auth_id": auth_id, "amount_minor": amount_minor},
    )


def run(n_transactions=500):
    approved, declined, errors, duplicate_matches = 0, 0, 0, 0
    latencies = []
    verbose = n_transactions <= 50

    for i in range(n_transactions):
        card_name = random.choices(list(CARDS.keys()), weights=[0.7, 0.2, 0.1])[0]
        card_token = CARDS[card_name]
        amount_minor = random.randint(500, 5000)
        merchant_id, mcc = random.choice(MERCHANTS)
        key = str(uuid.uuid4())

        t0 = time.perf_counter()
        resp = authorize(card_token, amount_minor, key, merchant_id, mcc)
        latencies.append((time.perf_counter() - t0) * 1000)

        if resp.status_code == 200:
            data = resp.json()
            if data["status"] == "approved":
                approved += 1
                if verbose:
                    print(f"[{i}] {card_name} ${amount_minor/100:.2f} -> approved ({data['auth_id'][:8]})")
                capture(data["auth_id"], amount_minor, f"cap-{key}")

                # 20% of the time, simulate a network resend: same key, same request.
                if random.random() < 0.2:
                    retry_resp = authorize(card_token, amount_minor, key, merchant_id, mcc)
                    if retry_resp.status_code != 200:
                        print(f"[{i}] WARNING: duplicate resend returned HTTP {retry_resp.status_code}: {retry_resp.text[:200]}")
                    elif retry_resp.json()["auth_id"] == data["auth_id"]:
                        duplicate_matches += 1
                    else:
                        print(f"[{i}] WARNING: duplicate resend created a DIFFERENT auth_id!")
            else:
                declined += 1
                if verbose:
                    print(f"[{i}] {card_name} ${amount_minor/100:.2f} -> declined ({data['decline_code']})")
        else:
            errors += 1
            print(f"[{i}] {card_name} -> HTTP {resp.status_code}: {resp.text[:200]}")

        time.sleep(0.02)

    latencies.sort()
    n = len(latencies)
    print("\n--- summary ---")
    print(f"approved: {approved}, declined: {declined}, errors: {errors}, "
          f"duplicate resends correctly deduplicated: {duplicate_matches}")
    print(f"authorize latency ms: p50={latencies[n//2]:.1f}  p95={latencies[int(n*0.95)]:.1f}  "
          f"max={latencies[-1]:.1f}  (n={n})")


if __name__ == "__main__":
    run()