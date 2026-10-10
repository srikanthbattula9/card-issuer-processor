# card-issuer-processor

A minimal **card issuer-processor** built in public: the system that sits between a cardholder and the card network, decides whether a transaction is approved, and keeps the books.

Modeled on the responsibilities of a card platform team: **authorization, transaction processing, account and card lifecycle, tokenization, and a double-entry ledger** — with a simulated card network, an event stream, and settlement reconciliation on top.

> **Scope, stated plainly:** simulated network, no real PANs, no real money, not PCI-scoped. The point is the *engineering* — correctness under retries, a ledger that always balances, and observability a payments team would actually watch.

## Status

| Module | State |
|---|---|
| Double-entry ledger (DB-enforced zero-sum invariant) | done, tested |
| Tokenized card vault (PAN kept out of operational tables) | schema only; encryption is a placeholder |
| Card lifecycle endpoints (issue, activate, freeze, close, one-time virtual cards) | planned |
| Authorization engine (card status, expiry, MCC blocklist, velocity limit, balance; decline codes 51/54/57/61/62/14; row-locked holds; invalid-card attempts recorded for audit with a NULL card_token and the raw value in attempted_card_token) | done, tested incl. concurrent-hold and concurrent-velocity races |
| Authorization rules: MCC blocklist (57), per-card velocity limit (61), evaluated under the account row lock | done, tested incl. concurrent-velocity test |
| Idempotency keys with recovery-point tracking | done, tested; completer to resume interrupted requests planned |
| Capture (partial/full), void, refund (offsetting entries) | done, tested |
| Three balances (posted, held, available, pending) from `GET /accounts/{id}/balance` | done, tested |
| ACH credit funding with simulated R01/R10 returns (`app/ach.py`, simulated clock) | done, tested incl. R10-after-posting taking a balance negative; no HTTP endpoint or scheduled sweep yet |
| Explicit hold expiry (`python -m app.expire`, `authorization.expired` event) | done, tested; a separate process, not scheduled |
| HTTP API (authorize, capture, void, refund, balance). Every authorization decline, including an unknown card token, returns HTTP 200 with `status: declined` and a `decline_code`; 4xx is reserved for malformed requests and idempotency conflicts | done |
| Kafka events (published after commit) | done, best-effort; delivery checked by count and by contents; still after-commit, so a crash between commit and publish loses the Kafka copy (the outbox row survives) |
| Webhooks: transactional outbox, Stripe-style signatures, retries with backoff, reference consumer that dedupes | done, tested end to end with forced failures and a forced duplicate; worker is a separate process, not scheduled |
| Network simulator (approve/decline/duplicate traffic, latency report) | done |
| Settlement reconciliation | planned; see `settlement-recon` |
| Grafana dashboards | planned |

This table is the roadmap. Each row becomes a PR with tests.

## Architecture

```
cardholder ──▶ [simulated network] ──auth/capture/settle──▶ [issuer-processor API]
                                                                  │
                     ┌──────────────┬──────────────┬──────────────┤
                     ▼              ▼              ▼              ▼
               card lifecycle   token vault    auth engine    double-entry ledger
                                                                  │
                                                       state changes ──▶ Kafka
                                                                  │
                                            ┌─────────────────────┴──────────────┐
                                            ▼                                    ▼
                                 settlement reconciliation                 metrics → Grafana
```

## Money model

Three balances, and they are different numbers (the `account_balances` view, served by `GET /accounts/{id}/balance`):

- **Posted (ledger) balance** — the sum of posted journal entries.
- **Held balance** is the remaining amount of open, unexpired authorization holds, which are pending debits.
- **Available balance** — ledger balance minus open authorization holds.

An authorization places a hold and does not move money. A capture posts the entry. Settlement is the network batch that confirms it. A void releases a hold. A refund is a new, opposite entry — never a deletion.

Every journal entry has legs that sum to zero. There is a test for that, and it runs on every push. Sign convention: customer and merchant balances are credit-normal, so a positive number is money held for that account; a capture lowers the customer's balance and raises the merchant clearing balance.

## Balances and hold expiry

`GET /accounts/{id}/balance` returns `posted_minor`, `held_minor` and `available_minor`, plus `ledger_balance_minor`, an alias of `posted_minor` kept for existing clients. All come from the `account_balances` view (migration 004). Holds are not ledger entries, so authorizing changes held and available but not posted. Capturing moves the captured amount from held into posted, and a partial capture leaves the remainder held. Pending credits are not modelled yet, because every credit posts immediately.

A hold stops counting against the available balance the moment `expires_at` passes, whether or not anything has run. `python -m app.expire` (or `--interval 30` to loop) makes that state explicit: it sets lapsed holds to `expired`, records `expired_at`, and publishes `authorization.expired` with the amount released. It skips rows locked by an in-flight capture and picks them up on the next pass, so it can run beside the server. After a partial capture, expiry releases only the uncaptured remainder.

Tested: balances through authorize, full capture and partial capture; a lapsed hold freeing available before the sweeper runs; the sweeper marking the row, its event contents, idempotency, partial-capture release, and skipping a locked row; capture rejected after expiry; and the new event type through the contents check (one live run, two holds, clean). Not tested: the sweeper under load, two sweepers running at once, and a fault injected into expired events (that matching has only a unit test). Nothing starts the sweeper for you, and the 24-hour hold length is a constant in `app/authorize.py`, not configuration. Publishing follows the same rule as every other event, after the commit, so a crash in between loses the event.

## Webhooks

Every state change (`authorization.decided`, `transaction.captured`, `transaction.refunded`, `authorization.voided`, `authorization.expired`) is written to an `events` table **in the same transaction** as the ledger or authorization change, with one `webhook_deliveries` row per active endpoint of the merchant (`app/outbox.py`). An event row therefore exists if and only if the change committed; there is no window in which one exists without the other. The `event_id` is generated there and the Kafka copy of the event carries the same ID.

Merchants register an endpoint with `POST /webhook_endpoints {"merchant_id", "url"}` and receive the signing secret once. A worker (`python -m app.webhooks [--interval N]`) claims due rows with `FOR UPDATE SKIP LOCKED`, POSTs the event, and marks the row `delivered` only on a 2xx. Failures are retried with full-jitter exponential backoff (`min(2s·2ⁿ, 5 min)`, 8 attempts, then `failed`). Delivery is **at-least-once**: a crash after the POST but before the row is updated re-sends the event.

Each request carries `Webhook-Id: <event_id>` and `Webhook-Signature: t=<unix>,v1=<hex>`, where `v1 = HMAC-SHA256(secret, "<t>.<raw body>")`. Signing the timestamp bounds replay; receivers reject signatures older than 5 minutes.

`consumer/app.py` is a reference receiver. It verifies the signature, then inserts the `event_id` into a table with a primary key inside the same transaction as its processing; a duplicate hits the key, does nothing, and is acknowledged with 200. `FAIL_FIRST_N=<n>` makes it reject the first n requests so the retry path can be watched.

Demonstrated locally (authorize → capture → refund against a consumer with `FAIL_FIRST_N=2`): first pass delivered 1 and scheduled 2 retries; both retried and delivered within ~4 s on attempt 2. Resetting a delivered row to `pending` re-sent it; the consumer reported `received 6, processed 3, duplicate 1, stored 3`. A request with a forged signature was rejected with 400.

Tested (`tests/test_webhooks.py`): signature round-trip, tampered body, wrong secret, stale and edited timestamp, malformed header; backoff bounds; one delivery row per active endpoint and none for inactive ones; signed successful delivery; failure → retry with later `next_attempt_at` → success; give-up after the maximum attempts; connection failure counts as an attempt; a row locked by another worker is skipped.

Limits: the worker holds the row lock across the HTTP call, so one worker delivers one row at a time (run several for parallelism). Endpoint secrets are stored in plaintext. There is no secret rotation, no endpoint management API beyond create, no per-merchant event-type subscription, and no dead-letter inspection endpoint. The outbox only covers webhooks; Kafka still publishes after commit.

## ACH

ACH credit funding, with simulated R01 (insufficient funds) and R10
(unauthorized) return behavior, driven by a simulated clock rather than
real time or backdated rows.

A credit (`app/ach.py:initiate_credit`) is **pending** -- in `pending_minor`,
not spendable -- until `post_due` posts it to the ledger once its R01
window has closed with no return. R01 can only return a still-pending
transfer, since in this simulated model its window closes exactly when
posting would happen. R10 has a much longer window and can return a
transfer that has already posted and been spent, reversing the original
journal entry -- which can take the account negative. That's real ACH
float risk, not a bug, and it's covered by a test.

```python
from app.ach import initiate_credit, post_due, apply_return
from app.clock import SystemClock

clock = SystemClock()
transfer_id = initiate_credit(conn, clock, account_id, amount_minor=5000)
conn.commit()

# ... two simulated days later ...
post_due(conn, clock, clearing_account_id)   # posts if no return arrived
conn.commit()

# ... a return file arrives ...
apply_return(conn, clock, clearing_account_id, transfer_id, "R01")
conn.commit()
```

`GET /accounts/{id}/balance` now returns `pending_minor` alongside the
three balances from Step 2.

Tested (`tests/test_ach.py`, 14 tests): a credit is pending and not
spendable; `post_due` does nothing before the window closes and posts
once it does; a hypothetical double-post is blocked by `journal_entries`'
unique idempotency key, not just application logic; R01 before posting
leaves no journal entries at all; R01 cannot return an already-posted
transfer; R10 before posting also leaves nothing; **R10 after posting
reverses a spent credit and takes the balance negative**; the R10 window
eventually closes; an unknown return code and a repeat return on an
already-returned transfer are both rejected; negative amounts are
rejected; the simulated clock advances independently of real time; the
balance endpoint reports `pending_minor`.

Limits: no HTTP endpoints for `initiate_credit`/`apply_return` yet (library
+ CLI-free for now, same stage webhooks were in before Step 3's API route);
`post_due` isn't run on a schedule, same as the hold-expiry sweeper; window
lengths are simulated constants, not NACHA's actual business-day rules
(which exclude weekends and holidays); ACH events publish to Kafka directly
and are not wired into the webhook outbox, since `record_event` resolves a
merchant via `auth_id` and ACH funding has neither.

## Card lifecycle

`issued → active → (frozen ⇄ active) → closed`

One-time virtual cards are a card type with a single-use constraint enforced inside the authorization engine, not by the caller.

## Tokenization

The PAN never appears in operational tables. It lives in a separate `card_vault` table; everything else references `card_token`. The vault has its own access path. This is a simulation of the boundary a real system would draw, not a claim of PCI compliance.

## Idempotency

Every mutating request carries an `Idempotency-Key`. The first request is processed; any retry with the same key returns the original result. This is tested under concurrent retries. A network timeout is *not* treated as a failure — the transaction is reconciled, never blindly retried.

## Stack

Python 3.12 · FastAPI · PostgreSQL · Kafka · Docker Compose · pytest · GitHub Actions · Grafana

## Running

```bash
docker compose up -d        # Postgres (localhost:5433) + Redpanda (localhost:9092)
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
make test
DATABASE_URL=postgresql://postgres:postgres@localhost:5433/cards uvicorn app.main:app --port 8000
python3 scripts/seed_cards.py && python3 scripts/simulate_network.py
```

The files in `db/` are applied in order, but only when the Postgres container is first created. After any schema change, `docker compose down && docker compose up -d` re-applies them (data is not persisted). CI applies the same files in the same order, so every new migration needs a matching `psql` step in `.github/workflows/ci.yml`.

## Load testing

```bash
python3 scripts/seed_cards.py --count 5000     # writes scripts/pool.json (gitignored)
# server: four processes, velocity limit raised so the test measures throughput, not the rule
VELOCITY_MAX_AUTHS=100000 DB_POOL_MAX=20 DATABASE_URL=postgresql://postgres:postgres@localhost:5433/cards \
  uvicorn app.main:app --port 8000 --workers 4
python3 scripts/load_test.py --rate 1000 --duration 30 --workers 128
```

`load_test.py` is open-loop: it schedules requests at the target rate and reports achieved throughput, latency percentiles, and queue wait separately, so a service that cannot keep up shows up as backlog and not as a quietly lower rate. The mix is about 85% approvals, 5% insufficient funds, 4% unknown card, 1% frozen, and 10% of approvals are immediately resent with the same idempotency key to check that the same `auth_id` comes back.

Server settings read from the environment:

| Variable | Default | Meaning |
|---|---|---|
| `DATABASE_URL` | local Postgres on 5433 | connection string |
| `DB_POOL_MIN` / `DB_POOL_MAX` | 10 / 60 | connections per server process; total across `--workers` must stay under Postgres's `max_connections` (100 by default) |
| `THREAD_LIMIT` | 40 | worker threads for synchronous endpoints |
| `VELOCITY_MAX_AUTHS` / `VELOCITY_WINDOW_SECONDS` | 5 / 60 | per-card velocity rule |
| `EVENTS_DISABLED` | unset | `1` skips Kafka publishing (used by the tests) |
| `EVENTS_NONBLOCKING` | unset | `1` publishes without waiting for broker acknowledgment (weaker delivery guarantee, see below) |

`GET /health` reports the active pool and thread settings and the event delivery failure counters of the process that answered.

## Measured results

Conditions for every number below: one laptop, Postgres and Redpanda in Docker on the same machine as the server and the load generator, `/authorize` only, velocity limit raised to 100,000, 30-second runs, Kafka publish waits for acknowledgment (the default).

| Configuration | Target/s | Achieved/s | p50 ms | p95 ms | Peak backlog |
|---|---|---|---|---|---|
| One process, connection per request | 500 | 361-385 | 305-325 | 372-405 | ~3,300-3,800 |
| + partial index on open holds (migration 003) | 500 | 405 | 289 | 352 | 2,730 |
| + connection pool | 500 | 500 | 8.4 | 11.4 | 16 |
| + connection pool | 800 | 799 | 11.5 | 49.4 | 22 |
| + connection pool | 1,000 | 858 | 137 | 168 | 4,031 |
| + four server processes, pool 20 each | 1,000 | 1,000 | 4.1 | 8.5 | 20 |
| + four server processes, pool 20 each | 1,500 | 1,455-1,480 | 130-131 | 260-271 | 415-1,132 |

What changed each step, from measurements: the index replaced a sequential scan of `authorizations` in the available-balance view (`EXPLAIN ANALYZE`: 3.7 ms to 0.5 ms for the query); the pool removed per-request connection setup (visible in a `py-spy` profile before and after); the extra server processes removed a single-interpreter ceiling at about 880/s that did not move with the Kafka flush, the thread limit, the pool size, or a second load generator.

### Event delivery check

`scripts/verify_events.sh RATE SECONDS` runs a load test and compares the number of authorizations written to Postgres with the number of events written to the `card.transactions` topic (read from the broker's high watermark), so the comparison does not depend on any in-process counter.

| Publish mode | Rate | Requests | Events on topic | Result |
|---|---|---|---|---|
| blocking (default) | 300/s | 6,000 | 6,000 | match |
| blocking (default) | 1,000/s | 20,000 | 20,000 | match |
| non-blocking (`EVENTS_NONBLOCKING=1`) | 1,000/s | 20,000 | 19,996 | 4 events lost |

Each row is one run. The four missing events were still missing when the topic was read again afterward, so they were lost, not late; the cause was not investigated. The non-blocking mode was a few milliseconds faster (p95 8.9 ms against 15.2 ms at 1,000/s) and gave no throughput gain once the server ran several processes, so it is not recommended and stays off by default. In both modes an event is published after the database commit, so a crash between the two still loses it; a transactional outbox would close that gap and is not built.

Not yet verified: throughput with the velocity rule at its default; runs longer than 30 seconds; the event check at rates above 1,000/s or with the blocking mode more than once per rate; results on hardware other than this laptop.

### Event contents check

Comparing counts cannot see a duplicate that cancels a loss: one missing event plus one extra copy leaves the totals equal. `scripts/verify_contents.py` compares contents instead. Run `python3 scripts/verify_contents.py snapshot` before a run (it records the topic offset and the database clock) and `python3 scripts/verify_contents.py check` after. The check reads the events in that window and matches each to its database row by key, `auth_id` for authorizations and voids and `entry_id` for captures and refunds, then reports four numbers per event type: missing, extra copies, unexpected, and mismatched.

What it compares: for authorizations, the status (any row that is not declined counts as approved, because the column later becomes captured, voided or expired while the event records the decision), decline code and amount; for captures and refunds, the event's `auth_id` and amount against the stored response and the ledger lines, and the ledger entry type; for voids, existence only, because the event carries only an `auth_id`.

Clean runs (blocking publish, one laptop):

| Run | Events in window | Result |
|---|---|---|
| 300/s load test (6,000 requests, 504 duplicate resends) plus `simulate_network.py` | 6,851: 6,500 decisions, 351 captures | clean |
| 1 void, a capture with 2 partial refunds, 2 partial captures on another authorization | 9 | clean |

The duplicate resends published no events.

Fault injection, each done on purpose on a local database:

| Fault | Count check | Contents check |
|---|---|---|
| Authorization row with no event | detects (one short) | `missing=1` |
| Same kind of missing row plus a duplicate event for another row | passes (totals equal) | `missing=1`, `extra_copies=1` |
| Authorization amount changed by 1 cent | passes | `mismatched=1` |
| Capture ledger lines shifted by 1 cent, entry kept balanced | not covered (counts authorizations only) | `mismatched=1` |
| Void record with no event | not covered | `missing=1` |

Limits: no fault was injected into refund events, so the refund comparison, which shares code with the capture comparison, has not been shown to fire on its own. The check has not been run with `EVENTS_NONBLOCKING=1`, above 300/s, or more than once per setting. It windows on the database clock, so run it when the system is idle; a request in flight at the snapshot can show up as unexpected. It needs Kafka and Postgres, so CI does not run it; the matching logic is covered by unit tests in `tests/test_verify_contents.py`. A crash between commit and publish would show up as a missing event, which is the gap a transactional outbox would close.

## Why this exists

I spent two years on the payments side of redBus (India's largest bus ticketing platform): reconciliation across seven gateways, UPI checkout migration, real-time gateway routing, and tracing double-debit incidents to missing idempotency. This project is that work, rebuilt in the vocabulary of US card issuing and processing, in public, with the invariants tested.
