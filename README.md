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
| HTTP API (authorize, capture, void, refund, balance). Every authorization decline, including an unknown card token, returns HTTP 200 with `status: declined` and a `decline_code`; 4xx is reserved for malformed requests and idempotency conflicts | done |
| Kafka events (published after commit) | done, best-effort; transactional outbox planned |
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

Two balances, and they are different numbers:

- **Ledger balance** — the sum of posted journal entries.
- **Available balance** — ledger balance minus open authorization holds.

An authorization places a hold and does not move money. A capture posts the entry. Settlement is the network batch that confirms it. A void releases a hold. A refund is a new, opposite entry — never a deletion.

Every journal entry has legs that sum to zero. There is a test for that, and it runs on every push.

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

## Why this exists

I spent two years on the payments side of redBus (India's largest bus ticketing platform): reconciliation across seven gateways, UPI checkout migration, real-time gateway routing, and tracing double-debit incidents to missing idempotency. This project is that work, rebuilt in the vocabulary of US card issuing and processing, in public, with the invariants tested.
