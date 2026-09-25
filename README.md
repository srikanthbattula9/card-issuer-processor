# card-issuer-processor

A minimal **card issuer-processor** built in public: the system that sits between a cardholder and the card network, decides whether a transaction is approved, and keeps the books.

Modeled on the responsibilities of a card platform team: **authorization, transaction processing, account and card lifecycle, tokenization, and a double-entry ledger** — with a simulated card network, an event stream, and settlement reconciliation on top.

> **Scope, stated plainly:** simulated network, no real PANs, no real money, not PCI-scoped. The point is the *engineering* — correctness under retries, a ledger that always balances, and observability a payments team would actually watch.

## Status

| Module | State |
|---|---|
| Double-entry ledger (accounts, journal entries, balance invariant) | in progress |
| Card lifecycle (issue, activate, freeze, close, one-time virtual cards) | planned |
| Tokenization (PAN vault separated from operational tables) | planned |
| Authorization engine (balance, status, MCC rules, velocity, decline codes) | planned |
| Transaction processing (auth → capture → settle, void, refund, partial capture, auth expiry) | planned |
| Idempotency keys + safe retries under concurrency | planned |
| Simulated card network (auth requests, capture, daily settlement files, chaos) | planned |
| Kafka event stream (every state change published) | planned |
| Settlement reconciliation consumer | planned |
| Grafana: approval rate, decline mix, p95 auth latency, ledger drift | planned |

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
docker compose up -d        # postgres, kafka, grafana
make migrate                # apply schema
make test                   # ledger invariant, idempotency, lifecycle
```

## Why this exists

I spent two years on the payments side of redBus (India's largest bus ticketing platform): reconciliation across seven gateways, UPI checkout migration, real-time gateway routing, and tracing double-debit incidents to missing idempotency. This project is that work, rebuilt in the vocabulary of US card issuing and processing, in public, with the invariants tested.
