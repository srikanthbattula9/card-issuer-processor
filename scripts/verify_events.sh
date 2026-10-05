#!/usr/bin/env bash
# Runs one load test and compares events written to Kafka against authorizations
# written to Postgres. Run against a single server process started with
# EVENTS_DISABLED unset. Authorize publishes one event per fresh request and none
# for an idempotent replay, so the two deltas should match.
set -euo pipefail

RATE="${1:-300}"
DURATION="${2:-20}"

topic_count() {
  docker compose exec -T redpanda rpk topic describe card.transactions -p | awk 'NR==2 {print $6}'
}
db_count() {
  docker compose exec -T postgres psql -U postgres -d cards -Atc "SELECT count(*) FROM authorizations;"
}

sleep 2
K0=$(topic_count); D0=$(db_count)
python3 scripts/load_test.py --rate "$RATE" --duration "$DURATION" --workers 128 || true
sleep 5   # let in-flight deliveries land
K1=$(topic_count); D1=$(db_count)

echo
echo "--- event delivery check ---"
echo "authorizations written: $((D1 - D0))"
echo "events on topic:        $((K1 - K0))"
if [ "$((D1 - D0))" -eq "$((K1 - K0))" ]; then echo "MATCH"; else echo "MISMATCH: $(( (D1 - D0) - (K1 - K0) )) events missing (negative = extra events)"; fi
