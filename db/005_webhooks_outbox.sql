-- Webhooks via a transactional outbox.
-- events: one row per state change, written in the same transaction as the
--         ledger/authorization change, so an event exists if and only if the
--         change committed.
-- webhook_deliveries: one row per (event, endpoint), also written in that
--         transaction. A worker delivers them at-least-once with retries.
-- Endpoint secrets are stored in plaintext: acceptable for this simulator,
-- not for production (use a KMS-encrypted column or a secrets manager).

CREATE TABLE webhook_endpoints (
    endpoint_id  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    merchant_id  TEXT NOT NULL,
    url          TEXT NOT NULL,
    secret       TEXT NOT NULL,
    active       BOOLEAN NOT NULL DEFAULT true,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX webhook_endpoints_merchant_idx ON webhook_endpoints (merchant_id) WHERE active;

CREATE TABLE events (
    event_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_type   TEXT NOT NULL,
    merchant_id  TEXT,
    payload      JSONB NOT NULL,
    occurred_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX events_merchant_idx ON events (merchant_id, occurred_at);

CREATE TABLE webhook_deliveries (
    delivery_id      BIGSERIAL PRIMARY KEY,
    event_id         UUID NOT NULL REFERENCES events(event_id),
    endpoint_id      UUID NOT NULL REFERENCES webhook_endpoints(endpoint_id),
    status           TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'delivered', 'failed')),
    attempts         INT NOT NULL DEFAULT 0,
    next_attempt_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_status_code INT,
    last_error       TEXT,
    delivered_at     TIMESTAMPTZ,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (event_id, endpoint_id)
);
-- The worker only ever looks for due pending rows.
CREATE INDEX webhook_deliveries_due_idx ON webhook_deliveries (next_attempt_at) WHERE status = 'pending';
