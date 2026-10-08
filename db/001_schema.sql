-- card-issuer-processor: core schema
-- Postgres 15+. MySQL-compatible types where practical.

CREATE TABLE accounts (
    account_id      BIGSERIAL PRIMARY KEY,
    account_type    TEXT NOT NULL CHECK (account_type IN ('customer','merchant','platform_fees','network_clearing','suspense')),
    currency        CHAR(3) NOT NULL DEFAULT 'USD',
    status          TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active','frozen','closed')),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The PAN lives ONLY here. Operational tables reference card_token.
CREATE TABLE card_vault (
    card_token      UUID PRIMARY KEY,
    pan_encrypted   BYTEA NOT NULL,
    pan_last4       CHAR(4) NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE cards (
    card_token      UUID PRIMARY KEY REFERENCES card_vault(card_token),
    account_id      BIGINT NOT NULL REFERENCES accounts(account_id),
    card_type       TEXT NOT NULL CHECK (card_type IN ('physical','virtual','virtual_one_time')),
    status          TEXT NOT NULL DEFAULT 'issued' CHECK (status IN ('issued','active','frozen','closed')),
    expires_on      DATE NOT NULL,
    uses_remaining  INT,                       -- NULL = unlimited; 1 for one-time virtual
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    activated_at    TIMESTAMPTZ,
    closed_at       TIMESTAMPTZ
);

-- One row per money movement; legs live in journal_lines and must sum to zero.
CREATE TABLE journal_entries (
    entry_id        BIGSERIAL PRIMARY KEY,
    entry_type      TEXT NOT NULL CHECK (entry_type IN ('capture','settlement','refund','fee','adjustment')),
    transaction_id  UUID NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    posted_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    description     TEXT
);

CREATE TABLE journal_lines (
    line_id         BIGSERIAL PRIMARY KEY,
    entry_id        BIGINT NOT NULL REFERENCES journal_entries(entry_id),
    account_id      BIGINT NOT NULL REFERENCES accounts(account_id),
    amount_minor    BIGINT NOT NULL,           -- cents; positive = money held for the account (see the 004 header); every entry sums to zero
    currency        CHAR(3) NOT NULL DEFAULT 'USD'
);
CREATE INDEX ON journal_lines(account_id);
CREATE INDEX ON journal_lines(entry_id);

-- Authorization holds are NOT ledger entries. They reduce available balance only.
CREATE TABLE authorizations (
    auth_id         UUID PRIMARY KEY,
    card_token      UUID NOT NULL REFERENCES cards(card_token),
    account_id      BIGINT NOT NULL REFERENCES accounts(account_id),
    amount_minor    BIGINT NOT NULL CHECK (amount_minor > 0),
    currency        CHAR(3) NOT NULL DEFAULT 'USD',
    merchant_id     TEXT NOT NULL,
    mcc             CHAR(4) NOT NULL,
    status          TEXT NOT NULL CHECK (status IN ('approved','declined','captured','voided','expired')),
    decline_code    TEXT,                      -- e.g. '51' insufficient funds, '05' do not honor, '14' invalid card
    idempotency_key TEXT NOT NULL UNIQUE,
    approved_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at      TIMESTAMPTZ NOT NULL,      -- unreleased holds expire and free the balance
    captured_amount_minor BIGINT NOT NULL DEFAULT 0
);
CREATE TABLE decline_codes (
    code        TEXT PRIMARY KEY,
    description TEXT NOT NULL
);

INSERT INTO decline_codes (code, description) VALUES
    ('51', 'insufficient funds'),
    ('05', 'do not honor'),
    ('14', 'invalid card number'),
    ('54', 'expired card'),
    ('43', 'card reported lost or stolen'),
    ('61', 'exceeds withdrawal/velocity limit'),
    ('62', 'restricted card (frozen)'),
    ('57', 'transaction not permitted (blocked merchant category)');

CREATE TABLE blocked_mccs (
    mcc         CHAR(4) PRIMARY KEY,
    description TEXT NOT NULL
);

INSERT INTO blocked_mccs (mcc, description) VALUES
    ('7995', 'betting / gambling'),
    ('6051', 'quasi-cash / crypto');
    
CREATE INDEX ON authorizations(card_token, approved_at);
CREATE INDEX ON authorizations(status, expires_at);

-- Idempotency: first response wins; retries get the stored response.
CREATE TABLE idempotency_keys (
    idempotency_key TEXT PRIMARY KEY,
    request_path    TEXT NOT NULL,
    request_hash    TEXT NOT NULL,
    recovery_point  TEXT NOT NULL DEFAULT 'started'
        CHECK (recovery_point IN ('started', 'hold_placed', 'network_called', 'finished')),
    locked_at       TIMESTAMPTZ,
    response_status INT,
    response_body   JSONB,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_run_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Ledger balance: sum of posted lines.
CREATE VIEW account_ledger_balance AS
SELECT a.account_id,
       COALESCE(SUM(l.amount_minor), 0) AS ledger_balance_minor
FROM accounts a
LEFT JOIN journal_lines l ON l.account_id = a.account_id
GROUP BY a.account_id;

-- Available balance: ledger balance minus open (approved, uncaptured) holds.
CREATE VIEW account_available_balance AS
SELECT b.account_id,
       b.ledger_balance_minor
       - COALESCE((SELECT SUM(amount_minor - captured_amount_minor)
                   FROM authorizations au
                   WHERE au.account_id = b.account_id
                     AND au.status = 'approved'
                     AND au.expires_at > now()), 0) AS available_balance_minor
FROM account_ledger_balance b;

-- Invariant: every journal entry's lines sum to zero.
CREATE OR REPLACE FUNCTION assert_entry_balanced() RETURNS TRIGGER AS $$
DECLARE total BIGINT;
BEGIN
    SELECT COALESCE(SUM(amount_minor),0) INTO total FROM journal_lines WHERE entry_id = NEW.entry_id;
    IF total <> 0 THEN
        RAISE EXCEPTION 'journal entry % does not balance (sum=%)', NEW.entry_id, total;
    END IF;
    RETURN NEW;
END $$ LANGUAGE plpgsql;

-- Deferred so multi-leg inserts can complete inside one transaction before the check fires.
CREATE CONSTRAINT TRIGGER journal_entry_balanced
AFTER INSERT OR UPDATE ON journal_lines
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION assert_entry_balanced();
