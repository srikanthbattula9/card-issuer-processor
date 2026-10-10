-- ACH credit funding with simulated return behavior.
--
-- A credit is pending (not in posted_minor, not spendable) until its R01
-- window closes with no return, at which point it posts to the ledger.
-- R01 (insufficient funds) can only return a still-pending transfer -- its
-- window closes exactly when posting would happen. R10 (unauthorized) has
-- a much longer window and can return a transfer that has already posted,
-- reversing the original entry; this can take the account negative, which
-- is real ACH float risk, not a bug.
--
-- Windows are simulated values, not NACHA's actual business-day rules.

ALTER TABLE accounts DROP CONSTRAINT accounts_account_type_check;
ALTER TABLE accounts ADD CONSTRAINT accounts_account_type_check
    CHECK (account_type IN ('customer','merchant','platform_fees','network_clearing','suspense','ach_clearing'));

ALTER TABLE journal_entries DROP CONSTRAINT journal_entries_entry_type_check;
ALTER TABLE journal_entries ADD CONSTRAINT journal_entries_entry_type_check
    CHECK (entry_type IN ('capture','settlement','refund','fee','adjustment','ach_credit','ach_reversal'));

CREATE TABLE ach_transfers (
    transfer_id     UUID PRIMARY KEY,
    account_id      BIGINT NOT NULL REFERENCES accounts(account_id),
    amount_minor    BIGINT NOT NULL CHECK (amount_minor > 0),
    status          TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','posted','returned')),
    initiated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    posted_at       TIMESTAMPTZ,
    returned_at     TIMESTAMPTZ,
    return_code     TEXT CHECK (return_code IN ('R01','R10')),
    entry_id        BIGINT REFERENCES journal_entries(entry_id),
    reversal_entry_id BIGINT REFERENCES journal_entries(entry_id)
);
CREATE INDEX ON ach_transfers(account_id);
CREATE INDEX ON ach_transfers(status, initiated_at);

-- Adds pending_minor (ACH credits not yet posted). Deliberately NOT added to
-- available_minor: a pending credit is not spendable money yet. Column set
-- changed from the 004 version, so this is a drop+create, not a replace.
DROP VIEW IF EXISTS account_available_balance;
DROP VIEW IF EXISTS account_balances;

CREATE VIEW account_balances AS
SELECT account_id, posted_minor, held_minor, pending_minor,
       posted_minor - held_minor AS available_minor
FROM (
    SELECT b.account_id,
           b.ledger_balance_minor AS posted_minor,
           COALESCE((SELECT SUM(au.amount_minor - au.captured_amount_minor)
                     FROM authorizations au
                     WHERE au.account_id = b.account_id
                       AND au.status = 'approved'
                       AND au.expires_at > now()), 0) AS held_minor,
           COALESCE((SELECT SUM(t.amount_minor)
                     FROM ach_transfers t
                     WHERE t.account_id = b.account_id
                       AND t.status = 'pending'), 0) AS pending_minor
    FROM account_ledger_balance b
) t;

CREATE VIEW account_available_balance AS
SELECT account_id, available_minor AS available_balance_minor
FROM account_balances;
