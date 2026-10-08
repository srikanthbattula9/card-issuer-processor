-- 004: three balances and explicit hold expiry.
--
-- Sign convention for every journal line: customer and merchant accounts are
-- credit-normal, so a positive balance is money held for that account. Capture
-- lowers the customer's balance and raises the merchant clearing balance.
--
--   posted    = sum of ledger lines (what has actually moved)
--   held      = remaining amount of open, unexpired authorization holds (pending debits)
--   available = posted - held
--
-- Holds are not ledger entries. Pending credits are not modelled yet: every
-- credit posts immediately, so there is nothing to hold on that side.

ALTER TABLE authorizations ADD COLUMN expired_at TIMESTAMPTZ;

CREATE VIEW account_balances AS
SELECT account_id,
       posted_minor,
       held_minor,
       posted_minor - held_minor AS available_minor
FROM (
    SELECT b.account_id,
           b.ledger_balance_minor AS posted_minor,
           COALESCE((SELECT SUM(au.amount_minor - au.captured_amount_minor)
                     FROM authorizations au
                     WHERE au.account_id = b.account_id
                       AND au.status = 'approved'
                       AND au.expires_at > now()), 0) AS held_minor
    FROM account_ledger_balance b
) t;

-- Same name and columns as before, so the authorization engine is untouched.
CREATE OR REPLACE VIEW account_available_balance AS
SELECT account_id, available_minor AS available_balance_minor
FROM account_balances;
