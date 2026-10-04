-- Allow an authorization attempt against a card token that doesn't exist
-- to be recorded for audit and fraud-signal purposes, rather than rejected
-- at the database level before we can log it.
--
-- card_token becomes nullable: null means "no matching card was found."
-- account_id becomes nullable for the same reason, no card means no account.
-- attempted_card_token preserves the raw value received, with no foreign
-- key, so repeated attempts against the same fake token can be detected
-- even though card_token itself is null.

ALTER TABLE authorizations
    ALTER COLUMN card_token DROP NOT NULL;

ALTER TABLE authorizations
    ALTER COLUMN account_id DROP NOT NULL;

ALTER TABLE authorizations
    ADD COLUMN attempted_card_token TEXT;
