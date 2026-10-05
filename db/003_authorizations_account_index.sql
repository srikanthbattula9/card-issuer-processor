CREATE INDEX authorizations_open_holds_idx
    ON authorizations (account_id, expires_at)
    WHERE status = 'approved';
