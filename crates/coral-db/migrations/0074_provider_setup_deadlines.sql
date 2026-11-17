-- A Provider Setup expires at the deadline it stores, one hour after it was
-- created. Coral has stored that deadline in every Setup it created, but the
-- column allows NULL. This step gives a Setup that holds NULL the deadline it
-- would have had. Coral then reads the deadline from the row alone, and
-- rejects a Setup that has none as corrupt.
--
-- 3600000000000 is one hour in nanoseconds: the Setup lifetime that Provider
-- Setup creation adds to the creation time
-- (`SETUP_CREDENTIAL_LIFETIME_NANOS` in coral-provider).
--
-- The column stays nullable. SQLite cannot add NOT NULL to a column without
-- rebuilding the table.
UPDATE provider_setups
SET expires_at_unix_nanos = created_at_unix_nanos + 3600000000000
WHERE expires_at_unix_nanos IS NULL;

-- `listProviders` reads the Setups that Programs asked a User to finish and
-- that are not yet at their deadline. The read now bounds the stored deadline,
-- not the creation time, so the index holds the deadline. Setups are never
-- deleted: without the bound, the read visits every Setup a Program ever
-- asked that User to finish. No other read states the index's predicate.
DROP INDEX idx_provider_setups_requested;
CREATE INDEX idx_provider_setups_requested
    ON provider_setups (workspace_id, actor_user_id, expires_at_unix_nanos)
    WHERE requested_namespace IS NOT NULL;
