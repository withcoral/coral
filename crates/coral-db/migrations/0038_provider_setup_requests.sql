-- What a Program asked a Provider Setup to publish: the namespace and the
-- Credential Route it gave `createProvider`. Reef reads them from the Setup,
-- not from its link, and `listProviders` reports the Setup while it is open.
-- A Setup a person started, and every earlier Setup, keeps NULL in both.
ALTER TABLE provider_setups ADD COLUMN requested_namespace TEXT;
ALTER TABLE provider_setups ADD COLUMN requested_route TEXT
    CONSTRAINT provider_setups_request_complete
    CHECK (COALESCE(
        (requested_namespace IS NULL AND requested_route IS NULL)
        OR
        (LENGTH(requested_namespace) > 0
            AND requested_route IN ('none', 'fixed', 'caller')),
        FALSE
    ));

CREATE INDEX idx_provider_setups_requested
    ON provider_setups (workspace_id, actor_user_id, created_at_unix_nanos)
    WHERE requested_namespace IS NOT NULL;
