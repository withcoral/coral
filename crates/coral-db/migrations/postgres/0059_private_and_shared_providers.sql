-- Every Workspace has a Private Provider at `coral.private`, and will have a
-- Shared Provider at `coral.shared` (ADR 0032). Coral performs their Tools
-- itself, so each pins a Generation of input kind 'private' or 'shared' that
-- holds no materialized Tools, and installing one records the lifecycle
-- action 'provider.private_installed' or 'provider.shared_installed'.
-- Each rule below only allows more values, so every existing row already
-- passes it. NOT VALID skips the row scan that would hold an ACCESS EXCLUSIVE
-- lock on the table; new writes are still checked. lock_timeout makes the
-- migration fail instead of queueing reads and writes behind its lock.
SET LOCAL lock_timeout = '5s';

ALTER TABLE provider_generations
    DROP CONSTRAINT provider_generations_input_kind_valid,
    ADD CONSTRAINT provider_generations_input_kind_valid
        CHECK (input_kind IN ('openapi', 'mcp', 'builtin', 'private', 'shared')) NOT VALID;

ALTER TABLE provider_lifecycle_events
    DROP CONSTRAINT provider_lifecycle_events_action_check,
    ADD CONSTRAINT provider_lifecycle_events_action_check
        CHECK (action IN (
            'provider_setup.created', 'provider_setup.update_prepared', 'provider_setup.configuration_confirmed', 'provider_setup.discovery_completed', 'provider_setup.canceled',
            'provider_setup.expired', 'provider_publication.completed',

            'provider.disabled', 'provider.updated', 'provider.restored', 'provider.deleted', 'provider.oauth_client_secret_rotated',
            'provider.builtin_installed', 'provider.private_installed', 'provider.shared_installed',
            'provider.credential_route_changed',
            'provider_account_authorization.started', 'provider_account_authorization.exchanging',
            'provider_account_authorization.completed', 'provider_account.connected', 'provider_account.deleted', 'provider_account.disconnected',
            'account_connection.completed',
            'provider_account.reconnected', 'provider_account.refreshed', 'provider_account.reconnection_required'
        )) NOT VALID;
