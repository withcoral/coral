-- Installing the Coral admin Built-in Provider records the lifecycle action
-- 'provider.builtin_installed'.
ALTER TABLE provider_lifecycle_events
    DROP CONSTRAINT provider_lifecycle_events_action_check,
    ADD CONSTRAINT provider_lifecycle_events_action_check
        CHECK (action IN (
            'provider_setup.created', 'provider_setup.update_prepared', 'provider_setup.configuration_confirmed', 'provider_setup.discovery_completed', 'provider_setup.canceled',
            'provider_setup.expired', 'provider_publication.completed',

            'provider.disabled', 'provider.updated', 'provider.restored', 'provider.deleted', 'provider.oauth_client_secret_rotated',
            'provider.builtin_installed',
            'provider.credential_route_changed',
            'provider_account_authorization.started', 'provider_account_authorization.exchanging',
            'provider_account_authorization.completed', 'provider_account.connected', 'provider_account.deleted', 'provider_account.disconnected',
            'account_connection.completed',
            'provider_account.reconnected', 'provider_account.refreshed', 'provider_account.reconnection_required'
        ));
