-- Private application material. Client settings remain in Provider configuration.
CREATE TABLE provider_oauth_client_secrets (
    id TEXT NOT NULL PRIMARY KEY,
    workspace_id TEXT NOT NULL REFERENCES workspace_records(id),
    bound_client_id TEXT NOT NULL,
    encrypted_secret TEXT NOT NULL
);

-- A Setup pins its exact input at the `ready` transition. Before that an MCP
-- source that demanded authentication holds only its safe source and the
-- discovered authorization requirement; consumption is recorded by Publication.
CREATE TABLE provider_setups (
    update_account_fence TEXT,
    id TEXT NOT NULL PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    input_kind TEXT NOT NULL,
    pinned_input TEXT,
    input_digest TEXT,
    configuration TEXT,
    source_analysis TEXT,
    configuration_preview TEXT,
    oauth_client_secret_id TEXT UNIQUE REFERENCES provider_oauth_client_secrets(id),
    configuration_revision BIGINT NOT NULL DEFAULT 0 CHECK (configuration_revision >= 0),
    update_provider_id TEXT,
    previous_configuration TEXT,
    state TEXT NOT NULL DEFAULT 'ready',
    input_url TEXT,
    actor_user_id TEXT,
    expires_at_unix_nanos BIGINT,
    authorization_requirement TEXT,
    initial_request_headers TEXT NOT NULL,
    discovery_failure_code TEXT,
    consumed_by_publication_id TEXT,
    created_at_unix_nanos BIGINT NOT NULL,

    CONSTRAINT provider_setups_workspace_fk
        FOREIGN KEY (workspace_id)
        REFERENCES workspace_records(id),
    CONSTRAINT provider_setups_input_kind_valid
        CHECK (input_kind IN ('openapi', 'mcp')),
    CONSTRAINT provider_setups_state_valid
        CHECK (state IN ('awaiting_configuration', 'awaiting_authorization', 'awaiting_discovery', 'ready', 'expired', 'canceled')),
    CONSTRAINT provider_setups_pinned_input_shape
        CHECK (
            (pinned_input IS NULL AND input_digest IS NULL)
            OR (LENGTH(pinned_input) > 0 AND LENGTH(input_digest) = 64)
        ),
    CONSTRAINT provider_setups_source_analysis_shape
        CHECK ((input_kind = 'openapi' AND source_analysis IS NOT NULL)
            OR (input_kind = 'mcp' AND source_analysis IS NULL)),
    CONSTRAINT provider_setups_preview_requires_input
        CHECK (configuration_preview IS NULL OR (pinned_input IS NOT NULL AND configuration IS NOT NULL)),
    CONSTRAINT provider_setups_ready_pins_input
        CHECK (state <> 'ready' OR pinned_input IS NOT NULL),
    CONSTRAINT provider_setups_consumed_from_ready
        CHECK (consumed_by_publication_id IS NULL OR state = 'ready'),
    CONSTRAINT provider_setups_pre_ready_requirement
        CHECK (
            state NOT IN ('awaiting_authorization', 'awaiting_discovery')
            OR ((authorization_requirement IS NOT NULL OR configuration IS NOT NULL) AND actor_user_id IS NOT NULL AND expires_at_unix_nanos IS NOT NULL)
        )
);

CREATE INDEX idx_provider_setups_workspace_id
    ON provider_setups (workspace_id, id);

CREATE TABLE provider_records (
    oauth_client_failure_code TEXT CHECK (oauth_client_failure_code IS NULL OR oauth_client_failure_code = 'oauth_client_configuration'),
    id TEXT NOT NULL PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    display_name TEXT NOT NULL,
    namespace TEXT NOT NULL,
    credential_route TEXT NOT NULL CHECK (credential_route IN ('none', 'caller', 'fixed')),
    account_setting_version BIGINT NOT NULL DEFAULT 1 CHECK (account_setting_version > 0),
    active_generation_id TEXT NOT NULL,
    source_url TEXT,
    oauth_client_secret_id TEXT UNIQUE REFERENCES provider_oauth_client_secrets(id),
    resource_version BIGINT NOT NULL DEFAULT 1 CHECK (resource_version > 0),
    created_at_unix_nanos BIGINT NOT NULL,
    state TEXT NOT NULL DEFAULT 'active'
    CHECK (state IN ('active', 'disabled')),

    UNIQUE (workspace_id, namespace),
    CONSTRAINT provider_records_workspace_fk
        FOREIGN KEY (workspace_id)
        REFERENCES workspace_records(id),
    CONSTRAINT provider_records_active_generation_fk
        FOREIGN KEY (active_generation_id)
        REFERENCES provider_generations(id),
    CONSTRAINT provider_records_display_name_not_empty
        CHECK (LENGTH(display_name) > 0)
);

CREATE UNIQUE INDEX idx_provider_records_workspace
    ON provider_records (workspace_id, id);

CREATE TABLE provider_publication_request_keys (
    request_id TEXT NOT NULL,
    workspace_id TEXT NOT NULL,
    actor_user_id TEXT NOT NULL,
    PRIMARY KEY (workspace_id, request_id),

    CONSTRAINT provider_publication_request_keys_workspace_fk
        FOREIGN KEY (workspace_id)
        REFERENCES workspace_records(id)
);

-- Publications and durable workflow records retain Provider/Account identifiers
-- independently of current authority records. Their snapshots are historical
-- attribution, not foreign-key roots that prevent destructive lifecycle.

CREATE TABLE provider_publications (
    id TEXT NOT NULL PRIMARY KEY,
    request_id TEXT NOT NULL,
    workspace_id TEXT NOT NULL,
    setup_id TEXT NOT NULL,
    requested_namespace TEXT NOT NULL,
    requested_route TEXT NOT NULL,
    requested_account_grant TEXT,
    outcome TEXT NOT NULL,
    diagnostic_code TEXT,
    diagnostic_message TEXT,
    provider_id TEXT,
    provider_display_name TEXT,
    provider_account_setting_version BIGINT,
    completed_at_unix_nanos BIGINT NOT NULL,

    CONSTRAINT provider_publications_workspace_request_unique
        UNIQUE (workspace_id, request_id),
    CONSTRAINT provider_publications_request_key_fk
        FOREIGN KEY (workspace_id, request_id)
        REFERENCES provider_publication_request_keys(workspace_id, request_id),
    CONSTRAINT provider_publications_workspace_fk
        FOREIGN KEY (workspace_id)
        REFERENCES workspace_records(id),
    CONSTRAINT provider_publications_setup_fk
        FOREIGN KEY (setup_id)
        REFERENCES provider_setups(id),
    CONSTRAINT provider_publications_requested_route_valid
        CHECK (requested_route IN ('none', 'caller', 'fixed')),
    CONSTRAINT provider_publications_outcome_valid
        CHECK (outcome IN ('succeeded', 'failed')),
    CONSTRAINT provider_publications_snapshot_fields_valid
        CHECK (
            (provider_display_name IS NULL OR LENGTH(provider_display_name) > 0)
            AND
            (provider_account_setting_version IS NULL OR provider_account_setting_version > 0)
        ),
    CONSTRAINT provider_publications_terminal_fields_consistent
        CHECK (
            (outcome = 'succeeded'
                AND diagnostic_code IS NULL
                AND diagnostic_message IS NULL
                AND provider_id IS NOT NULL
                AND provider_display_name IS NOT NULL
                AND provider_account_setting_version IS NOT NULL)
            OR
            (outcome = 'failed'
                AND diagnostic_code IS NOT NULL
                AND diagnostic_message IS NOT NULL
                AND provider_id IS NULL
                AND provider_display_name IS NULL
                AND provider_account_setting_version IS NULL)
        )
);

CREATE INDEX idx_provider_publications_workspace_request
    ON provider_publications (workspace_id, request_id);

CREATE TABLE provider_regeneration_publications (
    account_setting TEXT CHECK (account_setting IN ('none', 'caller', 'fixed')),
    account_grant TEXT,
    id TEXT NOT NULL PRIMARY KEY,
    allow_incompatible_configuration BOOLEAN NOT NULL,
    configuration_revision BIGINT NOT NULL CHECK (configuration_revision > 0),
    request_id TEXT NOT NULL,
    workspace_id TEXT NOT NULL,
    provider_id TEXT NOT NULL,
    setup_id TEXT NOT NULL,
    outcome TEXT NOT NULL,
    diagnostic_code TEXT,
    diagnostic_message TEXT,
    completed_at_unix_nanos BIGINT NOT NULL,

    CONSTRAINT provider_regeneration_publications_request_key_fk
        FOREIGN KEY (workspace_id, request_id)
        REFERENCES provider_publication_request_keys(workspace_id, request_id),
    CONSTRAINT provider_regeneration_publications_workspace_fk
        FOREIGN KEY (workspace_id)
        REFERENCES workspace_records(id),
    CONSTRAINT provider_regeneration_publications_setup_fk
        FOREIGN KEY (setup_id)
        REFERENCES provider_setups(id),
    CONSTRAINT provider_regeneration_publications_outcome_valid
        CHECK (outcome IN ('succeeded', 'failed')),
    CONSTRAINT provider_regeneration_publications_terminal_fields_consistent
        CHECK (
            (outcome = 'succeeded'
                AND diagnostic_code IS NULL
                AND diagnostic_message IS NULL)
            OR
            (outcome = 'failed'
                AND diagnostic_code IS NOT NULL
                AND diagnostic_message IS NOT NULL)
        )
);

CREATE UNIQUE INDEX idx_provider_regeneration_publications_workspace_request
    ON provider_regeneration_publications (workspace_id, request_id);

CREATE INDEX idx_provider_regeneration_publications_provider
    ON provider_regeneration_publications (provider_id, completed_at_unix_nanos, id);

-- Setup-scoped credential material sealed before Publication. It confers no
-- runtime Account authority: only the terminal Publication transaction promotes
-- it, and every other terminal outcome scrubs the sealed material.
CREATE TABLE provider_setup_credentials (
    setup_id TEXT NOT NULL PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    actor_user_id TEXT NOT NULL,
    provisional_grant_id TEXT NOT NULL,
    contract TEXT NOT NULL,
    credential_generation TEXT NOT NULL,
    encrypted_credentials TEXT,
    automatic_refresh BIGINT CHECK (automatic_refresh IS NULL OR automatic_refresh IN (0, 1)),
    granted_scope TEXT NOT NULL DEFAULT '',
    expires_at_unix_nanos BIGINT NOT NULL,
    oauth_client TEXT,
    private_flow TEXT,
    user_code TEXT,
    authorization_url TEXT,
    authorization_state TEXT NOT NULL DEFAULT 'authorized',
    authorization_expires_at_unix_nanos BIGINT,
    callback_fingerprint TEXT,
    failure_code TEXT,
    callback_state_digest TEXT,

    CONSTRAINT provider_setup_credentials_authorization_state_valid
        CHECK (authorization_state IN ('pending', 'exchanging', 'authorized', 'failed', 'canceled', 'expired')),
    CONSTRAINT provider_setup_credentials_failure_code_valid
        CHECK (failure_code IS NULL OR failure_code IN ('insufficient_scope', 'authorization_failed', 'access_denied', 'configuration_changed', 'oauth_client_configuration',
            'oauth_token_unavailable', 'oauth_token_timeout', 'oauth_token_response_invalid', 'oauth_code_rejected',
            'oauth_redirect_mismatch', 'oauth_scope_rejected', 'oauth_request_rejected', 'outbound_destination_rejected',
            'oauth_access_denied', 'oauth_device_expired')),
    CONSTRAINT provider_setup_credentials_private_flow_open_only
        CHECK (authorization_state IN ('pending', 'exchanging') OR private_flow IS NULL),
    CONSTRAINT provider_setup_credentials_setup_fk
        FOREIGN KEY (setup_id)
        REFERENCES provider_setups(id),
    CONSTRAINT provider_setup_credentials_workspace_fk
        FOREIGN KEY (workspace_id)
        REFERENCES workspace_records(id),
    CONSTRAINT provider_setup_credentials_contract_not_empty
        CHECK (LENGTH(contract) > 0),
    CONSTRAINT provider_setup_credentials_provisional_grant_shape
        CHECK (LENGTH(provisional_grant_id) = 36),
    CONSTRAINT provider_setup_credentials_generation_shape
        CHECK (LENGTH(credential_generation) = 36)
);

CREATE INDEX idx_provider_setup_credentials_callback_state
    ON provider_setup_credentials(callback_state_digest);

CREATE TABLE provider_accounts (
    id TEXT NOT NULL PRIMARY KEY,
    workspace_id TEXT NOT NULL REFERENCES workspace_records(id),
    provider_id TEXT NOT NULL,
    material_revision TEXT NOT NULL UNIQUE,
    authorization_version TEXT NOT NULL,
    connected_at_unix_nanos BIGINT,
    configured_by_user_id TEXT,
    contract TEXT NOT NULL CHECK (LENGTH(contract) > 0),
    -- Offboarding retains an unusable authorization fence, never a current Account.
    retired BIGINT NOT NULL DEFAULT 0 CHECK (retired IN (0, 1)),
    -- Explicit revocation scrubs material while retaining Account references.
    encrypted_credentials TEXT NOT NULL CHECK (LENGTH(encrypted_credentials) > 0 OR requires_authorization = 1),
    user_id TEXT,
    agent_id TEXT REFERENCES workspace_agents(id)
    CHECK (agent_id IS NULL OR (workspace_id IS NOT NULL AND user_id IS NULL)),
    automatic_refresh BIGINT CHECK (automatic_refresh IS NULL OR automatic_refresh IN (0, 1)),
    granted_scope TEXT NOT NULL,
    requires_authorization BIGINT NOT NULL DEFAULT 0 CHECK (requires_authorization IN (0, 1)),
    refresh_attempt_id TEXT,
    refresh_deadline BIGINT,
    FOREIGN KEY (workspace_id, provider_id) REFERENCES provider_records(workspace_id, id),
    CHECK (LENGTH(authorization_version) = 36),
    CHECK ((connected_at_unix_nanos IS NULL AND configured_by_user_id IS NULL)
        OR (connected_at_unix_nanos IS NOT NULL AND configured_by_user_id IS NOT NULL)),
    CHECK (encrypted_credentials = '' OR connected_at_unix_nanos IS NOT NULL),
    CHECK (retired = 0 OR (encrypted_credentials = '' AND requires_authorization = 1
        AND refresh_attempt_id IS NULL AND refresh_deadline IS NULL))
);

CREATE INDEX idx_provider_accounts_scope ON provider_accounts(workspace_id, id);
CREATE UNIQUE INDEX idx_provider_accounts_shared
    ON provider_accounts(workspace_id, provider_id) WHERE user_id IS NULL AND agent_id IS NULL AND retired = 0;
CREATE UNIQUE INDEX idx_provider_accounts_user
    ON provider_accounts(workspace_id, provider_id, user_id) WHERE user_id IS NOT NULL AND retired = 0;
CREATE UNIQUE INDEX idx_provider_accounts_agent
    ON provider_accounts(workspace_id, provider_id, agent_id) WHERE agent_id IS NOT NULL AND retired = 0;

CREATE TABLE provider_account_authorization_requests (
    workspace_id TEXT NOT NULL REFERENCES workspace_records(id),
    request_id TEXT NOT NULL,
    PRIMARY KEY (workspace_id, request_id)
);

CREATE TABLE account_connection_requests (
    id TEXT NOT NULL PRIMARY KEY,
    workspace_id TEXT NOT NULL REFERENCES workspace_records(id),
    provider_account_setting_version BIGINT NOT NULL,
    provider_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    contract TEXT NOT NULL,
    contract_key TEXT NOT NULL CHECK (LENGTH(contract_key) = 43),
    account_id TEXT NOT NULL,
    account_authorization_version TEXT NOT NULL,
    resolved_account_authorization_version TEXT,
    state TEXT NOT NULL CHECK (state IN ('pending', 'connected', 'declined', 'expired', 'route_changed', 'authorization_changed')),
    expires_at_unix_nanos BIGINT NOT NULL,
    CHECK ((state = 'connected' AND resolved_account_authorization_version IS NOT NULL) OR (state <> 'connected' AND resolved_account_authorization_version IS NULL))
);

CREATE UNIQUE INDEX idx_account_connection_active ON account_connection_requests(workspace_id, provider_id, user_id, contract_key) WHERE state = 'pending';

CREATE INDEX idx_account_connection_scope ON account_connection_requests(workspace_id, user_id, id);

CREATE TABLE provider_account_authorizations (
    id TEXT NOT NULL PRIMARY KEY,
    request_id TEXT NOT NULL,
    request_content TEXT NOT NULL,
    workspace_id TEXT NOT NULL REFERENCES workspace_records(id),
    provider_id TEXT NOT NULL,
    actor_user_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('pending', 'exchanging', 'succeeded', 'failed', 'canceled', 'expired')),
    contract TEXT NOT NULL CHECK (LENGTH(contract) > 0),
    -- Historical authorization identifiers survive current Account deletion.
    account_id TEXT,
    authorization_url TEXT,
    private_flow TEXT,
    user_code TEXT,
    expires_at_unix_nanos BIGINT NOT NULL,
    expected_account_id TEXT NOT NULL,
    expected_authorization_version TEXT NOT NULL,
    provider_account_setting_version BIGINT NOT NULL,
    connection_intent TEXT NOT NULL CHECK (connection_intent IN ('initial', 'replace')),
    agent_id TEXT REFERENCES workspace_agents(id),
    user_id TEXT,
    failure_code TEXT CHECK (failure_code IN ('authorization_failed', 'access_denied', 'oauth_client_configuration', 'configuration_changed',
        'oauth_token_unavailable', 'oauth_token_timeout', 'oauth_token_response_invalid', 'oauth_code_rejected',
        'oauth_redirect_mismatch', 'oauth_scope_rejected', 'oauth_request_rejected', 'outbound_destination_rejected',
        'oauth_access_denied', 'oauth_device_expired')),
    automatic_refresh BIGINT CHECK (automatic_refresh IS NULL OR automatic_refresh IN (0, 1)),
    granted_scope TEXT,
    UNIQUE (workspace_id, request_id),
    FOREIGN KEY (workspace_id, request_id)
        REFERENCES provider_account_authorization_requests(workspace_id, request_id),
    CHECK ((state = 'succeeded' AND account_id IS NOT NULL) OR (state <> 'succeeded' AND account_id IS NULL)),
    CHECK (state IN ('pending', 'exchanging') OR private_flow IS NULL)
);

CREATE INDEX idx_provider_account_authorizations_scope ON provider_account_authorizations(workspace_id, id);

CREATE TABLE provider_lifecycle_events (
    id TEXT NOT NULL PRIMARY KEY,
    workspace_id TEXT NOT NULL REFERENCES workspace_records(id),
    actor_user_id TEXT,
    private_user_id TEXT,
    authority_source TEXT CHECK (authority_source IN ('organization_owner', 'workspace_membership')),
    action TEXT NOT NULL CHECK (action IN (
        'provider_setup.created', 'provider_setup.update_prepared', 'provider_setup.configuration_confirmed', 'provider_setup.discovery_completed', 'provider_setup.canceled',
        'provider_setup.expired', 'provider_publication.completed',

        'provider.disabled', 'provider.updated', 'provider.restored', 'provider.deleted', 'provider.oauth_client_secret_rotated',
        'provider.credential_route_changed',
        'provider_account_authorization.started', 'provider_account_authorization.exchanging',
        'provider_account_authorization.completed', 'provider_account.connected', 'provider_account.deleted', 'provider_account.disconnected',
        'account_connection.completed',
        'provider_account.reconnected', 'provider_account.refreshed', 'provider_account.reconnection_required'
    )),
    target_id TEXT NOT NULL,
    outcome TEXT NOT NULL CHECK (outcome IN ('accepted', 'succeeded', 'failed')),
    changed_field_paths TEXT NOT NULL,
    occurred_at_unix_nanos BIGINT NOT NULL,
    trace_id TEXT NOT NULL CHECK (LENGTH(trace_id) = 32),
    CHECK ((actor_user_id IS NULL AND authority_source IS NULL) OR (actor_user_id IS NOT NULL AND authority_source IS NOT NULL))
);

CREATE INDEX idx_provider_lifecycle_events_workspace_order ON provider_lifecycle_events(workspace_id, occurred_at_unix_nanos, id);

-- One durable schedule per device Authorization (or Setup credential generation).
-- Secret flow material remains in its owning authorization record.
CREATE TABLE provider_device_jobs (
    id TEXT NOT NULL PRIMARY KEY,
    workspace_id TEXT NOT NULL REFERENCES workspace_records(id),
    actor_user_id TEXT NOT NULL,
    trace_id TEXT NOT NULL,
    setup_id TEXT REFERENCES provider_setups(id),
    interval_seconds BIGINT NOT NULL CHECK (interval_seconds > 0),
    next_poll_at BIGINT NOT NULL,
    lease_owner TEXT,
    lease_token TEXT,
    lease_until BIGINT NOT NULL
);
CREATE INDEX idx_provider_device_jobs_lease ON provider_device_jobs(lease_until);
