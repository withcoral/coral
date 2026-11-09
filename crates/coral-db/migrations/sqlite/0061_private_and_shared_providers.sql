-- Every Workspace has a Private Provider at `coral.private`, and will have a
-- Shared Provider at `coral.shared` (ADR 0032). Coral performs their Tools
-- itself, so each pins a Generation of input kind 'private' or 'shared' that
-- holds no materialized Tools, and installing one records the lifecycle
-- action 'provider.private_installed' or 'provider.shared_installed'.
-- SQLite cannot change a constraint, so each table is rebuilt: create it
-- with the new rules, copy every row, drop the old table, rename the new one.
-- Database::migrate runs SQLite migrations with foreign keys off, so dropping
-- a referenced table deletes no rows. It checks every foreign key afterward.
CREATE TABLE provider_generations_new (
    id TEXT NOT NULL PRIMARY KEY,
    input_kind TEXT NOT NULL,
    pinned_input TEXT NOT NULL,
    input_digest TEXT NOT NULL,
    configuration TEXT,
    credential_contract TEXT NOT NULL,
    schema_graph_json TEXT NOT NULL,
    typescript_view TEXT NOT NULL,
    materializer_version TEXT NOT NULL,
    created_at_unix_nanos BIGINT NOT NULL,

    CONSTRAINT provider_generations_input_kind_valid
        CHECK (input_kind IN ('openapi', 'mcp', 'builtin', 'private', 'shared')),
    CONSTRAINT provider_generations_pinned_input_not_empty
        CHECK (LENGTH(pinned_input) > 0),
    CONSTRAINT provider_generations_input_digest_shape
        CHECK (LENGTH(input_digest) = 64),
    CONSTRAINT provider_generations_credential_contract_valid
        CHECK (LENGTH(credential_contract) > 0),
    CONSTRAINT provider_generations_schema_graph_not_empty
        CHECK (LENGTH(schema_graph_json) > 0),
    CONSTRAINT provider_generations_typescript_view_not_empty
        CHECK (LENGTH(typescript_view) > 0),
    CONSTRAINT provider_generations_materializer_version_not_empty
        CHECK (LENGTH(materializer_version) > 0)
);

INSERT INTO provider_generations_new (
    id, input_kind, pinned_input, input_digest, configuration,
    credential_contract, schema_graph_json, typescript_view,
    materializer_version, created_at_unix_nanos
)
SELECT
    id, input_kind, pinned_input, input_digest, configuration,
    credential_contract, schema_graph_json, typescript_view,
    materializer_version, created_at_unix_nanos
FROM provider_generations;

DROP TABLE provider_generations;
ALTER TABLE provider_generations_new RENAME TO provider_generations;

CREATE TABLE provider_lifecycle_events_new (
    id TEXT NOT NULL PRIMARY KEY,
    workspace_id TEXT NOT NULL REFERENCES workspace_records(id),
    actor_user_id TEXT,
    private_user_id TEXT,
    authority_source TEXT CHECK (authority_source IN ('organization_owner', 'workspace_membership')),
    action TEXT NOT NULL CHECK (action IN (
        'provider_setup.created', 'provider_setup.update_prepared', 'provider_setup.configuration_confirmed', 'provider_setup.discovery_completed', 'provider_setup.canceled',
        'provider_setup.expired', 'provider_publication.completed',

        'provider.disabled', 'provider.updated', 'provider.restored', 'provider.deleted', 'provider.oauth_client_secret_rotated',
        'provider.builtin_installed', 'provider.private_installed', 'provider.shared_installed',
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

INSERT INTO provider_lifecycle_events_new (
    id, workspace_id, actor_user_id, private_user_id, authority_source,
    action, target_id, outcome, changed_field_paths, occurred_at_unix_nanos,
    trace_id
)
SELECT
    id, workspace_id, actor_user_id, private_user_id, authority_source,
    action, target_id, outcome, changed_field_paths, occurred_at_unix_nanos,
    trace_id
FROM provider_lifecycle_events;

DROP TABLE provider_lifecycle_events;
ALTER TABLE provider_lifecycle_events_new RENAME TO provider_lifecycle_events;

CREATE INDEX idx_provider_lifecycle_events_workspace_order ON provider_lifecycle_events(workspace_id, occurred_at_unix_nanos, id);
