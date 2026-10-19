-- Workspace-owned Middleware. Position orders the chain outermost first; the
-- domain keeps positions dense under the Workspace hold, so no unique index
-- has to tolerate an intermediate swap.
CREATE TABLE middleware_records (
    id TEXT NOT NULL PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    name TEXT NOT NULL,
    description TEXT NOT NULL,
    source TEXT NOT NULL,
    diagnostics_json TEXT NOT NULL,
    run_scope BOOLEAN NOT NULL,
    function_scope BOOLEAN NOT NULL,
    operation_scope BOOLEAN NOT NULL,
    enabled BOOLEAN NOT NULL,
    position BIGINT NOT NULL,
    revision BIGINT NOT NULL,
    created_at_unix_nanos BIGINT NOT NULL,
    updated_at_unix_nanos BIGINT NOT NULL,

    CONSTRAINT middleware_records_workspace_id_unique
        UNIQUE (workspace_id, id),
    CONSTRAINT middleware_records_workspace_name_unique
        UNIQUE (workspace_id, name),
    CONSTRAINT middleware_records_workspace_fk
        FOREIGN KEY (workspace_id)
        REFERENCES workspace_records(id)
        ON DELETE CASCADE,
    CONSTRAINT middleware_records_name_not_empty
        CHECK (name <> ''),
    CONSTRAINT middleware_records_description_not_empty
        CHECK (description <> ''),
    CONSTRAINT middleware_records_source_not_empty
        CHECK (source <> ''),
    CONSTRAINT middleware_records_position_nonnegative
        CHECK (position >= 0),
    CONSTRAINT middleware_records_revision_positive
        CHECK (revision > 0)
);

CREATE INDEX idx_middleware_records_chain
    ON middleware_records (workspace_id, position);

CREATE TABLE middleware_lifecycle_events (
    id TEXT NOT NULL PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    actor_user_id TEXT NOT NULL,
    authority_source TEXT NOT NULL,
    action TEXT NOT NULL,
    target_middleware_id TEXT NOT NULL,
    target_name TEXT NOT NULL,
    outcome TEXT NOT NULL,
    changed_field_paths TEXT NOT NULL,
    occurred_at_unix_nanos BIGINT NOT NULL,
    trace_id TEXT NOT NULL,

    CONSTRAINT middleware_lifecycle_events_workspace_fk
        FOREIGN KEY (workspace_id)
        REFERENCES workspace_records(id)
        ON DELETE CASCADE,
    CONSTRAINT middleware_lifecycle_events_authority_source_valid
        CHECK (authority_source IN ('organization_owner', 'workspace_membership')),
    CONSTRAINT middleware_lifecycle_events_action_valid
        CHECK (action IN (
            'middleware.created',
            'middleware.updated',
            'middleware.enabled',
            'middleware.disabled',
            'middleware.scopes_updated',
            'middleware.moved',
            'middleware.deleted'
        )),
    CONSTRAINT middleware_lifecycle_events_target_name_not_empty
        CHECK (target_name <> ''),
    CONSTRAINT middleware_lifecycle_events_outcome_valid
        CHECK (outcome = 'accepted'),
    CONSTRAINT middleware_lifecycle_events_trace_id_shape
        CHECK (length(trace_id) = 32 AND trace_id = lower(trace_id))
);

CREATE INDEX idx_middleware_lifecycle_events_workspace_order
    ON middleware_lifecycle_events (workspace_id, occurred_at_unix_nanos, id);
