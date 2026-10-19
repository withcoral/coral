CREATE TABLE operation_policy_lifecycle_events (
    id TEXT NOT NULL PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    actor_user_id TEXT NOT NULL,
    authority_source TEXT NOT NULL,
    action TEXT NOT NULL,
    target_provider_id TEXT NOT NULL,
    target_operation_key TEXT NOT NULL,
    outcome TEXT NOT NULL,
    changed_field_paths TEXT NOT NULL,
    occurred_at_unix_nanos BIGINT NOT NULL,
    trace_id TEXT NOT NULL,

    CONSTRAINT operation_policy_lifecycle_events_workspace_fk
        FOREIGN KEY (workspace_id)
        REFERENCES workspace_records(id),
    CONSTRAINT operation_policy_lifecycle_events_authority_source_valid
        CHECK (authority_source IN ('organization_owner', 'workspace_membership')),
    CONSTRAINT operation_policy_lifecycle_events_action_valid
        CHECK (action = 'operation_policy.access_state_updated'),
    CONSTRAINT operation_policy_lifecycle_events_target_key_not_empty
        CHECK (LENGTH(target_operation_key) > 0),
    CONSTRAINT operation_policy_lifecycle_events_outcome_valid
        CHECK (outcome = 'accepted'),
    CONSTRAINT operation_policy_lifecycle_events_trace_id_shape
        CHECK (LENGTH(trace_id) = 32)
);

CREATE INDEX idx_operation_policy_lifecycle_events_workspace_order
    ON operation_policy_lifecycle_events (
        workspace_id,
        occurred_at_unix_nanos,
        id
    );

CREATE TABLE operation_access_rules (
    workspace_id TEXT NOT NULL,
    provider_id TEXT NOT NULL,
    operation_key TEXT NOT NULL,
    access_state TEXT NOT NULL,
    created_at_unix_nanos BIGINT NOT NULL,
    updated_at_unix_nanos BIGINT NOT NULL,

    CONSTRAINT operation_access_rules_pk
        PRIMARY KEY (workspace_id, provider_id, operation_key),
    CONSTRAINT operation_access_rules_workspace_fk
        FOREIGN KEY (workspace_id)
        REFERENCES workspace_records(id),
    CONSTRAINT operation_access_rules_provider_fk
        FOREIGN KEY (workspace_id, provider_id)
        REFERENCES provider_records(workspace_id, id)
        ON DELETE CASCADE,
    CONSTRAINT operation_access_rules_operation_key_not_empty
        CHECK (LENGTH(operation_key) > 0),
    CONSTRAINT operation_access_rules_state_valid
        CHECK (access_state IN ('enabled', 'approval_required', 'disabled'))
);
