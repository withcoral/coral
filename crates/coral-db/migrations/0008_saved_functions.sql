CREATE TABLE saved_function_lifecycle_events (
    id TEXT NOT NULL PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    actor_principal_id TEXT NOT NULL,
    actor_principal_kind TEXT NOT NULL,
    private_user_id TEXT,
    action TEXT NOT NULL,
    target_saved_function_id TEXT NOT NULL,
    target_path TEXT NOT NULL,
    outcome TEXT NOT NULL,
    changed_field_paths TEXT NOT NULL,
    occurred_at_unix_nanos BIGINT NOT NULL,
    trace_id TEXT NOT NULL,

    CONSTRAINT saved_function_lifecycle_events_workspace_fk
        FOREIGN KEY (workspace_id)
        REFERENCES workspace_records(id)
        ON DELETE CASCADE,
    CONSTRAINT saved_function_lifecycle_events_actor_kind_valid
        CHECK (actor_principal_kind IN ('user', 'agent')),
    CONSTRAINT saved_function_lifecycle_events_action_valid
        CHECK (action IN (
            'saved_function.created',
            'saved_function.replaced',
            'saved_function.deleted'
        )),
    CONSTRAINT saved_function_lifecycle_events_target_path_not_empty
        CHECK (target_path <> ''),
    CONSTRAINT saved_function_lifecycle_events_outcome_valid
        CHECK (outcome = 'accepted'),
    CONSTRAINT saved_function_lifecycle_events_trace_id_shape
        CHECK (length(trace_id) = 32 AND trace_id = lower(trace_id))
);

CREATE INDEX idx_saved_function_lifecycle_events_owner_order
    ON saved_function_lifecycle_events (
        workspace_id,
        actor_principal_kind,
        actor_principal_id,
        occurred_at_unix_nanos,
        id
    );

CREATE TABLE saved_function_records (
    id TEXT NOT NULL PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    principal_id TEXT NOT NULL,
    principal_kind TEXT NOT NULL,
    path TEXT NOT NULL,
    description TEXT NOT NULL,
    source TEXT NOT NULL,
    declaration TEXT NOT NULL,
    declaration_fields_format TEXT NOT NULL,
    declaration_fields_status TEXT NOT NULL,
    declaration_fields_json TEXT,
    declaration_fields_failure TEXT,
    diagnostics_json TEXT NOT NULL,
    diagnostics_truncated BOOLEAN NOT NULL,
    saved_at_unix_nanos BIGINT NOT NULL,
    definition_revision BIGINT NOT NULL,

    CONSTRAINT saved_function_records_owner_path_unique
        UNIQUE (workspace_id, principal_id, principal_kind, path),
    CONSTRAINT saved_function_records_workspace_fk
        FOREIGN KEY (workspace_id)
        REFERENCES workspace_records(id)
        ON DELETE CASCADE,
    CONSTRAINT saved_function_records_principal_kind_valid
        CHECK (principal_kind IN ('user', 'agent')),
    CONSTRAINT saved_function_records_path_not_empty
        CHECK (path <> ''),
    CONSTRAINT saved_function_records_description_not_empty
        CHECK (description <> ''),
    CONSTRAINT saved_function_records_source_not_empty
        CHECK (source <> ''),
    CONSTRAINT saved_function_records_declaration_not_empty
        CHECK (declaration <> ''),
    CONSTRAINT saved_function_records_declaration_fields_valid
        CHECK (
            declaration_fields_format = 'saved-function-declaration-fields-v1'
            AND (
                (declaration_fields_status = 'ready'
                    AND declaration_fields_json IS NOT NULL
                    AND declaration_fields_failure IS NULL)
                OR
                (declaration_fields_status = 'failed'
                    AND declaration_fields_json IS NULL
                    AND declaration_fields_failure IS NOT NULL
                    AND declaration_fields_failure <> '')
            )
        ),
    CONSTRAINT saved_function_records_revision_positive
        CHECK (definition_revision > 0)
);

CREATE INDEX idx_saved_function_records_owner_path
    ON saved_function_records (
        workspace_id,
        principal_kind,
        principal_id,
        path
    );
