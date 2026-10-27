-- Operations are Provider Tools (ADR 0024). This step renames the tables,
-- columns, constraints and indexes named after Operations, and the two stored
-- values that a check constraint names: the Search Entity kind `operation`,
-- now `provider_tool`, and the policy lifecycle action
-- `operation_policy.access_state_updated`, now `tool_policy.*`.

-- SQLite cannot rename a constraint or change a check, so each table is
-- rebuilt: create it with the new names, copy every row, drop the old
-- table and create its indexes again. Database::migrate runs SQLite
-- migrations with foreign keys off, so dropping a table deletes no rows, and
-- it checks every foreign key afterward.

CREATE TABLE materialized_tools (
    provider_generation_id TEXT NOT NULL,
    tool_key TEXT NOT NULL,
    relative_path TEXT NOT NULL,
    protocol TEXT NOT NULL,
    method TEXT,
    path TEXT,
    server_url TEXT,
    summary TEXT NOT NULL,
    title TEXT,
    description TEXT NOT NULL,
    deprecated BOOLEAN NOT NULL,
    tags_json TEXT NOT NULL,
    guidance TEXT,
    read_only_hint BOOLEAN,
    destructive_hint BOOLEAN,
    idempotent_hint BOOLEAN,
    open_world_hint BOOLEAN,
    definition_json TEXT NOT NULL,
    input_schema_json TEXT NOT NULL,
    output_schema_json TEXT NOT NULL,
    typescript_declaration TEXT NOT NULL,
    availability TEXT NOT NULL,
    diagnostic_code TEXT,
    diagnostic_message TEXT,
    input_schema_id TEXT NOT NULL,
    diagnostics_json TEXT NOT NULL,
    type_references_json TEXT NOT NULL DEFAULT '[]',

    CONSTRAINT materialized_tools_pk
        PRIMARY KEY (provider_generation_id, tool_key),
    CONSTRAINT materialized_tools_generation_fk
        FOREIGN KEY (provider_generation_id)
        REFERENCES provider_generations(id),
    CONSTRAINT materialized_tools_protocol_valid
        CHECK (protocol IN ('http', 'mcp', 'builtin')),
    CONSTRAINT materialized_tools_protocol_index_valid
        CHECK (
            (protocol = 'http'
                AND method IN ('get', 'put', 'post', 'delete', 'options', 'head', 'patch', 'trace')
                AND path IS NOT NULL
                AND LENGTH(path) > 0
                AND (availability = 'unavailable'
                    OR (server_url IS NOT NULL AND LENGTH(server_url) > 0)))
            OR
            (protocol IN ('mcp', 'builtin')
                AND method IS NULL
                AND path IS NULL
                AND server_url IS NULL)
        ),
    CONSTRAINT materialized_tools_hints_declared_by_source
        CHECK (
            protocol IN ('mcp', 'builtin')
            OR (read_only_hint IS NULL
                AND destructive_hint IS NULL
                AND idempotent_hint IS NULL
                AND open_world_hint IS NULL)
        ),
    CONSTRAINT materialized_tools_availability_valid
        CHECK (availability IN ('available', 'unavailable')),
    CONSTRAINT materialized_tools_availability_fields_consistent
        CHECK (
            (availability = 'available'
                AND diagnostic_code IS NULL
                AND diagnostic_message IS NULL)
            OR
            (availability = 'unavailable'
                AND diagnostic_code IS NOT NULL
                AND diagnostic_message IS NOT NULL)
        ),
    CONSTRAINT materialized_tools_relative_path_valid
        CHECK (LENGTH(relative_path) > 0),
    CONSTRAINT materialized_tools_summary_valid
        CHECK (LENGTH(summary) > 0),
    CONSTRAINT materialized_tools_presentation_valid
        CHECK (
            LENGTH(description) > 0
            AND LENGTH(tags_json) > 0
        ),
    CONSTRAINT materialized_tools_definition_valid
        CHECK (
            LENGTH(definition_json) > 0
            AND LENGTH(input_schema_id) > 0
            AND LENGTH(input_schema_json) > 0
            AND LENGTH(output_schema_json) > 0
            AND LENGTH(typescript_declaration) > 0
            AND LENGTH(diagnostics_json) > 0
        )
);

INSERT INTO materialized_tools (
    provider_generation_id,
    tool_key,
    relative_path,
    protocol,
    method,
    path,
    server_url,
    summary,
    title,
    description,
    deprecated,
    tags_json,
    guidance,
    read_only_hint,
    destructive_hint,
    idempotent_hint,
    open_world_hint,
    definition_json,
    input_schema_json,
    output_schema_json,
    typescript_declaration,
    availability,
    diagnostic_code,
    diagnostic_message,
    input_schema_id,
    diagnostics_json,
    type_references_json
)
SELECT
    provider_generation_id,
    operation_key,
    relative_path,
    protocol,
    method,
    path,
    server_url,
    summary,
    title,
    description,
    deprecated,
    tags_json,
    guidance,
    read_only_hint,
    destructive_hint,
    idempotent_hint,
    open_world_hint,
    definition_json,
    input_schema_json,
    output_schema_json,
    typescript_declaration,
    availability,
    diagnostic_code,
    diagnostic_message,
    input_schema_id,
    diagnostics_json,
    type_references_json
FROM materialized_operations;

DROP TABLE materialized_operations;


-- Foreign keys of other tables name this one. Renaming it first rewrites
-- them, so they name the rebuilt table.
ALTER TABLE operation_invocations RENAME TO tool_invocations;

CREATE TABLE tool_invocations_new (
    id TEXT NOT NULL PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    provider_account_setting_version BIGINT NOT NULL,
    credential_route TEXT NOT NULL,
    provider_id TEXT NOT NULL,
    provider_generation_id TEXT NOT NULL,
    tool_key TEXT NOT NULL,
    call_path TEXT NOT NULL,
    arguments_json TEXT NOT NULL,
    state TEXT NOT NULL,
    admitted_at_unix_nanos BIGINT NOT NULL,
    dispatch_started_at_unix_nanos BIGINT,
    ended_at_unix_nanos BIGINT,
    result_kind TEXT,
    result_json TEXT,
    result_bytes BIGINT NOT NULL DEFAULT 0 CHECK (result_bytes >= 0),
    failure_code TEXT,
    failure_message TEXT,
    provider_account_id TEXT,
    account_authorization_version TEXT,
    account_connection_request_id TEXT,
    retained_account_connection_request_id TEXT,
    account_wait_started_at_unix_nanos BIGINT, source_document TEXT, source_saved_function_id TEXT, source_line BIGINT, source_column BIGINT
    CONSTRAINT tool_invocations_source_location_consistent
    CHECK (COALESCE(
        (source_document IS NULL AND source_saved_function_id IS NULL
            AND source_line IS NULL AND source_column IS NULL)
        OR
        (source_document = 'program' AND source_saved_function_id IS NULL
            AND source_line > 0 AND source_column > 0)
        OR
        (source_document = 'saved_function' AND LENGTH(source_saved_function_id) > 0
            AND source_line > 0 AND source_column > 0),
        FALSE
    )),

    CONSTRAINT tool_invocations_workspace_id_unique
        UNIQUE (workspace_id, id),
    CONSTRAINT tool_invocations_run_fk
        FOREIGN KEY (workspace_id, run_id)
        REFERENCES program_run_records(workspace_id, id),
    CONSTRAINT tool_invocations_account_setting_version_positive
        CHECK (provider_account_setting_version > 0),
    CONSTRAINT tool_invocations_route_valid
        CHECK (credential_route IN ('none', 'caller', 'fixed')),
    CONSTRAINT tool_invocations_tool_key_not_empty
        CHECK (LENGTH(tool_key) > 0),
    CONSTRAINT tool_invocations_call_path_not_empty
        CHECK (LENGTH(call_path) > 0),
    CONSTRAINT tool_invocations_arguments_not_empty
        CHECK (LENGTH(arguments_json) > 0),
    CONSTRAINT tool_invocations_state_valid
        CHECK (state IN ('pending', 'dispatching', 'ended')),
    CONSTRAINT tool_invocations_result_kind_valid
        CHECK (
            result_kind IS NULL
            OR result_kind IN ('upstream_outcome', 'coral_failure', 'run_terminated', 'indeterminate')
        ),
    CONSTRAINT tool_invocations_lifecycle_consistent
        CHECK (
            (state = 'pending'
                AND dispatch_started_at_unix_nanos IS NULL
                AND ended_at_unix_nanos IS NULL
                AND result_kind IS NULL
                AND result_json IS NULL
                AND failure_code IS NULL
                AND failure_message IS NULL)
            OR
            (state = 'dispatching'
                AND dispatch_started_at_unix_nanos IS NOT NULL
                AND dispatch_started_at_unix_nanos >= admitted_at_unix_nanos
                AND ended_at_unix_nanos IS NULL
                AND result_kind IS NULL
                AND result_json IS NULL
                AND failure_code IS NULL
                AND failure_message IS NULL)
            OR
            (state = 'ended'
                AND ended_at_unix_nanos IS NOT NULL
                AND ended_at_unix_nanos >= admitted_at_unix_nanos
                AND result_kind IS NOT NULL
                AND (
                    (result_kind = 'upstream_outcome'
                        AND dispatch_started_at_unix_nanos IS NOT NULL
                        AND result_json IS NOT NULL
                        AND failure_code IS NULL
                        AND failure_message IS NULL)
                    OR
                    (result_kind = 'coral_failure'
                        AND result_json IS NULL
                        AND failure_code IS NOT NULL
                        AND failure_message IS NOT NULL)
                    OR
                    (result_kind IN ('run_terminated', 'indeterminate')
                        AND result_json IS NULL
                        AND failure_code IS NULL
                        AND failure_message IS NULL)
                ))
        )
);

INSERT INTO tool_invocations_new (
    id,
    workspace_id,
    run_id,
    provider_account_setting_version,
    credential_route,
    provider_id,
    provider_generation_id,
    tool_key,
    call_path,
    arguments_json,
    state,
    admitted_at_unix_nanos,
    dispatch_started_at_unix_nanos,
    ended_at_unix_nanos,
    result_kind,
    result_json,
    result_bytes,
    failure_code,
    failure_message,
    provider_account_id,
    account_authorization_version,
    account_connection_request_id,
    retained_account_connection_request_id,
    account_wait_started_at_unix_nanos,
    source_document,
    source_saved_function_id,
    source_line,
    source_column
)
SELECT
    id,
    workspace_id,
    run_id,
    provider_account_setting_version,
    credential_route,
    provider_id,
    provider_generation_id,
    operation_key,
    call_path,
    arguments_json,
    state,
    admitted_at_unix_nanos,
    dispatch_started_at_unix_nanos,
    ended_at_unix_nanos,
    result_kind,
    result_json,
    result_bytes,
    failure_code,
    failure_message,
    provider_account_id,
    account_authorization_version,
    account_connection_request_id,
    retained_account_connection_request_id,
    account_wait_started_at_unix_nanos,
    source_document,
    source_saved_function_id,
    source_line,
    source_column
FROM tool_invocations;

DROP TABLE tool_invocations;
ALTER TABLE tool_invocations_new RENAME TO tool_invocations;

CREATE INDEX idx_invocation_account_connection ON tool_invocations(workspace_id, account_connection_request_id, state);
CREATE INDEX idx_invocation_retained_connection ON tool_invocations(workspace_id, run_id, retained_account_connection_request_id);
CREATE INDEX idx_tool_invocations_provider_history
    ON tool_invocations (workspace_id, provider_id, tool_key, admitted_at_unix_nanos, id);
CREATE INDEX idx_tool_invocations_run
    ON tool_invocations (workspace_id, run_id, admitted_at_unix_nanos, id);

CREATE TABLE tool_access_rules (
    workspace_id TEXT NOT NULL,
    provider_id TEXT NOT NULL,
    tool_key TEXT NOT NULL,
    access_state TEXT NOT NULL,
    created_at_unix_nanos BIGINT NOT NULL,
    updated_at_unix_nanos BIGINT NOT NULL,

    CONSTRAINT tool_access_rules_pk
        PRIMARY KEY (workspace_id, provider_id, tool_key),
    CONSTRAINT tool_access_rules_workspace_fk
        FOREIGN KEY (workspace_id)
        REFERENCES workspace_records(id),
    CONSTRAINT tool_access_rules_provider_fk
        FOREIGN KEY (workspace_id, provider_id)
        REFERENCES provider_records(workspace_id, id)
        ON DELETE CASCADE,
    CONSTRAINT tool_access_rules_tool_key_not_empty
        CHECK (LENGTH(tool_key) > 0),
    CONSTRAINT tool_access_rules_state_valid
        CHECK (access_state IN ('enabled', 'approval_required', 'disabled'))
);

INSERT INTO tool_access_rules (
    workspace_id,
    provider_id,
    tool_key,
    access_state,
    created_at_unix_nanos,
    updated_at_unix_nanos
)
SELECT
    workspace_id,
    provider_id,
    operation_key,
    access_state,
    created_at_unix_nanos,
    updated_at_unix_nanos
FROM operation_access_rules;

DROP TABLE operation_access_rules;


CREATE TABLE tool_policy_lifecycle_events (
    id TEXT NOT NULL PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    actor_user_id TEXT NOT NULL,
    authority_source TEXT NOT NULL,
    action TEXT NOT NULL,
    target_provider_id TEXT NOT NULL,
    target_tool_key TEXT NOT NULL,
    outcome TEXT NOT NULL,
    changed_field_paths TEXT NOT NULL,
    occurred_at_unix_nanos BIGINT NOT NULL,
    trace_id TEXT NOT NULL,

    CONSTRAINT tool_policy_lifecycle_events_workspace_fk
        FOREIGN KEY (workspace_id)
        REFERENCES workspace_records(id),
    CONSTRAINT tool_policy_lifecycle_events_authority_source_valid
        CHECK (authority_source IN ('organization_owner', 'workspace_membership')),
    CONSTRAINT tool_policy_lifecycle_events_action_valid
        CHECK (action = 'tool_policy.access_state_updated'),
    CONSTRAINT tool_policy_lifecycle_events_target_key_not_empty
        CHECK (LENGTH(target_tool_key) > 0),
    CONSTRAINT tool_policy_lifecycle_events_outcome_valid
        CHECK (outcome = 'accepted'),
    CONSTRAINT tool_policy_lifecycle_events_trace_id_shape
        CHECK (LENGTH(trace_id) = 32)
);

INSERT INTO tool_policy_lifecycle_events (
    id,
    workspace_id,
    actor_user_id,
    authority_source,
    action,
    target_provider_id,
    target_tool_key,
    outcome,
    changed_field_paths,
    occurred_at_unix_nanos,
    trace_id
)
SELECT
    id,
    workspace_id,
    actor_user_id,
    authority_source,
    CASE action WHEN 'operation_policy.access_state_updated' THEN 'tool_policy.access_state_updated' END,
    target_provider_id,
    target_operation_key,
    outcome,
    changed_field_paths,
    occurred_at_unix_nanos,
    trace_id
FROM operation_policy_lifecycle_events;

DROP TABLE operation_policy_lifecycle_events;

CREATE INDEX idx_tool_policy_lifecycle_events_workspace_order
    ON tool_policy_lifecycle_events (
        workspace_id,
        occurred_at_unix_nanos,
        id
    );

-- The text index triggers would copy each row again, so the rows move
-- before the triggers are created on the new table.
CREATE TABLE search_entities_new (
    workspace_id TEXT NOT NULL,
    doc_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    provider_id TEXT,
    provider_generation_id TEXT,
    tool_key TEXT,
    saved_function_id TEXT,
    principal_id TEXT,
    principal_kind TEXT,
    call_path TEXT NOT NULL,
    qualified_name TEXT NOT NULL,
    title TEXT NOT NULL,
    description TEXT NOT NULL,
    provider_name TEXT NOT NULL,
    searchable_text TEXT NOT NULL,
    deprecated BOOLEAN NOT NULL,
    fingerprint TEXT NOT NULL,
    indexed_at_unix_nanos BIGINT NOT NULL,

    CONSTRAINT search_entities_pk PRIMARY KEY (workspace_id, doc_id),
    CONSTRAINT search_entities_kind_valid
        CHECK (kind IN ('provider_tool', 'saved_function')),
    CONSTRAINT search_entities_owner_keys_valid CHECK (
        (kind = 'provider_tool'
            AND provider_id IS NOT NULL
            AND provider_generation_id IS NOT NULL
            AND tool_key IS NOT NULL
            AND doc_id = provider_generation_id || ':' || tool_key
            AND saved_function_id IS NULL
            AND principal_id IS NULL
            AND principal_kind IS NULL)
        OR (kind = 'saved_function'
            AND saved_function_id IS NOT NULL
            AND doc_id = saved_function_id
            AND principal_id IS NOT NULL
            AND principal_kind IS NOT NULL
            AND principal_kind IN ('user', 'agent')
            AND provider_id IS NULL
            AND provider_generation_id IS NULL
            AND tool_key IS NULL)
    ),
    CONSTRAINT search_entities_workspace_fk
        FOREIGN KEY (workspace_id)
        REFERENCES workspace_records(id)
        ON DELETE CASCADE,
    CONSTRAINT search_entities_generation_fk
        FOREIGN KEY (provider_generation_id)
        REFERENCES provider_generations(id)
        ON DELETE CASCADE,
    CONSTRAINT search_entities_saved_function_fk
        FOREIGN KEY (saved_function_id)
        REFERENCES saved_function_records(id)
        ON DELETE CASCADE
);

INSERT INTO search_entities_new (
    workspace_id,
    doc_id,
    kind,
    provider_id,
    provider_generation_id,
    tool_key,
    saved_function_id,
    principal_id,
    principal_kind,
    call_path,
    qualified_name,
    title,
    description,
    provider_name,
    searchable_text,
    deprecated,
    fingerprint,
    indexed_at_unix_nanos
)
SELECT
    workspace_id,
    doc_id,
    CASE kind WHEN 'operation' THEN 'provider_tool' ELSE kind END,
    provider_id,
    provider_generation_id,
    operation_key,
    saved_function_id,
    principal_id,
    principal_kind,
    call_path,
    qualified_name,
    title,
    description,
    provider_name,
    searchable_text,
    deprecated,
    fingerprint,
    indexed_at_unix_nanos
FROM search_entities;

DROP TRIGGER search_entities_fts_after_delete;
DROP TRIGGER search_entities_fts_after_insert;
DROP TRIGGER search_entities_fts_after_update;
DROP TABLE search_entities;
ALTER TABLE search_entities_new RENAME TO search_entities;

CREATE INDEX search_entities_generation_idx
    ON search_entities (provider_generation_id);
CREATE INDEX search_entities_principal_idx
    ON search_entities (workspace_id, kind, principal_id, principal_kind);
CREATE INDEX search_entities_provider_idx
    ON search_entities (workspace_id, kind, provider_id);

CREATE TRIGGER search_entities_fts_after_delete
AFTER DELETE ON search_entities
BEGIN
    DELETE FROM search_entities_fts
    WHERE rowid = (
        SELECT fts_rowid FROM search_entities_fts_keys
        WHERE workspace_id = OLD.workspace_id AND doc_id = OLD.doc_id
    );
    DELETE FROM search_entities_fts_keys
    WHERE workspace_id = OLD.workspace_id AND doc_id = OLD.doc_id;
END;

CREATE TRIGGER search_entities_fts_after_insert
AFTER INSERT ON search_entities
BEGIN
    INSERT INTO search_entities_fts (
        workspace_id, doc_id, qualified_name, title, description, provider_name,
        searchable_text
    )
    VALUES (
        NEW.workspace_id, NEW.doc_id, NEW.qualified_name, NEW.title, NEW.description,
        replace(NEW.provider_name, '.', ' '), NEW.searchable_text
    );
    INSERT INTO search_entities_fts_keys (workspace_id, doc_id, fts_rowid)
    VALUES (NEW.workspace_id, NEW.doc_id, last_insert_rowid());
END;

CREATE TRIGGER search_entities_fts_after_update
AFTER UPDATE ON search_entities
BEGIN
    DELETE FROM search_entities_fts
    WHERE rowid = (
        SELECT fts_rowid FROM search_entities_fts_keys
        WHERE workspace_id = OLD.workspace_id AND doc_id = OLD.doc_id
    );
    DELETE FROM search_entities_fts_keys
    WHERE workspace_id = OLD.workspace_id AND doc_id = OLD.doc_id;
    INSERT INTO search_entities_fts (
        workspace_id, doc_id, qualified_name, title, description, provider_name,
        searchable_text
    )
    VALUES (
        NEW.workspace_id, NEW.doc_id, NEW.qualified_name, NEW.title, NEW.description,
        replace(NEW.provider_name, '.', ' '), NEW.searchable_text
    );
    INSERT INTO search_entities_fts_keys (workspace_id, doc_id, fts_rowid)
    VALUES (NEW.workspace_id, NEW.doc_id, last_insert_rowid());
END;

CREATE TABLE search_index_states_new (
    workspace_id TEXT NOT NULL,
    target_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    provider_id TEXT,
    provider_generation_id TEXT,
    provider_display_name TEXT,
    saved_function_id TEXT,
    principal_id TEXT,
    principal_kind TEXT,
    state TEXT NOT NULL,
    attempted_at_unix_nanos BIGINT NOT NULL,
    error TEXT, error_kind TEXT, failed_attempts BIGINT, retry_after_unix_nanos BIGINT, failed_coral_version TEXT, failed_saved_function_revision BIGINT, indexer_revision BIGINT,

    CONSTRAINT search_index_states_pk PRIMARY KEY (target_id),
    CONSTRAINT search_index_states_kind_valid
        CHECK (kind IN ('provider_tool', 'saved_function')),
    CONSTRAINT search_index_states_state_valid CHECK (
        (state = 'complete' AND error IS NULL)
        OR (state = 'failed' AND error IS NOT NULL AND error <> '')
    ),
    CONSTRAINT search_index_states_target_valid CHECK (
        (kind = 'provider_tool'
            AND provider_id IS NOT NULL
            AND provider_generation_id IS NOT NULL
            AND target_id = provider_generation_id
            AND provider_display_name IS NOT NULL
            AND saved_function_id IS NULL
            AND principal_id IS NULL
            AND principal_kind IS NULL)
        OR (kind = 'saved_function'
            AND state = 'failed'
            AND saved_function_id IS NOT NULL
            AND target_id = saved_function_id
            AND principal_id IS NOT NULL
            AND principal_kind IS NOT NULL
            AND principal_kind IN ('user', 'agent')
            AND provider_id IS NULL
            AND provider_generation_id IS NULL
            AND provider_display_name IS NULL)
    ),
    CONSTRAINT search_index_states_workspace_fk
        FOREIGN KEY (workspace_id)
        REFERENCES workspace_records(id)
        ON DELETE CASCADE,
    CONSTRAINT search_index_states_generation_fk
        FOREIGN KEY (provider_generation_id)
        REFERENCES provider_generations(id)
        ON DELETE CASCADE,
    CONSTRAINT search_index_states_saved_function_fk
        FOREIGN KEY (saved_function_id)
        REFERENCES saved_function_records(id)
        ON DELETE CASCADE
);

INSERT INTO search_index_states_new (
    workspace_id,
    target_id,
    kind,
    provider_id,
    provider_generation_id,
    provider_display_name,
    saved_function_id,
    principal_id,
    principal_kind,
    state,
    attempted_at_unix_nanos,
    error,
    error_kind,
    failed_attempts,
    retry_after_unix_nanos,
    failed_coral_version,
    failed_saved_function_revision,
    indexer_revision
)
SELECT
    workspace_id,
    target_id,
    CASE kind WHEN 'operation' THEN 'provider_tool' ELSE kind END,
    provider_id,
    provider_generation_id,
    provider_display_name,
    saved_function_id,
    principal_id,
    principal_kind,
    state,
    attempted_at_unix_nanos,
    error,
    error_kind,
    failed_attempts,
    retry_after_unix_nanos,
    failed_coral_version,
    failed_saved_function_revision,
    indexer_revision
FROM search_index_states;

DROP TABLE search_index_states;
ALTER TABLE search_index_states_new RENAME TO search_index_states;

CREATE INDEX search_index_states_provider_idx
    ON search_index_states (workspace_id, kind, provider_id);
