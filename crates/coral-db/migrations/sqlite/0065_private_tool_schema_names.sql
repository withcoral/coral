-- Saved Functions are Private Tools (ADR 0033). This step renames the tables,
-- columns, constraints and indexes named after Saved Functions, and the stored
-- values that a check constraint names: the Search Entity and search state
-- kind `saved_function`, the source document `saved_function` that a Run
-- recorded for a location, the lifecycle actions `saved_function.*`, and the
-- declaration fields format. Each is now `private_tool`.

-- SQLite cannot rename a constraint or change a check, so each table is
-- rebuilt: create it with the new names, copy every row, drop the old
-- table and create its indexes and triggers again. Database::migrate runs
-- SQLite migrations with foreign keys off, so dropping a table deletes no
-- rows, and it checks every foreign key afterward.

CREATE TABLE private_tool_records (
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
    default_access_state TEXT NOT NULL DEFAULT 'enabled'
        CONSTRAINT private_tool_records_default_access_state_valid
            CHECK (default_access_state IN ('enabled', 'approval_required', 'disabled')),

    CONSTRAINT private_tool_records_owner_path_unique
        UNIQUE (workspace_id, principal_id, principal_kind, path),
    CONSTRAINT private_tool_records_workspace_fk
        FOREIGN KEY (workspace_id)
        REFERENCES workspace_records(id)
        ON DELETE CASCADE,
    CONSTRAINT private_tool_records_principal_kind_valid
        CHECK (principal_kind IN ('user', 'agent')),
    CONSTRAINT private_tool_records_path_not_empty
        CHECK (path <> ''),
    CONSTRAINT private_tool_records_description_not_empty
        CHECK (description <> ''),
    CONSTRAINT private_tool_records_source_not_empty
        CHECK (source <> ''),
    CONSTRAINT private_tool_records_declaration_not_empty
        CHECK (declaration <> ''),
    CONSTRAINT private_tool_records_declaration_fields_valid
        CHECK (
            declaration_fields_format = 'private-tool-declaration-fields-v1'
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
    CONSTRAINT private_tool_records_revision_positive
        CHECK (definition_revision > 0)
);

INSERT INTO private_tool_records (
    id,
    workspace_id,
    principal_id,
    principal_kind,
    path,
    description,
    source,
    declaration,
    declaration_fields_format,
    declaration_fields_status,
    declaration_fields_json,
    declaration_fields_failure,
    diagnostics_json,
    diagnostics_truncated,
    saved_at_unix_nanos,
    definition_revision,
    default_access_state
)
SELECT
    id,
    workspace_id,
    principal_id,
    principal_kind,
    path,
    description,
    source,
    declaration,
    'private-tool-declaration-fields-v1',
    declaration_fields_status,
    declaration_fields_json,
    declaration_fields_failure,
    diagnostics_json,
    diagnostics_truncated,
    saved_at_unix_nanos,
    definition_revision,
    default_access_state
FROM saved_function_records;

DROP TABLE saved_function_records;

CREATE INDEX idx_private_tool_records_owner_path
    ON private_tool_records (
        workspace_id,
        principal_kind,
        principal_id,
        path
    );

CREATE TABLE private_tool_lifecycle_events (
    id TEXT NOT NULL PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    actor_principal_id TEXT NOT NULL,
    actor_principal_kind TEXT NOT NULL,
    private_user_id TEXT,
    action TEXT NOT NULL,
    target_private_tool_id TEXT NOT NULL,
    target_path TEXT NOT NULL,
    outcome TEXT NOT NULL,
    changed_field_paths TEXT NOT NULL,
    occurred_at_unix_nanos BIGINT NOT NULL,
    trace_id TEXT NOT NULL,

    CONSTRAINT private_tool_lifecycle_events_workspace_fk
        FOREIGN KEY (workspace_id)
        REFERENCES workspace_records(id)
        ON DELETE CASCADE,
    CONSTRAINT private_tool_lifecycle_events_actor_kind_valid
        CHECK (actor_principal_kind IN ('user', 'agent')),
    CONSTRAINT private_tool_lifecycle_events_action_valid
        CHECK (action IN (
            'private_tool.created',
            'private_tool.replaced',
            'private_tool.deleted'
        )),
    CONSTRAINT private_tool_lifecycle_events_target_path_not_empty
        CHECK (target_path <> ''),
    CONSTRAINT private_tool_lifecycle_events_outcome_valid
        CHECK (outcome = 'accepted'),
    CONSTRAINT private_tool_lifecycle_events_trace_id_shape
        CHECK (length(trace_id) = 32 AND trace_id = lower(trace_id))
);

INSERT INTO private_tool_lifecycle_events (
    id,
    workspace_id,
    actor_principal_id,
    actor_principal_kind,
    private_user_id,
    action,
    target_private_tool_id,
    target_path,
    outcome,
    changed_field_paths,
    occurred_at_unix_nanos,
    trace_id
)
SELECT
    id,
    workspace_id,
    actor_principal_id,
    actor_principal_kind,
    private_user_id,
    'private_tool.' || SUBSTR(action, 16),
    target_saved_function_id,
    target_path,
    outcome,
    changed_field_paths,
    occurred_at_unix_nanos,
    trace_id
FROM saved_function_lifecycle_events;

DROP TABLE saved_function_lifecycle_events;

CREATE INDEX idx_private_tool_lifecycle_events_owner_order
    ON private_tool_lifecycle_events (
        workspace_id,
        actor_principal_kind,
        actor_principal_id,
        occurred_at_unix_nanos,
        id
    );

CREATE TABLE program_run_private_tool_snapshots (
    run_id TEXT NOT NULL,
    workspace_id TEXT NOT NULL,
    private_tool_id TEXT NOT NULL,
    path TEXT NOT NULL,
    source TEXT NOT NULL,

    PRIMARY KEY (run_id, private_tool_id),
    CONSTRAINT program_run_private_tool_snapshots_run_fk
        FOREIGN KEY (workspace_id, run_id)
        REFERENCES program_run_records(workspace_id, id)
        ON DELETE CASCADE,
    CONSTRAINT program_run_private_tool_snapshots_run_path_unique
        UNIQUE (run_id, path),
    CONSTRAINT program_run_private_tool_snapshots_path_not_empty
        CHECK (path <> ''),
    CONSTRAINT program_run_private_tool_snapshots_source_not_empty
        CHECK (source <> '')
);

INSERT INTO program_run_private_tool_snapshots (
    run_id,
    workspace_id,
    private_tool_id,
    path,
    source
)
SELECT
    run_id,
    workspace_id,
    saved_function_id,
    path,
    source
FROM program_run_saved_function_snapshots;

DROP TABLE program_run_saved_function_snapshots;


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
    account_wait_started_at_unix_nanos BIGINT,
    source_document TEXT,
    source_private_tool_id TEXT,
    source_line BIGINT,
    source_column BIGINT,
    parent_invocation_id TEXT, declared_default_access_state TEXT
        CONSTRAINT tool_invocations_declared_default_access_state_valid
            CHECK (
                declared_default_access_state IS NULL
                OR declared_default_access_state IN ('enabled', 'approval_required', 'disabled')
            ),

    CONSTRAINT tool_invocations_source_location_consistent
    CHECK (COALESCE(
        (source_document IS NULL AND source_private_tool_id IS NULL
            AND source_line IS NULL AND source_column IS NULL)
        OR
        (source_document = 'program' AND source_private_tool_id IS NULL
            AND source_line > 0 AND source_column > 0)
        OR
        (source_document = 'private_tool' AND LENGTH(source_private_tool_id) > 0
            AND source_line > 0 AND source_column > 0),
        FALSE
    )),

    CONSTRAINT tool_invocations_workspace_id_unique
        UNIQUE (workspace_id, id),
    CONSTRAINT tool_invocations_run_id_unique
        UNIQUE (workspace_id, run_id, id),
    CONSTRAINT tool_invocations_parent_fk
        FOREIGN KEY (workspace_id, run_id, parent_invocation_id)
        REFERENCES tool_invocations(workspace_id, run_id, id),
    CONSTRAINT tool_invocations_parent_not_self
        CHECK (parent_invocation_id IS NULL OR parent_invocation_id <> id),
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
    source_private_tool_id,
    source_line,
    source_column,
    parent_invocation_id,
    declared_default_access_state
)
SELECT
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
    CASE source_document WHEN 'saved_function' THEN 'private_tool' ELSE source_document END,
    source_saved_function_id,
    source_line,
    source_column,
    parent_invocation_id,
    declared_default_access_state
FROM tool_invocations;

DROP TABLE tool_invocations;
ALTER TABLE tool_invocations_new RENAME TO tool_invocations;

CREATE INDEX idx_invocation_account_connection ON tool_invocations(workspace_id, account_connection_request_id, state);
CREATE INDEX idx_invocation_retained_connection ON tool_invocations(workspace_id, run_id, retained_account_connection_request_id);
CREATE INDEX idx_tool_invocations_provider_history
    ON tool_invocations (workspace_id, provider_id, tool_key, admitted_at_unix_nanos, id);
CREATE INDEX idx_tool_invocations_run
    ON tool_invocations (workspace_id, run_id, admitted_at_unix_nanos, id);
CREATE INDEX idx_tool_invocations_ended_outcomes
    ON tool_invocations(workspace_id, ended_at_unix_nanos, run_id, call_path, result_kind, failure_code)
    WHERE ended_at_unix_nanos IS NOT NULL;
CREATE INDEX idx_tool_invocations_pending
    ON tool_invocations(workspace_id, run_id)
    WHERE state = 'pending';
CREATE INDEX idx_tool_invocations_parent
    ON tool_invocations (workspace_id, run_id, parent_invocation_id)
    WHERE parent_invocation_id IS NOT NULL;

CREATE TABLE program_run_events_new (
    workspace_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    sequence BIGINT NOT NULL,
    event_type TEXT NOT NULL,
    occurred_at_unix_nanos BIGINT NOT NULL,
    actor_principal_id TEXT,
    actor_principal_kind TEXT,
    terminal_state TEXT,
    invocation_id TEXT,
    result_kind TEXT,
    program_value_json TEXT,
    log_level TEXT,
    log_message TEXT,
    output_bytes BIGINT NOT NULL CHECK (output_bytes >= 0),
    source_document TEXT,
    source_private_tool_id TEXT,
    source_line BIGINT,
    source_column BIGINT
    CONSTRAINT program_run_events_source_location_consistent
    CHECK (COALESCE(
        (source_document IS NULL AND source_private_tool_id IS NULL
            AND source_line IS NULL AND source_column IS NULL)
        OR
        (event_type IN ('program.emitted', 'program.logged')
            AND (
                (source_document = 'program' AND source_private_tool_id IS NULL
                    AND source_line > 0 AND source_column > 0)
                OR
                (source_document = 'private_tool' AND LENGTH(source_private_tool_id) > 0
                    AND source_line > 0 AND source_column > 0)
            )),
        FALSE
    )),

    PRIMARY KEY (run_id, sequence),
    CONSTRAINT program_run_events_run_fk
        FOREIGN KEY (workspace_id, run_id)
        REFERENCES program_run_records(workspace_id, id),
    CONSTRAINT program_run_events_invocation_fk
        FOREIGN KEY (workspace_id, invocation_id)
        REFERENCES tool_invocations(workspace_id, id),
    CONSTRAINT program_run_events_sequence_positive
        CHECK (sequence > 0),
    CONSTRAINT program_run_events_fields_consistent
        CHECK (COALESCE(
            (event_type = 'run.started'
                AND actor_principal_id IS NULL AND actor_principal_kind IS NULL
                AND terminal_state IS NULL AND invocation_id IS NULL AND result_kind IS NULL
                AND program_value_json IS NULL AND log_level IS NULL AND log_message IS NULL)
            OR
            (event_type = 'cancellation.requested'
                AND actor_principal_id IS NOT NULL AND actor_principal_kind IN ('user', 'agent')
                AND terminal_state IS NULL AND invocation_id IS NULL AND result_kind IS NULL
                AND program_value_json IS NULL AND log_level IS NULL AND log_message IS NULL)
            OR
            (event_type = 'run.ended'
                AND actor_principal_id IS NULL AND actor_principal_kind IS NULL
                AND terminal_state IN ('succeeded', 'failed', 'canceled', 'timed_out', 'interrupted')
                AND invocation_id IS NULL AND result_kind IS NULL
                AND program_value_json IS NULL AND log_level IS NULL AND log_message IS NULL)
            OR
            (event_type = 'invocation.started'
                AND actor_principal_id IS NULL AND actor_principal_kind IS NULL AND terminal_state IS NULL
                AND invocation_id IS NOT NULL AND result_kind IS NULL
                AND program_value_json IS NULL AND log_level IS NULL AND log_message IS NULL)
            OR
            (event_type = 'invocation.ended'
                AND actor_principal_id IS NULL AND actor_principal_kind IS NULL AND terminal_state IS NULL
                AND invocation_id IS NOT NULL
                AND result_kind IN ('upstream_outcome', 'coral_failure', 'run_terminated', 'indeterminate')
                AND program_value_json IS NULL AND log_level IS NULL AND log_message IS NULL)
            OR
            (event_type = 'program.emitted'
                AND actor_principal_id IS NULL AND actor_principal_kind IS NULL
                AND terminal_state IS NULL AND invocation_id IS NULL AND result_kind IS NULL
                AND program_value_json IS NOT NULL AND log_level IS NULL AND log_message IS NULL)
            OR
            (event_type = 'program.logged'
                AND actor_principal_id IS NULL AND actor_principal_kind IS NULL
                AND terminal_state IS NULL AND invocation_id IS NULL AND result_kind IS NULL
                AND program_value_json IS NULL
                AND log_level IN ('debug', 'info', 'warn', 'error') AND log_message IS NOT NULL),
            FALSE
        ))
);

INSERT INTO program_run_events_new (
    workspace_id,
    run_id,
    sequence,
    event_type,
    occurred_at_unix_nanos,
    actor_principal_id,
    actor_principal_kind,
    terminal_state,
    invocation_id,
    result_kind,
    program_value_json,
    log_level,
    log_message,
    output_bytes,
    source_document,
    source_private_tool_id,
    source_line,
    source_column
)
SELECT
    workspace_id,
    run_id,
    sequence,
    event_type,
    occurred_at_unix_nanos,
    actor_principal_id,
    actor_principal_kind,
    terminal_state,
    invocation_id,
    result_kind,
    program_value_json,
    log_level,
    log_message,
    output_bytes,
    CASE source_document WHEN 'saved_function' THEN 'private_tool' ELSE source_document END,
    source_saved_function_id,
    source_line,
    source_column
FROM program_run_events;

DROP TABLE program_run_events;
ALTER TABLE program_run_events_new RENAME TO program_run_events;

CREATE INDEX idx_program_run_events_observation
    ON program_run_events (workspace_id, run_id, sequence);
CREATE INDEX idx_program_run_events_output
    ON program_run_events (workspace_id, run_id, sequence)
    WHERE event_type IN ('program.emitted', 'program.logged');

CREATE TABLE search_entities_new (
    workspace_id TEXT NOT NULL,
    doc_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    provider_id TEXT,
    provider_generation_id TEXT,
    tool_key TEXT,
    private_tool_id TEXT,
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
        CHECK (kind IN ('provider_tool', 'private_tool')),
    CONSTRAINT search_entities_owner_keys_valid CHECK (
        (kind = 'provider_tool'
            AND provider_id IS NOT NULL
            AND provider_generation_id IS NOT NULL
            AND tool_key IS NOT NULL
            AND doc_id = provider_generation_id || ':' || tool_key
            AND private_tool_id IS NULL
            AND principal_id IS NULL
            AND principal_kind IS NULL)
        OR (kind = 'private_tool'
            AND private_tool_id IS NOT NULL
            AND doc_id = private_tool_id
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
        ON DELETE CASCADE
);

INSERT INTO search_entities_new (
    workspace_id,
    doc_id,
    kind,
    provider_id,
    provider_generation_id,
    tool_key,
    private_tool_id,
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
    CASE kind WHEN 'saved_function' THEN 'private_tool' ELSE kind END,
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
    private_tool_id TEXT,
    principal_id TEXT,
    principal_kind TEXT,
    state TEXT NOT NULL,
    attempted_at_unix_nanos BIGINT NOT NULL,
    error TEXT,
    error_kind TEXT,
    failed_attempts BIGINT,
    retry_after_unix_nanos BIGINT,
    failed_coral_version TEXT,
    failed_private_tool_revision BIGINT,
    indexer_revision BIGINT,
    ever_visible BOOLEAN,
    unavailable_since_unix_nanos BIGINT,

    CONSTRAINT search_index_states_pk PRIMARY KEY (target_id),
    CONSTRAINT search_index_states_kind_valid
        CHECK (kind IN ('provider_tool', 'private_tool')),
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
            AND private_tool_id IS NULL
            AND principal_id IS NULL
            AND principal_kind IS NULL)
        OR (kind = 'private_tool'
            AND state = 'failed'
            AND private_tool_id IS NOT NULL
            AND target_id = private_tool_id
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
        ON DELETE CASCADE
);

INSERT INTO search_index_states_new (
    workspace_id,
    target_id,
    kind,
    provider_id,
    provider_generation_id,
    provider_display_name,
    private_tool_id,
    principal_id,
    principal_kind,
    state,
    attempted_at_unix_nanos,
    error,
    error_kind,
    failed_attempts,
    retry_after_unix_nanos,
    failed_coral_version,
    failed_private_tool_revision,
    indexer_revision,
    ever_visible,
    unavailable_since_unix_nanos
)
SELECT
    workspace_id,
    target_id,
    CASE kind WHEN 'saved_function' THEN 'private_tool' ELSE kind END,
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
    indexer_revision,
    ever_visible,
    unavailable_since_unix_nanos
FROM search_index_states;

DROP TABLE search_index_states;
ALTER TABLE search_index_states_new RENAME TO search_index_states;

CREATE INDEX search_index_states_provider_idx
    ON search_index_states (workspace_id, kind, provider_id);

ALTER TABLE task_interactions RENAME COLUMN function_id TO private_tool_id;
