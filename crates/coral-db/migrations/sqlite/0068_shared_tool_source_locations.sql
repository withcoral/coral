-- A call, an emission or a log made inside a Shared Tool's body records where
-- that Shared Tool's source made it (ADR 0033). The location names the
-- Shared Tool whose revision the Run linked, in `source_shared_tool_id`, with
-- the source document `shared_tool`.
--
-- SQLite cannot change a check, so each table is rebuilt: create it with the
-- new column and rule, copy every row, drop the old table and create its
-- indexes again. Database::migrate runs SQLite migrations with foreign keys
-- off, so dropping a table deletes no rows, and it checks every foreign key
-- afterward.

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
    source_shared_tool_id TEXT,
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
        (source_document IS NULL AND source_private_tool_id IS NULL AND source_shared_tool_id IS NULL
            AND source_line IS NULL AND source_column IS NULL)
        OR
        (source_document = 'program' AND source_private_tool_id IS NULL AND source_shared_tool_id IS NULL
            AND source_line > 0 AND source_column > 0)
        OR
        (source_document = 'private_tool' AND LENGTH(source_private_tool_id) > 0 AND source_shared_tool_id IS NULL
            AND source_line > 0 AND source_column > 0)
        OR
        (source_document = 'shared_tool' AND source_private_tool_id IS NULL AND LENGTH(source_shared_tool_id) > 0
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
    source_shared_tool_id,
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
    source_document,
    source_private_tool_id,
    NULL,
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
    source_shared_tool_id TEXT,
    source_line BIGINT,
    source_column BIGINT
    CONSTRAINT program_run_events_source_location_consistent
    CHECK (COALESCE(
        (source_document IS NULL AND source_private_tool_id IS NULL AND source_shared_tool_id IS NULL
            AND source_line IS NULL AND source_column IS NULL)
        OR
        (event_type IN ('program.emitted', 'program.logged')
            AND (
                (source_document = 'program' AND source_private_tool_id IS NULL AND source_shared_tool_id IS NULL
                    AND source_line > 0 AND source_column > 0)
                OR
                (source_document = 'private_tool' AND LENGTH(source_private_tool_id) > 0 AND source_shared_tool_id IS NULL
                    AND source_line > 0 AND source_column > 0)
                OR
                (source_document = 'shared_tool' AND source_private_tool_id IS NULL AND LENGTH(source_shared_tool_id) > 0
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
    source_shared_tool_id,
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
    source_document,
    source_private_tool_id,
    NULL,
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
