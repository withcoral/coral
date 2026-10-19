-- Each Run freezes the Workspace's exact Operation rules at admission. Rules
-- absent here fall back to the Operation's declared hints, as at admission.
CREATE TABLE program_run_operation_access_rules (
    run_id TEXT NOT NULL,
    workspace_id TEXT NOT NULL,
    provider_id TEXT NOT NULL,
    operation_key TEXT NOT NULL,
    access_state TEXT NOT NULL,

    PRIMARY KEY (run_id, provider_id, operation_key),
    CONSTRAINT program_run_operation_access_rules_run_fk
        FOREIGN KEY (workspace_id, run_id)
        REFERENCES program_run_records(workspace_id, id)
        ON DELETE CASCADE,
    CONSTRAINT program_run_operation_access_rules_state_valid
        CHECK (access_state IN ('enabled', 'approval_required', 'disabled'))
);

-- Each Run pins its enabled Middleware chain, outermost first.
CREATE TABLE program_run_middleware_snapshots (
    run_id TEXT NOT NULL,
    workspace_id TEXT NOT NULL,
    position BIGINT NOT NULL,
    middleware_id TEXT NOT NULL,
    name TEXT NOT NULL,
    source TEXT NOT NULL,
    run_scope BOOLEAN NOT NULL,
    function_scope BOOLEAN NOT NULL,
    operation_scope BOOLEAN NOT NULL,
    revision BIGINT NOT NULL,

    PRIMARY KEY (run_id, position),
    CONSTRAINT program_run_middleware_snapshots_run_fk
        FOREIGN KEY (workspace_id, run_id)
        REFERENCES program_run_records(workspace_id, id)
        ON DELETE CASCADE,
    CONSTRAINT program_run_middleware_snapshots_position_nonnegative
        CHECK (position >= 0),
    CONSTRAINT program_run_middleware_snapshots_source_not_empty
        CHECK (source <> '')
);

-- One Middleware execution wrapping one Run, Saved Function call, or
-- Operation Invocation. Enter and exit share this one record.
CREATE TABLE middleware_executions (
    id TEXT NOT NULL PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    position BIGINT NOT NULL,
    middleware_id TEXT NOT NULL,
    middleware_name TEXT NOT NULL,
    scope TEXT NOT NULL,
    target TEXT,
    phase TEXT NOT NULL,
    status TEXT NOT NULL,
    started_at_unix_nanos BIGINT NOT NULL,
    ended_at_unix_nanos BIGINT,
    preview_json TEXT,
    failure_summary TEXT,

    CONSTRAINT middleware_executions_workspace_id_unique
        UNIQUE (workspace_id, id),
    CONSTRAINT middleware_executions_run_fk
        FOREIGN KEY (workspace_id, run_id)
        REFERENCES program_run_records(workspace_id, id),
    CONSTRAINT middleware_executions_scope_valid
        CHECK (scope IN ('run', 'function', 'operation')),
    CONSTRAINT middleware_executions_target_consistent
        CHECK ((scope = 'run' AND target IS NULL) OR (scope <> 'run' AND target IS NOT NULL)),
    CONSTRAINT middleware_executions_phase_valid
        CHECK (phase IN ('enter', 'exit')),
    CONSTRAINT middleware_executions_lifecycle_consistent
        CHECK (
            (status = 'running'
                AND ended_at_unix_nanos IS NULL
                AND preview_json IS NULL
                AND failure_summary IS NULL)
            OR
            (status = 'succeeded'
                AND ended_at_unix_nanos IS NOT NULL
                AND ended_at_unix_nanos >= started_at_unix_nanos
                AND failure_summary IS NULL)
            OR
            (status = 'failed'
                AND ended_at_unix_nanos IS NOT NULL
                AND ended_at_unix_nanos >= started_at_unix_nanos
                AND preview_json IS NULL
                AND failure_summary IS NOT NULL)
            OR
            (status = 'run_terminated'
                AND ended_at_unix_nanos IS NOT NULL
                AND preview_json IS NULL
                AND failure_summary IS NULL)
        )
);

CREATE INDEX idx_middleware_executions_run
    ON middleware_executions (workspace_id, run_id, status);

-- Every new Invocation retains the effective Operation Access State it is
-- gated by, where that state came from, and the Middleware execution that
-- made a helper call. Invocations admitted before this migration have none
-- and keep reading the live rule.
ALTER TABLE operation_invocations ADD COLUMN access_state TEXT
    CHECK (access_state IS NULL OR access_state IN ('enabled', 'approval_required', 'disabled'));
ALTER TABLE operation_invocations ADD COLUMN policy_source TEXT
    CHECK (policy_source IS NULL OR policy_source IN ('run_snapshot', 'middleware', 'middleware_helper'));
ALTER TABLE operation_invocations ADD COLUMN middleware_execution_id TEXT;

-- Rebuild the Event table so its per-kind field check admits Middleware
-- execution Events. Portable SQL cannot alter a CHECK constraint in place.
-- The rebuild keeps every earlier column and check, including the authored
-- source location of each Emission and Log.
CREATE TABLE program_run_events_rebuilt (
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
    source_saved_function_id TEXT,
    source_line BIGINT,
    source_column BIGINT,
    middleware_execution_id TEXT,
    middleware_status TEXT,

    PRIMARY KEY (run_id, sequence),
    CONSTRAINT program_run_events_run_fk
        FOREIGN KEY (workspace_id, run_id)
        REFERENCES program_run_records(workspace_id, id),
    CONSTRAINT program_run_events_invocation_fk
        FOREIGN KEY (workspace_id, invocation_id)
        REFERENCES operation_invocations(workspace_id, id),
    CONSTRAINT program_run_events_middleware_fk
        FOREIGN KEY (workspace_id, middleware_execution_id)
        REFERENCES middleware_executions(workspace_id, id),
    CONSTRAINT program_run_events_sequence_positive
        CHECK (sequence > 0),
    CONSTRAINT program_run_events_source_location_consistent
        CHECK (COALESCE(
            (source_document IS NULL AND source_saved_function_id IS NULL
                AND source_line IS NULL AND source_column IS NULL)
            OR
            (event_type IN ('program.emitted', 'program.logged')
                AND (
                    (source_document = 'program' AND source_saved_function_id IS NULL
                        AND source_line > 0 AND source_column > 0)
                    OR
                    (source_document = 'saved_function' AND LENGTH(source_saved_function_id) > 0
                        AND source_line > 0 AND source_column > 0)
                )),
            FALSE
        )),
    CONSTRAINT program_run_events_fields_consistent
        CHECK (COALESCE(
            (event_type = 'run.started'
                AND actor_principal_id IS NULL AND actor_principal_kind IS NULL
                AND terminal_state IS NULL AND invocation_id IS NULL AND result_kind IS NULL
                AND program_value_json IS NULL AND log_level IS NULL AND log_message IS NULL
                AND middleware_execution_id IS NULL AND middleware_status IS NULL)
            OR
            (event_type = 'cancellation.requested'
                AND actor_principal_id IS NOT NULL AND actor_principal_kind IN ('user', 'agent')
                AND terminal_state IS NULL AND invocation_id IS NULL AND result_kind IS NULL
                AND program_value_json IS NULL AND log_level IS NULL AND log_message IS NULL
                AND middleware_execution_id IS NULL AND middleware_status IS NULL)
            OR
            (event_type = 'run.ended'
                AND actor_principal_id IS NULL AND actor_principal_kind IS NULL
                AND terminal_state IN ('succeeded', 'failed', 'canceled', 'timed_out', 'interrupted')
                AND invocation_id IS NULL AND result_kind IS NULL
                AND program_value_json IS NULL AND log_level IS NULL AND log_message IS NULL
                AND middleware_execution_id IS NULL AND middleware_status IS NULL)
            OR
            (event_type = 'invocation.started'
                AND actor_principal_id IS NULL AND actor_principal_kind IS NULL AND terminal_state IS NULL
                AND invocation_id IS NOT NULL AND result_kind IS NULL
                AND program_value_json IS NULL AND log_level IS NULL AND log_message IS NULL
                AND middleware_execution_id IS NULL AND middleware_status IS NULL)
            OR
            (event_type = 'invocation.ended'
                AND actor_principal_id IS NULL AND actor_principal_kind IS NULL AND terminal_state IS NULL
                AND invocation_id IS NOT NULL
                AND result_kind IN ('upstream_outcome', 'coral_failure', 'run_terminated', 'indeterminate')
                AND program_value_json IS NULL AND log_level IS NULL AND log_message IS NULL
                AND middleware_execution_id IS NULL AND middleware_status IS NULL)
            OR
            (event_type = 'program.emitted'
                AND actor_principal_id IS NULL AND actor_principal_kind IS NULL
                AND terminal_state IS NULL AND invocation_id IS NULL AND result_kind IS NULL
                AND program_value_json IS NOT NULL AND log_level IS NULL AND log_message IS NULL
                AND middleware_status IS NULL)
            OR
            (event_type = 'program.logged'
                AND actor_principal_id IS NULL AND actor_principal_kind IS NULL
                AND terminal_state IS NULL AND invocation_id IS NULL AND result_kind IS NULL
                AND program_value_json IS NULL
                AND log_level IN ('debug', 'info', 'warn', 'error') AND log_message IS NOT NULL
                AND middleware_execution_id IS NULL AND middleware_status IS NULL)
            OR
            (event_type = 'middleware.started'
                AND actor_principal_id IS NULL AND actor_principal_kind IS NULL
                AND terminal_state IS NULL AND invocation_id IS NULL AND result_kind IS NULL
                AND program_value_json IS NULL AND log_level IS NULL AND log_message IS NULL
                AND middleware_execution_id IS NOT NULL AND middleware_status IS NULL)
            OR
            (event_type = 'middleware.ended'
                AND actor_principal_id IS NULL AND actor_principal_kind IS NULL
                AND terminal_state IS NULL AND invocation_id IS NULL AND result_kind IS NULL
                AND program_value_json IS NULL AND log_level IS NULL AND log_message IS NULL
                AND middleware_execution_id IS NOT NULL
                AND middleware_status IN ('succeeded', 'failed', 'run_terminated')),
            FALSE
        ))
);

INSERT INTO program_run_events_rebuilt (
    workspace_id, run_id, sequence, event_type, occurred_at_unix_nanos,
    actor_principal_id, actor_principal_kind, terminal_state, invocation_id,
    result_kind, program_value_json, log_level, log_message, output_bytes,
    source_document, source_saved_function_id, source_line, source_column
)
SELECT
    workspace_id, run_id, sequence, event_type, occurred_at_unix_nanos,
    actor_principal_id, actor_principal_kind, terminal_state, invocation_id,
    result_kind, program_value_json, log_level, log_message, output_bytes,
    source_document, source_saved_function_id, source_line, source_column
FROM program_run_events;

DROP TABLE program_run_events;

ALTER TABLE program_run_events_rebuilt RENAME TO program_run_events;

CREATE INDEX idx_program_run_events_observation
    ON program_run_events (workspace_id, run_id, sequence);

CREATE INDEX idx_program_run_events_output
    ON program_run_events (workspace_id, run_id, sequence)
    WHERE event_type IN ('program.emitted', 'program.logged');
