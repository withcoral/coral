CREATE TABLE program_run_records (
    id TEXT NOT NULL PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    principal_id TEXT NOT NULL,
    principal_kind TEXT NOT NULL,
    state TEXT NOT NULL,
    accepted_at_unix_nanos BIGINT NOT NULL,
    deadline_at_unix_nanos BIGINT NOT NULL,
    execution_claim_id TEXT,
    execution_started_at_unix_nanos BIGINT,
    cancellation_requested_at_unix_nanos BIGINT,
    cancellation_principal_id TEXT,
    cancellation_principal_kind TEXT,
    terminal_at_unix_nanos BIGINT,
    failure_code TEXT,
    failure_message TEXT,
    failure_error_json TEXT,
    final_value_json TEXT,
    next_event_sequence BIGINT NOT NULL,
    accumulated_output_bytes BIGINT NOT NULL,
    worker_claim_id TEXT,
    worker_claim_expires_at_unix_nanos BIGINT,
    termination_kind TEXT,
    termination_started_at_unix_nanos BIGINT,
    termination_deadline_at_unix_nanos BIGINT,
    shutdown_grace_nanos BIGINT NOT NULL,
    value_limits_json TEXT NOT NULL,

    CONSTRAINT program_run_records_workspace_run_unique
        UNIQUE (workspace_id, id),
    CONSTRAINT program_run_records_workspace_fk
        FOREIGN KEY (workspace_id)
        REFERENCES workspace_records(id)
        ON DELETE CASCADE,
    CONSTRAINT program_run_records_principal_kind_valid
        CHECK (principal_kind IN ('user', 'agent')),
    CONSTRAINT program_run_records_state_valid
        CHECK (state IN ('running', 'succeeded', 'failed', 'canceled', 'timed_out', 'interrupted')),
    CONSTRAINT program_run_records_deadline_valid
        CHECK (deadline_at_unix_nanos >= accepted_at_unix_nanos),
    CONSTRAINT program_run_records_execution_consistent
        CHECK (
            (execution_claim_id IS NULL AND execution_started_at_unix_nanos IS NULL)
            OR
            (
                execution_claim_id IS NOT NULL
                AND execution_started_at_unix_nanos IS NOT NULL
                AND execution_started_at_unix_nanos >= accepted_at_unix_nanos
            )
        ),
    CONSTRAINT program_run_records_cancellation_consistent
        CHECK (
            (
                cancellation_requested_at_unix_nanos IS NULL
                AND cancellation_principal_id IS NULL
                AND cancellation_principal_kind IS NULL
            )
            OR
            (
                cancellation_requested_at_unix_nanos IS NOT NULL
                AND cancellation_requested_at_unix_nanos >= accepted_at_unix_nanos
                AND cancellation_principal_id IS NOT NULL
                AND cancellation_principal_kind IN ('user', 'agent')
            )
        ),
    CONSTRAINT program_run_records_terminal_consistent
        CHECK (
            (
                state = 'running'
                AND terminal_at_unix_nanos IS NULL
                AND failure_code IS NULL
                AND failure_message IS NULL
                AND failure_error_json IS NULL
                AND final_value_json IS NULL
            )
            OR
            (
                state = 'succeeded'
                AND terminal_at_unix_nanos IS NOT NULL
                AND terminal_at_unix_nanos >= accepted_at_unix_nanos
                AND failure_code IS NULL
                AND failure_message IS NULL
                AND failure_error_json IS NULL
            )
            OR
            (
                state = 'failed'
                AND terminal_at_unix_nanos IS NOT NULL
                AND terminal_at_unix_nanos >= accepted_at_unix_nanos
                AND failure_code IS NOT NULL
                AND failure_message IS NOT NULL
                AND final_value_json IS NULL
            )
            OR
            (
                state = 'canceled'
                AND terminal_at_unix_nanos IS NOT NULL
                AND terminal_at_unix_nanos >= accepted_at_unix_nanos
                AND cancellation_requested_at_unix_nanos IS NOT NULL
                AND failure_code IS NULL
                AND failure_message IS NULL
                AND failure_error_json IS NULL
                AND final_value_json IS NULL
            )
            OR
            (
                state IN ('timed_out', 'interrupted')
                AND terminal_at_unix_nanos IS NOT NULL
                AND terminal_at_unix_nanos >= accepted_at_unix_nanos
                AND failure_code IS NULL
                AND failure_message IS NULL
                AND failure_error_json IS NULL
                AND final_value_json IS NULL
            )
        ),
    CONSTRAINT program_run_records_event_sequence_valid
        CHECK (next_event_sequence >= 2),
    CONSTRAINT program_run_records_output_bytes_nonnegative
        CHECK (accumulated_output_bytes >= 0)
);

CREATE INDEX idx_program_run_records_principal_visibility
    ON program_run_records (
        workspace_id,
        principal_kind,
        principal_id,
        accepted_at_unix_nanos,
        id
    );

CREATE INDEX idx_program_run_records_personal_start
    ON program_run_records (principal_kind, principal_id, accepted_at_unix_nanos, workspace_id, id);

CREATE INDEX idx_program_run_records_recovery
    ON program_run_records (
        state,
        termination_kind,
        deadline_at_unix_nanos,
        worker_claim_expires_at_unix_nanos
    );

CREATE TABLE program_run_packages (
    run_id TEXT NOT NULL PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    submitted_source TEXT NOT NULL,
    emitted_javascript TEXT NOT NULL,
    source_map TEXT NOT NULL,
    compiler_diagnostics_json TEXT NOT NULL,
    typescript_profile_id TEXT NOT NULL,
    compiler_version TEXT NOT NULL,
    javascript_target TEXT NOT NULL,
    program_source_max_bytes BIGINT NOT NULL,
    emitted_javascript_max_bytes BIGINT NOT NULL,
    source_map_max_bytes BIGINT NOT NULL,
    program_value_max_bytes BIGINT NOT NULL,
    compilation_timeout_millis BIGINT NOT NULL,

    CONSTRAINT program_run_packages_run_fk
        FOREIGN KEY (workspace_id, run_id)
        REFERENCES program_run_records(workspace_id, id)
        ON DELETE CASCADE,
    CONSTRAINT program_run_packages_limits_positive
        CHECK (
            program_source_max_bytes > 0
            AND emitted_javascript_max_bytes > 0
            AND source_map_max_bytes > 0
            AND program_value_max_bytes > 0
            AND compilation_timeout_millis > 0
        )
);

CREATE TABLE operation_invocations (
    id TEXT NOT NULL PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    provider_account_setting_version BIGINT NOT NULL,
    credential_route TEXT NOT NULL,
    provider_id TEXT NOT NULL,
    provider_generation_id TEXT NOT NULL,
    operation_key TEXT NOT NULL,
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

    CONSTRAINT operation_invocations_workspace_id_unique
        UNIQUE (workspace_id, id),
    CONSTRAINT operation_invocations_run_fk
        FOREIGN KEY (workspace_id, run_id)
        REFERENCES program_run_records(workspace_id, id),
    CONSTRAINT operation_invocations_account_setting_version_positive
        CHECK (provider_account_setting_version > 0),
    CONSTRAINT operation_invocations_route_valid
        CHECK (credential_route IN ('none', 'caller', 'fixed')),
    CONSTRAINT operation_invocations_operation_key_not_empty
        CHECK (LENGTH(operation_key) > 0),
    CONSTRAINT operation_invocations_call_path_not_empty
        CHECK (LENGTH(call_path) > 0),
    CONSTRAINT operation_invocations_arguments_not_empty
        CHECK (LENGTH(arguments_json) > 0),
    CONSTRAINT operation_invocations_state_valid
        CHECK (state IN ('pending', 'dispatching', 'ended')),
    CONSTRAINT operation_invocations_result_kind_valid
        CHECK (
            result_kind IS NULL
            OR result_kind IN ('upstream_outcome', 'coral_failure', 'run_terminated', 'indeterminate')
        ),
    CONSTRAINT operation_invocations_lifecycle_consistent
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

CREATE INDEX idx_operation_invocations_run
    ON operation_invocations (workspace_id, run_id, admitted_at_unix_nanos, id);

CREATE INDEX idx_operation_invocations_provider_history
    ON operation_invocations (workspace_id, provider_id, operation_key, admitted_at_unix_nanos, id);

CREATE INDEX idx_invocation_account_connection ON operation_invocations(workspace_id, account_connection_request_id, state);
CREATE INDEX idx_invocation_retained_connection ON operation_invocations(workspace_id, run_id, retained_account_connection_request_id);

CREATE TABLE program_run_events (
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

    PRIMARY KEY (run_id, sequence),
    CONSTRAINT program_run_events_run_fk
        FOREIGN KEY (workspace_id, run_id)
        REFERENCES program_run_records(workspace_id, id),
    CONSTRAINT program_run_events_invocation_fk
        FOREIGN KEY (workspace_id, invocation_id)
        REFERENCES operation_invocations(workspace_id, id),
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

CREATE INDEX idx_program_run_events_observation
    ON program_run_events (workspace_id, run_id, sequence);

CREATE INDEX idx_program_run_events_output
    ON program_run_events (workspace_id, run_id, sequence)
    WHERE event_type IN ('program.emitted', 'program.logged');

CREATE TABLE program_run_saved_function_snapshots (
    run_id TEXT NOT NULL,
    workspace_id TEXT NOT NULL,
    saved_function_id TEXT NOT NULL,
    path TEXT NOT NULL,
    source TEXT NOT NULL,

    PRIMARY KEY (run_id, saved_function_id),
    CONSTRAINT program_run_saved_function_snapshots_run_fk
        FOREIGN KEY (workspace_id, run_id)
        REFERENCES program_run_records(workspace_id, id)
        ON DELETE CASCADE,
    CONSTRAINT program_run_saved_function_snapshots_run_path_unique
        UNIQUE (run_id, path),
    CONSTRAINT program_run_saved_function_snapshots_path_not_empty
        CHECK (path <> ''),
    CONSTRAINT program_run_saved_function_snapshots_source_not_empty
        CHECK (source <> '')
);
