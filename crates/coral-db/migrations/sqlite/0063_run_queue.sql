-- The queue of waiting Runs (ADR 0027). Acceptance writes no deadline: the
-- start records it, as the start plus the time limit. A take writes the
-- execution claim and its take time before any start mark, so a claim no
-- longer needs one. The Run Executor that holds a Run, the highest request
-- number committed and the last chunk commit's time are each recorded on the
-- row. Nullable: a Run accepted before this release has none of them.

-- SQLite cannot drop a NOT NULL or change a check, so the table is rebuilt:
-- create it, copy every row, drop the old table and create its indexes again.
-- Database::migrate runs SQLite migrations with foreign keys off, so dropping
-- the table deletes no rows, and it checks every foreign key afterward.

CREATE TABLE program_run_records_new (
    id TEXT NOT NULL PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    principal_id TEXT NOT NULL,
    principal_kind TEXT NOT NULL,
    state TEXT NOT NULL,
    accepted_at_unix_nanos BIGINT NOT NULL,
    deadline_at_unix_nanos BIGINT,
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
    chunk_sequence BIGINT NOT NULL DEFAULT 0
        CONSTRAINT program_run_records_chunk_sequence_nonnegative CHECK (chunk_sequence >= 0),
    holder_instance_id TEXT,
    trace_parent TEXT,
    taken_at_unix_nanos BIGINT,
    run_executor_id TEXT,
    run_executor_kind TEXT,
    committed_request_number BIGINT,
    last_commit_at_unix_nanos BIGINT,

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
            execution_started_at_unix_nanos IS NULL
            OR (
                execution_claim_id IS NOT NULL
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

INSERT INTO program_run_records_new (
    id, workspace_id, principal_id, principal_kind, state, accepted_at_unix_nanos,
    deadline_at_unix_nanos, execution_claim_id, execution_started_at_unix_nanos,
    cancellation_requested_at_unix_nanos, cancellation_principal_id,
    cancellation_principal_kind, terminal_at_unix_nanos, failure_code, failure_message,
    failure_error_json, final_value_json, next_event_sequence, accumulated_output_bytes,
    worker_claim_id, worker_claim_expires_at_unix_nanos, termination_kind,
    termination_started_at_unix_nanos, termination_deadline_at_unix_nanos,
    shutdown_grace_nanos, value_limits_json, chunk_sequence, holder_instance_id, trace_parent
)
SELECT
    id, workspace_id, principal_id, principal_kind, state, accepted_at_unix_nanos,
    deadline_at_unix_nanos, execution_claim_id, execution_started_at_unix_nanos,
    cancellation_requested_at_unix_nanos, cancellation_principal_id,
    cancellation_principal_kind, terminal_at_unix_nanos, failure_code, failure_message,
    failure_error_json, final_value_json, next_event_sequence, accumulated_output_bytes,
    worker_claim_id, worker_claim_expires_at_unix_nanos, termination_kind,
    termination_started_at_unix_nanos, termination_deadline_at_unix_nanos,
    shutdown_grace_nanos, value_limits_json, chunk_sequence, holder_instance_id, trace_parent
FROM program_run_records;

DROP TABLE program_run_records;
ALTER TABLE program_run_records_new RENAME TO program_run_records;

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

-- Waiting Runs, oldest first across Workspaces: running, with no claim and no
-- start mark. `state` leads the key so that SQLite picks this index for a
-- query that binds the state as a parameter.
CREATE INDEX idx_program_run_records_waiting
    ON program_run_records (state, accepted_at_unix_nanos, id)
    WHERE state = 'running'
        AND execution_claim_id IS NULL
        AND execution_started_at_unix_nanos IS NULL;
