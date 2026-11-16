-- The queue of waiting Runs (ADR 0027). Acceptance writes no deadline: the
-- start records it, as the start plus the time limit. A take writes the
-- execution claim and its take time before any start mark, so a claim no
-- longer needs one. The Run Executor that holds a Run, the highest request
-- number committed and the last chunk commit's time are each recorded on the
-- row. Nullable: a Run accepted before this release has none of them.
--
-- The wider check holds for every existing row, so NOT VALID skips the row
-- scan that would hold an ACCESS EXCLUSIVE lock; new writes are still checked.
-- The deadline drops NOT NULL on purpose, since acceptance writes none. SQLx
-- runs each step in a transaction, where `CONCURRENTLY` cannot run.
-- lock_timeout makes the migration fail instead of queueing reads and writes
-- behind its locks.
-- squawk-ignore-file ban-drop-not-null, require-concurrent-index-creation
SET LOCAL lock_timeout = '5s';

ALTER TABLE program_run_records
    ALTER COLUMN deadline_at_unix_nanos DROP NOT NULL,
    DROP CONSTRAINT program_run_records_execution_consistent,
    ADD CONSTRAINT program_run_records_execution_consistent
        CHECK (
            execution_started_at_unix_nanos IS NULL
            OR (
                execution_claim_id IS NOT NULL
                AND execution_started_at_unix_nanos >= accepted_at_unix_nanos
            )
        ) NOT VALID,
    ADD COLUMN taken_at_unix_nanos BIGINT,
    ADD COLUMN run_executor_id TEXT,
    ADD COLUMN run_executor_kind TEXT,
    ADD COLUMN committed_request_number BIGINT,
    ADD COLUMN last_commit_at_unix_nanos BIGINT;

-- Waiting Runs, oldest first across Workspaces: running, with no claim and no
-- start mark. `state` leads the key so that SQLite picks this index for a
-- query that binds the state as a parameter.
CREATE INDEX idx_program_run_records_waiting
    ON program_run_records (state, accepted_at_unix_nanos, id)
    WHERE state = 'running'
        AND execution_claim_id IS NULL
        AND execution_started_at_unix_nanos IS NULL;
