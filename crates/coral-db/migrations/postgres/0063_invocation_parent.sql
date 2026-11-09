-- The performed call whose body made an Invocation, in the same Run. The
-- Program's own calls and its modules' top-level calls have none.

-- The column is new, so every existing row holds NULL and each new
-- constraint's scan finds nothing to reject. The unique constraint and the
-- index build in SQLx's transaction, where `CONCURRENTLY` cannot run.
-- lock_timeout makes the step fail instead of queueing reads and writes
-- behind its locks.
-- squawk-ignore-file constraint-missing-not-valid, disallowed-unique-constraint, adding-foreign-key-constraint, require-concurrent-index-creation
SET LOCAL lock_timeout = '5s';

ALTER TABLE tool_invocations ADD COLUMN parent_invocation_id TEXT;

ALTER TABLE tool_invocations
    ADD CONSTRAINT tool_invocations_run_id_unique
        UNIQUE (workspace_id, run_id, id);
ALTER TABLE tool_invocations
    ADD CONSTRAINT tool_invocations_parent_fk
        FOREIGN KEY (workspace_id, run_id, parent_invocation_id)
        REFERENCES tool_invocations(workspace_id, run_id, id);
ALTER TABLE tool_invocations
    ADD CONSTRAINT tool_invocations_parent_not_self
        CHECK (parent_invocation_id IS NULL OR parent_invocation_id <> id);

CREATE INDEX idx_tool_invocations_parent
    ON tool_invocations (workspace_id, run_id, parent_invocation_id)
    WHERE parent_invocation_id IS NOT NULL;
