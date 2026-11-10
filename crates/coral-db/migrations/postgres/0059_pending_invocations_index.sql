-- Finds a Workspace's pending Invocations without reading every Invocation it
-- ever ran. Pending rows are the few calls waiting now. Postgres plans from its
-- statistics and reads them about as fast without this index; it keeps the same
-- index as SQLite so both databases have one schema.
--
-- SQLx runs each step in a transaction, where `CONCURRENTLY` cannot run. So the
-- build holds writes to tool_invocations until it ends. lock_timeout makes the
-- step fail instead of queueing writes behind a lock it cannot get; it does not
-- bound the build.
-- squawk-ignore-file require-concurrent-index-creation
SET LOCAL lock_timeout = '5s';

CREATE INDEX idx_tool_invocations_pending
    ON tool_invocations(workspace_id, run_id)
    WHERE state = 'pending';
