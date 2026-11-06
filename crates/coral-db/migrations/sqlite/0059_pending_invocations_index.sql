-- Finds a Workspace's pending Invocations without reading every Invocation it
-- ever ran. Without statistics, SQLite starts a Workspace's required actions
-- from tool_invocations. Pending rows are the few calls waiting now.
CREATE INDEX idx_tool_invocations_pending
    ON tool_invocations(workspace_id, run_id)
    WHERE state = 'pending';
