-- Serves a Workspace's ended Invocations in a window, grouped by Run, Call Path
-- and outcome, without reading a row's arguments or result. Only an ended
-- Invocation holds an ended time, so the index leaves out calls still in flight.
CREATE INDEX idx_tool_invocations_ended_outcomes
    ON tool_invocations(workspace_id, ended_at_unix_nanos, run_id, call_path, result_kind, failure_code)
    WHERE ended_at_unix_nanos IS NOT NULL;
