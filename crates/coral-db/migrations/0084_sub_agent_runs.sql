-- The Runs that a Sub-agent's `exec` started. A Program of such a Run cannot
-- call the Sub-agent's Tools. A row is written once, and deleted with the
-- Task that the Sub-agent works in.
CREATE TABLE sub_agent_runs (
    run_id TEXT NOT NULL PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    FOREIGN KEY (workspace_id, task_id) REFERENCES task_records(workspace_id, id) ON DELETE CASCADE
);
