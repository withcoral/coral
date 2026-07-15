CREATE TABLE IF NOT EXISTS trajectory_raw_steps (
    workspace_id TEXT NOT NULL,
    id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    started_at_unix_nanos BIGINT NOT NULL,
    completed_at_unix_nanos BIGINT NOT NULL,
    operation TEXT NOT NULL,
    input TEXT NOT NULL,
    status TEXT NOT NULL,
    row_count BIGINT,
    output_summary_json TEXT,
    error_kind TEXT,
    error_type TEXT,
    error_message TEXT,
    PRIMARY KEY (workspace_id, id),
    FOREIGN KEY (workspace_id, task_id)
        REFERENCES tasks(workspace_id, id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS trajectory_raw_steps_task_order_idx
    ON trajectory_raw_steps(
        workspace_id,
        task_id,
        started_at_unix_nanos,
        completed_at_unix_nanos,
        id
    );
