CREATE TABLE IF NOT EXISTS trajectory_distillations (
    workspace_id TEXT NOT NULL,
    id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    strategy TEXT NOT NULL,
    normalized_intent TEXT NOT NULL,
    path_key TEXT NOT NULL,
    input_step_count BIGINT NOT NULL,
    output_step_count BIGINT NOT NULL,
    created_at_unix_nanos BIGINT NOT NULL,
    PRIMARY KEY (workspace_id, id),
    UNIQUE (workspace_id, task_id),
    FOREIGN KEY (workspace_id, task_id)
        REFERENCES tasks(workspace_id, id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS trajectory_distillations_path_idx
    ON trajectory_distillations(workspace_id, path_key);

CREATE TABLE IF NOT EXISTS trajectory_distilled_steps (
    workspace_id TEXT NOT NULL,
    id TEXT NOT NULL,
    distillation_id TEXT NOT NULL,
    source_raw_step_id TEXT NOT NULL,
    ordinal BIGINT NOT NULL,
    sql_template TEXT NOT NULL,
    relations_json TEXT NOT NULL,
    result_row_count BIGINT,
    result_column_count BIGINT,
    exact_key TEXT NOT NULL,
    created_at_unix_nanos BIGINT NOT NULL,
    PRIMARY KEY (workspace_id, id),
    UNIQUE (workspace_id, distillation_id, ordinal),
    FOREIGN KEY (workspace_id, distillation_id)
        REFERENCES trajectory_distillations(workspace_id, id) ON DELETE CASCADE,
    FOREIGN KEY (workspace_id, source_raw_step_id)
        REFERENCES trajectory_raw_steps(workspace_id, id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS trajectory_distilled_steps_distillation_idx
    ON trajectory_distilled_steps(workspace_id, distillation_id, ordinal);
