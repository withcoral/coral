CREATE TABLE IF NOT EXISTS trajectory_consolidated_paths (
    workspace_id TEXT NOT NULL,
    normalized_intent TEXT NOT NULL,
    path_key TEXT NOT NULL,
    representative_distillation_id TEXT NOT NULL,
    support_count BIGINT NOT NULL,
    step_count BIGINT NOT NULL,
    updated_at_unix_nanos BIGINT NOT NULL,
    PRIMARY KEY (workspace_id, normalized_intent, path_key),
    FOREIGN KEY (workspace_id, representative_distillation_id)
        REFERENCES trajectory_distillations(workspace_id, id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS trajectory_consolidated_paths_intent_rank_idx
    ON trajectory_consolidated_paths(
        workspace_id,
        normalized_intent,
        support_count,
        step_count,
        path_key
    );

CREATE TABLE IF NOT EXISTS trajectory_exact_index (
    workspace_id TEXT NOT NULL,
    normalized_intent TEXT NOT NULL,
    path_key TEXT NOT NULL,
    support_count BIGINT NOT NULL,
    updated_at_unix_nanos BIGINT NOT NULL,
    PRIMARY KEY (workspace_id, normalized_intent),
    FOREIGN KEY (workspace_id, normalized_intent, path_key)
        REFERENCES trajectory_consolidated_paths(workspace_id, normalized_intent, path_key)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS trajectory_index_builds (
    workspace_id TEXT NOT NULL,
    id TEXT NOT NULL,
    normalized_intent TEXT NOT NULL,
    candidate_path_count BIGINT NOT NULL,
    selected_distillation_id TEXT,
    selected_path_key TEXT,
    selected_support_count BIGINT NOT NULL,
    created_at_unix_nanos BIGINT NOT NULL,
    PRIMARY KEY (workspace_id, id),
    FOREIGN KEY (workspace_id, selected_distillation_id)
        REFERENCES trajectory_distillations(workspace_id, id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS trajectory_index_builds_intent_idx
    ON trajectory_index_builds(workspace_id, normalized_intent, created_at_unix_nanos);
