CREATE TABLE IF NOT EXISTS episodes (
    workspace_id TEXT NOT NULL,
    id TEXT NOT NULL,
    intent TEXT NOT NULL,
    parent_episode_id TEXT,
    created_at_unix_nanos BIGINT NOT NULL,
    record_bytes BIGINT NOT NULL,
    PRIMARY KEY (workspace_id, id),
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE
);
