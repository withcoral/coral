CREATE TABLE IF NOT EXISTS tasks (
    workspace_id TEXT NOT NULL,
    id TEXT NOT NULL,
    intent TEXT NOT NULL,
    status TEXT,
    started_at_unix_nanos BIGINT NOT NULL,
    ended_at_unix_nanos BIGINT,
    PRIMARY KEY (workspace_id, id),
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE
);
