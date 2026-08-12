CREATE TABLE IF NOT EXISTS feedback_reports (
    id TEXT NOT NULL,
    workspace_id TEXT NOT NULL,
    created_at_unix_nanos BIGINT NOT NULL,
    trying_to_do TEXT NOT NULL,
    tried TEXT NOT NULL,
    stuck TEXT NOT NULL,
    publish_status TEXT,
    publish_error TEXT,
    published_at_unix_nanos BIGINT,
    PRIMARY KEY (workspace_id, id),
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE
);
