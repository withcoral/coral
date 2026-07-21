CREATE TABLE functions (
    workspace_id TEXT NOT NULL,
    name TEXT NOT NULL,
    artifact_sql TEXT NOT NULL,
    PRIMARY KEY (workspace_id, name),
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE
);
