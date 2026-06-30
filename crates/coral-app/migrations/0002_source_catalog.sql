CREATE TABLE IF NOT EXISTS sources (
    workspace_id TEXT NOT NULL,
    name TEXT NOT NULL,
    version TEXT,
    origin_kind TEXT NOT NULL,
    credential_storage TEXT,
    created_at_unix_nanos BIGINT NOT NULL,
    updated_at_unix_nanos BIGINT NOT NULL,
    PRIMARY KEY (workspace_id, name),
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS source_variables (
    workspace_id TEXT NOT NULL,
    source_name TEXT NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    PRIMARY KEY (workspace_id, source_name, key),
    FOREIGN KEY (workspace_id, source_name) REFERENCES sources(workspace_id, name) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS source_secret_keys (
    workspace_id TEXT NOT NULL,
    source_name TEXT NOT NULL,
    position BIGINT NOT NULL,
    key TEXT NOT NULL,
    PRIMARY KEY (workspace_id, source_name, position),
    FOREIGN KEY (workspace_id, source_name) REFERENCES sources(workspace_id, name) ON DELETE CASCADE
);
