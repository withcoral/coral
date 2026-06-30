CREATE TABLE IF NOT EXISTS source_manifests (
    workspace_id TEXT NOT NULL,
    source_name TEXT NOT NULL,
    manifest_yaml TEXT NOT NULL,
    manifest_hash TEXT NOT NULL,
    created_at_unix_nanos BIGINT NOT NULL,
    PRIMARY KEY (workspace_id, source_name),
    FOREIGN KEY (workspace_id, source_name) REFERENCES sources(workspace_id, name) ON DELETE CASCADE
);
