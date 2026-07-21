CREATE TABLE IF NOT EXISTS catalog_source_owners (
    workspace TEXT NOT NULL,
    source_name TEXT NOT NULL,
    owner_source_name TEXT NOT NULL,
    snapshot_fingerprint TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    PRIMARY KEY (workspace, source_name)
);

CREATE INDEX IF NOT EXISTS idx_catalog_source_owners_workspace_owner
    ON catalog_source_owners (workspace, owner_source_name, source_name);
