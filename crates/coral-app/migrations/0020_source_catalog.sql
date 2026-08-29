CREATE TABLE IF NOT EXISTS sources (
    workspace_id TEXT NOT NULL,
    name TEXT NOT NULL,
    version TEXT,
    origin_kind TEXT NOT NULL,
    -- Metadata only: the credential material itself stays in the store named
    -- here (file or keychain), never in this database.
    credential_storage TEXT,
    credential_revision TEXT NOT NULL DEFAULT '00000000-0000-0000-0000-000000000000',
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
    FOREIGN KEY (workspace_id, source_name)
        REFERENCES sources(workspace_id, name) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS source_secret_keys (
    workspace_id TEXT NOT NULL,
    source_name TEXT NOT NULL,
    key TEXT NOT NULL,
    PRIMARY KEY (workspace_id, source_name, key),
    FOREIGN KEY (workspace_id, source_name)
        REFERENCES sources(workspace_id, name) ON DELETE CASCADE
);

-- Deletion record: written in the same transaction as a delete by this binary,
-- so deletions stick across hosts sharing one database (the boot import skips
-- tombstoned entries the mirror ledger proves are stale mirrors). No foreign
-- key to sources -- the source row is gone by design; the workspace cascade is
-- the only one that applies.
CREATE TABLE IF NOT EXISTS source_tombstones (
    workspace_id TEXT NOT NULL,
    source_name TEXT NOT NULL,
    deleted_at_unix_nanos BIGINT NOT NULL,
    PRIMARY KEY (workspace_id, source_name),
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE
);
