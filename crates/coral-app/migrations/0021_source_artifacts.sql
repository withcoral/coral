CREATE TABLE IF NOT EXISTS source_manifests (
    workspace_id TEXT NOT NULL,
    source_name TEXT NOT NULL,
    manifest_yaml TEXT NOT NULL,
    -- sha256 of manifest_yaml; the on-disk manifest.yaml is a per-host cache
    -- and this is what its freshness is decided against.
    manifest_hash TEXT NOT NULL,
    created_at_unix_nanos BIGINT NOT NULL,
    PRIMARY KEY (workspace_id, source_name),
    FOREIGN KEY (workspace_id, source_name)
        REFERENCES sources(workspace_id, name) ON DELETE CASCADE
);

-- Singular by construction: v4 is single-surface with a flat file layout, so
-- one row reproduces one materialized directory. No child table, no surface id.
CREATE TABLE IF NOT EXISTS materializations (
    workspace_id TEXT NOT NULL,
    source_name TEXT NOT NULL,
    materialization_version TEXT NOT NULL,
    -- Nullable because the v4 loader treats fingerprints as optional.
    fingerprint_yaml TEXT,
    projections_yaml TEXT NOT NULL,
    -- Nullable for the same reason as fingerprint_yaml.
    diagnostics_yaml TEXT,
    -- SQLite stores this as BLOB (its type names are advisory), Postgres as bytea.
    source_document_raw BYTEA NOT NULL,
    source_document_yaml TEXT NOT NULL,
    semantic_ir_yaml TEXT NOT NULL,
    operation_metadata_yaml TEXT NOT NULL DEFAULT '',
    created_at_unix_nanos BIGINT NOT NULL,
    PRIMARY KEY (workspace_id, source_name),
    FOREIGN KEY (workspace_id, source_name)
        REFERENCES sources(workspace_id, name) ON DELETE CASCADE
);
