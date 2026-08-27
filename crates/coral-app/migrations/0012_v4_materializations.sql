CREATE TABLE IF NOT EXISTS materializations (
    workspace_id TEXT NOT NULL,
    source_name TEXT NOT NULL,
    materialization_version TEXT NOT NULL,
    fingerprint_yaml TEXT NOT NULL,
    projections_yaml TEXT NOT NULL,
    diagnostics_yaml TEXT NOT NULL,
    created_at_unix_nanos BIGINT NOT NULL,
    PRIMARY KEY (workspace_id, source_name),
    FOREIGN KEY (workspace_id, source_name) REFERENCES sources(workspace_id, name) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS materialization_surfaces (
    workspace_id TEXT NOT NULL,
    source_name TEXT NOT NULL,
    surface_id TEXT NOT NULL,
    source_document_raw BYTEA NOT NULL,
    source_document_yaml TEXT NOT NULL,
    semantic_ir_yaml TEXT NOT NULL,
    PRIMARY KEY (workspace_id, source_name, surface_id),
    FOREIGN KEY (workspace_id, source_name) REFERENCES materializations(workspace_id, source_name) ON DELETE CASCADE
);
