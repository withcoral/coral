CREATE TABLE IF NOT EXISTS identity_specs (
    id TEXT NOT NULL PRIMARY KEY,
    workspace_id TEXT,
    name TEXT NOT NULL,
    version TEXT NOT NULL,
    description TEXT NOT NULL,
    issuer TEXT NOT NULL,
    identity_type TEXT NOT NULL,
    manifest_yaml TEXT NOT NULL,
    created_at_unix_nanos BIGINT NOT NULL,
    updated_at_unix_nanos BIGINT NOT NULL,
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS identity_specs_global_name_uq
    ON identity_specs (name)
    WHERE workspace_id IS NULL;

CREATE UNIQUE INDEX IF NOT EXISTS identity_specs_workspace_name_uq
    ON identity_specs (workspace_id, name)
    WHERE workspace_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS identity_spec_documents (
    identity_spec_id TEXT NOT NULL PRIMARY KEY,
    document_version BIGINT NOT NULL,
    ciphertext BYTEA NOT NULL,
    nonce BYTEA NOT NULL,
    wrapped_dek BYTEA NOT NULL,
    wrapped_dek_nonce BYTEA NOT NULL,
    key_id TEXT NOT NULL,
    algorithm TEXT NOT NULL,
    binding_version BIGINT NOT NULL,
    created_at_unix_nanos BIGINT NOT NULL,
    updated_at_unix_nanos BIGINT NOT NULL,
    FOREIGN KEY (identity_spec_id) REFERENCES identity_specs(id) ON DELETE CASCADE
);
