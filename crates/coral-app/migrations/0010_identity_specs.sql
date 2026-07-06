CREATE TABLE IF NOT EXISTS identity_specs (
    scope_kind TEXT NOT NULL,
    scope_id TEXT NOT NULL,
    workspace_id TEXT,
    name TEXT NOT NULL,
    version TEXT NOT NULL,
    description TEXT NOT NULL,
    issuer TEXT NOT NULL,
    identity_type TEXT NOT NULL,
    manifest_yaml TEXT NOT NULL,
    created_at_unix_nanos BIGINT NOT NULL,
    updated_at_unix_nanos BIGINT NOT NULL,
    PRIMARY KEY (scope_kind, scope_id, name),
    CHECK (
        (
            scope_kind = 'global'
            AND scope_id = '__global__'
            AND workspace_id IS NULL
        )
        OR (
            scope_kind = 'workspace'
            AND workspace_id IS NOT NULL
            AND scope_id = workspace_id
        )
    ),
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS identity_spec_documents (
    scope_kind TEXT NOT NULL,
    scope_id TEXT NOT NULL,
    name TEXT NOT NULL,
    document_version BIGINT NOT NULL,
    ciphertext BYTEA NOT NULL,
    nonce BYTEA NOT NULL,
    wrapped_dek BYTEA NOT NULL,
    wrapped_dek_nonce BYTEA NOT NULL,
    key_id TEXT NOT NULL,
    algorithm TEXT NOT NULL,
    aad_version BIGINT NOT NULL,
    created_at_unix_nanos BIGINT NOT NULL,
    updated_at_unix_nanos BIGINT NOT NULL,
    PRIMARY KEY (scope_kind, scope_id, name),
    FOREIGN KEY (scope_kind, scope_id, name) REFERENCES identity_specs(scope_kind, scope_id, name) ON DELETE CASCADE
);
