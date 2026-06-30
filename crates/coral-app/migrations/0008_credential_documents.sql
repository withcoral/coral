CREATE TABLE IF NOT EXISTS credential_documents (
    workspace_id TEXT NOT NULL,
    source_name TEXT NOT NULL,
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
    PRIMARY KEY (workspace_id, source_name),
    FOREIGN KEY (workspace_id, source_name) REFERENCES sources(workspace_id, name) ON DELETE CASCADE
);
