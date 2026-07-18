CREATE TABLE IF NOT EXISTS identity_documents (
    owner_kind TEXT NOT NULL,
    owner_key TEXT NOT NULL,
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
    PRIMARY KEY (owner_kind, owner_key, name),
    FOREIGN KEY (owner_kind, owner_key, name)
        REFERENCES identities(owner_kind, owner_key, name)
        ON DELETE CASCADE
);
