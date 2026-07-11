CREATE TABLE IF NOT EXISTS identities (
    owner_kind TEXT NOT NULL,
    owner_key TEXT NOT NULL,
    workspace_id TEXT,
    name TEXT NOT NULL,
    identity_spec_scope_kind TEXT NOT NULL,
    identity_spec_scope_id TEXT NOT NULL,
    identity_spec_name TEXT NOT NULL,
    identity_spec_fingerprint TEXT NOT NULL,
    issuer TEXT NOT NULL,
    identity_type TEXT NOT NULL,
    created_at_unix_nanos BIGINT NOT NULL,
    updated_at_unix_nanos BIGINT NOT NULL,
    PRIMARY KEY (owner_kind, owner_key, name),
    CHECK (
        (
            owner_kind = 'user'
            AND workspace_id IS NULL
        )
        OR (
            owner_kind = 'workspace'
            AND workspace_id IS NOT NULL
            AND owner_key = workspace_id
        )
    ),
    CHECK (
        (
            owner_kind = 'user'
            AND identity_spec_scope_kind = 'global'
            AND identity_spec_scope_id = '__global__'
        )
        OR (
            owner_kind = 'workspace'
            AND (
                (
                    identity_spec_scope_kind = 'global'
                    AND identity_spec_scope_id = '__global__'
                )
                OR (
                    identity_spec_scope_kind = 'workspace'
                    AND identity_spec_scope_id = workspace_id
                )
            )
        )
    ),
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS identities_workspace_id_idx
    ON identities (workspace_id);

CREATE INDEX IF NOT EXISTS identities_identity_spec_idx
    ON identities (
        identity_spec_scope_kind,
        identity_spec_scope_id,
        identity_spec_name,
        identity_spec_fingerprint
    );

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
    FOREIGN KEY (owner_kind, owner_key, name) REFERENCES identities(owner_kind, owner_key, name) ON DELETE CASCADE
);
