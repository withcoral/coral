-- Identity spec references deliberately are not foreign keys: app lifecycle
-- owns replacement/orphan policy while fingerprints pin exact semantics.
CREATE TABLE IF NOT EXISTS identities (
    owner_kind TEXT NOT NULL,
    owner_key TEXT NOT NULL,
    workspace_id TEXT,
    name TEXT NOT NULL,
    identity_spec_workspace_id TEXT,
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
            AND identity_spec_workspace_id IS NULL
        )
        OR (
            owner_kind = 'workspace'
            AND (
                identity_spec_workspace_id IS NULL
                OR identity_spec_workspace_id = workspace_id
            )
        )
    ),
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS identities_workspace_id_idx
    ON identities (workspace_id);

CREATE INDEX IF NOT EXISTS identities_identity_spec_idx
    ON identities (
        identity_spec_workspace_id,
        identity_spec_name,
        identity_spec_fingerprint
    );
