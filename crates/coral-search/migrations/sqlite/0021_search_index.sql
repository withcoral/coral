-- Universal Search's own index, in the portable shared chain.
--
-- The text-search objects each backend needs on top of these tables live in
-- this crate's backend streams (`streams/`), run at boot after this chain and
-- recorded in `search_migrations`.

-- One row per Search Entity that indexed successfully. An Operation row belongs
-- to one Provider Generation; a Saved Function row to one Saved Function. Rows
-- store no visibility: retrieval asks the owning modules at query time.
CREATE TABLE search_entities (
    workspace_id TEXT NOT NULL,
    doc_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    provider_id TEXT,
    provider_generation_id TEXT,
    operation_key TEXT,
    saved_function_id TEXT,
    principal_id TEXT,
    principal_kind TEXT,
    call_path TEXT NOT NULL,
    qualified_name TEXT NOT NULL,
    title TEXT NOT NULL,
    description TEXT NOT NULL,
    provider_name TEXT NOT NULL,
    searchable_text TEXT NOT NULL,
    deprecated BOOLEAN NOT NULL,
    fingerprint TEXT NOT NULL,
    indexed_at_unix_nanos BIGINT NOT NULL,

    CONSTRAINT search_entities_pk PRIMARY KEY (workspace_id, doc_id),
    CONSTRAINT search_entities_kind_valid
        CHECK (kind IN ('operation', 'saved_function')),
    CONSTRAINT search_entities_owner_keys_valid CHECK (
        (kind = 'operation'
            AND provider_id IS NOT NULL
            AND provider_generation_id IS NOT NULL
            AND operation_key IS NOT NULL
            AND doc_id = provider_generation_id || ':' || operation_key
            AND saved_function_id IS NULL
            AND principal_id IS NULL
            AND principal_kind IS NULL)
        OR (kind = 'saved_function'
            AND saved_function_id IS NOT NULL
            AND doc_id = saved_function_id
            AND principal_id IS NOT NULL
            AND principal_kind IS NOT NULL
            AND principal_kind IN ('user', 'agent')
            AND provider_id IS NULL
            AND provider_generation_id IS NULL
            AND operation_key IS NULL)
    ),
    CONSTRAINT search_entities_workspace_fk
        FOREIGN KEY (workspace_id)
        REFERENCES workspace_records(id)
        ON DELETE CASCADE,
    CONSTRAINT search_entities_generation_fk
        FOREIGN KEY (provider_generation_id)
        REFERENCES provider_generations(id)
        ON DELETE CASCADE,
    CONSTRAINT search_entities_saved_function_fk
        FOREIGN KEY (saved_function_id)
        REFERENCES saved_function_records(id)
        ON DELETE CASCADE
);

CREATE INDEX search_entities_provider_idx
    ON search_entities (workspace_id, kind, provider_id);
CREATE INDEX search_entities_generation_idx
    ON search_entities (provider_generation_id);
CREATE INDEX search_entities_principal_idx
    ON search_entities (workspace_id, kind, principal_id, principal_kind);

-- The index state, apart from the documents. `target_id` is the id of what
-- an indexing attempt targeted: a Provider Generation, whose row is `complete`
-- or `failed` and is written in the same transaction as its Operation rows,
-- or a Saved Function, whose row exists only while its last attempt failed.
-- A Generation's row records the Provider display name the attempt indexed:
-- a rename keeps the Generation, so the reconcile task finds a stale name by
-- comparing it with the Provider's current one.
CREATE TABLE search_index_states (
    workspace_id TEXT NOT NULL,
    target_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    provider_id TEXT,
    provider_generation_id TEXT,
    provider_display_name TEXT,
    saved_function_id TEXT,
    principal_id TEXT,
    principal_kind TEXT,
    state TEXT NOT NULL,
    attempted_at_unix_nanos BIGINT NOT NULL,
    error TEXT,

    CONSTRAINT search_index_states_pk PRIMARY KEY (target_id),
    CONSTRAINT search_index_states_kind_valid
        CHECK (kind IN ('operation', 'saved_function')),
    CONSTRAINT search_index_states_state_valid CHECK (
        (state = 'complete' AND error IS NULL)
        OR (state = 'failed' AND error IS NOT NULL AND error <> '')
    ),
    CONSTRAINT search_index_states_target_valid CHECK (
        (kind = 'operation'
            AND provider_id IS NOT NULL
            AND provider_generation_id IS NOT NULL
            AND target_id = provider_generation_id
            AND provider_display_name IS NOT NULL
            AND saved_function_id IS NULL
            AND principal_id IS NULL
            AND principal_kind IS NULL)
        OR (kind = 'saved_function'
            AND state = 'failed'
            AND saved_function_id IS NOT NULL
            AND target_id = saved_function_id
            AND principal_id IS NOT NULL
            AND principal_kind IS NOT NULL
            AND principal_kind IN ('user', 'agent')
            AND provider_id IS NULL
            AND provider_generation_id IS NULL
            AND provider_display_name IS NULL)
    ),
    CONSTRAINT search_index_states_workspace_fk
        FOREIGN KEY (workspace_id)
        REFERENCES workspace_records(id)
        ON DELETE CASCADE,
    CONSTRAINT search_index_states_generation_fk
        FOREIGN KEY (provider_generation_id)
        REFERENCES provider_generations(id)
        ON DELETE CASCADE,
    CONSTRAINT search_index_states_saved_function_fk
        FOREIGN KEY (saved_function_id)
        REFERENCES saved_function_records(id)
        ON DELETE CASCADE
);

-- The ledger of the backend streams: one row per applied step.
CREATE TABLE search_migrations (
    stream TEXT NOT NULL,
    version BIGINT NOT NULL,
    applied_at_unix_nanos BIGINT NOT NULL,

    CONSTRAINT search_migrations_pk PRIMARY KEY (stream, version),
    CONSTRAINT search_migrations_stream_valid
        CHECK (stream IN ('postgres', 'sqlite')),
    CONSTRAINT search_migrations_version_positive
        CHECK (version > 0)
);
