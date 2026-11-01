-- Universal Search is a projection, and a change to its source never waits for
-- it (ADR 0017). These foreign keys tied the two together: deleting a Saved
-- Function cascaded into the search tables inside the deleting transaction.
-- That delete waited for any index write that held the search row, and on
-- Postgres the two could deadlock. The reconcile task now removes what a
-- source no longer holds. Each table keeps its Workspace key.

-- SQLite cannot drop a constraint, so each table is rebuilt: create it without
-- the keys, copy every row, drop the old table and create its indexes and
-- triggers again. Database::migrate runs SQLite migrations with foreign keys
-- off, so dropping a table deletes no rows, and it checks every foreign key
-- afterward.
--
-- The text index stays as it is. Its key table links each copy by the row's
-- key, not by the row's rowid, so a rebuild leaves it correct. The triggers
-- would copy each row again, so the rows move before they exist on the new
-- table.

CREATE TABLE search_entities_new (
    workspace_id TEXT NOT NULL,
    doc_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    provider_id TEXT,
    provider_generation_id TEXT,
    tool_key TEXT,
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
        CHECK (kind IN ('provider_tool', 'saved_function')),
    CONSTRAINT search_entities_owner_keys_valid CHECK (
        (kind = 'provider_tool'
            AND provider_id IS NOT NULL
            AND provider_generation_id IS NOT NULL
            AND tool_key IS NOT NULL
            AND doc_id = provider_generation_id || ':' || tool_key
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
            AND tool_key IS NULL)
    ),
    CONSTRAINT search_entities_workspace_fk
        FOREIGN KEY (workspace_id)
        REFERENCES workspace_records(id)
        ON DELETE CASCADE
);

INSERT INTO search_entities_new (
    workspace_id,
    doc_id,
    kind,
    provider_id,
    provider_generation_id,
    tool_key,
    saved_function_id,
    principal_id,
    principal_kind,
    call_path,
    qualified_name,
    title,
    description,
    provider_name,
    searchable_text,
    deprecated,
    fingerprint,
    indexed_at_unix_nanos
)
SELECT
    workspace_id,
    doc_id,
    kind,
    provider_id,
    provider_generation_id,
    tool_key,
    saved_function_id,
    principal_id,
    principal_kind,
    call_path,
    qualified_name,
    title,
    description,
    provider_name,
    searchable_text,
    deprecated,
    fingerprint,
    indexed_at_unix_nanos
FROM search_entities;

DROP TRIGGER search_entities_fts_after_delete;
DROP TRIGGER search_entities_fts_after_insert;
DROP TRIGGER search_entities_fts_after_update;
DROP TABLE search_entities;
ALTER TABLE search_entities_new RENAME TO search_entities;

CREATE INDEX search_entities_generation_idx
    ON search_entities (provider_generation_id);
CREATE INDEX search_entities_principal_idx
    ON search_entities (workspace_id, kind, principal_id, principal_kind);
CREATE INDEX search_entities_provider_idx
    ON search_entities (workspace_id, kind, provider_id);

CREATE TRIGGER search_entities_fts_after_delete
AFTER DELETE ON search_entities
BEGIN
    DELETE FROM search_entities_fts
    WHERE rowid = (
        SELECT fts_rowid FROM search_entities_fts_keys
        WHERE workspace_id = OLD.workspace_id AND doc_id = OLD.doc_id
    );
    DELETE FROM search_entities_fts_keys
    WHERE workspace_id = OLD.workspace_id AND doc_id = OLD.doc_id;
END;

CREATE TRIGGER search_entities_fts_after_insert
AFTER INSERT ON search_entities
BEGIN
    INSERT INTO search_entities_fts (
        workspace_id, doc_id, qualified_name, title, description, provider_name,
        searchable_text
    )
    VALUES (
        NEW.workspace_id, NEW.doc_id, NEW.qualified_name, NEW.title, NEW.description,
        replace(NEW.provider_name, '.', ' '), NEW.searchable_text
    );
    INSERT INTO search_entities_fts_keys (workspace_id, doc_id, fts_rowid)
    VALUES (NEW.workspace_id, NEW.doc_id, last_insert_rowid());
END;

CREATE TRIGGER search_entities_fts_after_update
AFTER UPDATE ON search_entities
BEGIN
    DELETE FROM search_entities_fts
    WHERE rowid = (
        SELECT fts_rowid FROM search_entities_fts_keys
        WHERE workspace_id = OLD.workspace_id AND doc_id = OLD.doc_id
    );
    DELETE FROM search_entities_fts_keys
    WHERE workspace_id = OLD.workspace_id AND doc_id = OLD.doc_id;
    INSERT INTO search_entities_fts (
        workspace_id, doc_id, qualified_name, title, description, provider_name,
        searchable_text
    )
    VALUES (
        NEW.workspace_id, NEW.doc_id, NEW.qualified_name, NEW.title, NEW.description,
        replace(NEW.provider_name, '.', ' '), NEW.searchable_text
    );
    INSERT INTO search_entities_fts_keys (workspace_id, doc_id, fts_rowid)
    VALUES (NEW.workspace_id, NEW.doc_id, last_insert_rowid());
END;

CREATE TABLE search_index_states_new (
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
    error TEXT, error_kind TEXT, failed_attempts BIGINT, retry_after_unix_nanos BIGINT, failed_coral_version TEXT, failed_saved_function_revision BIGINT, indexer_revision BIGINT, ever_visible BOOLEAN, unavailable_since_unix_nanos BIGINT,

    CONSTRAINT search_index_states_pk PRIMARY KEY (target_id),
    CONSTRAINT search_index_states_kind_valid
        CHECK (kind IN ('provider_tool', 'saved_function')),
    CONSTRAINT search_index_states_state_valid CHECK (
        (state = 'complete' AND error IS NULL)
        OR (state = 'failed' AND error IS NOT NULL AND error <> '')
    ),
    CONSTRAINT search_index_states_target_valid CHECK (
        (kind = 'provider_tool'
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
        ON DELETE CASCADE
);

INSERT INTO search_index_states_new (
    workspace_id,
    target_id,
    kind,
    provider_id,
    provider_generation_id,
    provider_display_name,
    saved_function_id,
    principal_id,
    principal_kind,
    state,
    attempted_at_unix_nanos,
    error,
    error_kind,
    failed_attempts,
    retry_after_unix_nanos,
    failed_coral_version,
    failed_saved_function_revision,
    indexer_revision,
    ever_visible,
    unavailable_since_unix_nanos
)
SELECT
    workspace_id,
    target_id,
    kind,
    provider_id,
    provider_generation_id,
    provider_display_name,
    saved_function_id,
    principal_id,
    principal_kind,
    state,
    attempted_at_unix_nanos,
    error,
    error_kind,
    failed_attempts,
    retry_after_unix_nanos,
    failed_coral_version,
    failed_saved_function_revision,
    indexer_revision,
    ever_visible,
    unavailable_since_unix_nanos
FROM search_index_states;

DROP TABLE search_index_states;
ALTER TABLE search_index_states_new RENAME TO search_index_states;

CREATE INDEX search_index_states_provider_idx
    ON search_index_states (workspace_id, kind, provider_id);
