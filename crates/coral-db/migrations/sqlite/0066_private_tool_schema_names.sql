-- Saved Functions are Private Tools (ADR 0034). This step renames the tables,
-- columns, constraints and indexes named after Saved Functions, and the stored
-- values that a check constraint names: the lifecycle actions
-- `saved_function.*`, the declaration fields format, and the Search Entity
-- and search state kind `saved_function`. Each is now `private_tool`.
--
-- Universal Search indexes Shared Tools beside Private Tools, so the search
-- tables name what the two kinds share: the Search Entity and search state
-- kind `shared_tool` joins `private_tool`, and one `authored_tool_id` column
-- holds the identity of either. Only a Private Tool's row names a Principal.
-- Neither table references a Private or Shared Tool's own tables (ADR 0017).
--
-- Each Private Tool keeps its path, source, declaration, diagnostics and
-- revision, and gains the Tool Access State that applies when no exact rule
-- matches it: every existing row is `enabled`.
--
-- SQLite cannot rename a constraint or change a check, so each table is
-- rebuilt: create it with the new names, copy every row, drop the old table
-- and create its indexes and triggers again. Database::migrate runs SQLite
-- migrations with foreign keys off, so dropping a table deletes no rows, and
-- it checks every foreign key afterward.

CREATE TABLE private_tool_records (
    id TEXT NOT NULL PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    principal_id TEXT NOT NULL,
    principal_kind TEXT NOT NULL,
    path TEXT NOT NULL,
    description TEXT NOT NULL,
    source TEXT NOT NULL,
    declaration TEXT NOT NULL,
    declaration_fields_format TEXT NOT NULL,
    declaration_fields_status TEXT NOT NULL,
    declaration_fields_json TEXT,
    declaration_fields_failure TEXT,
    diagnostics_json TEXT NOT NULL,
    diagnostics_truncated BOOLEAN NOT NULL,
    saved_at_unix_nanos BIGINT NOT NULL,
    definition_revision BIGINT NOT NULL,
    default_access_state TEXT NOT NULL DEFAULT 'enabled'
        CONSTRAINT private_tool_records_default_access_state_valid
            CHECK (default_access_state IN ('enabled', 'approval_required', 'disabled')),

    CONSTRAINT private_tool_records_owner_path_unique
        UNIQUE (workspace_id, principal_id, principal_kind, path),
    CONSTRAINT private_tool_records_workspace_fk
        FOREIGN KEY (workspace_id)
        REFERENCES workspace_records(id)
        ON DELETE CASCADE,
    CONSTRAINT private_tool_records_principal_kind_valid
        CHECK (principal_kind IN ('user', 'agent')),
    CONSTRAINT private_tool_records_path_not_empty
        CHECK (path <> ''),
    CONSTRAINT private_tool_records_description_not_empty
        CHECK (description <> ''),
    CONSTRAINT private_tool_records_source_not_empty
        CHECK (source <> ''),
    CONSTRAINT private_tool_records_declaration_not_empty
        CHECK (declaration <> ''),
    CONSTRAINT private_tool_records_declaration_fields_valid
        CHECK (
            declaration_fields_format = 'private-tool-declaration-fields-v1'
            AND (
                (declaration_fields_status = 'ready'
                    AND declaration_fields_json IS NOT NULL
                    AND declaration_fields_failure IS NULL)
                OR
                (declaration_fields_status = 'failed'
                    AND declaration_fields_json IS NULL
                    AND declaration_fields_failure IS NOT NULL
                    AND declaration_fields_failure <> '')
            )
        ),
    CONSTRAINT private_tool_records_revision_positive
        CHECK (definition_revision > 0)
);

INSERT INTO private_tool_records (
    id,
    workspace_id,
    principal_id,
    principal_kind,
    path,
    description,
    source,
    declaration,
    declaration_fields_format,
    declaration_fields_status,
    declaration_fields_json,
    declaration_fields_failure,
    diagnostics_json,
    diagnostics_truncated,
    saved_at_unix_nanos,
    definition_revision,
    default_access_state
)
SELECT
    id,
    workspace_id,
    principal_id,
    principal_kind,
    path,
    description,
    source,
    declaration,
    'private-tool-declaration-fields-v1',
    declaration_fields_status,
    declaration_fields_json,
    declaration_fields_failure,
    diagnostics_json,
    diagnostics_truncated,
    saved_at_unix_nanos,
    definition_revision,
    'enabled'
FROM saved_function_records;

DROP TABLE saved_function_records;

CREATE INDEX idx_private_tool_records_owner_path
    ON private_tool_records (
        workspace_id,
        principal_kind,
        principal_id,
        path
    );

CREATE TABLE private_tool_lifecycle_events (
    id TEXT NOT NULL PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    actor_principal_id TEXT NOT NULL,
    actor_principal_kind TEXT NOT NULL,
    private_user_id TEXT,
    action TEXT NOT NULL,
    target_private_tool_id TEXT NOT NULL,
    target_path TEXT NOT NULL,
    outcome TEXT NOT NULL,
    changed_field_paths TEXT NOT NULL,
    occurred_at_unix_nanos BIGINT NOT NULL,
    trace_id TEXT NOT NULL,

    CONSTRAINT private_tool_lifecycle_events_workspace_fk
        FOREIGN KEY (workspace_id)
        REFERENCES workspace_records(id)
        ON DELETE CASCADE,
    CONSTRAINT private_tool_lifecycle_events_actor_kind_valid
        CHECK (actor_principal_kind IN ('user', 'agent')),
    CONSTRAINT private_tool_lifecycle_events_action_valid
        CHECK (action IN (
            'private_tool.created',
            'private_tool.replaced',
            'private_tool.deleted'
        )),
    CONSTRAINT private_tool_lifecycle_events_target_path_not_empty
        CHECK (target_path <> ''),
    CONSTRAINT private_tool_lifecycle_events_outcome_valid
        CHECK (outcome = 'accepted'),
    CONSTRAINT private_tool_lifecycle_events_trace_id_shape
        CHECK (length(trace_id) = 32 AND trace_id = lower(trace_id))
);

INSERT INTO private_tool_lifecycle_events (
    id,
    workspace_id,
    actor_principal_id,
    actor_principal_kind,
    private_user_id,
    action,
    target_private_tool_id,
    target_path,
    outcome,
    changed_field_paths,
    occurred_at_unix_nanos,
    trace_id
)
SELECT
    id,
    workspace_id,
    actor_principal_id,
    actor_principal_kind,
    private_user_id,
    'private_tool.' || SUBSTR(action, 16),
    target_saved_function_id,
    target_path,
    outcome,
    changed_field_paths,
    occurred_at_unix_nanos,
    trace_id
FROM saved_function_lifecycle_events;

DROP TABLE saved_function_lifecycle_events;

CREATE INDEX idx_private_tool_lifecycle_events_owner_order
    ON private_tool_lifecycle_events (
        workspace_id,
        actor_principal_kind,
        actor_principal_id,
        occurred_at_unix_nanos,
        id
    );

CREATE TABLE program_run_private_tool_snapshots (
    run_id TEXT NOT NULL,
    workspace_id TEXT NOT NULL,
    private_tool_id TEXT NOT NULL,
    path TEXT NOT NULL,
    source TEXT NOT NULL,

    PRIMARY KEY (run_id, private_tool_id),
    CONSTRAINT program_run_private_tool_snapshots_run_fk
        FOREIGN KEY (workspace_id, run_id)
        REFERENCES program_run_records(workspace_id, id)
        ON DELETE CASCADE,
    CONSTRAINT program_run_private_tool_snapshots_run_path_unique
        UNIQUE (run_id, path),
    CONSTRAINT program_run_private_tool_snapshots_path_not_empty
        CHECK (path <> ''),
    CONSTRAINT program_run_private_tool_snapshots_source_not_empty
        CHECK (source <> '')
);

INSERT INTO program_run_private_tool_snapshots (
    run_id,
    workspace_id,
    private_tool_id,
    path,
    source
)
SELECT
    run_id,
    workspace_id,
    saved_function_id,
    path,
    source
FROM program_run_saved_function_snapshots;

DROP TABLE program_run_saved_function_snapshots;


CREATE TABLE search_entities_new (
    workspace_id TEXT NOT NULL,
    doc_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    provider_id TEXT,
    provider_generation_id TEXT,
    tool_key TEXT,
    authored_tool_id TEXT,
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
        CHECK (kind IN ('provider_tool', 'private_tool', 'shared_tool')),
    CONSTRAINT search_entities_owner_keys_valid CHECK (
        (kind = 'provider_tool'
            AND provider_id IS NOT NULL
            AND provider_generation_id IS NOT NULL
            AND tool_key IS NOT NULL
            AND doc_id = provider_generation_id || ':' || tool_key
            AND authored_tool_id IS NULL
            AND principal_id IS NULL
            AND principal_kind IS NULL)
        OR (kind = 'private_tool'
            AND authored_tool_id IS NOT NULL
            AND doc_id = authored_tool_id
            AND principal_id IS NOT NULL
            AND principal_kind IS NOT NULL
            AND principal_kind IN ('user', 'agent')
            AND provider_id IS NULL
            AND provider_generation_id IS NULL
            AND tool_key IS NULL)
        OR (kind = 'shared_tool'
            AND authored_tool_id IS NOT NULL
            AND doc_id = authored_tool_id
            AND principal_id IS NULL
            AND principal_kind IS NULL
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
    authored_tool_id,
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
    CASE kind WHEN 'saved_function' THEN 'private_tool' ELSE kind END,
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
    authored_tool_id TEXT,
    principal_id TEXT,
    principal_kind TEXT,
    state TEXT NOT NULL,
    attempted_at_unix_nanos BIGINT NOT NULL,
    error TEXT,
    error_kind TEXT,
    failed_attempts BIGINT,
    retry_after_unix_nanos BIGINT,
    failed_coral_version TEXT,
    failed_authored_tool_revision BIGINT,
    indexer_revision BIGINT,
    ever_visible BOOLEAN,
    unavailable_since_unix_nanos BIGINT,

    CONSTRAINT search_index_states_pk PRIMARY KEY (target_id),
    CONSTRAINT search_index_states_kind_valid
        CHECK (kind IN ('provider_tool', 'private_tool', 'shared_tool')),
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
            AND authored_tool_id IS NULL
            AND principal_id IS NULL
            AND principal_kind IS NULL)
        OR (kind = 'private_tool'
            AND state = 'failed'
            AND authored_tool_id IS NOT NULL
            AND target_id = authored_tool_id
            AND principal_id IS NOT NULL
            AND principal_kind IS NOT NULL
            AND principal_kind IN ('user', 'agent')
            AND provider_id IS NULL
            AND provider_generation_id IS NULL
            AND provider_display_name IS NULL)
        OR (kind = 'shared_tool'
            AND state = 'failed'
            AND authored_tool_id IS NOT NULL
            AND target_id = authored_tool_id
            AND principal_id IS NULL
            AND principal_kind IS NULL
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
    authored_tool_id,
    principal_id,
    principal_kind,
    state,
    attempted_at_unix_nanos,
    error,
    error_kind,
    failed_attempts,
    retry_after_unix_nanos,
    failed_coral_version,
    failed_authored_tool_revision,
    indexer_revision,
    ever_visible,
    unavailable_since_unix_nanos
)
SELECT
    workspace_id,
    target_id,
    CASE kind WHEN 'saved_function' THEN 'private_tool' ELSE kind END,
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
