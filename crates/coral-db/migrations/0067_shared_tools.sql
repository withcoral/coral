-- Shared Tools (ADR 0033). A Workspace Owner publishes one from a Private
-- Tool or from an earlier revision, and the Workspace owns it. Each publish
-- is one immutable revision; the record points at the current one. A
-- removal tombstones the record and keeps its revisions, so a path is unique
-- only among live Shared Tools. The author is attribution only, so no row
-- here references a User or Agent: access removal never touches them.

CREATE TABLE shared_tool_records (
    id TEXT NOT NULL PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    path TEXT NOT NULL,
    current_revision BIGINT NOT NULL,
    created_by_user_id TEXT NOT NULL,
    created_at_unix_nanos BIGINT NOT NULL,
    removed_at_unix_nanos BIGINT,

    CONSTRAINT shared_tool_records_workspace_fk
        FOREIGN KEY (workspace_id)
        REFERENCES workspace_records(id)
        ON DELETE CASCADE,
    CONSTRAINT shared_tool_records_workspace_id_unique
        UNIQUE (workspace_id, id),
    CONSTRAINT shared_tool_records_path_not_empty
        CHECK (path <> ''),
    CONSTRAINT shared_tool_records_current_revision_positive
        CHECK (current_revision > 0),
    CONSTRAINT shared_tool_records_creator_not_empty
        CHECK (created_by_user_id <> '')
);

CREATE UNIQUE INDEX idx_shared_tool_records_live_path
    ON shared_tool_records (workspace_id, path)
    WHERE removed_at_unix_nanos IS NULL;

-- A revision's number and its Shared Tool are its domain identity. The
-- source Private Tool is provenance only, with no foreign key: the Private
-- Tool may change or go away. A restore names the revision it copied.
CREATE TABLE shared_tool_revisions (
    shared_tool_id TEXT NOT NULL,
    revision BIGINT NOT NULL,
    workspace_id TEXT NOT NULL,
    description TEXT NOT NULL,
    source TEXT NOT NULL,
    declaration TEXT NOT NULL,
    declaration_fields_format TEXT NOT NULL,
    declaration_fields_status TEXT NOT NULL,
    declaration_fields_json TEXT,
    declaration_fields_failure TEXT,
    diagnostics_json TEXT NOT NULL,
    diagnostics_truncated BOOLEAN NOT NULL,
    content_hash TEXT NOT NULL,
    default_access_state TEXT NOT NULL,
    published_by_user_id TEXT NOT NULL,
    source_private_tool_id TEXT,
    restored_from_revision BIGINT,
    published_at_unix_nanos BIGINT NOT NULL,

    PRIMARY KEY (shared_tool_id, revision),
    CONSTRAINT shared_tool_revisions_tool_fk
        FOREIGN KEY (workspace_id, shared_tool_id)
        REFERENCES shared_tool_records(workspace_id, id)
        ON DELETE CASCADE,
    CONSTRAINT shared_tool_revisions_revision_positive
        CHECK (revision > 0),
    CONSTRAINT shared_tool_revisions_description_not_empty
        CHECK (description <> ''),
    CONSTRAINT shared_tool_revisions_source_not_empty
        CHECK (source <> ''),
    CONSTRAINT shared_tool_revisions_declaration_not_empty
        CHECK (declaration <> ''),
    CONSTRAINT shared_tool_revisions_declaration_fields_valid
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
    CONSTRAINT shared_tool_revisions_content_hash_shape
        CHECK (LENGTH(content_hash) = 64),
    CONSTRAINT shared_tool_revisions_default_access_state_valid
        CHECK (default_access_state IN ('enabled', 'approval_required', 'disabled')),
    CONSTRAINT shared_tool_revisions_publisher_not_empty
        CHECK (published_by_user_id <> ''),
    CONSTRAINT shared_tool_revisions_one_source
        CHECK (
            (source_private_tool_id IS NOT NULL AND restored_from_revision IS NULL)
            OR (source_private_tool_id IS NULL
                AND restored_from_revision IS NOT NULL
                AND restored_from_revision > 0)
        )
);

-- The Shared Tools one revision's source calls, at the revision each was
-- current when it was published.
CREATE TABLE shared_tool_revision_dependencies (
    shared_tool_id TEXT NOT NULL,
    revision BIGINT NOT NULL,
    dependency_shared_tool_id TEXT NOT NULL,
    workspace_id TEXT NOT NULL,
    dependency_revision BIGINT NOT NULL,

    PRIMARY KEY (shared_tool_id, revision, dependency_shared_tool_id),
    CONSTRAINT shared_tool_revision_dependencies_revision_fk
        FOREIGN KEY (shared_tool_id, revision)
        REFERENCES shared_tool_revisions(shared_tool_id, revision)
        ON DELETE CASCADE,
    CONSTRAINT shared_tool_revision_dependencies_tool_fk
        FOREIGN KEY (workspace_id, shared_tool_id)
        REFERENCES shared_tool_records(workspace_id, id)
        ON DELETE CASCADE,
    CONSTRAINT shared_tool_revision_dependencies_dependency_fk
        FOREIGN KEY (workspace_id, dependency_shared_tool_id)
        REFERENCES shared_tool_records(workspace_id, id)
        ON DELETE CASCADE,
    CONSTRAINT shared_tool_revision_dependencies_not_self
        CHECK (dependency_shared_tool_id <> shared_tool_id),
    CONSTRAINT shared_tool_revision_dependencies_revision_positive
        CHECK (dependency_revision > 0)
);

CREATE INDEX idx_shared_tool_revision_dependencies_dependency
    ON shared_tool_revision_dependencies (workspace_id, dependency_shared_tool_id);

-- Publishes and removals, with the accountable User and the Workspace
-- Membership that authorized them, never source.
CREATE TABLE shared_tool_lifecycle_events (
    id TEXT NOT NULL PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    actor_user_id TEXT NOT NULL,
    authority_source TEXT NOT NULL,
    action TEXT NOT NULL,
    target_shared_tool_id TEXT NOT NULL,
    target_path TEXT NOT NULL,
    revision BIGINT NOT NULL,
    content_hash TEXT NOT NULL,
    occurred_at_unix_nanos BIGINT NOT NULL,
    trace_id TEXT NOT NULL,

    CONSTRAINT shared_tool_lifecycle_events_workspace_fk
        FOREIGN KEY (workspace_id)
        REFERENCES workspace_records(id)
        ON DELETE CASCADE,
    CONSTRAINT shared_tool_lifecycle_events_authority_source_valid
        CHECK (authority_source = 'workspace_membership'),
    CONSTRAINT shared_tool_lifecycle_events_action_valid
        CHECK (action IN ('shared_tool.published', 'shared_tool.removed')),
    CONSTRAINT shared_tool_lifecycle_events_target_path_not_empty
        CHECK (target_path <> ''),
    CONSTRAINT shared_tool_lifecycle_events_revision_positive
        CHECK (revision > 0),
    CONSTRAINT shared_tool_lifecycle_events_content_hash_shape
        CHECK (LENGTH(content_hash) = 64),
    CONSTRAINT shared_tool_lifecycle_events_trace_id_shape
        CHECK (LENGTH(trace_id) = 32 AND trace_id = LOWER(trace_id))
);

CREATE INDEX idx_shared_tool_lifecycle_events_target_order
    ON shared_tool_lifecycle_events (
        workspace_id,
        target_shared_tool_id,
        occurred_at_unix_nanos,
        id
    );

-- The Shared Tool revisions one Run linked, with the source it ran. A Run
-- keeps them after the Shared Tool is removed.
CREATE TABLE program_run_shared_tool_snapshots (
    run_id TEXT NOT NULL,
    workspace_id TEXT NOT NULL,
    shared_tool_id TEXT NOT NULL,
    path TEXT NOT NULL,
    revision BIGINT NOT NULL,
    source TEXT NOT NULL,

    PRIMARY KEY (run_id, shared_tool_id),
    CONSTRAINT program_run_shared_tool_snapshots_run_fk
        FOREIGN KEY (workspace_id, run_id)
        REFERENCES program_run_records(workspace_id, id)
        ON DELETE CASCADE,
    CONSTRAINT program_run_shared_tool_snapshots_run_path_unique
        UNIQUE (run_id, path),
    CONSTRAINT program_run_shared_tool_snapshots_path_not_empty
        CHECK (path <> ''),
    CONSTRAINT program_run_shared_tool_snapshots_revision_positive
        CHECK (revision > 0),
    CONSTRAINT program_run_shared_tool_snapshots_source_not_empty
        CHECK (source <> '')
);
