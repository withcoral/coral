CREATE TABLE users (
    user_id TEXT NOT NULL PRIMARY KEY,
    issuer TEXT NOT NULL,
    subject TEXT NOT NULL,
    display_name TEXT,
    created_at_unix_nanos BIGINT NOT NULL,
    last_login_at_unix_nanos BIGINT NOT NULL,

    -- v1 supports one upstream identity provider, so a subject identifies a
    -- login on its own and a reused subject can be compared against its stored
    -- issuer. Multi-provider support needs an explicit issuer-qualified
    -- migration.
    CONSTRAINT users_subject_uq UNIQUE (subject)
);

CREATE TABLE workspace_members (
    workspace_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    role TEXT NOT NULL,
    created_at_unix_nanos BIGINT NOT NULL,

    CONSTRAINT workspace_members_pk
        PRIMARY KEY (workspace_id, user_id),

    CONSTRAINT workspace_members_workspace_fk
        FOREIGN KEY (workspace_id)
        REFERENCES workspaces(id)
        ON DELETE CASCADE,

    -- Deleting a user must not silently drop the workspace memberships that
    -- carry its owner floor, so this reference restricts instead of cascading.
    CONSTRAINT workspace_members_user_fk
        FOREIGN KEY (user_id)
        REFERENCES users(user_id),

    CONSTRAINT workspace_members_role_valid
        CHECK (role IN ('owner', 'member'))
);

CREATE INDEX idx_workspace_members_user_workspaces
    ON workspace_members (user_id, workspace_id);

CREATE INDEX idx_workspace_members_workspace_role
    ON workspace_members (workspace_id, role);

-- Concealing ownerless workspaces selects every owner-bearing workspace by
-- role alone, which the (workspace_id, role) index above cannot serve because
-- it leads with the workspace.
CREATE INDEX idx_workspace_members_role_workspaces
    ON workspace_members (role, workspace_id);

-- Every login re-runs the pre-v1 creator reattribution, whose predicate is on
-- tasks.created_by_principal_id alone; 0003 indexed only the retention scan, so
-- without this each login would full-scan tasks inside its write transaction.
CREATE INDEX idx_tasks_pre_v1_creator
    ON tasks (created_by_principal_id);
