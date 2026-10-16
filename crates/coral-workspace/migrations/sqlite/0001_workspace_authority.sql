-- Mutable profile attributes are neither identities nor grants. Emails are not unique.
CREATE TABLE user_profiles (
    user_id TEXT NOT NULL PRIMARY KEY,
    name TEXT,
    email TEXT,
    email_key TEXT,
    email_verified BIGINT CHECK (email_verified IN (0, 1)),
    hosted_domain TEXT,
    observed_at BIGINT NOT NULL
);
CREATE INDEX idx_user_profiles_email ON user_profiles (email_key);

CREATE TABLE organization_authority_records (
    user_id TEXT NOT NULL PRIMARY KEY,
    revision BIGINT NOT NULL CHECK (revision > 0),
    role TEXT NOT NULL CHECK (role IN ('owner', 'member')),
    enabled BIGINT NOT NULL DEFAULT 1 CHECK (enabled IN (0, 1)),
    -- Keep revision continuity across removal and rejoin, independent of event retention.
    removed BIGINT NOT NULL DEFAULT 0 CHECK (removed IN (0, 1))
);

-- Serialize Organization administration, including last-enabled-Owner checks.
CREATE TABLE organization_authority_guard (
    id BIGINT NOT NULL PRIMARY KEY CHECK (id = 1)
);
INSERT INTO organization_authority_guard (id) VALUES (1);

CREATE TABLE organization_lifecycle_events (
    id TEXT NOT NULL PRIMARY KEY,
    actor_user_id TEXT NOT NULL,
    target_user_id TEXT NOT NULL,
    action TEXT NOT NULL CHECK (action IN (
        'organization_membership.created', 'organization_membership.role_updated',
        'organization_membership.state_updated', 'organization_membership.removed'
    )),
    occurred_at_unix_nanos BIGINT NOT NULL,
    trace_id TEXT NOT NULL CHECK (LENGTH(trace_id) = 32)
);

CREATE TABLE workspace_records (
    id TEXT NOT NULL PRIMARY KEY,
    display_name TEXT NOT NULL,
    state TEXT NOT NULL,
    lifecycle_version BIGINT NOT NULL,
    created_at_unix_nanos BIGINT NOT NULL,
    updated_at_unix_nanos BIGINT NOT NULL,

    CONSTRAINT workspace_records_display_name_not_empty
        CHECK (LENGTH(display_name) > 0),
    CONSTRAINT workspace_records_state_valid
        CHECK (state IN ('active', 'archived')),
    CONSTRAINT workspace_records_lifecycle_version_positive
        CHECK (lifecycle_version > 0)
);

-- Current management concurrency metadata, independent of retained lifecycle Events.
-- The tuple is the identity of one resource in its Workspace.
CREATE TABLE workspace_resource_metadata (
    workspace_id TEXT NOT NULL REFERENCES workspace_records(id),
    kind TEXT NOT NULL,
    target_id TEXT NOT NULL,
    etag TEXT NOT NULL,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    PRIMARY KEY (workspace_id, kind, target_id)
);

-- Non-secret receipts prevent repeated first-party non-idempotent actions.
CREATE TABLE workspace_management_requests (
    user_id TEXT NOT NULL,
    request_id TEXT NOT NULL,
    operation_digest TEXT,
    grant_result TEXT,
    PRIMARY KEY (user_id, request_id)
);

CREATE TABLE workspace_memberships (
    workspace_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    role TEXT NOT NULL,
    created_at_unix_nanos BIGINT NOT NULL,
    updated_at_unix_nanos BIGINT NOT NULL,

    CONSTRAINT workspace_memberships_pk
        PRIMARY KEY (workspace_id, user_id),
    CONSTRAINT workspace_memberships_workspace_fk
        FOREIGN KEY (workspace_id)
        REFERENCES workspace_records(id)
        ON DELETE CASCADE,
    CONSTRAINT workspace_memberships_role_valid
        CHECK (role IN ('owner', 'member'))
);

CREATE INDEX idx_workspace_memberships_user_workspace
    ON workspace_memberships (user_id, workspace_id);

CREATE INDEX idx_workspace_memberships_workspace_role
    ON workspace_memberships (workspace_id, role, user_id);

CREATE TABLE workspace_agents (
    id TEXT NOT NULL PRIMARY KEY,
    workspace_id TEXT NOT NULL REFERENCES workspace_records(id),
    display_name TEXT NOT NULL CHECK (LENGTH(display_name) > 0),
    created_by TEXT NOT NULL,
    -- Deleted rows retain safe attribution but are excluded from current Agent reads.
    deleted BIGINT NOT NULL DEFAULT 0 CHECK (deleted IN (0, 1)),
    state TEXT NOT NULL CHECK (state IN ('enabled', 'disabled')),
    UNIQUE (workspace_id, id)
);

CREATE TABLE agent_registrations (
    id TEXT NOT NULL PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    agent_id TEXT NOT NULL UNIQUE,
    enabled BIGINT NOT NULL CHECK (enabled IN (0, 1)),
    expires_at_unix_nanos BIGINT,
    credential_verifier TEXT NOT NULL UNIQUE,
    FOREIGN KEY (workspace_id, agent_id) REFERENCES workspace_agents(workspace_id, id)
);

CREATE TABLE workspace_lifecycle_events (
    id TEXT NOT NULL PRIMARY KEY,
    workspace_id TEXT NOT NULL REFERENCES workspace_records(id),
    actor_user_id TEXT NOT NULL,
    authority_source TEXT NOT NULL CHECK (authority_source IN (
        'organization_owner', 'organization_membership', 'workspace_membership'
    )),
    action TEXT NOT NULL CHECK (action IN (
        'workspace.created', 'workspace.display_name_updated',
        'workspace.archived', 'workspace.restored',
        'workspace_membership.created', 'workspace_membership.role_updated',
        'workspace_membership.removed', 'agent.created',
        'agent_registration.created', 'agent_registration.credential_rotated',
        'agent.state_updated', 'agent_registration.updated', 'agent.deleted'
    )),
    target_kind TEXT NOT NULL CHECK (target_kind IN (
        'workspace', 'workspace_membership', 'agent', 'agent_registration'
    )),
    target_id TEXT NOT NULL,
    outcome TEXT NOT NULL CHECK (outcome = 'accepted'),
    changed_field_paths TEXT NOT NULL,
    occurred_at_unix_nanos BIGINT NOT NULL,
    trace_id TEXT NOT NULL CHECK (LENGTH(trace_id) = 32)
);

CREATE INDEX idx_workspace_lifecycle_events_workspace_order
    ON workspace_lifecycle_events (workspace_id, occurred_at_unix_nanos, id);
