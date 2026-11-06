-- An Organization Owner grants a Secret Store to a Workspace (ADR 0032). A grant
-- names the store: deployment config defines stores, so a grant can outlive one.
-- Each grant and revocation records an Administrative Lifecycle Event.
-- SQLite cannot change a constraint, so the Events table is rebuilt: create it
-- with the new rules, copy every row, drop the old table, rename the new one.
-- Database::migrate runs SQLite migrations with foreign keys off and checks every
-- foreign key afterward.
CREATE TABLE workspace_secret_store_grants (
    workspace_id TEXT NOT NULL REFERENCES workspace_records(id),
    store_name TEXT NOT NULL CHECK (LENGTH(store_name) BETWEEN 1 AND 64),
    PRIMARY KEY (workspace_id, store_name)
);

CREATE TABLE workspace_lifecycle_events_new (
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
        'agent.state_updated', 'agent_registration.updated', 'agent.deleted',
        'secret_store.granted', 'secret_store.revoked'
    )),
    target_kind TEXT NOT NULL CHECK (target_kind IN (
        'workspace', 'workspace_membership', 'agent', 'agent_registration', 'secret_store'
    )),
    target_id TEXT NOT NULL,
    outcome TEXT NOT NULL CHECK (outcome = 'accepted'),
    changed_field_paths TEXT NOT NULL,
    occurred_at_unix_nanos BIGINT NOT NULL,
    trace_id TEXT NOT NULL CHECK (LENGTH(trace_id) = 32)
);

INSERT INTO workspace_lifecycle_events_new (
    id, workspace_id, actor_user_id, authority_source, action, target_kind,
    target_id, outcome, changed_field_paths, occurred_at_unix_nanos, trace_id
)
SELECT
    id, workspace_id, actor_user_id, authority_source, action, target_kind,
    target_id, outcome, changed_field_paths, occurred_at_unix_nanos, trace_id
FROM workspace_lifecycle_events;

DROP TABLE workspace_lifecycle_events;
ALTER TABLE workspace_lifecycle_events_new RENAME TO workspace_lifecycle_events;

CREATE INDEX idx_workspace_lifecycle_events_workspace_order
    ON workspace_lifecycle_events (workspace_id, occurred_at_unix_nanos, id);
