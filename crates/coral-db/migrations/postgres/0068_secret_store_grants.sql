-- An Organization Owner grants a Secret Store to a Workspace (ADR 0036). A grant
-- names the store: deployment config defines stores, so a grant can outlive one.
-- Each grant and revocation records an Administrative Lifecycle Event.
-- Each rule below only allows more values, so every existing row already passes
-- it. NOT VALID skips the row scan that would hold an ACCESS EXCLUSIVE lock on
-- the table; new writes are still checked. lock_timeout limits how long the
-- migration waits for a lock to five seconds.
SET LOCAL lock_timeout = '5s';

CREATE TABLE workspace_secret_store_grants (
    workspace_id TEXT NOT NULL REFERENCES workspace_records(id),
    store_name TEXT NOT NULL CHECK (LENGTH(store_name) BETWEEN 1 AND 64),
    PRIMARY KEY (workspace_id, store_name)
);

ALTER TABLE workspace_lifecycle_events
    DROP CONSTRAINT workspace_lifecycle_events_action_check,
    ADD CONSTRAINT workspace_lifecycle_events_action_check
        CHECK (action IN (
            'workspace.created', 'workspace.display_name_updated',
            'workspace.archived', 'workspace.restored',
            'workspace_membership.created', 'workspace_membership.role_updated',
            'workspace_membership.removed', 'agent.created',
            'agent_registration.created', 'agent_registration.credential_rotated',
            'agent.state_updated', 'agent_registration.updated', 'agent.deleted',
            'secret_store.granted', 'secret_store.revoked'
        )) NOT VALID,
    DROP CONSTRAINT workspace_lifecycle_events_target_kind_check,
    ADD CONSTRAINT workspace_lifecycle_events_target_kind_check
        CHECK (target_kind IN (
            'workspace', 'workspace_membership', 'agent', 'agent_registration', 'secret_store'
        )) NOT VALID;
