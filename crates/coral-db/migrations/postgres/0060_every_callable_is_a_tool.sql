-- Every callable is a Tool (ADR 0033). This step changes what a Provider, an
-- Invocation and a Run Event keep.
--
-- Every Workspace has a Private Provider at `coral.private` and a Shared
-- Provider at `coral.shared`. Coral performs their Tools itself, so each pins
-- a Generation of input kind 'private' or 'shared' that holds no materialized
-- Tools, and installing one records the lifecycle action
-- 'provider.private_installed' or 'provider.shared_installed'.
--
-- An Invocation gains the performed call whose body made it, in the same Run
-- (the Program's own calls and its modules' top-level calls have none), and
-- the default Tool Access State of the revision the Run linked, which no
-- catalog stores.
--
-- Saved Functions are Private Tools. A location that a Run recorded names its
-- source document `private_tool`, where it named `saved_function`, in the
-- column `source_private_tool_id`. A call, an emission or a log made inside a
-- Shared Tool's body records where that Shared Tool's source made it, in
-- `source_shared_tool_id`, with the source document `shared_tool`. A Task
-- Interaction names the Private Tool it saved or deleted in `private_tool_id`.

-- Like the step that renamed Operations to Provider Tools, this one breaks
-- the previous release's queries, so the upgrade stops every earlier process
-- before the first one on this release boots. lock_timeout makes the
-- migration fail instead of queueing reads and writes behind its locks.
-- squawk-ignore-file renaming-column, constraint-missing-not-valid, disallowed-unique-constraint, adding-foreign-key-constraint, require-concurrent-index-creation
SET LOCAL lock_timeout = '5s';

-- Each of these two rules only allows more values, so every existing row
-- already passes it. NOT VALID skips the row scan that would hold an ACCESS
-- EXCLUSIVE lock on the table; new writes are still checked.
ALTER TABLE provider_generations
    DROP CONSTRAINT provider_generations_input_kind_valid,
    ADD CONSTRAINT provider_generations_input_kind_valid
        CHECK (input_kind IN ('openapi', 'mcp', 'builtin', 'private', 'shared')) NOT VALID;

ALTER TABLE provider_lifecycle_events
    DROP CONSTRAINT provider_lifecycle_events_action_check,
    ADD CONSTRAINT provider_lifecycle_events_action_check
        CHECK (action IN (
            'provider_setup.created', 'provider_setup.update_prepared', 'provider_setup.configuration_confirmed', 'provider_setup.discovery_completed', 'provider_setup.canceled',
            'provider_setup.expired', 'provider_publication.completed',

            'provider.disabled', 'provider.updated', 'provider.restored', 'provider.deleted', 'provider.oauth_client_secret_rotated',
            'provider.builtin_installed', 'provider.private_installed', 'provider.shared_installed',
            'provider.credential_route_changed',
            'provider_account_authorization.started', 'provider_account_authorization.exchanging',
            'provider_account_authorization.completed', 'provider_account.connected', 'provider_account.deleted', 'provider_account.disconnected',
            'account_connection.completed',
            'provider_account.reconnected', 'provider_account.refreshed', 'provider_account.reconnection_required'
        )) NOT VALID;

-- The parent and declared-default columns are new, so every existing row
-- holds NULL and each of their constraints' scans finds nothing to reject.
-- The unique constraint and the index build in SQLx's transaction, where
-- `CONCURRENTLY` cannot run, and a separate VALIDATE would take its lock in
-- the same transaction.
ALTER TABLE tool_invocations ADD COLUMN parent_invocation_id TEXT;

ALTER TABLE tool_invocations
    ADD CONSTRAINT tool_invocations_run_id_unique
        UNIQUE (workspace_id, run_id, id);
ALTER TABLE tool_invocations
    ADD CONSTRAINT tool_invocations_parent_fk
        FOREIGN KEY (workspace_id, run_id, parent_invocation_id)
        REFERENCES tool_invocations(workspace_id, run_id, id);
ALTER TABLE tool_invocations
    ADD CONSTRAINT tool_invocations_parent_not_self
        CHECK (parent_invocation_id IS NULL OR parent_invocation_id <> id);

CREATE INDEX idx_tool_invocations_parent
    ON tool_invocations (workspace_id, run_id, parent_invocation_id)
    WHERE parent_invocation_id IS NOT NULL;

ALTER TABLE tool_invocations
    ADD COLUMN declared_default_access_state TEXT
        CONSTRAINT tool_invocations_declared_default_access_state_valid
            CHECK (
                declared_default_access_state IS NULL
                OR declared_default_access_state IN ('enabled', 'approval_required', 'disabled')
            );

-- A recorded location names its source document `private_tool`, where it
-- named `saved_function`, or `shared_tool`. The step rewrites every row that
-- named `saved_function` before it adds the rule, so every existing row
-- passes it. NOT VALID skips the row scan that would hold an ACCESS EXCLUSIVE
-- lock on each table; new writes are still checked.
ALTER TABLE tool_invocations RENAME COLUMN source_saved_function_id TO source_private_tool_id;
ALTER TABLE tool_invocations ADD COLUMN source_shared_tool_id TEXT;
ALTER TABLE tool_invocations DROP CONSTRAINT tool_invocations_source_location_consistent;
UPDATE tool_invocations SET source_document = 'private_tool' WHERE source_document = 'saved_function';
ALTER TABLE tool_invocations
    ADD CONSTRAINT tool_invocations_source_location_consistent
    CHECK (COALESCE(
        (source_document IS NULL AND source_private_tool_id IS NULL AND source_shared_tool_id IS NULL
            AND source_line IS NULL AND source_column IS NULL)
        OR
        (source_document = 'program' AND source_private_tool_id IS NULL AND source_shared_tool_id IS NULL
            AND source_line > 0 AND source_column > 0)
        OR
        (source_document = 'private_tool' AND LENGTH(source_private_tool_id) > 0 AND source_shared_tool_id IS NULL
            AND source_line > 0 AND source_column > 0)
        OR
        (source_document = 'shared_tool' AND source_private_tool_id IS NULL AND LENGTH(source_shared_tool_id) > 0
            AND source_line > 0 AND source_column > 0),
        FALSE
    )) NOT VALID;

ALTER TABLE program_run_events RENAME COLUMN source_saved_function_id TO source_private_tool_id;
ALTER TABLE program_run_events ADD COLUMN source_shared_tool_id TEXT;
ALTER TABLE program_run_events DROP CONSTRAINT program_run_events_source_location_consistent;
UPDATE program_run_events SET source_document = 'private_tool' WHERE source_document = 'saved_function';
ALTER TABLE program_run_events
    ADD CONSTRAINT program_run_events_source_location_consistent
    CHECK (COALESCE(
        (source_document IS NULL AND source_private_tool_id IS NULL AND source_shared_tool_id IS NULL
            AND source_line IS NULL AND source_column IS NULL)
        OR
        (event_type IN ('program.emitted', 'program.logged')
            AND (
                (source_document = 'program' AND source_private_tool_id IS NULL AND source_shared_tool_id IS NULL
                    AND source_line > 0 AND source_column > 0)
                OR
                (source_document = 'private_tool' AND LENGTH(source_private_tool_id) > 0 AND source_shared_tool_id IS NULL
                    AND source_line > 0 AND source_column > 0)
                OR
                (source_document = 'shared_tool' AND source_private_tool_id IS NULL AND LENGTH(source_shared_tool_id) > 0
                    AND source_line > 0 AND source_column > 0)
            )),
        FALSE
    )) NOT VALID;

ALTER TABLE task_interactions RENAME COLUMN function_id TO private_tool_id;
