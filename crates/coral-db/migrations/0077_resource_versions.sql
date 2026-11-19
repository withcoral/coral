-- A Workspace Membership, an Agent and an Agent Registration each keep a
-- version on their own row: a positive count that the statement which changes
-- the row raises by one. A Workspace already has one, `lifecycle_version`.
-- An Agent and an Agent Registration also keep the times of their creation
-- and of their last accepted change, which only `workspace_resource_metadata`
-- held until now.
--
-- Coral still shows the etag and the times that `workspace_resource_metadata`
-- holds, and still checks a UUID etag against it. A Coral from before this
-- step writes only that table, so it stays the record that both agree on.
--
-- Each version starts at 1, also for a row that was changed before this step:
-- a version only has to rise on every change from here on.
--
-- The time columns are nullable. A Coral from before this step inserts an
-- Agent and its Registration without them, and SQLite cannot add NOT NULL to
-- a column later without rebuilding the table.
ALTER TABLE workspace_memberships ADD COLUMN version BIGINT NOT NULL DEFAULT 1
    CONSTRAINT workspace_memberships_version_positive CHECK (version > 0);

ALTER TABLE workspace_agents ADD COLUMN version BIGINT NOT NULL DEFAULT 1
    CONSTRAINT workspace_agents_version_positive CHECK (version > 0);
ALTER TABLE workspace_agents ADD COLUMN created_at_unix_nanos BIGINT;
ALTER TABLE workspace_agents ADD COLUMN updated_at_unix_nanos BIGINT;

ALTER TABLE agent_registrations ADD COLUMN version BIGINT NOT NULL DEFAULT 1
    CONSTRAINT agent_registrations_version_positive CHECK (version > 0);
ALTER TABLE agent_registrations ADD COLUMN created_at_unix_nanos BIGINT;
ALTER TABLE agent_registrations ADD COLUMN updated_at_unix_nanos BIGINT;

-- Each Agent and each Registration takes the times that its metadata row
-- holds. The metadata row of an Agent names the Agent's id, and that of a
-- Registration names the Registration's id. A deleted Agent keeps its
-- metadata row, so it takes its times too.
UPDATE workspace_agents
SET created_at_unix_nanos = (
        SELECT m.created_at
        FROM workspace_resource_metadata m
        WHERE m.workspace_id = workspace_agents.workspace_id
            AND m.kind = 'agent'
            AND m.target_id = workspace_agents.id
    ),
    updated_at_unix_nanos = (
        SELECT m.updated_at
        FROM workspace_resource_metadata m
        WHERE m.workspace_id = workspace_agents.workspace_id
            AND m.kind = 'agent'
            AND m.target_id = workspace_agents.id
    );

UPDATE agent_registrations
SET created_at_unix_nanos = (
        SELECT m.created_at
        FROM workspace_resource_metadata m
        WHERE m.workspace_id = agent_registrations.workspace_id
            AND m.kind = 'agent_registration'
            AND m.target_id = agent_registrations.id
    ),
    updated_at_unix_nanos = (
        SELECT m.updated_at
        FROM workspace_resource_metadata m
        WHERE m.workspace_id = agent_registrations.workspace_id
            AND m.kind = 'agent_registration'
            AND m.target_id = agent_registrations.id
    );
