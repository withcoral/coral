-- Coral now reads the times of an Agent and an Agent Registration from the
-- row. Step 0077 copied them there from `workspace_resource_metadata`. A Coral
-- from before that step served beside the one that applied it, and it writes
-- only that table: it creates an Agent and its Registration without the times
-- on the row, and it changes one without dating the change there. This step
-- copies the times again, to each row that has a metadata row.
--
-- When this step runs, `workspace_resource_metadata` is still the record that
-- every Coral agrees on. A Coral with step 0077 writes the row and the
-- metadata row with one instant, so the copy changes nothing for a row that
-- such a Coral wrote last. It corrects a row that a Coral from before step
-- 0077 created or changed last.
--
-- A row that has no metadata row is left as it is, so no time is set to NULL:
-- Coral reads a missing time as absent. No version changes.
--
-- Coral still writes `workspace_resource_metadata` with every change, for a
-- Coral that reads it.
--
-- rolling-upgrade: compatible. This step rewrites only the time columns that step 0077 added to workspace_agents and agent_registrations, with the times that workspace_resource_metadata holds for the same row. No release reads these columns, and the build that added them reads the times from workspace_resource_metadata and writes both with one instant.
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
    )
WHERE EXISTS (
    SELECT 1
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
    )
WHERE EXISTS (
    SELECT 1
    FROM workspace_resource_metadata m
    WHERE m.workspace_id = agent_registrations.workspace_id
        AND m.kind = 'agent_registration'
        AND m.target_id = agent_registrations.id
);
