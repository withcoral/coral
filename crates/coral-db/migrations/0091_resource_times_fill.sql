-- The step after this one drops `workspace_resource_metadata`. For an Agent
-- or an Agent Registration, that table can still hold a time that the row
-- lacks. A Coral from before step 0077 writes only the table: it creates an
-- Agent and its Registration without the times on the row, and it changes
-- one without dating the change there. Step 0085 brought each row up to the
-- table once. A row that such a Coral created or changed after that step ran
-- is behind the table again. This step brings each row that has a metadata
-- row up to the times that row holds, for the last time.
--
-- A time only moves forward, as in step 0085. The creation time is filled
-- where the row holds none. The time of the last change is taken where the
-- row holds none, or holds an earlier one than the metadata row. A Coral
-- that changes a row while this step waits for it leaves the later time on
-- the row, and the row keeps it.
--
-- A row that has no metadata row is left as it is, so no time is set to NULL:
-- Coral reads a missing time as absent. No version changes.
--
-- A Workspace and a Workspace Membership need nothing. Each has kept both
-- times on its own row since the first step, and every Coral that wrote the
-- metadata row of one wrote the row itself with the same instant. One time
-- differs: a Membership that was removed and added again holds the time of
-- the new grant as its creation time, and the table holds that of the first.
-- Coral shows the row's.
--
-- The Agents go first, to match the Agent deletion this step can meet. On
-- Postgres, Coral applies this step only in a run that also applies the drop,
-- and it refuses that run unless step 0090 is applied. So a Coral with step
-- 0090 has started on the database, and an operator who upgrades one release
-- at a time runs no Coral older than that one beside this step. Since the
-- release with step 0085, Coral deletes an Agent by marking the Agent and
-- then deleting its Registration. This step runs in one transaction, and so
-- takes every Agent row and then every Registration row: the same order.
-- Step 0085 went the other way, for a Coral whose plan ended before it,
-- which deletes the Registration first.
--
-- On SQLite no Coral serves beside this step. The run applies the plan in
-- one transaction that begins with the database's write lock, and a Coral
-- process holds the lock of its state root for as long as it serves.
--
-- This step and the drop are two steps so that, on Postgres, each commits by
-- itself. The drop explains why it must hold no Agent row.
--
-- rolling-upgrade: compatible. This step rewrites only the two time columns of workspace_agents and agent_registrations: it fills a time that is NULL, and moves the time of the last change forward to the one that workspace_resource_metadata holds for the same row. A Coral that reads these columns reads any time as it stands, and one that changes a row beside this step keeps its own later time.
UPDATE workspace_agents
SET created_at_unix_nanos = COALESCE(
        created_at_unix_nanos,
        (
            SELECT m.created_at
            FROM workspace_resource_metadata m
            WHERE m.workspace_id = workspace_agents.workspace_id
                AND m.kind = 'agent'
                AND m.target_id = workspace_agents.id
        )
    ),
    updated_at_unix_nanos = CASE
        WHEN updated_at_unix_nanos IS NULL
            OR updated_at_unix_nanos < (
                SELECT m.updated_at
                FROM workspace_resource_metadata m
                WHERE m.workspace_id = workspace_agents.workspace_id
                    AND m.kind = 'agent'
                    AND m.target_id = workspace_agents.id
            )
        THEN (
            SELECT m.updated_at
            FROM workspace_resource_metadata m
            WHERE m.workspace_id = workspace_agents.workspace_id
                AND m.kind = 'agent'
                AND m.target_id = workspace_agents.id
        )
        ELSE updated_at_unix_nanos
    END
WHERE EXISTS (
    SELECT 1
    FROM workspace_resource_metadata m
    WHERE m.workspace_id = workspace_agents.workspace_id
        AND m.kind = 'agent'
        AND m.target_id = workspace_agents.id
);

UPDATE agent_registrations
SET created_at_unix_nanos = COALESCE(
        created_at_unix_nanos,
        (
            SELECT m.created_at
            FROM workspace_resource_metadata m
            WHERE m.workspace_id = agent_registrations.workspace_id
                AND m.kind = 'agent_registration'
                AND m.target_id = agent_registrations.id
        )
    ),
    updated_at_unix_nanos = CASE
        WHEN updated_at_unix_nanos IS NULL
            OR updated_at_unix_nanos < (
                SELECT m.updated_at
                FROM workspace_resource_metadata m
                WHERE m.workspace_id = agent_registrations.workspace_id
                    AND m.kind = 'agent_registration'
                    AND m.target_id = agent_registrations.id
            )
        THEN (
            SELECT m.updated_at
            FROM workspace_resource_metadata m
            WHERE m.workspace_id = agent_registrations.workspace_id
                AND m.kind = 'agent_registration'
                AND m.target_id = agent_registrations.id
        )
        ELSE updated_at_unix_nanos
    END
WHERE EXISTS (
    SELECT 1
    FROM workspace_resource_metadata m
    WHERE m.workspace_id = agent_registrations.workspace_id
        AND m.kind = 'agent_registration'
        AND m.target_id = agent_registrations.id
);
