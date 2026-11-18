-- Coral now reads the times of an Agent and an Agent Registration from the
-- row. Step 0077 copied them there from `workspace_resource_metadata`. A Coral
-- from before that step served beside the one that applied it, and it writes
-- only that table: it creates an Agent and its Registration without the times
-- on the row, and it changes one without dating the change there. This step
-- brings each row that has a metadata row up to the times that row holds.
--
-- A time only moves forward. The creation time is filled where the row holds
-- none. The time of the last change is taken where the row holds none, or
-- holds an earlier one than the metadata row. So the step fills a row that a
-- Coral from before step 0077 created, and corrects one that such a Coral
-- changed last. A Coral with step 0077 writes the row and the metadata row
-- with one instant, and the step leaves a row that such a Coral wrote last.
--
-- A Coral with step 0077 still serves while this step runs. On Postgres, an
-- UPDATE that meets a row which that Coral is changing waits for the change
-- and then reads the changed row, but it still reads the metadata row as it
-- was before the change. The row then holds the later time, and keeps it:
-- this is why the step never sets a time back.
--
-- The Registrations go first, to match the only Agent deletion this step can
-- meet. This step runs in one transaction, and so takes every Registration
-- row and then every Agent row. A Coral whose plan ends before this step,
-- such as the one with step 0077, deletes an Agent by deleting the
-- Registration and then marking the Agent. With the Agents first here, the
-- two would wait for each other.
--
-- The Coral that brings this step deletes in the other order: it marks the
-- Agent and then deletes the Registration. Do not reverse this step for it.
-- No instance of that Coral, or of a later one, serves while this step runs,
-- because an instance applies its whole plan before it serves. On Postgres, a
-- second instance that starts meanwhile waits for the advisory lock of the
-- run that applies the plan, and then finds this step applied. On SQLite the
-- run takes no such lock. It applies the plan in one transaction that begins
-- with the database's write lock, so no other write, and so no deletion, runs
-- beside this step. A Coral process there also holds the lock of its state
-- root for as long as it serves, and a second process on that root fails at
-- startup. That Coral's deletion and an earlier Coral's do not wait for each
-- other either: on Postgres each holds the Workspace row before it takes
-- these rows, and on SQLite each writes in a transaction that begins with the
-- write lock.
--
-- A row that has no metadata row is left as it is, so no time is set to NULL:
-- Coral reads a missing time as absent. No version changes.
--
-- Coral still writes `workspace_resource_metadata` with every change, for a
-- Coral that reads it.
--
-- rolling-upgrade: compatible. This step rewrites only the time columns that step 0077 added to agent_registrations and workspace_agents: it fills a time that is NULL, and moves the time of the last change forward to the one that workspace_resource_metadata holds for the same row. No release before the one that brings this step reads these columns: the build that added them writes them, and reads the times from workspace_resource_metadata.
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
