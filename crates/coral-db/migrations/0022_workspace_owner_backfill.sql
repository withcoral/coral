-- Workspace authority now comes only from a Workspace Membership. Until now an
-- enabled Organization Owner held Owner authority in every Workspace without
-- one. Record that authority as a designated Owner Membership, so nobody loses
-- access on upgrade and every existing Workspace has an Owner on record.
--
-- The Events and concurrency metadata go first: they select the same pairs as
-- the Membership writes below, which change what that selection matches. The
-- migration has no clock or trace, so each Event carries its Workspace's last
-- update time and one fixed trace identifier.

INSERT INTO workspace_lifecycle_events (
    id, workspace_id, actor_user_id, authority_source, action, target_kind,
    target_id, outcome, changed_field_paths, occurred_at_unix_nanos, trace_id
)
SELECT
    'owner-backfill:' || w.id || ':' || o.user_id,
    w.id,
    o.user_id,
    'organization_owner',
    CASE
        WHEN m.user_id IS NULL THEN 'workspace_membership.created'
        ELSE 'workspace_membership.role_updated'
    END,
    'workspace_membership',
    o.user_id,
    'accepted',
    '["role"]',
    w.updated_at_unix_nanos,
    '00000000000000000000000000000621'
FROM workspace_records w
CROSS JOIN organization_authority_records o
LEFT JOIN workspace_memberships m
    ON m.workspace_id = w.id AND m.user_id = o.user_id
WHERE o.role = 'owner' AND o.enabled = 1 AND o.removed = 0
    AND (m.user_id IS NULL OR m.role = 'member');

-- A Membership removed earlier leaves its metadata row behind, so change the
-- rows that exist before inserting the ones that do not. An etag must be a
-- UUID and must differ from the Membership's earlier one. Portable SQL cannot
-- generate a UUID, so each backfilled Membership takes its Workspace's
-- identifier. An etag is compared only against its own resource's metadata.
UPDATE workspace_resource_metadata
SET etag = workspace_id,
    updated_at = (
        SELECT w.updated_at_unix_nanos
        FROM workspace_records w
        WHERE w.id = workspace_resource_metadata.workspace_id
    )
WHERE kind = 'workspace_membership'
    AND EXISTS (
        SELECT 1
        FROM organization_authority_records o
        WHERE o.user_id = workspace_resource_metadata.target_id
            AND o.role = 'owner' AND o.enabled = 1 AND o.removed = 0
    )
    AND NOT EXISTS (
        SELECT 1
        FROM workspace_memberships m
        WHERE m.workspace_id = workspace_resource_metadata.workspace_id
            AND m.user_id = workspace_resource_metadata.target_id
            AND m.role = 'owner'
    );

INSERT INTO workspace_resource_metadata (
    workspace_id, kind, target_id, etag, created_at, updated_at
)
SELECT
    w.id,
    'workspace_membership',
    o.user_id,
    w.id,
    w.updated_at_unix_nanos,
    w.updated_at_unix_nanos
FROM workspace_records w
CROSS JOIN organization_authority_records o
WHERE o.role = 'owner' AND o.enabled = 1 AND o.removed = 0
    AND NOT EXISTS (
        SELECT 1
        FROM workspace_resource_metadata existing
        WHERE existing.workspace_id = w.id
            AND existing.kind = 'workspace_membership'
            AND existing.target_id = o.user_id
    );

UPDATE workspace_memberships
SET role = 'owner',
    updated_at_unix_nanos = (
        SELECT w.updated_at_unix_nanos
        FROM workspace_records w
        WHERE w.id = workspace_memberships.workspace_id
    )
WHERE role = 'member'
    AND EXISTS (
        SELECT 1
        FROM organization_authority_records o
        WHERE o.user_id = workspace_memberships.user_id
            AND o.role = 'owner' AND o.enabled = 1 AND o.removed = 0
    );

INSERT INTO workspace_memberships (
    workspace_id, user_id, role, created_at_unix_nanos, updated_at_unix_nanos
)
SELECT w.id, o.user_id, 'owner', w.updated_at_unix_nanos, w.updated_at_unix_nanos
FROM workspace_records w
CROSS JOIN organization_authority_records o
WHERE o.role = 'owner' AND o.enabled = 1 AND o.removed = 0
    AND NOT EXISTS (
        SELECT 1
        FROM workspace_memberships m
        WHERE m.workspace_id = w.id AND m.user_id = o.user_id
    );
