# NeonDB

Query Neon organizations, projects, branches, databases, roles, compute endpoints, operations, and user identity from the Neon Developer API.

## Setup

### Get Your API Key

1. Open the Neon Console.
2. Go to Account settings > API keys.
3. Create a personal, organization, or project-scoped API key.
4. Copy the token when it is shown.

### Add the Source

```bash
coral source add neondb
```

When prompted, provide your Neon API key as `NEON_API_KEY`.

## Tables

### `current_user`
Returns the authenticated Neon user. Use this table to validate credentials and identify which account the API key belongs to.

### `organizations`
Organizations available to the authenticated Neon user.

**Useful for:**
- Discovering `org_id` values
- Understanding organization plan and MFA requirements
- Supplying the `org_id` filter for organization-scoped project queries

### `projects`
Top-level Neon project inventory.

**Useful for:**
- Project inventory
- Region and PostgreSQL version visibility
- Finding `project_id` values for scoped tables

Optional filters:
- `org_id`
- `search`: Substring match on project name or ID

Organization-scoped API keys may require `org_id`. Query `neondb.organizations` first to find it.

### `branches`
Branch inventory for a single Neon project.

**Requires:** `project_id`

**Useful for:**
- Preview and development environment discovery
- Default and protected branch inspection
- Branch topology through `parent_id`

Optional filters:
- `search`: Substring match on branch name or ID

### `databases`
Database metadata for a single Neon branch.

**Requires:** `project_id`, `branch_id`

**Useful for:**
- Database topology
- Owner role inspection
- Branch composition analysis

### `roles`
Postgres role metadata for a single Neon branch.

**Requires:** `project_id`, `branch_id`

> **Security note:** This table intentionally does not expose role passwords or generated credentials.

**Useful for:**
- Role inventory
- Protected role inspection
- Authentication method review

### `compute_endpoints`
Compute endpoint metadata for a single Neon branch.

**Requires:** `project_id`, `branch_id`

> **Security note:** This table intentionally does not expose connection URIs, passwords, or endpoint hostnames.

**Useful for:**
- Read-write and read-only compute inventory
- Autoscaling limit inspection
- Compute state and suspension analysis

### `operations`
Recent operation history for a single Neon project.

**Requires:** `project_id`

**Useful for:**
- Infrastructure change history
- Debugging failed operations
- Summarizing recent branch, endpoint, and project activity

Neon may remove operations older than six months.

## Authentication

The source uses the Neon Developer API with bearer authentication:

```text
Authorization: Bearer <NEON_API_KEY>
```

Use a key with read access to the projects you want to inspect. Organization-scoped keys may require the `org_id` filter when querying `projects`.

## Limits

- This source exposes read-only Neon API endpoints only.
- Management operations such as creating branches, deleting endpoints, resetting passwords, and changing autoscaling are out of scope.
- `branches` and `operations` use cursor pagination.
- `projects` uses a single-page list request capped at 400 rows in v1 because Neon returns a terminal cursor even when the following page is empty.
- `operations` caps cursor traversal defensively to avoid looping on empty terminal pages.
- Branch child tables require filters to avoid cross-project scans.
- Role passwords, connection strings, and endpoint hostnames are intentionally omitted.

## Example Queries

### List projects by region

```sql
SELECT region_id, COUNT(*) AS project_count
FROM neondb.projects
GROUP BY region_id
ORDER BY project_count DESC
```

### Inspect branches in a project

```sql
SELECT branch_id, branch_name, default_branch, protected, current_state
FROM neondb.branches
WHERE project_id = 'your-project-id'
ORDER BY updated_at DESC
```

### Find compute endpoints for a branch

```sql
SELECT endpoint_id, endpoint_type, current_state, autoscaling_limit_max_cu
FROM neondb.compute_endpoints
WHERE project_id = 'your-project-id'
  AND branch_id = 'your-branch-id'
```

### Review recent failed operations

```sql
SELECT operation_id, action, status, error, created_at
FROM neondb.operations
WHERE project_id = 'your-project-id'
  AND status IN ('failed', 'error')
ORDER BY created_at DESC
LIMIT 20
```

## Notes

- Use `neondb.projects` first to find project IDs.
- Use `neondb.organizations` first if Neon requires `org_id` for project listing.
- Use `neondb.branches` next to find branch IDs for branch-scoped tables.
- JSON columns preserve nested configuration without exposing credentials.
- Timestamps are stored as proper Timestamp columns derived from ISO 8601 strings.
