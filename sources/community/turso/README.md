# Turso

**Version:** 0.1.0
**Backend:** HTTP
**Tables:** 5
**Base URL:** `https://api.turso.tech/v1`

Query organizations, databases, groups, members, and edge locations from the Turso Platform API.

## Authentication

Requires a `TURSO_API_TOKEN`. Generate one from:
**Turso Dashboard → Settings → API Tokens**

Or via the CLI:

```bash
turso auth token
```

Then add the source:

```bash
TURSO_API_TOKEN=your_token coral source add --file sources/community/turso/manifest.yaml
```

Or interactively:

```bash
TURSO_API_TOKEN=your_token coral source add --file sources/community/turso/manifest.yaml --interactive
```

API docs: https://docs.turso.tech/api-reference/introduction

## Tables

| Table | Description | Required filters | Optional filters |
|---|---|---|---|
| `organizations` | All organizations accessible to the API token, with plan and status | — | — |
| `databases` | Databases in an organization, with hostname and region info | `organization_slug` | `group`, `schema`, `parent` |
| `groups` | Database groups in an organization, with primary region and replica locations | `organization_slug` | — |
| `members` | Members of an organization with their roles | `organization_slug` | — |
| `locations` | All available Turso edge regions (code + human-readable name) | — | — |

### Discovery order

`organizations` is the entry point — no filter required. Query it first to
get `slug` values, then use those as `organization_slug` in the other tables.

```text
organizations
  → slug (organization_slug)
    → databases  (WHERE organization_slug = '...')
    → groups     (WHERE organization_slug = '...')
    → members    (WHERE organization_slug = '...')

locations      (no filter — global reference table)
```

> **Note:** The `locations` table uses `row_strategy: dict_entries` to convert
> the API's key-value map response into rows. It does not require any filter and
> is useful as a reference for interpreting `primary_region` and `locations`
> columns in `databases` and `groups`.

### databases optional filters

| Filter | Description |
|---|---|
| `group` | Narrow results to databases in a specific group (e.g. `default`) |
| `schema` | Return databases that belong to a specific parent schema database |
| `parent` | Return branched databases by their parent database ID |

### members role values

| Role | Description |
|---|---|
| `owner` | Organization owner — full control, always exactly one per org |
| `admin` | Admin-level access to organization resources |
| `member` | Standard member access |

## Quick start

```bash
# Step 1 — discover your organizations and their slugs
coral sql "SELECT name, slug, type, plan_id FROM turso.organizations"

# Step 2 — list all databases in an organization
coral sql "
  SELECT name, db_id, hostname, group, primary_region
  FROM turso.databases
  WHERE organization_slug = 'my-org-slug'
  LIMIT 20
"

# Step 3 — list groups and their replica regions
coral sql "
  SELECT name, uuid, primary, locations
  FROM turso.groups
  WHERE organization_slug = 'my-org-slug'
"
```

## Example queries

### List all organizations and their plan

```sql
SELECT
  name,
  slug,
  type,
  plan_id,
  plan_timeline,
  overages,
  blocked_reads,
  blocked_writes
FROM turso.organizations;
```

### List all databases in an organization

```sql
SELECT
  name,
  db_id,
  hostname,
  group,
  primary_region,
  regions,
  block_reads,
  block_writes,
  delete_protection
FROM turso.databases
WHERE organization_slug = 'my-org-slug';
```

### Filter databases by group name

```sql
SELECT
  name,
  db_id,
  hostname,
  primary_region
FROM turso.databases
WHERE organization_slug = 'my-org-slug'
  AND group = 'default';
```

### List all groups in an organization

```sql
SELECT
  name,
  uuid,
  version,
  primary,
  locations,
  delete_protection
FROM turso.groups
WHERE organization_slug = 'my-org-slug';
```

### List all members of an organization

```sql
SELECT
  username,
  email,
  role
FROM turso.members
WHERE organization_slug = 'my-org-slug'
ORDER BY role;
```

### Browse available edge regions

```sql
SELECT code, name
FROM turso.locations
ORDER BY code;
```
