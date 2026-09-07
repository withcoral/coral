# Render

**Version:** 0.1.0
**Backend:** HTTP
**Tables:** 3

Query services, deploys, and workspaces from Render. Monitor deployment status, service configuration, and infrastructure through SQL.

## Installation

Install the source via the CLI:

```bash
coral source add --file sources/community/render/manifest.yaml
```

## Credentials

To use this source, you will need a Render API key.

1. Log in to [Render](https://dashboard.render.com).
2. Navigate to [Account Settings > API Keys](https://dashboard.render.com/u/settings#api-keys).
3. Create an API key (starts with `rnd_`).
4. Provide it when prompted by `coral source add` or set it as an environment variable:

```bash
export RENDER_API_KEY="rnd_your-api-key"
```

**Important:** The API key grants access to every workspace the user belongs to. There is no way to scope the key to a single workspace.

## Quick Start

```sql
-- List services (one page, up to 100 rows; use cursor filter for more)
SELECT service_id, name, type, status, url
FROM render.services;

-- List deploys for a service
SELECT deploy_id, status, trigger, commit_message
FROM render.deploys
WHERE service_id = 'srv-your-service-id'
LIMIT 10;

-- List workspaces
SELECT workspace_id, name, email, type
FROM render.workspaces;

-- Find services by type
SELECT service_id, name, url
FROM render.services
WHERE type = 'web_service';
```

## Tables

### `services`

Services deployed on Render. Includes static sites, web services, private services, background workers, and cron jobs. No required filters.

**Filters**

| Filter | Type | Required | Description |
|--------|------|----------|-------------|
| `type` | Utf8 | | Filter by service type (static_site, web_service, private_service, background_worker, cron_job) |
| `status` | Utf8 | | Filter by suspension status ('suspended' or 'not_suspended') |
| `cursor` | Utf8 | | Cursor from a previous query for manual pagination |

**Columns**

| Column | Type | Description |
|--------|------|-------------|
| `cursor` | Utf8 | Pagination cursor for manual pagination |
| `service_id` | Utf8 | Unique identifier for the service |
| `name` | Utf8 | Name of the service |
| `type` | Utf8 | Service type (static_site, web_service, private_service, background_worker, cron_job) |
| `status` | Utf8 | Suspension status of the service (not_suspended, suspended) |
| `repo` | Utf8 | Git repository URL |
| `branch` | Utf8 | Git branch used for deployments |
| `auto_deploy` | Utf8 | Whether auto-deploy is enabled (yes/no) |
| `url` | Utf8 | URL of the service (may be null; private services return a non-public internal URL) |
| `dashboard_url` | Utf8 | URL to the Render dashboard |
| `workspace_id` | Utf8 | ID of the workspace (user or team) this service belongs to |
| `slug` | Utf8 | URL slug of the service |
| `created_at` | Timestamp | When the service was created (ISO 8601) |
| `updated_at` | Timestamp | When the service was last updated (ISO 8601) |

---

### `deploys`

Deployment history for a Render service. Includes commit info, status, trigger, and timing. Requires `service_id` filter.

**Filters**

| Filter | Type | Required | Description |
|--------|------|----------|-------------|
| `service_id` | Utf8 | Yes | ID of the service to list deploys for |
| `status` | Utf8 | | Filter by deploy status (created, queued, build_in_progress, update_in_progress, live, deactivated, build_failed, update_failed, canceled, pre_deploy_in_progress, pre_deploy_failed) |
| `cursor` | Utf8 | | Cursor from a previous query for manual pagination |

**Columns**

| Column | Type | Description |
|--------|------|-------------|
| `cursor` | Utf8 | Pagination cursor for manual pagination |
| `service_id` | Utf8 | ID of the service (populated from filter via `from_filter`) |
| `deploy_id` | Utf8 | Unique identifier for the deploy |
| `status` | Utf8 | Deploy status (created, queued, build_in_progress, update_in_progress, live, deactivated, build_failed, update_failed, canceled, pre_deploy_in_progress, pre_deploy_failed) |
| `trigger` | Utf8 | What triggered the deploy (api, blueprint_sync, deploy_hook, deployed_by_render, manual, other, new_commit, rollback, service_resumed, service_updated) |
| `commit_id` | Utf8 | Git commit SHA |
| `commit_message` | Utf8 | Git commit message |
| `commit_created_at` | Timestamp | When the commit was created (ISO 8601) |
| `created_at` | Timestamp | When the deploy was created (ISO 8601) |
| `started_at` | Timestamp | When the deploy started (ISO 8601) |
| `finished_at` | Timestamp | When the deploy finished (ISO 8601) |
| `updated_at` | Timestamp | When the deploy was last updated (ISO 8601) |

---

### `workspaces`

Workspaces the API key has access to. The key grants access to every workspace the user belongs to. No required filters.

**Filters**

| Filter | Type | Required | Description |
|--------|------|----------|-------------|
| `cursor` | Utf8 | | Cursor from a previous query for manual pagination |

**Columns**

| Column | Type | Description |
|--------|------|-------------|
| `cursor` | Utf8 | Pagination cursor for manual pagination |
| `workspace_id` | Utf8 | Unique identifier for the workspace |
| `name` | Utf8 | Name of the workspace |
| `email` | Utf8 | Email address of the workspace owner |
| `type` | Utf8 | Workspace type (user or team) |

## Source scope

- Targets the Render API at `https://api.render.com/v1`.
- Requires `RENDER_API_KEY` authentication as a Bearer token.
- `deploys` requires a `service_id` filter (URL path segment). Use `services` to discover service IDs.
- SQL `LIMIT` is pushed to the API via `limit` query param (default 20, max 100).
- Render's API wraps each item in a `{cursor, entity}` object — columns extract from the nested entity.
- 1 declared test query (`services`) is source-independent.
- Provides read-only access. Creating, updating, or deleting services and deploys is out of scope.

## Limitations

- The source provides read-only list access only. Service creation, deployment triggers, environment variable management, and other write operations are out of scope.
- Render uses per-item cursor pagination. Each row includes a `cursor` column. To retrieve another page, pass the last row's cursor to a new query: `WHERE cursor = 'last_cursor_value'`.
- Timestamp fields use `Timestamp` type — Render returns RFC3339 strings with timezone (`Z` suffix) which Coral parses natively.
- The `url` column in `services` is extracted from `serviceDetails.url`. Private services return a non-public internal URL. Some service types may have null URLs.
- The `service_id` column in `deploys` is populated from the required filter via `from_filter` expression.

## Provider docs

- Render API reference: https://api-docs.render.com
- Services API: https://api-docs.render.com/reference/list-services
- Deploys API: https://api-docs.render.com/reference/list-deploys
- Workspaces API: https://api-docs.render.com/reference/list-owners
- API keys: https://dashboard.render.com/u/settings#api-keys

## Live validation output

Validated against a live Render account with a valid `RENDER_API_KEY`.

```bash
$ coral source lint sources/community/render/manifest.yaml
Manifest is valid
```

```bash
$ coral source add --file sources/community/render/manifest.yaml
Added source render (secrets: keychain)
Validating source...

  ✓ render connected successfully
  Secrets: keychain

    render (3 tables)
    ├─ deploys
    ├─ services
    └─ workspaces
    Query tests
    1 declared · 1 passed · 0 failed

    ✓ SELECT service_id, name, type, status FROM render.services LIMIT 3
      3 rows
```

```bash
$ coral source test render

  ✓ render connected successfully
  Secrets: keychain

    render (3 tables)
    ├─ deploys
    ├─ services
    └─ workspaces
    Query tests
    1 declared · 1 passed · 0 failed

    ✓ SELECT service_id, name, type, status FROM render.services LIMIT 3
      3 rows
```

**Table introspection:**

```sql
SELECT table_name, description, required_filters
FROM coral.tables
WHERE schema_name = 'render'
ORDER BY table_name;
```

```text
+------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------+
| table_name | description                                                                                                                                                       | required_filters |
+------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------+
| deploys    | Deployment history for a Render service. Includes commit info, status, trigger, and timing for each deploy.                                                       | service_id       |
| services   | Services deployed on Render. Includes static sites, web services, private services, background workers, and cron jobs with their configuration, status, and URLs. |                  |
| workspaces | Workspaces the API key has access to. The key grants access to every workspace the user belongs to.                                                               |                  |
+------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------+
```

**Live services proof (redacted):**

```sql
SELECT service_id, name, type, status, url
FROM render.services LIMIT 3;
```

```text
+--------------------------+------------------+-------------------+---------------+----------------------------------------+
| service_id               | name             | type              | status        | url                                    |
+--------------------------+------------------+-------------------+---------------+----------------------------------------+
| srv-0000000000000000000a | my-static-site   | static_site       | not_suspended | https://my-static-site.example.invalid |
| srv-0000000000000000000b | my-web-service   | web_service       | not_suspended | https://my-web-service.example.invalid |
| srv-0000000000000000000c | my-worker        | background_worker | not_suspended |                                        |
+--------------------------+------------------+-------------------+---------------+----------------------------------------+
```

**Live type filter proof (redacted):**

```sql
SELECT service_id, name, url
FROM render.services
WHERE type = 'web_service'
LIMIT 3;
```

```text
+--------------------------+------------------+----------------------------------------+
| service_id               | name             | url                                    |
+--------------------------+------------------+----------------------------------------+
| srv-0000000000000000000b | my-web-service   | https://my-web-service.example.invalid |
| srv-0000000000000000000d | api-server       | https://api-server.example.invalid     |
| srv-0000000000000000000e | dashboard        | https://dashboard.example.invalid      |
+--------------------------+------------------+----------------------------------------+
```

**Live status filter proof (redacted):**

```sql
SELECT service_id, name, status, url
FROM render.services
WHERE status = 'not_suspended'
LIMIT 3;
```

```text
+--------------------------+------------------+---------------+----------------------------------------+
| service_id               | name             | status        | url                                    |
+--------------------------+------------------+---------------+----------------------------------------+
| srv-0000000000000000000a | my-static-site   | not_suspended | https://my-static-site.example.invalid |
| srv-0000000000000000000b | my-web-service   | not_suspended | https://my-web-service.example.invalid |
| srv-0000000000000000000c | my-worker        | not_suspended |                                        |
+--------------------------+------------------+---------------+----------------------------------------+
```

**Live deploys proof (redacted):**

```sql
SELECT deploy_id, status, trigger
FROM render.deploys
WHERE service_id = 'srv-0000000000000000000b'
LIMIT 3;
```

```text
+--------------------------+--------------+------------+
| deploy_id                | status       | trigger    |
+--------------------------+--------------+------------+
| dep-0000000000000000000a | build_failed | manual     |
| dep-0000000000000000000b | canceled     | new_commit |
| dep-0000000000000000000c | build_failed | manual     |
+--------------------------+--------------+------------+
```

**Live deploy status filter proof (redacted):**

```sql
SELECT deploy_id, status, trigger
FROM render.deploys
WHERE service_id = 'srv-0000000000000000000b'
AND status = 'build_failed'
LIMIT 3;
```

```text
+--------------------------+--------------+---------+
| deploy_id                | status       | trigger |
+--------------------------+--------------+---------+
| dep-0000000000000000000a | build_failed | manual  |
| dep-0000000000000000000c | build_failed | manual  |
| dep-0000000000000000000d | build_failed | manual  |
+--------------------------+--------------+---------+
```

**Live workspaces proof (redacted):**

```sql
SELECT workspace_id, name, email, type
FROM render.workspaces;
```

```text
+--------------------------+---------+----------------------+------+
| workspace_id             | name    | email                | type |
+--------------------------+---------+----------------------+------+
| tea-0000000000000000000a | my-team | team@example.com     | team |
+--------------------------+---------+----------------------+------+
```
