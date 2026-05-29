# Coralogix Community Source

Query Coralogix dashboards, alert definitions, service SLOs, team groups, and
roles through Coral SQL using the
[Coralogix OpenAPI management APIs](https://docs.coralogix.com/introduction-latest).

## Setup

### 1. Create a Coralogix API key

Create an API key from **Data Flow > API Keys** in your Coralogix account.
Grant only the read permissions needed for the tables you plan to query, for
example:

- `team-dashboards:Read` for dashboards and dashboard folders
- `alerts:ReadConfig` for alert definitions
- APM SLO read permissions for service SLOs
- Team administration read permissions for team groups and roles

### 2. Choose the API endpoint

Use the management API endpoint for your Coralogix region. The default is:

```bash
export CORALOGIX_API_BASE="https://api.coralogix.com/mgmt/openapi/5"
```

If your account uses a regional endpoint, replace the host with the endpoint
from your Coralogix documentation/account.

### 3. Add the source

```bash
export CORALOGIX_API_KEY="<your-api-key>"
coral source add --file sources/community/coralogix/manifest.yaml
```

### 4. Verify

```bash
coral source test coralogix
```

The default test query reads `coralogix.dashboards`, so the API key must have
dashboard read access.

## Tables

### `coralogix.dashboards`

Dashboard catalog items accessible to the API key.

| Column | Type | Description |
|---|---|---|
| `id` | Utf8 | Dashboard ID |
| `name` | Utf8 | Dashboard name |
| `slug_name` | Utf8 | Dashboard slug |
| `description` | Utf8 | Dashboard description |
| `author_id` | Utf8 | Author ID |
| `folder__id` | Utf8 | Folder ID |
| `folder__name` | Utf8 | Folder name |
| `folder__parent_id` | Utf8 | Parent folder ID |
| `create_time` | Timestamp | Creation time |
| `update_time` | Timestamp | Update time |
| `is_default` | Boolean | Whether this is a default dashboard |
| `is_locked` | Boolean | Whether this dashboard is locked |
| `is_pinned` | Boolean | Whether this dashboard is pinned |
| `raw` | Json | Full dashboard catalog item |

### `coralogix.dashboard_folders`

Dashboard folders accessible to the API key.

| Column | Type | Description |
|---|---|---|
| `id` | Utf8 | Folder ID |
| `name` | Utf8 | Folder name |
| `parent_id` | Utf8 | Parent folder ID |

### `coralogix.alerts`

Alert definitions accessible to the API key.

| Column | Type | Description |
|---|---|---|
| `id` | Utf8 | Persistent alert definition ID |
| `alert_version_id` | Utf8 | Alert version ID |
| `name` | Utf8 | Alert name |
| `description` | Utf8 | Alert description |
| `status` | Utf8 | Alert status |
| `priority` | Utf8 | Alert priority |
| `created_time` | Timestamp | Creation time |
| `updated_time` | Timestamp | Update time |
| `last_triggered_time` | Timestamp | Last trigger time |
| `properties` | Json | Alert definition properties |
| `raw` | Json | Full alert definition payload |

### `coralogix.service_slos`

APM service-level objectives.

| Column | Type | Description |
|---|---|---|
| `id` | Utf8 | SLO ID |
| `name` | Utf8 | SLO name |
| `service_name` | Utf8 | Service name |
| `description` | Utf8 | SLO description |
| `status` | Utf8 | SLO status |
| `target_percentage` | Int64 | SLO target percentage |
| `remaining_error_budget_percentage` | Int64 | Remaining error budget percentage |
| `created_at` | Timestamp | Creation time |
| `filters` | Json | SLO filters |
| `period` | Json | Evaluation period |
| `latency_sli` | Json | Latency SLI configuration |
| `error_sli` | Json | Error SLI configuration |

**Optional filter:** `service_name`

### `coralogix.team_groups`

Team groups with role and scope configuration.

| Column | Type | Description |
|---|---|---|
| `group_id` | Int64 | Group ID |
| `name` | Utf8 | Group name |
| `description` | Utf8 | Group description |
| `external_id` | Utf8 | External identity provider ID |
| `group_origin` | Utf8 | Group origin |
| `group_type` | Utf8 | Group type |
| `team_id` | Int64 | Team ID |
| `next_gen_scope_id` | Utf8 | Scope ID |
| `created_at` | Timestamp | Creation time |
| `updated_at` | Timestamp | Update time |
| `roles` | Json | Assigned roles |
| `scope` | Json | Assigned scope |

### `coralogix.custom_roles`

Custom team roles.

| Column | Type | Description |
|---|---|---|
| `role_id` | Int64 | Role ID |
| `name` | Utf8 | Role name |
| `description` | Utf8 | Role description |
| `team_id` | Int64 | Team ID |
| `parent_role_id` | Int64 | Parent role ID |
| `parent_role_name` | Utf8 | Parent role name |
| `permissions` | Json | Role permissions |

**Optional filter:** `team_id`

### `coralogix.system_roles`

Built-in Coralogix roles.

| Column | Type | Description |
|---|---|---|
| `role_id` | Int64 | Role ID |
| `name` | Utf8 | Role name |
| `description` | Utf8 | Role description |
| `permissions` | Json | Role permissions |

## Example Queries

```sql
-- List dashboards by folder
SELECT folder__name, name, update_time, is_locked
FROM coralogix.dashboards
ORDER BY folder__name, name;

-- Review alert definitions and last trigger time
SELECT name, status, priority, last_triggered_time
FROM coralogix.alerts
ORDER BY last_triggered_time DESC
LIMIT 20;

-- Check service SLO error budgets
SELECT service_name, name, target_percentage, remaining_error_budget_percentage
FROM coralogix.service_slos
ORDER BY remaining_error_budget_percentage ASC
LIMIT 20;

-- Inspect group role assignments
SELECT name, group_origin, team_id, roles
FROM coralogix.team_groups
ORDER BY name;

-- Find roles with a specific permission
SELECT name, permissions
FROM coralogix.system_roles
WHERE CAST(permissions AS VARCHAR) LIKE '%alerts%';
```

## Validation

```bash
export CORALOGIX_API_KEY="<your-api-key>"
export CORALOGIX_API_BASE="https://api.coralogix.com/mgmt/openapi/5"
coral source lint sources/community/coralogix/manifest.yaml
coral source add --file sources/community/coralogix/manifest.yaml
coral source test coralogix
coral sql "SELECT * FROM coral.tables WHERE schema_name = 'coralogix'"
coral sql "SELECT * FROM coral.columns WHERE schema_name = 'coralogix'"
coral sql "SELECT name, folder__name FROM coralogix.dashboards LIMIT 5"
```

## Limitations

- **Read-only.** This source does not create, update, delete, enable, or disable
  Coralogix resources.
- **Regional endpoints.** Coralogix uses regional API endpoints. Set
  `CORALOGIX_API_BASE` to the endpoint for your account.
- **Pagination.** This first pass maps endpoints that return compact inventory
  payloads directly. Some Coralogix APIs expose advanced object-shaped
  pagination/filter parameters that can be added in a future revision.
- **Permissions.** Empty or unauthorized results usually mean the API key lacks
  the corresponding read permission.

## Out of scope for v1

- Logs, metrics, and trace data queries
- Incidents and cases
- Alert event history
- Parsing rules and data routing rules
- Dashboard widget expansion
- Write operations
