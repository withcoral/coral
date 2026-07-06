# Windmill Community Source

Query Windmill scripts, flows, apps, and schedules through Coral SQL using the
[Windmill HTTP API](https://app.windmill.dev/openapi.html).

This source exposes read-only SQL tables with selected metadata columns. The
configured Windmill list endpoints may still transfer unmapped operational
fields to Coral before projection, including script source, flow definitions,
and schedule arguments.

## Setup

### 1. Create a Windmill token

Create a [Windmill user token](https://www.windmill.dev/docs/core_concepts/user_tokens).
Enable **Limit token permissions** and the read-only token restriction, then
grant these scopes:

- `scripts:read`
- `flows:read`
- `apps:read`
- `schedules:read`

These permissions were validated against all four tables in this source.

### 2. Add the source

For Windmill Cloud:

```powershell
$env:WINDMILL_BASE_URL = "https://app.windmill.dev"
$env:WINDMILL_WORKSPACE = "example-workspace"
$env:WINDMILL_TOKEN = "replace-with-read-only-token"
coral source add --file sources/community/windmill/manifest.yaml
```

For self-hosted Windmill, set `WINDMILL_BASE_URL` to your instance URL without
a trailing slash. The official Docker Compose setup is available at
`http://localhost`.

Interactive setup is also supported:

```powershell
coral source add --interactive --file sources/community/windmill/manifest.yaml
```

### 3. Verify

```powershell
coral source test windmill
```

## Tables

### `windmill.scripts`

List selected script metadata columns. Script source code is not exposed as a
SQL column.

| Column | Type | Description |
|---|---|---|
| `path` | Utf8 | Workspace-relative script path |
| `hash` | Utf8 | Stable script version identifier |
| `summary` | Utf8 | Script summary |
| `description` | Utf8 | Script description |
| `language` | Utf8 | Script language |
| `created_by` | Utf8 | User who created the script version |
| `created_at` | Timestamp | Time the script version was created |
| `archived` | Boolean | Whether the script is archived |
| `starred` | Boolean | Whether the script is starred |
| `draft_only` | Boolean | Whether only a draft version exists |
| `dedicated_worker` | Boolean | Whether the script uses a dedicated worker |
| `labels` | Json | Script labels |

**Optional filter:** `path`

### `windmill.flows`

List selected flow metadata columns. Raw flow definitions are not exposed as
SQL columns.

| Column | Type | Description |
|---|---|---|
| `path` | Utf8 | Workspace-relative flow path |
| `summary` | Utf8 | Flow summary |
| `description` | Utf8 | Flow description |
| `edited_by` | Utf8 | User who last edited the flow |
| `edited_at` | Timestamp | Time the flow was last edited |
| `archived` | Boolean | Whether the flow is archived |
| `starred` | Boolean | Whether the flow is starred |
| `draft_only` | Boolean | Whether only a draft version exists |
| `tag` | Utf8 | Flow worker tag |
| `dedicated_worker` | Boolean | Whether the flow uses a dedicated worker |
| `timeout` | Float64 | Flow timeout in seconds |
| `labels` | Json | Flow labels |

**Optional filter:** `path`

### `windmill.apps`

List selected app metadata columns. Raw app definitions are not exposed as SQL
columns.

| Column | Type | Description |
|---|---|---|
| `id` | Int64 | App ID |
| `workspace_id` | Utf8 | Workspace containing the app |
| `path` | Utf8 | Workspace-relative app path |
| `summary` | Utf8 | App summary |
| `version` | Int64 | Current app version |
| `starred` | Boolean | Whether the app is starred |
| `edited_at` | Timestamp | Time the app was last edited |
| `execution_mode` | Utf8 | App execution mode |
| `raw_app` | Boolean | Whether the app uses the raw-app format |
| `labels` | Json | App labels |

**Optional filter:** `path`

### `windmill.schedules`

List selected schedule metadata columns. Arguments and permission maps are not
exposed as SQL columns.

| Column | Type | Description |
|---|---|---|
| `path` | Utf8 | Workspace-relative schedule path |
| `edited_by` | Utf8 | User who last edited the schedule |
| `edited_at` | Timestamp | Time the schedule was last edited |
| `schedule` | Utf8 | Cron schedule expression |
| `timezone` | Utf8 | Schedule timezone |
| `enabled` | Boolean | Whether the schedule is enabled |
| `script_path` | Utf8 | Script or flow path invoked by the schedule |
| `is_flow` | Boolean | Whether the schedule invokes a flow |

**Optional filters:** `path`, `script_path`, `is_flow`

For schedules, `path` filters the schedule record path and `script_path`
filters the script or flow invoked by the schedule.

## Example queries

```sql
-- Review scripts that changed recently
SELECT path, language, summary, created_at
FROM windmill.scripts
ORDER BY created_at DESC
LIMIT 20;
```

```sql
-- Review enabled schedules and their targets
SELECT path, enabled, schedule, timezone, script_path, is_flow
FROM windmill.schedules
WHERE enabled = true
ORDER BY path;
```

```sql
-- Review starred apps
SELECT path, summary, execution_mode, edited_at
FROM windmill.apps
WHERE starred = true
ORDER BY edited_at DESC;
```

## Validation

```powershell
coral source lint sources/community/windmill/manifest.yaml
coral source add --file sources/community/windmill/manifest.yaml
coral source test windmill
coral sql "SELECT * FROM coral.tables WHERE schema_name = 'windmill'"
coral sql "SELECT path, hash, language, created_at FROM windmill.scripts LIMIT 5"
coral sql "SELECT path, script_path, enabled FROM windmill.schedules LIMIT 5"
```

## Limitations

- **Read-only.** This source does not create, update, execute, archive, or
  delete Windmill resources.
- **Selected metadata columns only.** Raw script source, raw flow definitions,
  app definitions, schedule args, permission maps, errors, email fields,
  resource values, variable values, job history, logs, and result payloads are
  not exposed as SQL columns.
- **Provider responses may contain unmapped fields.** Windmill list endpoints
  may still transfer workflow definitions, schedule arguments, and other
  operational fields to Coral before unmapped fields are discarded.
- **Metadata may still be sensitive.** Paths, descriptions, labels, editor
  identities, and schedule targets can reveal operational details.
- **Workspace-scoped.** Configure one `WINDMILL_WORKSPACE` per Coral source
  registration.
- **Minimum privileges recommended.** Enable Windmill's read-only token
  restriction and grant only `scripts:read`, `flows:read`, `apps:read`, and
  `schedules:read`.

## Out of scope for v1

- Queued and completed job metadata
- Job logs and result payloads
- Resource and variable metadata or values
- Script, flow, app, or schedule mutations
- User and permission management
