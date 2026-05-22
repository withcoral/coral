# Kestra Connector

**Version:** 0.1.0
**Backend:** HTTP (BasicAuth)
**Tables:** 4 (`namespaces`, `flows`, `executions`, `logs`)
**Base URL:** `http://localhost:8080` (override with `KESTRA_BASE_URL`)

Connects to a self-hosted [Kestra](https://kestra.io) instance and exposes
flows, executions, logs, and namespaces as queryable SQL tables. Works with
Kestra OSS and any self-hosted deployment running v0.24+.

## Authentication

Kestra OSS v0.24+ requires HTTP Basic Auth by default. You need your Kestra
username (email) and password.

```bash
KESTRA_BASE_URL=http://localhost:8080 \
KESTRA_USERNAME=you@example.com \
KESTRA_PASSWORD=yourpassword \
coral source add --file ./sources/community/kestra/manifest.yaml
```

Or run interactively and be prompted for each value:

```bash
coral source add --file ./sources/community/kestra/manifest.yaml --interactive
```

| Input | Kind | Default | Description |
|---|---|---|---|
| `KESTRA_BASE_URL` | variable | `http://localhost:8080` | Base URL of your Kestra instance |
| `KESTRA_USERNAME` | secret | — | Kestra login email |
| `KESTRA_PASSWORD` | secret | — | Kestra login password |

## Tables

### `namespaces`

All namespaces in the Kestra instance. No filters required.

```sql
SELECT id FROM kestra.namespaces
```

| Column | Type | Description |
|---|---|---|
| `id` | Utf8 | Dot-separated namespace path (e.g. `company.team`) |

---

### `flows`

All flows across all namespaces. Filter by `namespace` or `flow_id` to narrow results.

```sql
-- All flows
SELECT id, namespace, revision, disabled, updated_at
FROM kestra.flows
ORDER BY namespace, id

-- Flows in a specific namespace
SELECT id, revision, disabled, label_names, label_values
FROM kestra.flows
WHERE namespace = 'dev'

-- Find disabled flows
SELECT id, namespace, updated_at
FROM kestra.flows
WHERE disabled = true
```

| Column | Type | Required filter | Description |
|---|---|---|---|
| `id` | Utf8 | | Flow ID (unique within namespace) |
| `namespace` | Utf8 | | Namespace the flow belongs to |
| `revision` | Int64 | | Revision number; higher = more recent |
| `description` | Utf8 | | Flow description (may contain Markdown) |
| `disabled` | Boolean | | `true` if the flow won't run on triggers |
| `deleted` | Boolean | | `true` if soft-deleted |
| `updated_at` | Timestamp | | Last updated time (UTC) |
| `label_names` | Utf8 | | Comma-joined label keys (e.g. `env,team`) |
| `label_values` | Utf8 | | Comma-joined label values (e.g. `prod,platform`) |

**Optional filters:** `namespace`, `flow_id`

---

### `executions`

Execution run history. `start_date` is required to bound the search window.

```sql
-- All executions since a date
SELECT id, flow_id, namespace, state, start_date, duration
FROM kestra.executions
WHERE start_date = '2026-05-01T00:00:00Z'
ORDER BY start_date DESC
LIMIT 20

-- Failed executions in a namespace
SELECT id, flow_id, state, start_date, duration
FROM kestra.executions
WHERE start_date = '2026-05-01T00:00:00Z'
  AND namespace = 'dev'
  AND state = 'FAILED'

-- Executions for a specific flow
SELECT id, state, start_date, end_date, duration, trigger_type
FROM kestra.executions
WHERE start_date = '2026-05-01T00:00:00Z'
  AND flow_id = 'my-flow'
  AND namespace = 'dev'
```

| Column | Type | Required filter | Description |
|---|---|---|---|
| `id` | Utf8 | | Execution ID |
| `flow_id` | Utf8 | | Flow that was executed |
| `namespace` | Utf8 | | Namespace of the flow |
| `flow_revision` | Int64 | | Flow revision at execution time |
| `state` | Utf8 | | Current state: SUCCESS, FAILED, RUNNING, KILLED, WARNING, PAUSED, QUEUED, RESTARTED, RETRYING |
| `start_date` | Timestamp | ✓ | Execution start time (UTC); also the required filter lower bound |
| `end_date` | Timestamp | | Execution end time (UTC); NULL if still running |
| `duration` | Utf8 | | Duration as ISO 8601 string (e.g. `PT0.94S`); NULL if still running |
| `trigger_type` | Utf8 | | What triggered the run: MANUAL, SCHEDULE, WEBHOOK, FLOW |
| `deleted` | Boolean | | Whether the execution record is soft-deleted |
| `label_names` | Utf8 | | Comma-joined label keys |
| `label_values` | Utf8 | | Comma-joined label values |

**Required filters:** `start_date` (ISO 8601 timestamp, e.g. `'2026-01-01T00:00:00Z'`)
**Optional filters:** `namespace`, `flow_id`, `state`

---

### `logs`

Task-level log entries from execution runs. `start_date` is required.

```sql
-- Recent ERROR logs
SELECT timestamp, flow_id, task_id, execution_id, message
FROM kestra.logs
WHERE start_date = '2026-05-01T00:00:00Z'
  AND level = 'ERROR'
ORDER BY timestamp DESC
LIMIT 50

-- Logs for a specific execution
SELECT timestamp, level, task_id, message
FROM kestra.logs
WHERE start_date = '2026-05-01T00:00:00Z'
  AND execution_id = 'your-execution-id'
ORDER BY timestamp

-- Logs for a specific flow
SELECT timestamp, level, execution_id, task_id, message
FROM kestra.logs
WHERE start_date = '2026-05-01T00:00:00Z'
  AND namespace = 'dev'
  AND flow_id = 'pr-reviewer'
  AND level = 'ERROR'
```

| Column | Type | Required filter | Description |
|---|---|---|---|
| `timestamp` | Timestamp | | When the log entry was emitted (UTC) |
| `level` | Utf8 | | Log level: TRACE, DEBUG, INFO, WARN, ERROR |
| `namespace` | Utf8 | | Namespace of the flow |
| `flow_id` | Utf8 | | Flow that produced this log entry |
| `task_id` | Utf8 | | Task within the flow |
| `execution_id` | Utf8 | | Execution this log entry belongs to |
| `task_run_id` | Utf8 | | Task run ID |
| `attempt_number` | Int64 | | Zero-based retry attempt number |
| `thread` | Utf8 | | Worker thread name |
| `message` | Utf8 | | Log message text |

**Required filters:** `start_date` (ISO 8601 timestamp)
**Optional filters:** `namespace`, `flow_id`, `execution_id`, `level`

> The `level` filter maps to Kestra's `minLevel` parameter — it returns entries
> at that level and above. For example, `level = 'WARN'` returns WARN and ERROR.

---

## Quick start

```bash
# 1. Add the source
KESTRA_BASE_URL=http://localhost:8080 \
KESTRA_USERNAME=you@example.com \
KESTRA_PASSWORD=yourpassword \
coral source add --file ./sources/community/kestra/manifest.yaml

# 2. Restart the server to pick up the new source
coral server stop && coral server start

# 3. Explore
coral sql "SELECT * FROM coral.tables WHERE schema_name = 'kestra'"
coral sql "SELECT id FROM kestra.namespaces"
coral sql "SELECT id, namespace, revision, disabled FROM kestra.flows ORDER BY namespace, id"
```

## Cascading queries

```text
kestra.namespaces
  → id (namespace)
    → kestra.flows WHERE namespace = '...'
      → id (flow_id), namespace
        → kestra.executions WHERE start_date = '...' AND flow_id = '...' AND namespace = '...'
          → id (execution_id)
            → kestra.logs WHERE start_date = '...' AND execution_id = '...'
```

## Notes

- Requires Kestra v0.24+ with HTTP Basic Auth enabled (the default for OSS).
- `start_date` on `executions` and `logs` maps to Kestra's `startDate` query
  parameter, which sets the **lower bound** of the search window. There is no
  upper bound filter — use `LIMIT` or `state` to constrain result size.
- The `level` filter on `logs` is a minimum-level filter (`minLevel`), not an
  exact match. `level = 'WARN'` returns WARN and ERROR entries.
- `duration` on executions is returned by Kestra as an ISO 8601 duration string
  (e.g. `PT0.94S`) and is surfaced as a `Utf8` column. Use your SQL client's
  string functions to parse it if needed.
- For Kestra Cloud or Enterprise with SSO, Basic Auth may not be available.
  Check your instance's authentication settings.
