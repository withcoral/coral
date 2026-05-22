# n8n Community Source

Query n8n workflows, executions, tags, and variables through Coral
SQL using the [n8n Public REST API](https://docs.n8n.io/api/).

## Setup

### 1. Create an n8n API key

In your n8n instance:

1. Go to **Settings** > **API**
2. Click **Generate API Key**
3. Copy the key

For n8n Cloud, your API base URL is `https://<account>.n8n.cloud/api/v1`.
For self-hosted, it is `http://localhost:5678/api/v1` (or your custom domain).

> **Note:** The n8n API is not available during the free trial. You need a
> paid or self-hosted instance.

### 2. Add the source

```bash
export N8N_BASE_URL="https://<your-instance>.n8n.cloud/api/v1"
export N8N_API_KEY="<your-api-key>"
coral source add --file sources/community/n8n/manifest.yaml
```

Or use interactive mode:

```bash
coral source add --interactive --file sources/community/n8n/manifest.yaml
```

### 3. Verify

```bash
coral source test n8n
```

## Tables

### `n8n.workflows`

List automation workflows.

| Column | Type | Description |
|---|---|---|
| `id` | Utf8 | Workflow ID |
| `name` | Utf8 | Workflow name |
| `active` | Boolean | Whether the workflow is published/active |
| `version_id` | Utf8 | Current version ID |
| `node_count` | Int64 | Number of nodes in the workflow |
| `tag_names` | Utf8 | Comma-separated tag names |
| `project_id` | Utf8 | Owning project ID |
| `project_name` | Utf8 | Owning project name |
| `created_at` | Timestamp | Creation time |
| `updated_at` | Timestamp | Last update time |
| `settings` | Json | Workflow settings object |

**Optional filters:** `active`, `tags`, `name`, `project_id`

### `n8n.executions`

List workflow execution runs.

| Column | Type | Description |
|---|---|---|
| `id` | Utf8 | Execution ID |
| `finished` | Boolean | Whether the execution finished |
| `mode` | Utf8 | Execution mode (trigger, manual, integrated) |
| `workflow_id` | Utf8 | Executed workflow ID |
| `started_at` | Timestamp | Start time |
| `stopped_at` | Timestamp | Stop time (NULL if running) |
| `wait_till` | Timestamp | When a waiting execution should resume |
| `retry_of` | Utf8 | Original execution ID if this is a retry |
| `retry_success_id` | Utf8 | Successful retry execution ID |

**Optional filters:** `status`, `workflow_id`, `project_id`

### `n8n.tags`

List workflow tags.

| Column | Type | Description |
|---|---|---|
| `id` | Utf8 | Tag ID |
| `name` | Utf8 | Tag name |
| `created_at` | Timestamp | Creation time |
| `updated_at` | Timestamp | Last update time |

### `n8n.variables`

List instance variables.

| Column | Type | Description |
|---|---|---|
| `id` | Utf8 | Variable ID |
| `key` | Utf8 | Variable key/name |
| `value` | Utf8 | Variable value |
| `type` | Utf8 | Data type (string, number, boolean) |

**Optional filter:** `project_id`

## Functions

### `n8n.executions_by_workflow`

Search executions for a specific workflow.

```sql
SELECT id, finished, mode, started_at
FROM n8n.executions_by_workflow(
  workflow_id => 'YOUR_WORKFLOW_ID_HERE'
)
LIMIT 10;
```

## Example queries

```sql
-- List active workflows with their tags
SELECT name, tag_names, node_count, updated_at
FROM n8n.workflows
WHERE active = true
ORDER BY updated_at DESC;

-- Find executions that did not finish in the last 24 hours
SELECT id, workflow_id, mode, started_at
FROM n8n.executions
WHERE finished = false
  AND started_at > NOW() - INTERVAL '24 hours'
ORDER BY started_at DESC;

-- Join executions to workflows for full context
SELECT
  w.name AS workflow_name,
  w.active AS workflow_active,
  e.finished,
  e.mode,
  e.started_at
FROM n8n.workflows w
JOIN n8n.executions e ON w.id = e.workflow_id
WHERE e.finished = false
  AND e.started_at > NOW() - INTERVAL '7 days'
ORDER BY e.started_at DESC
LIMIT 20;

-- Count executions by finished state per workflow
SELECT
  w.name AS workflow_name,
  e.finished,
  COUNT(*) AS execution_count
FROM n8n.workflows w
JOIN n8n.executions e ON w.id = e.workflow_id
WHERE e.started_at > NOW() - INTERVAL '30 days'
GROUP BY w.name, e.finished
ORDER BY execution_count DESC;

-- Find workflows using a specific tag
SELECT name, tag_names, active
FROM n8n.workflows
WHERE tag_names ILIKE '%production%';

-- Cross-source join: correlate n8n failures with PagerDuty incidents
SELECT
  w.name AS workflow_name,
  e.started_at,
  e.mode
FROM n8n.workflows w
JOIN n8n.executions e ON w.id = e.workflow_id
LEFT JOIN pagerduty.incidents p
  ON p.created_at BETWEEN e.started_at AND e.started_at + INTERVAL '5 minutes'
WHERE e.finished = false
  AND e.started_at > NOW() - INTERVAL '24 hours';
```

## Validation

```bash
export N8N_BASE_URL="https://<your-instance>.n8n.cloud/api/v1"
export N8N_API_KEY="<your-api-key>"
coral source lint sources/community/n8n/manifest.yaml
coral source add --file sources/community/n8n/manifest.yaml
coral source test n8n
coral sql "SELECT * FROM coral.tables WHERE schema_name = 'n8n'"
coral sql "SELECT column_name, data_type FROM coral.columns WHERE schema_name = 'n8n' AND table_name = 'workflows'"
coral sql "SELECT id, name, active FROM n8n.workflows LIMIT 5"
```

## Limitations

- **Read-only.** This source does not create, update, activate, deactivate,
  or delete any n8n resources.
- **No execution data.** The `includeData` parameter is always set to `false`
  to avoid extremely large responses. Execution metadata (status, timing,
  errors) is available.
- **API availability.** The n8n Public API requires a paid or self-hosted
  instance. It is not available during the n8n Cloud free trial.
- **Credentials endpoint.** The `/credentials` endpoint is not included because
  it returns `405 Method Not Allowed` on standard n8n self-hosted instances.
  Credential metadata may be added in a future version if the API behavior
  changes.

## Out of scope for v1

- Workflow create/update/delete operations
- Execution retry or stop operations
- Tag management
- Variable create/update/delete operations
- Source control operations
- Data table operations
- User management
- Audit log generation
