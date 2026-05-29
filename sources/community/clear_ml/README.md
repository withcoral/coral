# ClearML

Query ClearML MLOps metadata and experiment observability data from the ClearML REST API.

This source treats ClearML as an experiment metadata, model registry, pipeline observability, worker, and queue visibility platform. It does not execute training, run pipelines, control workers, mutate queues, or download artifacts.

## Setup

### Configure ClearML API Access

ClearML REST API requests use a bearer token. ClearML access keys and secret keys are used first to call `GET /auth.login`, which returns a token for subsequent API calls.

Set your API host:

```bash
export CLEARML_API_HOST="https://api.clear.ml"
```

For self-hosted ClearML, use the `api_server` value from `clearml.conf`, for example:

```bash
export CLEARML_API_HOST="https://clearml-api.example.com"
```

Generate a bearer token:

```bash
export CLEARML_API_TOKEN="$(
  curl -fsS -u "$CLEARML_API_ACCESS_KEY:$CLEARML_API_SECRET_KEY" \
    "$CLEARML_API_HOST/auth.login" | jq -r '.data.token'
)"
```

By default, ClearML tokens expire after 30 days. You can request a shorter lifetime through the `auth.login` `expiration_sec` parameter.

### Add the Source

```bash
coral source add clear_ml
```

When prompted, provide:

- `CLEARML_API_HOST`
- `CLEARML_API_TOKEN`

## Tables

### `current_user`

Returns the current authenticated ClearML user context when the server exposes `users.get_current_user`.

**Useful for:**

- Credential validation
- User and company/workspace identification
- Understanding which account the token belongs to

Some older or minimal self-hosted ClearML servers may not expose the users service. Use `clear_ml.projects` as the most portable smoke-test table.

### `projects`

ClearML project metadata.

**Useful for:**

- Project inventory
- Finding `project_id` values for scoped tables
- Understanding project tags and update timestamps

### `tasks`

Task and experiment metadata for a single project.

**Requires:** `project_id`

**Useful for:**

- Experiment inventory
- Failed/completed/queued task analysis
- Training, evaluation, inference, and pipeline-controller inspection
- Hyperparameter, configuration, execution, script, runtime, and tag review

### `models`

Model registry metadata for a single project.

**Requires:** `project_id`

**Useful for:**

- Model inventory
- Framework analysis
- Model-to-task linkage
- Ready/final model inspection

### `pipelines`

Pipeline controller task metadata for a single project.

**Requires:** `project_id`

ClearML pipelines are represented as task objects with `type = controller`.

**Useful for:**

- Pipeline controller inventory
- Pipeline status review
- Pipeline execution metadata inspection

### `workers`

Worker and agent metadata.

**Useful for:**

- Agent inventory
- Worker tag review
- Queue association inspection

This table is read-only and does not register, unregister, or control workers.

### `queues`

Execution queue metadata.

**Useful for:**

- Queue inventory
- Queue tag and metadata review
- Inspecting returned queue entries

Queue entries are exposed as JSON. Use `json_length(entries)` to estimate pending task count for queues where entries were returned.

### `artifacts`

Task artifact metadata for one task.

**Requires:** `task_id`

**Useful for:**

- Artifact inventory for a task
- Artifact type and URI review
- Artifact metadata inspection

This table exposes metadata only. It does not download binary artifact payloads.

## Authentication

This source sends:

```text
Authorization: Bearer <CLEARML_API_TOKEN>
Content-Type: application/json
```

ClearML documents the initial login flow as:

```bash
curl -u "<access_key>:<secret_key>" -X GET "$CLEARML_API_HOST/auth.login"
```

Use the returned `.data.token` as `CLEARML_API_TOKEN`.

## Limits

- This source is read-only.
- ClearML access key and secret key are not stored directly by this source.
- Token refresh is outside the v1 manifest scope.
- List tables are capped at 100 rows per request in v1.
- Project-scoped tables require `project_id` to avoid broad scans.
- Artifact metadata requires `task_id`.
- Raw logs and event streams are out of scope for v1.
- Artifact downloads are out of scope for v1.
- Task execution, task cloning, pipeline execution, worker control, and queue mutation are out of scope.

## Example Queries

### Count tasks by status

```sql
SELECT status, COUNT(*) AS task_count
FROM clear_ml.tasks
WHERE project_id = 'your-project-id'
GROUP BY status
ORDER BY task_count DESC
```

### List failed tasks

```sql
SELECT task_id, name, type, status, last_update
FROM clear_ml.tasks
WHERE project_id = 'your-project-id'
  AND status = 'failed'
ORDER BY last_update DESC
LIMIT 50
```

### Count models by framework

```sql
SELECT framework, COUNT(*) AS model_count
FROM clear_ml.models
WHERE project_id = 'your-project-id'
GROUP BY framework
ORDER BY model_count DESC
```

### Inspect queues

```sql
SELECT queue_name, display_name, json_length(entries) AS returned_entries
FROM clear_ml.queues
ORDER BY queue_name
```

### Inspect task artifacts

```sql
SELECT artifact_name, artifact_type, uri
FROM clear_ml.artifacts
WHERE task_id = 'your-task-id'
ORDER BY artifact_name
```

## Notes

- Use `clear_ml.projects` first to find project IDs.
- Use `clear_ml.tasks` to find task IDs for `clear_ml.artifacts`.
- JSON columns preserve nested ClearML metadata without flattening every provider-specific field.
- Timestamps are stored as proper Timestamp columns derived from ClearML ISO 8601 strings.
