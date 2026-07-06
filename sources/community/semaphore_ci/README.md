# Semaphore CI Source

Query CI/CD data from [Semaphore CI](https://semaphoreci.com) — workflows,
pipelines, and promotions for your projects. All tables require a
`project_id` or `pipeline_id` filter — Semaphore's v1alpha API does not
expose a list-projects endpoint, so obtain IDs from the Semaphore
dashboard or `sem` CLI. Read-only; cloud-only (assumes `*.semaphoreci.com`).

API docs: https://docs.semaphoreci.com/reference/api

## Setup

### 1. Get your API token

Generate an API token from your Semaphore account:
[Account Settings → API Tokens](https://me.semaphoreci.com/account).

### 2. Configure environment variables

```bash
export SEMAPHORE_API_TOKEN="your-semaphore-api-token"
export SEMAPHORE_ORG_SLUG="mycompany"
```

The org slug is the subdomain part of your Semaphore dashboard URL.
For example, if your URL is `https://mycompany.semaphoreci.com`, your slug
is `mycompany`.

### 3. Add the source

```bash
coral source add --file sources/community/semaphore_ci/manifest.yaml
```

## Authentication

| Input | Kind | Description |
|---|---|---|
| `SEMAPHORE_API_TOKEN` | Secret | Semaphore CI API token |
| `SEMAPHORE_ORG_SLUG` | Variable | Organization slug (subdomain) |

Auth uses `Authorization: Token <TOKEN>` per the Semaphore v1alpha API spec.
The documented `User-Agent: SemaphoreCI v2.0 Client` header is sent
automatically via `request_headers`.

## Tables

> **Note:** The Semaphore v1alpha API does not expose a list-projects
> endpoint. To get your `project_id`, use the Semaphore dashboard URL
> (visible in project settings) or the CLI: `sem get project <name>`.
> All tables below require a `project_id` or `pipeline_id` filter.

### workflows

Workflow runs for a project. Each row is one workflow triggered by a push,
tag, pull request, or manual rerun.

| Column | Type | Description |
|---|---|---|
| `project_id` | Utf8 | Project ID (required filter) |
| `wf_id` | Utf8 | Workflow UUID |
| `requester_id` | Utf8 | User or system that requested this workflow |
| `repository_id` | Utf8 | Repository UUID |
| `organization_id` | Utf8 | Organization UUID |
| `initial_ppl_id` | Utf8 | Initial pipeline UUID — use this for promotions |
| `hook_id` | Utf8 | Webhook event UUID |
| `branch_name` | Utf8 | Git branch name |
| `branch_id` | Utf8 | Semaphore branch UUID |
| `commit_sha` | Utf8 | Git commit SHA |
| `triggered_by` | Int64 | Trigger source (integer enum, e.g. 1 = hook) |
| `rerun_of` | Utf8 | Original workflow UUID if rerun |
| `created_at` | Timestamp | Workflow creation time (UTC) |

**Required filter:** `project_id`
**Optional filters:** `branch_name`, `created_after`, `created_before`
**Pagination:** Link header

---

### pipelines

Pipelines for a project or workflow. Each row is one pipeline execution
within a workflow. This Coral source requires `project_id` for a safe
default query path; Semaphore's API also accepts `wf_id` alone, but
this source always requires `project_id`. Optionally pass `wf_id` to
narrow to a single workflow.

> **Important:** The pipeline list endpoint does **not** return `ppl_id`.
> To get a pipeline ID for the promotions table, use `initial_ppl_id`
> from `semaphore_ci.workflows`.

| Column | Type | Description |
|---|---|---|
| `project_id` | Utf8 | Project ID (required filter) |
| `name` | Utf8 | Pipeline name from YAML config |
| `yaml_file_name` | Utf8 | YAML file defining this pipeline |
| `working_directory` | Utf8 | Pipeline working directory |
| `wf_id` | Utf8 | Parent workflow UUID |
| `state` | Utf8 | State (pending, queuing, running, stopping, done) |
| `result` | Utf8 | Result (passed, failed, stopped, canceled) |
| `branch_name` | Utf8 | Git branch name |
| `created_at` | Timestamp | Pipeline creation time (UTC) |

**Required filter:** `project_id` (Coral source restriction — Semaphore's
API also accepts `wf_id` alone)
**Optional filters:** `wf_id`, `branch_name`, `yml_file_path`,
`created_after`, `created_before`, `done_after`, `done_before`
**Pagination:** Link header

> **Note:** Richer fields like `result_reason`, `error_description`,
> `pending_at`, `running_at`, `done_at`, etc. are only available via
> the describe endpoint (`GET /pipelines/:pipeline_id`), which is not
> covered by this list table. Those timestamps also use a custom string
> format (`YYYY-MM-DD HH:MM:SS.ffffffZ`), not the `{seconds, nanos}`
> object used by the list endpoint.

---

### promotions

Promotions triggered from a pipeline (e.g. deploy to staging/production).

| Column | Type | Description |
|---|---|---|
| `pipeline_id` | Utf8 | Pipeline ID (required filter) |
| `name` | Utf8 | Promotion name (e.g. production) |
| `status` | Utf8 | Promotion status (passed, failed) |
| `triggered_by` | Utf8 | What triggered the promotion |

**Required filter:** `pipeline_id`
**Pagination:** None

> **Note:** The promotions list endpoint only returns `name`, `status`,
> and `triggered_by`. Timestamp and auto-promotion metadata are not
> exposed by this endpoint. Semaphore returns a server error (500) for
> pipelines with no promotions configured rather than an empty array.

---

## Typical Query Flow

Since there is no projects list endpoint and the pipeline list does not
return `ppl_id`, the typical workflow is:

1. **Get your project ID** from the Semaphore UI or CLI
2. **Query workflows** for that project — this gives you `initial_ppl_id`
3. **Drill into pipelines** using the project ID or a specific workflow ID
4. **Check promotions** using `initial_ppl_id` from step 2 as `pipeline_id`

## Example Queries

```sql
-- List recent workflows for a project
SELECT wf_id, branch_name, commit_sha, initial_ppl_id, created_at
FROM semaphore_ci.workflows
WHERE project_id = 'your-project-uuid'
LIMIT 10;

-- Find failed pipelines (use LOWER for case-safe comparison)
SELECT name, state, result, branch_name, created_at
FROM semaphore_ci.pipelines
WHERE project_id = 'your-project-uuid'
  AND LOWER(result) = 'failed';

-- Filter pipelines by workflow ID
SELECT name, state, result, yaml_file_name
FROM semaphore_ci.pipelines
WHERE project_id = 'your-project-uuid'
  AND wf_id = 'your-workflow-uuid';

-- Find pipelines by branch name
SELECT name, state, result, created_at
FROM semaphore_ci.pipelines
WHERE project_id = 'your-project-uuid'
  AND branch_name = 'main';

-- List promotions using initial_ppl_id from workflows
SELECT name, status, triggered_by
FROM semaphore_ci.promotions
WHERE pipeline_id = 'initial-ppl-id-from-workflows';
```

## Pagination

| Table | Mode | Default Page Size |
|---|---|---|
| `workflows` | Link header | 30 |
| `pipelines` | Link header | 30 |
| `promotions` | None | — |

## Notes

- **No projects endpoint**: The Semaphore v1alpha API does not provide a
  `GET /projects` endpoint. Obtain your `project_id` from the Semaphore
  dashboard (Project Settings) or CLI (`sem get project`).
- **No `ppl_id` in pipeline list**: The pipeline list endpoint does not
  return pipeline IDs. Use `initial_ppl_id` from the workflows table to
  obtain the initial pipeline ID for promotions queries.
- **Cloud-only**: The `base_url` assumes `*.semaphoreci.com`. Semaphore
  Enterprise users on a custom domain would need to fork and adjust.
- **Read-only**: This source only uses GET endpoints. No create, update,
  or delete operations.
- **Timestamps**: The pipeline list endpoint returns `created_at` as a
  protobuf-style `{seconds, nanos}` object. The describe endpoint
  (`GET /pipelines/:pipeline_id`) uses a different custom string format
  (`YYYY-MM-DD HH:MM:SS.ffffffZ`). This source only covers the list
  endpoint.
- **Case inconsistency**: The `state` and `result` fields may use different
  casing between API responses (e.g. `DONE` vs `done`, `FAILED` vs
  `failed`). Use `LOWER(result)` or `UPPER(state)` for reliable filtering.
- **Pipelines require `project_id`**: This is a Coral source restriction
  for a safe first-query experience. Semaphore's API contract accepts
  either `project_id` or `wf_id` (or both); this source always requires
  `project_id` and treats `wf_id` as an optional narrowing filter.
- **Promotions 500 on no-promotion pipelines**: Semaphore returns a
  server error (500) for pipelines that have no promotions configured.
  This is an upstream API behavior, not a source bug.
- **API docs**: https://docs.semaphoreci.com/reference/api
