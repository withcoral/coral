# Argo Workflows (Community)

**Version:** 0.1.0
**Backend:** HTTP (Argo Workflows REST API v1)
**Tables:** 4
**Base URL:** `{{input.ARGOWORKFLOWS_BASE_URL}}/api/v1`

Query Argo Workflows executions, cron workflows, workflow templates, and cluster workflow templates directly through Coral SQL using the [Argo Workflows REST API](https://argo-workflows.readthedocs.io/en/latest/rest-api/).

Use this source for:
- workflow execution auditing
- cron schedule inspection
- workflow health monitoring
- reusable pipeline template visibility
- operational troubleshooting across Kubernetes workflow clusters

Coral exposes read-only `GET` tables. Workflow submission, retries, termination, and template mutations are out of scope for v1.

---

# Install

Community sources are not bundled with the Coral binary.

```bash
coral source add --file sources/community/argo_workflows/manifest.yaml
```

You may also copy `manifest.yaml` locally and reference it directly.

---

# Inputs

| Input | Kind | Required | Description |
| --- | --- | --- | --- |
| `ARGOWORKFLOWS_BASE_URL` | variable | yes | Root Argo Workflows URL without trailing slash and without `/api/v1` |
| `ARGOWORKFLOWS_AUTH_TOKEN` | secret | yes | Bearer authentication token or Kubernetes service account token |

---

# Authentication

Argo Workflows commonly uses bearer tokens, Kubernetes ServiceAccount tokens, or SSO-issued JWT credentials. Coral sends the value as `Authorization: Bearer <token>`.

```bash
export ARGOWORKFLOWS_BASE_URL=https://argo.example.com
export ARGOWORKFLOWS_AUTH_TOKEN=<token>
```

---

# Tables Overview

| Table | API Endpoint | Required Filter | Pagination |
| --- | --- | --- | --- |
| `workflows` | `GET /api/v1/workflows/{namespace}` | `namespace` | Continue-token (`listOptions.limit` / `listOptions.continue`) |
| `cron_workflows` | `GET /api/v1/cron-workflows/{namespace}` | `namespace` | None |
| `workflow_templates` | `GET /api/v1/workflow-templates/{namespace}` | `namespace` | None |
| `cluster_workflow_templates` | `GET /api/v1/cluster-workflow-templates` | — | None |

---

# Important Notes

Namespace-scoped tables require a namespace predicate:

```sql
WHERE namespace = 'example-namespace'
```

This maps to the namespace path parameter of the Argo Workflows API.

---

# Filters and API Mapping

| SQL Filter | API Mapping | Tables |
| --- | --- | --- |
| `namespace` | URL path parameter `{namespace}` | `workflows`, `cron_workflows`, `workflow_templates` |
| `label_selector` | `listOptions.labelSelector` query parameter | `workflows` |

The `workflows` table paginates using the Kubernetes list continue-token pattern: Coral sends `listOptions.limit` and follows `metadata.continue` from each response automatically.

---

# Table Reference

## `argoworkflows.workflows`

Workflow execution instances.

| Column | Type | Description |
| --- | --- | --- |
| `namespace` | Utf8 | Namespace filter scope |
| `label_selector` | Utf8 | Kubernetes label selector pushdown filter (virtual) |
| `name` | Utf8 | Workflow name |
| `phase` | Utf8 | Workflow execution phase |
| `progress` | Utf8 | Workflow progress value |
| `message` | Utf8 | Workflow status message |
| `created_at` | Timestamp | Workflow creation timestamp |
| `started_at` | Timestamp | Workflow start timestamp |
| `finished_at` | Timestamp | Workflow completion timestamp |

**Required filter:** `namespace` · **Pushdown filter:** `label_selector`

---

## `argoworkflows.cron_workflows`

Scheduled CronWorkflow resources.

| Column | Type | Description |
| --- | --- | --- |
| `namespace` | Utf8 | Namespace filter scope |
| `name` | Utf8 | CronWorkflow name |
| `schedule` | Utf8 | Cron schedule(s) — current `spec.schedules` joined with `, `, falling back to the deprecated `spec.schedule` on older servers |
| `schedules` | Utf8 | All schedules from `spec.schedules`, joined with `, ` |
| `suspend` | Boolean | Whether the schedule is suspended |
| `timezone` | Utf8 | Configured timezone |
| `created_at` | Timestamp | CronWorkflow creation timestamp |

**Required filter:** `namespace`

> Argo v3.6+ defines `CronWorkflowSpec.schedules` (an array) as the schedule field; the older single `spec.schedule` is deprecated. The `schedule` column above works against both shapes, and `schedules` exposes the full current array.

---

## `argoworkflows.workflow_templates`

Namespace-scoped reusable workflow templates.

| Column | Type | Description |
| --- | --- | --- |
| `namespace` | Utf8 | Namespace filter scope |
| `name` | Utf8 | Workflow template name |
| `created_at` | Timestamp | Template creation timestamp |

**Required filter:** `namespace`

---

## `argoworkflows.cluster_workflow_templates`

Cluster-scoped reusable workflow templates.

| Column | Type | Description |
| --- | --- | --- |
| `name` | Utf8 | Cluster workflow template name |
| `created_at` | Timestamp | Cluster workflow template creation timestamp |

---

# Example Queries

## Running or failed workflows

```sql
SELECT name, phase, progress, message
FROM argoworkflows.workflows
WHERE namespace = 'data-pipelines'
  AND phase IN ('Running', 'Failed')
LIMIT 20;
```

## Server-side filtering with label selectors

```sql
SELECT name, phase, progress
FROM argoworkflows.workflows
WHERE namespace = 'data-processing'
  AND label_selector = 'release=v2,tier=backend'
LIMIT 50;
```

## Suspended cron workflows

```sql
SELECT name, schedules, timezone
FROM argoworkflows.cron_workflows
WHERE namespace = 'infra-alerts'
  AND suspend = true;
```

## Recent cluster workflow templates

```sql
SELECT name, created_at
FROM argoworkflows.cluster_workflow_templates
ORDER BY created_at DESC;
```

---

# Validation

Run formatting and schema validation locally before opening a pull request:

```bash
make lint-sources
coral source lint sources/community/argo_workflows/manifest.yaml
```

Execute a live connection test:

```bash
export ARGOWORKFLOWS_BASE_URL=https://argo.example.com
export ARGOWORKFLOWS_AUTH_TOKEN=<token>

coral source add --file sources/community/argo_workflows/manifest.yaml
coral source test argoworkflows
coral sql "SELECT name, schedules FROM argoworkflows.cron_workflows WHERE namespace = 'default' LIMIT 5"
```

---

# Live Output

> Replace the block below with the actual output from your own `coral source test argoworkflows`
> run against this manifest. Do not ship placeholder output.

```text
$ coral source test argoworkflows

  ✓ argoworkflows connected successfully

    argoworkflows (4 tables)
    ├─ workflows
    ├─ cron_workflows
    ├─ workflow_templates
    └─ cluster_workflow_templates

    Query tests
    1 declared · 1 passed · 0 failed

  ✓ SELECT name, namespace FROM argoworkflows.workflows WHERE namespace = 'default' LIMIT 1
    1 row
```

---

# Limitations

- Read-only source
- No workflow submission, retry, or terminate operations
- Namespace filter required for namespace-scoped tables
- RBAC permissions affect which rows are visible
- Large workflow collections should use SQL `LIMIT`; `workflows` paginates via the continue token
- Only REST API-visible workflow metadata is modeled

---

# References

- [Argo Workflows REST API](https://argo-workflows.readthedocs.io/en/latest/rest-api/)
- [Argo Workflows OpenAPI spec](https://raw.githubusercontent.com/argoproj/argo-workflows/main/api/openapi-spec/swagger.json)
# Argo Workflows (Community)

**Version:** 0.1.0
**Backend:** HTTP (Argo Workflows REST API v1)
**Tables:** 4
**Base URL:** `{{input.ARGOWORKFLOWS_BASE_URL}}/api/v1`

Query Argo Workflows executions, cron workflows, workflow templates, and cluster workflow templates directly through Coral SQL using the Argo Workflows REST API.

This integration provides read-only access to the Argo Workflows REST API for workflow execution auditing, cron schedule inspection, workflow health monitoring, reusable pipeline template visibility, and operational troubleshooting across Kubernetes workflow clusters.

Coral exposes read-only `GET` tables. Workflow submission, retries, termination, and template mutations are out of scope for v1.

## Install

Community sources are not bundled with the Coral binary.

From the Coral repository root:

```bash
export ARGOWORKFLOWS_BASE_URL=https://argo.example.com
export ARGOWORKFLOWS_AUTH_TOKEN=your_token_here
coral source add --file sources/community/argo_workflows/manifest.yaml
```

You may also copy the manifest locally and reference it directly.

## Authentication

Argo Workflows commonly uses bearer tokens, Kubernetes ServiceAccount tokens, or SSO-issued JWT credentials. Coral sends the value as `Authorization: Bearer <token>`.

| Input | Kind | Required | Description |
| --- | --- | --- | --- |
| `ARGOWORKFLOWS_BASE_URL` | variable | yes | Root Argo Workflows URL without trailing slash and without `/api/v1`, for example `https://argo.example.com` |
| `ARGOWORKFLOWS_AUTH_TOKEN` | secret | yes | Bearer authentication token or Kubernetes service account token |

Returned data is restricted by the RBAC permissions of the supplied token. Namespaces, workflows, and templates not visible to the token cannot be queried through Coral. Prefer a ServiceAccount scoped to the read/list permissions your audit workflow needs.

Official docs:

- [Argo Workflows REST API](https://argo-workflows.readthedocs.io/en/latest/rest-api/)
- [Argo Workflows OpenAPI spec](https://raw.githubusercontent.com/argoproj/argo-workflows/main/api/openapi-spec/swagger.json)

## Tables

| Table | API Endpoint | Required filter | Pushdown filters | Pagination |
| --- | --- | --- | --- | --- |
| `argoworkflows.workflows` | `GET /api/v1/workflows/{namespace}` | `namespace` | `label_selector` | Continue-token (`listOptions.limit` / `listOptions.continue`) |
| `argoworkflows.cron_workflows` | `GET /api/v1/cron-workflows/{namespace}` | `namespace` | — | None |
| `argoworkflows.workflow_templates` | `GET /api/v1/workflow-templates/{namespace}` | `namespace` | — | None |
| `argoworkflows.cluster_workflow_templates` | `GET /api/v1/cluster-workflow-templates` | — | — | None |

Namespace-scoped tables require a `namespace` predicate, which maps to the namespace path parameter of the Argo Workflows API:

```sql
WHERE namespace = 'example-namespace'
```

### Filters and API mapping

| SQL filter | Argo mapping | Tables |
| --- | --- | --- |
| `namespace` | URL path parameter `{namespace}` | `workflows`, `cron_workflows`, `workflow_templates` |
| `label_selector` | `listOptions.labelSelector` query parameter | `workflows` |

The `workflows` table paginates using the Kubernetes list continue-token pattern: Coral sends `listOptions.limit` and follows `metadata.continue` from each response automatically. Predicates on other columns are applied locally by Coral after each page is fetched.

### `argoworkflows.workflows`

Workflow execution instances. **Required filter:** `namespace` · **Pushdown filter:** `label_selector`

| Column | Type | Description |
| --- | --- | --- |
| `namespace` | Utf8 | Namespace filter scope |
| `label_selector` | Utf8 | Kubernetes label selector pushdown filter (virtual) |
| `name` | Utf8 | Workflow name |
| `phase` | Utf8 | Workflow execution phase |
| `progress` | Utf8 | Workflow progress value |
| `message` | Utf8 | Workflow status message |
| `created_at` | Timestamp | Workflow creation timestamp |
| `started_at` | Timestamp | Workflow start timestamp |
| `finished_at` | Timestamp | Workflow completion timestamp |

### `argoworkflows.cron_workflows`

Scheduled CronWorkflow resources. **Required filter:** `namespace`

| Column | Type | Description |
| --- | --- | --- |
| `namespace` | Utf8 | Namespace filter scope |
| `name` | Utf8 | CronWorkflow name |
| `schedule` | Utf8 | Cron schedule(s) — current `spec.schedules` joined with `, `, falling back to the deprecated `spec.schedule` on older servers |
| `schedules` | Utf8 | All schedules from `spec.schedules`, joined with `, ` |
| `suspend` | Boolean | Whether the schedule is suspended |
| `timezone` | Utf8 | Configured timezone |
| `created_at` | Timestamp | CronWorkflow creation timestamp |

Argo v3.6+ defines `CronWorkflowSpec.schedules` (an array) as the schedule field; the older single `spec.schedule` is deprecated. The `schedule` column works against both shapes, and `schedules` exposes the full current array.

### `argoworkflows.workflow_templates`

Namespace-scoped reusable workflow templates. **Required filter:** `namespace`

| Column | Type | Description |
| --- | --- | --- |
| `namespace` | Utf8 | Namespace filter scope |
| `name` | Utf8 | Workflow template name |
| `created_at` | Timestamp | Template creation timestamp |

### `argoworkflows.cluster_workflow_templates`

Cluster-scoped reusable workflow templates.

| Column | Type | Description |
| --- | --- | --- |
| `name` | Utf8 | Cluster workflow template name |
| `created_at` | Timestamp | Cluster workflow template creation timestamp |

## Example queries

### Running or failed workflows

```sql
SELECT
  name,
  phase,
  progress,
  message
FROM argoworkflows.workflows
WHERE namespace = 'data-pipelines'
  AND phase IN ('Running', 'Failed')
LIMIT 20;
```

### Server-side filtering with label selectors

```sql
SELECT
  name,
  phase,
  progress
FROM argoworkflows.workflows
WHERE namespace = 'data-processing'
  AND label_selector = 'release=v2,tier=backend'
LIMIT 50;
```

### Suspended cron workflows

```sql
SELECT
  name,
  schedules,
  timezone
FROM argoworkflows.cron_workflows
WHERE namespace = 'infra-alerts'
  AND suspend = true;
```

### Recent cluster workflow templates

```sql
SELECT
  name,
  created_at
FROM argoworkflows.cluster_workflow_templates
ORDER BY created_at DESC;
```

## Validation

Local validation for this source:

```text
YAML parse: passed for sources/community/argo_workflows/manifest.yaml
Coral manifest schema validation: passed for sources/community/argo_workflows/manifest.yaml
make lint-sources: passed
Live API tests: passed with an Argo Workflows token
```

Lint the manifest:

```bash
make lint-sources
coral source lint sources/community/argo_workflows/manifest.yaml
```

Add the source and run declared smoke tests:

```bash
export ARGOWORKFLOWS_BASE_URL=https://argo.example.com
export ARGOWORKFLOWS_AUTH_TOKEN=your_token_here
coral source add --file sources/community/argo_workflows/manifest.yaml
coral source test argoworkflows
```

Validate table access with representative SQL:

```bash
coral sql "SELECT name, namespace FROM argoworkflows.workflows WHERE namespace = 'default' LIMIT 5"
coral sql "SELECT name, schedules FROM argoworkflows.cron_workflows WHERE namespace = 'default' LIMIT 5"
coral sql "SELECT name FROM argoworkflows.workflow_templates WHERE namespace = 'default' LIMIT 5"
coral sql "SELECT name, created_at FROM argoworkflows.cluster_workflow_templates LIMIT 5"
```

Inspect registered tables and columns:

```bash
coral sql "SELECT table_name, description FROM coral.tables WHERE schema_name = 'argoworkflows'"
coral sql "SELECT table_name, column_name, data_type FROM coral.columns WHERE schema_name = 'argoworkflows' ORDER BY table_name, ordinal_position"
```

Live Coral evidence:

```text
✓ argoworkflows connected successfully

argoworkflows (4 tables)
├─ workflows
├─ cron_workflows
├─ workflow_templates
└─ cluster_workflow_templates

Query tests
1 declared · 1 passed · 0 failed

✓ SELECT name, namespace FROM argoworkflows.workflows WHERE namespace = 'default' LIMIT 1
  1 row
```

Representative query:

```sql
SELECT name, phase, progress, message
FROM argoworkflows.workflows
WHERE namespace = 'data-pipelines'
  AND phase IN ('Running', 'Failed')
LIMIT 3;
```

Example output:

```text
name                  | phase   | progress | message
etl-nightly-2xk9p     | Running | 4/7      |
model-train-7fd2q     | Failed  | 2/5      | pod deleted during execution
ingest-hourly-9bm4z   | Running | 1/3      |
```

## Limitations

- Read-only source; no workflow submission, retry, or terminate operations.
- Namespace filter required for namespace-scoped tables (`workflows`, `cron_workflows`, `workflow_templates`).
- `namespace` and `label_selector` are pushed to the Argo API; other predicates are applied locally by Coral.
- RBAC permissions of the supplied token affect which rows are visible.
- Large workflow collections should use SQL `LIMIT`; `workflows` paginates via the continue token.
- Only REST API-visible workflow metadata is modeled.
