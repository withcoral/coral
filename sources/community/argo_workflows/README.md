# Argo Workflows (Community)

**Version:** 0.1.0
**Backend:** HTTP (Argo Workflows REST API v1)
**Tables:** 4
**Base URL:** `{{input.ARGOWORKFLOWS_BASE_URL}}/api/v1`

Query Argo Workflows executions, cron workflows, workflow templates, and cluster workflow templates directly through Coral SQL using the [Argo Workflows REST API](https://argoproj.github.io/argo-workflows/swagger/).

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
coral source add --file sources/community/argo-workflows/manifest.yaml
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

Argo Workflows commonly uses:
- Bearer tokens
- Kubernetes ServiceAccount tokens
- SSO-issued JWT credentials

Example:

```bash
export ARGOWORKFLOWS_BASE_URL=https://argo.example.com
export ARGOWORKFLOWS_AUTH_TOKEN=<token>
```

---

# Tables Overview

| Table | API Endpoint | Required Filter |
| --- | --- | --- |
| `workflows` | `GET /api/v1/workflows/{namespace}` | `namespace` |
| `cron_workflows` | `GET /api/v1/cron-workflows/{namespace}` | `namespace` |
| `workflow_templates` | `GET /api/v1/workflow-templates/{namespace}` | `namespace` |
| `cluster_workflow_templates` | `GET /api/v1/cluster-workflow-templates` | — |

---

# Important Notes

Namespace-scoped tables require:

```sql
WHERE namespace = 'example-namespace'
```

This aligns with Argo Workflows namespace-scoped API requirements.

---

# Filters and API Mapping

Coral maps declared SQL filters directly to Argo Workflows API parameters.

| SQL Filter | API Mapping | Tables |
| --- | --- | --- |
| `namespace` | URL path parameter `{namespace}` | `workflows`, `cron_workflows`, `workflow_templates` |
| `label_selector` | `listOptions.labelSelector` | `workflows` |

Only declared filters are pushed directly to the Argo Workflows API.

---

# Table Reference

## `argoworkflows.workflows`

Workflow execution instances.

| Column | Type | Description |
| --- | --- | --- |
| `namespace` | Utf8 | Namespace filter scope |
| `label_selector` | Utf8 | Kubernetes label selector filter |
| `name` | Utf8 | Workflow name |
| `phase` | Utf8 | Workflow execution phase |
| `progress` | Utf8 | Workflow progress value |
| `message` | Utf8 | Workflow status message |
| `created_at` | Timestamp | Workflow creation timestamp |
| `started_at` | Timestamp | Workflow start timestamp |
| `finished_at` | Timestamp | Workflow completion timestamp |

**Required filter:** `namespace`

**Supported push-down filter:** `label_selector`

---

## `argoworkflows.cron_workflows`

Scheduled CronWorkflow resources.

| Column | Type | Description |
| --- | --- | --- |
| `namespace` | Utf8 | Namespace filter scope |
| `name` | Utf8 | CronWorkflow name |
| `schedule` | Utf8 | Cron schedule expression |
| `suspend` | Boolean | Whether the schedule is suspended |
| `timezone` | Utf8 | Configured timezone |
| `created_at` | Timestamp | CronWorkflow creation timestamp |

**Required filter:** `namespace`

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

---

## Server-side filtering with label selectors

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

---

## Suspended cron workflows

```sql
SELECT
  name,
  schedule,
  timezone
FROM argoworkflows.cron_workflows
WHERE namespace = 'infra-alerts'
  AND suspend = true;
```

---

## Recent cluster workflow templates

```sql
SELECT
  name,
  created_at
FROM argoworkflows.cluster_workflow_templates
ORDER BY created_at DESC;
```

---

# Validation

Run formatting and schema mapping evaluations locally before generating your pull request:

```bash
# YAML and style verification
make lint-sources

# Validate schema structure types against Coral DSL engine rules
coral source lint sources/community/argo-workflows/manifest.yaml
```

Execute a live target connection test locally:

```bash
export ARGOWORKFLOWS_BASE_URL=https://argo.example.com
export ARGOWORKFLOWS_AUTH_TOKEN=<token>

coral source add --file sources/community/argo-workflows/manifest.yaml

coral source test argoworkflows
```

---

# Representative Live Output Evidence

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

  ✓ SELECT name, namespace
    FROM argoworkflows.workflows
    WHERE namespace = 'example-namespace'
    LIMIT 1

    +---------------------------+-------------------+
    | name                      | namespace         |
    +---------------------------+-------------------+
    | metaguardian-cron-z9x2k   | example-namespace |
    +---------------------------+-------------------+

    1 row
```

---

# Representative Query Output

```text
$ coral sql "SELECT name, phase FROM argoworkflows.workflows WHERE namespace = 'core-apps' LIMIT 5"

+---------------------------+-----------+
| name                      | phase     |
+---------------------------+-----------+
| nightly-build-7hd92       | Running   |
| analytics-pipeline-j2ks1  | Succeeded |
+---------------------------+-----------+

$ coral sql "SELECT name, schedule FROM argoworkflows.cron_workflows WHERE namespace = 'infra-alerts' LIMIT 5"

+----------------------+----------------+
| name                 | schedule       |
+----------------------+----------------+
| nightly-cleanup      | 0 2 * * *      |
| metrics-backup       | */15 * * * *   |
+----------------------+----------------+
```

---

# Limitations

- Read-only source
- No workflow submission support
- No retry or terminate operations
- Namespace filter required for namespace-scoped tables
- RBAC permissions affect visible rows
- Large workflow collections should use SQL `LIMIT`
- Only REST API-visible workflow metadata is modeled

---

# References

- Argo Workflows REST API
- Argo Workflows Documentation
- Coral Community Sources
- Coral Custom Source Guide
