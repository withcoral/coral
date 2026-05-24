# Argo CD (Community)

**Version:** 0.1.0
**Backend:** HTTP (Argo CD REST API v1)
**Tables:** 4
**Base URL:** `{{input.ARGOCD_BASE_URL}}/api/v1`

Query Argo CD applications, projects, clusters, and repositories through Coral SQL using the [Argo CD REST API](https://cd.apps.argoproj.io/swagger-ui/). Use this data source for GitOps deployment state auditing, cluster alignment tracking, repo sync health monitoring, and managing environment topologies across self-hosted Argo CD installations.

Coral exposes read-only `GET` tables. Write operations (syncing applications, creating projects, refreshing repositories) are out of scope for v1.

## Install

Community sources are not bundled with the Coral binary. From the Coral repo root (or with a copied manifest):

```bash
coral source add --file sources/community/argocd/manifest.yaml
```

Or copy `manifest.yaml` into your workspace and pass that path to `coral source add --file`.

Set credentials via environment variables (recommended) or `coral source add --file ... --interactive`.

## Inputs

| Input | Kind | Required | Description |
| --- | --- | --- | --- |
| `ARGOCD_BASE_URL` | variable | yes | Root API URL of your Argo CD instance with **no** trailing slash and **no** `/api/v1` path suffix (for example `https://argocd.example.com` or `http://localhost:8080`). |
| `ARGOCD_AUTH_TOKEN` | secret | yes | Authentication token generated via your Argo CD panel under User Settings or via `argocd account generate-token`. |

---

## Tables overview

| Table | API endpoint | Required filter | Pagination |
| --- | --- | --- | --- |
| `applications` | `GET /api/v1/applications` | — | Wrapped array response (`items`) |
| `projects` | `GET /api/v1/projects` | — | Wrapped array response (`items`) |
| `clusters` | `GET /api/v1/clusters` | — | Raw array response |
| `repositories` | `GET /api/v1/repositories` | — | Raw array response |

---

## Filters and API mapping

Coral maps declared SQL filters to native Argo CD API query parameters. Only listed filters are pushed directly down to the REST endpoint; other clauses are filtered in-memory.

| SQL filter | Argo CD query param | Tables |
| --- | --- | --- |
| `project` | query `project` | `applications` |

---

## Table reference

### `argocd.applications`

Deployed GitOps applications managed under Argo CD controllers.

| Column | Type | Description |
| --- | --- | --- |
| `name` | Utf8 | Name of the application |
| `namespace` | Utf8 | Target cluster namespace where resources are deployed |
| `destination_server` | Utf8 | Target cluster API server host location endpoint |
| `repo_url` | Utf8 | Source control repository URL containing configurations (returns null for multi-source applications) |
| `target_revision` | Utf8 | Tracked Git revision tag, branch, or pinned commit SHA (returns null for multi-source applications) |
| `path` | Utf8 | Target file directory path within the source repository (returns null for multi-source applications) |
| `sync_status` | Utf8 | Sync status relative to Git state (e.g., `Synced`, `OutOfSync`) |
| `health_status` | Utf8 | Operational resource status evaluation (e.g., `Healthy`, `Degraded`) |
| `project` | Utf8 | Argo CD project scope assignment grouping |
| `created_at` | Timestamp | Timestamp detailing when the application was registered |

**Optional filter:** `project`

### `argocd.projects`

AppProjects defining deployment boundaries, resource limits, and cluster permissions.

| Column | Type | Description |
| --- | --- | --- |
| `name` | Utf8 | Project unique target name identification |
| `description` | Utf8 | Text details explaining project target environment roles |

### `argocd.clusters`

Connected Kubernetes clusters managed by Argo CD.

| Column | Type | Description |
| --- | --- | --- |
| `name` | Utf8 | Human-friendly deployment cluster label name |
| `server` | Utf8 | Host destination API endpoint location of the control plane |
| `connection_status` | Utf8 | Connectivity evaluation metric status value (e.g., `Successful`) |

### `argocd.repositories`

Configured Git and Helm repositories connected to Argo CD.

| Column | Type | Description |
| --- | --- | --- |
| `repo_url` | Utf8 | Connected endpoint sync source repository URL |
| `type` | Utf8 | Version controller storage backend engine layout (`git` or `helm`) |
| `connection_status` | Utf8 | Active connection diagnostic authentication status output |

---

## Example queries

### Active Application Delivery Auditing
```sql
SELECT name, namespace, destination_server, sync_status, health_status 
FROM argocd.applications 
WHERE health_status = 'Degraded' 
LIMIT 50;
```

```sql
SELECT name, repo_url, target_revision, created_at 
FROM argocd.applications 
WHERE project = 'production'
ORDER BY created_at DESC;
```

### Connected Infrastructure Cluster Review
```sql
SELECT name, server, connection_status 
FROM argocd.clusters 
WHERE connection_status != 'Successful';
```

### GitOps Source Repository Status
```sql
SELECT repo_url, type, connection_status 
FROM argocd.repositories 
ORDER BY type DESC;
```

---

## Validation

Run the following format and syntax pipeline validation commands prior to generating a GitHub pull request:

```bash
# YAML and file style compliance check
make lint-sources

# Structural schema and type mapping verification
coral source lint sources/community/argocd/manifest.yaml
```

Execute a live target connection test locally:

```bash
export ARGOCD_BASE_URL=https://argocd.example.com
export ARGOCD_AUTH_TOKEN=your_jwt_token_here

coral source add --file sources/community/argocd/manifest.yaml
coral source test argocd
```

Example smoke-test output:

```text
$ coral source test argocd

  ✓ argocd connected successfully

    argocd (4 tables)
    ├─ applications
    ├─ projects
    ├─ clusters
    └─ repositories
    Query tests
    1 declared · 1 passed · 0 failed

  ✓ SELECT name, namespace FROM argocd.applications LIMIT 1
    1 row
```

---

## Limitations

- **Read-only source.** No sync operations, resource creation, or remote deployment modifications.
- **No sync or deployment execution.** Reconciliations must be triggered via the Argo CD UI, CLI, or Git engines.
- **No streaming/watch support.** Data updates rely on polling API cycles; live stream channels are out of scope for v1.
- **RBAC permissions affect visible resources.** Returned SQL rows are strictly bounded by your personal access token's access controls.
- **Large Argo CD instances should use SQL `LIMIT`.** Instances with thousands of tracked application records should include tight constraints to prevent memory limits during unpacking operations.
- **Only REST API-visible resources are modeled.** Underlying custom fields or unexposed cluster telemetry targets are omitted from row strategizing.