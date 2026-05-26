# Argo CD (Community)

**Version:** 1.1.1
**Backend:** HTTP (Argo CD REST API v1)
**Tables:** 4
**Base URL:** `{{input.ARGOCD_SERVER}}`

Query Argo CD applications, projects, clusters, and repositories through Coral SQL using the Argo CD REST API.

Use this source for GitOps deployment auditing, cluster inventory visibility, repository sync inspection, and application health tracking.

Coral exposes read-only access. Application syncs, repository refreshes, project creation, and other write operations are out of scope.

## Install

Community sources are not bundled with the Coral binary.

```bash
export ARGOCD_SERVER="https://argocd.example.com"
export ARGOCD_TOKEN="<your-token>"
coral source add --file sources/community/argocd/manifest.yaml
```

You may also copy the manifest locally and reference it directly.

## Authentication

Argo CD uses JWT bearer tokens in the `Authorization: Bearer` header.

| Input | Description |
| --- | --- |
| `ARGOCD_SERVER` | Argo CD server URL with scheme and port, no trailing slash |
| `ARGOCD_TOKEN` | JWT bearer token for API access |

### Obtain a token

**Option A — Project automation token (recommended):**

```bash
argocd proj role create-token <project> <role>
```

**Option B — Admin session token (expires after 24 hours):**

```bash
curl -k -X POST https://<argocd-host>/api/v1/session \
  -H 'Content-Type: application/json' \
  -d '{"username":"admin","password":"<password>"}'
```

### Minimum RBAC permissions

```csv
p, role:coral-reader, applications, get, */*, allow
p, role:coral-reader, projects,     get, *,   allow
p, role:coral-reader, clusters,     get, *,   allow
p, role:coral-reader, repositories, get, *,   allow
```

| Table | Resource | Action | Scope |
| --- | --- | --- | --- |
| `argocd.applications` | `applications` | `get` | `<project>/*` or `*/*` |
| `argocd.projects` | `projects` | `get` | `*` |
| `argocd.clusters` | `clusters` | `get` | `*` |
| `argocd.repositories` | `repositories` | `get` | `*` |

Project automation tokens need explicit policies:

```bash
argocd proj role add-policy <project> <role> -a get -p allow -r applications  -o '*'
argocd proj role add-policy <project> <role> -a get -p allow -r clusters       -o '*'
argocd proj role add-policy <project> <role> -a get -p allow -r repositories   -o '*'
argocd proj role add-policy <project> <role> -a get -p allow -r projects       -o '<project>'
```

Official docs:

- [Argo CD RBAC](https://argo-cd.readthedocs.io/en/stable/operator-manual/rbac/)
- [Argo CD API](https://argo-cd.readthedocs.io/en/stable/developer-guide/api-docs/)

### TLS note

Argo CD uses HTTPS by default. Self-signed certificates cause connection failures unless you run `argocd-server` with `--insecure` and use an `http://` URL.

For local testing with port-forward:

```bash
kubectl port-forward svc/argocd-server -n argocd 8080:80 &
export ARGOCD_SERVER="http://localhost:8080"
export ARGOCD_TOKEN="<your-token>"
coral source add --file sources/community/argocd/manifest.yaml
```

## Performance and query execution

All four tables perform full collection reads from Argo CD list endpoints:

```text
GET /api/v1/applications
GET /api/v1/projects
GET /api/v1/clusters
GET /api/v1/repositories
```

SQL `WHERE` and `LIMIT` reduce rows returned to the client but do not shrink the upstream API payload, except for the `project` filter on `argocd.applications`, which is pushed to the API as `?project=...`.

Each table uses `fetch_limit_default: 200` unless SQL sets an explicit `LIMIT`.

## Tables

| Table | Description | Optional pushdown filters |
| --- | --- | --- |
| `argocd.applications` | Applications with sync status, health, source, and destination | `project` |
| `argocd.projects` | AppProjects with repo and destination restrictions | — |
| `argocd.clusters` | Registered Kubernetes clusters | — |
| `argocd.repositories` | Configured Git and Helm repositories | — |

### `argocd.applications`

Primary inventory table. Supports single-source apps (`source__*` columns) and multi-source apps (`sources`, `sync_revisions` JSON columns).

Only `project` is pushed to the API. Filters such as `health_status`, `sync_status`, and `dest_server` apply locally after the list is fetched.

### `argocd.projects`

Project boundaries, allowed source repositories, and destination restrictions.

### `argocd.clusters`

Registered deployment targets with connection status and application counts.

### `argocd.repositories`

Configured repositories with connection status. May return zero rows when repositories are accessed without stored credentials.

## Example queries

### Application health audit

```sql
SELECT
  name,
  dest_namespace,
  dest_server,
  sync_status,
  health_status
FROM argocd.applications
WHERE health_status = 'Degraded'
LIMIT 25;
```

### Applications by project (API pushdown)

```sql
SELECT
  name,
  source__repo_url,
  source__target_revision,
  created_at
FROM argocd.applications
WHERE project = 'production'
ORDER BY created_at DESC
LIMIT 25;
```

### Cluster connectivity review

```sql
SELECT
  name,
  server,
  connection_status,
  server_version
FROM argocd.clusters
WHERE connection_status != 'Successful'
LIMIT 25;
```

### Repository connectivity audit

```sql
SELECT
  repo,
  type,
  insecure,
  enable_lfs,
  connection_status
FROM argocd.repositories
ORDER BY type
LIMIT 25;
```

## Validation

Local validation for this source:

```text
YAML parse: passed for sources/community/argocd/manifest.yaml
Coral manifest schema validation: passed for sources/community/argocd/manifest.yaml
make lint-sources: passed
Live API tests: passed against a self-hosted Argo CD instance
```

Lint the manifest:

```bash
make lint-sources
coral source lint sources/community/argocd/manifest.yaml
```

Add the source and run declared smoke tests:

```bash
export ARGOCD_SERVER="https://argocd.example.com"
export ARGOCD_TOKEN="<your-token>"
coral source add --file sources/community/argocd/manifest.yaml
coral source test argocd
```

Validate table access with representative SQL:

```bash
coral sql "SELECT name, health_status, sync_status FROM argocd.applications LIMIT 5"
coral sql "SELECT name, description FROM argocd.projects LIMIT 5"
coral sql "SELECT name, server, connection_status FROM argocd.clusters LIMIT 5"
coral sql "SELECT repo, type, connection_status FROM argocd.repositories LIMIT 5"
```

Inspect registered tables and columns:

```bash
coral sql "SELECT table_name, description FROM coral.tables WHERE schema_name = 'argocd'"
coral sql "SELECT table_name, column_name, data_type FROM coral.columns WHERE schema_name = 'argocd' ORDER BY table_name, ordinal_position"
```

Live Coral evidence:

```text
✓ argocd connected successfully

argocd (4 tables)
├─ applications
├─ clusters
├─ projects
└─ repositories

Query tests
2 declared · 2 passed · 0 failed

✓ SELECT * FROM argocd.applications LIMIT 1
  1 row

✓ SELECT * FROM argocd.applications WHERE project = 'default' LIMIT 1
  1 row
```

Representative query:

```sql
SELECT
  name,
  health_status,
  sync_status,
  dest_namespace,
  created_at
FROM argocd.applications
WHERE project = 'default'
ORDER BY created_at DESC
LIMIT 3;
```

Example output:

```text
name            | health_status | sync_status | dest_namespace | created_at
guestbook       | Healthy       | Synced      | default        | 2025-03-12T09:14:22Z
payments-api    | Degraded      | OutOfSync   | payments       | 2025-02-04T16:31:08Z
platform-config | Healthy       | Synced      | argocd         | 2024-11-20T11:02:44Z
```

## Limitations

- Read-only source.
- Application syncs, repository refreshes, and write operations are not supported.
- No streaming or watch protocol support.
- Full collection reads are performed for all tables except the `project` pushdown on applications.
- Project automation tokens only see their project's applications.
- Admin session tokens expire after 24 hours.
- `repositories` may return zero rows on fresh installs without stored repository credentials.
- Multi-source apps populate `sources` and `sync_revisions` instead of `source__*` / `sync_revision`.
- No `resource_tree` table in v1.
