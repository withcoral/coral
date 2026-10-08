# ArgoCD Community Source

Query ArgoCD applications, projects, clusters, and repositories through
Coral SQL using the ArgoCD REST API. Works with both **ArgoCD Cloud**
(via any hosted instance) and **self-hosted ArgoCD**.

## Setup

### 1. Obtain a bearer token

ArgoCD uses JWT bearer tokens for API access. Two options:

**Option A — Project automation token (recommended for long-lived installs):**

```bash
argocd proj role create-token <project> <role>
```

Project tokens are scoped to one project, have configurable expiry, and
can be revoked immediately without affecting other tokens.

**Option B — Admin session token (expires after 24 hours):**

```bash
curl -k -X POST https://<argocd-host>/api/v1/session \
  -H 'Content-Type: application/json' \
  -d '{"username":"admin","password":"<password>"}'
```

Copy the `token` value from the JSON response.

### 2. Minimum RBAC permissions

Create a least-privilege role with only the permissions needed for the
tables you plan to query. In an ArgoCD RBAC policy file:

```csv
# Minimum permissions for all four tables
p, role:coral-reader, applications, get, */*, allow
p, role:coral-reader, projects,     get, *,   allow
p, role:coral-reader, clusters,     get, *,   allow
p, role:coral-reader, repositories, get, *,   allow
```

Per-table breakdown:

| Table | Resource | Action | Scope |
|---|---|---|---|
| `applications` | `applications` | `get` | `<project>/*` or `*/*` |
| `projects` | `projects` | `get` | `*` |
| `clusters` | `clusters` | `get` | `*` |
| `repositories` | `repositories` | `get` | `*` |

Project automation tokens inherit only the policies explicitly added to
their role — nothing is granted automatically. Add each permission you
need via `argocd proj role add-policy` (note: `-r` sets the resource
type; `-o` is the object pattern within the project, not the resource):

```bash
argocd proj role add-policy <project> <role> -a get -p allow -r applications  -o '*'
argocd proj role add-policy <project> <role> -a get -p allow -r clusters       -o '*'
argocd proj role add-policy <project> <role> -a get -p allow -r repositories   -o '*'
argocd proj role add-policy <project> <role> -a get -p allow -r projects       -o '<project>'
```

See the [ArgoCD RBAC docs](https://argo-cd.readthedocs.io/en/stable/operator-manual/rbac/)
for full policy syntax.

### 3. TLS note

ArgoCD's API server uses HTTPS by default. Coral requires a valid TLS
certificate — self-signed certs will cause connection failures. If your
server uses a self-signed cert, run argocd-server with `--insecure` and
use an `http://` URL instead:

```bash
# Patch argocd-server to run in insecure mode (dev/test only)
kubectl patch deployment argocd-server -n argocd \
  --type='json' \
  -p='[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--insecure"}]'
```

### 4. Add the source

```bash
export ARGOCD_SERVER="https://argocd.example.com"   # no trailing slash
export ARGOCD_TOKEN="<your-token>"
coral source add --file sources/community/argocd/manifest.yaml
```

For a local port-forwarded instance in insecure mode:

```bash
kubectl port-forward svc/argocd-server -n argocd 8080:80 &
export ARGOCD_SERVER="http://localhost:8080"
export ARGOCD_TOKEN="<your-token>"
coral source add --file sources/community/argocd/manifest.yaml
```

### 5. Verify

```bash
coral source test argocd
```

The built-in test query reads `argocd.applications` and verifies auth
and column mapping.

## Tables

### `argocd.applications`

All ArgoCD applications visible to the configured token, with sync
status, health, source, and destination.

**Optional filter:** `project` — scopes the API call to a single ArgoCD
project. Especially important when using a project-scoped automation
token, which can only see its own project's apps.

**Note:** `project` is the only server-side filter. All other `WHERE`
clauses (`health_status`, `sync_status`, `dest_server`, etc.) apply
locally after the full list is fetched.

**Single-source vs multi-source apps:**

ArgoCD supports two application shapes:

- **Single-source** (`spec.source`): `source__repo_url`,
  `source__target_revision`, `source__path`, and `source__chart` are
  populated. `sync_revision` holds the single deployed Git revision.
  `sources` and `sync_revisions` are null.
- **Multi-source** (`spec.sources[]`, ArgoCD v2.6+): all `source__*`
  columns are null. Use the `sources` JSON column for the full array of
  source definitions. `sync_revisions` holds the JSON array of deployed
  revisions (one per source); `sync_revision` is null.

### `argocd.projects`

ArgoCD projects with allowed source repositories, destination
restrictions, and cluster-scoped resource whitelists.

**Minimum permission:** `projects, get, *`

### `argocd.clusters`

Kubernetes clusters registered as deployment targets, with connection
status and application count.

**Minimum permission:** `clusters, get, *`

### `argocd.repositories`

Git and Helm repositories configured in ArgoCD with connection status.
Returns 0 rows when no repositories are explicitly registered (repos
accessed without stored credentials do not appear).

**Minimum permission:** `repositories, get, *`

## Example Queries

```sql
-- Fleet health: which apps are unhealthy or out of sync?
SELECT name, health_status, sync_status, dest_namespace, dest_server,
       operation_finished_at
FROM argocd.applications
WHERE health_status != 'Healthy' OR sync_status != 'Synced';

-- What is deployed in production right now?
SELECT name, source__repo_url, source__target_revision,
       sync_revision, sync_revisions, operation_phase
FROM argocd.applications
WHERE dest_server = 'https://prod-cluster.example.com';

-- Multi-source apps: inspect all sources and their deployed revisions
SELECT name, sources, sync_revisions
FROM argocd.applications
WHERE sources IS NOT NULL;

-- Scope to a specific project (server-side filter)
SELECT name, health_status, sync_status, dest_namespace
FROM argocd.applications
WHERE project = 'payments';

-- Cluster connectivity overview
SELECT name, server, connection_status, server_version, applications_count
FROM argocd.clusters;

-- Repository health
SELECT repo, type, connection_status, connection_message
FROM argocd.repositories
WHERE connection_status = 'Failed';

-- Cross-source: degraded apps with open PagerDuty incidents
SELECT a.name, a.health_status, i.title, i.created_at
FROM argocd.applications a
JOIN pagerduty.incidents i ON a.name = i.service_name
WHERE a.health_status = 'Degraded';
```

## Limitations

- **Read-only.** This source does not sync, delete, or modify ArgoCD
  resources.
- **Token scope.** Project automation tokens only see their project's
  applications. Use the `project` filter when querying with a scoped
  token to avoid empty results.
- **Admin token expiry.** Admin session tokens expire after 24 hours.
  Project automation tokens are recommended for persistent installs.
- **`repositories` returns 0 rows** on fresh installs where repos are
  accessed without stored credentials. This is correct ArgoCD behaviour.
- **Multi-source revision shape.** For multi-source apps, `sync_revision`
  is null and deployed revisions are in `sync_revisions` (JSON array, one
  entry per source). For single-source apps, `sync_revision` is a string
  and `sync_revisions` is null.
- **No `resource_tree` in v1.** The Kubernetes resource tree per
  application is complex to flatten usefully and is left for a follow-on.
