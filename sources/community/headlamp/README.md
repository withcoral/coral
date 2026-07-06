# Headlamp (Community)

**Version:** 0.1.0
**Backend:** HTTP (Headlamp Backend Configuration API)
**Tables:** 1
**Base URL:** `{{input.HEADLAMP_BASE_URL}}`

Query Headlamp cluster connection footprints directly through Coral SQL.

Rather than replacing primary Kubernetes resource scraping workflows, this integration provides read-only access to Headlamp's backend configuration as an operational auditing layer — identifying which cluster control planes are exposed through the active Headlamp workspace configuration.

Coral exposes read-only `GET` tables. Modifying kubeconfig structures, registering clusters, and performing cluster role mutations are out of scope.

## Install

Community sources are not bundled with the Coral binary.

From the Coral repository root:

```bash
export HEADLAMP_BASE_URL=https://headlamp.example.com
coral source add --file sources/community/headlamp/manifest.yaml
```

You may also copy the manifest locally and reference it directly.

## Authentication

**None is required, and this source sends no credentials.**

| Input | Kind | Required | Description |
| --- | --- | --- | --- |
| `HEADLAMP_BASE_URL` | variable | yes | Headlamp base URL without a trailing slash, for example `https://headlamp.example.com` or `http://localhost:4466`. Include any sub-path Headlamp is served under; the source appends `/config`. |

This source reads Headlamp's `GET /config` endpoint, which is read-only and unauthenticated. Headlamp's `X-HEADLAMP-BACKEND-TOKEN` guard protects only its mutating and in-cluster backend APIs, not `/config`, so the manifest declares no auth and sends no token.

If your deployment places Headlamp behind a reverse proxy that adds its own authentication, terminating that auth is out of scope for this source — point `HEADLAMP_BASE_URL` at an endpoint that serves `/config` directly.

Official docs:

- [Headlamp Documentation](https://headlamp.dev/docs/latest/)

## Tables

| Table | API Endpoint | Pushdown filters | Pagination |
| --- | --- | --- | --- |
| `headlamp.clusters` | `GET /config` | — | None (maps from the `clusters` array) |

### `headlamp.clusters`

Kubernetes clusters accessible through the Headlamp backend configuration. Each row is one entry from the `clusters` array returned by `GET /config`.

| Column | Type | Description |
| --- | --- | --- |
| `name` | Utf8 | Cluster name |
| `server` | Utf8 | Kubernetes API server URL |
| `auth_type` | Utf8 | Authentication type configured for the cluster |

## Example queries

### Audit exposed cluster control planes

```sql
SELECT
  name,
  server,
  auth_type
FROM headlamp.clusters
ORDER BY name ASC;
```

## Validation

Local validation for this source:

```text
YAML parse: passed for sources/community/headlamp/manifest.yaml
Coral manifest schema validation: passed for sources/community/headlamp/manifest.yaml
make lint-sources: passed
Live API tests: passed against a Headlamp backend
```

Lint the manifest:

```bash
make lint-sources
coral source lint sources/community/headlamp/manifest.yaml
```

Add the source and run declared smoke tests:

```bash
export HEADLAMP_BASE_URL=https://headlamp.example.com
coral source add --file sources/community/headlamp/manifest.yaml
coral source test headlamp
```

Validate table access with representative SQL:

```bash
coral sql "SELECT name FROM headlamp.clusters LIMIT 5"
coral sql "SELECT name, server, auth_type FROM headlamp.clusters LIMIT 5"
```

Inspect registered tables and columns:

```bash
coral sql "SELECT table_name, description FROM coral.tables WHERE schema_name = 'headlamp'"
coral sql "SELECT table_name, column_name, data_type FROM coral.columns WHERE schema_name = 'headlamp' ORDER BY table_name, ordinal_position"
```

Live Coral evidence:

```text
✓ headlamp connected successfully

headlamp (1 table)
└─ clusters

Query tests
1 declared · 1 passed · 0 failed

✓ SELECT name FROM headlamp.clusters LIMIT 1
  1 row
```

Representative query:

```sql
SELECT name, server, auth_type
FROM headlamp.clusters
ORDER BY name ASC
LIMIT 3;
```

Example output:

```text
name          | server                          | auth_type
prod-us-east  | https://10.0.12.4:6443          | serviceAccount
prod-eu-west  | https://10.1.8.9:6443           | oidc
staging       | https://staging.k8s.internal    | serviceAccount
```

## Limitations

- Read-only execution scope; no authentication is used and none is sent.
- Cluster registration and configuration mutations are out of scope.
- Does not expose Kubernetes workload telemetry such as Pods, Deployments, Events, or Logs.
- Intended for auditing cluster connectivity visibility through Headlamp.
- Targets the Headlamp configuration endpoint (`/config`) directly; specialized reverse-proxy authentication configurations are out of scope.
