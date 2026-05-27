# Headlamp (Community)

**Version:** 0.1.0
**Backend:** HTTP (Headlamp Backend Configuration API)
**Tables:** 1
**Base URL:** `{{input.HEADLAMP_BASE_URL}}`

Query Headlamp cluster connection footprints directly through Coral SQL.

Instead of replacing primary Kubernetes resource scraping workflows, this integration acts as an operational auditing layer to identify which cluster control planes are exposed through the active Headlamp workspace configuration.

Coral exposes read-only `GET` tables. Modifying kubeconfig structures, registering clusters, or performing cluster role mutations are out of scope.

---

# Install

Community sources are not bundled with the Coral binary.

From the Coral repository root:

```bash
coral source add --file sources/community/headlamp/manifest.yaml
```

Or copy `manifest.yaml` into your workspace and pass that path to:

```bash
coral source add --file <path-to-manifest>
```

---

# Inputs

| Input | Kind | Required | Description |
|---|---|---|---|
| `HEADLAMP_BASE_URL` | variable | yes | Headlamp base URL without a trailing slash, for example `https://headlamp.example.com` or `http://localhost:4466`. Include any sub-path Headlamp is served under; the source appends `/config`. |

---

# Authentication

No credentials are required. This source reads Headlamp's `GET /config` endpoint, which is read-only and unauthenticated. Headlamp's `X-HEADLAMP_BACKEND-TOKEN` guard protects only its mutating and in-cluster backend APIs, not `/config`, so the manifest declares no auth and sends no token.

If your deployment places Headlamp behind a reverse proxy that adds its own authentication, terminating that auth is out of scope for this source — point `HEADLAMP_BASE_URL` at an endpoint that serves `/config` directly.

---

# Tables Overview

| Table | API Endpoint | Required Filters | Pagination |
|---|---|---|---|
| `clusters` | `GET /config` | — | None (maps from the `clusters` array) |

---

# Table Reference

## headlamp.clusters

Kubernetes clusters accessible through the Headlamp backend configuration. Each row is one entry from the `clusters` array returned by `GET /config`.

| Column | Type | Description |
|---|---|---|
| `name` | Utf8 | Cluster name |
| `server` | Utf8 | Kubernetes API server URL |
| `auth_type` | Utf8 | Authentication type configured for the cluster |

---

# Example Queries

## Audit Exposed Cluster Control Planes

```sql
SELECT name, server, auth_type
FROM headlamp.clusters
ORDER BY name ASC;
```

---

# Validation

Run formatting and schema validation locally before opening a pull request.

## Lint Sources

```bash
make lint-sources
```

## Validate Coral Source Schema

```bash
coral source lint sources/community/headlamp/manifest.yaml
```

## Execute Live Connection Test

```bash
export HEADLAMP_BASE_URL=https://headlamp.example.com

coral source add --file sources/community/headlamp/manifest.yaml
coral source test headlamp
coral sql "SELECT name, server, auth_type FROM headlamp.clusters LIMIT 5"
```

---

# Live Output

> Replace the block below with the actual output from your own `coral source test headlamp`
> run against this manifest. Do not ship placeholder output.

```text
$ coral source test headlamp

✓ headlamp connected successfully

  headlamp (1 table)
  └─ clusters

  Query tests
  1 declared · 1 passed · 0 failed

✓ SELECT name FROM headlamp.clusters LIMIT 1
  1 row
```

---

# Limitations

- Read-only execution scope; no authentication is used and none is sent.
- Cluster registration and configuration mutations are out of scope.
- Does not expose Kubernetes workload telemetry such as Pods, Deployments, Events, or Logs.
- Intended for auditing cluster connectivity visibility through Headlamp.
- Targets the Headlamp configuration endpoint (`/config`) directly; specialized reverse-proxy authentication configurations are out of scope.
