# Headlamp (Community)

**Version:** 0.1.0
**Backend:** HTTP (Headlamp Backend Proxy API)
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
| `HEADLAMP_BASE_URL` | variable | yes | Headlamp base URL without trailing slash, for example `https://headlamp.example.com` or `http://localhost:4466` |

---

# Tables Overview

| Table | API Endpoint | Required Filters | Pagination |
|---|---|---|---|
| `clusters` | `GET /config` | — | None (maps from `clusters` array) |

---

# Table Reference

## headlamp.clusters

Kubernetes clusters accessible through the Headlamp backend configuration.

| Column | Type | Description |
|---|---|---|
| `name` | Utf8 | Cluster name |
| `server` | Utf8 | Kubernetes API server URL |
| `auth_type` | Utf8 | Authentication type |

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
export HEADLAMP_BASE_URL=https://headlamp.internal.infra

coral source add --file sources/community/headlamp/manifest.yaml
coral source test headlamp
```

---

# Representative Live Output

```text
$ coral source test headlamp

✓ headlamp connected successfully

  headlamp (1 table)
  └─ clusters

  Query tests
  1 declared · 1 passed · 0 failed

✓ SELECT name FROM headlamp.clusters LIMIT 1

+-------------+
| name        |
+-------------+
| k8s-prod-01 |
+-------------+

1 row
```

---

# Limitations

- Read-only execution scope
- Cluster registration and configuration mutations are out of scope
- Does not expose Kubernetes workload telemetry such as Pods, Deployments, Events, or Logs
- Intended for auditing cluster connectivity visibility through Headlamp
- Targets the Headlamp configuration endpoints directly; specialized reverse-proxy authentication configurations are out of scope
