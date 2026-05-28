# Rancher (Community)

**Version:** 0.1.0
**Backend:** HTTP (Rancher Server API v3)
**Tables:** 1
**Base URL:** `{{input.RANCHER_URL}}/v3`

Query downstream Kubernetes cluster inventory and control plane metadata directly through Coral SQL using the Rancher API.

This integration is intended for operational auditing workflows, helping administrators inspect provisioning states, infrastructure providers, Kubernetes versions, and cluster topology footprints across environments managed through Rancher.

Coral exposes read-only `GET` tables. Cluster mutation, deletion, provisioning, or downstream workload management operations are out of scope.

---

# Install

Community sources are not bundled with the Coral binary.

From the Coral repository root:

```bash
coral source add --file sources/community/rancher/manifest.yaml
```

Or copy `manifest.yaml` into your workspace and pass that path to:

```bash
coral source add --file <path-to-manifest>
```

---

# Inputs

| Input | Kind | Required | Description |
|---|---|---|---|
| `RANCHER_URL` | variable | yes | Rancher Manager URL without trailing slash and without `/v3`, for example `https://rancher.infra.local` |
| `RANCHER_TOKEN` | secret | yes | Rancher API Bearer Token generated from the Rancher API Keys UI (for example `token-xxxxx:yyyyyyyyyyyyyyyy`) |

> **Access Control Note**
> The data returned by `/v3/clusters` is scoped by the cluster and project role bindings assigned to the provided token. Ensure the token has sufficient visibility when performing infrastructure-wide audits.

---

# Tables Overview

| Table | API Endpoint | Required Filters | Pagination |
|---|---|---|---|
| `clusters` | `GET /clusters` | — | None (supports server-side query pushdowns) |

---

# Table Reference

## rancher.clusters

Managed Kubernetes clusters tracked through Rancher.

| Column | Type | Description |
|---|---|---|
| `id` | Utf8 | Unique cluster ID assigned by Rancher |
| `name` | Utf8 | Human-readable cluster name |
| `state` | Utf8 | Operational cluster state |
| `provider` | Utf8 | Infrastructure provider associated with the cluster |
| `kubernetes_version` | Utf8 | Kubernetes version reported by the cluster |
| `node_count` | Int64 | Number of nodes associated with the cluster |

---

# Example Queries

## Operational Topology Audit

```sql
SELECT
  name,
  state,
  provider,
  kubernetes_version,
  node_count
FROM rancher.clusters
WHERE state = 'active'
ORDER BY node_count DESC;
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
coral source lint sources/community/rancher/manifest.yaml
```

## Execute Live Connection Test

```bash
export RANCHER_URL=https://rancher.infra.local
export RANCHER_TOKEN=token-abcde:1234567890abcdef

coral source add --file sources/community/rancher/manifest.yaml
coral source test rancher
```

---

# Representative Live Output

```text
$ coral source test rancher

✓ rancher connected successfully

  rancher (1 table)
  └─ clusters

  Query tests
  1 declared · 1 passed · 0 failed

✓ SELECT id, name FROM rancher.clusters LIMIT 1

+--------------+-------------+
| id           | name        |
+--------------+-------------+
| c-m-z8p2wxlk | k8s-prod-01 |
+--------------+-------------+

1 row
```

---

# Limitations

- Read-only retrieval scope
- Cluster provisioning, mutation, deletion, or downstream scaling workflows are out of scope
- Exposes top-level cluster inventory metadata only
- Does not expose downstream Kubernetes workload resources such as Pods, Deployments, ConfigMaps, Events, or Logs
- Filtering is handled server-side for supported fields such as `name` and `state`
- Rancher collection endpoints may paginate large cluster inventories depending on server configuration
- This source currently targets smaller operational inventories and does not yet implement Rancher pagination traversal
- For large deployments, use server-side pushdown filters such as `WHERE name = 'cluster-name'` whenever possible
