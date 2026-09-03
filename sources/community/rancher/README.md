# Rancher (Community)

**Version:** 0.1.0
**Backend:** HTTP (Rancher Server API v3)
**Tables:** 1
**Base URL:** `{{input.RANCHER_URL}}/v3`

Query downstream Kubernetes cluster inventory and control-plane metadata directly through Coral SQL using the Rancher API.

This integration provides read-only access to Rancher's Server API v3 for operational auditing — inspecting provisioning states, infrastructure providers, Kubernetes versions, and cluster topology across environments managed through Rancher.

Coral exposes read-only `GET` tables. Cluster mutation, deletion, provisioning, and downstream workload management are out of scope.

## Install

Community sources are not bundled with the Coral binary.

From the Coral repository root:

```bash
export RANCHER_URL=https://rancher.infra.local
export RANCHER_TOKEN=token-abcde:1234567890abcdef
coral source add --file sources/community/rancher/manifest.yaml
```

You may also copy the manifest locally and reference it directly.

## Authentication

Rancher API access requires a valid bearer token. Coral sends the token as `Authorization: Bearer <token>`.

| Input | Kind | Required | Description |
| --- | --- | --- | --- |
| `RANCHER_URL` | variable | yes | Rancher Manager URL without trailing slash and without `/v3`, for example `https://rancher.infra.local` |
| `RANCHER_TOKEN` | secret | yes | Rancher API bearer token from the API Keys UI (for example `token-xxxxx:yyyyyyyyyyyyyyyy`) |

Generate a token from the Rancher UI under **API & Keys → Add Key**. Copy it immediately and store it securely — Rancher does not display the secret again.

The data returned by `/v3/clusters` is scoped by the cluster and project role bindings of the supplied token. Ensure the token has sufficient visibility for infrastructure-wide audits; clusters not visible to the token cannot be queried through Coral.

Official docs:

- [Rancher API — API Keys](https://ranchermanager.docs.rancher.com/reference-guides/user-settings/api-keys)
- [Rancher API v3 Reference](https://ranchermanager.docs.rancher.com/api/quickstart)

## Tables

| Table | API Endpoint | Pushdown filters | Pagination |
| --- | --- | --- | --- |
| `rancher.clusters` | `GET /v3/clusters` | `name`, `state` | Single bounded page (`limit=1000`) |

### Pagination

Rancher v3 collections paginate using an opaque `marker`, and expose the next page only as a **full URL** in the response `pagination.next` field (there is no bare cursor token). Coral cannot follow a full body next-link, so this source is **explicitly bounded**: it requests a single page with `limit=1000` (Rancher's suggested upper bound) rather than silently returning only a default-size first page.

For virtually all Rancher installations the cluster count is well under this bound. If you operate more than 1000 clusters in one Rancher, narrow with the `name` / `state` pushdown filters; results beyond the first 1000 are not returned.

### `rancher.clusters`

Managed Kubernetes clusters tracked through Rancher.

| Column | Type | Description |
| --- | --- | --- |
| `id` | Utf8 | Unique cluster ID assigned by Rancher |
| `name` | Utf8 | Cluster name (also a pushdown filter) |
| `state` | Utf8 | Operational cluster state (also a pushdown filter) |
| `provider` | Utf8 | Infrastructure provider associated with the cluster |
| `kubernetes_version` | Utf8 | Kubernetes version reported by the cluster |
| `node_count` | Int64 | Number of nodes associated with the cluster |

#### Pushdown filters

These are pushed to the Rancher API as query parameters (server-side filtering), and are also readable columns:

| SQL filter | Rancher mapping | Description |
| --- | --- | --- |
| `name` | `?name=` | Exact cluster name |
| `state` | `?state=` | Cluster state (e.g., `active`, `pending`) |

Predicates on other columns are applied locally by Coral after the page is fetched.

## Example queries

### Operational topology audit

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

### Look up a specific cluster (server-side filter)

```sql
SELECT
  id,
  name,
  state,
  kubernetes_version
FROM rancher.clusters
WHERE name = 'k8s-prod-01';
```

## Validation

Local validation for this source:

```text
YAML parse: passed for sources/community/rancher/manifest.yaml
Coral manifest schema validation: passed for sources/community/rancher/manifest.yaml
make lint-sources: passed
Live API tests: passed with a Rancher API token
```

Lint the manifest:

```bash
make lint-sources
coral source lint sources/community/rancher/manifest.yaml
```

Add the source and run declared smoke tests:

```bash
export RANCHER_URL=https://rancher.infra.local
export RANCHER_TOKEN=token-abcde:1234567890abcdef
coral source add --file sources/community/rancher/manifest.yaml
coral source test rancher
```

Validate table access with representative SQL:

```bash
coral sql "SELECT id, name FROM rancher.clusters LIMIT 5"
coral sql "SELECT name, state, node_count FROM rancher.clusters WHERE state = 'active' LIMIT 5"
coral sql "SELECT id, name, state, kubernetes_version FROM rancher.clusters WHERE name = 'k8s-prod-01'"
```

Inspect registered tables and columns:

```bash
coral sql "SELECT table_name, description FROM coral.tables WHERE schema_name = 'rancher'"
coral sql "SELECT table_name, column_name, data_type FROM coral.columns WHERE schema_name = 'rancher' ORDER BY table_name, ordinal_position"
```

Live Coral evidence:

```text
✓ rancher connected successfully

rancher (1 table)
└─ clusters

Query tests
1 declared · 1 passed · 0 failed

✓ SELECT id, name FROM rancher.clusters LIMIT 1
  1 row
```

Representative query:

```sql
SELECT name, state, provider, kubernetes_version, node_count
FROM rancher.clusters
WHERE state = 'active'
LIMIT 3;
```

Example output:

```text
name          | state  | provider    | kubernetes_version | node_count
k8s-prod-01   | active | rke2        | v1.29.4+rke2r1     | 9
k8s-staging   | active | rke2        | v1.29.4+rke2r1     | 4
edge-west-02  | active | k3s         | v1.28.8+k3s1       | 3
```

## Limitations

- Read-only retrieval scope.
- Cluster provisioning, mutation, deletion, and downstream scaling are out of scope.
- Exposes top-level cluster inventory metadata only; no downstream Kubernetes workloads (Pods, Deployments, ConfigMaps, Events, Logs).
- `name` and `state` are pushed to the Rancher API; other predicates are applied locally by Coral.
- Query results are limited by the cluster and project role bindings of the supplied token.
- **Bounded result set:** the source fetches a single page of up to 1000 clusters and does not traverse Rancher's `marker` pagination, because Rancher exposes the next page only as a full URL that Coral cannot follow. Installations exceeding 1000 clusters should filter by `name`/`state`.
