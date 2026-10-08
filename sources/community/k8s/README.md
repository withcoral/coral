# Kubernetes Connector (Community)

**Version:** 0.1.3
**Backend:** HTTP (Kubernetes REST API)
**Tables:** 16
**Default base URL:** `http://127.0.0.1:8080` (override with `K8S_BASE_URL`)

Query live Kubernetes cluster state with SQL: workloads, networking, storage,
events, and nodes. Read-only v1 uses unauthenticated HTTP against a base URL
that already handles auth (typically `kubectl proxy`).

## Install

Community sources are not bundled with the Coral binary. Add the manifest from
this directory:

```bash
coral source add --file sources/community/k8s/manifest.yaml
```

Or copy `manifest.yaml` into your workspace and pass that path to
`coral source add --file`.

Reference the linked GitHub issue in your PR so maintainers can connect the
contribution to the prior discussion.

## Authentication and setup

### Local development (recommended for contributors)

`kubectl proxy` reuses your kubeconfig credentials and avoids extra Coral auth
configuration:

```bash
kubectl proxy --port=8080
```

Keep the default `K8S_BASE_URL` (`http://127.0.0.1:8080`) or set it when
adding the source:

```bash
export K8S_BASE_URL=http://127.0.0.1:8080
coral source add --file sources/community/k8s/manifest.yaml
```

### Authenticated gateways (advanced)

v1 does not send `Authorization` headers or client certificates. To reach a
cluster without `kubectl proxy`, point `K8S_BASE_URL` at a reverse proxy or API
gateway that authenticates on your behalf (for example an in-cluster OAuth proxy
or a corporate API front door). A raw Kubernetes API server URL such as
`https://kubernetes.default.svc` requires bearer-token auth and is not supported
in v1; use `kubectl proxy` locally or add a follow-on auth input in a later
release.

### Multi-cluster

Register one Coral source per cluster (for example `k8s_dev`, `k8s_prod`), each
with its own `K8S_BASE_URL`.

## RBAC requirements

The 15 namespace-scoped tables in this source have two request paths:

| Query shape | API path | RBAC required |
|---|---|---|
| `SELECT … FROM k8s.pods` (no `namespace` filter) | `GET /api/v1/pods` (list across all namespaces) | Cluster-wide `list` on the resource (e.g. a `ClusterRoleBinding` to a `ClusterRole` with `list pods`) |
| `SELECT … FROM k8s.pods WHERE namespace = 'foo'` | `GET /api/v1/namespaces/foo/pods` (list in one namespace) | Namespace-scoped `list` is sufficient (a `RoleBinding` in `foo` is enough) |

The manifest exposes a `namespace` filter on every namespace-scoped table; when
the filter is present Coral rewrites the request to the namespaced URL. This
gives users with namespace-scoped kubeconfigs a working first-success path —
they only need permission inside their namespace, not cluster-wide.

The `nodes` table is cluster-scoped; it requires cluster-wide `list nodes` and
has no namespace filter.

Minimal read-only `ClusterRole` for the cluster-wide path (drop the namespaced
resources you don't need). The connector only performs paginated `GET` list
requests, so `watch` is intentionally omitted:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: coral-k8s-source-readonly
rules:
  - apiGroups: [""]
    resources: [pods, services, endpoints, events, configmaps, persistentvolumeclaims, serviceaccounts, nodes]
    verbs: [get, list]
  - apiGroups: [apps]
    resources: [deployments, daemonsets, statefulsets, replicasets]
    verbs: [get, list]
  - apiGroups: [batch]
    resources: [jobs, cronjobs]
    verbs: [get, list]
  - apiGroups: [networking.k8s.io]
    resources: [ingresses, networkpolicies]
    verbs: [get, list]
```

Reference: <https://kubernetes.io/docs/reference/access-authn-authz/rbac/>.

## Table categories

### Workloads

| Table | Description |
|---|---|
| `pods` | Pod phase, scheduling, labels, container statuses |
| `deployments` | Replica counts and availability |
| `daemonsets` | Node-level daemon workload health |
| `statefulsets` | Stateful workload replica health |
| `replicasets` | ReplicaSet replica counts |
| `jobs` | Batch job success and failure counts |
| `cronjobs` | Scheduled workloads |

### Networking and storage

| Table | Description |
|---|---|
| `services` | Service discovery metadata |
| `endpoints` | Service endpoint subsets |
| `ingresses` | Ingress routing configuration |
| `networkpolicies` | Network policy selectors |
| `persistentvolumeclaims` | PVC phase and storage class |

### Cluster resources

| Table | Description |
|---|---|
| `nodes` | Node kubelet version and labels |
| `events` | Cluster events for triage (event_time / first_timestamp / last_timestamp / count / action / reason / type) |
| `configmaps` | ConfigMap metadata (data may be omitted in list responses) |
| `serviceaccounts` | ServiceAccount metadata |

## Filters and pagination

Most list tables support Kubernetes server-side pushdown filters:

- `label_selector` maps to the Kubernetes `labelSelector` query parameter
- `field_selector` maps to the Kubernetes `fieldSelector` query parameter
- `namespace` rewrites the request to the namespaced list path
  (`/api/v1/namespaces/<ns>/<resource>`), which is cheaper for the API server
  and works for users without cluster-wide RBAC

Example:

```sql
SELECT namespace, name, status
FROM k8s.pods
WHERE namespace = 'kube-system'
  AND label_selector = 'k8s-app=kube-dns'
LIMIT 50;
```

### Total-row safety cap (`fetch_limit_default`) vs Kubernetes `limit`

- The Kubernetes `limit` parameter is a **page size**, not a result cap. The API
  server may return a `metadata.continue` token, and Coral paginates through
  the chunks automatically. See
  <https://kubernetes.io/docs/reference/using-api/api-concepts/#retrieving-large-results-sets-in-chunks>.
- This manifest sets `fetch_limit_default: 500` on every table, which caps the
  total rows Coral materializes per query unless the SQL itself sets `LIMIT`.
  This prevents accidental full-cluster scans of high-cardinality tables
  (`pods`, `events`, `configmaps`, etc.) when a user forgets `LIMIT`.
- For very large clusters, set an explicit SQL `LIMIT` and add
  `label_selector` / `field_selector` / `namespace` filters; do not rely on the
  default cap alone, because it deterministically truncates results.

## Example relationships

| From | To | Join hint |
|---|---|---|
| `k8s.pods.node_name` | `k8s.nodes.name` | Node scheduling |
| `k8s.events.object_uid` | `k8s.pods.uid` | Preferred when `object_kind = 'Pod'` |
| `k8s.events.object_name` | `k8s.pods.name` | Fallback; same namespace when kind matches |
| `k8s.services.name` | `k8s.endpoints.name` | Same namespace |

## Example queries

### Failing pods (namespace-scoped, namespace-RBAC friendly)

```sql
SELECT namespace, name, status, status_reason
FROM k8s.pods
WHERE namespace = 'production'
  AND status != 'Running'
LIMIT 20;
```

### Deployment replica mismatch

```sql
SELECT namespace, name, replicas, available_replicas
FROM k8s.deployments
WHERE replicas != available_replicas
LIMIT 50;
```

### Recent warning events (uses new timing/count columns)

`/api/v1/events` mixes core/v1 events (which set `last_timestamp`) and
`events.k8s.io/v1`-style events (which set `event_time` instead). The
`COALESCE(last_timestamp, event_time)` ordering picks whichever timestamp the
event actually populated, so newly-emitted events.k8s.io-style events are not
buried by `NULLS LAST`.

```sql
SELECT namespace, object_kind, object_name, reason, message,
       COALESCE(last_timestamp, event_time) AS event_at, count
FROM k8s.events
WHERE type = 'Warning'
ORDER BY COALESCE(last_timestamp, event_time) DESC NULLS LAST
LIMIT 20;
```

### Pod events

```sql
SELECT namespace, reason, message, object_name,
       COALESCE(last_timestamp, event_time) AS event_at, count
FROM k8s.events
WHERE object_kind = 'Pod'
  AND object_name = 'api-service'
ORDER BY COALESCE(last_timestamp, event_time) DESC NULLS LAST
LIMIT 20;
```

### Pending PVCs

```sql
SELECT namespace, name, phase, storage_class
FROM k8s.persistentvolumeclaims
WHERE phase = 'Pending'
LIMIT 20;
```

### Pods on a node

```sql
SELECT p.namespace, p.name, p.status, n.kubelet_version
FROM k8s.pods p
JOIN k8s.nodes n ON p.node_name = n.name
LIMIT 20;
```

## Validation

```bash
# YAML style (requires: cargo install ryl --locked)
make lint-sources

# Manifest structure and smoke queries (requires Coral CLI)
coral source lint sources/community/k8s/manifest.yaml
kubectl proxy --port=8080 &
coral source add --file sources/community/k8s/manifest.yaml
coral source test k8s
```

## Limitations

- Read-only v1; no mutating Kubernetes API calls.
- No bearer-token or client-cert auth in the manifest; use `kubectl proxy` or a
  pre-authenticated API base URL.
- Large list responses can be heavy; `fetch_limit_default: 500` is a safety cap
  per query — for large clusters, add SQL `LIMIT` and pushdown filters
  (`namespace`, `label_selector`, `field_selector`).
- Nested Kubernetes fields are exposed as `Json` columns for downstream parsing.
- Event timing columns map to core/v1 `Event` fields
  (`firstTimestamp` / `lastTimestamp` / `count`). Some controllers now emit
  events via `events.k8s.io/v1` only; for those, `event_time` and `action` are
  populated and the core/v1 timestamps may be `null`. The `/api/v1/events`
  endpoint surfaces both styles on most clusters.
- Community sources are maintained separately from bundled core sources.

## References

- Kubernetes API reference: <https://kubernetes.io/docs/reference/kubernetes-api/>
- API resource URIs and listing semantics:
  <https://kubernetes.io/docs/reference/using-api/api-concepts/#resource-uris>
- Listing pods in one namespace vs all namespaces:
  <https://kubernetes.io/docs/reference/kubernetes-api/workload-resources/pod-v1/#get-list>,
  <https://kubernetes.io/docs/reference/kubernetes-api/workload-resources/pod-v1/#get-list-all-namespaces>
- Chunked list pagination (`limit` / `continue`):
  <https://kubernetes.io/docs/reference/using-api/api-concepts/#retrieving-large-results-sets-in-chunks>
- Label selectors:
  <https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#label-selectors>
- Field selectors:
  <https://kubernetes.io/docs/concepts/overview/working-with-objects/field-selectors/>
- Event resource (`event_time`, `first_timestamp`, `last_timestamp`, `count`, `action`):
  <https://kubernetes.io/docs/reference/kubernetes-api/cluster-resources/event-v1/>
- `kubectl proxy`:
  <https://kubernetes.io/docs/reference/kubectl/generated/kubectl_proxy/>
- RBAC:
  <https://kubernetes.io/docs/reference/access-authn-authz/rbac/>

## Contributing

Follow [CONTRIBUTING.md](../../../CONTRIBUTING.md): discuss on the issue first,
sign the CLA if this is your first contribution, run `make lint-sources`, and
open a focused PR titled `feat(sources/community/k8s): add kubernetes community source`.
