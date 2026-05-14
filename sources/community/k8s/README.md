# Kubernetes Connector (Community)

**Version:** 0.1.0
**Backend:** HTTP (Kubernetes REST API)
**Tables:** 16
**Default base URL:** `http://127.0.0.1:8080` (override with `K8S_BASE_URL`)

Community source for querying live Kubernetes cluster state with SQL: workloads,
networking, storage, events, and nodes. Designed for operational triage and
Day-2 workflows without custom kubectl scripts.

## Install

Community sources are not bundled with the Coral binary. Import the manifest from
this directory:

```bash
coral source import sources/community/k8s/manifest.yaml
```

Or copy `manifest.yaml` into your workspace and import the local path.

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
importing:

```bash
export K8S_BASE_URL=http://127.0.0.1:8080
coral source import sources/community/k8s/manifest.yaml
```

### In-cluster or direct API access

Point `K8S_BASE_URL` at the API server or in-cluster service URL
(`https://kubernetes.default.svc`). Ensure the caller identity has Kubernetes
list/get permissions for the resources you query.

### Multi-cluster

Register one Coral source per cluster (for example `k8s_dev`, `k8s_prod`), each
with its own `K8S_BASE_URL`.

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
| `events` | Cluster events for triage |
| `configmaps` | ConfigMap metadata (data may be omitted in list responses) |
| `serviceaccounts` | ServiceAccount metadata |

## Filters and pagination

Most list tables support Kubernetes server-side pushdown filters:

- `label_selector` maps to `labelSelector`
- `field_selector` maps to `fieldSelector`

Example:

```sql
SELECT namespace, name, status
FROM k8s.pods
WHERE label_selector = 'app=api'
LIMIT 50;
```

Tables use Kubernetes `continue` token pagination (`cursor_query`) with a default
page size of 200. Prefer `LIMIT` and filters on large clusters.

## Example relationships

| From | To | Join hint |
|---|---|---|
| `k8s.pods.node_name` | `k8s.nodes.name` | Node scheduling |
| `k8s.events.object_name` | `k8s.pods.name` | When `object_kind = 'Pod'` |
| `k8s.services.name` | `k8s.endpoints.name` | Same namespace |

## Example queries

### Failing pods

```sql
SELECT namespace, name, status, status_reason
FROM k8s.pods
WHERE status != 'Running'
LIMIT 20;
```

### Deployment replica mismatch

```sql
SELECT namespace, name, replicas, available_replicas
FROM k8s.deployments
WHERE replicas != available_replicas
LIMIT 50;
```

### Pod events

```sql
SELECT namespace, reason, message, object_name
FROM k8s.events
WHERE object_kind = 'Pod'
  AND object_name = 'api-service'
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
coral source import sources/community/k8s/manifest.yaml
coral source test k8s
```

## Limitations

- Large list responses can be heavy; use filters and `LIMIT`.
- Nested Kubernetes fields are exposed as `Json` columns for downstream parsing.
- Community sources are maintained separately from bundled core sources.

## Contributing

Follow [CONTRIBUTING.md](../../../CONTRIBUTING.md): discuss on the issue first,
sign the CLA if this is your first contribution, run `make lint-sources`, and
open a focused PR titled `feat(sources/community/k8s): add kubernetes community source`.
