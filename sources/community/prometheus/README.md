# Prometheus Connector (Community)

**Version:** 0.1.0
**Backend:** HTTP (Prometheus query API)
**Tables:** 3
**Default base URL:** `http://127.0.0.1:9090` (override with `PROMETHEUS_BASE_URL`)

Query Prometheus scrape health, firing alerts, and instant PromQL results with SQL.
Read-only v1 uses unauthenticated HTTP against a base URL that already handles
auth (typically a local Prometheus server or an authenticating gateway). Pairs
with the community **k8s** source for alert-to-pod triage when rules expose
`pod` and `namespace` labels.

## Install

Community sources are not bundled with the Coral binary. Add the manifest from
this directory:

```bash
coral source add --file sources/community/prometheus/manifest.yaml
```

Or copy `manifest.yaml` into your workspace and pass that path to
`coral source add --file`.

Reference the linked GitHub issue in your PR so maintainers can connect the
contribution to the prior discussion.

## Authentication and setup

### Local development (recommended for contributors)

A local Prometheus server on port 9090 usually needs no extra Coral auth:

```bash
export PROMETHEUS_BASE_URL=http://127.0.0.1:9090
coral source add --file sources/community/prometheus/manifest.yaml
```

Confirm `http://127.0.0.1:9090/-/healthy` responds before running queries.

### Authenticated gateways (advanced)

v1 does not send `Authorization` headers from Coral. For secured Prometheus,
point `PROMETHEUS_BASE_URL` at a reverse proxy or gateway that authenticates on
your behalf. Bearer-token support in the manifest is a potential follow-on.

### Multiple Prometheus instances

Register one Coral source per server (for example `prometheus_dev`,
`prometheus_prod`), each with its own `PROMETHEUS_BASE_URL`.

## Table categories

### Instant queries

| Table | Description |
| --- | --- |
| `query_up` | Scrape health via fixed PromQL `up` |
| `query_custom` | Arbitrary instant query via required `promql` filter |

### Alerts

| Table | Description |
| --- | --- |
| `alerts` | Firing and pending alerts from PromQL `ALERTS` |

## Filters and pagination

`query_custom` requires a `promql` filter with the full PromQL expression.
`query_up` and `alerts` use fixed queries.

Example:

```sql
SELECT metric_name, instance, sample_value
FROM prometheus.query_custom
WHERE promql = 'kube_pod_status_phase{phase="Pending"}'
LIMIT 20;
```

Each table calls `GET /api/v1/query` once. Pagination is `none`; use `LIMIT` and
aggregations in PromQL to control load.

## Example relationships

| From | To | Join hint |
| --- | --- | --- |
| `prometheus.alerts.pod` | `k8s.pods.name` | When alert rules set `pod` |
| `prometheus.alerts.namespace` | `k8s.pods.namespace` | Scope alerts to a namespace |

## Example queries

### Scrape health (`up`)

```sql
SELECT metric_name, instance, job, sample_value
FROM prometheus.query_up
LIMIT 20;
```

### Targets not reporting `up`

```sql
SELECT instance, job, sample_value
FROM prometheus.query_up
WHERE sample_value != '1'
LIMIT 20;
```

### Firing alerts

```sql
SELECT alert_name, alert_state, severity, namespace, pod, instance
FROM prometheus.alerts
WHERE alert_state = 'firing'
LIMIT 20;
```

### Custom PromQL

```sql
SELECT promql, metric_name, instance, pod, sample_value
FROM prometheus.query_custom
WHERE promql = 'sum(rate(container_cpu_usage_seconds_total[5m])) by (pod, namespace)'
LIMIT 10;
```

## Validation

```bash
make lint-sources
coral source lint sources/community/prometheus/manifest.yaml
export PROMETHEUS_BASE_URL=http://127.0.0.1:9090
coral source add --file sources/community/prometheus/manifest.yaml
coral source test prometheus
```

## Limitations

- Read-only v1; instant queries (`/api/v1/query`) only, not `query_range`.
- No bearer-token auth in the manifest; use a local server or authenticated gateway.
- Sample timestamps and values are strings from Prometheus value tuples.
- `query_custom` does not validate PromQL; errors surface at query time.
- High-cardinality PromQL can overload Prometheus; use aggregations and `LIMIT`.

## Contributing

Follow [CONTRIBUTING.md](../../../CONTRIBUTING.md): discuss on the issue first,
sign the CLA if this is your first contribution, run `make lint-sources`, and
open a focused PR titled `feat(sources/community/prometheus): add prometheus community source`.
