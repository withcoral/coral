# Prometheus Connector (Community)

**Version:** 0.1.2
**Backend:** HTTP (Prometheus query and alerts APIs)
**Tables:** 2
**Default base URL:** `http://127.0.0.1:9090` (override with `PROMETHEUS_BASE_URL`)

Query Prometheus scrape health and active alerts with SQL. Read-only v1 uses
unauthenticated HTTP against a base URL that already handles auth (typically a
local Prometheus server or an authenticating gateway).

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

## Tables

| Table | Description |
| --- | --- |
| `query_up` | Scrape health via PromQL `up` (`limit=500` on the query API) |
| `alerts` | Active alerts from `GET /api/v1/alerts` (Coral fetch cap only; no API `limit`) |

## Load control

`query_up` sends Prometheus’s native `limit=500` query parameter on `/api/v1/query`
so the server does not return unbounded series before Coral applies
`fetch_limit_default` or SQL `LIMIT`.

`alerts` uses `GET /api/v1/alerts`, which does not accept a `limit` parameter.
Coral caps rows at 200 via `fetch_limit_default`; use SQL `LIMIT` on large alert sets.

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
WHERE sample_value < 1
LIMIT 20;
```

> **Caveat:** `query_up` hard-codes Prometheus `limit=500` on `/api/v1/query`,
> and the SQL `WHERE` filter is applied *after* Coral fetches the response.
> Prometheus does not guarantee instant-vector ordering, so on a fleet with
> more than 500 `up` series a down target can be truncated server-side before
> Coral evaluates `sample_value < 1`. For fleets above ~500 targets, run
> `up < 1` directly in Prometheus or treat this query as best-effort within
> the provider-side cap.

### Firing alerts

```sql
SELECT alert_name, alert_state, severity, namespace, pod, summary, active_at
FROM prometheus.alerts
WHERE alert_state = 'firing'
LIMIT 20;
```

> The `/api/v1/alerts` endpoint only returns active alerts, so `alert_state`
> values are `firing` or `pending`. Inactive alerts are not returned by this
> API.

### Optional join to a Kubernetes source

If you also install a community `k8s` source (not bundled on Coral `main`), you
can correlate alert labels to pods:

```sql
SELECT a.alert_name, a.pod, a.namespace, p.status
FROM prometheus.alerts a
JOIN k8s.pods p
  ON a.pod = p.name AND a.namespace = p.namespace
WHERE a.alert_state = 'firing'
LIMIT 20;
```

## Validation

```bash
make lint-sources
coral source lint sources/community/prometheus/manifest.yaml
export PROMETHEUS_BASE_URL=http://127.0.0.1:9090
coral source add --file sources/community/prometheus/manifest.yaml
coral source test prometheus
```

Sanitized output from a real Prometheus instance (`coral source test
prometheus` plus representative `query_up` and `alerts` queries) is included
in the pull request description so reviewers can see end-to-end behavior
without re-running the suite.

## Limitations

- Read-only v1; instant queries (`/api/v1/query`) for `query_up` only, not `query_range`.
- No arbitrary PromQL table in v1 (`query_custom` removed; use Prometheus directly or a follow-on).
- `alerts` uses `/api/v1/alerts`, not the internal `ALERTS` time series.
- Native histogram samples populate `sample_histogram`; classic samples use `sample_at` / `sample_value`.
- `sample_value_raw` preserves the JSON string for special float encodings.
- No bearer-token auth in the manifest; use a local server or authenticated gateway.

## Contributing

Follow [CONTRIBUTING.md](../../../CONTRIBUTING.md): discuss on the issue first,
sign the CLA if this is your first contribution, run `make lint-sources`, and
open a focused PR titled `feat(sources/community/prometheus): add prometheus community source`.
