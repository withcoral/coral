# Netdata (Community)

**Version:** 0.2.0
**Backend:** HTTP (Netdata Agent REST API v3)
**Tables:** 3
**Base URL:** `{{input.NETDATA_URL}}`

Query Netdata node topology, health alerts, and the metric context catalog through Coral SQL using the Netdata **Agent** v3 API. Read-only access for infrastructure observability and operational auditing.

Coral exposes read-only `GET` tables. Modifying alert definitions, collector configuration, or telemetry pipelines, and querying raw historical timeseries, are out of scope.

## Install

```bash
export NETDATA_URL=http://localhost:19999
coral source add --file sources/community/netdata/manifest.yaml
```

Requires **Netdata v2.0 or newer** — the v3 API does not exist on older agents.

## Authentication

**None is required, and this source sends no credentials.**

| Input | Kind | Required | Description |
| --- | --- | --- | --- |
| `NETDATA_URL` | variable | yes | Netdata **Agent** base URL with port, no trailing slash (e.g. `http://localhost:19999`). Point it at the Agent's HTTP endpoint, not at Netdata Cloud. |

This source targets a Netdata **Agent**. Its v3 data APIs are public by default — governed by IP-based access control (`allow dashboard from` in `netdata.conf`), not by a token.

Two things that are easy to get wrong:

- **Netdata Cloud API tokens** (from app.netdata.cloud) authenticate against the Netdata **Cloud** API, a different product surface. They do not apply to a self-hosted Agent and are not used here.
- The Agent can optionally enable **bearer protection** (`/api/v3/bearer_protection`) or sit behind an authenticating proxy. Coral does not send a bearer token, so for those setups point `NETDATA_URL` at an endpoint reachable without app-level auth (e.g. an internal address allowed by the Agent's ACL).

Docs: [Netdata Agent REST API](https://learn.netdata.cloud/api) · [Securing Agents](https://learn.netdata.cloud/docs/netdata-agent/securing-netdata-agents)

## Tables

| Table | Endpoint | Pagination |
| --- | --- | --- |
| `netdata.nodes` | `GET /api/v3/nodes?options=long-keys` | None (iterates the `nodes` array) |
| `netdata.alarms` | `GET /api/v3/alerts?options=instances,values,long-keys,rfc3339` | None (iterates `alert_instances`) |
| `netdata.metrics_metadata` | `GET /api/v3/contexts?options=titles` | None (iterates the contexts dictionary) |

All three tables use **v3** endpoints. Netdata's OpenAPI marks v1 and v2 as deprecated and back-compat only, and states new integrations should use v3 exclusively.

### Request options

The v3 contexts/nodes/alerts endpoints share an options parameter that materially changes the response:

- **`long-keys`** — v3 emits short member names by default (`nm`, `ni`, `st`). This source requests long keys so the mapping is readable and stable.
- **`instances`** (alerts) — without it the response carries only alert names and indexes; it adds `status`, `context`, `family`, `units`, `info`, `summary`, and the transition fields.
- **`values`** (alerts) — adds `last_updated_value` and `last_updated_timestamp`.
- **`rfc3339`** (alerts) — emits timestamps as ISO-8601 strings rather than epoch seconds.

### `netdata.nodes`

Nodes monitored by the agent (`/api/v3/nodes`). A standalone Agent reports itself as a single node; a parent also reports its children.

| Column | Type | Description |
| --- | --- | --- |
| `node_index` | Int64 | Index uniquely identifying this node for the query |
| `hostname` | Utf8 | Node hostname |
| `node_id` | Utf8 | Node id (absent unless registered to Netdata Cloud) |
| `machine_guid` | Utf8 | Machine GUID |
| `state` | Utf8 | Node state on this Agent (`reachable`, `stale`) |
| `version` | Utf8 | Netdata Agent version the node runs |
| `os_name` | Utf8 | Operating system name |
| `os_id` | Utf8 | Operating system id |
| `os_version` | Utf8 | Operating system version |
| `kernel_name` | Utf8 | Kernel name |
| `kernel_version` | Utf8 | Kernel version |
| `architecture` | Utf8 | CPU architecture |
| `cpu_cores` | Utf8 | CPU core count — the API reports this as a **string**, not a number |
| `memory_total` | Utf8 | Total host RAM as reported by the Agent |
| `virtualization` | Utf8 | Detected virtualization technology |
| `container` | Utf8 | Detected container technology |

OS and hardware fields arrive nested (`os.nm`, `os.kernel.v`, `hw.cpus`) and are flattened into the columns above.

### `netdata.alarms`

Health alert instances (`/api/v3/alerts`), one row per alert instance.

| Column | Type | Description |
| --- | --- | --- |
| `alarm_name` | Utf8 | Health alert rule name |
| `node_index` | Int64 | Index of the node the alert belongs to |
| `instance_name` | Utf8 | Chart the alert is attached to |
| `instance_id` | Utf8 | Chart id the alert is attached to |
| `context` | Utf8 | Metric context the alert is attached to |
| `status` | Utf8 | Alert state (WARNING, CRITICAL, CLEAR, UNDEFINED, UNINITIALIZED, REMOVED) |
| `value` | Float64 | Most recent evaluated value |
| `last_updated` | Timestamp | When the alert was last evaluated |
| `last_transition_value` | Float64 | Value at the last status change |
| `last_transition` | Timestamp | When the alert last changed status |
| `family` | Utf8 | Subsystem group (e.g. cpu, disk) |
| `info` | Utf8 | Human-readable description |
| `summary` | Utf8 | Short summary |
| `units` | Utf8 | Units of the evaluated value |
| `recipient` | Utf8 | Notification routing target |
| `type` | Utf8 | Alert type from its configuration |
| `component` | Utf8 | Alert component from its configuration |
| `classification` | Utf8 | Alert classification from its configuration |
| `source` | Utf8 | Configuration file the alert was defined in |

### `netdata.metrics_metadata`

Metric context catalog (`/api/v3/contexts?options=titles`). Returned as a keyed dictionary, so Coral uses the `dict_entries` row strategy and exposes the key via `_key` as `context_id`.

| Column | Type | Description |
| --- | --- | --- |
| `context_id` | Utf8 | Unique context identifier (e.g. `system.cpu`, `disk.io`) |
| `title` | Utf8 | Context title (via the `titles` option) |
| `units` | Utf8 | Measurement units (e.g. percentage, MiB/s) |
| `family` | Utf8 | Subsystem family (e.g. cpu, disk, network) |
| `priority` | Int64 | Display priority (lower sorts higher) |
| `live` | Boolean | Whether the context is currently collecting |

Per-context dimensions are available from the same endpoint via the `dimensions` option but are not modeled as a separate table in this revision.

## Example queries

Active warning or critical alerts:

```sql
SELECT alarm_name, instance_name, context, status, value, units
FROM netdata.alarms
WHERE status IN ('WARNING', 'CRITICAL')
ORDER BY last_transition DESC;
```

Node inventory:

```sql
SELECT hostname, state, version, os_name, os_version, kernel_version
FROM netdata.nodes
ORDER BY hostname;
```

Percentage-based metric contexts:

```sql
SELECT context_id, title, family, priority
FROM netdata.metrics_metadata
WHERE units = 'percentage'
ORDER BY context_id ASC;
```

## Validation

```bash
make lint-sources
coral source lint sources/community/netdata/manifest.yaml
coral source test netdata
```

Live output:

```text
<PASTE: coral source test netdata>
```

`netdata.nodes`:

```text
<PASTE: coral sql "SELECT hostname, state, os_name, kernel_version FROM netdata.nodes LIMIT 3">
```

`netdata.alarms`:

```text
<PASTE: coral sql "SELECT alarm_name, instance_name, status, value FROM netdata.alarms LIMIT 3">
```

## Limitations

- Read-only retrieval scope.
- No credentials are sent; the Agent API must be reachable under its IP-based ACL. Agent bearer protection and authenticating proxies are not supported.
- Requires Netdata v2.0+; the v3 API is absent on older agents.
- Does not expose raw historical timeseries data (`/api/v3/data`).
- `nodes.cpu_cores` and `memory_total` are strings because the Agent reports them as strings.
- `alarms` depends on the `instances` and `values` options; several columns are null without them.
- `metrics_metadata` uses the `dict_entries` strategy because contexts are returned as a keyed dictionary rather than an array.
- Large context catalogs may need targeted SQL filtering.
