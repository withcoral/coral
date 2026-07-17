# Netdata (Community)

**Version:** 0.1.0
**Backend:** HTTP (Netdata Agent REST API)
**Tables:** 3
**Base URL:** `{{input.NETDATA_URL}}`

Query Netdata host metadata, active alarms, and the metric context catalog through Coral SQL using the Netdata **Agent** API. Read-only access for infrastructure observability and operational auditing.

Coral exposes read-only `GET` tables. Modifying alarm definitions, collector configuration, or telemetry pipelines, and querying raw historical timeseries, are out of scope.

## Install

```bash
export NETDATA_URL=http://localhost:19999
coral source add --file sources/community/netdata/manifest.yaml
```

## Authentication

**None is required, and this source sends no credentials.**

| Input | Kind | Required | Description |
| --- | --- | --- | --- |
| `NETDATA_URL` | variable | yes | Netdata **Agent** base URL with port, no trailing slash (e.g. `http://localhost:19999`). Point it at the Agent's HTTP endpoint, not at Netdata Cloud. |

This source targets a Netdata **Agent**. Its data APIs (`/api/v1`, `/api/v3`) are public by default — governed by IP-based access control (`allow dashboard from` in `netdata.conf`), not by a token.

Two things that are easy to get wrong:

- **Netdata Cloud API tokens** (from app.netdata.cloud) authenticate against the Netdata **Cloud** API, a different product surface. They do not apply to a self-hosted Agent and are not used here.
- The Agent can optionally enable **bearer protection** (`/api/v3/bearer_protection`) or sit behind an authenticating proxy. Coral does not send a bearer token, so for those setups point `NETDATA_URL` at an endpoint reachable without app-level auth (e.g. an internal address allowed by the Agent's ACL).

Docs: [Netdata Agent REST API](https://learn.netdata.cloud/api) · [Securing Agents](https://learn.netdata.cloud/docs/netdata-agent/securing-netdata-agents)

## Tables

| Table | Endpoint | Pagination |
| --- | --- | --- |
| `netdata.nodes` | `GET /api/v1/info` | None (single object) |
| `netdata.alarms` | `GET /api/v1/alarms?all=true` | None (iterates the alarms dictionary) |
| `netdata.metrics_metadata` | `GET /api/v3/contexts?options=titles` | None (iterates the contexts dictionary) |

Netdata's current API is **v3**. `metrics_metadata` uses `/api/v3/contexts`, which replaces the deprecated `/api/v1/charts` for context metadata. `nodes` and `alarms` still use their `/api/v1` endpoints, which remain functional.

`alarms` and `metrics_metadata` come from JSON **dictionaries** (keyed objects), so Coral uses the `dict_entries` row strategy: each entry's fields are read directly and the dictionary key is exposed via `_key` (surfaced as `alarm_id` / `context_id`).

### `netdata.nodes`

Runtime parameters, OS footprint, and hardware specs of the agent host (`/api/v1/info`).

| Column | Type | Description |
| --- | --- | --- |
| `version` | Utf8 | Netdata agent version |
| `os_name` | Utf8 | OS distribution name (e.g. ubuntu, debian) |
| `os_version` | Utf8 | OS distribution version |
| `kernel_version` | Utf8 | Running kernel version |
| `cpu_cores` | Int64 | Number of CPU cores monitored on the host |

### `netdata.alarms`

Active and configured health alarm states (`/api/v1/alarms?all=true`).

| Column | Type | Description |
| --- | --- | --- |
| `alarm_id` | Utf8 | Alarm dictionary key |
| `alarm_name` | Utf8 | Health alert rule name |
| `chart` | Utf8 | Chart the alarm is attached to |
| `status` | Utf8 | Current alarm state (e.g. WARNING, CRITICAL, CLEAR) |
| `value` | Float64 | Most recent evaluated value |
| `family` | Utf8 | Subsystem group (e.g. cpu, disk, network) |
| `recipient` | Utf8 | Notification routing target |

### `netdata.metrics_metadata`

Metric context catalog from the v3 contexts API (`/api/v3/contexts?options=titles`).

| Column | Type | Description |
| --- | --- | --- |
| `context_id` | Utf8 | Unique context identifier (e.g. `system.cpu`, `disk.io`) |
| `title` | Utf8 | Context title (included via the `titles` option) |
| `units` | Utf8 | Measurement units (e.g. percentage, MiB/s) |
| `family` | Utf8 | Subsystem family (e.g. cpu, disk, network) |
| `priority` | Int64 | Relative display priority (lower sorts higher) |
| `live` | Boolean | Whether the context is currently collecting |

Per-context dimensions are available from the same endpoint via the `dimensions` option but are not modeled as a separate table in this revision.

## Example queries

Active warning or critical alarms:

```sql
SELECT alarm_id, alarm_name, chart, family, status, value
FROM netdata.alarms
WHERE status IN ('WARNING', 'CRITICAL')
ORDER BY value DESC;
```

Percentage-based metric contexts:

```sql
SELECT context_id, title, family, priority
FROM netdata.metrics_metadata
WHERE units = 'percentage'
ORDER BY context_id ASC;
```

Host runtime overview:

```sql
SELECT version, os_name, os_version, kernel_version, cpu_cores
FROM netdata.nodes;
```

## Validation

```bash
make lint-sources
coral source lint sources/community/netdata/manifest.yaml
coral source test netdata
```

Live output:

```text
✓ netdata connected successfully

netdata (3 tables)
├─ nodes
├─ alarms
└─ metrics_metadata

Query tests
1 declared · 1 passed · 0 failed

✓ SELECT os_name, kernel_version FROM netdata.nodes LIMIT 1
  1 row
```

## Limitations

- Read-only retrieval scope.
- No credentials are sent; the Agent API must be reachable under its IP-based ACL. Agent bearer protection and authenticating proxies are not supported.
- Does not expose raw historical timeseries data (`/api/v3/data`).
- `metrics_metadata` uses `/api/v3/contexts`; `nodes` and `alarms` use `/api/v1` endpoints, which Netdata marks deprecated but still serves.
- `alarms` and `metrics_metadata` use Coral's `dict_entries` strategy because Netdata returns keyed dictionaries rather than arrays.
- Large context catalogs may need targeted SQL filtering.
