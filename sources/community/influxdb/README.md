# InfluxDB

Query InfluxDB v2 server health, organizations, buckets (with retention),
and tasks from a self-hosted InfluxDB 2.x instance or InfluxDB Cloud,
through the InfluxDB v2 REST API.

## Setup

### Requirements

- Network access to an InfluxDB v2 HTTP endpoint (default port `8086`).
- An InfluxDB API token (read access is enough).

### Add the Source

Set the inputs as environment variables, then add the source from this
manifest:

```bash
export INFLUXDB_URL=http://localhost:8086
export INFLUXDB_TOKEN=your_influxdb_token
coral source add --file sources/community/influxdb/manifest.yaml
```

Inputs:

- `INFLUXDB_URL` — base URL including scheme and port, e.g.
  `http://localhost:8086` or an InfluxDB Cloud region URL such as
  `https://us-east-1-1.aws.cloud2.influxdata.com`. No trailing slash.
- `INFLUXDB_TOKEN` — API token, sent as `Authorization: Token <token>`.

## Tables

### `health`
Single-row server health and version from `/health`.

**Useful for:**
- Connectivity checks (`status = 'pass'`)
- Version reporting

### `orgs`
Organizations from `/api/v2/orgs`.

**Useful for:**
- Listing organizations the token can see
- Finding the `name` used by the `org` filter on other tables

### `buckets`
Buckets with retention from `/api/v2/buckets`.

**Useful for:**
- Bucket inventory and `system` vs `user` classification
- Auditing retention (`retention_seconds`, `retention_rules`)

Optional filter:
- `org` — organization name; pushed down to the API.

### `tasks`
Tasks from `/api/v2/tasks`.

**Useful for:**
- Task inventory and schedule (`every` / `cron`)
- Spotting inactive tasks or stale `latest_completed`
- Reviewing the `flux` source of a task

Optional filters (pushed down to the API):
- `org` — organization name
- `status` — `active` or `inactive`

## Authentication

InfluxDB v2 uses token authentication. This source sends:

```text
Authorization: Token <INFLUXDB_TOKEN>
```

A read-scoped token is sufficient. Username/password is not used.

## Known limitations

- **`tasks` is capped at 500 rows per request and does not paginate.**
  InfluxDB's tasks API paginates by an `after` cursor (task ID), not by
  `offset`, and Coral's DSL does not currently express that mode. On
  instances with more than 500 tasks the table silently truncates at the
  cap — apply the `org` or `status` filter to narrow results.
- `retention_seconds` exposes the **first** retention rule only. Buckets
  with multiple rules need `retention_rules` (JSON) plus the JSON
  accessor functions to inspect the rest.
- The `buckets.org` column is a `from_filter` echo: it only populates
  when the query uses `WHERE org = '...'` (which pushes the predicate
  down to the API). Without that filter the column is null — join
  `buckets.org_id = orgs.id` to attach organization names.

## Limits

- This source is **read-only**. It exposes health and metadata endpoints
  only — no writing points and no running Flux/SQL queries.
- `buckets` uses `limit`/`offset` pagination (default 20, max 100 per
  page); Coral follows pages until empty.
- Timestamps (`created_at`, `updated_at`, `latest_completed`) are parsed
  from RFC 3339 / ISO 8601 strings into real `Timestamp` columns.
- No server-side filtering beyond the declared filters; filter the rest
  with SQL `WHERE` after fetching.

## Example Queries

### Server health

```sql
SELECT name, status, version, commit FROM influxdb.health
```

### User buckets and their retention (days)

```sql
SELECT name, type,
       retention_seconds / 86400 AS retention_days
FROM influxdb.buckets
WHERE type = 'user'
ORDER BY name
```

### Inactive tasks

```sql
SELECT name, org, every, cron, latest_completed
FROM influxdb.tasks
WHERE status = 'inactive'
ORDER BY name
```

### Tasks for one organization

```sql
SELECT name, status, every, last_run_status
FROM influxdb.tasks
WHERE org = 'demo-org'
```

## Notes

- Verified against InfluxDB 2.7. The `/health`, `/api/v2/orgs`,
  `/api/v2/buckets`, and `/api/v2/tasks` endpoints are stable across
  InfluxDB 2.x and InfluxDB Cloud.
- This is InfluxDB 2.x (v2 API). InfluxDB 1.x and the 3.x SQL endpoints
  are out of scope for this source.
