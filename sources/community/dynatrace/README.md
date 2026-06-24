# Dynatrace (Community)

**Version:** 0.1.0
**Backend:** HTTP (Dynatrace Environment API v2)
**Tables:** 3
**Base URL:** configured via `DYNATRACE_BASE_URL` input

Query Dynatrace problems, monitored entities, and software releases via the
Environment API v2. Join with GitHub deploys and Sentry errors for
cross-source SRE and observability intelligence.

## Install

```bash
coral source add --file sources/community/dynatrace/manifest.yaml
```

## Authentication and setup

Requires a Dynatrace API token with read scopes for the resources you plan to
query.

1. In Dynatrace, go to **Access Tokens -> Generate new token**.
2. Enable the following scopes:
   - `problems.read`
   - `entities.read`
   - `releases.read`
3. Copy the generated token.

```bash
export DYNATRACE_BASE_URL=https://abc12345.live.dynatrace.com
export DYNATRACE_API_TOKEN=dt0s01.ST2EY72KQINMH5...
coral source add --file sources/community/dynatrace/manifest.yaml
```

For managed (on-premise) environments use `https://your-host/e/your-environment-id`.

## Tables

| Table | Required filters | Optional provider filters | Description |
| --- | --- | --- | --- |
| `problems` | none | `problem_selector` | Problems from the last two weeks (up to 50 per query) |
| `entities` | `entity_selector` | none | Monitored entities scoped by Dynatrace entity selector syntax |
| `releases` | none | `releases_selector` | Software releases from the last two weeks (up to 200 per query) |

Multi-page cursor pagination is not enabled on any table. Dynatrace requires
`nextPageKey` follow-up requests to omit all other query params, which Coral's
cursor pagination does not do. Each query returns the first page only.
Provider filters use Dynatrace selector syntax and are sent to the API before
Coral applies remaining SQL conditions.

## Validation

```bash
# Add the source and run test queries (output sanitized)
coral source add --file sources/community/dynatrace/manifest.yaml
# coral source test dynatrace produces the same output
```

```text
  ✓ dynatrace connected successfully
  Secrets: keyring

    dynatrace (3 tables)
    ├─ entities
    ├─ problems
    └─ releases

    Query tests
    2 declared · 2 passed · 0 failed

    ✓ SELECT problem_id, title, status, severity_level FROM dynatrace.problems LIMIT 1
      1 row

    ✓ SELECT entity_id, display_name, type FROM dynatrace.entities WHERE entity_selector = 'type(SERVICE)' LIMIT 1
      1 row
```

```bash
# Representative query (output sanitized)
coral sql "SELECT problem_id, title, status, severity_level FROM dynatrace.problems WHERE status = 'OPEN' LIMIT 3"
```

```text
+------------------+------------------------------------+--------+------------------+
| problem_id       | title                              | status | severity_level   |
+------------------+------------------------------------+--------+------------------+
| P-12345678       | High response time on /api/orders  | OPEN   | PERFORMANCE      |
| P-87654321       | Error rate spike in payment-svc    | OPEN   | ERROR            |
| P-11223344       | Host CPU saturation                | OPEN   | RESOURCE_CONT... |
+------------------+------------------------------------+--------+------------------+
3 rows
```

## Example queries

### Open problems by severity

```sql
SELECT problem_id, display_id, title, severity_level, impact_level, start_time_ms
FROM dynatrace.problems
WHERE problem_selector = 'status("open")'
  AND status = 'OPEN'
ORDER BY start_time_ms DESC
LIMIT 20;
```

### All monitored services

```sql
SELECT entity_id, display_name, first_seen_ms, last_seen_ms
FROM dynatrace.entities
WHERE entity_selector = 'type(SERVICE)'
LIMIT 50;
```

### All monitored hosts

```sql
SELECT entity_id, display_name, first_seen_ms, last_seen_ms
FROM dynatrace.entities
WHERE entity_selector = 'type(HOST)'
LIMIT 50;
```

### Releases with problems from the first page

```sql
SELECT name, version, product, stage, problem_count, vulnerability_count
FROM dynatrace.releases
WHERE affected_by_problems = true
ORDER BY problem_count DESC
LIMIT 20;
```

### Problem duration for closed problems

`end_time_ms` is -1 for open problems (not null). Use `NULLIF` when
computing durations:

```sql
SELECT problem_id, title, start_time_ms,
       NULLIF(end_time_ms, -1) AS end_time_ms,
       (NULLIF(end_time_ms, -1) - start_time_ms) / 60000 AS duration_min
FROM dynatrace.problems
WHERE problem_selector = 'status("closed")'
  AND status = 'CLOSED'
ORDER BY start_time_ms DESC
LIMIT 20;
```

## Cross-source examples

### Dynatrace problems overlapping with Sentry errors

Requires the `sentry` source.

```sql
SELECT dt.title AS dynatrace_problem,
       dt.severity_level,
       se.title AS sentry_error,
       se.first_seen
FROM dynatrace.problems dt
JOIN sentry.issues se
  ON CAST(se.first_seen AS TIMESTAMP) >= TO_TIMESTAMP(dt.start_time_ms / 1000)
  AND CAST(se.first_seen AS TIMESTAMP) <= TO_TIMESTAMP(
        COALESCE(NULLIF(dt.end_time_ms, -1), dt.start_time_ms + 3600000) / 1000
      )
WHERE dt.problem_selector = 'status("open")'
  AND dt.status = 'OPEN'
LIMIT 20;
```

### GitHub deploys that opened Dynatrace problems within one hour

`github.repo_deployments` requires `owner` and `repo` filters:

```sql
SELECT d.environment, d.created_at AS deploy_time,
       dt.title AS problem_title, dt.severity_level
FROM github.repo_deployments d
JOIN dynatrace.problems dt
  ON dt.start_time_ms >= CAST(EXTRACT(EPOCH FROM CAST(d.created_at AS TIMESTAMP)) * 1000 AS BIGINT)
  AND dt.start_time_ms <= CAST(EXTRACT(EPOCH FROM CAST(d.created_at AS TIMESTAMP)) * 1000 AS BIGINT) + 3600000
WHERE d.owner = 'myorg'
  AND d.repo = 'myapp'
ORDER BY d.created_at DESC
LIMIT 20;
```

### Dynatrace releases with problems matched against GitHub release tags

`github.releases` requires `owner` and `repo` filters:

```sql
SELECT dr.version, dr.product, dr.stage, dr.problem_count,
       gr.name AS github_release
FROM dynatrace.releases dr
LEFT JOIN github.releases gr ON gr.tag_name = dr.version
WHERE dr.affected_by_problems = true
  AND gr.owner = 'myorg'
  AND gr.repo = 'myapp'
ORDER BY dr.problem_count DESC
LIMIT 20;
```

## Limitations

- **First page only** — multi-page cursor pagination is not enabled because
  Dynatrace requires `nextPageKey` follow-up requests to omit all other query
  params. Problems return up to 50 rows; entities and releases return up to 200
  rows per query. Use `problem_selector`, `entity_selector`, or
  `releases_selector` to narrow the provider response. Other SQL `WHERE`
  conditions run client-side after the first page is fetched.
- **Two-week window for problems** — the manifest explicitly sends `from=now-2w`
  (Dynatrace API default is `now-2h`). Releases default to `now-2w`.
- **end_time_ms = -1 for open problems** — Dynatrace returns -1, not null.
  Use `NULLIF(end_time_ms, -1)` when computing durations or filtering closed
  problems.
- **entity_selector is required** — Dynatrace requires `entitySelector` with a
  `type(...)` or `entityId(...)` criterion on the first page; omitting it
  returns an API error.
- **Millisecond timestamps** — `start_time_ms`, `end_time_ms`, `first_seen_ms`,
  and `last_seen_ms` are Unix epoch milliseconds; use `TO_TIMESTAMP(x / 1000)`
  for SQL timestamp comparisons.
- Community sources are maintained separately from bundled core sources.

## API reference

- [Problems list](https://docs.dynatrace.com/docs/dynatrace-api/environment-api/problems-v2/problems/get-problems-list)
- [Entities list](https://docs.dynatrace.com/docs/dynatrace-api/environment-api/entity-v2/get-entities-list)
- [Releases list](https://docs.dynatrace.com/docs/dynatrace-api/environment-api/releaseapi/get-releaseall)
- [Tokens and authentication](https://docs.dynatrace.com/docs/dynatrace-api/basics/dynatrace-api-authentication)

## Contributing

Follow [CONTRIBUTING.md](../../../CONTRIBUTING.md): discuss on the linked issue
first, sign the CLA if this is your first contribution, run `make lint-sources`,
and open a focused PR titled
`feat(sources/community/dynatrace): add dynatrace community source`.
