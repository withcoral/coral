# Healthchecks.io source

Query Healthchecks.io cron job and background task monitoring data through
Coral SQL.

This community source uses Healthchecks.io Management API v3 and only performs
read-only `GET` requests supported by Healthchecks.io read-only API keys.

## Configuration

Create a project API key from:

`Project Settings -> API Access`

A read-only API key is sufficient for this source.

```bash
export HEALTHCHECKS_IO_API_KEY="your-project-api-key"

coral source add --file sources/community/healthchecks_io/manifest.yaml
coral source test healthchecks_io
```

## Example Queries

List monitored jobs:

```sql
SELECT
  name,
  slug,
  status,
  tags,
  last_ping,
  next_ping
FROM healthchecks_io.checks
ORDER BY name
LIMIT 50;
```

Filter checks by tag:

```sql
SELECT name, slug, status, last_ping, badge_url
FROM healthchecks_io.checks
WHERE tag = 'production'
LIMIT 50;
```

Review status changes:

```sql
SELECT timestamp, up
FROM healthchecks_io.flips
WHERE check_id = 'check-uuid-or-unique-key'
  AND seconds = '86400';
```

## Tables

- `healthchecks_io.checks`
- `healthchecks_io.flips`

## Notes

- `healthchecks_io.flips` accepts either a check UUID or read-only `unique_key`.
- Healthchecks.io asks API clients to stay under 100 API requests per minute.
  Queries that inspect flips for many checks should be batched thoughtfully to
  avoid HTTP 429 rate-limit responses.
- Read-only API keys omit sensitive URL fields such as `uuid`, `ping_url`,
  `update_url`, `pause_url`, `resume_url`, and `channels`.
