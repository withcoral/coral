# AppSignal Coral source

Query AppSignal monitoring data with Coral for debugging and incident analysis.
This source uses AppSignal's GraphQL API for read-only observability data.

## Setup

Create a personal API token from AppSignal API settings. The app ID is visible
in app settings and in AppSignal app URLs such as `/sites/<app_id>`.

```bash
APPSIGNAL_API_TOKEN=... \
APPSIGNAL_APP_ID=... \
coral source add --file sources/community/appsignal/manifest.yaml
```

Run validation:

```bash
coral source test appsignal
```

## Tables

| Table | Description |
| --- | --- |
| `appsignal.app_overview` | Configured AppSignal app metadata. |
| `appsignal.deploy_markers` | Deploy markers for release correlation. |
| `appsignal.exception_incidents` | Exception incidents, counts, state, severity, namespace, and timestamps. |
| `appsignal.uptime_monitors` | Partial uptime monitor view with selected alert fields. |

## Example queries

```sql
SELECT number, exception_name, state, namespace, severity, last_occurred_at
FROM appsignal.exception_incidents
WHERE state = 'open'
ORDER BY last_occurred_at DESC
LIMIT 20;
```

```sql
SELECT short_revision, user, exception_count, exception_rate, created_at
FROM appsignal.deploy_markers
WHERE start = '2026-05-01T00:00:00Z' AND end = '2026-05-29T00:00:00Z'
ORDER BY created_at DESC
LIMIT 20;
```

```sql
SELECT name, url, alerts_json
FROM appsignal.uptime_monitors
LIMIT 20;
```

## Notes

- AppSignal requires the personal API token as a `token` query parameter.
- App-scoped GraphQL queries require `APPSIGNAL_APP_ID`.
- `appsignal.deploy_markers` and `appsignal.exception_incidents` pass `limit`
  through Coral's first-page body pagination and expose manual `offset`
  filters for follow-up pages. AppSignal supports offset pagination in
  GraphQL, but Coral's HTTP DSL cannot currently mutate a GraphQL body offset
  automatically across pages.
- `appsignal.exception_incidents` pushes `state`, `order`, and `marker_id` to
  AppSignal. The returned `namespace` column can still be filtered locally.
- `appsignal.uptime_monitors` intentionally selects a partial documented
  monitor and alert shape; `alerts_json` includes only fields selected by this
  GraphQL query, not every field available in AppSignal's schema.
- The source intentionally models documented GraphQL read paths instead of
  undocumented REST incident endpoints.

## Validation evidence

Static validation run locally:

```bash
coral source lint sources/community/appsignal/manifest.yaml
make lint-sources
yamllint sources/community/appsignal/manifest.yaml
git diff --check origin/main..HEAD
gitleaks detect --no-banner --redact --source . --log-opts=origin/main..HEAD
```

Credentialed `coral source add --file`, `coral source test appsignal`, and
representative live queries require an AppSignal token/app ID and were not run
in this workspace.

## API references

- https://docs.appsignal.com/api.html
- https://docs.appsignal.com/api/graphql-explorer.html
- https://docs.appsignal.com/api/graphql/examples.html
