# AppSignal Coral source

Query AppSignal monitoring data with Coral for debugging and incident analysis.
This source uses AppSignal's GraphQL API for read-only observability data.

## Setup

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
| `appsignal.uptime_monitors` | Uptime monitors and alert payloads. |

## Example queries

```sql
SELECT number, exception_name, state, namespace, severity, last_occurred_at
FROM appsignal.exception_incidents
ORDER BY last_occurred_at DESC
LIMIT 20;
```

```sql
SELECT short_revision, user, exception_count, exception_rate, created_at
FROM appsignal.deploy_markers
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
- The source intentionally models documented GraphQL read paths instead of
  undocumented REST incident endpoints.

## API references

- https://docs.appsignal.com/api.html
- https://docs.appsignal.com/api/graphql-explorer.html
- https://docs.appsignal.com/api/graphql/examples.html
