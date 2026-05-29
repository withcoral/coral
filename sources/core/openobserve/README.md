# OpenObserve

This source connects Coral to an OpenObserve instance over HTTP.

## Auth

Set `OPENOBSERVE_USERNAME` and `OPENOBSERVE_PASSWORD`. Coral sends them with HTTP Basic auth.

Example:

```text
export OPENOBSERVE_USERNAME="user@example.com"
export OPENOBSERVE_PASSWORD="password"
```

## Inputs

- `OPENOBSERVE_URL`: Base URL for the OpenObserve instance, for example `http://localhost:5080`
- `OPENOBSERVE_ORG`: OpenObserve organization name. Defaults to `default`
- `OPENOBSERVE_USERNAME`: Username or email for HTTP Basic auth
- `OPENOBSERVE_PASSWORD`: Password for HTTP Basic auth

## Example Queries

List streams:

```sql
SELECT name, stream_type, doc_num
FROM openobserve.streams
ORDER BY doc_num DESC
```

Search logs:

```sql
SELECT _timestamp, body, severity, service_name
FROM openobserve.logs
WHERE stream = 'default'
  AND start_time = 1700000000000000
  AND end_time = 1700003600000000
LIMIT 10
```

Search metrics:

```sql
SELECT _timestamp, metric_name, value, service_name
FROM openobserve.metrics
WHERE stream = 'my_metric_stream'
  AND start_time = 1700000000000000
  AND end_time = 1700003600000000
LIMIT 10
```

Search traces:

```sql
SELECT _timestamp, trace_id, span_id, operation_name, duration
FROM openobserve.traces
WHERE stream = 'default'
  AND start_time = 1700000000000000
  AND end_time = 1700003600000000
LIMIT 10
```
