# OpenSearch

**Version:** 0.1.0
**Backend:** HTTP
**Tables:** 1
**Base URL:** Configurable via `OPENSEARCH_URL` (e.g., `http://localhost:9200`)

Query application and infrastructure logs directly from an active OpenSearch REST API using Coral SQL. 

## Configuration

This source does not strictly require an API key for local/unauthenticated clusters, but it does rely on environment variables to target your specific OpenSearch instance and index pattern.

You can configure the connector using the following inputs:
* `OPENSEARCH_URL`: Base URL for the OpenSearch cluster (Default: `http://opensearch:9200`)
* `OPENSEARCH_INDEX`: The index pattern to query (Default: `logs-*`)

```bash
OPENSEARCH_URL="http://localhost:9200" OPENSEARCH_INDEX="logs-*" coral source add --file sources/community/opensearch/manifest.yaml
```

Or interactively:

```bash
coral source add --file sources/community/opensearch/manifest.yaml --interactive
```

## Tables

| Table | Description | Required filters | Optional filters |
|---|---|---|---|
| `logs` | Flattens the OpenSearch `/_search` JSON document response into a relational table format, exposing core log metadata. | — | — |

### `logs` columns

| Column | Type | Description |
|---|---|---|
| `id` | Utf8 | The unique document ID (`_id`) |
| `timestamp` | Utf8 | The log timestamp (`_source.@timestamp`) |
| `level` | Utf8 | The severity level of the log (`_source.level`) |
| `message` | Utf8 | The raw log message (`_source.message`) |
| `pod_name` | Utf8 | The Kubernetes pod name (`_source.kubernetes.pod_name`) |

## Quick start

```bash
# Step 1 — Check your connection and get a broad overview of recent logs
coral sql "SELECT timestamp, level, pod_name FROM opensearch.logs LIMIT 10"

# Step 2 — Isolate critical errors across your infrastructure
coral sql "
  SELECT timestamp, pod_name, message
  FROM opensearch.logs
  WHERE level = 'ERROR'
  ORDER BY timestamp DESC
  LIMIT 5
"
```

## Example queries

### View recent production errors

```sql
SELECT
  timestamp,
  pod_name,
  level,
  message
FROM opensearch.logs
WHERE level = 'ERROR'
ORDER BY timestamp DESC
LIMIT 20;
```

### Investigate a specific Kubernetes Pod

```sql
SELECT
  id,
  timestamp,
  level,
  message
FROM opensearch.logs
WHERE pod_name = 'payment-service-abc-999'
ORDER BY timestamp DESC;
```

### Join OpenSearch logs with external data (Data Federation)
*Note: This assumes you have another source registered, such as a local Git deployment file.*

```sql
SELECT 
  l.timestamp, 
  l.message as error_log, 
  g.commit_id, 
  g.author 
FROM opensearch.logs l
JOIN git_history.deployments g 
  ON g.repo = 'payment-service'
WHERE l.level = 'ERROR' 
  AND l.timestamp >= g.committed_at
ORDER BY l.timestamp DESC 
LIMIT 1;
```
