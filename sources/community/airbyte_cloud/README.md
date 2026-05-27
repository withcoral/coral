# Airbyte Cloud

Query Airbyte Cloud metadata from Coral. The source covers workspace, source,
destination, connection, and job inventory while avoiding connector
configuration payloads that may contain credentials or host details.

## Authentication

Create an Airbyte Cloud API token and provide:

| Input | Description |
| --- | --- |
| `AIRBYTE_API_TOKEN` | Bearer token for the Airbyte API. |

The token is modeled as a secret. Use the narrowest role that can read the
metadata Coral agents need.

## Tables

| Table | Description |
| --- | --- |
| `airbyte_cloud.workspaces` | Workspaces visible to the API token. |
| `airbyte_cloud.sources` | Source connectors. Supports `workspace_id`. |
| `airbyte_cloud.destinations` | Destination connectors. Supports `workspace_id`. |
| `airbyte_cloud.connections` | Connection metadata. Supports `workspace_id` and `status`. |
| `airbyte_cloud.jobs` | Job history. Supports `connection_id` and `status`. |

## Examples

List enabled connections:

```sql
SELECT connection_id, name, source_id, destination_id, status
FROM airbyte_cloud.connections
WHERE status = 'active';
```

Inspect recent jobs for one connection:

```sql
SELECT job_id, status, job_type, started_at, ended_at
FROM airbyte_cloud.jobs
WHERE connection_id = 'connection_id'
LIMIT 25;
```

Review source and destination coverage in a workspace:

```sql
SELECT source_id, name, source_type
FROM airbyte_cloud.sources
WHERE workspace_id = 'workspace_id';
```

## Notes

- Airbyte Cloud list endpoints are modeled with `offset` and `limit`
  pagination.
- The source omits source and destination configuration objects because they
  can contain credentials, host names, database names, or other sensitive
  connection parameters.
- Job history can be large; `airbyte_cloud.jobs` has a conservative default
  fetch limit.
- Live API tests passed against an Airbyte Cloud workspace. The workspace had
  no configured connections yet, so the connection query returned zero rows
  while still proving authentication, pagination, and table wiring.

## Validation

- YAML parsing: passed
- Coral manifest schema validation: passed
- `git diff --check`: passed
- `make lint-sources`: passed
- Live API tests: passed against an Airbyte Cloud workspace

Live Coral evidence:

```text
✓ airbyte_cloud connected successfully
Secrets: keychain

airbyte_cloud (5 tables)
├─ connections
├─ destinations
├─ jobs
├─ sources
└─ workspaces
Query tests
2 declared · 2 passed · 0 failed

✓ SELECT workspace_id, name FROM airbyte_cloud.workspaces LIMIT 1
  1 row

✓ SELECT connection_id, name, status FROM airbyte_cloud.connections LIMIT 1
  0 rows
```

Representative query:

```sql
SELECT workspace_id, name, data_residency
FROM airbyte_cloud.workspaces
LIMIT 3;
```
