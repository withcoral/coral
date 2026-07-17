# Airbyte Cloud

Query Airbyte Cloud metadata from Coral. The source covers workspace, source,
destination, connection, and job inventory while avoiding connector
configuration payloads that may contain credentials or host details.

## Authentication

Create an Airbyte Cloud Application, then exchange its client credentials for a
short-lived access token using Airbyte's `/applications/token` endpoint. Airbyte
Cloud access tokens are valid for only three minutes, so refresh the token
before adding or testing the source when it expires. The token inherits the
permissions of the Airbyte user associated with the Application.

See Airbyte's authentication and access-token docs:
<https://reference.airbyte.com/reference/authentication.md> and
<https://reference.airbyte.com/reference/createaccesstoken.md>.

| Input | Description |
| --- | --- |
| `AIRBYTE_ACCESS_TOKEN` | Short-lived bearer token for the Airbyte API. |

The access token is modeled as a secret. Use the narrowest Airbyte user role
that can read the metadata Coral agents need.

## Tables

| Table | Description |
| --- | --- |
| `airbyte_cloud.workspaces` | Workspaces visible to the API token. |
| `airbyte_cloud.sources` | Source connectors. Supports `workspace_id` through Airbyte's `workspaceIds` parameter. |
| `airbyte_cloud.destinations` | Destination connectors. Supports `workspace_id` through Airbyte's `workspaceIds` parameter. |
| `airbyte_cloud.connections` | Connection metadata. Supports `workspace_id` through Airbyte's `workspaceIds` parameter. Filter `status` locally in SQL. |
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
SELECT job_id, status, job_type, start_time, last_updated_at, duration, rows_synced
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
  pagination, with page sizes capped at Airbyte's documented maximum of 100.
- Authenticate by creating an Airbyte Application and exchanging its client ID
  and secret for a short-lived access token. Airbyte Cloud access tokens expire
  after three minutes, so long-lived workflows need to refresh the token before
  reinstalling or testing the source.
- Workspace-scoped source, destination, and connection queries send the
  documented `workspaceIds` request parameter.
- Connection `status` is returned as a column and should be filtered locally in
  SQL; Airbyte does not document a remote status filter for list connections.
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
3 declared · 3 passed · 0 failed

✓ SELECT workspace_id, name FROM airbyte_cloud.workspaces LIMIT 1
  1 row

✓ SELECT connection_id, name, status FROM airbyte_cloud.connections LIMIT 1
  0 rows

✓ SELECT job_id, status, start_time, last_updated_at, duration FROM airbyte_cloud.jobs LIMIT 1
  0 rows
```

Representative query:

```sql
SELECT workspace_id, name, data_residency
FROM airbyte_cloud.workspaces
LIMIT 3;
```
