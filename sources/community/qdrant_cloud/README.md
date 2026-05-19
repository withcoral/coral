# Qdrant Cloud

**Version:** 0.1.1
**Backend:** HTTP
**Tables:** 3
**Base URL:** `https://api.cloud.qdrant.io`

Query Qdrant Cloud accounts, clusters, and backups using the Qdrant Cloud
management API.

## Authentication

Requires a `QDRANT_CLOUD_API_KEY` (Cloud management key). This is different
from the per-cluster database API key.

Create one in the Qdrant Cloud Console under
**Access Management → Cloud Management Keys**.

```bash
QDRANT_CLOUD_API_KEY=<key> coral source add --file sources/community/qdrant_cloud/manifest.yaml
```

Or interactively:

```bash
QDRANT_CLOUD_API_KEY=<key> coral source add --file sources/community/qdrant_cloud/manifest.yaml --interactive
```

### Finding your Account ID

`QDRANT_ACCOUNT_ID` is needed for the `clusters` and `backups` tables. You
can discover it after installing the source by querying `qdrant_cloud.accounts`:

```bash
coral sql "SELECT id, name FROM qdrant_cloud.accounts"
```

Then re-add with the account ID:

```bash
QDRANT_CLOUD_API_KEY=<key> QDRANT_ACCOUNT_ID=<account_id> \
  coral source add --file sources/community/qdrant_cloud/manifest.yaml
```

## Rate limits

The Qdrant Cloud management API is rate-limited per API key. Avoid running
large fan-out queries in tight loops. The `clusters` endpoint returns all
clusters in a single response with no server-side pagination, so filter in
SQL when working with large accounts.

## Tables

| Table | Description | Requires `QDRANT_ACCOUNT_ID` |
|---|---|---|
| `accounts` | Qdrant Cloud accounts accessible with the management key | No |
| `clusters` | Qdrant Cloud clusters in the account | Yes |
| `backups` | Cluster backup snapshots in the account | Yes |

## Quick start

```bash
# Confirm connectivity and discover your account ID
coral sql "SELECT id, name, owner_email FROM qdrant_cloud.accounts"

# List all clusters with health status and endpoint
coral sql "
  SELECT id, name, cloud_provider_id, cloud_provider_region_id,
         version, number_of_nodes, state_phase, endpoint_url
  FROM qdrant_cloud.clusters
"

# Find unhealthy clusters
coral sql "
  SELECT id, name, state_phase, state_nodes_up, number_of_nodes
  FROM qdrant_cloud.clusters
  WHERE state_phase != 'CLUSTER_PHASE_HEALTHY'
"

# List backups with status
coral sql "
  SELECT id, name, cluster_id, status, backup_duration, created_at
  FROM qdrant_cloud.backups
  ORDER BY created_at DESC
"

# Join clusters and backups
coral sql "
  SELECT c.name as cluster_name, b.status, b.backup_duration, b.created_at
  FROM qdrant_cloud.backups b
  JOIN qdrant_cloud.clusters c ON b.cluster_id = c.id
  ORDER BY b.created_at DESC
"
```

## Discovery order

```text
accounts
  → id → QDRANT_ACCOUNT_ID
    → clusters
      → id (cluster_id)
        → backups (WHERE cluster_id = '...')
```
