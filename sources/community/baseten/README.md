# Baseten

Monitor and query your [Baseten](https://www.baseten.co) models, deployments, and secrets via the Management API.

## Setup

```bash
coral source add --file sources/community/baseten/manifest.yaml
# Enter when prompted:
#   BASETEN_API_KEY → your API key from https://app.baseten.co/settings/account/api_keys
```

## Usage

```sql
-- List all deployed models
SELECT id, name, instance_type_name, deployments_count
FROM baseten.models;

-- Find models running on A100 GPUs
SELECT id, name, deployments_count
FROM baseten.models
WHERE instance_type_name = 'gpu.a100.1x';

-- Check deployment status for a model
SELECT id, name, status, active_replica_count, instance_type_name
FROM baseten.deployments
WHERE model_id = 'YOUR_MODEL_ID';

-- Find scaled-to-zero deployments
SELECT id, name, status, min_replicas, max_replicas
FROM baseten.deployments
WHERE model_id = 'YOUR_MODEL_ID'
  AND status = 'SCALED_TO_ZERO';

-- List all registered secrets (names only, no values)
SELECT name, created_at, team_name
FROM baseten.secrets;

-- Cross-source: models + their deployment details
SELECT m.name AS model_name, d.status, d.active_replica_count, d.instance_type_name
FROM baseten.models m
JOIN baseten.deployments d ON d.model_id = m.id
WHERE d.is_production = true;
```

## Tables

| Table | Description |
|-------|-------------|
| `models` | All models in your Baseten account |
| `deployments` | Deployments for a specific model (requires `model_id` filter) |
| `secrets` | Registered secret names and metadata (values are never exposed) |

## Authentication

Get your API key from **app.baseten.co → Settings → API Keys**.

## Notes

- `deployments` requires the `model_id` filter — get IDs from `baseten.models`
- `autoscaling_settings` fields (`min_replicas`, `max_replicas`, `target_concurrency`) are flattened into the `deployments` table using nested `expr` paths
- Secret values are never returned by the API for security reasons
