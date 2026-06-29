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

-- Find scaled-to-zero deployments (get model_id from baseten.models first)
SELECT id, name, status, min_replicas, max_replicas
FROM baseten.deployments
WHERE model_id = '<model_id from baseten.models>'
  AND status = 'SCALED_TO_ZERO';

-- List all registered secrets (names only, no values)
SELECT name, created_at, team_name
FROM baseten.secrets;

-- Production deployment details for a model (get the model id from baseten.models first)
SELECT id, name, status, active_replica_count, instance_type_name
FROM baseten.deployments
WHERE model_id = '<model_id from baseten.models>'
  AND is_production = true;
```

## Tables

| Table | Description |
|-------|-------------|
| `models` | All models in your Baseten account |
| `deployments` | Deployments for a specific model (requires `model_id` filter) |
| `secrets` | Registered secret names and metadata (values are never exposed) |

## Authentication

Get your API key from **app.baseten.co → Settings → API Keys**.

Use a personal API key, or a team API key with **"Full access"** permissions.
"Inference only" and "Metrics only" team keys cannot list models, deployments,
or secrets and will fail against this source. See
[Baseten API keys](https://docs.baseten.co/organization/api-keys) for details
on key types and how to create one.

## Notes

- `deployments` requires the `model_id` filter — get IDs from `baseten.models`.
  Required filters must be a literal value, not a subquery or join — query
  `baseten.models` first, then use that `id` in a separate `baseten.deployments`
  query.
- `autoscaling_settings` fields are flattened into the `deployments` table as `min_replicas`, `max_replicas`, `target_concurrency` (mapped from Baseten's `min_replica`, `max_replica`, `concurrency_target` fields)
- Secret values are never returned by the API for security reasons
- The Management API may return `429 Too Many Requests` if you exceed Baseten's
  rate limits. If you hit this, back off and retry after a short delay. See
  [Baseten Rate Limits & Budgets](https://docs.baseten.co/development/model-apis/rate-limits-and-budgets)
  for current limits and how to request an increase.
