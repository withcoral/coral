# Honeycomb

Query datasets, columns, boards, markers, and triggers from
[Honeycomb](https://www.honeycomb.io/) observability environments.

## Setup

You need a **Honeycomb Configuration Key** to use this source.

1. Log in to Honeycomb → **Environment Settings** → **API Keys** → **Configuration** tab.
2. Create a new Configuration Key with read access.
3. The key starts with `hcxlk_`.

Export the key before adding the source:

```sh
export HONEYCOMB_API_KEY="hcxlk_..."
coral source add --file sources/community/honeycomb/manifest.yaml
```

For EU environments, also set:

```sh
export HONEYCOMB_API_BASE="https://api.eu1.honeycomb.io/1"
```

## Tables

| Table | Description | Required Filters |
|-------|-------------|------------------|
| `honeycomb.datasets` | All datasets in the environment | — |
| `honeycomb.columns` | Column metadata for a dataset | `dataset_slug` |
| `honeycomb.boards` | Non-secret boards and dashboards | — |
| `honeycomb.markers` | Deployment and event markers | `dataset_slug` |
| `honeycomb.triggers` | Alert triggers and their status | `dataset_slug` |

Use `'__all__'` as the `dataset_slug` filter to query across all datasets
(not available for Classic environments).

## Example queries

```sql
-- List all datasets and their column counts
SELECT name, slug, regular_columns_count, created_at
FROM honeycomb.datasets
ORDER BY created_at DESC;

-- Inspect columns in a specific dataset
SELECT key_name, type, hidden, last_written
FROM honeycomb.columns
WHERE dataset_slug = 'my-dataset';

-- Find all columns across the environment
SELECT key_name, type, description
FROM honeycomb.columns
WHERE dataset_slug = '__all__';

-- List all boards
SELECT name, description, board_type
FROM honeycomb.boards;

-- Find recent deployment markers
SELECT id, message, marker_type, start_time
FROM honeycomb.markers
WHERE dataset_slug = '__all__';

-- Find triggers that are currently firing
SELECT name, alert_type, triggered, disabled
FROM honeycomb.triggers
WHERE dataset_slug = '__all__';
```

## Auth

This source uses the `X-Honeycomb-Team` header with a Configuration Key.
See [Honeycomb API Authentication](https://docs.honeycomb.io/api/authentication/)
for details on key types and permissions.
