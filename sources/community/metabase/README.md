# Metabase Coral Source

## What This Source Exposes

This source exposes [Metabase](https://www.metabase.com/) BI workspace metadata
as SQL tables in Coral. It is designed for analytics governance: collections,
dashboards, saved questions, connected databases, users, and permission groups.

It is read-only. It does not execute Metabase questions and does not expose
database connection details or credentials.

## Authentication

Create a Metabase API key and pass it as `METABASE_API_KEY`. The key is sent
with the `X-API-Key` header.

Required inputs:

| Input | Kind | Default | Description |
|---|---|---|---|
| `METABASE_BASE_URL` | variable | `http://localhost:3000` | Base URL for your Metabase instance, without a trailing slash. |
| `METABASE_API_KEY` | secret | - | Metabase API key with read access to the metadata you want to inspect. |

## Setup

```bash
METABASE_BASE_URL=https://metabase.example.com \
METABASE_API_KEY=mb_your_key_here \
coral source add --file sources/community/metabase/manifest.yaml
```

Or run interactively:

```bash
coral source add --interactive --file sources/community/metabase/manifest.yaml
```

## Tables

| Table | Purpose |
|---|---|
| `metabase.collections` | Collections that organize dashboards, questions, models, and child collections. |
| `metabase.collection_items` | Dashboards, cards, models, and child collections inside a specific collection. |
| `metabase.dashboards` | Dashboards visible to the configured API key. |
| `metabase.cards` | Saved questions and models. The Metabase API refers to questions as cards. |
| `metabase.databases` | Databases connected to Metabase. |
| `metabase.users` | Metabase users visible to the configured API key. |
| `metabase.permission_groups` | Metabase permission groups. This may require an admin-scoped API key. |

## Example Queries

Recent cards and models:

```sql
SELECT id, name, collection_id, database_id, updated_at
FROM metabase.cards
ORDER BY updated_at DESC
LIMIT 20;
```

Recently edited content in the root collection:

```sql
SELECT model, name, last_edited_at, last_edited_by_name
FROM metabase.collection_items
WHERE collection_id = 'root'
ORDER BY last_edited_at DESC
LIMIT 25;
```

Active cards grouped by source database:

```sql
SELECT d.name AS database_name, COUNT(*) AS card_count
FROM metabase.cards c
JOIN metabase.databases d ON c.database_id = d.id
WHERE c.archived = false
GROUP BY d.name
ORDER BY card_count DESC;
```

Dashboards owned by inactive users:

```sql
SELECT d.id, d.name, u.email, u.is_active
FROM metabase.dashboards d
JOIN metabase.users u ON d.creator_id = u.id
WHERE u.status = 'all'
  AND u.is_active = false;
```

Archived dashboards:

```sql
SELECT id, name, archived, updated_at
FROM metabase.dashboards
WHERE f = 'archived'
ORDER BY updated_at DESC;
```

Permission groups by size:

```sql
SELECT id, name, member_count
FROM metabase.permission_groups
ORDER BY member_count DESC;
```

## Limitations

- Metabase's API is not versioned and can change between releases.
- `metabase.users` returns active users by default; use `status = 'all'`,
  `status = 'deactivated'`, or `include_deactivated = true` when auditing
  inactive users.
- `metabase.dashboards` and `metabase.cards` accept `f = 'archived'` when you
  want archived content from the Metabase API.
- Metabase can return a root collection with a null `id`; use
  `collection_id = 'root'` when querying root collection items.
- `metabase.collection_items` requires `collection_id`; use `root` for the
  root collection.
- `metabase.permission_groups` may require an admin-scoped API key.
- Pagination is not configured for endpoints that Metabase commonly returns as
  bounded arrays or `data` payloads.
- This source does not run saved questions, fetch query results, or expose raw
  database connection details or credentials.

## Validation

Local validation for this source:

```text
YAML parse: passed for sources/community/metabase/manifest.yaml
Coral manifest schema validation: passed for sources/community/metabase/manifest.yaml
git diff --check: passed
make lint-sources: passed
Live API tests: passed against a local Metabase Docker instance with `coral source add` and `coral source test metabase`
```

Testing note: the live API test used user-provided Metabase credentials. The manifest includes `test_queries`, and no credentials or customer data are committed.
