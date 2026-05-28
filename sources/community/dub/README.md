# Dub

**Version:** 0.1.0
**Backend:** HTTP
**Tables:** 3
**Base URL:** `https://api.dub.co`

Query links, domains, and tags from Dub.co — the modern link attribution platform for short links, conversion tracking, and affiliate programs.

## Authentication

Requires a `DUB_API_KEY`. Generate one from:
**Workspace Settings → API Keys → Create API Key**

### Recommended Permissions
To ensure security, generate a **restricted, server-side secret API key** instead of a publishable key. Ensure the key has at least read-only access for the following scopes:
- `links.read`
- `domains.read`
- `tags.read`

Keys start with `dub_` and are scoped to a single workspace — no `workspaceId` parameter is needed in queries.

```bash
coral source add --file sources/community/dub/manifest.yaml
```

You will be prompted to enter your API key interactively.

API docs: https://dub.co/docs/api-reference/introduction

## Tables

| Table | Description | Required filters | Optional filters |
|---|---|---|---|
| `links` | Short links with click, lead, and sales analytics | — | `domain`, `search`, `show_archived`, `tag_ids`, `folder_id`, `sort_by`, `sort_order` |
| `domains` | Custom domains, DNS verification, and deep links | — | `archived`, `search` |
| `tags` | Tags for organizing and categorizing links | — | `search` |

### Key design notes

- **No workspace scoping filter needed.** Dub API keys are workspace-scoped
  since July 2024. The API key determines which workspace data is returned.
- **`links` is the richest table.** It includes `clicks`, `leads`, `sales`,
  `sale_amount`, and `conversions` directly in the list response.
- **`tags` are referenced by `links`.** The `tags` column on links is a JSON
  array of `{id, name, color}` objects.

```text
links      → short links with analytics (filterable by domain, tags)
domains    → custom domains, DNS verification, and deep links
tags       → tag definitions (id, name, color)
```

### links filter values

| Filter | Description |
|---|---|
| `domain` | Filter by domain (e.g. `dub.sh`, `yourdomain.com`) |
| `search` | Search link slugs and destination URLs |
| `show_archived` | Set to `true` to include archived links (default: false) |
| `tag_ids` | Filter by tag ID(s) |
| `folder_id` | Filter by folder ID |
| `sort_by` | Sort links by `createdAt`, `clicks`, `saleAmount`, or `lastClicked` |
| `sort_order` | Sort direction (`asc` or `desc`) |

### domains filter values

| Filter | Description |
|---|---|
| `search` | Search domains by name or slug |
| `archived` | Set to `true` to include archived domains (default: false) |

### Rate Limits & Fetch Limits
Dub.co enforces plan-based rate limits on its API. The Free plan is limited to **60 requests/minute**. The API returns a `429 Too Many Requests` status code with rate limit headers indicating when you can retry.

To prevent unbounded queries that could exhaust your rate limit, the high-cardinality `links` table has a default fetch limit of **100 links**. You can override this limit in your SQL query by specifying a `LIMIT` clause (e.g. `LIMIT 500`).

## Quick start

```bash
# Step 1 — list links with click analytics (pushing sort to API)
coral sql "
  SELECT id, domain, key, url, clicks, leads, sales, created_at
  FROM dub.links
  WHERE sort_by = 'clicks' AND sort_order = 'desc'
  LIMIT 20
"

# Step 2 — list all custom domains (including deep link settings)
coral sql "
  SELECT id, slug, verified, primary, archived, expired_url, not_found_url
  FROM dub.domains
"

# Step 3 — list all tags
coral sql "SELECT id, name, color FROM dub.tags"

# Step 4 — filter links by domain
coral sql "
  SELECT id, key, url, clicks
  FROM dub.links
  WHERE domain = 'yourdomain.com'
  LIMIT 20
"
```

## Example queries

### All links with click analytics

```sql
SELECT
  id,
  domain,
  key,
  url,
  short_link,
  clicks,
  leads,
  sales,
  sale_amount,
  created_at
FROM dub.links
WHERE sort_by = 'clicks' AND sort_order = 'desc'
LIMIT 50;
```

### Top performing links by clicks

```sql
SELECT
  short_link,
  url,
  clicks,
  leads,
  sales,
  conversions,
  last_clicked,
  created_at
FROM dub.links
WHERE sort_by = 'clicks' AND sort_order = 'desc'
LIMIT 10;
```

### Filter links by domain

```sql
SELECT
  id,
  key,
  url,
  clicks,
  leads,
  archived,
  created_at
FROM dub.links
WHERE domain = 'yourdomain.com'
LIMIT 50;
```

### List all custom domains

```sql
SELECT
  id,
  slug,
  verified,
  primary,
  archived,
  expired_url,
  not_found_url,
  logo,
  created_at
FROM dub.domains;
```

### List tags and their colors

```sql
SELECT
  id,
  name,
  color
FROM dub.tags;
```
