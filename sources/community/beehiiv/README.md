# Beehiiv

**Version:** 0.1.0
**Backend:** HTTP
**Tables:** 3
**Base URL:** `https://api.beehiiv.com/v2`

Query publications, posts, and subscribers from Beehiiv — the newsletter platform built for growth.

## Authentication

Requires a `BEEHIIV_API_KEY`. Generate one from:
**Workspace Settings → API → Generate API Key**

```bash
BEEHIIV_API_KEY=your_key coral source add --file sources/community/beehiiv/manifest.yaml
```

Or interactively:

```bash
BEEHIIV_API_KEY=your_key coral source add --file sources/community/beehiiv/manifest.yaml --interactive
```

API docs: https://developers.beehiiv.com/docs/v2/

## Tables

| Table | Description | Required filters | Optional filters |
|---|---|---|---|
| `publications` | All publications accessible to the API key, with subscriber and engagement stats | — | — |
| `posts` | Newsletter posts for a publication | `publication_id` | `status`, `audience`, `platform` |
| `subscriptions` | Subscribers for a publication, with UTM attribution | `publication_id` | `status`, `email` |

### Discovery order

`publications` is the entry point. Query it first to get `id` values, then
use those as `publication_id` in `posts` and `subscriptions`.

```text
publications
  → id (publication_id)
    → posts       (WHERE publication_id = '...')
    → subscriptions (WHERE publication_id = '...')
```

### posts filter values

| Filter | Accepted values |
|---|---|
| `status` | `draft`, `confirmed`, `archived`, `all` |
| `audience` | `free`, `premium`, `all` |
| `platform` | `web`, `email`, `both`, `all` |

### subscriptions filter values

| Filter | Notes |
|---|---|
| `status` | `active`, `churned`, `pending`, `validating` |
| `email` | Exact match (case sensitivity not documented in the API spec) |

## Quick start

```bash
# Step 1 — discover your publication IDs and subscriber stats
coral sql "SELECT id, name, stats__active_subscriptions, stats__average_open_rate FROM beehiiv.publications"

# Step 2 — list confirmed posts for a publication
coral sql "
  SELECT id, title, slug, status, published_at, web_url
  FROM beehiiv.posts
  WHERE publication_id = 'pub_00000000-0000-0000-0000-000000000000'
    AND status = 'confirmed'
  ORDER BY published_at DESC
  LIMIT 20
"

# Step 3 — list active subscribers for a publication
coral sql "
  SELECT id, email, status, created_at, utm_source, referring_site
  FROM beehiiv.subscriptions
  WHERE publication_id = 'pub_00000000-0000-0000-0000-000000000000'
    AND status = 'active'
  LIMIT 50
"

# Step 4 — look up a subscriber by email
coral sql "
  SELECT id, email, status, created_at, referral_code, utm_campaign
  FROM beehiiv.subscriptions
  WHERE publication_id = 'pub_00000000-0000-0000-0000-000000000000'
    AND email = 'reader@example.com'
  LIMIT 1
"
```

## Example queries

### All publications with subscriber stats

```sql
SELECT
  id,
  name,
  organization_name,
  stats__active_subscriptions,
  stats__active_free_subscriptions,
  stats__active_premium_subscriptions,
  stats__average_open_rate,
  stats__average_click_rate,
  created_at
FROM beehiiv.publications;
```

### Confirmed posts for a publication

```sql
SELECT
  id,
  title,
  subject_line,
  slug,
  status,
  audience,
  platform,
  published_at,
  authors,
  web_url
FROM beehiiv.posts
WHERE publication_id = 'pub_00000000-0000-0000-0000-000000000000'
  AND status = 'confirmed'
ORDER BY published_at DESC
LIMIT 50;
```

### Active subscribers for a publication

```sql
SELECT
  id,
  email,
  status,
  subscription_tier,
  created_at,
  referring_site,
  utm_source,
  utm_medium,
  utm_campaign,
  utm_channel
FROM beehiiv.subscriptions
WHERE publication_id = 'pub_00000000-0000-0000-0000-000000000000'
  AND status = 'active'
LIMIT 100;
```

### Find a subscriber by email

```sql
SELECT
  id,
  email,
  status,
  subscription_tier,
  created_at,
  referral_code,
  utm_source,
  utm_campaign,
  subscription_premium_tiers,
  tags,
  stats
FROM beehiiv.subscriptions
WHERE publication_id = 'pub_00000000-0000-0000-0000-000000000000'
  AND email = 'reader@example.com'
LIMIT 1;
```
