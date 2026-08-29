# Yelp Fusion

Query [Yelp Fusion](https://docs.developer.yelp.com/docs/fusion-intro) as SQL — businesses, reviews, events, and the Yelp category taxonomy. Built for the **Pirates of the Coral-bean** hackathon to add Local Business Intelligence to Coral.

## Setup

1. Create a free Yelp Fusion app at [yelp.com/developers/v3/manage_app](https://www.yelp.com/developers/v3/manage_app) (only an app name + email are required).
2. Copy the **API Key** field from the app management page.
3. Install the source:

   ```bash
   YELP_API_KEY=your-key-here \
   coral source add --file sources/community/yelp/manifest.yaml
   ```

   Or pass `--interactive` and Coral will prompt for the key:

   ```bash
   coral source add --interactive --file sources/community/yelp/manifest.yaml
   ```

The free Yelp plan allows 500 API calls per day, which is plenty for interactive SQL exploration and most agent workloads.

## What it exposes

| Kind | Name | Purpose |
|------|------|---------|
| function (`kind: search`) | `search_businesses` | Provider-ranked business search by term + location + filters |
| function (`kind: search`) | `search_events` | Provider-ranked local events search |
| function | `autocomplete_terms` | Yelp autocomplete suggestions for partial text |
| table | `business_details` | Full business profile by `id` (requires filter) |
| table | `business_reviews` | Up to 3 review excerpts per business (requires `business_id`) |
| table | `event_details` | Full event detail by `id` (requires filter) |
| table | `categories` | Yelp category taxonomy (requires Yelp Developer Beta access) |

## Example queries

**Top-rated ramen spots in NYC:**

```sql
SELECT name, rating, review_count, location__city, category_titles
FROM yelp.search_businesses(
  location => 'New York, NY',
  term     => 'ramen',
  sort_by  => 'rating'
)
WHERE rating >= 4.5
LIMIT 10;
```

**Drill from a search hit into full details + reviews:**

```sql
WITH top_pick AS (
  SELECT id
  FROM yelp.search_businesses(location => 'Austin, TX', term => 'tacos', sort_by => 'rating')
  WHERE review_count >= 100
  ORDER BY rating DESC
  LIMIT 1
)
SELECT d.name, d.phone, d.url, r.rating AS review_rating, r.text
FROM top_pick t
JOIN yelp.business_details d ON d.id = t.id
JOIN yelp.business_reviews r ON r.business_id = t.id
ORDER BY r.rating DESC;
```

**Cross-source — join Yelp ratings with a Google Calendar lunch meeting:**

```sql
SELECT y.name, y.rating, y.location__address1, y.display_phone, c.summary AS meeting
FROM google_calendar.events c
CROSS JOIN LATERAL yelp.search_businesses(
  location => c.location,
  term     => 'lunch'
) y
WHERE c.start_at::date = current_date
  AND y.rating >= 4.0
ORDER BY y.review_count DESC
LIMIT 5;
```

## Nested response handling

Yelp returns deeply nested JSON. The spec flattens commonly-used nested paths using Coral's `__` convention:

- `coordinates__latitude`, `coordinates__longitude`
- `location__address1`, `location__city`, `location__state`, `location__zip_code`, `location__country`
- `user__id`, `user__name`, `user__profile_url` (on reviews)
- `category_aliases`, `category_titles` (comma-joined from the `categories[]` array via `join_array_path`)

Multi-day schedules (`hours[]`), variable-length arrays (`transactions[]`, `photos[]`, `special_hours[]`), and `parent_aliases[]` are exposed as raw JSON strings so callers can parse them with DuckDB JSON functions as needed.

## Auth

`HeaderAuth` with `Authorization: Bearer {{input.YELP_API_KEY}}`. The key is stored in your OS keychain by Coral, not on disk.

## Rate limits

- Free plan: 500 calls/day, ~5 QPS burst.
- Each search function call counts as 1 API call regardless of `LIMIT` (pagination uses offset/limit up to 50 per page).
- The `categories` table is gated behind Yelp's Developer Beta and the `business_reviews` and `event_details`/`search_events` endpoints have been moved behind Yelp's Business / Partner tiers since mid-2024. They remain in the spec as documentation — if your app has the elevated access they will work automatically; otherwise expect 403/404 on those endpoints. The free plan happily serves `search_businesses`, `business_details`, and `autocomplete_terms`.

## Validation

```bash
coral source lint sources/community/yelp/manifest.yaml
coral source test yelp
```

Declared `test_queries`:

```sql
SELECT id, name, rating FROM yelp.search_businesses(location => 'San Francisco, CA', term => 'coffee') LIMIT 1;
SELECT text FROM yelp.autocomplete_terms(text => 'piz') LIMIT 1;
```

Both pass against the free plan with no extra configuration.
