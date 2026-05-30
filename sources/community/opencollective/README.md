# Open Collective (opencollective)

**Version:** 1.0.0
**Backend:** HTTP
**Tables:** 1
**Base URL:** `https://api.opencollective.com/graphql/v2`

Query Open Collective funding data — balances, totals raised and spent,
contributor counts, and project metadata — via the public
[Open Collective GraphQL API](https://docs.opencollective.com/help/contributing/development/api).
No authentication required.

```bash
coral source add --file sources/community/opencollective/manifest.yaml
```

## Configuration

This source does not require any authentication or input configuration.
The Open Collective GraphQL API is fully public for read-only queries.

## Tables

| Table                          | Description                                                  | Key filters            |
| ------------------------------ | ------------------------------------------------------------ | ---------------------- |
| `opencollective.collectives`   | Look up funding stats and metadata for a specific collective | `slug` (**required**)  |

## Example queries

```sql
-- Fetch funding overview for webpack
SELECT name, total_amount_received, balance, contributors_count, currency
FROM opencollective.collectives
WHERE slug = 'webpack';

-- Compare financials: total raised vs total spent
SELECT name, total_amount_received, total_amount_spent, balance
FROM opencollective.collectives
WHERE slug = 'babel';

-- Inspect project metadata and social links
SELECT name, description, website, github_handle, twitter_handle
FROM opencollective.collectives
WHERE slug = 'vuejs';

-- Check creation date and yearly budget
SELECT name, yearly_budget, created_at, type
FROM opencollective.collectives
WHERE slug = 'eslint';
```

## Pagination

The Open Collective API returns a single object per slug, so
`opencollective.collectives` always returns exactly one row if the
collective exists, or zero rows if it does not. The `LIMIT` clause
is not required.

## Notes

- **No authentication required.** The GraphQL API is public for
  read-only queries.
- **Lookup only.** You must know the exact slug of the collective you
  want to query. Find it in the URL: `https://opencollective.com/<slug>`.
- **Graceful missing collectives.** If a slug does not exist, the query
  returns zero rows rather than throwing an error.
- **Financial values.** All monetary amounts (`total_amount_received`,
  `total_amount_spent`, `balance`, `yearly_budget`) are returned in
  the collective's default `currency`.
- **Rate limiting.** Please consume the API responsibly — avoid
  high-frequency polling loops.

## Validation

```bash
coral source lint sources/community/opencollective/manifest.yaml
coral source add --file sources/community/opencollective/manifest.yaml
coral source test opencollective

coral sql "SELECT name, total_amount_received, balance, contributors_count FROM opencollective.collectives WHERE slug = 'webpack'"
# +---------+-----------------------+-----------+--------------------+
# | name    | total_amount_received | balance   | contributors_count |
# +---------+-----------------------+-----------+--------------------+
# | webpack | 1962347.35            | 89176.17  | 2633               |
# +---------+-----------------------+-----------+--------------------+
```
