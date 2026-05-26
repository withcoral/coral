# Remotive

**Version:** 0.1.0
**Backend:** HTTP
**Tables:** 1
**Base URL:** `https://remotive.com/api`

Remote job listings from Remotive.com. No authentication required.
Data is delayed 24 hours from actual posting. The API recommends at
most 4 requests per day.

## Authentication

No authentication required. Remotive exposes a public API.

```bash
coral source add --file sources/community/remotive/manifest.yaml
```

## Tables

| Table | Description | Filters |
|---|---|---|
| `jobs` | Remote job listings from Remotive | None (Filtering is done locally in SQL) |



## Quick start

```bash
# Confirm connectivity — fetch one job
coral sql "SELECT id, title, company_name, category FROM remotive.jobs LIMIT 1"

# Software development jobs
coral sql "
  SELECT title, company_name, salary, candidate_required_location
  FROM remotive.jobs
  WHERE category = 'Software Development'
  LIMIT 10
"

# Search by keyword using ILIKE
coral sql "
  SELECT title, company_name, category, job_type
  FROM remotive.jobs
  WHERE search ILIKE '%python%'
  LIMIT 10
"

# Recently published jobs
coral sql "
  SELECT title, company_name, publication_date, url
  FROM remotive.jobs
  ORDER BY publication_date DESC
  LIMIT 10
"

# Jobs with salary info
coral sql "
  SELECT title, company_name, salary, candidate_required_location
  FROM remotive.jobs
  WHERE salary != ''
  ORDER BY publication_date DESC
"

# Job count by category
coral sql "
  SELECT category, COUNT(*) as count
  FROM remotive.jobs
  GROUP BY category
  ORDER BY count DESC
"

# Jobs by employment type
coral sql "
  SELECT job_type, COUNT(*) as count
  FROM remotive.jobs
  GROUP BY job_type
  ORDER BY count DESC
"
```

## Attribution & Terms of Service

According to Remotive's API Terms and Legal Notice:

1. **Attribution:** Consumers **must** link back to the job's URL on Remotive and mention Remotive as the source.
2. **Redistribution Restrictions:** Do **not** submit jobs fetched from the Remotive API to third-party job boards/websites (including but not limited to Jooble, Neuvoo, Google Jobs, LinkedIn Jobs).
3. **Usage Restrictions:** Displaying jobs to collect signups or email addresses constitutes a breach of terms of service.
4. **Rate Limits & Delay:** Data is delayed by 24 hours. The API recommends requesting data at most 4 times per day. Excessive requests will be blocked.

Refer to [Remotive API Documentation](https://remotive.com/api-documentation) for detailed rules.

## Notes

- The API ignores query parameters, so all SQL filters (such as `category` and `search`) are evaluated locally on the fetched payload.
- The API returns all jobs in a single response (no pagination). Results are bounded locally by `fetch_limit_default`.
- The HTML job `description` field is intentionally excluded to keep payloads compact for downstream agent workflows. Use the `url` column to link to the full listing.
