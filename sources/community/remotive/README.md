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
| `jobs` | Remote job listings from Remotive | `category`, `search` |

### Category filter values

The `category` filter accepts slugs. Common values:

| Slug | Display name |
|---|---|
| `software-dev` | Software Development |
| `marketing` | Marketing |
| `design` | Design |
| `data` | Data |
| `product` | Product |
| `customer-support` | Customer Support |
| `sales` | Sales |
| `devops` | DevOps / Sysadmin |
| `finance` | Finance / Legal |
| `human-resources` | Human Resources |
| `qa` | QA |
| `writing` | Writing |
| `all-others` | All Others |

## Quick start

```bash
# Confirm connectivity — fetch one job
coral sql "SELECT id, title, company_name, category FROM remotive.jobs LIMIT 1"

# Software development jobs
coral sql "
  SELECT title, company_name, salary, candidate_required_location
  FROM remotive.jobs
  WHERE category = 'software-dev'
  LIMIT 10
"

# Search by keyword
coral sql "
  SELECT title, company_name, category, job_type
  FROM remotive.jobs
  WHERE search = 'python'
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

## Notes

- The API returns all matching jobs in a single response (no pagination).
- The HTML job `description` field is intentionally excluded to keep
  payloads compact for agent workflows. Use the `url` column to view
  the full listing.
- Data is delayed 24 hours; `publication_date` reflects this delay.
- The API recommends at most 4 requests per day — avoid excessive polling.
