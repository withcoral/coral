# Adzuna

**Version:** 0.1.0
**Backend:** HTTP
**Tables:** 2
**Functions:** 1

Adzuna is a global job aggregator indexing millions of active vacancies across 16+ countries including GB, US, CA, AU, IN, DE, FR, and more. This Coral community source enables users and AI agents to query live job listings, enumerate job categories, and analyze salary distributions using standard SQL — with no custom API wrappers.

## Installation

Install the source via the CLI:

```bash
coral source add --file sources/community/adzuna/manifest.yaml
```

## Credentials

To use this source, you will need an Application ID and Application Key from Adzuna.

1. Register at [developer.adzuna.com](https://developer.adzuna.com) (free developer tier, no credit card required).
2. Create a new App in your developer dashboard.
3. Once created, you will see your `App ID` and `App Key`.
4. Provide these when prompted by `coral source add` or set them as environment variables:

```bash
export ADZUNA_APP_ID="your_app_id"
export ADZUNA_APP_KEY="your_app_key"
```

## Quick Start

```sql
-- Find developer jobs in London using the provider-native search function
SELECT title, company, salary_min, salary_max, redirect_url
FROM adzuna.search_jobs(what => 'developer', location_filter => 'London')
LIMIT 10;

-- List all categories in the US
SELECT tag, label FROM adzuna.categories WHERE country = 'us';

-- Salary distribution for Rust developers in the UK
SELECT salary_lower_bound, vacancy_count
FROM adzuna.salary_histogram
WHERE what = 'rust'
ORDER BY salary_lower_bound ASC;
```

## Cross-Source JOIN

Combine Adzuna jobs with other data sources in Coral. For example, joining against GitHub open issues for the same tech stack:

```sql
-- Cross-source JOIN: Adzuna jobs + GitHub open issues
SELECT g.title AS issue, g.html_url, a.company, a.redirect_url
FROM github.issues g
JOIN adzuna.search_jobs(what => 'python') a ON true
WHERE g.owner = 'tiangolo' AND g.repo = 'fastapi' AND g.assignee IS NULL
LIMIT 7;
```

## Functions

### `adzuna.search_jobs`
Provider-native search for active job listings with keyword, location, category, and salary filters.

| Argument / Column | Type | Description |
|--------|------|-------------|
| `id` | Utf8 | Unique job ID |
| `title` | Utf8 | Job title |
| `description` | Utf8 | Full description text |
| `company` | Utf8 | Hiring company name |
| `location` | Utf8 | Human-readable location |
| `location_area` | Utf8 | Top-level area string |
| `category` | Utf8 | Category display name |
| `category_tag` | Utf8 | Category slug |
| `salary_min` | Float64 | Lower end of the salary band (nullable) |
| `salary_max` | Float64 | Upper end of the salary band (nullable) |
| `salary_is_predicted` | Utf8 | Raw 0/1 string indicating if salary is predicted |
| `contract_type` | Utf8 | Type of contract (e.g. permanent, contract) |
| `contract_time` | Utf8 | Hours of the job (e.g. full_time, part_time) |
| `created_at` | Timestamp | Timestamp of job creation |
| `redirect_url` | Utf8 | Link to apply to the job |
| `latitude` | Float64 | Latitude of the workspace (nullable) |
| `longitude` | Float64 | Longitude of the workspace (nullable) |
| `country` | Utf8 | Argument/Virtual column to override country (default: gb) |
| `what` | Utf8 | Argument/Virtual column from keyword search filter |
| `location_filter` | Utf8 | Argument/Virtual column from location search filter |
| `full_time` | Utf8 | Pass `1` to require full-time jobs; omit otherwise |
| `permanent` | Utf8 | Pass `1` to require permanent jobs; omit otherwise |

## Tables

### `adzuna.categories`
Enumerate available job sectors for a given country.

| Column | Type | Description |
|--------|------|-------------|
| `tag` | Utf8 | Category slug |
| `label` | Utf8 | Human-readable category name |
| `country` | Utf8 | Virtual column to override country |

### `adzuna.salary_histogram`
Salary distribution showing vacancy counts per salary band.

| Column | Type | Description |
|--------|------|-------------|
| `salary_lower_bound` | Float64 | Lower bound of salary band |
| `vacancy_count` | Int64 | Number of vacancies in this band |
| `country` | Utf8 | Virtual column to override country |
| `what` | Utf8 | Virtual column from keyword search filter |
| `category` | Utf8 | Filter by category tag |

## Notes

- **Rate Limits:** Rate limits apply on the free developer tier. Please respect Adzuna's per-minute limits to avoid HTTP 429 throttling errors.
- **Country Availability:** Not all countries have equal data density; `gb` and `us` tend to have the most listings and detailed geolocation.
- **Nullable Fields:** `latitude` and `longitude` are not guaranteed in all responses. Missing coordinates will yield `NULL`. Similarly, salary fields may be `NULL` if not specified by the employer.
