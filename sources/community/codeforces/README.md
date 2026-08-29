# Codeforces Source

Query Codeforces competitive programming data using SQL via the official public Codeforces API.

No authentication required — all endpoints are open. Just provide a Codeforces handle.

## Setup

```bash
CODEFORCES_HANDLE=your_handle coral source add --file sources/community/codeforces/manifest.yaml
```

Or interactively:

```bash
coral source add --interactive --file sources/community/codeforces/manifest.yaml
```

When prompted, enter your Codeforces handle (e.g. `tourist`, `jiangly`, `yashasvithakur2005`).
Find your handle at https://codeforces.com/profile — it appears at the top of your profile page.

## Tables

| Table | Endpoint | Description |
|-------|----------|-------------|
| `codeforces.user_info` | `/api/user.info` | Profile, rating, rank, max rating, country, avatar |
| `codeforces.submissions` | `/api/user.status` | Up to 500 most recent submissions with verdicts and problem metadata |
| `codeforces.rating_history` | `/api/user.rating` | Contest-by-contest rating progression over time |
| `codeforces.contests` | `/api/contest.list` | All contests — upcoming and finished, filterable by phase |

## Example Queries

```sql
-- Get user profile
SELECT handle, rating, rank, max_rating, max_rank, country
FROM codeforces.user_info;
```

```sql
-- Count unique solved problems
SELECT COUNT(DISTINCT problem_contest_id || problem_index) AS solved
FROM codeforces.submissions
WHERE verdict = 'OK';
```

```sql
-- Problems solved by difficulty rating
SELECT problem_rating, COUNT(*) AS solved
FROM codeforces.submissions
WHERE verdict = 'OK' AND problem_rating IS NOT NULL
GROUP BY problem_rating
ORDER BY problem_rating;
```

```sql
-- Most used programming languages
SELECT language, COUNT(*) AS submissions
FROM codeforces.submissions
GROUP BY language
ORDER BY submissions DESC
LIMIT 5;
```

```sql
-- Recent 10 rating changes
SELECT contest_name, old_rating, new_rating,
       (new_rating - old_rating) AS delta
FROM codeforces.rating_history
ORDER BY rated_at DESC
LIMIT 10;
```

```sql
-- Upcoming contests (phase = BEFORE means not yet started)
SELECT name, type, duration_seconds / 3600.0 AS duration_hours,
       start_time_seconds
FROM codeforces.contests
WHERE phase = 'BEFORE'
ORDER BY start_time_seconds ASC
LIMIT 10;
```

```sql
-- Acceptance rate by problem difficulty
SELECT problem_rating,
       COUNT(CASE WHEN verdict = 'OK' THEN 1 END) AS accepted,
       COUNT(*) AS total,
       ROUND(100.0 * COUNT(CASE WHEN verdict = 'OK' THEN 1 END) / COUNT(*), 1) AS acceptance_pct
FROM codeforces.submissions
WHERE problem_rating IS NOT NULL
GROUP BY problem_rating
ORDER BY problem_rating;
```

## Auth

None required. The Codeforces public API is fully open — no API key, no OAuth, no token.
The only input is `CODEFORCES_HANDLE`, the username of the account to query.

## Rate Limits

Codeforces does not publish official rate limit numbers, but community-observed limits are approximately:

| Limit | Value |
|-------|-------|
| Requests per second | ~5 |
| Requests per 6 hours (unofficial) | ~300 |

Exceeding the limit returns HTTP 503. Adding a short delay between queries avoids this in practice.

## Notes

- `codeforces.submissions` fetches up to **500 most recent submissions**. Users with very long submission histories may not see their earliest records.
- `rating` and `rank` fields in `user_info` are `null` for users who have never participated in a rated contest.
- Contest `start_time_seconds` is a Unix timestamp. Multiply by 1000 for JavaScript `Date` compatibility.
- `codeforces.contests` returns all contests (past and upcoming). Filter `WHERE phase = 'BEFORE'` for upcoming contests.

## Provider Docs

- Codeforces API reference: https://codeforces.com/apiHelp
- User methods: https://codeforces.com/apiHelp/methods#user.info
- Contest methods: https://codeforces.com/apiHelp/methods#contest.list
