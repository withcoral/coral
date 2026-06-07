# Stack Overflow (stackoverflow)

**Version:** 0.1.0
**Backend:** HTTP
**Tables:** 4
**Functions:** 1
**Base URL:** `https://api.stackexchange.com/2.3`

Query Stack Overflow questions, answers, search results, users, and tags via the
public [Stack Exchange API v2.3](https://api.stackexchange.com/docs). Works
without authentication.

```bash
coral source add --file sources/community/stackoverflow/manifest.yaml
```

## Configuration

| Input                     | Kind       | Required | Default         | Description                                                  |
| ------------------------- | ---------- | -------- | --------------- | ------------------------------------------------------------ |
| `STACKOVERFLOW_SITE`      | variable   | no       | `stackoverflow` | Site to query (`serverfault`, `askubuntu`, `superuser`, etc.) |

Authentication is not required for read-only access.

## Tables

| Table / Function                  | Description                                    | Key filters / args                                       |
| --------------------------------- | ---------------------------------------------- | -------------------------------------------------------- |
| `stackoverflow.questions`         | Browse questions by activity, votes, or tags   | `tagged`, `sort`, `fromdate`, `todate`, `min`, `max`     |
| `stackoverflow.search_questions()`| Search questions by title keyword (function)   | `intitle` (**required**), `tagged`, `sort`               |
| `stackoverflow.answers`           | Recent answers with score and acceptance status | `sort`, `fromdate`, `todate`, `min`, `max`                |
| `stackoverflow.users`             | Stack Overflow users sorted by reputation      | `inname`                                                 |
| `stackoverflow.tags`              | Tags sorted by popularity with question counts | —                                                        |

## Example queries

```sql
-- Recent questions sorted by activity
SELECT question_id, title, score, view_count, answer_count
FROM stackoverflow.questions
LIMIT 10;

-- Questions tagged with 'rust'
SELECT question_id, title, score, answer_count, creation_date
FROM stackoverflow.questions
WHERE tagged = 'rust'
LIMIT 10;

-- High-scoring questions tagged with 'rust' (min/max apply to the sort field)
SELECT question_id, title, score
FROM stackoverflow.questions
WHERE tagged = 'rust' AND sort = 'votes' AND min = 50
LIMIT 10;

-- Questions tagged with both 'python' AND 'django'
SELECT question_id, title, score
FROM stackoverflow.questions
WHERE tagged = 'python;django'
LIMIT 10;

-- Search questions by title keyword (search function)
SELECT question_id, title, score, view_count
FROM stackoverflow.search_questions(intitle => 'async await')
LIMIT 10;

-- Search with tag and sort arguments
SELECT question_id, title, score
FROM stackoverflow.search_questions(intitle => 'dependency injection', tagged => 'java', sort => 'votes')
LIMIT 10;

-- Recent answers
SELECT answer_id, question_id, score, is_accepted, creation_date
FROM stackoverflow.answers
LIMIT 10;

-- Top users by reputation
SELECT user_id, display_name, reputation, location
FROM stackoverflow.users
LIMIT 10;

-- Most popular tags
SELECT name, count, has_synonyms
FROM stackoverflow.tags
LIMIT 20;
```

## Pagination & Quota Safety

All tables use Stack Exchange page-based pagination (default page size 30, max 100). Coral handles this automatically — just use `LIMIT` to control how many rows you want. Without an explicit `LIMIT`, results are capped at 100 rows (`fetch_limit_default`) to avoid exhausting the API quota.

**Important Quota & Rate Limit Notes:**
- **API Backoff:** The Stack Exchange API returns a `backoff` field in its response wrapper when rate limits are approached, requiring clients to pause before making further requests. Coral does not currently honor or wait on this `backoff` field dynamically.
- **Throttling:** Rapid, sequential pagination requests can trigger IP-based throttling. To ensure quota-safe usage, always use narrow limits, specify date bounds (`fromdate`/`todate`), and specify bounds (`min`/`max`) on the active sort field when exploring (e.g. set `sort = 'votes'` to filter by score).

## Notes

- **No authentication required.** Anonymous access provides 300 API
  requests per day per IP.
- **Read-only.** This source does not support write operations.
- **HTML-encoded titles.** Question titles may contain HTML entities
  (e.g. `&amp;`, `&#39;`). Use them as-is or decode in your application.
- **Tag semantics differ by endpoint.** On `stackoverflow.questions`,
  `tagged = 'python;django'` uses AND logic (questions with **both**
  tags). On `search_questions()`, `tagged` uses OR logic (questions
  with **at least one** of the tags). Passing more than 5 tags
  always returns zero results.
- **Configurable site.** Set `STACKOVERFLOW_SITE` to query any Stack
  Exchange network site: `serverfault`, `askubuntu`, `superuser`,
  `math`, `unix`, etc.
- **Timestamps.** All date columns are converted from Unix epoch
  seconds to UTC timestamps.
- **Pagination and quota limits.** The API limits anonymous pagination to page 25 (750 rows at pagesize 30) and allows 300 requests/day per IP. Coral caps pagination at 25 pages to prevent exceeding this limit. Always use `LIMIT` to stay within quota.

## Validation

```bash
coral source lint sources/community/stackoverflow/manifest.yaml
coral source add --file sources/community/stackoverflow/manifest.yaml
coral source test stackoverflow
coral sql "SELECT * FROM coral.tables WHERE schema_name = 'stackoverflow'"

coral sql "SELECT question_id, title, score FROM stackoverflow.questions LIMIT 1"
# +-------------+--------------------------------------------------------------------------+-------+
# | question_id | title                                                                    | score |
# +-------------+--------------------------------------------------------------------------+-------+
# | 79946255    | How to enable &quot;Annotate with Git Blame&quot; using WebStorm 2026.1? | 0     |
# +-------------+--------------------------------------------------------------------------+-------+

coral sql "SELECT question_id, title, score FROM stackoverflow.search_questions(intitle => 'python') LIMIT 1"
# +-------------+-----------------------------------------------+-------+
# | question_id | title                                         | score |
# +-------------+-----------------------------------------------+-------+
# | 72108098    | Sorting words into alphabetic order in Python | -2    |
# +-------------+-----------------------------------------------+-------+

coral sql "SELECT answer_id, score, is_accepted FROM stackoverflow.answers LIMIT 1"
# +-----------+-------+-------------+
# | answer_id | score | is_accepted |
# +-----------+-------+-------------+
# | 53381692  | 76    | false       |
# +-----------+-------+-------------+

coral sql "SELECT user_id, display_name, reputation FROM stackoverflow.users LIMIT 1"
# +---------+--------------+------------+
# | user_id | display_name | reputation |
# +---------+--------------+------------+
# | 22656   | Jon Skeet    | 1527510    |
# +---------+--------------+------------+

coral sql "SELECT name, count FROM stackoverflow.tags LIMIT 1"
# +------------+---------+
# | name       | count   |
# +------------+---------+
# | javascript | 2531304 |
# +------------+---------+
```
