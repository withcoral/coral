# Stack Overflow

Search Stack Overflow questions via the [Stack Exchange API](https://api.stackexchange.com/). No authentication required.

## Setup

```bash
coral source add --file sources/community/stackoverflow/manifest.yaml
```

## Usage

```sql
-- Search questions by keyword
SELECT question_id, title, score, is_answered
FROM stackoverflow.search_questions('rust async await')
LIMIT 10;

-- Find unanswered questions
SELECT title, score, view_count, link
FROM stackoverflow.search_questions('python fastapi')
WHERE is_answered = false
LIMIT 5;

-- High-score questions
SELECT title, score, answer_count, link
FROM stackoverflow.search_questions('zed editor')
ORDER BY score DESC
LIMIT 10;
```

## Functions

| Function | Description |
|----------|-------------|
| `search_questions(query)` | Search Stack Overflow questions by keyword (matched in title) |

## Notes

- Uses `kind: search` — a native search function, not a scannable table
- Pagination is enabled (`mode: page`, up to 100 results per page, max 3 pages per query)
- `Accept-Encoding: identity` header is required to prevent gzip compression that breaks JSON parsing
- Unauthenticated requests are limited to 300/day per IP; register an app at [stackapps.com](https://stackapps.com) for 10,000/day
- `owner_display_name` uses nested path `[owner, display_name]` to access the question author's name
