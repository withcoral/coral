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
FROM stackoverflow.questions
WHERE query = 'rust async await'
LIMIT 10;

-- Find unanswered questions
SELECT title, score, view_count
FROM stackoverflow.questions
WHERE query = 'python fastapi'
  AND is_answered = false
LIMIT 5;
```

## Tables

| Table | Description |
|-------|-------------|
| `questions` | Search Stack Overflow questions by keyword |

## Notes

- `query` filter is required
- The `Accept-Encoding: identity` header prevents gzip compression that would otherwise break JSON parsing
- `owner_display_name` uses nested path `[owner, display_name]` to access the question author's name
- Unauthenticated requests are limited to 300/day per IP; register an app for 10,000/day
