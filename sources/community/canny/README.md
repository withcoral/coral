# Canny Coral source

Query Canny feedback data with Coral. The source exposes boards, posts,
comments, users, companies, tags, and votes for product-feedback triage and
roadmap analysis.

## Setup

Create a Canny API key from your workspace API settings, then add the source:

```bash
CANNY_API_KEY=... coral source add --file sources/community/canny/manifest.yaml
```

Run the smoke tests:

```bash
coral source test canny
```

## Tables

| Table | Description |
| --- | --- |
| `canny.boards` | Feedback boards in the workspace. |
| `canny.posts` | Feedback posts with status, score, board, owner, author, and tags. |
| `canny.comments` | Comments for a required `post_id`. |
| `canny.users` | Canny users visible to the API key. |
| `canny.companies` | Companies visible to the API key. |
| `canny.tags` | Tags used to organize feedback posts. |
| `canny.votes` | Votes with post, voter, board, and priority metadata. |

## Example queries

Find the highest-priority feedback:

```sql
SELECT title, status, score, comment_count, board__name
FROM canny.posts
ORDER BY score DESC
LIMIT 20;
```

Inspect comments for a post:

```sql
SELECT author__name, value, created_at
FROM canny.comments
WHERE post_id = 'post_id_here'
ORDER BY created_at DESC
LIMIT 20;
```

Find important votes on planned work:

```sql
SELECT post__title, voter__name, vote_priority, created_at
FROM canny.votes
WHERE post_id = 'post_id_here'
ORDER BY created_at DESC
LIMIT 20;
```

## Notes

- This source is read-only.
- Canny expects the API key in the JSON body as `apiKey`.
- `canny.comments` is scoped by `post_id`; `canny.votes` can be scoped by
  `post_id`, `board_id`, `user_id`, or `company_id`.

## API references

- https://developers.canny.io/api-reference
