# Reddit

Search Reddit posts across all subreddits using the [public Reddit JSON API](https://www.reddit.com/dev/api/). No authentication required.

## Setup

```bash
coral source add --file sources/community/reddit/manifest.yaml
```

## Usage

```sql
-- Search posts across all subreddits
SELECT id, title, score, subreddit
FROM reddit.posts
WHERE query = 'rust programming'
LIMIT 10;

-- Find top posts about a project
SELECT title, score, subreddit, permalink
FROM reddit.posts
WHERE query = 'zed editor'
ORDER BY score DESC
LIMIT 5;
```

## Tables

| Table | Description |
|-------|-------------|
| `posts` | Search Reddit link posts by keyword across all subreddits |

## Notes

- `query` filter is required
- Reddit wraps each post in `{kind, data}` — this spec uses `expr: kind: path` to navigate the nested structure cleanly
- `permalink` is a relative path — prefix with `https://reddit.com` for a full URL
- Reddit rate-limits unauthenticated requests (~60 req/10 min per IP)
- **Cloud deployments**: Reddit blocks requests from cloud provider IPs (AWS, GCP, Fly.io, etc.) with HTTP 403. This spec works on local machines. For cloud use, switch to the [Reddit OAuth API](https://www.reddit.com/dev/api/) with a registered app (`oauth.reddit.com` base URL + `Authorization: Bearer <token>` header).
