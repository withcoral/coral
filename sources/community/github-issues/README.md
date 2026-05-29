# GitHub Issues

Query open GitHub issues for any public repository via the [GitHub REST API](https://docs.github.com/en/rest/issues). No token required for public repositories.

## Setup

```bash
coral source add --file sources/community/github-issues/manifest.yaml
```

## Usage

```sql
-- Open issues for a repo
SELECT number, title, comments, created_at
FROM github_issues.issues
WHERE owner = 'rust-lang' AND repo = 'rust'
LIMIT 10;

-- Issues with most comments
SELECT number, title, comments, html_url
FROM github_issues.issues
WHERE owner = 'zed-industries' AND repo = 'zed'
ORDER BY comments DESC
LIMIT 5;

-- Cross-source JOIN with Hacker News
SELECT i.number, i.title, h.title AS hn_post, h.points
FROM github_issues.issues i
LEFT JOIN hackernews.stories h
  ON LOWER(h.title) LIKE '%' || LOWER(i.title) || '%'
WHERE i.owner = 'zed-industries' AND i.repo = 'zed'
ORDER BY i.updated_at DESC
LIMIT 20;
```

## Tables

| Table | Description |
|-------|-------------|
| `issues` | Open issues for a repository, most recently updated first |

## Notes

- Both `owner` and `repo` filters are required
- Returns first 100 issues only (`pagination: mode: none`) — avoids timeouts on large repos with thousands of open issues
- No token needed for public repositories (60 req/hr unauthenticated)
- For private repos or higher rate limits (5,000 req/hr), set the `Authorization: Bearer <token>` header
