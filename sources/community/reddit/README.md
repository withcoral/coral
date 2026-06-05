# Reddit

**Version:** 0.2.0
**Backend:** HTTP
**Tables:** 6
**Functions:** 2
**Base URL:** `https://oauth.reddit.com`

Query Reddit posts, comments, user activity, and keyword search via the Reddit
OAuth API. Requires a Reddit OAuth access token from a registered Reddit app.

```bash
coral source add --file sources/community/reddit/manifest.yaml
coral source test reddit
```

## Authentication

This source uses Reddit's OAuth API. Reddit requires clients to use a
registered app and OAuth token for Data API access; unauthenticated `.json`
traffic can be blocked and is not the supported Coral access model.

You need a Reddit account and a registered script application.

1. Go to https://www.reddit.com/prefs/apps and click **create another app**.
2. Choose **script** as the app type.
3. Use a unique, descriptive app name and contact URL/email so Reddit can
   identify your client.
4. Note the **client ID** shown under the app name and the **client secret**.
5. Request an OAuth access token from Reddit, then register it as a Coral
   secret through `source add`:

```bash
REDDIT_ACCESS_TOKEN=$(
  curl -s -X POST https://www.reddit.com/api/v1/access_token \
    -u '<your_client_id>:<your_client_secret>' \
    -A 'Coral source by <your reddit username or contact>' \
    -d 'grant_type=client_credentials' \
  | jq -r .access_token \
  | tr -d '\r\n'
)

REDDIT_ACCESS_TOKEN="$REDDIT_ACCESS_TOKEN" \
  coral source add --file sources/community/reddit/manifest.yaml
```

Coral sends the stored token as `Authorization: Bearer <token>`. Reddit access
tokens are time-limited, so refresh and re-store the token if queries return
`401`.

For full details on Reddit's API access rules see:
https://support.reddithelp.com/hc/en-us/articles/16160319875092-Reddit-Data-API-Wiki
and Reddit's Responsible Builder Policy:
https://support.reddithelp.com/hc/en-us/articles/42728983564564-Responsible-Builder-Policy

## Tables And Functions

| Surface | Kind | Description | Required inputs |
| --- | --- | --- | --- |
| `subreddit_hot` | table | Hot posts from a subreddit | `subreddit` filter |
| `subreddit_new` | table | Newest posts from a subreddit | `subreddit` filter |
| `subreddit_top` | table | Top posts from a subreddit | `subreddit` filter |
| `search_posts(q => ...)` | search function | Provider-ranked global Reddit post search | `q` argument |
| `search_subreddit_posts(subreddit => ..., q => ...)` | search function | Provider-ranked post search within one subreddit | `subreddit`, `q` arguments |
| `user_posts` | table | Public posts submitted by a user | `username` filter |
| `user_comments` | table | Public comments written by a user | `username` filter |
| `post_comments` | table | Top-level comment listing for a post | `subreddit`, `post_id` filters |

## Quick Start

Verify your credentials work first — keep the limit small:

```bash
coral sql "
  SELECT title, score
  FROM reddit.subreddit_hot
  WHERE subreddit = 'redditdev'
  LIMIT 3
"
```

List hot posts from a subreddit:

```bash
coral sql "
  SELECT title, author, score, num_comments, permalink
  FROM reddit.subreddit_hot
  WHERE subreddit = 'LocalLLaMA'
  LIMIT 25
"
```

List newest posts:

```bash
coral sql "
  SELECT title, author, score, created_utc, permalink
  FROM reddit.subreddit_new
  WHERE subreddit = 'redditdev'
  LIMIT 25
"
```

Search all of Reddit:

```bash
coral sql "
  SELECT title, subreddit, author, score, permalink
  FROM reddit.search_posts(q => 'open source agents')
  LIMIT 25
"
```

Search newest matching posts:

```bash
coral sql "
  SELECT title, subreddit, author, score, permalink
  FROM reddit.search_posts(q => 'vector database', sort => 'new')
  LIMIT 25
"
```

Search within one subreddit:

```bash
coral sql "
  SELECT title, subreddit, author, score, permalink
  FROM reddit.search_subreddit_posts(
    subreddit => 'redditdev',
    q => 'oauth',
    sort => 'new'
  )
  LIMIT 25
"
```

Get comments from a post:

```bash
coral sql "
  SELECT author, score, body, created_utc
  FROM reddit.post_comments
  WHERE subreddit = 'redditdev'
    AND post_id = '<post_id>'
  LIMIT 100
"
```

Sort or focus the comment listing:

```bash
coral sql "
  SELECT author, score, body, kind, replies
  FROM reddit.post_comments
  WHERE subreddit = 'redditdev'
    AND post_id = '<post_id>'
    AND comment_sort = 'top'
    AND depth = 2
    AND showmore = true
  LIMIT 100
"
```

List public posts by a user:

```bash
coral sql "
  SELECT title, subreddit, score, permalink
  FROM reddit.user_posts
  WHERE username = '<username>'
  LIMIT 25
"
```

List public comments by a user:

```bash
coral sql "
  SELECT body, subreddit, score, permalink
  FROM reddit.user_comments
  WHERE username = '<username>'
  LIMIT 25
"
```

## Pagination

Reddit listing endpoints use cursor-based (`after` / `before`) pagination.
Coral handles this automatically using the `after` token from each response.
Page size is controlled by the `limit` parameter (default 25, max 100 per
request). Coral fetches up to 10 pages per query.

The `after` cursor is the `fullname` of the last item in the previous page,
prefixed with `t3_` for posts and `t1_` for comments. The manifest uses this
cursor internally for automatic pagination; it does not currently expose a
manual `after` or `before` filter.

`post_comments` exposes Reddit's documented sort control as `comment_sort`,
plus `depth`, `showmore`, `comment`, and `context` controls. Treat it as the
comment listing Reddit returns for that request, not a guarantee of every
possible comment in a discussion. Coral exposes nested replies as JSON when
Reddit includes them, but does not recursively expand every `more` placeholder
automatically.

## Rate Limits

For eligible free Data API usage, Reddit currently documents a limit of 100
queries per minute per OAuth client ID, averaged over a 10-minute window to
allow bursts. Each OAuth response includes approximate rate-limit headers:

| Header | Meaning |
| --- | --- |
| `X-Ratelimit-Used` | Requests consumed in the current window |
| `X-Ratelimit-Remaining` | Requests available before throttling |
| `X-Ratelimit-Reset` | Seconds until the current rate-limit period ends |

Keep exploratory queries small with `LIMIT`. If Coral surfaces a 429 or
rate-limit error, Coral uses `X-Ratelimit-Reset` as a retry delay. Full
remaining-quota tracking is not wired because Reddit's reset header is seconds
until reset, while Coral's `reset_header` expects an absolute Unix epoch reset
time.

## Validation

Lint the manifest:

```bash
coral source lint sources/community/reddit/manifest.yaml
```

Install and test with a registered Reddit app token:

```bash
REDDIT_ACCESS_TOKEN="<token>" \
  coral source add --file sources/community/reddit/manifest.yaml
coral source test reddit
```

Sanitized output from a live Reddit API test:

```text
reddit connected successfully

reddit (6 tables)
- post_comments
- subreddit_hot
- subreddit_new
- subreddit_top
- user_comments
- user_posts

Query tests
3 declared, 3 passed, 0 failed

PASS SELECT title, author, score FROM reddit.subreddit_hot WHERE subreddit = 'redditdev' LIMIT 1
PASS SELECT title, subreddit, author FROM reddit.search_posts(q => 'rust') LIMIT 1
PASS SELECT title, subreddit, author FROM reddit.search_subreddit_posts(subreddit => 'redditdev', q => 'oauth') LIMIT 1
```

Inspect the registered source catalog:

```bash
coral sql "SELECT table_name, description, required_filters FROM coral.tables WHERE schema_name = 'reddit' ORDER BY table_name"
coral sql "SELECT function_name, kind, arguments_json, search_limits_json FROM coral.table_functions WHERE schema_name = 'reddit' ORDER BY function_name"
coral sql "SELECT table_name, filter_name, is_required, data_type FROM coral.filters WHERE schema_name = 'reddit' ORDER BY table_name, filter_name"
```

## Common Columns

Post tables expose these commonly useful columns:

| Column | Description |
| --- | --- |
| `id` | Reddit post ID without the `t3_` prefix |
| `fullname` | Reddit fullname, usually prefixed with `t3_` |
| `title` | Post title |
| `subreddit` | Subreddit name without the `r/` prefix |
| `author` | Reddit username |
| `score` | Current Reddit score |
| `upvote_ratio` | Upvote ratio when Reddit provides it |
| `num_comments` | Number of comments on the post |
| `permalink` | Relative Reddit permalink |
| `url` | Linked URL or Reddit URL |
| `selftext` | Text body for self posts |
| `created_utc` | Creation time as a UTC timestamp |
| `raw` | Raw Reddit listing child JSON |

Comment tables expose:

| Column | Description |
| --- | --- |
| `id` | Reddit comment ID without the `t1_` prefix |
| `fullname` | Reddit fullname, usually prefixed with `t1_` |
| `body` | Comment text |
| `author` | Reddit username |
| `score` | Current comment score |
| `parent_id` | Parent post or comment fullname |
| `permalink` | Relative Reddit permalink |
| `created_utc` | Creation time as a UTC timestamp |
| `raw` | Raw Reddit listing child JSON |

## Notes And Limitations

- This source uses the Reddit OAuth API with a bearer access token from a
  registered Reddit app. See **Authentication** above.
- Private subreddits, saved posts, inbox data, moderation queues, and votes
  are not available.
- `post_comments` returns the comment listing Reddit provides for the requested
  `comment_sort`, `depth`, `showmore`, `comment`, and `context` controls. It
  may include `more` placeholders as rows with `kind = 'more'`; nested replies
  are available in the `replies` JSON column when Reddit includes them.
- All requests include `raw_json=1` so `title`, `selftext`, and `body` are
  returned as unescaped Unicode. Without this parameter Reddit would
  HTML-escape `<`, `>`, and `&`.
- Subreddit filters should not include the `r/` prefix. Use `LocalLLaMA`, not
  `r/LocalLLaMA`.
- Username filters should not include the `u/` prefix.

## Useful Queries

Find product mentions:

```bash
coral sql "
  SELECT title, subreddit, author, score, permalink
  FROM reddit.search_posts(q => 'RasmalAI bug OR error OR pricing')
  LIMIT 50
"
```

Watch launch sentiment:

```bash
coral sql "
  SELECT title, subreddit, score, num_comments, created_utc, permalink
  FROM reddit.search_posts(q => 'Coral SQL', sort => 'new')
  LIMIT 50
"
```

Search one community for issue reports:

```bash
coral sql "
  SELECT title, author, score, num_comments, permalink
  FROM reddit.search_subreddit_posts(
    subreddit => 'LocalLLaMA',
    q => 'crash OR regression',
    sort => 'new'
  )
  LIMIT 50
"
```

Find highly discussed posts:

```bash
coral sql "
  SELECT title, author, score, num_comments, permalink
  FROM reddit.subreddit_top
  WHERE subreddit = 'LocalLLaMA'
    AND t = 'week'
  ORDER BY num_comments DESC
  LIMIT 25
"
```

## AI And Operational Intelligence Use Cases

Reddit can be useful for:

- product mention monitoring
- launch sentiment tracking
- competitor research
- community trend detection
- public bug-report discovery
- incident chatter monitoring
- feeding AI agents real user language and pain points

Example workflow:

```text
Reddit mentions
+ GitHub issues
+ Stripe customer state
+ PostHog product events
= community and customer intelligence brief
```
