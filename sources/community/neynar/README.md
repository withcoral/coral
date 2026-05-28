# Neynar (Farcaster) Source

[Neynar](https://neynar.com) is the leading API provider for the [Farcaster](https://farcaster.xyz) decentralized social protocol. This community source exposes Farcaster cast search as a read-only SQL table function via [Coral](https://withcoral.com).

Uses `kind: search` — a provider-ranked retrieval pattern where the API decides relevance ordering, not SQL WHERE clauses.

---

## Setup

### 1. Get a Neynar API Key

1. Create an account at [neynar.com](https://neynar.com)
2. Navigate to your dashboard and generate an API key
3. **Note:** The cast search endpoint requires a paid plan. Free keys return 402 on `/cast/search`.

### 2. Set the API Key

```bash
export NEYNAR_API_KEY=your-api-key-here
```

### 3. Add the Source to Coral

```bash
coral source add --file sources/community/neynar/manifest.yaml --interactive
```

### 4. Verify Connection

```bash
coral source test neynar
```

---

## Functions

### `neynar.search_casts`

Provider-ranked Farcaster cast search. Returns casts matching a search query, ordered by relevance (not chronologically).

| Argument | Type | Required | Description |
|----------|------|----------|-------------|
| `q` | Utf8 | Yes | Search query (keywords, phrases, or usernames) |

| Column | Type | Description |
|--------|------|-------------|
| `hash` | Utf8 | Cast hash identifier |
| `text` | Utf8 | Cast text content |
| `author__username` | Utf8 | Author's Farcaster username |
| `author__fid` | Int64 | Author's Farcaster ID |
| `author__display_name` | Utf8 | Author's display name |
| `reactions__likes_count` | Int64 | Number of likes |
| `reactions__recasts_count` | Int64 | Number of recasts |
| `replies__count` | Int64 | Number of replies |
| `timestamp` | Utf8 | Cast timestamp |
| `embeds` | Json | Embedded content (URLs, casts, etc.) |

**Call syntax:**

```sql
SELECT hash, text, author__username
FROM neynar.search_casts(q => 'your search query')
LIMIT 25;
```

---

## SQL Examples

### Search for casts about a topic

```sql
SELECT hash, text, author__username, reactions__likes_count
FROM neynar.search_casts(q => 'DAO governance')
ORDER BY reactions__likes_count DESC
LIMIT 10;
```

### Search for casts mentioning a specific topic

```sql
SELECT hash, text, author__username, timestamp
FROM neynar.search_casts(q => 'ethereum scaling')
LIMIT 20;
```

### Sentiment analysis on protocol mentions

```sql
SELECT
  author__username,
  text,
  reactions__likes_count,
  reactions__recasts_count
FROM neynar.search_casts(q => 'optimism grants')
LIMIT 50;
```

### Cross-source JOIN with other tables

Since `search_casts` is a table function with a required argument, join it with pre-computed data rather than passing outer columns into the function call:

```sql
-- Search for casts about a project, then join with local data
WITH cast_results AS (
  SELECT hash, text, author__username, reactions__likes_count
  FROM neynar.search_casts(q => 'aave')
  LIMIT 50
)
SELECT author__username, COUNT(*) AS mentions, AVG(reactions__likes_count) AS avg_likes
FROM cast_results
GROUP BY author__username
ORDER BY mentions DESC;
```

---

## Key API Limitations

### Paid plan required

The `/cast/search` endpoint requires a **paid Neynar API plan**. Free API keys return HTTP 402 (Payment Required). This is a Neynar restriction, not a Coral limitation.

### Provider-ranked retrieval

This source uses `kind: search` — a provider-ranked retrieval pattern. The API decides relevance ordering based on its own ranking algorithm. You cannot filter by `WHERE author__username = '...'` at the SQL level; instead, include relevant keywords in the search query to influence ranking.

### Search limits

- `default_top_k`: 25 results per call
- `max_top_k`: 100 results per call
- `max_calls_per_query`: 1 (no automatic pagination across multiple API calls)

### Rate limits

Neynar rate limits depend on your plan tier. The source supports cursor-based pagination for fetching additional pages of results within a query, up to the `max_top_k` limit.

---

## Source

- [Neynar API docs](https://docs.neynar.com)
- [Neynar dashboard](https://neynar.com)
- [Farcaster protocol](https://farcaster.xyz)
