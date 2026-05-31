# Neynar (Farcaster) Source

[Neynar](https://neynar.com) is the leading API provider for the [Farcaster](https://farcaster.xyz) decentralized social protocol. This community source exposes Farcaster cast search as a read-only SQL table function via [Coral](https://withcoral.com).

Uses `kind: search` — a provider-ranked retrieval pattern. Results come back in Neynar's default reverse-chronological order (`sort_type => 'desc_chron'`); pass `sort_type => 'algorithmic'` to order by engagement and time. Search criteria are passed as function arguments, not SQL `WHERE` clauses.

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

Farcaster cast search. Returns casts matching a search query in reverse-chronological order by default (`sort_type => 'desc_chron'`), or ordered by engagement and time with `sort_type => 'algorithmic'`.

| Argument | Type | Required | Description |
|----------|------|----------|-------------|
| `q` | Utf8 | Yes | Search query (supports `+` AND, `\|` OR, `*` prefix, `""` phrase, `~n` fuzziness, `-` negate) |
| `author_fid` | Int64 | No | Restrict to casts from this Farcaster ID |
| `channel_id` | Utf8 | No | Restrict to a channel ID |
| `mode` | Utf8 | No | Search mode: `literal` (default), `semantic`, or `hybrid` |
| `sort_type` | Utf8 | No | Result ordering: `desc_chron` (default), `chron`, or `algorithmic` |

All arguments are passed in the function call, e.g. `search_casts(q => '...', channel_id => '...', mode => 'semantic')` — not as SQL `WHERE` clauses.

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
| `timestamp` | Timestamp | Cast publication time (ISO 8601 from the API, exposed as a Timestamp) |
| `embeds` | Json | Embedded content (URLs, casts, etc.) |

**Call syntax:**

```sql
-- SQL LIMIT controls how many results the Neynar API returns (default 25, max 100)
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

### Search within a specific channel

```sql
SELECT hash, text, author__username, timestamp
FROM neynar.search_casts(q => 'ethereum', channel_id => 'ethereum')
LIMIT 20;
```

### Semantic search for conceptually related casts

```sql
SELECT hash, text, author__username
FROM neynar.search_casts(q => 'decentralized governance proposals', mode => 'semantic')
LIMIT 15;
```

### Cross-source JOIN with other tables

Since `search_casts` is a table function with a required argument, use a CTE to search first, then join:

```sql
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

### Search-function semantics

This source uses `kind: search`. **Provider-side** search criteria are passed as function arguments — `q`, `author_fid`, `channel_id`, `mode`, `sort_type` — which is how you narrow what Neynar fetches and ranks. You can still apply ordinary SQL predicates on output columns (e.g. `WHERE author__username = '...'`), but those filter the returned result set **locally, after retrieval** — they are not pushed to Neynar, so they don't change which casts the provider ranks and returns (and they only see the casts already fetched for that call). Results are reverse-chronological by default; pass `sort_type => 'algorithmic'` for Neynar's engagement-and-time ordering. Scope by author with the `author_fid` argument or keywords in `q`.

### Result limits

SQL `LIMIT` controls how many results the Neynar API returns per page (default 25, max 100). This is mapped to the API's `limit` query parameter via Coral's pagination system. The `mode` parameter controls search behavior: `literal` (exact words), `semantic` (meaning-based), or `hybrid` (both).

### Rate limits

Neynar applies a separate, lower rate limit to `/cast/search` than its global per-key limit. By plan tier: **Starter 60 RPM, Growth 120 RPM, Scale 240 RPM**, Enterprise custom. Each `search_casts(...)` call is a single request (`max_calls_per_query: 1`), so size pages with `LIMIT` (max 100) and add retry/backoff if you fan out many searches. See [Neynar rate limits](https://docs.neynar.com/reference/what-are-the-rate-limits-on-neynar-apis).

---

## Source

- [Neynar API docs](https://docs.neynar.com/reference/search-casts)
- [Neynar dashboard](https://neynar.com)
- [Farcaster protocol](https://farcaster.xyz)
