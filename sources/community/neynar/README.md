# Neynar (Farcaster) Source

[Neynar](https://neynar.com) is the leading API provider for the [Farcaster](https://farcaster.xyz) decentralized social protocol. This community source exposes Farcaster cast search as a read-only SQL table function via [Coral](https://withcoral.com).

Uses `kind: search` — a provider-ranked retrieval pattern. Results come back in Neynar's default reverse-chronological order (`sort_type => 'desc_chron'`); pass `sort_type => 'algorithmic'` for relevance ranking. Search criteria are passed as function arguments, not SQL `WHERE` clauses.

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

Farcaster cast search. Returns casts matching a search query in reverse-chronological order by default (`sort_type => 'desc_chron'`), or by relevance with `sort_type => 'algorithmic'`.

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

This source uses `kind: search`. Search criteria are passed as function arguments — `q`, `author_fid`, `channel_id`, `mode`, `sort_type` — not SQL `WHERE` clauses, so you cannot filter by an output column such as `WHERE author__username = '...'`. Results are reverse-chronological by default; pass `sort_type => 'algorithmic'` for Neynar's relevance ranking. Use `author_fid` or keywords in `q` to scope by author.

### Result limits

SQL `LIMIT` controls how many results the Neynar API returns per page (default 25, max 100). This is mapped to the API's `limit` query parameter via Coral's pagination system. The `mode` parameter controls search behavior: `literal` (exact words), `semantic` (meaning-based), or `hybrid` (both).

### Rate limits

Neynar rate limits depend on your plan tier.

---

## Source

- [Neynar API docs](https://docs.neynar.com/reference/search-casts)
- [Neynar dashboard](https://neynar.com)
- [Farcaster protocol](https://farcaster.xyz)
