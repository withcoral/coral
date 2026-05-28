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
| `q` | Utf8 | Yes | Search query (supports `+` AND, `\|` OR, `*` prefix, `""` phrase, `~n` fuzziness, `-` negate) |

| Filter | Type | Required | Description |
|--------|------|----------|-------------|
| `author_fid` | Int64 | No | FID of the user whose casts to search |
| `channel_id` | Utf8 | No | Filter by channel ID |
| `mode` | Utf8 | No | Search mode: `literal` (default), `semantic`, or `hybrid` |
| `limit` | Int64 | No | Results per page (1-100, default 25) |

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

### Search within a specific channel

```sql
SELECT hash, text, author__username, timestamp
FROM neynar.search_casts(q => 'ethereum')
WHERE channel_id = 'ethereum'
LIMIT 20;
```

### Semantic search for conceptually related casts

```sql
SELECT hash, text, author__username
FROM neynar.search_casts(q => 'decentralized governance proposals')
WHERE mode = 'semantic'
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

### Provider-ranked retrieval

This source uses `kind: search` — a provider-ranked retrieval pattern. The API decides relevance ordering based on its own ranking algorithm. You cannot filter by `WHERE author__username = '...'` at the SQL level; instead, use the `author_fid` filter or include keywords in the search query.

### Result limits

Returns up to 100 results per query (one page). Use the `limit` parameter to control result count. The `mode` parameter controls search behavior: `literal` (exact words), `semantic` (meaning-based), or `hybrid` (both).

### Rate limits

Neynar rate limits depend on your plan tier.

---

## Source

- [Neynar API docs](https://docs.neynar.com/reference/search-casts)
- [Neynar dashboard](https://neynar.com)
- [Farcaster protocol](https://farcaster.xyz)
