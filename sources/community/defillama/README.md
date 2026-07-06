# DeFiLlama Source

[DeFiLlama](https://defillama.com) is the largest TVL aggregator for DeFi protocols. This community source exposes DeFi protocol TVL and TVL-change metrics as read-only SQL tables via [Coral](https://withcoral.com).

No authentication required — the DeFiLlama API is free and keyless.

---

## Setup

### 1. Add the Source to Coral

```bash
coral source add --file sources/community/defillama/manifest.yaml
```

### 2. Verify Connection

```bash
coral source test defillama
```

---

## Tables

### `defillama.protocols`

All DeFi protocols tracked by DeFiLlama with TVL and TVL-change metrics over 1d and 7d windows.

No required filters — returns all protocols in a single request.

| Column | Type | Description |
|--------|------|-------------|
| `name` | Utf8 | Protocol display name |
| `slug` | Utf8 | Protocol slug identifier (use for cross-referencing) |
| `category` | Utf8 | DeFi category (e.g. Liquid Staking, Lending, DEX) |
| `chains` | Json | Array of chain names the protocol operates on |
| `tvl` | Float64 | Total value locked in USD |
| `change_1d` | Float64 | TVL % change over 1 day |
| `change_7d` | Float64 | TVL % change over 7 days |

### `defillama.protocol_tvl`

Protocol metadata with chain-level TVL breakdown. Returns a single row with `currentChainTvls` and `chainTvls` JSON objects.

| Filter | Type | Required | Description |
|--------|------|----------|-------------|
| `slug` | Utf8 | Yes | Protocol slug (e.g. `'aave'`, `'uniswap'`, `'lido'`) |

| Column | Type | Description |
|--------|------|-------------|
| `name` | Utf8 | Protocol display name |
| `category` | Utf8 | DeFi category |
| `chains` | Json | Array of chain names the protocol operates on |
| `current_chain_tvls` | Json | Current TVL per chain (e.g. `{"Ethereum": 5000000000}`) |
| `chain_tvls` | Json | Historical TVL data per chain |

---

## SQL Examples

### Top 10 protocols by TVL

```sql
SELECT name, tvl, change_7d, category
FROM defillama.protocols
WHERE tvl IS NOT NULL
ORDER BY tvl DESC
LIMIT 10;
```

### Protocols with declining TVL (risk signal)

```sql
SELECT name, slug, tvl, change_7d
FROM defillama.protocols
WHERE change_7d < -10 AND tvl IS NOT NULL
ORDER BY change_7d ASC
LIMIT 20;
```

### Single protocol chain breakdown

```sql
SELECT name, current_chain_tvls, chain_tvls
FROM defillama.protocol_tvl
WHERE slug = 'aave';
```

### Cross-source JOIN with a grantee registry

```sql
SELECT g.recipient_name, d.tvl, d.change_7d
FROM grantees.registry g
JOIN defillama.protocols d ON d.slug = g.project_slug
WHERE d.tvl IS NOT NULL
ORDER BY d.tvl DESC;
```

---

## API Limitations

- **No authentication required** — the DeFiLlama API is fully public.
- **No pagination** — the `/protocols` endpoint returns all protocols in a single response (~2000+ protocols).
- **Null TVL rows** — some protocols have null TVL. Use `WHERE tvl IS NOT NULL` in queries and examples.
- **Rate limits** — DeFiLlama does not publish rate limits, but aggressive polling may be throttled. One request per query is typical.
- **Data freshness** — TVL data is updated periodically (typically every few hours), not in real-time.

---

## Source

- [DeFiLlama API docs](https://api-docs.defillama.com/llms-free.txt)
- [DeFiLlama website](https://defillama.com)
