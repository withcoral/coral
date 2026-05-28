# DeFiLlama Source

[DeFiLlama](https://defillama.com) is the largest TVL aggregator for DeFi protocols. This community source exposes DeFi protocol TVL, market cap, and price-change metrics as read-only SQL tables via [Coral](https://withcoral.com).

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

All DeFi protocols tracked by DeFiLlama with TVL, market cap, and price change metrics over 1d, 7d, and 30d windows.

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
| `change_1m` | Float64 | TVL % change over 30 days |
| `mcap` | Float64 | Circulating market cap in USD |
| `fdv` | Float64 | Fully diluted valuation in USD |
| `staking` | Float64 | Total staked value in USD |

### `defillama.protocol_tvl`

Historical TVL for a specific protocol by slug. Returns a single row with current TVL and chain breakdown.

| Filter | Type | Required | Description |
|--------|------|----------|-------------|
| `slug` | Utf8 | Yes | Protocol slug (e.g. `'aave'`, `'uniswap'`, `'lido'`) |

| Column | Type | Description |
|--------|------|-------------|
| `name` | Utf8 | Protocol display name |
| `slug` | Utf8 | Protocol slug identifier |
| `tvl` | Float64 | Current total value locked in USD |
| `chain_tvls` | Json | TVL breakdown by chain as a JSON object |
| `change_1d` | Float64 | TVL % change over 1 day |
| `change_7d` | Float64 | TVL % change over 7 days |
| `change_1m` | Float64 | TVL % change over 30 days |

---

## SQL Examples

### Top 10 protocols by TVL

```sql
SELECT name, tvl, change_7d, category
FROM defillama.protocols
ORDER BY tvl DESC
LIMIT 10;
```

### Protocols with declining TVL (risk signal)

```sql
SELECT name, slug, tvl, change_7d, change_1m
FROM defillama.protocols
WHERE change_7d < -10
ORDER BY change_7d ASC
LIMIT 20;
```

### Single protocol detail

```sql
SELECT name, tvl, change_1d, change_7d, chain_tvls
FROM defillama.protocol_tvl
WHERE slug = 'aave';
```

### Cross-source JOIN with a grantee registry

```sql
SELECT g.recipient_name, d.tvl, d.change_7d
FROM grantees.registry g
JOIN defillama.protocols d ON d.slug = g.project_slug
ORDER BY d.tvl DESC;
```

---

## API Limitations

- **No authentication required** — the DeFiLlama API is fully public.
- **No pagination** — the `/protocols` endpoint returns all protocols in a single response (~2000+ protocols).
- **Rate limits** — DeFiLlama does not publish rate limits, but aggressive polling may be throttled. One request per query is typical.
- **Data freshness** — TVL data is updated periodically (typically every few hours), not in real-time.

---

## Source

- [DeFiLlama API](https://defillama.com/docs/api)
- [DeFiLlama website](https://defillama.com)
