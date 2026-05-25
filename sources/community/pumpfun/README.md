# Pump.fun

A community-maintained [Coral](https://withcoral.com) data source for [Pump.fun](https://pump.fun) — the leading Solana-native token launchpad.

Query real-time data on new token launches, bonding curve progress, market caps, graduated tokens, and more using Pump.fun's public `frontend-api-v3` endpoints.

> **Note:** This source uses unofficial public frontend endpoints. No API key is required — it works out of the box.

---

## Authentication

No authentication required. The source injects standard browser-like headers (`Origin`, `Accept`, `User-Agent`) automatically via `HeaderAuth`.

---

## Setup

```bash
coral source add --file path/to/sources/community/pumpfun/manifest.yaml
coral source test pumpfun
```

---

## Available Tables

| Table | Description |
|---|---|
| `coins` | General search & filtered list of all tokens |
| `latest_coins` | Most recently launched tokens (sorted by `created_timestamp DESC`) |
| `graduated_coins` | Tokens that have completed their bonding curve and migrated to Raydium |
| `active_trading_coins` | Tokens with the highest recent trading activity |
| `king_of_the_hill` | Current top bonding-curve token by market cap |
| `coin_detail` | Full metadata for a single token by mint address (requires `mint` filter) |
| `sol_price` | Current Solana price in USD |

---

## Usage Examples

### 1. Latest Token Launches

```sql
SELECT
    name,
    symbol,
    creator,
    created_timestamp,
    market_cap,
    usd_market_cap
FROM pumpfun.latest_coins
LIMIT 5;
```

### 2. Search Tokens by Keyword

```sql
SELECT
    mint,
    name,
    symbol,
    complete,
    market_cap
FROM pumpfun.coins
WHERE searchTerm = 'cat'
AND includeNsfw = false
LIMIT 10;
```

### 3. Tokens by a Specific Creator

```sql
SELECT
    mint,
    name,
    symbol,
    complete,
    market_cap
FROM pumpfun.coins
WHERE creator = '5NsQvpFG4RoVq3F3TDa9u9Cu8uppj2YNTr2ujwCcAez8'
LIMIT 5;
```

### 4. King of the Hill

```sql
SELECT
    name,
    symbol,
    market_cap,
    usd_market_cap
FROM pumpfun.king_of_the_hill;
```

### 5. Graduated Tokens (on Raydium)

```sql
SELECT
    name,
    symbol,
    creator,
    market_cap
FROM pumpfun.graduated_coins
LIMIT 10;
```

### 6. Full Token Detail

```sql
SELECT
    name,
    symbol,
    description,
    twitter,
    website,
    complete,
    market_cap
FROM pumpfun.coin_detail
WHERE mint = 'EHUbL3EDyeDsGq5qzewurxJkHLHSbb2Ypd9AtrSepump';
```

### 7. Current SOL Price

```sql
SELECT sol_price FROM pumpfun.sol_price;
```

---

## Live Query Evidence

**`coral source test pumpfun`**
```
✓ pumpfun connected successfully

  pumpfun (7 tables)
  ├─ active_trading_coins
  ├─ coin_detail
  ├─ coins
  ├─ graduated_coins
  ├─ king_of_the_hill
  ├─ latest_coins
  └─ sol_price
```

**Latest coins query:**
```
+---------------------------+--------+-------------------+--------------------+--------------------+
| name                      | symbol | created_timestamp | market_cap         | usd_market_cap     |
+---------------------------+--------+-------------------+--------------------+--------------------+
| zyn                       | ZYN    | 1779702519000     | 462.8968301271755  | 39738.66289003443  |
| Candy                     | CANDY  | 1779702297000     | 542.6430286443136  | 46584.6965921094   |
| I choose crypto everytime | CRYPTO | 1779700796000     | 23.73654828828164  | 2037.7298551426643 |
| 67FISH                    | 67FISH | 1779700349000     | 153.15493987384673 | 13148.010808192796 |
| Neuromancer               | Neuro  | 1779699422000     | 109.72791642390432 | 9419.897473044752  |
+---------------------------+--------+-------------------+--------------------+--------------------+
```

**King of the Hill:**
```
+----------+----------+--------------------+-------------------+
| name     | symbol   | market_cap         | usd_market_cap    |
+----------+----------+--------------------+-------------------+
| Fartcoin | Fartcoin | 2133832.6160587473 | 183202365.3905973 |
+----------+----------+--------------------+-------------------+
```

**SOL Price:**
```
+-------------------+
| sol_price         |
+-------------------+
| 85.85601514001486 |
+-------------------+
```

---

## Notes

- **Rate Limiting:** Pump.fun is protected by Cloudflare. Avoid polling more than once every 2–3 seconds. Excessive requests may result in HTTP 429 or temporary IP blocks.
- **Timestamps:** `created_timestamp` and `last_trade_timestamp` are returned as Unix milliseconds (`Int64`).
- **NSFW:** All preset tables (e.g., `latest_coins`, `graduated_coins`) set `includeNsfw = false` by default.
- **Pagination:** Set `fetch_limit_default` is applied per-table. Use `LIMIT` in your queries to control how many rows are returned.

---

*Maintained by the Coral Community.*
