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

## Schema Alignment & Core Columns

All coin-related tables (`coins`, `latest_coins`, `graduated_coins`, `active_trading_coins`, `king_of_the_hill`, and `coin_detail`) share a consistent aligned schema:

| Column | Type | Description |
|---|---|---|
| `mint` | `Utf8` | Token mint address (Solana token identifier) |
| `name` | `Utf8` | Full name of the token |
| `symbol` | `Utf8` | Token symbol |
| `description` | `Utf8` | Token description |
| `image_uri` | `Utf8` | Token image URI (IPFS or web URL) |
| `metadata_uri` | `Utf8` | Token metadata JSON URI |
| `bonding_curve` | `Utf8` | Bonding curve program address |
| `associated_bonding_curve` | `Utf8` | Associated bonding curve token account |
| `creator` | `Utf8` | Wallet address of the token creator |
| `created_timestamp` | `Int64` | Token creation Unix timestamp (milliseconds) |
| `last_trade_timestamp` | `Int64` | Last trade Unix timestamp (milliseconds) |
| `complete` | `Boolean` | Whether the bonding curve has completed / graduated |
| `market_cap` | `Float64` | Current token market cap in SOL |
| `usd_market_cap` | `Float64` | Current token market cap in USD |
| `virtual_sol_reserves` | `Int64` | Virtual SOL reserves in the bonding curve |
| `virtual_token_reserves` | `Int64` | Virtual token reserves in the bonding curve |
| `real_sol_reserves` | `Int64` | Real SOL reserves |
| `real_token_reserves` | `Int64` | Real token reserves |
| `total_supply` | `Int64` | Total token supply |
| `reply_count` | `Int64` | Number of replies/comments on the token page |
| `is_currently_live` | `Boolean` | Whether the token currently has an active livestream |
| `nsfw` | `Boolean` | Whether the token is flagged as NSFW |
| `is_banned` | `Boolean` | Whether the token is banned |
| `ath_market_cap` | `Float64` | All-Time High market cap in SOL |
| `ath_market_cap_timestamp` | `Int64` | Unix timestamp when All-Time High market cap was reached (milliseconds) |
| `twitter` | `Utf8` | Creator's Twitter link |
| `telegram` | `Utf8` | Creator's Telegram link |
| `website` | `Utf8` | Creator's Website link |

---

## Usage Examples

### 1. Latest Token Launches

```sql
SELECT
    name,
    symbol,
    total_supply,
    created_timestamp,
    market_cap,
    usd_market_cap
FROM pumpfun.latest_coins
LIMIT 5;
```

### 2. Paginated Token Search

```sql
SELECT
    mint,
    name,
    symbol,
    complete,
    market_cap
FROM pumpfun.coins
WHERE searchTerm = 'cat'
ORDER BY market_cap DESC
LIMIT 5 OFFSET 10;
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

### 4. King of the Hill (Highest market cap bonding curve token)

```sql
SELECT
    name,
    symbol,
    total_supply,
    ath_market_cap,
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
    bonding_curve,
    total_supply,
    ath_market_cap,
    complete
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
    Query tests
    2 declared · 2 passed · 0 failed

    ✓ SELECT name, symbol, creator, created_timestamp, market_cap, usd_market_cap FROM pumpfun.latest_coins LIMIT 5
      5 rows

    ✓ SELECT sol_price FROM pumpfun.sol_price
      1 row
```

**Latest coins query:**
```
+---------------------------+---------+-------------------+------------------+--------------------+--------------------+
| name                      | symbol  | created_timestamp | total_supply     | market_cap         | usd_market_cap     |
+---------------------------+---------+-------------------+------------------+--------------------+--------------------+
| I choose job everytime    | OFFICE  | 1779704422000     | 1000000000000000 | 749.1793146186504  | 64339.3792241575   |
| Chief Wellbeing Officer   | Horatio | 1779704162000     | 1000000000000000 | 310.2255474226236  | 26642.11190989168  |
| zyn                       | ZYN     | 1779702519000     | 1000000000000000 | 254.9461853468451  | 21894.73064820611  |
| Candy                     | CANDY   | 1779702297000     | 1000000000000000 | 1373.753849074799  | 117977.72326543785 |
| I choose crypto everytime | CRYPTO  | 1779700796000     | 1000000000000000 | 22.704759438703285 | 1949.880488176033  |
+---------------------------+---------+-------------------+------------------+--------------------+--------------------+
```

**King of the Hill:**
```
+-----------+-----------+-----------------+--------------------+------------------+--------------------+
| name      | symbol    | total_supply    | ath_market_cap     | market_cap       | usd_market_cap     |
+-----------+-----------+-----------------+--------------------+------------------+--------------------+
| Fartcoin  | Fartcoin  | 999978899969322 | 428151807937.17694 | 2148308.49226242 | 184639724.66632503 |
+-----------+-----------+-----------------+--------------------+------------------+--------------------+
```

**SOL Price:**
```
+-------------------+
| sol_price         |
+-------------------+
| 85.88176255660566 |
+-------------------+
```

---

## Notes

- **Rate Limiting:** Pump.fun is protected by Cloudflare. Avoid polling more than once every 2–3 seconds. Excessive requests may result in HTTP 429 or temporary IP blocks.
- **Timestamps:** `created_timestamp` and `last_trade_timestamp` are returned as Unix milliseconds (`Int64`).
- **NSFW:** All preset tables (e.g., `latest_coins`, `graduated_coins`) set `includeNsfw = false` by default.
- **Pagination:** Supported natively via `LIMIT` and `OFFSET` in SQL queries, utilizing the backend API's offset pagination.

---

*Maintained by the Coral Community.*
