# Pump.fun

A community-maintained data source for [Pump.fun](https://pump.fun), the popular Solana-native token launchpad and decentralized exchange.

This source provides real-time and historical data on token launches, token metadata, bonding curve status, market caps, and trading information via the `frontend-api-v3` endpoints. 

> **Disclaimer:** This source interacts with unofficial, publicly accessible frontend API endpoints owned and operated by Pump.fun. No authentication is required for the configured endpoints.

## Authentication

This integration utilizes public endpoints and does **not** require any API tokens or authentication keys. It works completely out of the box!

## Available Tables

This source exposes 7 powerful, structured tables designed to make querying Pump.fun easy and flexible:

1. `coins` - General search and listing of tokens.
2. `latest_coins` - The most recently launched tokens.
3. `graduated_coins` - Tokens that have completed their bonding curves and successfully migrated to Raydium.
4. `active_trading_coins` - Tokens with recent trading activity.
5. `king_of_the_hill` - The current top token closest to completing its curve.
6. `coin_detail` - Detailed information for a specific token mint.
7. `sol_price` - The current price of Solana (SOL) in USD.

## Usage Examples

### 1. View the Latest Token Launches
```sql
SELECT 
    mint, 
    name, 
    symbol, 
    created_timestamp, 
    market_cap, 
    usd_market_cap
FROM coral.pumpfun.latest_coins
LIMIT 10;
```

### 2. Search for Tokens by Creator
```sql
SELECT 
    mint, 
    name, 
    symbol,
    complete,
    virtual_sol_reserves
FROM coral.pumpfun.coins
WHERE creator = '5NsQvpFG4RoVq3F3TDa9u9Cu8uppj2YNTr2ujwCcAez8'
LIMIT 5;
```

### 3. Check the "King of the Hill"
```sql
SELECT 
    name, 
    symbol, 
    market_cap, 
    usd_market_cap 
FROM coral.pumpfun.king_of_the_hill;
```

### 4. Get Details for a Specific Coin
```sql
SELECT 
    name, 
    symbol, 
    description, 
    twitter, 
    telegram, 
    bonding_curve, 
    complete 
FROM coral.pumpfun.coin_detail
WHERE mint = '2Gxq9WmKxQiu7GmyzZ2wcJ9BeiZUEnWbchjCgpeTpump';
```

## Implementation Details & Considerations

*   **Rate Limiting:** Pump.fun employs Cloudflare and rate limits its endpoints. Typical limits are around 50 requests per minute for certain paths. Heavy polling or excessive querying may result in HTTP 429 (Too Many Requests) or Cloudflare blocks.
*   **Pagination:** Tables like `coins`, `latest_coins`, and `graduated_coins` default to a fetch limit of 20 or 50 records. This limit is passed natively through the `limit` query parameter.
*   **NSFW Content:** All pre-configured tables (e.g., `latest_coins`, `graduated_coins`) set `includeNsfw` to `false` by default to ensure safe data retrieval.

---
*Maintained by the Coral Community.*

### Live SQL Query Evidence

**1. Latest Coins Output:**
```
+-------------------------------+---------+--------------------+
| name                          | symbol  | market_cap         |
+-------------------------------+---------+--------------------+
| 67FISH                        | 67FISH  | 309.06011973778993 |
| Neuromancer                   | Neuro   | 154.04064900466471 |
| I choose mcdonald's everytime | MCDNLDS | 857.5740536338459  |
+-------------------------------+---------+--------------------+
```

**2. King of the Hill Output:**
```
+-----------+-----------+-------------------+
| name      | symbol    | market_cap        |
+-----------+-----------+-------------------+
| Fartcoin  | Fartcoin  | 2121975.682232697 |
+-----------+-----------+-------------------+
```

**3. SOL Price Output:**
```
+-------------------+
| sol_price         |
+-------------------+
| 85.83553494292714 |
+-------------------+
```
