# CoinGecko API

Query live cryptocurrency prices, market caps, and volumes using the free [CoinGecko API](https://www.coingecko.com/en/api).

## Rate Limits

**Important:** The keyless public API is heavily rate-limited (shared per IP) and is intended for low-volume testing only. If you encounter errors, you have likely hit the rate limit. See the [Keyless Public API Docs](https://docs.coingecko.com/docs/keyless-public-api) and [Rate Limit Errors](https://docs.coingecko.com/docs/common-errors-rate-limit) for more details.

## Setup

While heavily rate-limited, no API key or authentication is required to add the public source:

```bash
coral source add --file sources/community/coingecko/manifest.yaml
```

## Tables

### `markets`
Fetch live market data for cryptocurrencies. Requires the `vs_currency` filter (e.g. `usd`, `eur`). You can optionally provide a comma-separated list of `ids` (e.g. `bitcoin,ethereum`).

> **Note:** For a full list of valid values, refer to CoinGecko's [Supported vs_currencies](https://docs.coingecko.com/reference/simple-supported-currencies) and [Coins List](https://docs.coingecko.com/reference/coins-list) endpoints.

**Example:**
```sql
SELECT id, symbol, current_price, market_cap, price_change_percentage_24h
FROM coingecko.markets
WHERE vs_currency = 'usd'
LIMIT 10;
```

### `trending`
Fetch the top trending cryptocurrencies as searched by users in the last 24 hours.

**Example:**
```sql
SELECT id, name, symbol, market_cap_rank, price_btc
FROM coingecko.trending
ORDER BY score ASC
LIMIT 7;
```

### `exchanges`
Fetch a list of cryptocurrency exchanges with their trust scores and 24h trade volume.

**Example:**
```sql
SELECT name, country, trust_score, trade_volume_24h_btc
FROM coingecko.exchanges
ORDER BY trust_score_rank ASC
LIMIT 10;
```
