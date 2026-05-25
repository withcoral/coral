# CoinGecko API

Query live cryptocurrency prices, market caps, and volumes using the free [CoinGecko API](https://www.coingecko.com/en/api).

## Setup

No API key or authentication is needed for the public endpoints. Add the source directly:

```bash
coral source add --file sources/community/coingecko/manifest.yaml
```

## Tables

### `markets`
Fetch live market data for cryptocurrencies. Requires the `vs_currency` filter (e.g. `usd`, `eur`). You can optionally provide a comma-separated list of `ids` (e.g. `bitcoin,ethereum`).

**Example:**
```sql
SELECT id, symbol, current_price, market_cap, price_change_percentage_24h
FROM coingecko.markets
WHERE vs_currency = 'usd'
LIMIT 10;
```
