# Polymarket CLOB

Query public Polymarket Central Limit Order Book reads: simplified markets,
order books, prices, midpoints, spreads, last trades, tick sizes, fee rates,
and price history.

No authentication required for these reads.

This source covers `https://clob.polymarket.com` public GET endpoints only.
Authenticated trading (`POST /order`, cancels, user trades) is not included.
Coral cannot compute Polymarket's per-request HMAC-SHA256 L2 headers.

## Setup

```bash
coral source add --file sources/community/polymarket_clob/manifest.yaml
coral source test polymarket_clob
```

## Tables

| Table | Description | Required filters |
|---|---|---|
| `simplified_markets` | CLOB market list | — |
| `sampling_markets` | Sampling-program markets | — |
| `sampling_simplified_markets` | Simplified sampling list | — |
| `order_book` | Book snapshot | `token_id` |
| `price` | Best price for a side | `token_id`, `side` (`BUY`/`SELL`) |
| `midpoint` | Midpoint price | `token_id` |
| `spread` | Bid-ask spread | `token_id` |
| `last_trade_price` | Last public trade | `token_id` |
| `tick_size` | Minimum tick size | `token_id` |
| `fee_rate` | Base fee | `token_id` |
| `prices_history` | Historical price points | `market` (token id), `history_interval` |

`token_id` values come from `polymarket.markets.clob_token_ids` (a
JSON-encoded string of outcome token ids).

### Not included

- `POST /order`, `POST /orders`, cancels, heartbeats, and authenticated
  `/trades` or `/orders` lists. Those require L2 HMAC signing.
- Body-form batch POSTs (`/books`, `/prices`). Use the GET query forms.

## SQL examples

```sql
SELECT condition_id, active, accepting_orders
FROM polymarket_clob.simplified_markets
LIMIT 10;
```

```sql
SELECT market, tick_size, last_trade_price, bids, asks
FROM polymarket_clob.order_book
WHERE token_id = '32338220190071351435772801779725302244575775216413325951443816017994629993401';
```

```sql
SELECT t, p
FROM polymarket_clob.prices_history
WHERE market = '32338220190071351435772801779725302244575775216413325951443816017994629993401'
  AND history_interval = '1d'
LIMIT 20;
```

## Limitations

- Market lists use cursor pagination (`next_cursor`).
- Book bids/asks are JSON arrays on one row, matching the live snapshot.
- Price fields on several CLOB endpoints are strings in the live payload.
