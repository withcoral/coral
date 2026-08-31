# Polymarket Data

Query Polymarket's public Data API: trades, leaderboards, wallet positions,
activity, holders, live event volume, and builder stats.

No authentication required.

This source covers `https://data-api.polymarket.com` only. The Gamma catalog
is `polymarket`. Public CLOB reads are `polymarket_clob`.

Columns were mapped from live Data API responses. Official docs:
[docs.polymarket.com](https://docs.polymarket.com).

## Setup

```bash
coral source add --file sources/community/polymarket_data/manifest.yaml
coral source test polymarket_data
```

## Tables

| Table | Description | Required filters |
|---|---|---|
| `trades` | Recent public trades | — |
| `leaderboard` | Trader leaderboard | — |
| `builder_leaderboard` | Builder leaderboard | — |
| `builder_volume` | Daily builder volume series | — |
| `positions` | Open positions for a wallet | `user` |
| `closed_positions` | Closed positions for a wallet | `user` |
| `activity` | Wallet activity | `user` |
| `value` | Portfolio value for a wallet | `user` |
| `holders` | Top holders for a market | `market` (condition id) |
| `live_volume` | Live volume for an event | `event_id` (Gamma event id) |

`builder_volume` returns a large unpaginated list. Always `LIMIT` it.

`holders` is one row per outcome token. The `holders` column is a JSON array
of holder objects from the live payload.

## SQL examples

```sql
SELECT title, side, size, price, transaction_hash
FROM polymarket_data.trades
LIMIT 10;
```

```sql
SELECT rank, user_name, pnl, vol
FROM polymarket_data.leaderboard
LIMIT 10;
```

```sql
SELECT title, size, cash_pnl, percent_pnl
FROM polymarket_data.positions
WHERE user = '0x34dd4a4b70eaf79a17878f7938263c801d4dfd83'
LIMIT 10;
```

```sql
SELECT title, realized_pnl, total_bought
FROM polymarket_data.closed_positions
WHERE user = '0x34dd4a4b70eaf79a17878f7938263c801d4dfd83'
LIMIT 10;
```

```sql
SELECT total, markets
FROM polymarket_data.live_volume
WHERE event_id = '903269';
```

## Limitations

- Wallet and market filters must be supplied as query parameters. They are
  not implied by a join alone.
- `live_volume.id` is a Gamma **event** id, not a condition id.
- No credentials are stored. All endpoints used here are public reads.
