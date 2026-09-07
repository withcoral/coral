# Kalshi

Query Kalshi's public Trade API: series, events, markets, public trades,
order books, candlesticks, milestones, and exchange metadata.

No authentication required for these reads.

This source covers `https://external-api.kalshi.com/trade-api/v2` only.
Authenticated portfolio, orders, RFQ, FCM, WebSocket, and FIX surfaces are
not included. Coral cannot compute Kalshi's RSA-PSS request signatures.

Columns were mapped from live responses. Official docs:
[docs.kalshi.com](https://docs.kalshi.com).

## Setup

```bash
coral source add --file sources/community/kalshi/manifest.yaml
coral source test kalshi
```

## Tables

| Table | Description | Required filters |
|---|---|---|
| `series` | One series by ticker | `ticker` |
| `events` | Events (not multivariate combos) | — |
| `multivariate_events` | Combo events | — |
| `event` | One event with nested markets | `event_ticker` |
| `event_metadata` | Images and market-detail metadata | `event_ticker` |
| `markets` | Binary markets | — |
| `market` | One market by ticker | `ticker` |
| `orderbook` | YES/NO bid book | `ticker` |
| `orderbooks` | Batched books | `tickers` |
| `trades` | Recent public trades | — |
| `candlesticks` | OHLC candles | `series_ticker`, `ticker`, `start_ts`, `end_ts`, `period_interval` |
| `historical_markets` | Archived markets | — |
| `historical_trades` | Archived public trades | — |
| `historical_cutoff` | Live vs archive split | — |
| `milestones` | Sports and other milestones | — |
| `structured_targets` | Structured target definitions | — |
| `multivariate_collections` | Combo collections | — |
| `incentive_programs` | Liquidity incentives | — |
| `exchange_status` | Trading/shard status | — |
| `exchange_schedule` | Hours and maintenance | — |
| `tags_by_category` | Tags grouped by category | — |

Join keys: `markets.event_ticker` / `event.event_ticker`,
`events.series_ticker` / `series.ticker`, `trades.ticker` / `markets.ticker`.

### Not included

- `GET /series` list: the API ignores `limit` and returns a multi-megabyte
  payload. Use `series` with `ticker`.
- `/portfolio/*`, order create/cancel/amend, RFQ, FCM, and WebSocket/FIX.
  Those need RSA-PSS signed headers.
- Live-data milestone paths that returned HTTP 404 without extra ids.
- Sport filter taxonomy (`/search/filters_by_sport`): nested, not row-shaped.

## SQL examples

```sql
SELECT ticker, title, yes_bid_dollars, yes_ask_dollars, status
FROM kalshi.markets
WHERE status = 'open'
LIMIT 10;
```

```sql
SELECT event_ticker, title, category
FROM kalshi.events
WHERE status = 'open'
LIMIT 10;
```

```sql
SELECT ticker, title, category, frequency
FROM kalshi.series
WHERE ticker = 'KXELONMARS';
```

```sql
SELECT ticker, yes_dollars, no_dollars
FROM kalshi.orderbook
WHERE ticker = 'KXELONMARS-99';
```

```sql
SELECT ticker, end_period_ts, volume_fp, price
FROM kalshi.candlesticks
WHERE series_ticker = 'KXELONMARS'
  AND ticker = 'KXELONMARS-99'
  AND start_ts = '1787702400'
  AND end_ts = '1788393600'
  AND period_interval = '1440';
```

## Limitations

- `GET /markets?status=open` is the list filter. A single `market` row
  may report `status` as `active` for the same contract.
- Dollar and size fields are fixed-point strings (`yes_bid_dollars`,
  `volume_fp`), matching the live payload.
- Order books contain bids only. A YES bid at X is a NO ask at `1 - X`.
- `candlesticks.period_interval` must be `1`, `60`, or `1440`.
- `GET /series` is not exposed as a list table; look up one ticker at a time.
- Rate limits are token-bucket by API tier. Keep validation queries small.
