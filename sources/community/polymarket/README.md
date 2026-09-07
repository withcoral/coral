# Polymarket (Gamma)

Query Polymarket's public Gamma catalog: markets, events, tags, series, sports
metadata, and public profiles.

No authentication required.

This source covers `https://gamma-api.polymarket.com` only. Analytics live in
`polymarket_data`. Public CLOB reads live in `polymarket_clob`. Authenticated
CLOB trading is not included.

Columns were mapped from live Gamma list responses. The reverse-engineered
OpenAPI in [modiqo/api-specs](https://github.com/modiqo/api-specs) was used as
a draft, not as proof of row shape. Official docs:
[docs.polymarket.com](https://docs.polymarket.com).

## Setup

```bash
coral source add --file sources/community/polymarket/manifest.yaml
coral source test polymarket
```

## Tables

| Table | Description | Required filters |
|---|---|---|
| `markets` | Prediction markets | — |
| `events` | Event containers for one or more markets | — |
| `tags` | Catalog tags | — |
| `series` | Recurring series such as NFL | — |
| `sports` | Sports metadata | — |
| `public_profiles` | Public profile for a wallet | `address` |

Join keys: `markets.condition_id` matches Data/CLOB condition ids.
`events.id` is the Data API `live_volume` event id.

### Not included

- `GET /comments` and `GET /sports/teams` return HTTP 422 on the public API.
- `GET /search` returns HTTP 401 without credentials.
- `GET /profiles/{address}` is not a public GET. Use `public_profiles`.

## SQL examples

```sql
SELECT question, volume_24hr, best_bid, best_ask, active
FROM polymarket.markets
WHERE active_filter = 'true'
ORDER BY volume_24hr DESC
LIMIT 10;
```

```sql
SELECT title, volume, comment_count
FROM polymarket.events
WHERE active_filter = 'true'
LIMIT 10;
```

```sql
SELECT name, verified_badge, weighted_volume
FROM polymarket.public_profiles
WHERE address = '0x34dd4a4b70eaf79a17878f7938263c801d4dfd83';
```

## Limitations

- Use `active_filter` / `closed_filter` / `archived_filter` to push those
  flags to Gamma. `active`, `closed`, and `archived` are Boolean response
  columns and cannot take a string equality.
- Use `sort_by` (not `order` or `sort`) for Gamma's `order` query. Values
  are camelCase (`volume24hr`, `volume`, `liquidity`). The returned order
  is Gamma's.
- Gamma list fields such as `outcomes` and `clob_token_ids` are JSON-encoded
  strings, matching the live payload.
- Nested `events`, `markets`, and `tags` arrays are exposed as `Json`.
- Rate limits are not published. Keep validation queries small.
