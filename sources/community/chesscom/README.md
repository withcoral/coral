# Chess.com
Adds a read-only community source for the [Chess.com Public API](https://www.chess.com/news/view/published-data-api), exposing player rating statistics, profile information, game history, and club memberships.

## What's included
`manifest.yaml` — four tables, requiring no authentication.

| Table | Purpose | API call |
|---|---|---|
| `stats` | Player rating statistics across rapid, blitz, bullet, and daily formats | `/player/{username}/stats` |
| `profile` | Player public profile information | `/player/{username}` |
| `games` | Game history for a given year and month. Includes PGN, results, and accuracy | `/player/{username}/games/{year}/{month}` |
| `clubs` | Chess clubs the player is a member of | `/player/{username}/clubs` |

## Setup
No authentication is required. You only need to provide the variables when adding the source:

```bash
coral source add --file sources/community/chesscom/manifest.yaml
```

**Variables:**
- `CHESSCOM_USERNAME`: Your Chess.com username (e.g. `anish789098`).
- `YEAR`: Year for game archive (e.g. `2024`).
- `MONTH`: Month for game archive, zero-padded (e.g. `05`).

## Verification
You can verify the source using the provided test queries:

```bash
coral source test chesscom
```

### Live Query - Player Statistics

```sql
SELECT chess_blitz__last__rating, chess_rapid__last__rating, chess_blitz__record__win
FROM chesscom.stats;
```

### Live Query - Recent Games

```sql
SELECT url, time_class, white__username, white__result, black__username, black__result
FROM chesscom.games 
LIMIT 5;
```
