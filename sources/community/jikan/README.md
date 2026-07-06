# Jikan (Community)

**Version:** 0.1.0
**Backend:** HTTP (Jikan REST API v4)
**Tables:** 4
**Base URL:** `https://api.jikan.moe/v4`

Query anime and manga data from MyAnimeList via the Jikan REST API v4.
Search anime and manga by keyword, browse top rankings, and filter by type,
status, and rating. No authentication required.

## Setup

The Jikan API is open and does not require authentication or API keys.

### 1. Add the source

```sh
coral source add --file sources/community/jikan/manifest.yaml
```

### 2. Verify

```sh
coral sql "SELECT mal_id, title, score FROM jikan.anime WHERE q = 'Cowboy Bebop' LIMIT 3"
```

## Tables

| Table | Description | Required filters | Optional filters |
|---|---|---|---|
| `jikan.anime` | Search and browse anime from MyAnimeList | — | `q`, `type`, `status`, `rating`, `sfw`, `order_by`, `sort` |
| `jikan.manga` | Search and browse manga from MyAnimeList | — | `q`, `type`, `status`, `sfw`, `order_by`, `sort` |
| `jikan.top_anime` | Fetch top-ranked anime from MyAnimeList | — | `type`, `filter`, `rating`, `sfw` |
| `jikan.top_manga` | Fetch top-ranked manga from MyAnimeList | — | `type`, `filter` |

All tables are read-only. This source does not create, modify, or delete any
MyAnimeList data.

### `anime`

Search and browse anime from MyAnimeList. Filter by keyword (`q`), type (e.g. `tv`, `movie`), airing status, age rating, and more. Results are paginated; use SQL `LIMIT` to control how many rows are returned.

### `manga`

Search and browse manga from MyAnimeList. Filter by keyword (`q`), type (e.g. `manga`, `novel`), publishing status, and more.

### `top_anime`

Fetch top-ranked anime from MyAnimeList. Optionally filter by media type (`type`), ranking category (`filter`), age rating (`rating`), or exclude adult content (`sfw`).

### `top_manga`

Fetch top-ranked manga from MyAnimeList. Optionally filter by media type (`type`) or ranking category (`filter`).

## Filters and pagination

All tables use page-based pagination (`page`, `limit`). The default page size is 25; the maximum is 25. Always use `LIMIT` when querying.

## Example queries

Search for an anime by keyword:

```sql
SELECT mal_id, title, score, episodes
FROM jikan.anime
WHERE q = 'Cowboy Bebop'
LIMIT 3;
```

```text
+--------+---------------------------------+-------+----------+
| mal_id | title                           | score | episodes |
+--------+---------------------------------+-------+----------+
| 1      | Cowboy Bebop                    | 8.75  | 26       |
| 5      | Cowboy Bebop: Tengoku no Tobira | 8.38  | 1        |
| 4037   | Cowboy Bebop: Yose Atsume Blues | 7.41  | 1        |
+--------+---------------------------------+-------+----------+
```

Fetch top-ranked anime:

```sql
SELECT mal_id, title, score, rank
FROM jikan.top_anime
LIMIT 5;
```

```text
+--------+------------------------------------------+-------+------+
| mal_id | title                                    | score | rank |
+--------+------------------------------------------+-------+------+
| 52991  | Sousou no Frieren                        | 9.27  | 1    |
| 61469  | Steel Ball Run: JoJo no Kimyou na Bouken | 9.13  | 2    |
| 5114   | Fullmetal Alchemist: Brotherhood         | 9.11  | 3    |
| 57555  | Chainsaw Man Movie: Reze-hen             | 9.07  | 4    |
| 9253   | Steins;Gate                              | 9.07  | 5    |
+--------+------------------------------------------+-------+------+
```

Search for a manga by keyword:

```sql
SELECT mal_id, title, score, chapters
FROM jikan.manga
WHERE q = 'Berserk'
LIMIT 3;
```

```text
+--------+--------------------------------------------------------------------------+-------+----------+
| mal_id | title                                                                    | score | chapters |
+--------+--------------------------------------------------------------------------+-------+----------+
| 2      | Berserk                                                                  | 9.46  |          |
| 113958 | Boushoku no Berserk: Ore dake Level to Iu Gainen wo Toppa suru the Comic | 6.87  |          |
| 115165 | Boushoku no Berserk: Ore dake Level to Iu Gainen wo Toppa suru           | 7.21  |          |
+--------+--------------------------------------------------------------------------+-------+----------+
```

Fetch top-ranked manga:

```sql
SELECT mal_id, title, score, rank
FROM jikan.top_manga
LIMIT 5;
```

```text
+--------+-------------------------------------------------+-------+------+
| mal_id | title                                           | score | rank |
+--------+-------------------------------------------------+-------+------+
| 2      | Berserk                                         | 9.46  | 1    |
| 1706   | JoJo no Kimyou na Bouken Part 7: Steel Ball Run | 9.34  | 2    |
| 656    | Vagabond                                        | 9.27  | 3    |
| 13     | One Piece                                       | 9.21  | 4    |
| 162032 | Guimi Zhi Zhu                                   | 9.18  | 5    |
+--------+-------------------------------------------------+-------+------+
```

Filter anime by type and status:

```sql
SELECT mal_id, title, type, status, score
FROM jikan.anime
WHERE type = 'tv' AND status = 'airing' AND order_by = 'score' AND sort = 'desc'
LIMIT 10;
```

```text
+--------+-------------------------------------------------+------+------------------+-------+
| mal_id | title                                           | type | status           | score |
+--------+-------------------------------------------------+------+------------------+-------+
| 61316  | Re:Zero kara Hajimeru Isekai Seikatsu 4th Season| TV   | Currently Airing | 8.9   |
| 51553  | Tongari Boushi no Atelier                       | TV   | Currently Airing | 8.74  |
| 21     | One Piece                                       | TV   | Currently Airing | 8.73  |
| 50250  | Chiikawa                                        | TV   | Currently Airing | 8.63  |
| 63375  | Nippon Sangoku                                  | TV   | Currently Airing | 8.48  |
| 62568  | Dr. Stone: Science Future Part 3                | TV   | Currently Airing | 8.28  |
| 59983  | Tsue to Tsurugi no Wistoria Season 2            | TV   | Currently Airing | 8.26  |
| 235    | Meitantei Conan                                 | TV   | Currently Airing | 8.19  |
| 60310  | Mairimashita! Iruma-kun 4th Season              | TV   | Currently Airing | 8.17  |
+--------+-------------------------------------------------+------+------------------+-------+
```

## Validation

Lint the manifest:

```sh
coral source lint sources/community/jikan/manifest.yaml
```

Add the source:

```sh
coral source add --file sources/community/jikan/manifest.yaml
```

Validate each table:

```sh
# anime
coral sql "SELECT mal_id, title, score FROM jikan.anime WHERE q = 'Cowboy Bebop' LIMIT 3"

# top_anime
coral sql "SELECT mal_id, title, score, rank FROM jikan.top_anime LIMIT 5"

# manga
coral sql "SELECT mal_id, title, score FROM jikan.manga WHERE q = 'Berserk' LIMIT 3"

# top_manga
coral sql "SELECT mal_id, title, score, rank FROM jikan.top_manga LIMIT 5"
```

Inspect registered tables and columns:

```sh
coral sql "SELECT table_name, description FROM coral.tables WHERE schema_name = 'jikan'"
coral sql "SELECT table_name, column_name, data_type FROM coral.columns WHERE schema_name = 'jikan' ORDER BY table_name, ordinal_position"
```

## Notes

- **No authentication:** The Jikan API is completely open and requires no authentication.
- **Rate limiting:** Jikan API has rate limits (e.g. 3 requests per second and 60 requests per minute). Use caching or throttling when querying large datasets to avoid being rate-limited.
- **Page-based pagination:** All tables paginate by `page` and `limit`. Always use `LIMIT` to avoid fetching all pages unnecessarily.
