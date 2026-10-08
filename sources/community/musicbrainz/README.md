# MusicBrainz

**Version:** 0.1.0
**Backend:** HTTP
**Functions:** 4
**Base URL:** `https://musicbrainz.org/ws/2`

Query the [MusicBrainz](https://musicbrainz.org) open music encyclopedia for artists, releases, recordings, and labels. No authentication required.

## Authentication

No API key or authentication is needed for read-only search operations. MusicBrainz requires a meaningful `User-Agent` header on every request; Coral automatically sets a descriptive one that satisfies this policy.

```bash
coral source add --file sources/community/musicbrainz/manifest.yaml
```

## Search Functions

| Function | Description | Args |
|---|---|---|
| `artists` | Search for artists | `query` (required) |
| `releases` | Search for releases (albums, singles, EPs) | `query` (required) |
| `recordings` | Search for recordings (individual tracks) | `query` (required) |
| `labels` | Search for labels (record companies, imprints) | `query` (required) |

## Quick start

```bash
# Search for an artist
coral sql "
  SELECT name, type, country, disambiguation
  FROM musicbrainz.artists
  WHERE query = 'The Beatles'
  LIMIT 5
"

# Search for releases
coral sql "
  SELECT title, status, date, country
  FROM musicbrainz.releases
  WHERE query = 'Abbey Road'
  LIMIT 5
"

# Search for recordings (tracks)
coral sql "
  SELECT title, length, artist_credit
  FROM musicbrainz.recordings
  WHERE query = 'Yesterday'
  LIMIT 5
"

# Search for labels
coral sql "
  SELECT name, type, country
  FROM musicbrainz.labels
  WHERE query = 'Parlophone'
  LIMIT 5
"

# Advanced Lucene query — British rock groups formed in the 1960s
coral sql "
  SELECT name, country, begin_date
  FROM musicbrainz.artists
  WHERE query = 'type:group AND country:GB AND begin:[1960 TO 1969]'
  LIMIT 10
"

# Find releases by a specific artist
coral sql "
  SELECT title, date, status, barcode
  FROM musicbrainz.releases
  WHERE query = 'artist:Radiohead'
  LIMIT 10
"

# Find recordings with a specific artist and title
coral sql "
  SELECT title, length, artist_credit
  FROM musicbrainz.recordings
  WHERE query = 'recording:Bohemian Rhapsody AND artist:Queen'
  LIMIT 5
"
```

## Live Testing Results

### Testing `artists` query
```console
$ coral sql "SELECT name, type, country FROM musicbrainz.artists WHERE query = 'The Beatles' LIMIT 1;"
+-------------+-------+---------+
| name        | type  | country |
+-------------+-------+---------+
| The Beatles | Group | GB      |
+-------------+-------+---------+
```

### Testing `releases` query
```console
$ coral sql "SELECT title, status, date FROM musicbrainz.releases WHERE query = 'Abbey Road' LIMIT 1;"
+--------------+----------+------+
| title        | status   | date |
+--------------+----------+------+
| Abbey's Road | Official | 2016 |
+--------------+----------+------+
```

### Testing `recordings` query
```console
$ coral sql "SELECT title, length FROM musicbrainz.recordings WHERE query = 'Yesterday' LIMIT 1;"
+---------------------+--------+
| title               | length |
+---------------------+--------+
| Yesterday Yesterday | 169939 |
+---------------------+--------+
```

### Testing `labels` query
```console
$ coral sql "SELECT name, type FROM musicbrainz.labels WHERE query = 'Parlophone' LIMIT 1;"
+------------+---------------------+
| name       | type                |
+------------+---------------------+
| Parlophone | Original Production |
+------------+---------------------+
```

## Column reference

### `artists`

| Column | Type | Description |
|---|---|---|
| `id` | Utf8 | MusicBrainz identifier (MBID) |
| `name` | Utf8 | Artist name |
| `sort_name` | Utf8 | Sort name for ordering |
| `type` | Utf8 | Artist type (Person, Group, Orchestra, etc.) |
| `country` | Utf8 | ISO 3166-1 alpha-2 country code |
| `area_name` | Utf8 | Primary associated area name |
| `disambiguation` | Utf8 | Disambiguation comment |
| `score` | Int64 | Search relevance score (higher is better) |
| `begin_date` | Utf8 | Begin date (e.g. `1960` or `1940-10-09`) |
| `end_date` | Utf8 | End date, if applicable |
| `ended` | Boolean | Whether the artist has ended |
| `query` | Utf8 | Echoes the search query used |

### `releases`

| Column | Type | Description |
|---|---|---|
| `id` | Utf8 | MusicBrainz identifier (MBID) |
| `title` | Utf8 | Release title |
| `status` | Utf8 | Release status (Official, Bootleg, Promotion, etc.) |
| `date` | Utf8 | Release date (`YYYY`, `YYYY-MM`, or `YYYY-MM-DD`) |
| `country` | Utf8 | Release country code |
| `barcode` | Utf8 | GTIN barcode |
| `disambiguation` | Utf8 | Disambiguation comment |
| `score` | Int64 | Search relevance score |
| `artist_credit` | Utf8 | Primary artist credit name |
| `query` | Utf8 | Echoes the search query used |

### `recordings`

| Column | Type | Description |
|---|---|---|
| `id` | Utf8 | MusicBrainz identifier (MBID) |
| `title` | Utf8 | Recording title |
| `length` | Int64 | Duration in milliseconds |
| `disambiguation` | Utf8 | Disambiguation comment |
| `score` | Int64 | Search relevance score |
| `artist_credit` | Utf8 | Primary artist credit name |
| `query` | Utf8 | Echoes the search query used |

### `labels`

| Column | Type | Description |
|---|---|---|
| `id` | Utf8 | MusicBrainz identifier (MBID) |
| `name` | Utf8 | Label name |
| `type` | Utf8 | Label type |
| `country` | Utf8 | ISO country code |
| `area_name` | Utf8 | Primary associated area name |
| `disambiguation` | Utf8 | Disambiguation comment |
| `score` | Int64 | Search relevance score |
| `begin_date` | Utf8 | Label begin date |
| `end_date` | Utf8 | Label end date, if applicable |
| `ended` | Boolean | Whether the label has ended |
| `query` | Utf8 | Echoes the search query used |

## Search query syntax

MusicBrainz uses [Lucene search syntax](https://lucene.apache.org/core/7_7_2/queryparser/org/apache/lucene/queryparser/classic/package-summary.html#package.description). Some useful patterns:

- Simple keyword: `The Beatles`
- Field search: `artist:Radiohead`, `country:GB`, `type:group`
- Boolean logic: `artist:Queen AND recording:Bohemian Rhapsody`
- Exact phrase: `"Abbey Road"`
- Escaping: `ac\/dc` for special characters

Common field names by entity type:

**Artists:** `artist`, `type`, `country`, `area`, `begin`, `end`, `gender`, `tag`
**Releases:** `release`, `artist`, `date`, `country`, `status`, `barcode`, `type`, `tag`
**Recordings:** `recording`, `artist`, `date`, `country`, `tag`
**Labels:** `label`, `type`, `country`, `area`, `tag`

## Rate limiting

MusicBrainz enforces a rate limit of **1 request per second** per client application. Because of this constraint, Coral limits each query to a single request (page). Add explicit `LIMIT` and `OFFSET` clauses to paginate through large result sets manually while spacing out your requests.

## Pagination

All search tables use offset pagination with `limit` (1–100, default 25) and `offset` request parameters. Because MusicBrainz rate limits each client to 1 request per second, Coral limits each query to a single request (page). Use explicit `LIMIT` and `OFFSET` clauses to paginate through larger result sets manually.

## Notes

- This source only exposes **search** endpoints. To look up an entity by its MBID, use the MusicBrainz API directly or extend this source with browse/lookup tables.
- The `length` column in `recordings` is in milliseconds. Divide by 1000 for seconds.
- Dates are returned as strings in varying precision (`YYYY`, `YYYY-MM`, or `YYYY-MM-DD`) depending on what MusicBrainz knows.
- `artist_credit` returns the first credited artist name only. For full credit details (including join phrases), the raw API response contains the full `artist-credit` array.
- Read-only. No tag submission, rating, or collection management via this source.

## References

- [MusicBrainz API documentation](https://musicbrainz.org/doc/MusicBrainz_API)
- [MusicBrainz Search documentation](https://musicbrainz.org/doc/MusicBrainz_API/Search)
- [MusicBrainz Rate Limiting](https://musicbrainz.org/doc/MusicBrainz_API/Rate_Limiting)
