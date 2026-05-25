# Spotify community source

The `spotify` community source exposes read-only Spotify profile, playlist,
library, catalog search/detail, device, queue, top-item, and recent listening
data through Coral SQL.

## Setup

Create a Spotify app in the Spotify Developer Dashboard:

https://developer.spotify.com/dashboard

Add this redirect URI exactly:

```text
http://127.0.0.1:53682/oauth/callback
```

For OAuth setup, copy the app Client ID and install the source interactively:

```sh
export SPOTIFY_CLIENT_ID="<your-spotify-client-id>"
coral source add --file sources/community/spotify/manifest.yaml --interactive
```

Choose **Connect with Spotify**. Coral uses Spotify Authorization Code with
PKCE, stores the resulting access token, and sends it as
`Authorization: Bearer <token>`.

The source requests these read-only scopes:

| Scope | Used for |
| --- | --- |
| `user-read-private` | Profile country and subscription product. |
| `user-read-email` | Profile email. |
| `playlist-read-private` | Private playlists. |
| `playlist-read-collaborative` | Collaborative playlists. |
| `user-library-read` | Saved tracks and albums. |
| `user-top-read` | Top tracks and artists. |
| `user-read-recently-played` | Recent listening history. |
| `user-read-playback-state` | Current playback queue and visible devices. |

You can also paste an existing Spotify OAuth access token when adding the
source. Pasted-token setup only needs `SPOTIFY_ACCESS_TOKEN`; a Spotify Client
ID is only needed for the interactive OAuth flow.

## Tables

| Table | Purpose | Required filters | Optional filters |
| --- | --- | --- | --- |
| `spotify.profile` | Authenticated user profile and account metadata. | — | — |
| `spotify.playlists` | Playlists owned or followed by the authenticated user. | — | — |
| `spotify.devices` | Spotify Connect devices visible to the account. | — | — |
| `spotify.queue` | Current playback queue in order; first row is next. | — | — |
| `spotify.playlist_items` | Tracks and episodes in a playlist. | `playlist_id` | `market` |
| `spotify.saved_tracks` | Tracks saved in the user's library. | — | `market` |
| `spotify.saved_albums` | Albums saved in the user's library. | — | `market` |
| `spotify.saved_shows` | Shows saved in the user's library. | — | `market` |
| `spotify.saved_episodes` | Episodes saved in the user's library. | — | `market` |
| `spotify.saved_audiobooks` | Audiobooks saved in the user's library. | — | — |
| `spotify.top_tracks` | Tracks with highest affinity for the user. | — | `time_range` |
| `spotify.top_artists` | Artists with highest affinity for the user. | — | `time_range` |
| `spotify.tracks` | Track details by ID. | `id` | `market` |
| `spotify.albums` | Album details by ID. | `id` | `market` |
| `spotify.artists` | Artist details by ID. | `id` | — |
| `spotify.shows` | Show details by ID. | `id` | `market` |
| `spotify.episodes` | Episode details by ID. | `id` | `market` |
| `spotify.audiobooks` | Audiobook details by ID. | `id` | `market` |
| `spotify.album_tracks` | Tracks in an album. | `album_id` | `market` |
| `spotify.artist_albums` | Albums for an artist. | `artist_id` | `include_groups`, `market` |
| `spotify.show_episodes` | Episodes in a show. | `show_id` | `market` |
| `spotify.audiobook_chapters` | Chapters in an audiobook. | `audiobook_id` | `market` |
| `spotify.recently_played` | Recent listening-history items. | — | `after` or `before` |

All tables are read-only. This source does not create, update, or delete any
Spotify data.

## Example queries

Confirm the connected account:

```sql
SELECT id, display_name, email, country, product
FROM spotify.profile;
```

List playlists and discover playlist IDs:

```sql
SELECT id, name, owner__display_name, tracks_total
FROM spotify.playlists
ORDER BY name
LIMIT 25;
```

List tracks in an owned or collaborative playlist. Spotify may reject item
queries for followed playlists that are neither owned by the current user nor
collaborative, even when they appear in `spotify.playlists`:

```sql
SELECT added_at, track__name, artist_names, album__name
FROM spotify.playlist_items
WHERE playlist_id = '<owned-or-collaborative-playlist-id>'
LIMIT 50;
```

Inspect saved tracks:

```sql
SELECT added_at, track__name, artist_names, album__name
FROM spotify.saved_tracks
ORDER BY added_at DESC
LIMIT 50;
```

Top artists over the medium-term affinity window:

```sql
SELECT time_range, name, image_url
FROM spotify.top_artists
WHERE time_range = 'medium_term'
LIMIT 20;
```



Next queued song or episode:

```sql
SELECT name, artist_names, album__name, external_url
FROM spotify.queue
LIMIT 1;
```

Recently played tracks:

```sql
SELECT played_at, track__name, artist_names, context__uri
FROM spotify.recently_played
LIMIT 50;
```

## Validation

Lint the manifest:

```sh
coral source lint sources/community/spotify/manifest.yaml
```

Install and test with real Spotify credentials:

```sh
export SPOTIFY_CLIENT_ID="<your-spotify-client-id>"
coral source add --file sources/community/spotify/manifest.yaml --interactive
coral source test spotify
```

Sanitized live validation from this source while authoring:

```text
$ coral source add --file sources/community/spotify/manifest.yaml --interactive
Source `spotify` installed successfully.

$ coral source test spotify
✓ spotify connected successfully
spotify (23 tables)
2 declared · 2 passed · 0 failed

$ coral sql --format json "SELECT id, display_name, country, product FROM spotify.profile LIMIT 1"
[{"id":"<redacted>","display_name":"<redacted>","country":"CH","product":"premium"}]

$ coral sql --format json "SELECT id, name, tracks_total FROM spotify.playlists LIMIT 3"
[
  {"id":"<redacted>","name":"<redacted>","tracks_total":0},
  {"id":"<redacted>","name":"<redacted>","tracks_total":4},
  {"id":"<redacted>","name":"<redacted>","tracks_total":29}
]

$ coral sql --format json "SELECT id, name, external_url FROM spotify.search_tracks(query => 'Lee Morgan Sidewinder') LIMIT 20"
returned 10 rows, matching Spotify's Search API per-call limit.

$ coral sql --format json "SELECT name, type, is_active FROM spotify.devices LIMIT 5"
returned visible Spotify Connect devices.

$ coral sql --format json "SELECT name, artist_names, album__name FROM spotify.queue LIMIT 1"
[]
```

Inspect the registered source:

```sh
coral sql "SELECT table_name, description, required_filters FROM coral.tables WHERE schema_name = 'spotify' ORDER BY table_name"
coral sql "SELECT table_name, column_name, is_required_filter FROM coral.columns WHERE schema_name = 'spotify' ORDER BY table_name, ordinal_position"
coral sql "SELECT key, kind, required, is_set FROM coral.inputs WHERE schema_name = 'spotify' ORDER BY key"
```

## Notes

- Spotify paginated collection endpoints use `limit` and `offset`; search
  functions are capped at 10 results per call to match Spotify's Search API.
- `spotify.recently_played` returns up to 50 items and supports Spotify's
  mutually exclusive `after`/`before` millisecond timestamp filters rather than
  offset pagination.
- Spotify's current-player endpoints can return HTTP 204 No Content when
  nothing is playing. Until Coral source specs can model 204-empty responses,
  this source exposes stable player-adjacent reads such as `devices` and
  `queue`, but not current playback state rows.
- `spotify.playlist_items` requests `additional_types=episode` so playlist
  episode items are included alongside Spotify's default track items; common
  item fields are flattened and the raw item is preserved in `raw_track`.
- `spotify.playlists` can include owned and followed playlists, but
  `spotify.playlist_items` may be limited by Spotify to playlists the user owns
  or collaborates on.

## Known limitations

- Spotify applies API-wide rate limits. If Spotify returns HTTP 429, wait for
  the `Retry-After` value before retrying and keep broad catalog scans bounded.
- Market availability, relinking, and nullable media fields can vary by user
  account, region, and catalog item.
- Spotify artwork URLs, preview/external URLs, and metadata remain subject to
  Spotify platform policy, including attribution requirements and restrictions
  on downloading, copying, or using Spotify content for AI training.

## Search functions

Search Spotify catalog entities with provider-ranked functions. Spotify search
functions return at most 10 rows per call:

```sql
SELECT id, name, external_url
FROM spotify.search_tracks(query => 'Lee Morgan Sidewinder')
LIMIT 10;
```

Available search functions: `search_tracks`, `search_albums`,
`search_artists`, `search_playlists`, `search_shows`, `search_episodes`, and
`search_audiobooks`.
