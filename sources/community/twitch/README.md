# Twitch Coral source

Query Twitch Helix data with Coral for creator dashboards and live-stream
analytics.

## Setup

Create a Twitch application in the developer console, then provide the app's
Client ID and an OAuth access token issued for that same Client ID. For most
tables in this source, a client-credentials app access token is enough. Use a
user access token only when you intentionally query authenticated-user behavior,
such as `twitch.users` without an `id` or `login` selector. No additional
scopes are required for the public read endpoints modeled here.

```bash
TWITCH_CLIENT_ID=... \
TWITCH_ACCESS_TOKEN=... \
coral source add --file sources/community/twitch/manifest.yaml
```

Run validation:

```bash
coral source test twitch
```

First-success query after setup, using a public login instead of an internal
numeric ID:

```sql
SELECT id, login, display_name, broadcaster_type
FROM twitch.users
WHERE login = 'twitch';
```

## Tables and functions

| Name | Description |
| --- | --- |
| `twitch.users` | Twitch users; no filters returns the authenticated user. |
| `twitch.channels` | Channel metadata for required broadcaster IDs. |
| `twitch.streams` | Live streams. |
| `twitch.top_games` | Top games/categories by current activity. |
| `twitch.game_by_id(...)` | Exact game/category lookup by Twitch game ID. |
| `twitch.game_by_name(...)` | Exact game/category lookup by game name. |
| `twitch.game_by_igdb_id(...)` | Exact game/category lookup by IGDB ID. |
| `twitch.video_by_id(...)` | Exact video lookup by video ID. |
| `twitch.videos_by_user(...)` | Published videos for one broadcaster user ID. |
| `twitch.videos_by_game(...)` | Published videos for one game/category ID. |
| `twitch.clip_by_id(...)` | Exact clip lookup by clip ID. |
| `twitch.clips_by_broadcaster(...)` | Clips for one broadcaster ID. |
| `twitch.clips_by_game(...)` | Clips for one game/category ID. |
| `twitch.search_channels(...)` | Provider-ranked channel search. |
| `twitch.search_categories(...)` | Provider-ranked category search. |

## Example queries

```sql
SELECT user_name, game_name, viewer_count, title
FROM twitch.streams
ORDER BY viewer_count DESC
LIMIT 20;
```

```sql
SELECT broadcaster_login, broadcaster_name, game_name, title
FROM twitch.channels
WHERE broadcaster_id = '141981764';
```

```sql
SELECT display_name, game_name, title, is_live
FROM twitch.search_channels(query => 'software engineering')
LIMIT 20;
```

```sql
SELECT title, broadcaster_name, game_id, view_count, created_at
FROM twitch.clips_by_broadcaster(
  broadcaster_id => '123456',
  started_at => '2026-05-01T00:00:00Z',
  ended_at => '2026-05-08T00:00:00Z'
)
ORDER BY view_count DESC
LIMIT 50;
```

```sql
SELECT title, user_name, view_count, duration, published_at
FROM twitch.videos_by_user(user_id => '123456', type => 'archive')
ORDER BY published_at DESC
LIMIT 20;
```

```sql
SELECT id, name, box_art_url
FROM twitch.top_games
LIMIT 20;
```

## Notes

- The `Client-Id` header and bearer token must belong to the same Twitch
  application. Twitch rejects mismatched app credentials.
- Twitch selector-sensitive endpoints are modeled as table functions rather
  than scan tables. Use `twitch.game_by_id(...)`, `twitch.game_by_name(...)`,
  or `twitch.game_by_igdb_id(...)` for exact category lookup; use
  `twitch.top_games` or `twitch.search_categories(...)` for discovery.
- Use `twitch.video_by_id(...)` for exact video lookup. Use
  `twitch.videos_by_user(...)` with optional `language`, `period`, `sort`, and
  `type` arguments for broadcaster videos. Use `twitch.videos_by_game(...)`
  with optional `language` for game/category videos. Valid `period` values are
  `all`, `day`, `week`, and `month`; valid `sort` values are `time`,
  `trending`, and `views`; valid `type` values are `all`, `upload`, `archive`,
  and `highlight`.
- Use `twitch.clip_by_id(...)` for exact clip lookup. Use
  `twitch.clips_by_broadcaster(...)` or `twitch.clips_by_game(...)` for clip
  lists, with optional `started_at`, `ended_at`, and `is_featured` arguments.
  Narrow broadcaster/game clip queries with `started_at` and `ended_at` because
  Twitch caps clip pagination.
- `twitch.streams`, `twitch.top_games`, videos/clips list functions, and search
  functions use Twitch cursor pagination. Keep `LIMIT` values practical and
  expect provider rate-limit headers to apply to repeated scans.
- Deprecated Twitch fields `users.view_count` and `streams.is_mature` are not
  exposed because Twitch documents them as invalid or always false.

## Validation evidence

Static validation run locally:

```bash
coral source lint sources/community/twitch/manifest.yaml
make lint-sources
yamllint sources/community/twitch/manifest.yaml
git diff --check origin/main..HEAD
gitleaks detect --no-banner --redact --source . --log-opts=origin/main..HEAD
```

Credentialed live validation with a Twitch app access token:

```text
Manifest is valid
Added source twitch (secrets: file (plaintext))

  ✓ twitch connected successfully
  Secrets: file (plaintext)

    twitch (4 tables)
    ├─ channels
    ├─ streams
    ├─ top_games
    └─ users
    Query tests
    9 declared · 9 passed · 0 failed

    ✓ SELECT id, login, display_name FROM twitch.users WHERE login = 'twitch' LIMIT 1
      1 row
    ✓ SELECT id, name FROM twitch.game_by_name(name => 'Fortnite') LIMIT 1
      1 row
    ✓ SELECT id, title FROM twitch.videos_by_user(user_id => '141981764') LIMIT 1
      1 row
    ✓ SELECT id, title FROM twitch.clips_by_broadcaster(broadcaster_id => '141981764') LIMIT 1
      1 row
```

Representative live query output:

```text
| login  | display_name |
| twitch | Twitch       |

| broadcaster_login | game_name                     | title                                             |
| twitchdev         | Software and Game Development | Standard Output // October 16, 2024 at 2:30pm EDT |

| id    | name     |
| 33214 | Fortnite |

| id         | title                               |
| 2277656159 | Standard Output // October 16, 2024 |

| id                              | title |
| LivelySaltyAyeayeNerfRedBlaster | YIKES |

| id        | display_name |
| 141981764 | TwitchDev    |
```

## API references

- https://dev.twitch.tv/docs/api/reference/
- https://dev.twitch.tv/docs/api/clips/
- https://dev.twitch.tv/docs/api/videos/
