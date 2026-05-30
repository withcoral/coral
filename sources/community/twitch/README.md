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
| `twitch.games` | Exact games/categories lookup by one of `id`, `name`, or `igdb_id`. |
| `twitch.top_games` | Top games/categories by current activity. |
| `twitch.videos` | Published videos by ID, broadcaster, or game/category. |
| `twitch.clips` | Clips by ID, broadcaster, or game/category. |
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
WHERE broadcaster_id IN ('141981764');
```

```sql
SELECT display_name, game_name, title, is_live
FROM twitch.search_channels(query => 'software engineering')
LIMIT 20;
```

```sql
SELECT title, broadcaster_name, game_id, view_count, created_at
FROM twitch.clips
WHERE broadcaster_id = '123456'
  AND started_at = '2026-05-01T00:00:00Z'
  AND ended_at = '2026-05-08T00:00:00Z'
ORDER BY view_count DESC
LIMIT 50;
```

```sql
SELECT title, user_name, view_count, duration, published_at
FROM twitch.videos
WHERE user_id = '123456' AND type = 'archive'
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
- `twitch.games` requires exactly one selector: `id`, `name`, or `igdb_id`.
  Use `twitch.top_games` or `twitch.search_categories(...)` for discovery.
- `twitch.videos` requires exactly one of `id`, `user_id`, or `game_id`.
  Valid `period` values are `all`, `day`, `week`, and `month`; valid `sort`
  values are `time`, `trending`, and `views`; valid `type` values are `all`,
  `upload`, `archive`, and `highlight`.
- `twitch.clips` requires exactly one of `id`, `broadcaster_id`, or `game_id`.
  Narrow broadcaster/game clip queries with `started_at` and `ended_at` because
  Twitch caps clip pagination. Use SQL booleans for `is_featured`, for example
  `WHERE broadcaster_id = '123456' AND is_featured = true`.
- `twitch.streams`, `twitch.top_games`, `twitch.videos`, `twitch.clips`, and
  search functions use Twitch cursor pagination. Keep `LIMIT` values practical
  and expect provider rate-limit headers to apply to repeated scans.
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

Credentialed `coral source add --file`, `coral source test twitch`, and
representative live queries require Twitch app credentials and were not run in
this workspace.

## API references

- https://dev.twitch.tv/docs/api/reference/
- https://dev.twitch.tv/docs/api/clips/
- https://dev.twitch.tv/docs/api/videos/
