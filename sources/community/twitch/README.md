# Twitch Coral source

Query Twitch Helix data with Coral for creator dashboards and live-stream
analytics.

## Setup

Create a Twitch app and provide a client ID plus app/user access token:

```bash
TWITCH_CLIENT_ID=... \
TWITCH_ACCESS_TOKEN=... \
coral source add --file sources/community/twitch/manifest.yaml
```

Run validation:

```bash
coral source test twitch
```

## Tables and functions

| Name | Description |
| --- | --- |
| `twitch.users` | Twitch users; no filters returns the authenticated user. |
| `twitch.channels` | Channel metadata for required broadcaster IDs. |
| `twitch.streams` | Live streams. |
| `twitch.games` | Games/categories. |
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
SELECT display_name, game_name, title, is_live
FROM twitch.search_channels(query => 'software engineering')
LIMIT 20;
```

```sql
SELECT title, broadcaster_name, game_id, view_count, created_at
FROM twitch.clips
WHERE broadcaster_id = '123456'
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

## API references

- https://dev.twitch.tv/docs/api/reference/
- https://dev.twitch.tv/docs/api/clips/
- https://dev.twitch.tv/docs/api/videos/
