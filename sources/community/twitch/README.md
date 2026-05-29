# Twitch

Query Twitch Helix user profiles, followed channels, followed live streams,
and broadcaster clips from Coral.

## Setup

Create a Twitch Developer application at <https://dev.twitch.tv/console> and
set its OAuth redirect URL to:

```text
http://localhost:3000
```

The app provides a Client ID and Client Secret. The Client ID is a public app
identifier and is stored as a Coral variable so Helix requests can send the
required `Client-Id` header. The Client Secret is prompted only during OAuth
setup and is stored as private credential metadata for token exchange and
refresh.

```bash
export TWITCH_CLIENT_ID="<your-twitch-client-id>"
coral source add --interactive --file sources/community/twitch/manifest.yaml
```

Choose **Connect with Twitch** and enter the Client Secret when prompted for
the OAuth credential flow. Coral stores the resulting OAuth user access token
locally and refreshes it when Twitch returns refresh metadata.

If you already have a Twitch OAuth user access token with the
`user:read:follows` scope, you can choose **Paste access token** instead:

```bash
export TWITCH_CLIENT_ID="<your-twitch-client-id>"
export TWITCH_ACCESS_TOKEN="<your-oauth-user-access-token>"
coral source add --file sources/community/twitch/manifest.yaml
```

## Tables

| Table | Description |
| --- | --- |
| `users` | Twitch user profile data. Without filters, returns the authenticated user. |
| `followed_channels` | Channels followed by the authenticated user. Requires `follower_user_id`. |
| `followed_streams` | Live streams from followed channels. Requires `follower_user_id`. |
| `clips` | Clips for a specific broadcaster. Requires `broadcaster_id`. |

## Example queries

Discover the authenticated Twitch user:

```sql
SELECT id, login, display_name, broadcaster_type
FROM twitch.users;
```

List followed channels:

```sql
SELECT broadcaster_id, broadcaster_name, followed_at
FROM twitch.followed_channels
WHERE follower_user_id = '<your-user-id>'
ORDER BY followed_at DESC;
```

Find followed channels that are live:

```sql
SELECT user_name, game_name, title, viewer_count, started_at
FROM twitch.followed_streams
WHERE follower_user_id = '<your-user-id>'
ORDER BY viewer_count DESC;
```

Find top clips for a broadcaster:

```sql
SELECT title, view_count, creator_name, duration, url
FROM twitch.clips
WHERE broadcaster_id = '<broadcaster-id>'
ORDER BY view_count DESC
LIMIT 20;
```

## Notes

- Twitch requires both `Authorization: Bearer <token>` and `Client-Id:
  <client-id>` on Helix API requests. The source sends `Authorization` through
  `auth.headers` and `Client-Id` through top-level `request_headers` because
  the Client ID is a non-secret variable.
- `followed_channels` and `followed_streams` require `follower_user_id` because
  Twitch requires the `user_id` query parameter to match the OAuth token's user.
  Get that ID from `twitch.users` first.
- `clips` models Twitch's broadcaster-scoped Get Clips path. Twitch also
  supports mutually exclusive `game_id` and `id` clip lookups, which can be
  added later if Coral maintainers want those as separate query surfaces.
