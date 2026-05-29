# youtube

Query YouTube liked videos, playlists, and channel stats using SQL.

Pulls data from the [YouTube Data API v3](https://developers.google.com/youtube/v3) via OAuth2. Works with any Google account — no channel required to query liked videos or playlists.

---

## Quick start

```bash
# Register the source (one-time setup)
YOUTUBE_ACCESS_TOKEN=<your-token> coral source add --file manifest.yaml

# Run a query
coral sql "SELECT title, channel_title FROM youtube.liked_videos LIMIT 10"
```

---

## Getting an access token

The YouTube source requires a Google OAuth2 access token with the `youtube.readonly` scope.

**Option A — Google OAuth Playground (quickest for testing)**

1. Go to [developers.google.com/oauthplayground](https://developers.google.com/oauthplayground)
2. Find **YouTube Data API v3** → select `https://www.googleapis.com/auth/youtube.readonly`
3. Click **Authorize APIs** and sign in with your Google account
4. Click **Exchange authorization code for tokens**
5. Copy the **Access token**

**Option B — Your own Google Cloud project (for applications)**

1. Create a project in [Google Cloud Console](https://console.cloud.google.com)
2. Enable the **YouTube Data API v3**
3. Create an **OAuth 2.0 Client ID** (Web application or Desktop app)
4. Use the OAuth flow to request the `youtube.readonly` scope
5. Pass the resulting access token as `YOUTUBE_ACCESS_TOKEN`

> If you already use Gmail or Google Calendar with Coral, this source uses the same Google Cloud project — just add the `youtube.readonly` scope to your existing OAuth credentials.

---

## Tables

### `youtube.liked_videos`

Videos liked by the authenticated user, pulled from the special liked videos playlist (ID: `LL`). Returns up to 50 most recently liked videos ordered by position (0 = most recently liked).

> **Note on watch history:** Google permanently restricted watch history API access in 2016. Liked videos is the best available proxy — they indicate content the user actively found useful.

| Column | Type | Description |
|---|---|---|
| `video_id` | text | Unique YouTube video ID |
| `title` | text | Video title |
| `channel_title` | text | Channel that published the video |
| `channel_id` | text | Channel's unique ID |
| `liked_at` | text | ISO 8601 timestamp when the video was liked |
| `position` | integer | Position in liked playlist (0 = most recent) |
| `description` | text | Video description |
| `thumbnail_url` | text | Default thumbnail URL (120×90px) |
| `playlist_item_id` | text | Unique ID for this playlist entry |

### `youtube.playlists`

Playlists created by the authenticated user.

| Column | Type | Description |
|---|---|---|
| `id` | text | Playlist ID |
| `title` | text | Playlist title |
| `description` | text | Playlist description |
| `published_at` | text | When the playlist was created (ISO 8601) |
| `item_count` | integer | Number of videos in the playlist |
| `privacy_status` | text | `public`, `unlisted`, or `private` |
| `thumbnail_url` | text | Default thumbnail URL |

### `youtube.channels`

The authenticated user's YouTube channel metadata and statistics.

| Column | Type | Description |
|---|---|---|
| `id` | text | Channel ID |
| `title` | text | Channel name |
| `description` | text | Channel description |
| `custom_url` | text | The @handle or custom URL |
| `published_at` | text | When the channel was created (ISO 8601) |
| `subscriber_count` | integer | Total subscribers |
| `video_count` | integer | Total uploaded videos |
| `view_count` | integer | Total lifetime views |
| `country` | text | Country associated with the channel |

---

## Example queries

**Top channels in your liked videos**
```sql
SELECT channel_title, COUNT(*) AS liked_count
FROM youtube.liked_videos
GROUP BY channel_title
ORDER BY liked_count DESC
LIMIT 10
```

**Most recently liked videos**
```sql
SELECT title, channel_title, liked_at
FROM youtube.liked_videos
ORDER BY position ASC
LIMIT 20
```

**Find tutorial / educational content you liked**
```sql
SELECT title, channel_title
FROM youtube.liked_videos
WHERE LOWER(title) LIKE '%tutorial%'
   OR LOWER(title) LIKE '%course%'
   OR LOWER(title) LIKE '%learn%'
```

**Your largest playlists**
```sql
SELECT title, item_count, privacy_status
FROM youtube.playlists
ORDER BY item_count DESC
```

**Channel stats**
```sql
SELECT title, subscriber_count, video_count, view_count
FROM youtube.channels
```

**Learning activity by month (liked videos)**
```sql
SELECT SUBSTR(liked_at, 1, 7) AS month, COUNT(*) AS videos_liked
FROM youtube.liked_videos
GROUP BY month
ORDER BY month DESC
```

---

## Auth scope

| Scope | Required for |
|---|---|
| `https://www.googleapis.com/auth/youtube.readonly` | All tables |

---

## Rate limits

YouTube Data API v3 uses a **quota unit** system rather than request-per-minute limits.

| Operation | Quota cost |
|---|---|
| Playlist items read | 1 unit |
| Playlists list | 1 unit |
| Channel stats | 1 unit |

The default daily quota is **10,000 units** per project. Typical usage of this source costs 3 units per full query run (one request per table). See [YouTube API quota docs](https://developers.google.com/youtube/v3/getting-started#quota) for details.

---

## Notes

- `liked_videos` returns at most 50 rows — the YouTube API caps `playlistItems` at `maxResults=50` per page. Pagination is not yet supported.
- `channels` returns data for the authenticated user's own channel. Returns empty rows if the account has no channel.
- Access tokens expire after 1 hour. For long-running workflows, use a refresh token to obtain a new access token before registering or querying.
- The `liked_at` field reflects when the video was added to the liked playlist, not when it was published.

---

## Provider docs

- [YouTube Data API v3 Overview](https://developers.google.com/youtube/v3/getting-started)
- [PlaylistItems resource](https://developers.google.com/youtube/v3/docs/playlistItems)
- [Playlists resource](https://developers.google.com/youtube/v3/docs/playlists)
- [Channels resource](https://developers.google.com/youtube/v3/docs/channels)
- [OAuth 2.0 for Google APIs](https://developers.google.com/identity/protocols/oauth2)
