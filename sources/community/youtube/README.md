# YouTube Community Source

Query YouTube channels, videos, playlist items, search results, and comments
through Coral SQL using the YouTube Data API v3.

## Authentication

This source authenticates with a **YouTube Data API v3 API key** — *not* OAuth
2.0. An API key grants access only to **public** YouTube data: channels (by
`id` or `forHandle`), public videos, playlists, search, and public comments. Owner-scoped parameters such as `mine=true` or
`managedByMe=true` require OAuth 2.0 per the
[YouTube auth guide](https://developers.google.com/youtube/v3/guides/authentication)
and aren't exposed by this source. See [Limitations](#limitations) for fields
that can still be hidden on public content.

Every call counts against your Google Cloud project's daily quota. YouTube
splits this into two independent buckets: a **general Data API quota**
(default 10,000 units/day) used by `channels`, `videos`, `playlist_items`,
`comment_threads`, and `comments`; and a separate **Search Queries** bucket
(default 100 `search.list` calls/day) used only by `youtube.search`. See
[Quota](#quota) for the per-table breakdown. The source follows YouTube's
`pageToken` cursors automatically, so bound queries with SQL `LIMIT` to cap
row volume and quota use.

## Setup

### 1. Create a Google Cloud project and enable the API

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project (or select an existing one)
3. Navigate to **APIs & Services → Library**
4. Search for **YouTube Data API v3** and click **Enable**

### 2. Create an API key

1. Go to **APIs & Services → Credentials**
2. Click **Create Credentials → API key**
3. Copy the key
4. (Recommended) Click **Restrict key**, set the API restriction to
   **YouTube Data API v3**, and restrict by HTTP referrer or IP if applicable

### 3. Add the source

```bash
export YOUTUBE_API_KEY="AIzaSy..."
coral source add --file sources/community/youtube/manifest.yaml
```

### 4. Verify

```bash
coral source test youtube
```

The built-in test query reads `youtube.channels_by_handle` for `@DynamoGaming`
and verifies that authentication and column mapping are working.

## Quick start

Once the source is added, this query works against any public channel — try
[Marques Brownlee](https://www.youtube.com/@mkbhd) (1 quota unit):

```sql
SELECT snippet__title,
       statistics__subscriber_count,
       statistics__video_count,
       content_details__related_playlists__uploads
FROM youtube.channels_by_handle
WHERE handle = '@mkbhd';
```

The `uploads` playlist ID returned here can be passed straight to
`youtube.playlist_items` to list the channel's recent videos. See
[Common workflows](#common-workflows) for the rest of the chain.

## Quota

The YouTube Data API v3 splits requests across two independent quota buckets:

- **General Data API quota** — default **10,000 units/day** per project.
  Every table here except `search` draws from this bucket at **1 unit per
  call**.
- **Search Queries bucket** — default **100 `search.list` calls/day** per
  project, each costing 1 unit in that bucket. `youtube.search` is the only
  table that draws from this bucket; it does **not** consume general-quota
  units.

| Table | Quota bucket | Cost per call |
|---|---|---|
| `channels` | General (10,000/day) | 1 unit |
| `channels_by_handle` | General | 1 unit |
| `playlist_items` | General | 1 unit |
| `videos` | General | 1 unit |
| `comment_threads` | General | 1 unit |
| `comments` | General | 1 unit |
| **`search`** | **Search Queries (100/day)** | **1 unit (separate bucket)** |

Because the Search Queries bucket caps you at 100 `search.list` calls per day
out of the box, `youtube.search` defaults to 50 rows (`fetch_limit_default:
50`) to keep one query to one call. Request a quota increase in Google
Cloud Console if you need more search capacity.

## Common workflows

Most YouTube analysis chains a few tables together. The typical paths:

- **Browse a channel's videos** — start with `youtube.channels_by_handle`
  (by `@handle`) or `youtube.channels` (by ID). Read
  `content_details__related_playlists__uploads`, query `youtube.playlist_items`
  with that playlist ID, then enrich the resulting video IDs via
  `youtube.videos` for full metadata + statistics. All four steps cost 1 unit
  per call.
- **Keyword search → video detail** — query `youtube.search` for a keyword
  (set `type='video'` if you only want videos), collect `id__video_id`s, then
  enrich them via `youtube.videos`. Each `search` page counts against the
  separate **Search Queries** bucket (default 100 calls/day); prefer
  `playlist_items` when you only need one channel's own uploads.
- **Comment analysis** — for a video ID, `youtube.comment_threads` returns
  top-level threads with their first comment inline. For full reply trees,
  follow up with `youtube.comments` per thread.
- **Statistics** — counts (`statistics__view_count`,
  `statistics__subscriber_count`, etc.) appear on `channels`,
  `channels_by_handle`, and `videos`. YouTube returns them as JSON strings;
  cast to `BIGINT` for numeric comparisons.

## Pagination

YouTube paginates list responses with opaque `pageToken` cursors
(`nextPageToken` / `prevPageToken`), and each method has its own per-page
`maxResults` cap (e.g. up to 50 for `channels.list`). Coral follows the
cursors automatically, so use SQL `LIMIT` to cap how many pages — and
therefore how many quota units — each query consumes. `youtube.search`
defaults to 50 rows for that reason.

## Tables

### `youtube.channels`

Channel metadata and statistics, looked up by channel ID.

**Required filter:** `channel_id` — UC-prefixed channel ID, or
comma-separated list of up to 50.

**Note:** Statistics (`statistics__view_count`, `statistics__subscriber_count`,
`statistics__video_count`) are returned as strings by the YouTube API.

### `youtube.channels_by_handle`

Channel metadata and statistics, looked up by @-handle. Returns exactly
one row — zero if the handle does not exist.

**Required filter:** `handle` — the channel's handle, with or without a
leading `@`. YouTube's `forHandle` parameter accepts both forms — e.g.
`@DynamoGaming` and `DynamoGaming` are equivalent.

### `youtube.playlist_items`

Videos in a YouTube playlist, in order.

**Required filter:** `playlist_id`

A channel's uploads playlist ID is available directly as the
`content_details__related_playlists__uploads` column in `youtube.channels`.

### `youtube.videos`

Full video metadata including content details, statistics, and status.

**Required filter:** `video_id` — single ID or comma-separated list of up to 50

Use `youtube.playlist_items` or `youtube.search` to discover video IDs,
then enrich them here.

**Note:** `dislikeCount` has been private since December 2021.
`content_details__duration` is ISO 8601 format (e.g. `PT3M34S`).
Statistics are returned as strings.

### `youtube.search`

Search YouTube for videos, channels, or playlists by keyword.

**Required filter:** `q` — the search keyword (e.g. `'machine learning'`)

**Optional filters:** `channel_id`, `type`, `order`, `published_after`,
`published_before`

**WARNING:** `search.list` uses YouTube's separate **Search Queries** quota
bucket (default **100 calls/day**), distinct from the 10,000-unit general
Data API quota. Each call costs 1 unit in that bucket; the table defaults to
50 results to keep one query to one call. To browse a channel's videos
against the general quota (1 unit/call), use `youtube.playlist_items` instead.

### `youtube.comment_threads`

Top-level comment threads on a video.

**Required filter:** `video_id`

Each row is one thread. `snippet__total_reply_count` shows how many replies
exist. The `replies` part is not fetched — use `youtube.comments WHERE
parent_id = <top_comment__id>` to retrieve all replies for a thread.

> The `id` column on this table is the **thread ID**, not the top-level
> comment ID. Don't pass it as `parent_id` to `youtube.comments`; pass
> `top_comment__id` (which is `snippet.topLevelComment.id`).

### `youtube.comments`

All replies to a specific top-level comment.

**Required filter:** `parent_id` — the **top-level comment ID** from
`youtube.comment_threads.top_comment__id` (NOT the thread `id` column;
YouTube's `comments.list` `parentId` expects a comment ID, not a thread ID).

## Example Queries

```sql
-- Look up a channel by @-handle (uses channels_by_handle)
SELECT id, snippet__title, statistics__subscriber_count,
       statistics__video_count, content_details__related_playlists__uploads
FROM youtube.channels_by_handle
WHERE handle = '@DynamoGaming';

-- Look up a channel by ID, or multiple channels at once (uses channels)
SELECT id, snippet__title, snippet__country, statistics__subscriber_count
FROM youtube.channels
WHERE channel_id = 'UCVTqNpUKJA_Ef_sJVWg7NdQ';

-- List recent uploads from a channel
-- Step 1: get the uploads playlist ID from channels
-- Step 2: use it in playlist_items
SELECT snippet__title, snippet__resource_id__video_id,
       content_details__video_published_at
FROM youtube.playlist_items
WHERE playlist_id = 'UUqNH56x9g4QYVpzmWTzqVYg'
ORDER BY snippet__position
LIMIT 20;

-- Full video metadata for specific IDs
SELECT id, snippet__title, snippet__channel_title,
       statistics__view_count, statistics__like_count,
       content_details__duration, status__privacy_status
FROM youtube.videos
WHERE video_id = 'dQw4w9WgXcQ,jNQXAC9IVRw';

-- Search for videos (1 call against the Search Queries bucket; 100/day default)
SELECT id__video_id, snippet__title, snippet__channel_title,
       snippet__published_at
FROM youtube.search
WHERE q = 'BGMI tips' AND type = 'video'
LIMIT 10;

-- Top comment threads on a video
SELECT id, top_comment__text, top_comment__author_name,
       top_comment__like_count, snippet__total_reply_count
FROM youtube.comment_threads
WHERE video_id = 'dQw4w9WgXcQ'
ORDER BY top_comment__like_count DESC
LIMIT 10;

-- All replies under a specific top-level comment.
-- parent_id is top_comment__id (snippet.topLevelComment.id) from
-- youtube.comment_threads — NOT the thread `id` column.
SELECT snippet__author_display_name, snippet__text_display,
       snippet__like_count, snippet__published_at
FROM youtube.comments
WHERE parent_id = 'Ugzge340dBgB75hWBm54AaABAg'
LIMIT 20;
```

## Limitations

- **Read-only.** This source does not create, update, or delete YouTube
  resources.
- **Public data only.** The API key only accesses public content. Owner-scoped
  parameters (`mine=true`, `managedByMe=true`), watch history, and the
  authenticated user's likes/subscriptions require OAuth 2.0 per the
  [YouTube auth guide](https://developers.google.com/youtube/v3/guides/authentication)
  and are not supported in v1.
  - **Private videos** require the owner's OAuth credentials — not accessible
    with an API key at all.
  - **Unlisted videos** *are* viewable by anyone with a direct link, so
    `youtube.videos WHERE video_id = '<id>'` works with an API key once you
    know the ID. What an API key cannot do is **discover** them: unlisted
    videos don't appear in `youtube.search` results, channel uploads via
    `youtube.playlist_items`, or other public listings.
- **Fields hidden on otherwise-public content.** Some columns can come back
  empty or zero even with a valid API key:
  - `statistics.dislikeCount` is not exposed at all — YouTube made it
    private in December 2021, so no API-key call returns it.
  - `statistics__subscriber_count` is hidden (set to `0`) when a channel owner
    has opted to hide their subscriber count.
  - When comments are disabled on a video, `commentThreads.list` returns a
    `403 commentsDisabled` error from YouTube; this source surfaces that as
    a provider error rather than silently coercing it to an empty result.
  - Region-restricted videos return reduced metadata in regions where they're
    blocked.
- **Statistics as strings.** YouTube returns `viewCount`, `likeCount`,
  `subscriberCount`, and similar counts as JSON strings, not numbers. Use
  `CAST(statistics__view_count AS BIGINT)` for numeric comparisons.
- **`search` quota bucket.** `youtube.search` draws from a separate
  **Search Queries** quota bucket (default **100 `search.list` calls/day**),
  not the general 10,000-unit Data API quota. Each call costs 1 unit in that
  bucket; the 50-row default keeps one query to one call.
- **`comment_threads` has no reply content.** Each row contains the top-level
  comment and a reply count (`snippet__total_reply_count`), but reply text is
  not fetched. Use `youtube.comments WHERE parent_id = '<top_comment__id>'`
  (the `top_comment__id` value from the thread row, not the thread `id`
  column) to retrieve all replies.
- **`search` → `videos` join.** `youtube.videos` requires a constant
  `video_id` filter, so a dynamic join from `search` results is not
  supported. Use a two-step approach: run `search`, collect the video IDs,
  then query `videos`.
