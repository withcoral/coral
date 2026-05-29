# Bluesky Coral source

Query public Bluesky / AT Protocol data with Coral. This source is designed for
creator dashboards, social listening, community discovery, and public post
search.

## Setup

No credentials are required for the public endpoints in this source. The
default AppView base URL is `https://api.bsky.app/xrpc` and supports the full
source test suite, including `app.bsky.feed.searchPosts`:

```bash
coral source add --file sources/community/bluesky/manifest.yaml
```

Run validation:

```bash
coral source test bluesky
```

Live validation against the default no-auth host on 2026-05-29:

```text
$ coral source add --file sources/community/bluesky/manifest.yaml
Added source bluesky (secrets: none)

  ✓ bluesky connected successfully
  Secrets: none
    Query tests
    10 declared · 10 passed · 0 failed
```

```text
$ coral source test bluesky
  ✓ bluesky connected successfully
  Secrets: none
    Query tests
    10 declared · 10 passed · 0 failed

    ✓ bluesky.search_posts(...)  1 row
    ✓ bluesky.search_actors(...) 1 row
    ✓ bluesky.author_feed(...)   1 row
    ✓ bluesky.list_feed(...)     1 row
    ✓ bluesky.feed(...)          1 row
    ✓ bluesky.quotes(...)        1 row
    ✓ bluesky.post_thread(...)   1 row
    ✓ bluesky.profile(...)       1 row
    ✓ bluesky.followers(...)     1 row
    ✓ bluesky.follows(...)       1 row
```

## Functions

| Function | Description |
| --- | --- |
| `bluesky.search_posts(...)` | Provider-ranked public post search with language, author, mention, domain, URL, tag, and time filters. |
| `bluesky.search_actors(...)` | Provider-ranked public actor/profile search. |
| `bluesky.author_feed(...)` | Public feed for an actor, including replies, repost reasons, pins, and engagement counts. |
| `bluesky.list_feed(...)` | Public feed for a Bluesky list URI. |
| `bluesky.feed(...)` | Public feed for a Bluesky feed generator URI. |
| `bluesky.quotes(...)` | Posts that quote a given post URI. |
| `bluesky.post_thread(...)` | Root post plus raw parent/reply tree for a thread. |
| `bluesky.profile(...)` | Public profile for an actor. |
| `bluesky.followers(...)` | Public followers for an actor. |
| `bluesky.follows(...)` | Public accounts followed by an actor. |

## Example queries

```sql
SELECT uri, author__handle, text, indexed_at, like_count
FROM bluesky.search_posts(q => 'bluesky')
LIMIT 20;
```

```sql
SELECT uri, author__handle, text, indexed_at, like_count, quote_count
FROM bluesky.search_posts(
  q => 'coral',
  sort => 'latest',
  lang => 'en',
  since => '2026-05-01'
)
LIMIT 20;
```

```sql
SELECT did, handle, display_name, description
FROM bluesky.search_actors(q => 'coral')
LIMIT 20;
```

```sql
SELECT uri, text, indexed_at, like_count
FROM bluesky.author_feed(actor => 'bsky.app')
ORDER BY indexed_at DESC
LIMIT 20;
```

```sql
SELECT uri, author__handle, text, like_count
FROM bluesky.feed(
  feed => 'at://did:plc:z72i7hdynmk6r22z27h6tvur/app.bsky.feed.generator/whats-hot'
)
LIMIT 20;
```

```sql
SELECT did, handle, display_name
FROM bluesky.followers(actor => 'bsky.app')
LIMIT 50;
```

```sql
SELECT uri, text, author__handle, reply_count, like_count
FROM bluesky.post_thread(
  uri => 'at://did:plc:z72i7hdynmk6r22z27h6tvur/app.bsky.feed.post/3l6oveex3ii2l',
  depth => 3,
  parent_height => 2
);
```

## Notes

- This source uses public unauthenticated AT Protocol endpoints.
- `search_posts` is modeled as a `kind: search` table function because the
  provider ranks results.
- The `search_posts` `tag` argument sends one tag value. Bluesky's API accepts
  repeated tag parameters, but this source intentionally exposes the common
  single-tag case.
- Complex AT Protocol shapes such as labels, embeds, profile associations,
  thread replies, and raw feed views are exposed as `Json` columns so SQL can
  extract additional fields with Coral's JSON helpers.
- `https://public.api.bsky.app/xrpc` is not recommended here because
  `app.bsky.feed.searchPosts` can return 403 there; keep the default
  `https://api.bsky.app/xrpc` unless you have verified all needed functions
  against another AppView host.

## API references

- https://docs.bsky.app/docs/advanced-guides/api-directory
- https://docs.bsky.app/docs/api/app-bsky-feed-search-posts
- https://docs.bsky.app/docs/api/app-bsky-actor-search-actors
- https://docs.bsky.app/docs/api/app-bsky-feed-get-author-feed
- https://docs.bsky.app/docs/api/app-bsky-feed-get-list-feed
- https://docs.bsky.app/docs/api/app-bsky-feed-get-feed
- https://docs.bsky.app/docs/api/app-bsky-feed-get-quotes
- https://docs.bsky.app/docs/api/app-bsky-feed-get-post-thread
- https://docs.bsky.app/docs/api/app-bsky-actor-get-profile
- https://docs.bsky.app/docs/api/app-bsky-graph-get-followers
- https://docs.bsky.app/docs/api/app-bsky-graph-get-follows
