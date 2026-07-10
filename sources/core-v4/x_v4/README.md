# X DSL v4 preview source

This preview source queries posts, users, timelines, mentions, and engagement
from the X API v2 OpenAPI surface.

## Authentication

Set `X_ACCESS_TOKEN` to an X API bearer token or OAuth 2.0 user-context access
token with access to the endpoints you query.

```sh
X_ACCESS_TOKEN=... coral source add --file sources/core-v4/x_v4/manifest.yaml
```

## Recent search with author metadata

Recent search rows expose post fields such as `author_id`, `username`,
`created_at`, and `public_metrics`. Request the X field sets you need with the
comma-separated query arguments generated from `tweet.fields`, `expansions`,
and `user.fields`:

```sql
SELECT id, text, author_id, username, created_at, public_metrics
FROM x_v4.tweets_searchpostsrecent(
  query => 'from:XDevelopers',
  tweet_fields => 'author_id,created_at,conversation_id,public_metrics,referenced_tweets,in_reply_to_user_id',
  expansions => 'author_id',
  user_fields => 'username,name,public_metrics,verified,verified_type,description'
)
LIMIT 5;
```

For profile metrics or a reliable handle lookup, query users directly:

```sql
SELECT id, username, name, public_metrics
FROM x_v4.users_getusersbyusernames(
  usernames => 'XDevelopers',
  user_fields => 'username,name,public_metrics,verified,verified_type,description'
)
LIMIT 5;
```
