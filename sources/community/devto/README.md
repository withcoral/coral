# DEV.to (Forem)

**Version:** 0.1.0
**Backend:** HTTP
**Tables:** 4
**Base URL:** `https://dev.to/api`

Query articles, users, and tags from [DEV.to](https://dev.to) via the public [Forem API](https://developers.forem.com/api/v1). No authentication required.

```bash
coral source add --file sources/community/devto/manifest.yaml
```

## Tables

| Table | Description | Filters |
|---|---|---|
| `articles` | Published articles on DEV.to | `tag`, `username`, `state`, `top` |
| `users_by_id` | User profile lookup by ID | `id` (required) |
| `users_by_username` | User profile lookup by username | `username` (required) |
| `tags` | Active tags on the platform | — |

---

### `articles`

Fetch a list of published articles. Supports filtering by tag, author, state, and top-of-timeframe.

#### Filters

| Filter | Type | Description |
|---|---|---|
| `tag` | string | Filter articles by a specific tag (e.g. `rust`, `javascript`) |
| `username` | string | Filter articles by a specific author's username |
| `state` | string | Filter by state: `rising`, `fresh`, or `all`. Note: `state` can only be combined with the `username` filter when set to `all`. |
| `top` | integer | Return top articles over the past N days (e.g. `7` for the week) |

#### Columns

| Column | Type | Description |
|---|---|---|
| `id` | Int64 | Article ID |
| `title` | Utf8 | Article title |
| `description` | Utf8 | Short description |
| `slug` | Utf8 | URL slug |
| `url` | Utf8 | Full URL to the article on DEV.to |
| `canonical_url` | Utf8 | Original URL if crossposted |
| `comments_count` | Int64 | Number of comments |
| `public_reactions_count` | Int64 | Number of public reactions |
| `positive_reactions_count` | Int64 | Number of positive reactions |
| `reading_time_minutes` | Int64 | Estimated reading time in minutes |
| `tags` | Utf8 | Comma-separated list of tags |
| `published_at` | Timestamp | Publication timestamp |
| `created_at` | Timestamp | Creation timestamp |
| `last_comment_at` | Timestamp | Timestamp of the last comment |
| `edited_at` | Timestamp | Timestamp of last edit |
| `author_user_id` | Int64 | User ID of the author (from nested `user` object) |
| `author_username` | Utf8 | Username of the author (from nested `user` object) |
| `author_name` | Utf8 | Display name of the author (from nested `user` object) |
| `organization_name` | Utf8 | Organization name if posted under one |
| `cover_image` | Utf8 | URL of the article cover image |
| `social_image` | Utf8 | URL of the social share image |
| `path` | Utf8 | Relative path on DEV.to (e.g. `/username/slug`) |
| `published_timestamp` | Utf8 | ISO 8601 publication timestamp string |
| `language` | Utf8 | Language code (e.g. `en`) |
| `readable_publish_date` | Utf8 | Human-readable publish date (e.g. `May 26`) |
| `type_of` | Utf8 | Type of the resource (always `article`) |
| `tag` | Utf8 | Echoes the `tag` filter used |
| `username` | Utf8 | Echoes the `username` filter used |
| `state` | Utf8 | Echoes the `state` filter used |
| `top` | Int64 | Echoes the `top` filter used |

---

### `users_by_id`

Lookup a specific DEV.to user profile by their ID.

#### Filters

| Filter | Type | Required | Description |
|---|---|---|---|
| `id` | Int64 | Yes | The numerical user ID |

#### Columns

| Column | Type | Description |
|---|---|---|
| `id` | Int64 | User ID |
| `username` | Utf8 | Username |
| `name` | Utf8 | Display name |
| `summary` | Utf8 | User profile summary/bio |
| `location` | Utf8 | User location |
| `website_url` | Utf8 | User website URL |
| `joined_at` | Utf8 | Date the user joined DEV.to (e.g. `Dec 27, 2015`) |
| `twitter_username` | Utf8 | Twitter handle |
| `github_username` | Utf8 | GitHub handle |
| `profile_image` | Utf8 | URL of the user profile image |
| `email` | Utf8 | Public email address, if set |

---

### `users_by_username`

Lookup a specific DEV.to user profile by their username.

#### Filters

| Filter | Type | Required | Description |
|---|---|---|---|
| `username` | string | Yes | The DEV.to handle (e.g. `ben`) |

#### Columns

| Column | Type | Description |
|---|---|---|
| `id` | Int64 | User ID |
| `username` | Utf8 | Username |
| `name` | Utf8 | Display name |
| `summary` | Utf8 | User profile summary/bio |
| `location` | Utf8 | User location |
| `website_url` | Utf8 | User website URL |
| `joined_at` | Utf8 | Date the user joined DEV.to (e.g. `Dec 27, 2015`) |
| `twitter_username` | Utf8 | Twitter handle |
| `github_username` | Utf8 | GitHub handle |
| `profile_image` | Utf8 | URL of the user profile image |
| `email` | Utf8 | Public email address, if set |

---

### `tags`

List active tags used on DEV.to.

#### Columns

| Column | Type | Description |
|---|---|---|
| `id` | Int64 | Tag ID |
| `name` | Utf8 | Tag name |
| `bg_color_hex` | Utf8 | Hex color code for the tag background |
| `text_color_hex` | Utf8 | Hex color code for the tag text |
| `short_summary` | Utf8 | A short description of the tag |

---

## Quick start

```bash
# Confirm connectivity
coral sql "SELECT title, url FROM devto.articles WHERE tag = 'rust' LIMIT 1"

# Find the most popular articles of the week
coral sql "
  SELECT title, url, positive_reactions_count, reading_time_minutes
  FROM devto.articles
  WHERE tag = 'javascript' AND top = 7
  ORDER BY positive_reactions_count DESC
  LIMIT 10
"

# Find articles from a specific user
coral sql "
  SELECT title, url, published_at
  FROM devto.articles
  WHERE username = 'ben'
  LIMIT 5
"

# Lookup a user profile
coral sql "
  SELECT name, summary, location, github_username, profile_image
  FROM devto.users_by_username
  WHERE username = 'ben'
"

# Lookup a user by ID
coral sql "
  SELECT username, name, joined_at, email
  FROM devto.users_by_id
  WHERE id = 1
"

# Browse active tags
coral sql "
  SELECT name, short_summary, bg_color_hex
  FROM devto.tags
  LIMIT 10
"
```

## Notes

- The `users_by_id` and `users_by_username` tables function as **lookup tables** — you must provide the required `id` or `username` filter respectively. The Forem API does not support listing all users without admin authentication.
- **No SQL Joins on User Tables:** Standard SQL `JOIN` queries (e.g. joining `devto.articles` with `devto.users_by_id` on user IDs) are not supported because `users_by_id` and `users_by_username` are lookup-only tables requiring a constant filter (e.g. `WHERE id = <constant>`). Instead, you must use a two-step query workflow: query `articles` to retrieve the `author_user_id`, and then perform a separate lookup query on `users_by_id` with that ID.
- **DEV.to Specific Table:** The `users_by_username` table utilizes the DEV.to-specific `/users/by_username` API endpoint. This endpoint is unique to DEV.to and is not part of the standard/published Forem API v1 contract. Other Forem instances might not support this lookup table.
- `joined_at` in user tables returns a human-readable date string (e.g. `Dec 27, 2015`), not an ISO 8601 timestamp.
- `tags` in `articles` is a comma-separated string (e.g. `javascript, node, rust`). Filter by a single tag using the `tag` filter.
- No authentication or API key is required for any of the implemented endpoints.
- The Forem API enforces a rate limit of **30 requests per 30 seconds** for unauthenticated access. Keep this in mind when running large paginated queries.
