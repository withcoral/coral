# Blogger

**Version:** 0.2.0
**Backend:** HTTP
**Tables:** 6
**Base URL:** `https://www.googleapis.com/blogger/v3`

Query **public** blogs, posts, pages, and comments from Google Blogger via the Blogger v3 API. Access blog metadata, search and list posts, browse pages, and read comments. Includes a search function for keyword-based post discovery. Private or authorized-only blogs are not supported.

## Installation

Install the source via the CLI:

```bash
coral source add --file sources/community/blogger/manifest.yaml
```

## Credentials

To use this source, you need a Google API key with the Blogger API enabled. The API key provides read-only access to **public** blog content — private or authorized-only blogs are not accessible.

1. Go to the [Google Cloud Console](https://console.cloud.google.com/apis/credentials).
2. Create a new API key or select an existing one.
3. Enable the **Blogger API v3** for your project.
4. Provide the key when prompted by `coral source add` or set it as an environment variable:

```bash
export BLOGGER_API_KEY="your-google-api-key"
```

## Quick Start

```sql
-- Look up a blog by URL
SELECT id, name, posts_total_items, locale_language
FROM blogger.blog_by_url
WHERE url = 'https://googledevelopers.blogspot.com/';

-- Look up a blog by ID
SELECT id, name, description, posts_total_items
FROM blogger.blogs
WHERE blog_id = '596098824972435195';

-- List recent posts
SELECT id, title, published, author_display_name
FROM blogger.posts
WHERE blog_id = '596098824972435195'
LIMIT 10;

-- Search posts by keyword (function call syntax)
SELECT id, title, published, url
FROM blogger.posts_search(blog_id => '596098824972435195', query => 'AI')
LIMIT 5;

-- Get a specific post
SELECT title, content, author_display_name, labels
FROM blogger.post_by_id
WHERE blog_id = '596098824972435195'
  AND post_id = '4125177830831475108';

-- List pages on a blog
SELECT id, title, published
FROM blogger.pages
WHERE blog_id = '596098824972435195';

-- List comments on a post
SELECT id, author_display_name, content, published
FROM blogger.comments
WHERE blog_id = '2399953'
  AND post_id = '7531385161213212970'
LIMIT 10;
```

## Tables

### `blogs`

Get a blog by its Blogger blog ID. Returns a single row with metadata.

**Filters**

| Filter | Type | Required | Description |
|--------|------|----------|-------------|
| `blog_id` | Utf8 | Yes | Blogger blog ID |

**Columns**

| Column | Type | Description |
|--------|------|-------------|
| `id` | Utf8 | Blogger blog ID |
| `name` | Utf8 | Blog display name |
| `description` | Utf8 | Blog description |
| `published` | Timestamp | RFC 3339 publication date |
| `updated` | Timestamp | RFC 3339 last updated date |
| `url` | Utf8 | Blog URL |
| `posts_total_items` | Int64 | Total number of posts |
| `pages_total_items` | Int64 | Total number of pages |
| `locale_language` | Utf8 | Locale language code (e.g. `en`) |
| `locale_country` | Utf8 | Locale country code |

---

### `blog_by_url`

Get a blog by its public URL. Returns the same columns as `blogs`.

**Filters**

| Filter | Type | Required | Description |
|--------|------|----------|-------------|
| `url` | Utf8 | Yes | Blog public URL (e.g. `https://example.blogspot.com/`) |

**Columns**

Same as `blogs`.

---

### `posts`

List posts from a blog, ordered by publication date (newest first). Paginates automatically.

**Filters**

| Filter | Type | Required | Description |
|--------|------|----------|-------------|
| `blog_id` | Utf8 | Yes | Blog ID to list posts from |

**Columns**

| Column | Type | Description |
|--------|------|-------------|
| `id` | Utf8 | Post ID |
| `blog_id` | Utf8 | Blog ID this post belongs to |
| `published` | Timestamp | RFC 3339 publication date |
| `updated` | Timestamp | RFC 3339 last updated date |
| `title` | Utf8 | Post title |
| `content` | Utf8 | Post body as HTML |
| `url` | Utf8 | Post URL |
| `author_display_name` | Utf8 | Author display name |
| `author_id` | Utf8 | Author profile ID |
| `author_url` | Utf8 | Author profile URL |
| `author_image_url` | Utf8 | Author avatar image URL |
| `labels` | Json | Labels/tags as a JSON array of strings |
| `replies_total_items` | Int64 | Total number of comments on this post |
| `status` | Utf8 | Post status (`LIVE`, `DRAFT`, `SCHEDULED`) |

---

### `post_by_id`

Get a single post by blog ID and post ID.

**Filters**

| Filter | Type | Required | Description |
|--------|------|----------|-------------|
| `blog_id` | Utf8 | Yes | Blog ID |
| `post_id` | Utf8 | Yes | Post ID |

**Columns**

Same as `posts`.

---

### `pages`

List static pages from a blog, ordered by publication date (newest first). Many blogs have zero pages.

**Filters**

| Filter | Type | Required | Description |
|--------|------|----------|-------------|
| `blog_id` | Utf8 | Yes | Blog ID to list pages from |

**Columns**

| Column | Type | Description |
|--------|------|-------------|
| `id` | Utf8 | Page ID |
| `blog_id` | Utf8 | Blog ID this page belongs to |
| `published` | Timestamp | RFC 3339 publication date |
| `updated` | Timestamp | RFC 3339 last updated date |
| `title` | Utf8 | Page title |
| `content` | Utf8 | Page body as HTML |
| `url` | Utf8 | Page URL |
| `author_display_name` | Utf8 | Author display name |
| `author_id` | Utf8 | Author profile ID |
| `author_url` | Utf8 | Author profile URL |
| `author_image_url` | Utf8 | Author avatar image URL |
| `status` | Utf8 | Page status (`LIVE`, `DRAFT`) |

---

### `comments`

List comments on a post, ordered by publication date (newest first).

**Filters**

| Filter | Type | Required | Description |
|--------|------|----------|-------------|
| `blog_id` | Utf8 | Yes | Blog ID |
| `post_id` | Utf8 | Yes | Post ID to list comments from |

**Columns**

| Column | Type | Description |
|--------|------|-------------|
| `id` | Utf8 | Comment ID |
| `blog_id` | Utf8 | Blog ID |
| `post_id` | Utf8 | Post ID this comment belongs to |
| `published` | Timestamp | RFC 3339 publication date |
| `updated` | Timestamp | RFC 3339 last updated date |
| `content` | Utf8 | Comment body as HTML |
| `author_display_name` | Utf8 | Author display name |
| `author_id` | Utf8 | Author profile ID |
| `author_url` | Utf8 | Author profile URL |
| `author_image_url` | Utf8 | Author avatar image URL |
| `status` | Utf8 | Comment status (`LIVE`, `EMPTIED`, `PENDING`, `SPAM`) |

## Functions

### `posts_search`

Search posts within a blog by keyword. Returns posts ordered by relevance. Uses function call syntax: `FROM blogger.posts_search(blog_id => '...', query => '...')`.

**Arguments**

| Argument | Type | Required | Description |
|----------|------|----------|-------------|
| `blog_id` | Utf8 | Yes | Blog ID to search within |
| `query` | Utf8 | Yes | Search keyword or phrase |
**Columns**

Same as `posts` (table).

**Search defaults**

| Setting | Default |
|---------|---------|
| Default top-K | 10 |
| Max top-K | 100 |

---

## Live request costs

Each table query performs at least one live API call to `https://www.googleapis.com/blogger/v3`. Token-based pagination may trigger additional calls when `LIMIT` exceeds a single page's results. See the [Blogger API performance tips](https://developers.google.com/blogger/docs/3.0/performance) for guidance and the [Google Cloud Console](https://console.cloud.google.com/apis/api/blogger.googleapis.com/quotas) for current quota limits.

## Source scope

- Targets the Blogger v3 REST API at `https://www.googleapis.com/blogger/v3`.
- Requires `BLOGGER_API_KEY` authentication via the `X-Goog-Api-Key` header.
- **Public content only**: the API key provides access to public blogs, posts, pages, and comments. Private or authorized-only blogs are not supported.
- Covers read-only access: blogs, posts, pages, and comments.
- `posts_search` is a search function using function call syntax: `FROM blogger.posts_search(blog_id => '...', query => '...')`.
- Automatic pagination via the Blogger API's `pageToken` mechanism.
- The `content` column contains HTML — use string functions or your SQL client's HTML-to-text features for plain text extraction.
- The `labels` column is a JSON array of strings (e.g. `["AI", "Announcements"]`).

## Limitations

- The source provides read-only access. Blog creation, post writing, and comment moderation are intentionally out of scope.
- Pagination uses the Blogger API token-based cursor system; page size is controlled by the API's default (25 for posts/pages, 20 for comments). Pass `max_results` as an optional filter on `posts`, `pages`, and `comments` to override page size (sent as `maxResults` to the API). The `posts_search` function does not support `max_results`.
- `replies_total_items` is typed as Int64 per the [Blogger API resource schema](https://developers.google.com/blogger/docs/3.0/reference/posts#resource), though the wire format may return it as a string for some responses.
- The `blog_by_url` table requires the full public blog URL including protocol (e.g. `https://googledevelopers.blogspot.com/`).

## Provider docs

- Blogger API v3 reference: https://developers.google.com/blogger/docs/3.0/reference
- Google Cloud Console (API keys): https://console.cloud.google.com/apis/credentials
- Enable the Blogger API: https://console.cloud.google.com/apis/library/blogger.googleapis.com
- API usage limits: https://developers.google.com/blogger/docs/3.0/performance

## Live validation output

Validated against a live Google Cloud project with the Blogger API enabled and a valid `BLOGGER_API_KEY`.

```bash
$ ./target/debug/coral source lint sources/community/blogger/manifest.yaml
Manifest is valid
```

```bash
$ BLOGGER_API_KEY=... ./target/debug/coral source add --file sources/community/blogger/manifest.yaml
Added source blogger (secrets: ***)

  ✓ blogger connected successfully
  Secrets: ***
    blogger (6 tables)
    ├─ blog_by_url
    ├─ blogs
    ├─ comments
    ├─ pages
    ├─ post_by_id
    └─ posts
    Query tests
    3 declared · 3 passed · 0 failed

    ✓ SELECT id, name, posts_total_items, locale_language FROM blogger.blog_by_url WHERE url = 'https://googledevelopers.blogspot.com/'
      1 row

    ✓ SELECT id, title, published FROM blogger.posts WHERE blog_id = '596098824972435195' LIMIT 1
      1 row

    ✓ SELECT id, title, published FROM blogger.posts_search(blog_id => '596098824972435195', query => 'AI') LIMIT 3
      3 rows
```

**Table introspection:**

```sql
SELECT table_name, description, required_filters
FROM coral.tables
WHERE schema_name = 'blogger'
ORDER BY table_name;
```

```text
+-------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------------+
| table_name  | description                                                                                                                                                                           | required_filters       |
+-------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------------+
| blog_by_url | Get a blog by its public URL. Returns a single row with the same metadata columns as the blogs table. Use when you know the blog's URL but not its ID.                                | url                    |
| blogs       | Get a blog by its Blogger blog ID. Returns a single row with the blog's metadata including name, description, post and page counts, and locale.                                       | blog_id                |
| comments    | List comments on a post. Requires both blog_id and post_id filters. Returns comments ordered by publication date (newest first).                                                      | blog_id,post_id        |
| pages       | List pages from a blog. Requires a blog_id filter. Returns static pages ordered by publication date (newest first). Many blogs have zero pages.                                       | blog_id                |
| post_by_id  | Get a single post by blog ID and post ID. Returns one row with the full post content including author info, labels, and comment count.                                                | blog_id,post_id        |
| posts       | List posts from a blog. Requires a blog_id filter. Returns posts ordered by publication date (newest first). Paginates automatically through the Blogger API's token-based pagination. | blog_id                |
+-------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------------+
```

**Function introspection:**

```sql
SELECT function_name, kind, arguments_json, search_limits_json
FROM coral.table_functions
WHERE schema_name = 'blogger'
ORDER BY function_name;
```

```text
+---------------+--------+-----------------------------------------------------------------------------------------------+--------------------------------------------------------------+
| function_name | kind   | arguments_json                                                                                | search_limits_json                                           |
+---------------+--------+-----------------------------------------------------------------------------------------------+--------------------------------------------------------------+
| posts_search  | search | [{"name":"blog_id","required":true,"values":[]},{"name":"query","required":true,"values":[]}] | {"default_top_k":10,"max_top_k":100,"max_calls_per_query":1} |
+---------------+--------+-----------------------------------------------------------------------------------------------+--------------------------------------------------------------+
```

**Inputs introspection:**

```sql
SELECT key, kind, required, is_set
FROM coral.inputs
WHERE schema_name = 'blogger'
ORDER BY key;
```

```text
+-----------------+--------+----------+--------+
| key             | kind   | required | is_set |
+-----------------+--------+----------+--------+
| BLOGGER_API_KEY | secret | true     | true   |
+-----------------+--------+----------+--------+
```

**Live blog lookup proof:**

```sql
SELECT id, name, posts_total_items, locale_language
FROM blogger.blog_by_url
WHERE url = 'https://googledevelopers.blogspot.com/';
```

```text
+--------------------+-------------------------------------------------------------------+-------------------+-----------------+
| id                 | name                                                              | posts_total_items | locale_language |
+--------------------+-------------------------------------------------------------------+-------------------+-----------------+
| 596098824972435195 | Google for Developers Blog - News about Web, Mobile, AI and Cloud | 2443              | en              |
+--------------------+-------------------------------------------------------------------+-------------------+-----------------+
```

**Live search proof:**

```sql
SELECT id, title, published
FROM blogger.posts_search(blog_id => '596098824972435195', query => 'AI')
LIMIT 3;
```

```text
+---------------------+--------------------------------------------------------------------------------------------------------------------------+----------------------+
| id                  | title                                                                                                                    | published            |
+---------------------+--------------------------------------------------------------------------------------------------------------------------+----------------------+
| 4125177830831475108 | Introducing new AI tools on Google for Developers                                                                        | 2024-04-29T21:00:00Z |
| 3703054361452072781 | Achieving privacy compliance with your CI/CD: A guide for compliance teams                                               | 2024-04-10T16:00:00Z |
| 364483124263275268  | Gemini 1.5 Pro Now Available in 180+ Countries; With Native Audio Understanding, System Instructions, JSON Mode and More | 2024-04-09T16:00:00Z |
+---------------------+--------------------------------------------------------------------------------------------------------------------------+----------------------+
```
