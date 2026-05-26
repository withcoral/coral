# DEV.to (Forem)

Query articles, users, and tags from [DEV.to](https://dev.to) and other Forem communities. This source uses the public Forem API and requires absolutely **no authentication**.

## Available Tables

### `articles`

Fetch a list of published articles.

```sql
-- Find the most popular Rust articles
SELECT title, url, positive_reactions_count 
FROM devto.articles 
WHERE tag = 'rust' 
ORDER BY positive_reactions_count DESC 
LIMIT 5;

-- Find fresh articles from a specific user
SELECT title, url 
FROM devto.articles 
WHERE username = 'ben' AND state = 'fresh';
```

**Supported Filters:**
- `tag`: Filter articles by a specific tag (e.g. `javascript`, `python`).
- `username`: Filter articles published by a specific user.
- `state`: Filter by state, such as `fresh` or `rising`.
- `top`: Number of days to filter top articles (e.g. `7` for top articles of the week).

### `users`

Lookup a specific user profile. You must provide either an `id` or a `username`.

```sql
-- Lookup a user by username
SELECT name, summary, location, website_url 
FROM devto.users 
WHERE username = 'ben';

-- Lookup a user by ID
SELECT username, name, joined_at 
FROM devto.users 
WHERE id = 1;
```

**Supported Filters:**
- `id`: The numerical ID of the user.
- `username`: The DEV.to handle (e.g. `ben`).

### `tags`

List active tags used on DEV.to.

```sql
SELECT name, short_summary 
FROM devto.tags 
LIMIT 10;
```
