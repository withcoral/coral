# Chuck Norris API

Query Chuck Norris facts from the free [chucknorris.io API](https://api.chucknorris.io/).

## Setup

No API key or authentication is needed. Add the source directly:

```bash
coral source add --file sources/community/chucknorris/manifest.yaml
```

## Rate Limits

No API key or authentication is needed, and there is no officially documented public rate limit. However, please be polite and avoid heavy bulk harvesting.

## Tables

### `jokes`
Fetch a random Chuck Norris joke.

**Example:**
```sql
SELECT id, value, created_at, icon_url
FROM chucknorris.jokes
LIMIT 1;
```

### `jokes_by_category`
Fetch a random Chuck Norris joke from a specific category.

**Example:**
```sql
SELECT id, value, category
FROM chucknorris.jokes_by_category
WHERE category = 'animal';
```

### `categories`
List all available joke categories.

**Example:**
```sql
SELECT category
FROM chucknorris.categories;
```

### `search`
Search for Chuck Norris jokes using a free-text query. Requires the `query` filter.

**Example:**
```sql
SELECT id, value
FROM chucknorris.search
WHERE query = 'rust';
```
