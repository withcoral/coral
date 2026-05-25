# Open Library API

Search books, authors, and subjects from the free [Open Library API](https://openlibrary.org/developers/api).

## Setup

No API key or authentication is needed. Add the source directly:

```bash
coral source add --file sources/community/openlibrary/manifest.yaml
```

## Tables

### `search`
Search for books by title, author, or keyword. Requires the `q` filter.

**Example:**
```sql
SELECT title, first_publish_year, author_name
FROM openlibrary.search
WHERE q = 'lord of the rings';
```

### `subjects`
Fetch books belonging to a specific subject or genre. Requires the `subject` filter.

**Example:**
```sql
SELECT title, first_publish_year, authors
FROM openlibrary.subjects
WHERE subject = 'love';
```
