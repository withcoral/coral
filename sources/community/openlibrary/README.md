# Open Library API

Search books, authors, and subjects from the free [Open Library API](https://openlibrary.org/developers/api).

## Rate Limits & Usage

Open Library APIs require no authentication, but users are expected to cache responses and avoid bulk harvesting. 

Per the [Usage Guidelines](https://openlibrary.org/developers/api):
- The **default rate limit is 1 request per second** for non-identified requests.
- **Identified requests enjoy a 3x request limit**. To identify your application, include a `User-Agent` header that specifies the name of your application and a contact email or phone number.

If you need bulk access, please use their [Data Dumps](https://openlibrary.org/developers/dumps) instead of the API.

## Setup

Add the source directly:

```bash
coral source add --file sources/community/openlibrary/manifest.yaml
```

## Functions

### `search(q)`
Search for books by title, author, or keyword. Requires the `q` argument.

**Example:**
```sql
SELECT title, first_publish_year, author_name
FROM openlibrary.search(q => 'lord of the rings')
LIMIT 10;
```

## Tables

### `subjects`
Fetch books belonging to a specific subject or genre. Requires the `subject` filter.

**Example:**
```sql
SELECT title, first_publish_year, authors
FROM openlibrary.subjects
WHERE subject = 'love'
LIMIT 10;
```
