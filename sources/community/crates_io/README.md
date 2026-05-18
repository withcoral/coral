# Crates.io — Coral community source

Query the [crates.io](https://crates.io) Rust package registry with SQL.

## Overview

This source exposes the public crates.io REST API as read-only SQL tables.
No authentication is required — all endpoints are fully public.

| Table | Description |
| ----- | ----------- |
| `crates_io.crates` | Search and list crates |
| `crates_io.crate_versions` | List versions of a specific crate |
| `crates_io.crate_dependencies` | Dependencies of a specific crate version |
| `crates_io.crate_owners` | User and team owners of a crate |
| `crates_io.categories` | All registry categories |
| `crates_io.keywords` | All registry keywords |

## Setup

No API token or authentication is needed. Add the source directly:

```bash
coral source add --file sources/community/crates_io/manifest.yaml
```

## Auth

None. The crates.io API is fully public for read operations. Coral
automatically sets a descriptive `User-Agent` header on every request, which
satisfies the crates.io crawler policy.

## Rate limiting

Crates.io enforces a rate limit of **1 request per second** for API
consumers. Coral handles `429 Too Many Requests` responses with
automatic retry, but paginated queries across many pages may be slower
than usual because of this constraint.

Paginated tables use conservative default fetch limits when a query omits
`LIMIT`. Add an explicit `LIMIT` when you need a larger result set, and keep
large scans narrow with filters such as `q`, `category`, `keyword`, or
`crate_name`.

## Pagination and filters

The `crates`, `categories`, and `keywords` tables use crates.io page
pagination with `page` and `per_page` request parameters.
The `crate_versions`, `crate_dependencies`, and `crate_owners` tables return
all rows in a single response (no pagination).

The `crates_io.crates` table supports `include_yanked = 'yes'` when you need
to include yanked crates in search/list results. The `categories` and
`keywords` columns are exposed as JSON when crates.io includes them in the
list response; for many list/search responses crates.io returns them as
`null`.

## Example queries

### Search for crates

```sql
SELECT name, description, downloads, recent_downloads
FROM crates_io.crates
WHERE q = 'serialization'
LIMIT 10;
```

### List crates in a category

```sql
SELECT name, description, downloads
FROM crates_io.crates
WHERE category = 'web-programming'
LIMIT 20;
```

### List crates by keyword

```sql
SELECT name, description, downloads
FROM crates_io.crates
WHERE keyword = 'async'
LIMIT 20;
```

### Include yanked crates

```sql
SELECT name, downloads, yanked
FROM crates_io.crates
WHERE q = 'serde' AND include_yanked = 'yes'
LIMIT 20;
```

### List versions of a crate

```sql
SELECT num, downloads, created_at, yanked, license, rust_version
FROM crates_io.crate_versions
WHERE crate_name = 'serde'
LIMIT 20;
```

### Find dependencies of a specific version

```sql
SELECT crate_id, req, kind, optional, default_features
FROM crates_io.crate_dependencies
WHERE crate_name = 'tokio' AND version = '1.38.0';
```

### Find crate owners

```sql
SELECT login, kind, name, url
FROM crates_io.crate_owners
WHERE crate_name = 'serde';
```

### Browse categories

```sql
SELECT id, category, description, crates_cnt
FROM crates_io.categories
LIMIT 50;
```

### Browse keywords by popularity

```sql
SELECT keyword, crates_cnt
FROM crates_io.keywords
WHERE sort = 'crates'
LIMIT 20;
```

## Validation

Lint the manifest:

```bash
coral source lint sources/community/crates_io/manifest.yaml
```

Run smoke tests (no token needed):

```bash
coral source add --file sources/community/crates_io/manifest.yaml
coral source test crates_io
```

Ad-hoc queries:

```bash
coral sql "SELECT name, downloads FROM crates_io.crates WHERE q = 'serde' LIMIT 5"
coral sql "SELECT id, category, crates_cnt FROM crates_io.categories LIMIT 5"
coral sql "SELECT num, yanked, license FROM crates_io.crate_versions WHERE crate_name = 'tokio' LIMIT 5"
```

## Limitations

- Read-only. No publish, yank, or owner-management operations.
- Rate limited to 1 request/second by crates.io policy.
- Does not include per-day download statistics, reverse dependencies,
  user profiles, or the front-page summary endpoint.
- The crates.io API is labeled "experimental" but the list/search
  endpoints are well-established and used by cargo itself.

## References

- [crates.io data access policy](https://crates.io/data-access)
- [crates.io OpenAPI spec](https://crates.io/api/openapi.json)
- [Cargo Registry Web API](https://doc.rust-lang.org/cargo/reference/registries.html#web-api)
