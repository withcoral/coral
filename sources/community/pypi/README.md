# PyPI — Coral community source

Query PyPI package download statistics using SQL.

## Overview

This source exposes the public [pypistats.org](https://pypistats.org) API as a read-only SQL table.
No authentication is required — the endpoint is fully public.

| Table | Description |
| ----- | ----------- |
| `pypi.packages` | Retrieve last day, last week, and last month download stats for any package |

## Setup

No API token or authentication is needed. Add the source directly:

```bash
coral source add --file sources/community/pypi/manifest.yaml
```

## Example query

```sql
SELECT last_month_downloads, last_week_downloads
FROM pypi.packages
WHERE name = 'fastapi'
LIMIT 1;
```

## Validation

Lint the manifest:

```bash
coral source lint sources/community/pypi/manifest.yaml
```

Run test query:

```bash
coral sql "SELECT * FROM pypi.packages WHERE name = 'fastapi'"
```
