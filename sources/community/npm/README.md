# npm — Coral community source

Query npm package download statistics using SQL.

## Overview

This source exposes the public [api.npmjs.org](https://api.npmjs.org) API as a read-only SQL table.
No authentication is required — the endpoint is fully public.

| Table | Description |
| ----- | ----------- |
| `npm.packages` | Retrieve download stats (daily, weekly, monthly) for any npm package |

## Setup

No API token or authentication is needed. Add the source directly:

```bash
coral source add --file sources/community/npm/manifest.yaml
```

## Example query

```sql
SELECT package, downloads, start, end
FROM npm.packages
WHERE name = 'express' AND period = 'last-week'
LIMIT 1;
```

## Validation

Lint the manifest:

```bash
coral source lint sources/community/npm/manifest.yaml
```

Run test query:

```bash
coral sql "SELECT * FROM npm.packages WHERE name = 'express'"
```
