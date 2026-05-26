# Open Collective — Coral community source

Query Open Collective funding data using SQL.

## Overview

This source exposes the public Open Collective GraphQL API as a read-only SQL table.
No authentication is required for basic public project information.

| Table | Description |
| ----- | ----------- |
| `opencollective.collectives` | Retrieve funding details, balance, and contributor counts by project slug |

## Setup

No API token or authentication is needed. Add the source directly:

```bash
coral source add --file sources/community/opencollective/manifest.yaml
```

## Example query

```sql
SELECT total_amount_received, contributors_count, currency
FROM opencollective.collectives
WHERE slug = 'webpack'
LIMIT 1;
```

## Validation

Lint the manifest:

```bash
coral source lint sources/community/opencollective/manifest.yaml
```

Run test query:

```bash
coral sql "SELECT * FROM opencollective.collectives WHERE slug = 'webpack'"
```
