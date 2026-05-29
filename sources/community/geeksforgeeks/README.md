# GeeksforGeeks Practice

Query coding problems, company tags, topic tags, difficulty levels, accuracy statistics, submission counts, and popularity data from GeeksforGeeks Practice using Coral SQL.

```bash
coral source add --file sources/community/geeksforgeeks/manifest.yaml
```

## Overview

This source exposes the GeeksforGeeks Practice problem catalog as a SQL table.

You can:

- Browse coding problems
- Filter by difficulty
- Filter by company tags
- Filter by topic/category tags
- Sort by popularity, submissions, difficulty, or accuracy
- Analyze problem statistics
- Build company-specific interview preparation lists
- Discover trending coding questions

---

## Tables

| Table | Description |
|---------|------------|
| `problems` | GeeksforGeeks Practice problem catalog with metadata, statistics, tags, and filters. |

---

# Schema

## geeksforgeeks.problems

### Core Information

| Column | Type | Description |
|----------|---------|-------------|
| `id` | STRING | Unique problem identifier |
| `problem_name` | STRING | Problem title |
| `problem_url` | STRING | Direct URL to problem |
| `problem_slug` | STRING | URL-friendly slug |
| `difficulty` | STRING | Difficulty label |
| `difficulty_level` | INTEGER | Internal difficulty code |
| `marks` | FLOAT | Marks assigned to problem |

### Statistics

| Column | Type | Description |
|---------|---------|-------------|
| `accuracy` | FLOAT | Problem accuracy percentage |
| `all_submissions` | INTEGER | Total submissions |
| `popularity` | FLOAT | Popularity score |
| `likes` | INTEGER | Number of likes |
| `dislikes` | INTEGER | Number of dislikes |

### Tags

| Column | Type | Description |
|---------|---------|-------------|
| `category` | STRING | Topic/category tags |
| `company` | STRING | Company tags |
| `status` | STRING | Problem status metadata |

### Metadata

| Column | Type | Description |
|---------|---------|-------------|
| `created_at` | TIMESTAMP | Creation timestamp |
| `updated_at` | TIMESTAMP | Last updated timestamp |

---

# Supported Filters

## Difficulty

| Value | Meaning |
|---------|---------|
| `-1` | School |
| `0` | Basic |
| `1` | Easy / Medium |
| `2` | Hard |

Example:

```sql
SELECT problem_name, difficulty
FROM geeksforgeeks.problems
WHERE difficulty_level = 2
LIMIT 20;
```

---

## Category

Examples:

```sql
SELECT problem_name
FROM geeksforgeeks.problems
WHERE category = 'Arrays';
```

```sql
SELECT problem_name
FROM geeksforgeeks.problems
WHERE category = 'Strings';
```

```sql
SELECT problem_name
FROM geeksforgeeks.problems
WHERE category = 'Trees';
```

```sql
SELECT problem_name
FROM geeksforgeeks.problems
WHERE category = 'Graphs';
```

```sql
SELECT problem_name
FROM geeksforgeeks.problems
WHERE category = 'Dynamic Programming';
```

---

## Multiple Categories

```sql
SELECT problem_name
FROM geeksforgeeks.problems
WHERE category = 'Arrays,Strings';
```

```sql
SELECT problem_name
FROM geeksforgeeks.problems
WHERE category = 'Trees,Graphs';
```

```sql
SELECT problem_name
FROM geeksforgeeks.problems
WHERE category = 'Dynamic Programming,Greedy';
```

---

## Company

```sql
SELECT problem_name
FROM geeksforgeeks.problems
WHERE company = 'Amazon';
```

```sql
SELECT problem_name
FROM geeksforgeeks.problems
WHERE company = 'Google';
```

```sql
SELECT problem_name
FROM geeksforgeeks.problems
WHERE company = 'Microsoft';
```

```sql
SELECT problem_name
FROM geeksforgeeks.problems
WHERE company = 'Adobe';
```

---

## Multiple Companies

```sql
SELECT problem_name
FROM geeksforgeeks.problems
WHERE company = 'Amazon,Google';
```

```sql
SELECT problem_name
FROM geeksforgeeks.problems
WHERE company = 'Amazon,Google,Microsoft';
```

```sql
SELECT problem_name
FROM geeksforgeeks.problems
WHERE company = 'Meta,Netflix,Uber';
```

---

## Sorting

Supported values:

- difficulty
- accuracy
- submissions
- popularity
- name

Example:

```sql
SELECT problem_name, accuracy
FROM geeksforgeeks.problems
WHERE sort_by = 'accuracy'
LIMIT 20;
```

---

# Example Queries

## Explore Problems

```sql
SELECT problem_name, difficulty
FROM geeksforgeeks.problems
LIMIT 20;
```

## Hard Problems

```sql
SELECT problem_name, difficulty, accuracy
FROM geeksforgeeks.problems
WHERE difficulty_level = 2
LIMIT 50;
```

## Google Interview Problems

```sql
SELECT problem_name, difficulty
FROM geeksforgeeks.problems
WHERE company = 'Google'
LIMIT 50;
```

## Amazon Graph Problems

```sql
SELECT problem_name, difficulty
FROM geeksforgeeks.problems
WHERE company = 'Amazon'
  AND category = 'Graphs'
LIMIT 50;
```

## Dynamic Programming Problems

```sql
SELECT problem_name, accuracy
FROM geeksforgeeks.problems
WHERE category = 'Dynamic Programming'
LIMIT 50;
```

## Most Attempted Problems

```sql
SELECT problem_name, all_submissions
FROM geeksforgeeks.problems
WHERE sort_by = 'submissions'
LIMIT 50;
```

## Highest Accuracy Problems

```sql
SELECT problem_name, accuracy
FROM geeksforgeeks.problems
WHERE sort_by = 'accuracy'
LIMIT 50;
```

## Company + Category Combination

```sql
SELECT problem_name, difficulty
FROM geeksforgeeks.problems
WHERE company = 'Google'
  AND category = 'Trees'
LIMIT 50;
```

## Hard DP Problems From Google

```sql
SELECT problem_name, accuracy
FROM geeksforgeeks.problems
WHERE difficulty_level = 2
  AND company = 'Google'
  AND category = 'Dynamic Programming'
LIMIT 100;
```

---

# Local Testing

```bash
coral source add \
  --file sources/community/geeksforgeeks/manifest.yaml
```

Expected output:

```text
Added source geeksforgeeks

✓ geeksforgeeks connected successfully

  geeksforgeeks (1 table)
  └─ problems

  Query tests
  1 declared · 1 passed · 0 failed
```

Run a sample query:

```bash
coral sql "
SELECT problem_name, difficulty
FROM geeksforgeeks.problems
LIMIT 5
"
```

Example output:

```text
+-------------------------+------------+
| problem_name            | difficulty |
+-------------------------+------------+
| Two Sum                 | Easy       |
| Merge Intervals         | Medium     |
| Detect Cycle in Graph   | Hard       |
+-------------------------+------------+
```

---

# Data Source

Public GeeksforGeeks Practice API:

https://practiceapi.geeksforgeeks.org

---

# Notes

- Filters are pushed down to the GeeksforGeeks API whenever possible.
- Multi-value filters are supported using comma-separated values.
- Results are paginated internally.
- Query performance depends on API response times.
- Company tags and categories are sourced directly from GeeksforGeeks metadata.
- Statistics such as accuracy and submissions reflect values provided by GeeksforGeeks.