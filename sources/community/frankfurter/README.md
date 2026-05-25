# Frankfurter

Query currency exchange rates from the free, open-source [Frankfurter API](https://www.frankfurter.app/docs/).

## Setup

No API key or authentication is needed. Add the source directly:

```bash
coral source add --file sources/community/frankfurter/manifest.yaml
```

## Tables

### `latest`
Fetch the latest currency exchange rates.

**Example:**
```sql
SELECT amount, base, date, rates
FROM frankfurter.latest;
```

### `historical`
Fetch historical currency exchange rates for a specific date (YYYY-MM-DD format).

**Example:**
```sql
SELECT amount, base, date, rates
FROM frankfurter.historical
WHERE date = '2023-01-01';
```

### `currencies`
List all supported currency codes and their full names.

**Example:**
```sql
SELECT code, name
FROM frankfurter.currencies;
```
