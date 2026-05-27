# Frankfurter API

Fetch current and historical foreign exchange rates using the [Frankfurter API](https://frankfurter.dev/docs/). Data is sourced from central banks like the European Central Bank.

## Setup

No API key or authentication is needed. Add the source directly:

```bash
coral source add --file sources/community/frankfurter/manifest.yaml
```

## Rate Limits

Frankfurter is an open-source public API. It does not enforce hard daily or monthly request quotas, but requests are rate-limited to prevent abuse. Please be polite and cache data when possible. See their [documentation](https://frankfurter.dev/docs/) for more details.

## Tables

### `rates`
Fetch current and historical currency exchange rates. Each row represents a single currency pair rate for a specific date.

Optional filters:
- `date`: A specific date (YYYY-MM-DD). If omitted without `from`/`to`, returns the latest available rate. Note: Frankfurter may return a fallback date for non-trading days. Use the `requested_date` column to check your requested filter, while the `date` column shows the actual returned trading date.
- `from` and `to`: A date range (YYYY-MM-DD).
- `base`: The base currency (defaults to `EUR`).
- `quotes`: Comma-separated list of quote currencies to include.
- `providers`: Comma-separated list of specific data providers (e.g., `ecb`).
- `group`: Grouping interval for historical rates (e.g., `monthly`, `yearly`).
- `expand`: Boolean to expand the rate details instead of returning simple values.

**Example: Fetch the latest exchange rate for USD to EUR**
```sql
SELECT date, base, quote, rate
FROM frankfurter.rates
WHERE base = 'USD' AND quotes = 'EUR';
```

**Example: Fetch historical rates for a specific month**
```sql
SELECT date, base, quote, rate
FROM frankfurter.rates
WHERE "from" = '2023-01-01' AND "to" = '2023-01-31' AND base = 'EUR';
```

### `currencies`
List all supported currencies (both active and legacy), their full names, and symbols.

**Example:**
```sql
SELECT iso_code, name, symbol
FROM frankfurter.currencies;
```
