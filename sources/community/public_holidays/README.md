# Public Holidays (Nager.Date)

Query global public holidays, available countries, and long weekends using the free [Nager.Date API](https://date.nager.at/Api).

## Setup

No API key or authentication is needed. Add the source directly:

```bash
coral source add --file sources/community/public_holidays/manifest.yaml
```

## Rate Limits

According to the official Nager.Date API documentation, there are currently **no rate limits** for this API. However, caching and polite usage is always recommended.

## Tables

### `available_countries`
List all supported countries and their respective ISO codes.

**Example:**
```sql
SELECT country_code, name
FROM public_holidays.available_countries;
```

### `holidays`
Fetch all public holidays for a specific country and year. Requires `year` and `country_code` filters. Note: the `fixed` and `launch_year` columns are officially deprecated in the upstream API.

**Example:**
```sql
SELECT date, name, local_name, global
FROM public_holidays.holidays
WHERE year = '2023' AND country_code = 'US';
```

### `long_weekend`
Fetch all long weekends for a specific country and year. Requires `year` and `country_code` filters. Optional filters: `subdivision_code` and `available_bridge_days`.

**Example:**
```sql
SELECT start_date, end_date, day_count, need_bridge_day
FROM public_holidays.long_weekend
WHERE year = '2023' AND country_code = 'US';
```
