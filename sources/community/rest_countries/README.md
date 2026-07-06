# REST Countries

Query country data, currencies, languages, and more from the free REST Countries API.

## Setup

No API key or authentication is needed. Add the source directly:

```bash
coral source add --file sources/community/rest_countries/manifest.yaml
```

## Tables

### `all`

Returns all countries in the world.

**Example:**
```sql
SELECT name_common, name_official, region, population
FROM rest_countries."all"
ORDER BY population DESC
LIMIT 10;
```

### `by_name`

Search for a country by its common or official name.

**Example:**
```sql
SELECT name_common, region, subregion, area
FROM rest_countries.by_name
WHERE name = 'france';
```

### `by_code`

Search for a country by its ISO 3166-1 alpha-2, alpha-3, numeric code, or CIOC.

**Example:**
```sql
SELECT name_common, currencies, languages, flags_png
FROM rest_countries.by_code
WHERE code = 'jpn';
```

### `by_region`

Search for countries by region.

**Example:**
```sql
SELECT name_common, subregion, population
FROM rest_countries.by_region
WHERE region = 'europe';
```

### `by_currency`

Search for countries by currency code or name.

**Example:**
```sql
SELECT name_common, region, currencies
FROM rest_countries.by_currency
WHERE currency = 'eur';
```

## Limits

- This API is entirely free and public, maintained by a community project.
- Please be respectful of rate limits.
- Nested fields like `currencies` and `languages` are exposed as `Json`.
