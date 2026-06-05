# World Bank Open Data

Query macroeconomic and development indicators from the [World Bank Open Data API](https://datahelpdesk.worldbank.org/knowledgebase/articles/889392-about-the-indicators-api-documentation). No authentication required.

**Coverage:** 29,500+ time-series indicators across 200+ countries and regions, with annual observations typically from 1960 to the most recently published year.

## Tables

| Table | Description |
|---|---|
| `world_bank.data` | **Core query table.** Annual indicator values for a country/region and indicator code |
| `world_bank.indicators` | Full catalog of 29,500+ indicator codes with names and source metadata |
| `world_bank.topic_indicators` | Indicators filtered by thematic topic — for discovering codes by domain |
| `world_bank.countries` | Country/region metadata: ISO codes, geographic region, income level, capital city |
| `world_bank.topics` | The 21 thematic topic groups used to organize indicators |

## Install

```
coral source add --file sources/community/world_bank/manifest.yaml
```

## Example Queries

### GDP over time

```sql
SELECT date, value AS gdp_usd
FROM world_bank.data
WHERE country = 'US' AND indicator = 'NY.GDP.MKTP.CD'
  AND value IS NOT NULL
ORDER BY date DESC
LIMIT 10
```

### GDP for a specific year or year range

```sql
-- Single year (server-side filter — does not over-fetch)
SELECT country_name, date, value AS gdp_usd
FROM world_bank.data
WHERE country = 'all' AND indicator = 'NY.GDP.MKTP.CD' AND date = '2023'
  AND value IS NOT NULL
ORDER BY value DESC
LIMIT 20

-- Year range
SELECT date, value AS gdp_usd
FROM world_bank.data
WHERE country = 'US' AND indicator = 'NY.GDP.MKTP.CD' AND date = '2010:2023'
ORDER BY date DESC
```

> Use the `date` filter to push the year predicate to the API. Without it,
> a `WHERE date = '2023'` clause in SQL applies **after** the fetch, so
> paginated requests may silently miss rows. Supported formats: `'2024'`
> (single year) and `'2010:2020'` (inclusive range).

### Compare poverty rates — latest non-null value per country

```sql
SELECT country_name, date, value AS poverty_rate_pct
FROM world_bank.data
WHERE country = 'all' AND indicator = 'SI.POV.DDAY'
  AND mrnev = '1' AND value IS NOT NULL
ORDER BY value DESC
LIMIT 20
```

> **`mrnev = '1'`** (Most Recent Non-Empty Value) returns one row per
> country/region with its latest **non-null** observation — essential for
> sparse indicators like poverty rates where recent periods often have no
> published data. Use `mrv = '1'` instead when you want the most recent
> period regardless of whether data is null.

### Countries by income level

```sql
SELECT id, name, region, income_level, capital_city
FROM world_bank.countries
WHERE income_level = 'Low income'
ORDER BY name
```

### Discover CO2-related indicators by topic

```sql
-- Step 1: find the Environment topic id
SELECT id, topic FROM world_bank.topics WHERE topic LIKE '%Environment%'
-- Returns: id=6, topic=Environment

-- Step 2: browse environment indicators
SELECT id, name FROM world_bank.topic_indicators WHERE topic = '6' LIMIT 20
```

> The API does not support server-side text search on indicators. Use
> `world_bank.topic_indicators` to browse by theme, or fetch with a high
> `LIMIT` and filter client-side:
> ```sql
> SELECT id, name FROM world_bank.indicators LIMIT 500
> -- then filter results in your SQL client
> ```

### Browse health indicators by topic

```sql
-- Step 1: find topic IDs
SELECT id, topic FROM world_bank.topics

-- Step 2: list health indicators (topic 8)
SELECT id, name
FROM world_bank.topic_indicators
WHERE topic = '8'
LIMIT 20
```

### Renewable energy adoption — top countries (most recent)

```sql
SELECT country_name, date, value AS renewable_pct
FROM world_bank.data
WHERE country = 'all' AND indicator = 'EG.FEC.RNEW.ZS'
  AND mrnev = '1' AND value IS NOT NULL
ORDER BY value DESC
LIMIT 15
```

### Unemployment rate — latest per country

```sql
SELECT country_name, date, value AS unemployment_pct
FROM world_bank.data
WHERE country = 'all' AND indicator = 'SL.UEM.TOTL.ZS'
  AND mrnev = '1' AND value IS NOT NULL
ORDER BY value DESC
LIMIT 10
```

## Commonly Used Indicator Codes

| Code | Indicator |
|---|---|
| `NY.GDP.MKTP.CD` | GDP (current US$) |
| `NY.GNP.PCAP.CD` | GNI per capita (current US$) |
| `SP.POP.TOTL` | Population, total |
| `FP.CPI.TOTL.ZG` | Inflation, consumer prices (annual %) |
| `SL.UEM.TOTL.ZS` | Unemployment, total (% of labor force) |
| `SI.POV.DDAY` | Poverty headcount ratio at $3.00/day (2021 PPP) % |
| `EN.ATM.CO2E.PC` | CO2 emissions (metric tons per capita) |
| `EG.FEC.RNEW.ZS` | Renewable energy consumption (% of total final energy) |
| `SH.DYN.MORT` | Mortality rate, under-5 (per 1,000 live births) |
| `SE.PRM.ENRR` | School enrollment, primary (% gross) |
| `SH.MED.BEDS.ZS` | Hospital beds (per 1,000 people) |
| `IT.NET.USER.ZS` | Individuals using the Internet (% of population) |

Use `world_bank.indicators` or `world_bank.topic_indicators` to discover more codes.

## Country Codes

The `country` filter in `world_bank.data` accepts:
- **ISO2 codes** (2-letter): `US`, `IN`, `CN`, `GB`, `DE`
- **ISO3 codes** (3-letter): `USA`, `IND`, `CHN`, `GBR`, `DEU`
- **`all`** — returns data for all countries in one result set (large)

Use `world_bank.countries` to look up codes for any country or region.

## Data Notes

- **Annual data only:** The `date` column is a year string (e.g. `'2024'`), not a timestamp. Use `WHERE date = '2024'` or `WHERE date = '2010:2020'` to push year filtering to the API — a SQL predicate on `date` without this filter applies after fetching and may silently miss rows when results are paginated.
- **`mrv` vs `mrnev`:** `mrv = '1'` returns the most recent period per country (may be null). `mrnev = '1'` returns the most recent **non-null** period per country. For sparse indicators (poverty rates, hospital beds, etc.), prefer `mrnev`.
- **Null values:** Many country/year combinations have no published data — always filter `WHERE value IS NOT NULL` unless you want to see coverage gaps.
- **Aggregate regions:** Entries like `EAS` (East Asia & Pacific) or `SSA` (Sub-Saharan Africa) are valid country codes and return regional aggregates.
- **Data freshness:** Varies by indicator. Most WDI indicators are updated annually.

## Validation

- `coral source lint`: ✅ passed
- `coral source add`: ✅ connected, 5 tables, 5/5 test queries passed
- Live queries verified:
  - `world_bank.data` — US GDP (NY.GDP.MKTP.CD): annual data returned, nested `indicator.id` and `country.value` fields resolved correctly
  - `world_bank.data` — India population (SP.POP.TOTL): 2025 null (not yet published), 2024 = 1,450,935,791 ✅
  - `world_bank.data` — `country = 'all'`, `mrv = '1'`: one row per country, top result is World aggregate ✅
  - `world_bank.indicators` — pagination working, 29,511 total indicators
  - `world_bank.countries` — 296 countries and regions in single request, nested `region.value` and `incomeLevel.value` resolved correctly ✅
  - `world_bank.topics` — all 21 topics with full `sourceNote` text returned ✅
  - `world_bank.topic_indicators` — topic `3` (Economy & Growth): 306 indicators ✅
  - Invalid country code: returns empty rows via `null` data array handling (no error) ✅
  - `rows_path: ["1"]` navigates numeric index in root array — confirmed working ✅
