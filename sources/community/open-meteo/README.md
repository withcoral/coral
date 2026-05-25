# Open-Meteo

[Open-Meteo](https://open-meteo.com) is an open-source weather API with free access for non-commercial use.

## Setup

### 1. Add the source

Open-Meteo requires no authentication, so you can add it immediately:

```bash
coral source add --file sources/community/open-meteo/manifest.yaml
```

### 2. Verify

```bash
coral source test open_meteo
```

## Tables

| Table | Description | Required filters | Pagination |
|---|---|---|---|
| `open_meteo.forecast` | Current weather, hourly, and daily forecasts for a coordinate | `latitude`, `longitude` | none |

## Authentication

Open-Meteo is completely free for non-commercial use and requires **no authentication**. You can start querying immediately.

## Attribution

**Important:** Open-Meteo data is licensed under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) and requires attribution. If you build an application or service using this source, you must visibly credit Open-Meteo.

## Rate Limits

The free tier allows for roughly **10,000 requests per day**. If you encounter failing queries, ensure you have not exceeded this limit. See [Open-Meteo Pricing](https://open-meteo.com/en/pricing) for more details.

## Examples

### Get Current Weather

```sql
SELECT
  current_time,
  current_temperature_2m,
  current_weather_code
FROM open_meteo.forecast
WHERE latitude = 52.52
  AND longitude = 13.41
  AND current = 'temperature_2m,weather_code'
```



## Note on Projection

Because Coral cannot natively flatten parallel arrays, the `hourly` and `daily` time series are returned as single JSON objects (e.g., `{"time": [...], "temperature_2m": [...]}`). You must extract or iterate these in your application. The `current`, `hourly`, and `daily` API parameters control what data is fetched; standard SQL `SELECT` projection does not trim the upstream API request.
