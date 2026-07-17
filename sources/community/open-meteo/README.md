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

### Optional Filters

| Filter | Description |
|---|---|
| `current` | Comma-separated current weather variables (e.g. `temperature_2m,wind_speed_10m`) |
| `hourly` | Comma-separated hourly forecast variables |
| `daily` | Comma-separated daily forecast variables |
| `timezone` | Timezone for time values (e.g. `auto`, `Europe/Berlin`) |
| `forecast_days` | Number of forecast days (1–16) |
| `past_days` | Number of past days to include (0–92) |
| `temperature_unit` | `celsius` (default) or `fahrenheit` |
| `wind_speed_unit` | `kmh` (default), `ms`, `mph`, or `kn` |
| `precipitation_unit` | `mm` (default) or `inch` |

## Authentication

Open-Meteo is completely free for non-commercial use and requires **no authentication**. You can start querying immediately.

## Attribution

**Important:** Open-Meteo data is licensed under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) and requires attribution. If you build an application or service using this source, you must visibly credit Open-Meteo with a link to their service and indicate if any changes were made. See the full [Open-Meteo Licence Terms](https://open-meteo.com/en/licence) for the required attribution link and modification notice.

## Rate Limits

The free tier allows for **10,000 requests per day**, **5,000 per hour**, and **600 per minute**. Be aware that large requests may count as multiple API calls. If you encounter failing queries, ensure you have not exceeded these limits. See [Open-Meteo Pricing](https://open-meteo.com/en/pricing) for more details.

## Examples

### Get Current Weather

```sql
SELECT
  current_valid_at,
  current_temperature_2m,
  current_weather_code
FROM open_meteo.forecast
WHERE latitude = 52.52
  AND longitude = 13.41
  AND current = 'temperature_2m,weather_code'
LIMIT 1;
```

### Get Weather in Fahrenheit

```sql
SELECT
  current_temperature_2m,
  current_units
FROM open_meteo.forecast
WHERE latitude = 40.71
  AND longitude = -74.01
  AND current = 'temperature_2m'
  AND temperature_unit = 'fahrenheit'
LIMIT 1;
```

### Get Hourly Forecast (JSON)

```sql
SELECT hourly
FROM open_meteo.forecast
WHERE latitude = 48.85
  AND longitude = 2.35
  AND hourly = 'temperature_2m,precipitation'
  AND forecast_days = '3'
LIMIT 1;
```

## Note on Projection

Because Coral cannot natively flatten parallel arrays, the `hourly` and `daily` time series are returned as single JSON objects (e.g., `{"time": [...], "temperature_2m": [...]}`). You must extract or iterate these in your application. The `current`, `hourly`, and `daily` API parameters control what data is fetched; standard SQL `SELECT` projection does not trim the upstream API request.

## WMO Weather Codes

The `current_weather_code` column returns a WMO weather interpretation code. Common values:

| Code | Description |
|---|---|
| 0 | Clear sky |
| 1–3 | Mainly clear, partly cloudy, overcast |
| 45, 48 | Fog, depositing rime fog |
| 51–55 | Drizzle (light, moderate, dense) |
| 61–65 | Rain (slight, moderate, heavy) |
| 71–75 | Snow fall (slight, moderate, heavy) |
| 80–82 | Rain showers (slight, moderate, violent) |
| 85–86 | Snow showers (slight, heavy) |
| 95 | Thunderstorm |

Full reference: [Open-Meteo Docs](https://open-meteo.com/en/docs)
