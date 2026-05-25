# Open-Meteo

A community source that exposes [Open-Meteo](https://open-meteo.com/), a free and open-source weather API, to Coral SQL.

## Authentication

Open-Meteo is completely free for non-commercial use and does **not** require an API key or authentication! It works straight out of the box.

## Tables

| Table | Description | Required Filters |
| :--- | :--- | :--- |
| `open_meteo.current_weather` | Forecast API for current weather data | `latitude`, `longitude` |

> [!IMPORTANT]
> The `current_weather` table requires exact `latitude` and `longitude` coordinates.

## Example Queries

### Get Current Weather
Get the current weather for Berlin (lat 52.52, lon 13.41).

```sql
SELECT time, temperature_2m, wind_speed_10m, relative_humidity_2m
FROM open_meteo.current_weather
WHERE latitude = 52.52 AND longitude = 13.41;
```

## Limitations
- Open-Meteo returns hourly and daily forecast data as columnar arrays. This source focuses on exposing the "current" conditions, as these map cleanly to SQL rows.
- If you intend to use this source for commercial purposes or exceed the free tier limits (10,000 API calls per day), you will need to purchase an Open-Meteo commercial API key and manually add it to the queries in the manifest file.
