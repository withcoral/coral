# USGS Earthquakes Coral Source

Query earthquake events from the public USGS Earthquake Catalog API as SQL.

## Why this source

This source makes live earthquake data queryable without an API key. It is useful for agents that need to correlate natural hazard events with incident response, infrastructure status, weather, travel, logistics, or public safety data.

## Install

```bash
coral source lint sources/community/usgs_earthquakes/manifest.yaml
coral source add --file sources/community/usgs_earthquakes/manifest.yaml
coral source test usgs_earthquakes
```

## Example Queries

Recent earthquakes:

```sql
SELECT id, magnitude, place, event_time, url
FROM usgs_earthquakes.events
ORDER BY event_time DESC
LIMIT 10;
```

Magnitude 5+ earthquakes:

```sql
SELECT id, magnitude, place, alert, event_time
FROM usgs_earthquakes.events
WHERE min_magnitude = 5
ORDER BY event_time DESC
LIMIT 20;
```

Earthquakes near San Francisco:

```sql
SELECT id, magnitude, place, event_time
FROM usgs_earthquakes.events
WHERE latitude = 37.7749
  AND longitude = -122.4194
  AND max_radius_km = 500
  AND min_magnitude = 2.5
ORDER BY event_time DESC
LIMIT 20;
```

Earthquakes in a time window:

```sql
SELECT id, magnitude, place, event_time
FROM usgs_earthquakes.events
WHERE start_time = '2026-05-01'
  AND end_time = '2026-05-27'
  AND min_magnitude = 4
ORDER BY event_time DESC
LIMIT 50;
```

## Exposed Table

`usgs_earthquakes.events` returns GeoJSON feature rows from `/query?format=geojson`, with common fields flattened into typed columns and the full `geometry` and `properties` objects available as JSON.

Common filters include:

- `start_time`, `end_time`, `updated_after`
- `min_magnitude`, `max_magnitude`
- `latitude`, `longitude`, `max_radius_km`
- `min_latitude`, `max_latitude`, `min_longitude`, `max_longitude`
- `event_type`, `alert_level`, `status`, `order_by`

The source uses offset pagination with USGS `limit` and `offset` parameters.
