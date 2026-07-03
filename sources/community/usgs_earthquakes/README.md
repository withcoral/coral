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
SELECT id, magnitude, place, latitude, longitude, depth_km, event_time, url
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
SELECT id, magnitude, place, latitude, longitude, depth_km, event_time
FROM usgs_earthquakes.events
WHERE center_latitude = 37.7749
  AND center_longitude = -122.4194
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
  AND min_magnitude = 5
ORDER BY event_time DESC
LIMIT 50;
```

## Exposed Table

`usgs_earthquakes.events` returns GeoJSON feature rows from `/query?format=geojson`, with common fields flattened into typed columns and the full `geometry` and `properties` objects available as JSON. The `latitude`, `longitude`, and `depth_km` columns represent the event coordinates from `geometry.coordinates`.

## Query Guidance

USGS limits query responses and broad historical scans can fail upstream even when a SQL `LIMIT` is present. For reliable first results, keep at least one of these filters narrow:

- Time range: use `start_time` and `end_time`.
- Magnitude: use `min_magnitude` for broader time windows.
- Geography: use either `center_latitude`, `center_longitude`, and `max_radius_km`, or a bounding box.

For very broad recent-event use cases, USGS also publishes real-time feeds outside the catalog query endpoint.

USGS is a shared public API: it can return `429 Too Many Requests` under load and its responses may be cached, so avoid polling aggressively. Prefer narrow filters and let cached results serve repeated reads rather than re-querying in a tight loop. See the USGS guidance on [real-time feed rate and cache behavior](https://geohazards.usgs.gov/pipermail/realtime-feeds/2022-January/000028.html).

### Ordering and top-N queries

`order_by` pushes ordering down to USGS (`orderby`), which matters for top-N questions because USGS truncates the result set on the server before Coral sees it. A SQL `ORDER BY ... LIMIT n` only sorts the page that was already fetched, so it can miss the true top results across the full catalog. To get the largest or most recent events overall, push the order to USGS with `order_by` and combine it with `LIMIT`:

```sql
-- Largest events in a window (ask USGS for magnitude ordering)
SELECT event_time, place, magnitude
FROM usgs_earthquakes.events
WHERE start_time = '2026-05-01'
  AND end_time = '2026-05-27'
  AND order_by = 'magnitude'
LIMIT 5;
```

Supported `order_by` values are `time` (newest first, the default), `time-asc`, `magnitude` (largest first), and `magnitude-asc`. Use SQL `ORDER BY` only for re-sorting within an already-narrowed result set; use `order_by` whenever the ranking itself must reflect the full catalog.

Common filters include:

- `start_time`, `end_time`, `updated_after`
- `min_magnitude`, `max_magnitude`
- `center_latitude`, `center_longitude`, `max_radius_km`
- `min_latitude`, `max_latitude`, `min_longitude`, `max_longitude`
- `event_type`, `alert_level`, `status`, `order_by`

The source uses offset pagination with USGS `limit` and `offset` parameters.
