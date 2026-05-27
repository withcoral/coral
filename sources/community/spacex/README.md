# SpaceX Coral Source

Query the public, unofficial SpaceXData launch, rocket, launchpad, and crew dataset as SQL.

## Why this source

This source makes the unofficial SpaceXData mission and vehicle dataset available to agents without credentials. It is useful for demos, historical aerospace research, educational workflows, and examples that need rich linked public data.

SpaceXData v4 is best treated as a historical community dataset, not a live launch-monitoring API. At the time this source was added, the newest launch record returned by the API was from December 5, 2022.

## Install

```bash
coral source lint sources/community/spacex/manifest.yaml
coral source add --file sources/community/spacex/manifest.yaml
coral source test spacex
```

## Example Queries

Most recent launches in the SpaceXData dataset:

```sql
SELECT name, date_utc, success, rocket_id, launchpad_id
FROM spacex.launches
ORDER BY date_unix DESC
LIMIT 10;
```

Upcoming launches recorded in SpaceXData:

```sql
SELECT name, date_utc, rocket_id, launchpad_id
FROM spacex.launches
WHERE upcoming = true
ORDER BY date_unix DESC
LIMIT 10;
```

Successful Falcon 9 launches with rocket details:

```sql
SELECT l.name, l.date_utc, r.name AS rocket_name, r.success_rate_pct
FROM spacex.launches AS l
JOIN spacex.rockets AS r ON l.rocket_id = r.id
WHERE l.success = true
  AND r.name = 'Falcon 9'
ORDER BY l.date_unix DESC
LIMIT 20;
```

Launchpads by historical success:

```sql
SELECT name, region, status, launch_attempts, launch_successes
FROM spacex.launchpads
ORDER BY launch_successes DESC
LIMIT 10;
```

Crew members and their agencies:

```sql
SELECT name, agency, status, wikipedia
FROM spacex.crew
ORDER BY agency, name
LIMIT 25;
```

## Exposed Tables

- `spacex.launches`
- `spacex.rockets`
- `spacex.launchpads`
- `spacex.crew`

Each table exposes common fields as typed columns and keeps the full source object in a `raw` JSON column for advanced use.

`spacex.launches` uses the `/v4/launches/query` endpoint with page-based response metadata, sorted by `date_unix` descending by default. Filters on `upcoming`, `success`, `rocket_id`, `launchpad_id`, and `flight_number` are pushed down to the SpaceXData API.
