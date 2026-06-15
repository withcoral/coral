# Strava
Adds a community source for the [Strava API](https://developers.strava.com/docs/reference/), exposing athlete activities, profiles, and historical statistics.

## What's included
`manifest.yaml` — three tables, using the OAuth 2.0 authorization code flow.

| Table | Purpose | API call |
|---|---|---|
| `activities` | Athlete's activity history (runs, rides, swims). Ordered by most recent | `/athlete/activities` |
| `athlete` | Authenticated athlete profile and stats | `/athlete` |
| `athlete_stats` | Aggregate running, riding, and swimming totals (recent, YTD, all time) | `/athletes/{id}/stats` |

## Authentication Setup
Strava requires OAuth 2.0. You must register an API application to get a Client ID and Client Secret. Coral will request `activity:read_all` and `profile:read_all` scopes during authorization.

1. Go to your [Strava API Settings](https://www.strava.com/settings/api).
2. Create an API Application.
3. Set the **Authorization Callback Domain** to `127.0.0.1` (this is required for Coral's local OAuth loopback).
4. Note down your **Client ID** and **Client Secret**.

## Adding the Source
When you add the source, Coral will prompt you for your Client ID, Client Secret, and Athlete ID (found in the URL of your Strava profile). It will then open a browser to complete the OAuth flow.

```bash
coral source add --file sources/community/strava/manifest.yaml --interactive
```

## Verification
Test queries are included to validate the connection:

```bash
coral source test strava
```

### Live Query - Recent Activities

```sql
SELECT name, sport_type, distance, moving_time, start_date_local
FROM strava.activities
LIMIT 5;
```

The `activities` table supports optional `before` and `after` filters (epoch timestamps) to narrow the date range and respect [Strava rate limits](https://developers.strava.com/docs/rate-limits/).

### Live Query - Athlete Stats

```sql
SELECT firstname, lastname, city, weight
FROM strava.athlete;
```
