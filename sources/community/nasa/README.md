# NASA Astronomy Picture of the Day (NASA APOD)

**Version:** 0.1.0  
**Backend:** HTTP  
**Tables:** 1  
**Base URL:** `https://api.nasa.gov`

Query NASA's [Astronomy Picture of the Day](https://apod.nasa.gov/apod/astropix.html) API to fetch daily space images, videos, and their scientific explanations.

```bash
coral source add --file sources/community/nasa/manifest.yaml
```

## Configuration

| Input          | Kind     | Required | Default   | Description                                                              |
| -------------- | -------- | -------- | --------- | ------------------------------------------------------------------------ |
| `NASA_API_KEY` | secret   | no       | (none)     | NASA Open API key. Leave unset to use `DEMO_KEY` (30 req/hr). Free personal keys are available at [api.nasa.gov](https://api.nasa.gov/). |

## Tables

| Table       | Description                           | Key filters                                              |
| ----------- | ------------------------------------- | -------------------------------------------------------- |
| `nasa.apod` | NASA Astronomy Picture of the Day     | `date`, `start_date`, `end_date`, `count`, `thumbs`, `concept_tags` |

## Example queries

```sql
-- Today's picture of the day
SELECT date, title, url, media_type
FROM nasa.apod
LIMIT 1;

-- APOD for a specific date
SELECT date, title, explanation, url, hdurl
FROM nasa.apod
WHERE date = '2024-06-01'
LIMIT 1;

-- Fetch 5 random APOD entries
SELECT date, title, url, media_type
FROM nasa.apod
WHERE count = 5
LIMIT 5;

-- Fetch a date range of APOD entries
SELECT date, title, url
FROM nasa.apod
WHERE start_date = '2024-06-01' AND end_date = '2024-06-10'
LIMIT 10;

-- Request video thumbnails for APOD entries
SELECT date, title, url, thumbnail_url
FROM nasa.apod
WHERE thumbs = true AND count = 3
LIMIT 3;
```

## API Parameters

The `apod` table supports these query parameters:

- **`date`** — A specific date in `YYYY-MM-DD` format. Returns a single row.
- **`start_date` / `end_date`** — A date range (inclusive). Returns one row per day.
- **`count`** — Number of *random* APOD entries to return (max 100). Returns that many rows.
- **`thumbs`** — Set to `true` to request video thumbnail URLs when `media_type` is `video`.
- **`concept_tags`** — Set to `true` to include NASA concept tags in the response.

**Note:** `date`, `start_date`/`end_date`, and `count` are mutually exclusive. Only use one style per query.

## Notes

- **Default auth.** Leaving `NASA_API_KEY` unset uses `DEMO_KEY` (30 req/hr). For higher limits, set a free personal key from [api.nasa.gov](https://api.nasa.gov/).
- **Rate limits:** `DEMO_KEY` is limited to 30 requests per hour and 50 requests per day. A free personal API key raises this to 1000 requests per hour.
- **Images vs. Videos:** When `media_type` is `video`, `url` points to a video page and `hdurl` is absent. Set `thumbs = true` to receive a `thumbnail_url` for video items.
- **Date range:** The APOD archive begins on **1995-06-16**. Dates before this return no results.
- **Read-only.** This source does not support write operations.

## Validation

```bash
coral source lint sources/community/nasa/manifest.yaml
coral source add --file sources/community/nasa/manifest.yaml
coral source test nasa

coral sql "SELECT date, title, url FROM nasa.apod LIMIT 1"
```
