# HackerEarth

**Version:** 0.1.0
**Backend:** HTTP
**Tables:** 1
**Base URL:** `https://www.hackerearth.com`

Query hackathons and coding challenges from [HackerEarth](https://www.hackerearth.com)
via its public events feed (`/chrome-extension/events/`). No authentication required.

```bash
coral source add --file sources/community/hackerearth/manifest.yaml
coral source test hackerearth
```

## Tables

| Table | Description | Filters |
|---|---|---|
| `events` | Current hackathons and challenges feed | — |

---

### `events`

Returns the full current events feed in a single request (no pagination).

```sql
SELECT title, status, start_utc, end_utc
FROM hackerearth.events
WHERE status = 'ONGOING'
LIMIT 20;
```

#### Columns

| Column | Type | Description |
|---|---|---|
| `title` | Utf8 | Event title |
| `description` | Utf8 | Short description |
| `url` | Utf8 | Full URL to the challenge |
| `status` | Utf8 | `ONGOING` or `UPCOMING` |
| `college` | Boolean | Whether the event is college-restricted |
| `challenge_type` | Utf8 | e.g. "Monthly Challenges", "Hiring Challenge" |
| `start_utc` | Utf8 | Start time, UTC (e.g. `2026-04-27 04:30:00+00:00`) |
| `end_utc` | Utf8 | End time, UTC |
| `thumbnail` | Utf8 | Thumbnail image URL |
