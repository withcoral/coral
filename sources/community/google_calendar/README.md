# Google Calendar

**Version:** 0.1.0
**Backend:** HTTP
**Tables:** 3
**Base URL:** `https://www.googleapis.com/calendar/v3`

Query calendar lists, calendar metadata, and events from Google Calendar
via the Google Calendar REST API v3.

## Authentication

Requires a `GOOGLE_CALENDAR_ACCESS_TOKEN`. This is an OAuth2 bearer access
token scoped to `https://www.googleapis.com/auth/calendar.readonly`.

Access tokens expire after approximately 1 hour. Re-run the appropriate
command below to get a fresh token when needed.

### Method A — Google Cloud CLI (recommended)

If you have the [Google Cloud CLI](https://cloud.google.com/sdk/docs/install)
installed and authenticated:

```bash
gcloud auth login                          # first time only
gcloud auth print-access-token            # copy the token printed here
```

Then install the source:

```bash
GOOGLE_CALENDAR_ACCESS_TOKEN=ya29.xxx \
  coral source add --file sources/community/google_calendar/manifest.yaml
```

### Method B — OAuth2 Playground (no CLI required)

1. Open <https://developers.google.com/oauthplayground>
2. Under **Step 1**, paste this scope and click **Authorize APIs**:
   ```
   https://www.googleapis.com/auth/calendar.readonly
   ```
3. Sign in with your Google account and grant access.
4. Under **Step 2**, click **Exchange authorization code for tokens**.
5. Copy the **Access token** value (starts with `ya29.`).

Then install the source:

```bash
GOOGLE_CALENDAR_ACCESS_TOKEN=ya29.xxx \
  coral source add --file sources/community/google_calendar/manifest.yaml
```

Or interactively:

```bash
coral source add --file sources/community/google_calendar/manifest.yaml --interactive
```

## Tables

| Table | Description | Required filters | Optional filters |
|---|---|---|---|
| `calendar_lists` | All calendars on the user's calendar list | — | — |
| `calendars` | Metadata for a single calendar | `calendar_id` | — |
| `events` | Events on a calendar | `calendar_id` | `time_min`, `time_max`, `single_events` |

### `calendar_lists`

Returns one row per calendar the authenticated user has on their list.
Use the `id` column as the `calendar_id` filter on `calendars` and `events`.

Key columns: `id`, `summary`, `time_zone`, `access_role`, `primary`, `selected`.

### `calendars`

Single-object lookup. Returns one row with metadata for the requested
calendar. Use `primary` as the `calendar_id` to inspect the default calendar.

Key columns: `id`, `summary`, `description`, `location`, `time_zone`.

### `events`

Returns events from the specified calendar. Without time filters the Google
Calendar API returns events from approximately the past 6 months.

Key columns: `id`, `summary`, `status`, `start__date_time`, `end__date_time`,
`start__date`, `end__date`, `creator__email`, `organizer__email`, `attendees`,
`recurring_event_id`, `html_link`.

#### All-day vs timed events

| Column | Timed events | All-day events |
|---|---|---|
| `start__date_time` | UTC Timestamp ✓ | NULL |
| `start__date` | NULL | YYYY-MM-DD string ✓ |
| `end__date_time` | UTC Timestamp ✓ | NULL |
| `end__date` | NULL | YYYY-MM-DD string ✓ |

#### `single_events` filter note

Set `single_events = 'true'` to expand recurring event series into individual
instances. Without this flag, only the master recurring event record is
returned once. When enabled, results are ordered by `start__date_time`
ascending by the API.

## Quick start

```bash
# List all calendars — discover calendar IDs
coral sql "SELECT id, summary, time_zone, access_role, primary FROM google_calendar.calendar_lists"

# Inspect your primary calendar metadata
coral sql "
  SELECT id, summary, description, time_zone
  FROM google_calendar.calendars
  WHERE calendar_id = 'primary'
"

# Get the next 5 upcoming events on the primary calendar
coral sql "
  SELECT id, summary, status, start__date_time, end__date_time, location
  FROM google_calendar.events
  WHERE calendar_id = 'primary'
    AND time_min = '$(date -u +%Y-%m-%dT%H:%M:%SZ)'
    AND single_events = 'true'
  LIMIT 5
"

# Find events with a specific keyword in the summary
coral sql "
  SELECT id, summary, start__date_time, organizer__email, html_link
  FROM google_calendar.events
  WHERE calendar_id = 'primary'
    AND single_events = 'true'
  LIMIT 50
" | grep -i 'standup'

# Get all events in a time range
coral sql "
  SELECT id, summary, start__date_time, end__date_time,
         organizer__email, status
  FROM google_calendar.events
  WHERE calendar_id = 'primary'
    AND time_min = '2026-05-01T00:00:00Z'
    AND time_max = '2026-05-31T23:59:59Z'
    AND single_events = 'true'
  ORDER BY start__date_time
"

# Count events per organizer this month
coral sql "
  SELECT organizer__email, COUNT(*) AS event_count
  FROM google_calendar.events
  WHERE calendar_id = 'primary'
    AND time_min = '2026-05-01T00:00:00Z'
    AND time_max = '2026-05-31T23:59:59Z'
    AND single_events = 'true'
  GROUP BY organizer__email
  ORDER BY event_count DESC
"

# List all-day events (no start__date_time)
coral sql "
  SELECT id, summary, start__date, end__date
  FROM google_calendar.events
  WHERE calendar_id = 'primary'
    AND single_events = 'true'
    AND start__date_time IS NULL
  LIMIT 20
"

# Expand recurring events into individual instances
coral sql "
  SELECT id, summary, recurring_event_id, start__date_time
  FROM google_calendar.events
  WHERE calendar_id = 'primary'
    AND time_min = '2026-05-01T00:00:00Z'
    AND single_events = 'true'
  ORDER BY start__date_time
  LIMIT 20
"
```

## Discovery order

```text
calendar_lists
  → id (calendar_id)
    → calendars (WHERE calendar_id = '...')
    → events    (WHERE calendar_id = '...')

events
  → recurring_event_id  — groups instances of a recurring series
  → i_cal_uid           — stable cross-calendar deduplication key
  → attendees (JSON)    — parse with json_get_str for individual attendee data
```

## API reference

- [Google Calendar REST API v3](https://developers.google.com/calendar/api/v3/reference)
- [Events: list](https://developers.google.com/calendar/api/v3/reference/events/list)
- [CalendarList: list](https://developers.google.com/calendar/api/v3/reference/calendarList/list)
- [Calendars: get](https://developers.google.com/calendar/api/v3/reference/calendars/get)
