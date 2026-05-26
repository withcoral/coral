# Google Calendar

Query calendar metadata and events from Google Calendar API v3. Supports listing accessible calendars, fetching events with date-range filters, and pagination.

## Authentication

This source requires a Google OAuth 2.0 access token with the `https://www.googleapis.com/auth/calendar.readonly` scope.

For local development or testing, you can use the Google Cloud CLI to generate a short-lived token:
```bash
gcloud auth application-default login \
  --scopes=https://www.googleapis.com/auth/calendar.readonly

export GOOGLE_CALENDAR_ACCESS_TOKEN=$(gcloud auth print-access-token)
```
*Note: Tokens generated via `gcloud` expire after approximately 60 minutes.*

For production use, you must configure a Google Cloud project with OAuth 2.0 credentials and implement a token refresh flow. See the [Google Calendar Auth Guide](https://developers.google.com/calendar/api/guides/auth).

## Installation

Set up interactively via Google OAuth:
```bash
coral source add --interactive --file sources/community/google_calendar/manifest.yaml
```

Or provide a manual token from the environment:
```bash
coral source add --file sources/community/google_calendar/manifest.yaml
```

## Tables

### `calendars`
Lists all calendars (owned, subscribed, shared) visible to the authenticated user.

| Column | Type | Description |
|---|---|---|
| `id` | string | Calendar identifier. Use this as the `calendar_id` filter when querying the `events` table. Note: If the ID contains reserved URL characters (such as `#`), you must URL-encode it first (e.g. replace `#` with `%23`). |
| `summary` | string | Calendar title. |
| `description` | string | Calendar description. |
| `time_zone` | string | IANA time zone (e.g. `America/New_York`). |
| `access_role` | string | Effective access role (`freeBusyReader`, `reader`, `writer`, `owner`). |
| `primary` | boolean | `true` for the user's primary calendar. |

### `events`
Lists events from a single calendar. **A `calendar_id` filter is required.** To query the default calendar, use `calendar_id = 'primary'`.

| Column | Type | Description |
|---|---|---|
| `id` | string | Opaque event identifier. |
| `summary` | string | Event title. |
| `description` | string | Event description or notes. |
| `start_date_time` | timestamp | ISO 8601 start datetime with timezone offset (null for all-day events). |
| `start_date` | string | All-day event start date `YYYY-MM-DD` (null for timed events). |
| `end_date_time` | timestamp | ISO 8601 end datetime with timezone offset (null for all-day events). |
| `end_date` | string | All-day event end date `YYYY-MM-DD` (null for timed events). |
| `start_time_zone` | string | IANA time zone of the event start. |
| `calendar_id` | string | Virtual column exposing the `calendar_id` filter value. If the ID contains reserved URL characters (such as `#`), it must be URL-encoded. |
| `q` | string | Virtual column exposing the `q` (free-text search query) filter value. |
| `time_min` | string | Virtual column exposing the `time_min` filter value (RFC 3339 exclusive lower bound for event end time). |
| `time_max` | string | Virtual column exposing the `time_max` filter value (RFC 3339 exclusive upper bound for event start time). |
| `organizer_email` | string | Email of the event organizer. |
| `organizer_self` | boolean | `true` if the organizer is the authenticated user. |
| `status` | string | Event status (`confirmed`, `tentative`, or `cancelled`). |
| `html_link` | string | URL to open the event in the Google Calendar web UI. |
| `location` | string | Location text or Google Meet link. |
| `created` | timestamp | Creation timestamp (RFC 3339). |
| `updated` | timestamp | Last modification timestamp (RFC 3339). |
| `creator_email` | string | Email of the user who created the event. |
| `event_type` | string | Type of event. Example event types include: `default`, `focusTime`, `outOfOffice`, `workingLocation`, `birthday`, `fromGmail`. |
| `recurring_event_id` | string | ID of the master recurring event, if applicable. |
| `ical_uid` | string | RFC 5545 iCalendar UID. |
| `hangout_link` | string | Attached Google Meet link. |
| `visibility` | string | Event visibility (`default`, `public`, `private`, `confidential`). |
| `transparency` | string | Schedule blocking (`opaque` for busy, `transparent` for free). |

## Push-down Filters

The `events` table supports pushing down filters directly to the Google Calendar API:
- `calendar_id` (required, must be URL-encoded if it contains reserved characters like `#`)
- `time_min` (RFC 3339 exclusive lower bound for event end time)
- `time_max` (RFC 3339 exclusive upper bound for event start time)
- `q` (free-text search)

An event is returned if its duration overlaps the window (`event.end > time_min` AND `event.start < time_max`). Events that exactly touch the boundaries are excluded.

## Example Queries

```sql
-- List accessible calendars
SELECT id, summary, access_role
FROM google_calendar.calendars;

-- Upcoming events this week on the primary calendar
SELECT summary, start_date_time, end_date_time
FROM google_calendar.events
WHERE calendar_id = 'primary'
  AND time_min = '2026-05-25T00:00:00Z'
  AND time_max = '2026-06-01T00:00:00Z'
ORDER BY start_date_time;

-- Keyword search
SELECT summary, start_date_time, location
FROM google_calendar.events
WHERE calendar_id = 'primary'
  AND q = 'standup';

-- Identify all-day events
SELECT summary, start_date, end_date
FROM google_calendar.events
WHERE calendar_id = 'primary'
  AND start_date IS NOT NULL;
```

## Example Output

```bash
$ coral sql "SELECT id, summary, access_role FROM google_calendar.calendars LIMIT 1"
+------------------------------------+----------------+-------------+
| id                                 | summary        | access_role |
+------------------------------------+----------------+-------------+
| primary                            | Alice's Events | owner       |
+------------------------------------+----------------+-------------+
```

## API References

- [calendarList.list](https://developers.google.com/workspace/calendar/api/v3/reference/calendarList/list)
- [events.list](https://developers.google.com/workspace/calendar/api/v3/reference/events/list)
- [Event Resource](https://developers.google.com/workspace/calendar/api/v3/reference/events)

## Quotas and Rate Limits

The Google Calendar API enforces quotas and rate limits on requests. Exceeding these limits will result in HTTP 403 or 429 errors. For details on usage limits and how to request increases, refer to the [Google Calendar API Quotas](https://developers.google.com/workspace/calendar/api/guides/quota).
