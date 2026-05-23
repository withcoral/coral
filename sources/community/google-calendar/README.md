# Google Calendar

Query calendar metadata and events from Google Calendar API v3. Supports listing accessible calendars, fetching events with date-range filters, and pagination.

## Authentication

This source requires a Google OAuth 2.0 access token with the `https://www.googleapis.com/auth/calendar.readonly` scope.

For local development or testing, you can use the Google Cloud CLI to generate a short-lived token:
```bash
gcloud auth application-default login \
  --scopes=https://www.googleapis.com/auth/calendar.readonly

export GOOGLE_CALENDAR_TOKEN=$(gcloud auth print-access-token)
```
*Note: Tokens generated via `gcloud` expire after approximately 60 minutes.*

For production use, you must configure a Google Cloud project with OAuth 2.0 credentials and implement a token refresh flow. See the [Google Calendar Auth Guide](https://developers.google.com/calendar/api/guides/auth).

## Installation

```bash
coral source add --file sources/google-calendar/manifest.yaml
```

## Tables

### `calendars`
Lists all calendars (owned, subscribed, shared) visible to the authenticated user.

| Column | Type | Description |
|---|---|---|
| `id` | string | Calendar identifier. Use this as the `calendar_id` filter when querying the `events` table. |
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
| `start_date_time` | string | ISO 8601 start datetime with timezone offset (null for all-day events). |
| `start_date` | string | All-day event start date `YYYY-MM-DD` (null for timed events). |
| `end_date_time` | string | ISO 8601 end datetime with timezone offset (null for all-day events). |
| `end_date` | string | All-day event end date `YYYY-MM-DD` (null for timed events). |
| `start_time_zone` | string | IANA time zone of the event start. |
| `calendar_id` | string | Virtual column exposing the `calendar_id` filter value. |
| `q` | string | Virtual column exposing the `q` (free-text search query) filter value. |
| `time_min` | string | Virtual column exposing the `time_min` (RFC 3339 lower bound) filter value. |
| `time_max` | string | Virtual column exposing the `time_max` (RFC 3339 upper bound) filter value. |
| `organizer_email` | string | Email of the event organizer. |
| `organizer_self` | boolean | `true` if the organizer is the authenticated user. |
| `status` | string | Event status (`confirmed`, `tentative`, or `cancelled`). |
| `html_link` | string | URL to open the event in the Google Calendar web UI. |
| `location` | string | Location text or Google Meet link. |
| `created` | string | Creation timestamp (RFC 3339). |
| `updated` | string | Last modification timestamp (RFC 3339). |
| `creator_email` | string | Email of the user who created the event. |
| `event_type` | string | Type of event (`default`, `outOfOffice`, `focusTime`, `workingLocation`). |
| `recurring_event_id` | string | ID of the master recurring event, if applicable. |
| `ical_uid` | string | RFC 5545 iCalendar UID. |
| `hangout_link` | string | Attached Google Meet link. |
| `visibility` | string | Event visibility (`default`, `public`, `private`, `confidential`). |
| `transparency` | string | Schedule blocking (`opaque` for busy, `transparent` for free). |

## Push-down Filters

The `events` table supports pushing down filters directly to the Google Calendar API:
- `calendar_id` (required)
- `time_min` (RFC 3339 lower bound, inclusive)
- `time_max` (RFC 3339 upper bound, exclusive)
- `q` (free-text search)

## Example Queries

```sql
-- List accessible calendars
SELECT id, summary, access_role
FROM gcal.calendars;

-- Upcoming events this week on the primary calendar
SELECT summary, start_date_time, end_date_time
FROM gcal.events
WHERE calendar_id = 'primary'
  AND time_min = '2026-05-25T00:00:00Z'
  AND time_max = '2026-06-01T00:00:00Z'
ORDER BY start_date_time;

-- Keyword search
SELECT summary, start_date_time, location
FROM gcal.events
WHERE calendar_id = 'primary'
  AND q = 'standup';

-- Identify all-day events
SELECT summary, start_date, end_date
FROM gcal.events
WHERE calendar_id = 'primary'
  AND start_date IS NOT NULL;
```
