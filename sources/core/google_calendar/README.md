# google_calendar

Query Google Calendar events, calendars, settings, and color palettes using SQL.

Connects to the [Google Calendar API v3](https://developers.google.com/calendar/api/v3/reference) via OAuth2. Supports filtering by date range, full-text search, recurring events, and multi-calendar queries.

---

## Quick start

```bash
# Register the source using Coral's guided OAuth flow
coral source add google_calendar

# Or paste an existing access token
GOOGLE_CALENDAR_ACCESS_TOKEN=<your-token> coral source add google_calendar

# List your calendars
coral sql "SELECT id, summary, time_zone FROM google_calendar.calendars"

# Upcoming events on your primary calendar
coral sql "SELECT summary, start_date_time FROM google_calendar.events WHERE time_min = '2026-01-01T00:00:00Z' LIMIT 10"
```

---

## Authentication

The source requires a Google OAuth2 access token with the `calendar.readonly` scope.

**Option A — Guided OAuth flow (recommended)**

```bash
coral source add google_calendar
```

Coral will open a browser window and walk through the Google OAuth flow automatically. You will need:
- A Google Cloud project with the **Google Calendar API** enabled
- An **OAuth 2.0 Client ID** of type **Desktop app**
- The `GOOGLE_CALENDAR_OAUTH_CLIENT_ID` and `GOOGLE_CALENDAR_OAUTH_CLIENT_SECRET` env vars set

**Option B — Paste an existing access token**

If you already have an OAuth token with `calendar.readonly` scope:

```bash
GOOGLE_CALENDAR_ACCESS_TOKEN=ya29.xxx coral source add google_calendar
```

**Option C — Google OAuth Playground (testing)**

1. Visit [developers.google.com/oauthplayground](https://developers.google.com/oauthplayground)
2. Select **Google Calendar API v3** → `calendar.readonly`
3. Authorize and copy the access token
4. `GOOGLE_CALENDAR_ACCESS_TOKEN=<token> coral source add google_calendar`

> Access tokens expire after 1 hour. Re-register the source with a fresh token, or use the guided flow which handles refresh automatically.

---

## Tables

### `google_calendar.calendars`

All calendars on the authenticated user's calendar list. Start here to discover calendar IDs.

| Column | Type | Description |
|---|---|---|
| `id` | text | Calendar identifier |
| `events_calendar_id` | text | URL-safe calendar ID for use in the `events` table |
| `summary` | text | Calendar title |
| `description` | text | Calendar description |
| `location` | text | Geographic location |
| `time_zone` | text | Calendar time zone |
| `summary_override` | text | User's custom title override |
| `color_id` | text | Color ID (join to `calendar_colors`) |
| `background_color` | text | Background hex color |
| `foreground_color` | text | Foreground hex color |
| `access_role` | text | User's access role: `owner`, `writer`, `reader`, `freeBusyReader` |
| `selected` | boolean | Whether this calendar appears in the Google Calendar UI |
| `hidden` | boolean | Whether this calendar is hidden |
| `primary` | boolean | Whether this is the user's primary calendar |
| `deleted` | boolean | Whether this calendar list entry has been deleted |
| `default_reminders` | json | Default reminders for this calendar |
| `notification_settings` | json | Notification settings |
| `conference_solution_types` | text | Comma-separated allowed conference solution types |
| `raw` | json | Full raw API response for this calendar |

**Filters:** `min_access_role`, `show_deleted`, `show_hidden`

### `google_calendar.events`

Events on a Google Calendar. Defaults to the primary calendar; pass `calendar_id` to query another.

| Column | Type | Description |
|---|---|---|
| `calendar_id` | text | Calendar ID queried (or `primary`) |
| `id` | text | Event ID |
| `status` | text | `confirmed`, `tentative`, or `cancelled` |
| `html_link` | text | Web link to open this event in Google Calendar |
| `created` | timestamp | Event creation time |
| `updated` | timestamp | Last update time |
| `summary` | text | Event title |
| `description` | text | Event description |
| `location` | text | Event location |
| `color_id` | text | Event color ID (join to `event_colors`) |
| `creator__email` | text | Creator's email |
| `creator__display_name` | text | Creator's display name |
| `organizer__email` | text | Organizer's email |
| `organizer__display_name` | text | Organizer's display name |
| `start_date_time` | timestamp | Timed event start (null for all-day events) |
| `start_date` | text | All-day event start date (null for timed events) |
| `start_time_zone` | text | Start time zone |
| `end_date_time` | timestamp | Timed event end |
| `end_date` | text | All-day event end date |
| `end_time_zone` | text | End time zone |
| `recurring_event_id` | text | Parent recurring event ID (for instances) |
| `original_start_date_time` | timestamp | Original start for modified recurring instances |
| `original_start_date` | text | Original all-day start for modified recurring instances |
| `i_cal_uid` | text | iCalendar UID |
| `event_type` | text | Event type |
| `transparency` | text | `opaque` (blocks time) or `transparent` (free) |
| `visibility` | text | `default`, `public`, `private`, or `confidential` |
| `hangout_link` | text | Google Meet or Hangouts link |
| `attendees_emails` | text | Comma-separated attendee emails |
| `attendees` | json | Full attendee objects |
| `recurrence` | json | RRULE / EXRULE recurrence rules |
| `reminders` | json | Reminder configuration |
| `conference_data` | json | Conference data (Meet, Zoom, etc.) |
| `source__title` | text | Source title for externally created events |
| `source__url` | text | Source URL for externally created events |
| `raw` | json | Full raw event object |

**Filters:** `calendar_id`, `time_min`, `time_max`, `updated_min`, `q` (search), `i_cal_uid`, `event_type`, `order_by`, `time_zone`, `show_deleted`, `show_hidden_invitations`, `single_events`

> Pass `events_calendar_id` from the `calendars` table, not the raw `id`. The `events_calendar_id` column URL-encodes `#` characters that appear in shared calendar IDs.

### `google_calendar.settings`

User-level Google Calendar settings (timezone, date format, default event duration, etc.).

| Column | Type | Description |
|---|---|---|
| `id` | text | Setting key (e.g., `timezone`, `dateFieldOrder`) |
| `value` | text | Setting value |
| `raw` | json | Full raw setting object |

### `google_calendar.calendar_colors`

Color palette for calendars. Join to `calendars.color_id`.

| Column | Type | Description |
|---|---|---|
| `id` | text | Color ID |
| `background` | text | Background hex color |
| `foreground` | text | Foreground hex color |
| `raw` | json | Full raw color definition |

### `google_calendar.event_colors`

Color palette for individual events. Join to `events.color_id`.

| Column | Type | Description |
|---|---|---|
| `id` | text | Color ID |
| `background` | text | Background hex color |
| `foreground` | text | Foreground hex color |
| `raw` | json | Full raw color definition |

---

## Example queries

**All your calendars**
```sql
SELECT summary, time_zone, access_role, primary
FROM google_calendar.calendars
```

**Upcoming events (next 7 days)**
```sql
SELECT summary, start_date_time, location
FROM google_calendar.events
WHERE time_min = '2026-05-29T00:00:00Z'
  AND time_max = '2026-06-05T00:00:00Z'
  AND single_events = true
ORDER BY start_date_time ASC
```

**Search events by keyword**
```sql
SELECT summary, start_date_time, organizer__email
FROM google_calendar.events
WHERE q = 'standup'
  AND time_min = '2026-01-01T00:00:00Z'
```

**Events from a specific calendar (not primary)**
```sql
-- Step 1: find the calendar_id
SELECT summary, events_calendar_id FROM google_calendar.calendars

-- Step 2: query events on that calendar
SELECT summary, start_date_time
FROM google_calendar.events
WHERE calendar_id = '<events_calendar_id from step 1>'
  AND time_min = '2026-05-01T00:00:00Z'
```

**All-day events this month**
```sql
SELECT summary, start_date, end_date
FROM google_calendar.events
WHERE time_min = '2026-05-01T00:00:00Z'
  AND time_max = '2026-06-01T00:00:00Z'
  AND single_events = true
  AND start_date IS NOT NULL
```

**Events with Google Meet links**
```sql
SELECT summary, start_date_time, hangout_link
FROM google_calendar.events
WHERE time_min = '2026-05-29T00:00:00Z'
  AND hangout_link IS NOT NULL
```

**Your timezone setting**
```sql
SELECT value AS timezone
FROM google_calendar.settings
WHERE id = 'timezone'
```

**Resolve calendar color names**
```sql
SELECT c.summary, cc.background, cc.foreground
FROM google_calendar.calendars c
JOIN google_calendar.calendar_colors cc ON c.color_id = cc.id
```

---

## Auth scope

| Scope | Required for |
|---|---|
| `https://www.googleapis.com/auth/calendar.readonly` | All tables |

---

## Rate limits

Google Calendar API enforces per-user per-project quotas.

| Quota | Limit |
|---|---|
| Requests per day | 1,000,000 |
| Requests per 100 seconds per user | 500 |
| Requests per 100 seconds | 500 |

Typical queries cost 1 API request per table read. Pagination (large event lists) may issue additional requests automatically.

See [Google Calendar API quotas](https://developers.google.com/calendar/api/guides/quota) for current limits.

---

## Notes

- **Primary calendar**: Omit `calendar_id` in the `events` filter to query the authenticated user's primary calendar.
- **Recurring events**: By default, recurring events return as a single event with `recurrence` rules. Set `single_events = true` to expand them into individual instances — required when using `order_by = 'startTime'`.
- **Timed vs all-day events**: Timed events populate `start_date_time`/`end_date_time`; all-day events populate `start_date`/`end_date`. Both are null for the other type.
- **Shared calendars**: Calendar IDs containing `#` (e.g., `group@resource.calendar.google.com#...`) must use `events_calendar_id` (URL-encoded) when querying events, not the raw `id`.
- **Cancelled events**: Use `show_deleted = true` to include cancelled events in results.

---

## Provider docs

- [Google Calendar API v3 Reference](https://developers.google.com/calendar/api/v3/reference)
- [Events resource](https://developers.google.com/calendar/api/v3/reference/events)
- [CalendarList resource](https://developers.google.com/calendar/api/v3/reference/calendarList)
- [Settings resource](https://developers.google.com/calendar/api/v3/reference/settings)
- [Colors resource](https://developers.google.com/calendar/api/v3/reference/colors)
- [OAuth 2.0 for Google APIs](https://developers.google.com/identity/protocols/oauth2)
