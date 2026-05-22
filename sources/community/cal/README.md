# Cal.com

**Version:** 0.1.0
**Backend:** HTTP
**Tables:** 4
**Base URL:** `https://api.cal.com` (override with `CAL_BASE_URL`)

Query bookings, event types, schedules, and profile data from Cal.com
(Cloud or self-hosted).

## Authentication

Requires a `CAL_API_KEY`. Find it in **Settings → Security → API Keys**.

- Test mode keys have the prefix `cal_test_`
- Live mode keys have the prefix `cal_live_`

```bash
CAL_API_KEY=cal_live_... coral source add --file sources/community/cal/manifest.yaml
```

Run from the repo root. Or interactively:

```bash
CAL_API_KEY=cal_live_... coral source add --file sources/community/cal/manifest.yaml --interactive
```

### Self-hosted

Set `CAL_BASE_URL` to your instance URL:

```bash
CAL_API_KEY=cal_test_... CAL_BASE_URL=https://cal.example.com \
  coral source add --file sources/community/cal/manifest.yaml
```

## Tables

| Table | Description | Optional filters |
|---|---|---|
| `me` | Profile of the authenticated user | — |
| `event_types` | Bookable meeting configurations | — |
| `bookings` | Scheduled meetings and their status | `status`, `attendee_email`, `attendee_name`, `event_type_id`, `after_start`, `before_end`, `after_created_at`, `before_created_at` |
| `schedules` | Availability schedules | — |

### Bookings status filter note

The `status` filter and the `status` response column use different vocabularies:

| | Values |
|---|---|
| Filter (`WHERE status = '...'`) | `upcoming`, `recurring`, `past`, `cancelled`, `unconfirmed` |
| Response column | `accepted`, `cancelled`, `pending`, `rejected` |

The filter accepts **at most one value** per query — passing multiple values returns 400.

## Quick start

```bash
# Confirm connectivity and see your profile
coral sql "SELECT id, username, email, name, time_zone FROM cal.me"

# List all event types
coral sql "
  SELECT id, title, slug, length_in_minutes, hidden, booking_url
  FROM cal.event_types
  ORDER BY length_in_minutes
"

# Recent bookings
coral sql "
  SELECT id, uid, status, start, end_time, duration, host_name, attendee_email
  FROM cal.bookings
  ORDER BY start DESC
  LIMIT 20
"

# Bookings in the last 7 days
coral sql "
  SELECT uid, title, status, start, host_name, attendee_email
  FROM cal.bookings
  WHERE after_start = '2026-05-06T00:00:00Z'
    AND before_end = '2026-05-13T00:00:00Z'
  ORDER BY start DESC
"

# Cancelled bookings
coral sql "
  SELECT uid, title, start, cancellation_reason, cancelled_by_email
  FROM cal.bookings
  WHERE status = 'cancelled'
  ORDER BY start DESC
  LIMIT 20
"

# Bookings for a specific event type
coral sql "
  SELECT uid, title, start, status, attendee_email
  FROM cal.bookings
  WHERE event_type_id = 12345
  ORDER BY start DESC
"

# Booking volume by event type slug
coral sql "
  SELECT event_type_slug, status, COUNT(*) as count
  FROM cal.bookings
  GROUP BY event_type_slug, status
  ORDER BY count DESC
"

# Availability schedules
coral sql "SELECT id, name, time_zone, is_default FROM cal.schedules"
```

## Discovery order

```text
me
  → default_schedule_id → schedules.id

event_types
  → id (event_type_id)
    → bookings (WHERE event_type_id = '...')
  → schedule_id → schedules.id

bookings
  → event_type_id → event_types.id
```
