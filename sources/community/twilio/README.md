# Twilio

**Version:** 0.1.0
**Backend:** HTTP
**Tables:** 12
**Base URL:** `https://api.twilio.com`

Query messages, calls, phone numbers, recordings, conferences, queues,
applications, caller IDs, accounts, and usage records from Twilio.

## Authentication

Requires:

- `TWILIO_ACCOUNT_SID`: Account SID to query, usually starting with `AC`
- `TWILIO_API_KEY`: API key SID, usually starting with `SK`
- `TWILIO_API_KEY_SECRET`: API key secret

Twilio recommends API keys for production. For local testing, you can set
`TWILIO_API_KEY` to your Account SID and `TWILIO_API_KEY_SECRET` to your Auth
Token. Prefer a Restricted API key with only the read/list permissions for the
tables you plan to query.

```bash
TWILIO_ACCOUNT_SID=AC... \
TWILIO_API_KEY=SK... \
TWILIO_API_KEY_SECRET=... \
coral source add --file sources/community/twilio/manifest.yaml
```

Run from the repo root.

## Tables

| Table | Description | Pagination |
|---|---|---|
| `accounts` | Main account and subaccounts visible to the credential | Page |
| `messages` | SMS, MMS, WhatsApp, and channel message records | Page |
| `calls` | Programmable Voice call records | Page |
| `incoming_phone_numbers` | Provisioned, ported, or hosted Twilio numbers | Page |
| `recordings` | Voice call, conference, and SIP Trunk recording metadata | Page |
| `conferences` | Programmable Voice conference records | Page |
| `queues` | Voice queues and live queue size snapshots | Page |
| `applications` | TwiML application callback configuration | Page |
| `outgoing_caller_ids` | Verified outbound caller IDs | Page |
| `usage_records` | All-time or date-filtered usage totals | Page |
| `usage_records_daily` | Daily usage records by category | Page |
| `usage_records_monthly` | Monthly usage records by category | Page |

## Quick Start

```bash
# Confirm messaging access
coral sql "
  SELECT sid, direction, status, from_number, to_number, date_sent
  FROM twilio.messages
  LIMIT 10
"

# Recent failed or undelivered messages
coral sql "
  SELECT sid, to_number, status, error_code, error_message, date_sent
  FROM twilio.messages
  WHERE status IN ('failed', 'undelivered')
  LIMIT 50
"

# Recent calls with duration and cost fields
coral sql "
  SELECT sid, from_number, to_number, status, direction, duration, price, start_time
  FROM twilio.calls
  LIMIT 25
"

# Inventory active Twilio phone numbers and webhook URLs
coral sql "
  SELECT sid, phone_number, friendly_name, voice_url, sms_url
  FROM twilio.incoming_phone_numbers
  ORDER BY phone_number
"

# Usage by category for the current account
coral sql "
  SELECT category, description, usage, usage_unit, count, count_unit, price, price_unit
  FROM twilio.usage_records
  ORDER BY category
"

# Daily SMS usage for a bounded date range
coral sql "
  SELECT start_date, end_date, category, usage, usage_unit, count, count_unit, price
  FROM twilio.usage_records_daily
  WHERE category = 'sms'
    AND start_date = '2026-05-01'
    AND end_date = '2026-05-15'
  ORDER BY start_date
"

# Recording metadata for a call
coral sql "
  SELECT sid, call_sid, status, duration, channels, start_time, media_url
  FROM twilio.recordings
  WHERE call_sid = 'CAxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
"
```

## Discovery Order

```text
incoming_phone_numbers
  -> phone_number (filter messages/calls by to_number/from_number)
  -> sms_application_sid -> applications.sid
  -> voice_application_sid -> applications.sid

messages
  -> sid (message identifier)
  -> messaging_service_sid (join mentally to Messaging Service APIs, not exposed here)

calls
  -> sid -> recordings.call_sid
  -> sid -> conferences subresources outside this source
  -> parent_call_sid (call leg hierarchy)

conferences
  -> sid -> recordings.conference_sid

recordings
  -> call_sid -> calls.sid
  -> conference_sid -> conferences.sid

usage_records
  -> category (reuse in usage_records_daily/monthly)

applications
  -> sid (referenced by incoming_phone_numbers.voice_application_sid and sms_application_sid)
```

## Notes

- Twilio list responses include `next_page_uri`, but this source uses Twilio's
  numbered `Page` and `PageSize` parameters because Coral's source spec follows
  query-parameter pagination today.
- Twilio date fields in the classic API are RFC 2822 strings, so they are
  exposed as `Utf8` instead of `Timestamp`.
- Message bodies, phone numbers, webhook URLs, recordings, and call metadata can
  contain PII. Scope API keys and SQL filters accordingly.
- The `accounts` table may require Account SID/Auth Token or a key type with
  account read access; Standard API keys do not cover every Account API.
