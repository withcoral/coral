# Twilio

**Version:** 0.1.0
**Backend:** HTTP
**Tables:** 3

Query SMS messages, voice calls, and account details from Twilio. Monitor communication history, delivery status, and usage through SQL.

## Installation

Install the source via the CLI:

```bash
coral source add --file sources/community/twilio/manifest.yaml
```

## Credentials

To use this source, you need your Twilio Account SID and Auth Token.

1. Log in to the [Twilio Console](https://console.twilio.com).
2. Copy your **Account SID** (starts with `AC`) and **Auth Token** from the dashboard.
3. Base64-encode `AccountSID:AuthToken` to create the Basic auth token:

```bash
printf '%s:%s' YOUR_ACCOUNT_SID YOUR_AUTH_TOKEN | base64
```

4. Provide all values as environment variables or when prompted by `coral source add`:

```bash
export TWILIO_ACCOUNT_SID="ACxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
export TWILIO_BASIC_AUTH="QUNiYjdhMjdmMTI0MmE5Mzk4N2U1Y2YwMTM5..."
```

## Quick Start

```sql
-- Check account info
SELECT sid, status, type, friendly_name
FROM twilio.account;

-- List recent messages
SELECT sid, direction, "from", "to", body, status
FROM twilio.messages
LIMIT 10;

-- List voice calls
SELECT sid, "from", "to", status, duration, price
FROM twilio.calls
LIMIT 10;

-- Filter messages by date
SELECT sid, "from", "to", body, status
FROM twilio.messages
WHERE date_sent_after = '2026-01-01'
LIMIT 10;

-- Find failed calls
SELECT sid, "from", "to", status, direction
FROM twilio.calls
WHERE status = 'failed';
```

## Tables

### `account`

Twilio account details including status, type, and friendly name. Returns exactly one row.

**Columns**

| Column | Type | Description |
|--------|------|-------------|
| `sid` | Utf8 | Account SID |
| `friendly_name` | Utf8 | Friendly name of the account |
| `status` | Utf8 | Account status (active, suspended, closed) |
| `type` | Utf8 | Account type (Trial, Full) |
| `date_created` | Utf8 | When the account was created |
| `date_updated` | Utf8 | When the account was last updated |

---

### `messages`

SMS and MMS messages sent and received through Twilio.

**Filters**

| Filter | Type | Required | Description |
|--------|------|----------|-------------|
| `date_sent_after` | Utf8 | | Only messages sent after this date (YYYY-MM-DD) |
| `date_sent_before` | Utf8 | | Only messages sent before this date (YYYY-MM-DD) |
| `from_number` | Utf8 | | Filter by sender phone number |
| `to_number` | Utf8 | | Filter by recipient phone number |

**Columns**

| Column | Type | Description |
|--------|------|-------------|
| `sid` | Utf8 | Unique identifier for the message |
| `direction` | Utf8 | Direction (inbound, outbound-api, outbound-call, outbound-reply) |
| `from` | Utf8 | Sender phone number or short code |
| `to` | Utf8 | Recipient phone number |
| `body` | Utf8 | Text body of the message |
| `status` | Utf8 | Delivery status (queued, sending, sent, delivered, undelivered, failed, received) |
| `price` | Utf8 | Cost of the message as a string (e.g. -0.08320) |
| `price_unit` | Utf8 | Currency (e.g. USD) |
| `num_segments` | Utf8 | Number of message segments |
| `error_code` | Utf8 | Error code if the message failed |
| `error_message` | Utf8 | Error message if the message failed |
| `date_created` | Utf8 | When the message was created |
| `date_sent` | Utf8 | When the message was sent |

---

### `calls`

Voice calls made and received through Twilio.

**Filters**

| Filter | Type | Required | Description |
|--------|------|----------|-------------|
| `start_time_after` | Utf8 | | Only calls started after this date (YYYY-MM-DD) |
| `start_time_before` | Utf8 | | Only calls started before this date (YYYY-MM-DD) |
| `from_number` | Utf8 | | Filter by caller phone number |
| `to_number` | Utf8 | | Filter by recipient phone number |
| `status` | Utf8 | | Filter by call status |

**Columns**

| Column | Type | Description |
|--------|------|-------------|
| `sid` | Utf8 | Unique identifier for the call |
| `from` | Utf8 | Caller phone number |
| `to` | Utf8 | Recipient phone number |
| `status` | Utf8 | Call status (queued, ringing, in-progress, completed, busy, failed, no-answer, canceled) |
| `direction` | Utf8 | Direction (inbound, outbound-api, outbound-dial) |
| `duration` | Int64 | Duration of the call in seconds |
| `price` | Utf8 | Cost of the call as a string (e.g. -0.03000) |
| `price_unit` | Utf8 | Currency (e.g. USD) |
| `start_time` | Utf8 | When the call started |
| `end_time` | Utf8 | When the call ended |
| `date_created` | Utf8 | When the call record was created |

## Source scope

- Targets the Twilio REST API at `https://api.twilio.com/2010-04-01/Accounts/{AccountSid}`.
- Requires `TWILIO_ACCOUNT_SID` (URL path variable) and `TWILIO_BASIC_AUTH` (HTTP Basic auth, base64-encoded `SID:Token`).
- `messages` supports date range and phone number filters pushed to the API.
- `calls` supports date range, phone number, and status filters pushed to the API.
- SQL `LIMIT` is pushed to the API via `PageSize` query param (default 50, max 1000).
- Twilio timestamps are RFC 2822 format strings (e.g. `Sun, 23 Nov 2025 02:47:12 +0000`), kept as `Utf8`.
- 1 declared test query (`account`) requires no filters.
- Provides read-only access. Sending messages, making calls, and other write operations are out of scope.

## Limitations

- The source provides read-only list access only. Sending SMS, making calls, and managing phone numbers are out of scope.
- Pagination uses Twilio's `PageToken` URL-based cursors. This source uses `mode: none` with `PageSize` (max 1000), so a single query returns at most 1000 items.
- Twilio timestamps are RFC 2822 strings (not ISO 8601), kept as `Utf8` since Coral's `format_timestamp/iso8601` does not parse RFC 2822.
- The `price` column is `Utf8` — Twilio returns prices as strings (e.g. `"-0.08320"`). Cast with `CAST(price AS DOUBLE)` for aggregation.
- The `incoming_phone_numbers` endpoint is not modeled (trial accounts may have no numbers).
- Date filters use Twilio's `DateSent>` / `StartTime>` syntax mapped to friendly filter names.

## Provider docs

- Twilio REST API: https://www.twilio.com/docs/usage/api
- Messages API: https://www.twilio.com/docs/messaging/api/message-resource
- Calls API: https://www.twilio.com/docs/voice/api/call-resource
- Console: https://console.twilio.com

## Live validation output

Validated against a live Twilio Trial account with a valid `TWILIO_ACCOUNT_SID` and `TWILIO_BASIC_AUTH`.

```bash
$ coral source lint sources/community/twilio/manifest.yaml
Manifest is valid
```

```bash
$ coral source add --file sources/community/twilio/manifest.yaml
Added source twilio

  ✓ twilio connected successfully

    twilio (3 tables)
    ├─ account
    ├─ calls
    └─ messages
    Query tests
    1 declared · 1 passed · 0 failed

    ✓ SELECT sid, status, friendly_name FROM twilio.account
      1 row
```

**Live account proof:**

```sql
SELECT sid, status, type, friendly_name FROM twilio.account;
```

```text
+--------------------------------------+--------+-------+-------------------------+
| sid                                  | status | type  | friendly_name           |
+--------------------------------------+--------+-------+-------------------------+
| AC00000000000000000000000000000000c7 | active | Trial | My first Twilio account |
+--------------------------------------+--------+-------+-------------------------+
```

**Live messages proof:**

```sql
SELECT sid, direction, status, body FROM twilio.messages LIMIT 3;
```

```text
+--------------------------------------+--------------+-----------+-------------------------------------------------------+
| sid                                  | direction    | status    | body                                                  |
+--------------------------------------+--------------+-----------+-------------------------------------------------------+
| SM00000000000000000000000000000000e3 | outbound-api | delivered | Sent from your Twilio trial account - this is user     |
| SM00000000000000000000000000000000ea | outbound-api | delivered | Sent from your Twilio trial account - this is user     |
| MM00000000000000000000000000000000a7 | outbound-api | delivered | Sent from your Twilio trial account - Hi user, great.. |
+--------------------------------------+--------------+-----------+-------------------------------------------------------+
```

**Live calls proof:**

```sql
SELECT sid, status, direction, duration FROM twilio.calls LIMIT 3;
```

```text
+--------------------------------------+--------+-----------+----------+
| sid                                  | status | direction | duration |
+--------------------------------------+--------+-----------+----------+
| CA00000000000000000000000000000000a1 | failed | inbound   | 0        |
+--------------------------------------+--------+-----------+----------+
```
