# Twilio Source Spec

**Version:** 0.1.0
**Backend:** HTTP
**Tables:** 15
**Base URL:** `https://api.twilio.com/2010-04-01/Accounts/{AccountSid}`

Query your entire Twilio communications infrastructure via SQL — SMS/MMS/WhatsApp messages, voice calls, phone number inventory, call recordings, transcriptions, conferences, call queues, usage & billing records (aggregate/daily/monthly), TwiML applications, debugging alerts, notifications, and message delivery feedback. Turn your communications data into a queryable analytics layer for delivery rate monitoring, cost tracking, error debugging, capacity planning, and compliance auditing.

## Authentication

Requires two inputs:

| Input | Kind | Description |
|---|---|---|
| `TWILIO_ACCOUNT_SID` | variable | Your Account SID (starts with "AC"). Found on the [Twilio Console Dashboard](https://console.twilio.com). |
| `TWILIO_AUTH_TOKEN` | secret | Your Auth Token. Found on the Console Dashboard next to the Account SID. |

Authentication uses HTTP Basic Auth (`Account SID` as username, `Auth Token` as password) per the [Twilio API authentication docs](https://www.twilio.com/docs/iam/api-keys).

## Available Tables

### Messaging

| Table | Required Filter | Description |
|---|---|---|
| `messages` | None | All SMS, MMS, and WhatsApp messages — body, from/to, status, direction, price, segments, error codes. Filterable by phone number and date range. |

### Voice

| Table | Required Filter | Description |
|---|---|---|
| `calls` | None | All voice calls — from/to, status, duration, direction, price, queue time, caller ID, forwarding. Filterable by phone number, status, and date range. |
| `recordings` | None | Call recordings — duration, channels, status, encryption, source. Filterable by call SID and date. |
| `transcriptions` | None | Speech-to-text transcriptions of recordings — full text, status, duration, price. |
| `conferences` | None | Multi-party conference calls — friendly name, status, region, end reason. Filterable by status and date. |
| `queues` | None | Call queues — current size, max size, average wait time. Real-time capacity metrics. |

### Billing & Usage

| Table | Required Filter | Description |
|---|---|---|
| `usage_records` | None | Aggregate usage — category, quantity, event count, cost. Filterable by category and date range. |
| `usage_records_daily` | None | Daily usage breakdown by category for trend analysis and anomaly detection. |
| `usage_records_monthly` | None | Monthly usage for billing reports, budget tracking, and month-over-month comparisons. |

### Infrastructure & Configuration

| Table | Required Filter | Description |
|---|---|---|
| `account` | None | Singleton — the authenticated Twilio account details (SID, type, status). |
| `phone_numbers` | None | Provisioned number inventory — E.164 number, capabilities (SMS/voice/MMS/fax), webhook URLs, emergency address. |
| `applications` | None | TwiML Applications — reusable webhook configurations for voice and SMS routing. |

## Quick Start

```bash
# Step 1 — add the source spec to your workspace
coral source add --file sources/community/twilio/manifest.yaml --interactive
# You will be prompted for TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN
```

## Example Queries

### Message Analytics

```sql
-- Recent messages with delivery status
SELECT sid, body, from_number, to_number, status, direction, price, date_sent
FROM twilio.messages
LIMIT 25;

-- Failed message analysis with error codes
SELECT sid, from_number, to_number, error_code, error_message, date_sent
FROM twilio.messages
WHERE status = 'failed' OR status = 'undelivered';

-- Messages in a date range
SELECT sid, body, status, direction, price
FROM twilio.messages
WHERE date_sent_after = '2024-01-01' AND date_sent_before = '2024-01-31';

-- Message volume by direction
SELECT direction, COUNT(*) AS message_count
FROM twilio.messages
GROUP BY direction;

-- Messages to a specific number
SELECT sid, body, status, date_sent
FROM twilio.messages
WHERE to = '+14155551234';
```

### Voice Call Analytics

```sql
-- Recent calls with duration and cost
SELECT sid, from_number, to_number, status, duration, direction, price, start_time
FROM twilio.calls
LIMIT 25;

-- Failed calls analysis
SELECT sid, from_number, to_number, status, direction, start_time
FROM twilio.calls
WHERE status IN ('failed', 'busy', 'no-answer');

-- Average call duration by direction
SELECT direction, AVG(CAST(duration AS INTEGER)) AS avg_duration_secs
FROM twilio.calls
WHERE status = 'completed'
GROUP BY direction;

-- Calls with answering machine detection
SELECT sid, from_number, to_number, answered_by, duration
FROM twilio.calls
WHERE answered_by IS NOT NULL;
```

### Cost & Billing

```sql
-- Total spend by category (current period)
SELECT category, description, price, price_unit, usage, usage_unit
FROM twilio.usage_records
ORDER BY CAST(price AS FLOAT) DESC;

-- Daily SMS cost trend
SELECT start_date, category, price, count, usage
FROM twilio.usage_records_daily
WHERE category = 'sms';

-- Monthly billing summary
SELECT start_date, end_date, category, price, usage, count
FROM twilio.usage_records_monthly
ORDER BY start_date DESC;

-- Cost comparison across communication channels
SELECT category, price, usage_unit
FROM twilio.usage_records
WHERE category IN ('calls', 'sms', 'mms', 'recordings', 'transcriptions');
```

### Phone Number Inventory

```sql
-- All provisioned numbers with capabilities
SELECT phone_number, friendly_name, sms_enabled, voice_enabled,
       mms_enabled, status
FROM twilio.phone_numbers;

-- Numbers without emergency address (compliance risk)
SELECT phone_number, friendly_name, emergency_address_sid, emergency_status
FROM twilio.phone_numbers
WHERE emergency_address_sid IS NULL;

-- Numbers with webhook configuration
SELECT phone_number, voice_url, sms_url, voice_application_sid
FROM twilio.phone_numbers
WHERE voice_url IS NOT NULL OR sms_url IS NOT NULL;
```

### Recording & Transcription Analysis

```sql
-- Recent recordings with call context
SELECT r.sid, r.call_sid, r.duration, r.status, r.channels, r.price
FROM twilio.recordings r
LIMIT 25;

-- Transcription text search
SELECT sid, recording_sid, transcription_text, status, duration
FROM twilio.transcriptions
WHERE status = 'completed';
```

### Conference & Queue Monitoring

```sql
-- Active and recent conferences
SELECT sid, friendly_name, status, region, reason_conference_ended
FROM twilio.conferences
LIMIT 25;

-- Queue capacity monitoring
SELECT friendly_name, current_size, max_size, average_wait_time
FROM twilio.queues;
```

### Multi-Table Joins

```sql
-- Messages with phone number details
SELECT m.sid, m.body, m.from_number, m.status, m.price,
       p.friendly_name AS number_label
FROM twilio.messages m
JOIN twilio.phone_numbers p ON m.from_number = p.phone_number;

-- Recordings with call details
SELECT r.sid AS recording_sid, r.duration AS rec_duration,
       c.from_number, c.to_number, c.duration AS call_duration
FROM twilio.recordings r
JOIN twilio.calls c ON r.call_sid = c.sid;
```

## Rate Limits

Twilio enforces per-endpoint rate limits. This source spec uses the `next_page_uri` pagination pattern which Twilio handles natively. General limits:

| Resource | Rate Limit |
|---|---|
| Read operations | 100 requests/second |
| List operations | Varies by endpoint |

Use `LIMIT` clauses on large tables (`messages`, `calls`, `recordings`) and date range filters to avoid excessive API calls.

## Pagination

All list endpoints use offset-based pagination via the `Page` and `PageSize` query parameters. Default page size is 50; maximum is 1,000.

## Join Reference

Key relationships between tables:

```
calls.phone_number_sid           → phone_numbers.sid
calls.parent_call_sid            → calls.sid (self-referencing)
recordings.call_sid              → calls.sid
recordings.conference_sid        → conferences.sid
transcriptions.recording_sid     → recordings.sid
phone_numbers.voice_application_sid → applications.sid
phone_numbers.sms_application_sid   → applications.sid
```

## Twilio Error Code Reference

Common error codes you'll encounter in `messages.error_code` and `alerts.error_code`:

| Code | Description |
|---|---|
| 30001 | Queue overflow — message rate exceeded |
| 30003 | Unreachable destination handset |
| 30005 | Unknown destination handset |
| 30006 | Landline or unreachable carrier |
| 30007 | Carrier violation (content filtering) |
| 30008 | Unknown error |
| 11200 | HTTP retrieval failure (webhook down) |
| 11205 | HTTP connection failure |
| 12100 | Document parse failure (bad TwiML) |
| 12200 | Schema validation warning |
| 21211 | Invalid 'To' phone number |
| 21610 | Message blocked (STOP received) |

Full reference: https://www.twilio.com/docs/api/errors
