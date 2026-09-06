# Stream Video

**Version:** 0.1.0
**Backend:** HTTP
**Tables:** 4
**Base URL:** `https://video.stream-io-api.com/api/v2`

Query video calls, call details, and call members from Stream Video via the Stream Video API v2. Provides read-only access to video call data using a server JWT token.

## Installation

Install the source via the CLI:

```bash
coral source add --file sources/community/stream_video/manifest.yaml
```

## Credentials

To use this source, you need a Stream Video API key and a server JWT token.

1. Go to the [Stream Dashboard](https://dashboard.getstream.io/) and find your app.
2. Copy the **API key** and **API secret** from the dashboard.
3. Generate a server JWT token using the secret:

```bash
HEADER=$(echo -n '{"alg":"HS256","typ":"JWT"}' | openssl base64 -e -A | tr '+/' '-_' | tr -d '=')
PAYLOAD=$(echo -n '{"server":true}' | openssl base64 -e -A | tr '+/' '-_' | tr -d '=')
SECRET='<your-api-secret>'
SIGNATURE=$(echo -n "${HEADER}.${PAYLOAD}" | openssl dgst -sha256 -hmac "${SECRET}" -binary | openssl base64 -e -A | tr '+/' '-_' | tr -d '=')
echo "${HEADER}.${PAYLOAD}.${SIGNATURE}"
```

4. Provide both values when prompted by `coral source add` or set them as environment variables:

```bash
export STREAM_API_KEY="your-api-key"
export STREAM_SERVER_TOKEN="your-server-jwt"
```

## Quick Start

```sql
-- Verify connectivity and get app metadata
SELECT id, name, organization, placement
FROM stream_video.app;

-- List recent calls
SELECT id, call_type, cid, created_by_id, created_at, backstage
FROM stream_video.calls
LIMIT 5;

-- Get details for a specific call by type and ID
SELECT id, call_type, cid, created_by_id, created_at, backstage, recording
FROM stream_video.call
WHERE call_type = 'default'
  AND call_id = 'call_abc123';

-- List members of a call
SELECT call_type, call_id, user_id, role, created_at
FROM stream_video.call_members
WHERE call_type = 'default'
  AND call_id = 'call_abc123';
```

## Tables

### `app`

Get information about the Stream Video app. Returns a single row with the app's ID, name, organization, and placement region.

**Columns**

| Column | Type | Description |
|--------|------|-------------|
| `id` | Int64 | Stream app ID |
| `name` | Utf8 | App name |
| `organization` | Utf8 | Organization name |
| `placement` | Utf8 | Server placement region |

---

### `calls`

Query video calls with optional filters, sorting, and pagination. Returns calls ordered by creation date (newest first).

**Columns**

| Column | Type | Description |
|--------|------|-------------|
| `id` | Utf8 | Call ID |
| `call_type` | Utf8 | Call type (e.g. `default`, `livestream`, `audio_room`) |
| `cid` | Utf8 | Call CID (`type:id`) |
| `created_by_id` | Utf8 | User ID of the call creator |
| `created_by_name` | Utf8 | Display name of the call creator |
| `created_at` | Int64 | Unix nanoseconds when the call was created |
| `updated_at` | Int64 | Unix nanoseconds when the call was last updated |
| `starts_at` | Int64 | Unix nanoseconds of the scheduled start time |
| `ended_at` | Int64 | Unix nanoseconds when the call ended |
| `backstage` | Boolean | Whether the call is in backstage mode |
| `recording` | Boolean | Whether the call is being recorded |
| `transcribing` | Boolean | Whether the call is being transcribed |
| `current_session_id` | Utf8 | Session ID if the call has an active session (empty string = none) |
| `custom` | Json | Custom data JSON object associated with the call |
| `captioning` | Boolean | Whether closed captioning is active on the call |
| `translating` | Boolean | Whether translation is active on the call |
| `channel_cid` | Utf8 | Channel CID if linked to a Stream Chat channel |
| `team` | Utf8 | Team identifier for the call |

---

### `call`

Get a single video call by its call type and ID. Returns the full call object including settings, creator info, and status flags.

**Filters**

| Filter | Type | Required | Description |
|--------|------|----------|-------------|
| `call_type` | Utf8 | Yes | Call type (e.g. `default`) |
| `call_id` | Utf8 | Yes | Call ID |

**Columns**

| Column | Type | Description |
|--------|------|-------------|
| `id` | Utf8 | Call ID |
| `call_id` | Utf8 | Call ID (alias, used as required filter) |
| `call_type` | Utf8 | Call type (e.g. `default`, `livestream`, `audio_room`) |
| `cid` | Utf8 | Call CID (`type:id`) |
| `created_by_id` | Utf8 | User ID of the call creator |
| `created_by_name` | Utf8 | Display name of the call creator |
| `created_by_role` | Utf8 | Role of the call creator |
| `created_at` | Int64 | Unix nanoseconds when the call was created |
| `updated_at` | Int64 | Unix nanoseconds when the call was last updated |
| `starts_at` | Int64 | Unix nanoseconds of the scheduled start time |
| `ended_at` | Int64 | Unix nanoseconds when the call ended |
| `backstage` | Boolean | Whether the call is in backstage mode |
| `recording` | Boolean | Whether the call is being recorded |
| `transcribing` | Boolean | Whether the call is being transcribed |
| `custom` | Json | Custom data JSON object associated with the call |
| `captioning` | Boolean | Whether closed captioning is active on the call |
| `translating` | Boolean | Whether translation is active on the call |
| `channel_cid` | Utf8 | Channel CID if linked to a Stream Chat channel |
| `team` | Utf8 | Team identifier for the call |

---

### `call_members`

List members of a video call. Requires both `call_type` and `call_id` filters. Returns users who are members of the call with their roles.

**Filters**

| Filter | Type | Required | Description |
|--------|------|----------|-------------|
| `call_type` | Utf8 | Yes | Call type (e.g. `default`) |
| `call_id` | Utf8 | Yes | Call ID |

**Columns**

| Column | Type | Description |
|--------|------|-------------|
| `call_type` | Utf8 | Call type filter value |
| `call_id` | Utf8 | Call ID filter value |
| `user_id` | Utf8 | User ID of the member |
| `role` | Utf8 | Role of the member in the call |
| `name` | Utf8 | Display name of the member |
| `created_at` | Int64 | Unix nanoseconds when the member was added |
| `updated_at` | Int64 | Unix nanoseconds when the membership was last updated |
| `custom` | Json | Custom member data JSON object associated with the membership |
| `deleted_at` | Int64 | Unix nanoseconds when the membership was deleted |

## Live request costs

Each table query performs at least one live API call to `https://video.stream-io-api.com/api/v2`. Cursor-based pagination may trigger additional calls when `LIMIT` exceeds a single page's results. See the [Stream Video API reference](https://getstream.io/video/docs/api/) for rate limit details.

## Source scope

- Targets the Stream Video API v2 at `https://video.stream-io-api.com/api/v2`.
- Requires `STREAM_API_KEY` (query parameter) and `STREAM_SERVER_TOKEN` (JWT `Authorization` header) authentication.
- Covers read-only access: app metadata, call listing, single call details, and call member listing.
- Timestamps are Unix epoch nanoseconds (`Int64`) — the live API returns 19-digit nanosecond integers (e.g. `1773473105657621000`), not ISO 8601 strings. Coral's `format_timestamp` doesn't support nanosecond input, so these are exposed as `Int64` for arithmetic filtering. See evidence below.
- Automatic pagination via the API's cursor (`next`) mechanism.
- The `custom` column contains a JSON object with application-specific data.
- Column definitions are validated against the [official OpenAPI spec](https://github.com/GetStream/protocol/blob/main/openapi/v2/video-serverside-api.yaml). The spec documents timestamps as `type: number, format: date-time`, but the live API emits nanosecond integers.

## Limitations

- The source provides read-only access. Call creation, modification, and moderation are intentionally out of scope.
- The `call_members` table requires an active call with members to return rows — calls with zero members return an empty result set.
- Call types (`/video/calltypes`) and call participants (`/video/call/participants`) are not yet exposed; they require response format adjustments or an active call session respectively.
- Pagination cursor values are opaque strings — they cannot be constructed manually.

## Provider docs

- Stream Video API reference: https://getstream.io/video/docs/api/
- Stream Dashboard (API keys): https://dashboard.getstream.io/
- Server JWT tokens: https://getstream.io/video/docs/api/authentication/
- OpenAPI spec (Video v2): https://github.com/GetStream/protocol/blob/main/openapi/v2/video-serverside-api.yaml

## Live validation output

Validated against a live Stream Video app with a valid `STREAM_API_KEY` and `STREAM_SERVER_TOKEN`.

```bash
$ ./target/debug/coral source lint sources/community/stream_video/manifest.yaml
Manifest is valid
```

```bash
$ STREAM_API_KEY=... STREAM_SERVER_TOKEN=... ./target/debug/coral source add --file sources/community/stream_video/manifest.yaml
Added source stream_video (secrets: keychain)

  ✓ stream_video connected successfully
  Secrets: keychain

    stream_video (4 tables)
    ├─ app
    ├─ call
    ├─ call_members
    └─ calls
    Query tests
    2 declared · 2 passed · 0 failed

    ✓ SELECT id, name, organization, placement FROM stream_video.app
      1 row

    ✓ SELECT id, call_type, created_by_id, created_at, backstage, captioning, translating, team FROM stream_video.calls LIMIT 3
      3 rows
```

**Table introspection:**

```sql
SELECT table_name, description, required_filters
FROM coral.tables
WHERE schema_name = 'stream_video'
ORDER BY table_name;
```

```text
+--------------+----------------------------------------------------------------------------------------------------------------------------------------+-------------------+
| table_name   | description                                                                                                                            | required_filters  |
+--------------+----------------------------------------------------------------------------------------------------------------------------------------+-------------------+
| app          | Get information about the Stream Video app. Returns a single row with the app's ID, name, organization, and placement region.          |                   |
| call         | Get a single video call by its call type and ID. Returns the full call object including settings, creator info, and status flags.      | call_type,call_id |
| call_members | List members of a video call. Requires both call_type and call_id filters. Returns users who are members of the call with their roles. | call_type,call_id |
| calls        | Query video calls with optional filters, sorting, and pagination. Returns calls ordered by creation date (newest first).               |                   |
+--------------+----------------------------------------------------------------------------------------------------------------------------------------+-------------------+
```

**Inputs introspection:**

```sql
SELECT key, kind, required, is_set
FROM coral.inputs
WHERE schema_name = 'stream_video'
ORDER BY key;
```

```text
+---------------------+--------+----------+--------+
| key                 | kind   | required | is_set |
+---------------------+--------+----------+--------+
| STREAM_API_KEY      | secret | true     | true   |
| STREAM_SERVER_TOKEN | secret | true     | true   |
+---------------------+--------+----------+--------+
```

**Live app proof:**

```sql
SELECT id, name, organization, placement
FROM stream_video.app;
```

```text
+---------+---------+--------------+-----------+
| id      | name    | organization | placement |
+---------+---------+--------------+-----------+
| 1234567 | your-app | your-org      | ohio.c1   |
+---------+---------+--------------+-----------+
```

**Live calls proof:**

```sql
SELECT id, call_type, created_by_id, created_at, backstage, captioning, translating, team
FROM stream_video.calls
LIMIT 3;
```

```text
+----------------------------+-----------+----------------+---------------------+-----------+-------------+-------------+------+
| id                         | call_type | created_by_id  | created_at          | backstage | captioning | translating | team |
+----------------------------+-----------+----------------+---------------------+-----------+-------------+-------------+------+
| call_abc123 | default   | user_abc123 | 1773473105657621000 | false     | false       | false       |      |
| call_def456 | default   | user_abc123 | 1773472678601915000 | false     | false       | false       |      |
| call_ghi789 | default   | user_abc123 | 1773472575736554000 | false     | false       | false       |      |
+----------------------------+-----------+----------------+---------------------+-----------+-------------+-------------+------+
```



**Live call proof:**

```sql
SELECT id, call_type, cid, created_by_id, created_by_name, created_at, backstage, recording, captioning, translating
FROM stream_video.call
WHERE call_type = 'default'
  AND call_id = 'call_xyz789';
```

```text
+---------------+-----------+------------------------------------------+----------------+----------------+---------------------+-----------+-----------+-------------+-------------+
| id            | call_type | cid                                      | created_by_id  | created_by_name | created_at          | backstage | recording | captioning | translating |
+---------------+-----------+------------------------------------------+----------------+----------------+---------------------+-----------+-----------+-------------+-------------+
| call_xyz789   | default   | default:call_xyz789                      | user_abc123    | Alice           | 1773467506411120000 | false     | false     | false       | false       |
+---------------+-----------+------------------------------------------+----------------+----------------+---------------------+-----------+-----------+-------------+-------------+
```

**Live call_members proof:**

```sql
SELECT call_type, call_id, user_id, role, name, created_at, updated_at
FROM stream_video.call_members
WHERE call_type = 'default'
  AND call_id = 'call_xyz789';
```

```text
+-----------+-------------+--------------+------+-------+---------------------+---------------------+
| call_type | call_id     | user_id      | role | name  | created_at          | updated_at          |
+-----------+-------------+--------------+------+-------+---------------------+---------------------+
| default   | call_xyz789 | user_abc123  | host | Alice | 1780933352582581000 | 1780933352582581000 |
+-----------+-------------+--------------+------+-------+---------------------+---------------------+
```

## Timestamp evidence

The Stream Video OpenAPI spec documents timestamps as `type: number, format: date-time`, but the live API returns 19-digit Unix nanosecond integers. Raw API response from `GET /api/v2/video/call/default/call_abc123`:

```json
{
  "call": {
    "id": "call_abc123",
    "created_at": 1773473105657621000,
    "updated_at": 1773473105657621000,
    "starts_at": 1773473105657621000,
    "ended_at": null
  }
}
```

The value `1773473105657621000` is 19 digits:
- `1773473105657621000 / 1_000_000_000 = 1_773_473_105` → epoch year ~2026 ✓
- `1773473105657621000 / 1_000_000 = 1_773_473_105_657` → epoch year ~58,000 ✗

This confirms nanosecond precision. Coral's `format_timestamp` does not support nanosecond input, so timestamps are exposed as `Int64` for arithmetic filtering (`WHERE created_at > 1773473100000000000`).
