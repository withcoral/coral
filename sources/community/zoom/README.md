# Zoom community source

The `zoom` community source exposes read-only Zoom user profile, meeting, and
cloud recording data through Coral SQL.

## Setup

### 1. Create a Zoom Server-to-Server OAuth app

1. Go to the [Zoom App Marketplace](https://marketplace.zoom.us/develop/create).
2. Choose **Server-to-Server OAuth** and create the app.
3. Under **Scopes**, add:
   - `user:read:user`
   - `meeting:read:list_meetings`
   - `cloud_recording:read:list_user_recordings`
4. **Activate** the app.
5. Copy the **Account ID**, **Client ID**, and **Client Secret**.

### 2. Obtain an access token

```sh
curl -s -X POST https://zoom.us/oauth/token \
  -H "Authorization: Basic $(printf '%s' 'CLIENT_ID:CLIENT_SECRET' | base64 | tr -d '\n')" \
  -d "grant_type=account_credentials&account_id=ACCOUNT_ID"
```

Copy the `access_token` from the JSON response. Tokens expire after one hour.

### 3. Install the source

```sh
export ZOOM_ACCESS_TOKEN="<token>"
cargo run -p coral-cli -- source add --file sources/community/zoom/manifest.yaml
```

## Tables

| Table | Purpose |
| --- | --- |
| `zoom.user` | Authenticated user profile (one row). Uses `row_strategy: direct` to parse the single root JSON object. |
| `zoom.meetings` | Meetings for the authenticated user. The `meeting_status` filter controls which Zoom list mode to use (`scheduled`, `live`, `upcoming`, `upcoming_meetings`, `previous_meetings`). Defaults to `scheduled`. |
| `zoom.recordings` | Cloud recordings. **Requires** `from_date` and `to_date` filters in `yyyy-MM-dd` format, limited to a 1-month range per query. Requires a Pro plan or higher with Cloud Recording enabled. |

All tables are read-only. This source does not create, update, or delete Zoom
resources.

### Limitations

- **List meetings defaults to `scheduled` only.** The `meeting_status` filter
  controls which meetings Zoom returns. Without it, only scheduled (unexpired)
  meetings appear. The `upcoming`, `upcoming_meetings`, and
  `previous_meetings` modes only span a 6-month window. See
  [List meetings](https://developers.zoom.us/docs/api/meetings/#tag/meetings/GET/users/{userId}/meetings).
- **Cloud Recording requires a Pro plan or higher** with Cloud Recording
  enabled on the Zoom account. Free/Basic plans do not have cloud recordings.
  See [List recordings](https://developers.zoom.us/docs/api/meetings/#tag/cloud-recording/GET/users/{userId}/recordings).
- **Recording date range is capped at 1 month per query.** The `from_date`
  and `to_date` filters are required and must not span more than one calendar
  month.
- **Tokens expire after one hour.** Re-run the `curl` command and
  `source add` to refresh.

## Example queries

Verify credentials and inspect the authenticated user:

```sql
SELECT id, email, first_name, last_name, role_name, timezone
FROM zoom.user
LIMIT 1;
```

List recent scheduled meetings (default mode):

```sql
SELECT uuid, topic, start_time, duration, join_url
FROM zoom.meetings
LIMIT 20;
```

List previous meetings from the last 6 months:

```sql
SELECT uuid, topic, start_time, duration
FROM zoom.meetings
WHERE meeting_status = 'previous_meetings'
LIMIT 10;
```

Query cloud recordings for a specific month:

```sql
SELECT uuid, topic, start_time, duration, recording_count, share_url
FROM zoom.recordings
WHERE from_date = '2026-05-01' AND to_date = '2026-05-31'
LIMIT 20;
```

Inspect individual recording files from a month of recordings:

```sql
SELECT uuid, topic, recording_files
FROM zoom.recordings
WHERE from_date = '2026-04-01' AND to_date = '2026-04-30'
LIMIT 5;
```

## Validation

Lint the manifest:

```sh
cargo run -p coral-cli -- source lint sources/community/zoom/manifest.yaml
```

Install and test:

```sh
export ZOOM_ACCESS_TOKEN="<token>"
cargo run -p coral-cli -- source add --file sources/community/zoom/manifest.yaml
cargo run -p coral-cli -- source test zoom
```

Inspect the registered source:

```sh
cargo run -p coral-cli -- sql "SELECT table_name, description, required_filters FROM coral.tables WHERE schema_name = 'zoom'"
cargo run -p coral-cli -- sql "SELECT table_name, column_name, is_required_filter FROM coral.columns WHERE schema_name = 'zoom' ORDER BY table_name, ordinal_position"
```

## Notes

- Zoom sends `X-RateLimit-Remaining` and `Retry-After` headers on every
  response. The manifest declares these under `rate_limit` so Coral
  automatically backs off when a rate limit is hit.
- The `user` table fetches `GET /users/me`, which returns a single JSON object
  (not an array). Coral handles this via `row_strategy: direct` with no
  `rows_path`, producing exactly one row.
- The `meeting_status` filter controls Zoom's
  [list mode](https://developers.zoom.us/docs/api/meetings/#tag/meetings/GET/users/{userId}/meetings)
  and maps to the `type` query parameter in the API. The response column `type`
  is a separate concept — the numeric meeting kind (1 = instant, 2 = scheduled,
  etc.).
- The `recordings` table requires `from_date` and `to_date` because Zoom
  defaults to the current date when these are omitted, which typically returns
  no results. The maximum date range per API call is one calendar month.
- Zoom has migrated to granular scopes. This source uses the granular scope
  names (`user:read:user`, `meeting:read:list_meetings`,
  `cloud_recording:read:list_user_recordings`). Legacy scope names such as
  `user:read` or `recording:read` may also work for older Zoom apps.
- See the [Zoom API reference](https://developers.zoom.us/docs/api/) for
  full endpoint documentation.
