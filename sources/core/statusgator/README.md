# StatusGator source

This bundled source queries StatusGator's status board API.

## Configure

Create a read-only organization API token from the API menu in your
StatusGator dashboard (only organization admins can generate tokens), then:

```sh
export STATUSGATOR_API_TOKEN="..."
coral source add statusgator
```

- API version: v3 (base URL: `https://statusgator.com/api/v3`).
- Authentication: bearer token, required for all requests.

## Tables

| Table | Description | Required filters |
|---|---|---|
| `boards` | Monitoring boards | none |
| `board_detail` | Detailed info for a single board | `board_id` |
| `monitors` | Service monitors on a board | `board_id` |
| `history` | Status history for a board | `board_id` |
| `incidents` | Incidents for a board | `board_id` |
| `monitor_components` | Components for a specific monitor | `board_id`, `monitor_id` |
| `service_search` | Search for services by name | `q` |
| `service_components` | Components for a service | `service_id` |
| `status_page_subscribers` | Subscribers to a board's status page | `board_id` |
| `users` | Users in the StatusGator account | none |
| `monitoring_regions` | Available monitoring regions | none |
| `ping` | Health check endpoint | none |

## Example queries

```sql
-- List boards
SELECT id, name, public_token FROM statusgator.boards LIMIT 50;

-- List monitors for a board
SELECT id, display_name, filtered_status, unfiltered_status, checked_at
FROM statusgator.monitors
WHERE board_id = 'your-board-id'
LIMIT 100;

-- List incidents for a board
SELECT id, name, phase, severity, started_at, resolved_at
FROM statusgator.incidents
WHERE board_id = 'your-board-id'
LIMIT 100;
```

## Caveats

- Coral handles pagination internally for endpoints that support it. Use
  `LIMIT` and table-specific filters (`board_id`, `monitor_id`, date ranges,
  `phase`, `severity`, `status`) to keep result sets focused.
- `monitors` only supports the documented `status` request filter; it does
  not support arbitrary page-based pagination beyond what Coral manages.

## Documentation

- StatusGator API docs: https://statusgator.com/api/v3/docs
