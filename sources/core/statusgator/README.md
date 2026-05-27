# StatusGator Source (core)

This is the canonical `statusgator` source shipped in `sources/core`.

Notes
- API version: v3 (base URL: `https://statusgator.com/api/v3`).
- Authentication: A bearer token is required for all requests. Create a read-only organization API token from the StatusGator dashboard. Set it in the `STATUSGATOR_API_TOKEN` input (required).
- Coral handles pagination internally for supported endpoints. Use SQL `LIMIT` and table-specific filters such as `board_id`, `monitor_id`, date ranges, `phase`, `severity`, or `status` to keep result sets focused.

Usage examples

- List boards

  SELECT id, name, public_token FROM statusgator.boards LIMIT 50

- List monitors for a board

  SELECT id, display_name, filtered_status, unfiltered_status, checked_at FROM statusgator.monitors WHERE board_id = 'your-board-id' LIMIT 100

- List incidents for a board

  SELECT id, name, phase, severity, started_at, resolved_at FROM statusgator.incidents WHERE board_id = 'your-board-id' LIMIT 100

Authentication example (env):

- Set the token as an input in Coral or via your integration secrets:
  - `STATUSGATOR_API_TOKEN` — required, bearer token

Documentation
- StatusGator API docs: https://statusgator.com/api/v3/docs

If you want additional columns or board-scoped helpers (e.g. mapping monitors to services), tell me which fields you need and I'll add them to the manifest.