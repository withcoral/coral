# Trello
A community source that exposes Trello boards, lists, cards, and members to Coral SQL.

## Authentication
Trello requires an **API Key** and a **User Token** to authenticate requests.

1.  **Get your API Key:** Log in to Trello and go to the [Power-Up Admin page](https://trello.com/power-ups/admin). Create a new Power-Up if you don't have one, and copy the **API Key**.
2.  **Get your API Token:** On the same page, next to the API Key, look for the option to generate a token manually. **Note:** Ensure you grant the token at least **read** access (this is usually the default) so it has permission to fetch your boards, lists, and cards. Click authorize and copy your **Token**.

Set the following environment variables before running Coral:

```bash
export TRELLO_API_KEY="your_trello_api_key"
export TRELLO_API_TOKEN="your_trello_user_token"
```

## Tables

| Table | Description | Required Filters | Optional Filters |
| :--- | :--- | :--- | :--- |
| `trello.boards` | Lists all boards you belong to | None | — |
| `trello.lists` | Lists all lists (columns) on a board | `board_id` | `filter` |
| `trello.cards` | Lists all cards on a board | `board_id` | `filter`, `limit`, `since`, `before` |
| `trello.members`| Lists all members on a board | `board_id` | — |

> [!IMPORTANT]
> Because Trello's API is heavily board-centric, you **must** provide a `board_id` in the `WHERE` clause when querying `lists`, `cards`, and `members`. You can find your `board_id` by first querying the `boards` table.

### `trello.cards` filter reference

| Filter | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `board_id` | string | — | **Required.** Board to fetch cards from. |
| `filter` | string | `all` | Card status: `all`, `closed`, `none`, `open`, `visible`. |
| `limit` | integer | `50` | Max cards returned. Trello maximum is 1000. Keep low on large boards. |
| `since` | string | — | ISO 8601 date or card ID. Return cards created on or after this point. |
| `before` | string | — | ISO 8601 date or card ID. Return cards created on or before this point. |

## Example Queries

### Find your boards
```sql
SELECT id, name, closed
FROM trello.boards;
```

### Get cards on a board (with conservative limit)
```sql
SELECT id, name, due, due_complete, list_id
FROM trello.cards
WHERE board_id = 'your_board_id_here'
LIMIT 50
ORDER BY pos ASC;
```

### Scope cards by date window on a large board
```sql
SELECT id, name, due, list_id
FROM trello.cards
WHERE board_id = 'your_board_id_here'
  AND since = '2024-01-01'
  AND before = '2024-03-31';
```

### Join cards with their corresponding list names
```sql
SELECT
  c.name AS card_name,
  l.name AS list_name,
  c.due
FROM trello.cards c
JOIN trello.lists l ON c.list_id = l.id
WHERE c.board_id = 'your_board_id_here'
  AND l.board_id = 'your_board_id_here';
```

## Rate Limits

Trello enforces two independent rate limits on the REST API:

- **300 requests per 10 seconds** per API key
- **100 requests per 10 seconds** per token

When a limit is exceeded Trello returns HTTP **429**. Coral will surface this as a source error. To stay within limits, use the `limit`, `since`, and `before` filters on `trello.cards` to avoid broad board scans, and avoid running many board-scoped queries in rapid succession.

See the [Trello rate limits documentation](https://developer.atlassian.com/cloud/trello/guides/rest-api/rate-limits/) for full details.

## Limitations

- **Card fetch size:** The `trello.cards` endpoint defaults to 50 cards per request. Use the `limit` filter (max 1000) and `since`/`before` date filters to control fetch size on large boards.
- **Read-Only:** This source provides read-only visibility into Trello and does not support creating, updating, or archiving cards.

## Live Testing Results

### Testing boards query
```console
$ coral sql "SELECT id, name, closed FROM trello.boards LIMIT 5;"
+--------------------------+------+--------+
| id                       | name | closed |
+--------------------------+------+--------+
| 6a1410bae00948891ddabc14 | test | false  |
+--------------------------+------+--------+
```

### Testing cards query
```console
$ coral sql "SELECT id, name FROM trello.cards WHERE board_id = '6a1410bae00948891ddabc14' LIMIT 5;"
+--------------------------+-----------+
| id                       | name      |
+--------------------------+-----------+
| 6a1410bae00948891ddabc48 | Product   |
| 6a1410bae00948891ddabc4e | Marketing |
| 6a1410bae00948891ddabc51 | Sales     |
| 6a1410bae00948891ddabc54 | Support   |
| 6a1410bae00948891ddabc57 | People    |
+--------------------------+-----------+
```
