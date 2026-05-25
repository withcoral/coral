# Trello

A community source that exposes Trello boards, lists, cards, and members to Coral SQL.

## Authentication

Trello requires an **API Key** and a **User Token** to authenticate requests.

1.  **Get your API Key:** Log in to Trello and go to the [Power-Up Admin page](https://trello.com/power-ups/admin). Create a new Power-Up if you don't have one, and copy the **API Key**.
2.  **Get your API Token:** On the same page, next to the API Key, look for the option to generate a token manually. Click it, authorize the app, and copy your **Token**.

Set the following environment variables before running Coral:
```bash
export TRELLO_API_KEY="your_trello_api_key"
export TRELLO_API_TOKEN="your_trello_user_token"
```

## Tables

| Table | Description | Required Filters |
| :--- | :--- | :--- |
| `trello.boards` | Lists all boards you belong to | None |
| `trello.lists` | Lists all lists (columns) on a board | `board_id` |
| `trello.cards` | Lists all cards on a board | `board_id` |
| `trello.members`| Lists all members on a board | `board_id` |

> [!IMPORTANT]
> Because Trello's API is heavily board-centric, you **must** provide a `board_id` in the `WHERE` clause when querying `lists`, `cards`, and `members`. You can find your `board_id` by first querying the `boards` table.

## Example Queries

### Find your boards
```sql
SELECT id, name, closed
FROM trello.boards;
```

### Get all cards on a specific board
```sql
SELECT id, name, due, due_complete, list_id
FROM trello.cards
WHERE board_id = 'your_board_id_here'
ORDER BY pos ASC;
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

## Limitations

- **Pagination:** Trello's board-level endpoints generally return all lists/cards for a board in a single response, bounded by API limits. For exceptionally large boards (thousands of cards), the API might truncate the response. This source does not currently support `before`/`since` offset pagination.
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
