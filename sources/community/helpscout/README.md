# Help Scout (Community)

**Version:** 0.1.0
**Backend:** HTTP (Help Scout Mailbox API v2)
**Tables:** 4
**Functions:** 1
**Base URL:** `https://api.helpscout.net/v2`

Query Help Scout mailboxes, conversations, customers, and users. Designed for
support inbox discovery, provider-native conversation search, and cross-source
joins with bundled **Stripe**, **Intercom**, and **Linear** on customer email,
and with community **Gmail** on sender email.

## Install

Community sources are not bundled with the Coral binary. Add the manifest from
this directory:

```bash
coral source add --file sources/community/helpscout/manifest.yaml
```

Or copy `manifest.yaml` into your workspace and pass that path to
`coral source add --file`.

Reference the linked GitHub issue in your PR so maintainers can connect the
contribution to the prior discussion.

## Authentication and setup

Requires a Help Scout OAuth application and access token.

### 1. Create a Help Scout OAuth app

1. Sign in to [Help Scout](https://www.helpscout.com/).
2. Open **Your Profile → My apps**.
3. Click **Create My App**.
4. Register this **exact** loopback redirect URI (Help Scout does not accept
   variable localhost ports):

   `http://127.0.0.1:8765/oauth/callback`
5. Copy the **Application ID** and **Application Secret**.

Help Scout documents two OAuth2 flows: **Authorization Code** (user signs in
in the browser; this source uses that flow via Coral’s **Connect Help Scout**)
and **Client Credentials** (machine-to-machine for internal integrations). API
access requires a token associated with an **active, invited** Help Scout user.
Help Scout does not document per-resource OAuth scope names in their auth
overview—configure the app and complete authorization as described in their
docs.

See [Help Scout authentication](https://developer.helpscout.com/mailbox-api/overview/authentication/).

### Permissions

Authorization-code tokens inherit the connecting user's Help Scout account
permissions. Help Scout does not document per-resource OAuth scope names in
My apps—configure the OAuth app and sign in as a user with read access to the
Mailbox API resources below.

| Mailbox API access | Coral surface |
| --- | --- |
| Read mailboxes | `helpscout.mailboxes` |
| Read customers | `helpscout.customers` |
| Read conversations | `helpscout.conversations` |
| Read users | `helpscout.users` |
| Conversation search | `helpscout.search_conversations` |

### 2. Add the source

Interactive OAuth (recommended):

```bash
coral source add --interactive --file sources/community/helpscout/manifest.yaml
```

When prompted:

- Choose **Connect Help Scout**
- Enter your OAuth application ID and secret
- Complete sign-in in the browser (callback goes to `127.0.0.1:8765`)
- If the browser cannot reach localhost, paste the full redirect URL from the
  address bar into the terminal when Coral prompts for it

Or paste an existing token:

```bash
export HELPSCOUT_ACCESS_TOKEN=your_token
coral source add --file sources/community/helpscout/manifest.yaml
```

Access tokens expire in about **48 hours**. Re-run `coral source add` and
choose **Connect Help Scout** when queries return HTTP 401.

### Help Scout API vs Coral

Help Scout's REST API is ideal for application integrations. Use this Coral
source when you need **SQL joins and aggregations** across Help Scout and other
Coral sources (Gmail, Stripe, Linear, Intercom) in one query.

## Tables and functions

| Name | Kind | Description |
| --- | --- | --- |
| `mailboxes` | table | Shared inboxes (mailbox ID, name, email) |
| `customers` | table | End customers; join via `conversations.customer_email`, or `email` when embedded |
| `conversations` | table | Support conversations with `customer_email` |
| `users` | table | Help Scout agents and admins |
| `search_conversations` | function | Provider-native search via `query` argument |

### Conversation status values

| Value | Meaning |
| --- | --- |
| `active` | Default list — active conversations |
| `all` | Every status |
| `open` | Open |
| `pending` | Pending |
| `closed` | Closed |
| `spam` | Spam |

Example:

```sql
SELECT id, number, subject, status, customer_email
FROM helpscout.conversations
WHERE status = 'open'
LIMIT 20;
```

### Search syntax

Use `search_conversations` for Help Scout query syntax:

```sql
SELECT id, number, subject, status, customer_email
FROM helpscout.search_conversations(query => 'tag:vip')
LIMIT 25;
```

Other examples: `(number:123)`, `(email:"user@example.com")`,
`(subject:"billing")`. See [Help Scout conversation search](https://developer.helpscout.com/mailbox-api/endpoints/conversations/list/).

For community source PRs, include sanitized output from the commands below:
`coral source lint`, `coral source add` (or interactive Connect), `coral source
test helpscout`, and at least one row query per table plus
`search_conversations`.

### Commands to capture

```bash
coral source lint sources/community/helpscout/manifest.yaml
coral source add --interactive --file sources/community/helpscout/manifest.yaml
# or: export HELPSCOUT_ACCESS_TOKEN=... && coral source add --file sources/community/helpscout/manifest.yaml
coral source test helpscout
coral sql "SELECT id, name, slug FROM helpscout.mailboxes LIMIT 3"
coral sql "SELECT id, email, first_name FROM helpscout.customers LIMIT 3"
coral sql "SELECT id, number, subject, status, customer_email FROM helpscout.conversations LIMIT 3"
coral sql "SELECT id, email, first_name, last_name FROM helpscout.users LIMIT 3"
coral sql "SELECT id, number, subject, status FROM helpscout.search_conversations(query => 'status:active') LIMIT 3"
```

## Live validation output

The following output was captured against Help Scout Mailbox API v2 with Coral
using interactive OAuth (`redirect_uri` `http://127.0.0.1:8765/oauth/callback`).
This account's customer list response omitted embedded email values, so
`customers.email` is blank in the sample; cross-source examples use
`conversations.customer_email`.

```text
$ coral source lint sources/community/helpscout/manifest.yaml
Manifest is valid
```

```text
$ coral source add --interactive --file sources/community/helpscout/manifest.yaml
Added source helpscout (secrets: keychain)

  ✓ helpscout connected successfully
  Secrets: keychain

    helpscout (4 tables)
    ├─ conversations
    ├─ customers
    ├─ mailboxes
    └─ users
    Query tests
    4 declared · 4 passed · 0 failed

    ✓ SELECT id, name, email FROM helpscout.mailboxes LIMIT 5
      1 row

    ✓ SELECT id, first_name, last_name, email FROM helpscout.customers LIMIT 5
      2 rows

    ✓ SELECT id, number, subject, status, customer_email FROM helpscout.conversations LIMIT 5
      4 rows

    ✓ SELECT id, email, first_name, last_name FROM helpscout.users LIMIT 5
      1 row
```

```text
$ coral source test helpscout

  ✓ helpscout connected successfully
  Secrets: keychain

    helpscout (4 tables)
    ├─ conversations
    ├─ customers
    ├─ mailboxes
    └─ users
    Query tests
    4 declared · 4 passed · 0 failed

    ✓ SELECT id, name, email FROM helpscout.mailboxes LIMIT 5
      1 row

    ✓ SELECT id, first_name, last_name, email FROM helpscout.customers LIMIT 5
      2 rows

    ✓ SELECT id, number, subject, status, customer_email FROM helpscout.conversations LIMIT 5
      4 rows

    ✓ SELECT id, email, first_name, last_name FROM helpscout.users LIMIT 5
      1 row
```

```sql
SELECT id, name, slug FROM helpscout.mailboxes LIMIT 3;
```

```text
+--------+----------------+------------------+
| id     | name           | slug             |
+--------+----------------+------------------+
| 369162 | Support Inbox  | a1b2c3d4e5f6g7h8 |
+--------+----------------+------------------+
```

```sql
SELECT id, email, first_name FROM helpscout.customers LIMIT 3;
```

```text
+-----------+-------+------------+
| id        | email | first_name |
+-----------+-------+------------+
| 883767758 |       | Example    |
| 883767756 |       | Helper     |
+-----------+-------+------------+
```

```sql
SELECT id, number, subject, status, customer_email FROM helpscout.conversations LIMIT 3;
```

```text
+------------+--------+---------------------+--------+-------------------+
| id         | number | subject             | status | customer_email    |
+------------+--------+---------------------+--------+-------------------+
| 3342196193 | 3      | Example subject one | active | user@example.com  |
| 3342196197 | 4      | Example subject two | active | user@example.com  |
| 3342196184 | 1      | Welcome message     | active | help@example.com  |
+------------+--------+---------------------+--------+-------------------+
```

```sql
SELECT id, email, first_name, last_name FROM helpscout.users LIMIT 3;
```

```text
+--------+-------------------+------------+-----------+
| id     | email             | first_name | last_name |
+--------+-------------------+------------+-----------+
| 939115 | agent@example.com | Example    | Agent     |
+--------+-------------------+------------+-----------+
```

```sql
SELECT id, number, subject, status
FROM helpscout.search_conversations(query => 'status:active')
LIMIT 3;
```

```text
+------------+--------+---------------------+--------+
| id         | number | subject             | status |
+------------+--------+---------------------+--------+
| 3342196193 | 3      | Example subject one | active |
| 3342196197 | 4      | Example subject two | active |
| 3342196184 | 1      | Welcome message     | active |
+------------+--------+---------------------+--------+
```

## Cross-source JOIN examples

### Gmail + Help Scout

Customers who emailed you recently and have open Help Scout conversations
(requires community `gmail` source with `message_details`):

```sql
SELECT
  d.from_email,
  d.subject AS gmail_subject,
  c.number,
  c.subject AS conversation_subject,
  c.status
FROM gmail.search_messages(q => 'newer_than:7d') m
JOIN gmail.message_details d ON d.message_id = m.id
JOIN helpscout.conversations c
  ON LOWER(c.customer_email) = LOWER(d.from_email)
WHERE c.status IN ('active', 'open', 'pending')
LIMIT 20;
```

### Stripe + Help Scout

Paying Stripe customers with active Help Scout conversations:

```sql
SELECT
  s.email,
  s.id AS stripe_customer_id,
  c.number,
  c.subject,
  c.status
FROM stripe.customers s
JOIN helpscout.conversations c
  ON LOWER(c.customer_email) = LOWER(s.email)
WHERE c.status IN ('active', 'open', 'pending')
LIMIT 50;
```

### Intercom + Help Scout

Compare Intercom contacts with Help Scout customers on the same email:

```sql
SELECT
  i.email,
  i.id AS intercom_id,
  hc.id AS helpscout_customer_id,
  h.number,
  h.subject,
  h.status
FROM intercom.contacts i
JOIN helpscout.conversations h
  ON LOWER(h.customer_email) = LOWER(i.email)
LEFT JOIN helpscout.customers hc ON hc.id = h.customer_id
LIMIT 30;
```

### Linear + Help Scout

Support conversations tied to Linear teammates by email:

```sql
SELECT
  u.email,
  u.name AS linear_name,
  c.number,
  c.subject,
  c.status
FROM linear.users u
JOIN helpscout.conversations c
  ON LOWER(c.customer_email) = LOWER(u.email)
LIMIT 25;
```

## Notes

- All tables are strictly read-only.
- List endpoints use page-based pagination (`page=1`, `page=2`, …). Coral
  follows pages until the fetch limit is reached.
- `helpscout.conversations` defaults to **active** conversations on the API
  side when no `status` filter is supplied. Use `WHERE status = 'all'` to
  include every status.
- `customer_email` on `conversations` comes from `primaryCustomer.email` and
  is the primary join key for cross-source SQL.
- Help Scout rate limits apply; respect fetch limits and use targeted filters
  on large accounts.

## Limitations (v0.1)

- No `threads`, `folders`, or custom-field tables yet.
- Customer list rows may omit embedded emails; prefer
  `conversations.customer_email` for cross-source joins and use
  `customers.email` only when `_embedded.emails` is populated by the list API.
