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
4. Register a loopback redirect URI, for example `http://127.0.0.1/oauth/callback`
   (Coral may bind a random port on `127.0.0.1` during interactive setup).
5. Copy the **Application ID** and **Application Secret**.
6. Grant these OAuth scopes to the app:
   - `mailboxes:read`
   - `conversations:read`
   - `customers:read`
   - `users:read`

See [Help Scout authentication](https://developer.helpscout.com/mailbox-api/overview/authentication/).

### 2. Add the source

Interactive OAuth (recommended):

```bash
coral source add --interactive --file sources/community/helpscout/manifest.yaml
```

When prompted:

- Choose **Connect Help Scout**
- Enter your OAuth application ID and secret
- Complete sign-in in the browser

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
| `customers` | table | End customers; join on `email` when present |
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
JOIN helpscout.customers hc ON LOWER(hc.email) = LOWER(i.email)
JOIN helpscout.conversations h ON h.customer_id = hc.id
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
- Customer list rows may omit `email`; prefer `conversations.customer_email`
  or join through `customer_id` when needed.
