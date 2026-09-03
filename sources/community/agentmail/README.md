# AgentMail

**Version:** 0.1.0
**Backend:** HTTP
**Tables:** 4

Query inboxes, messages, threads, and domains from AgentMail. Provides read-only access to AI agent email infrastructure including conversations, attachments, and domain configuration.

## Installation

Install the source via the CLI:

```bash
coral source add --file sources/community/agentmail/manifest.yaml
```

## Credentials

To use this source, you will need an AgentMail **organization-level** API key with read permissions.

1. Sign up at [agentmail.to](https://agentmail.to).
2. Navigate to your dashboard at [app.agentmail.to](https://app.agentmail.to).
3. Create an API key (starts with `am_`) with the following read permissions: `inbox_read`, `message_read`, `thread_read`, `domain_read`.
4. Provide it when prompted by `coral source add` or set it as an environment variable:

```bash
export AGENTMAIL_API_KEY="am_your-api-key"
```

**Note:** Inbox-scoped API keys only support the `messages` and `threads` tables for that specific inbox. Organization-level keys are required to list all inboxes and domains. See [AgentMail Permissions](https://docs.agentmail.to/permissions) for details.

## Quick Start

```sql
-- List all inboxes
SELECT inbox_id, email, display_name, created_at
FROM agentmail.inboxes;

-- List messages in an inbox
SELECT message_id, subject, "from", preview
FROM agentmail.messages
WHERE inbox_id = 'your-inbox-id'
LIMIT 10;

-- List threads in an inbox
SELECT thread_id, subject, message_count, preview
FROM agentmail.threads
WHERE inbox_id = 'your-inbox-id'
LIMIT 10;

-- List configured domains
SELECT domain_id, domain, feedback_enabled, created_at
FROM agentmail.domains;
```

## Tables

### `inboxes`

Email inboxes managed by AgentMail. Each inbox is an API-first email account for an AI agent with its own address and metadata. No required filters.

**Columns**

| Column | Type | Description |
|--------|------|-------------|
| `inbox_id` | Utf8 | Unique identifier for the inbox |
| `email` | Utf8 | Email address of the inbox |
| `display_name` | Utf8 | Display name shown in emails |
| `pod_id` | Utf8 | ID of the pod this inbox belongs to |
| `client_id` | Utf8 | Client-assigned identifier for the inbox |
| `metadata` | Json | Custom key-value metadata attached to the inbox |
| `updated_at` | Timestamp | When the inbox was last updated (ISO 8601) |
| `created_at` | Timestamp | When the inbox was created (ISO 8601) |

---

### `messages`

Email messages in an AgentMail inbox. Includes sender, recipients, subject, preview text, labels, and attachment metadata. Requires `inbox_id` filter.

**Filters**

| Filter | Type | Required | Description |
|--------|------|----------|-------------|
| `inbox_id` | Utf8 | Yes | ID of the inbox to list messages from |
| `labels` | Utf8 | | Filter by label (comma-separated) |
| `before` | Utf8 | | Only messages before this timestamp (ISO 8601) |
| `after` | Utf8 | | Only messages after this timestamp (ISO 8601) |
| `from` | Utf8 | | Filter by sender (substring match) |
| `to` | Utf8 | | Filter by recipient (substring match) |
| `subject` | Utf8 | | Filter by subject (substring match) |
| `include_spam` | Boolean | | Include spam messages (default false) |
| `include_blocked` | Boolean | | Include blocked messages (default false) |
| `include_unauthenticated` | Boolean | | Include unauthenticated messages (default false) |
| `include_trash` | Boolean | | Include trashed messages (default false) |
| `ascending` | Boolean | | Sort in ascending temporal order (default false) |

**Columns**

| Column | Type | Description |
|--------|------|-------------|
| `inbox_id` | Utf8 | ID of the inbox containing this message |
| `thread_id` | Utf8 | ID of the thread this message belongs to |
| `message_id` | Utf8 | Unique identifier for the message |
| `from` | Utf8 | Sender address |
| `to` | Json | Recipient addresses (JSON array) |
| `cc` | Json | CC recipient addresses (JSON array) |
| `bcc` | Json | BCC recipient addresses (JSON array) |
| `subject` | Utf8 | Subject line of the message |
| `preview` | Utf8 | Text preview of the message body |
| `labels` | Json | Labels assigned to the message (JSON array) |
| `size` | Int64 | Size of the message in bytes |
| `attachments` | Json | Attachment metadata (JSON array) |
| `timestamp` | Timestamp | When the message was sent or drafted (ISO 8601) |
| `created_at` | Timestamp | When the message was created (ISO 8601) |

---

### `threads`

Email threads (conversations) in an AgentMail inbox. Groups messages into conversations with sender/recipient summaries, message counts, and preview text. Requires `inbox_id` filter.

**Filters**

| Filter | Type | Required | Description |
|--------|------|----------|-------------|
| `inbox_id` | Utf8 | Yes | ID of the inbox to list threads from |
| `labels` | Utf8 | | Filter by label (comma-separated) |
| `before` | Utf8 | | Only threads before this timestamp (ISO 8601) |
| `after` | Utf8 | | Only threads after this timestamp (ISO 8601) |
| `senders` | Utf8 | | Filter by sender (substring match) |
| `recipients` | Utf8 | | Filter by recipient (substring match) |
| `subject` | Utf8 | | Filter by subject (substring match) |
| `include_spam` | Boolean | | Include spam threads (default false) |
| `include_blocked` | Boolean | | Include blocked threads (default false) |
| `include_unauthenticated` | Boolean | | Include unauthenticated threads (default false) |
| `include_trash` | Boolean | | Include trashed threads (default false) |
| `ascending` | Boolean | | Sort in ascending temporal order (default false) |

**Columns**

| Column | Type | Description |
|--------|------|-------------|
| `inbox_id` | Utf8 | ID of the inbox containing this thread |
| `thread_id` | Utf8 | Unique identifier for the thread |
| `subject` | Utf8 | Subject line of the thread |
| `preview` | Utf8 | Text preview of the last message |
| `senders` | Json | Sender addresses in the thread (JSON array) |
| `recipients` | Json | Recipient addresses in the thread (JSON array) |
| `labels` | Json | Labels assigned to the thread (JSON array) |
| `message_count` | Int64 | Number of messages in the thread |
| `last_message_id` | Utf8 | ID of the last message in the thread |
| `size` | Int64 | Total size of the thread in bytes |
| `timestamp` | Timestamp | Timestamp of last sent or received message (ISO 8601) |
| `created_at` | Timestamp | When the thread was created (ISO 8601) |

---

### `domains`

Custom domains configured in AgentMail. Includes feedback settings and subdomain configuration. No required filters.

**Columns**

| Column | Type | Description |
|--------|------|-------------|
| `domain_id` | Utf8 | Unique identifier for the domain |
| `domain` | Utf8 | Domain name (e.g. example.com) |
| `pod_id` | Utf8 | ID of the pod this domain belongs to |
| `feedback_enabled` | Boolean | Whether bounce/complaint notifications are sent to inboxes |
| `subdomains_enabled` | Boolean | Whether inboxes on any subdomain are allowed |
| `client_id` | Utf8 | Client-assigned identifier for the domain |
| `updated_at` | Timestamp | When the domain was last updated (ISO 8601) |
| `created_at` | Timestamp | When the domain was created (ISO 8601) |

## Source scope

- Targets the AgentMail API at `https://api.agentmail.to/v0`. The EU endpoint (`https://api.agentmail.eu/v0`) is not supported in this version.
- Requires `AGENTMAIL_API_KEY` authentication as a Bearer token.
- `messages` and `threads` require an `inbox_id` filter (URL path segment). Use `inboxes` to discover inbox IDs.
- Cursor-based pagination (`page_token` query param) on all tables with `limit` default 50, max 100.
- 2 declared test queries (`inboxes` + `domains`) are source-independent.
- Provides read-only access. Creating, sending, updating, or deleting inboxes, messages, threads, or domains is intentionally out of scope.

## Limitations

- The source provides read-only list access only. Send, reply, forward, draft, webhook, and pod management endpoints are out of scope.
- Message body text (HTML/plain) is not returned by the list endpoint — only `preview` (text snippet). Use the Get Message endpoint directly for full body content.
- Attachment binary content is not accessible through this source — only attachment metadata (attachment_id, filename, size, content_type).
- The `messages` and `threads` filter substring matches (`from`, `to`, `subject`, `senders`, `recipients`) are served by AgentMail's search backend, which caps `limit` at 100.
- Rate limits apply based on your AgentMail plan.

## Provider docs

- AgentMail introduction: https://docs.agentmail.to/introduction
- Inboxes API: https://docs.agentmail.to/api-reference/inboxes/list
- Messages API: https://docs.agentmail.to/api-reference/inboxes/messages/list
- Threads API: https://docs.agentmail.to/api-reference/inboxes/threads/list
- Domains API: https://docs.agentmail.to/api-reference/domains/list
- API keys: https://app.agentmail.to

## Live validation output

Validated against a live AgentMail account with a valid `AGENTMAIL_API_KEY`.

```bash
$ coral source lint sources/community/agentmail/manifest.yaml
Manifest is valid
```

```bash
$ coral source add --file sources/community/agentmail/manifest.yaml
Added source agentmail

  ✓ agentmail connected successfully

    agentmail (4 tables)
    ├─ domains
    ├─ inboxes
    ├─ messages
    └─ threads
    Query tests
    2 declared · 2 passed · 0 failed

    ✓ SELECT inbox_id, email, created_at FROM agentmail.inboxes LIMIT 3
      1 row

    ✓ SELECT domain_id, domain, created_at FROM agentmail.domains LIMIT 3
      0 rows
```

**Table introspection:**

```sql
SELECT table_name, description, required_filters
FROM coral.tables
WHERE schema_name = 'agentmail'
ORDER BY table_name;
```

```text
+------------+------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------+
| table_name | description                                                                                                                                                | required_filters |
+------------+------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------+
| domains    | Custom domains configured in AgentMail. Includes verification status, feedback settings, and subdomain configuration.                                      |                  |
| inboxes    | Email inboxes managed by AgentMail. Each inbox is an API-first email account for an AI agent with its own address and metadata.                            |                  |
| messages   | Email messages in an AgentMail inbox. Includes sender, recipients, subject, preview text, labels, and attachment metadata.                                 | inbox_id         |
| threads    | Email threads (conversations) in an AgentMail inbox. Groups messages into conversations with sender/recipient summaries, message counts, and preview text. | inbox_id         |
+------------+------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------+
```

**Inputs introspection:**

```sql
SELECT key, kind, required, is_set
FROM coral.inputs
WHERE schema_name = 'agentmail'
ORDER BY key;
```

```text
+-------------------+--------+----------+--------+
| key               | kind   | required | is_set |
+-------------------+--------+----------+--------+
| AGENTMAIL_API_KEY | secret | true     | true   |
+-------------------+--------+----------+--------+
```

```bash
$ coral source test agentmail
  ✓ agentmail connected successfully
  Secrets: keychain
    Query tests
    2 declared · 2 passed · 0 failed

    ✓ SELECT inbox_id, email, created_at FROM agentmail.inboxes LIMIT 3
      1 row

    ✓ SELECT domain_id, domain, created_at FROM agentmail.domains LIMIT 3
      0 rows
```

**Live inboxes proof:**

```sql
SELECT inbox_id, email, display_name, created_at
FROM agentmail.inboxes;
```

```text
+--------------------+--------------------+--------------+--------------------------+
| inbox_id           | email              | display_name | created_at               |
+--------------------+--------------------+--------------+--------------------------+
| user_abc@agent... | user_abc@agent... | AgentMail    | 2026-06-07T14:58:11.359Z |
+--------------------+--------------------+--------------+--------------------------+
```

**Live messages proof:**

```sql
SELECT message_id, subject, "from", preview
FROM agentmail.messages
WHERE inbox_id = 'user_abc@agentmail.to'
LIMIT 3;
```

```text
+------------------+-------------------+--------------------------+---------------------+
| message_id       | subject           | from                     | preview             |
+------------------+-------------------+--------------------------+---------------------+
| <msg_abc123...>  | learn for english | User <user@example.com>  | we work for english |
+------------------+-------------------+--------------------------+---------------------+
```

**Live threads proof:**

```sql
SELECT thread_id, subject, message_count, preview
FROM agentmail.threads
WHERE inbox_id = 'user_abc@agentmail.to'
LIMIT 3;
```

```text
+--------------------------------------+-------------------+---------------+---------------------+
| thread_id                            | subject           | message_count | preview             |
+--------------------------------------+-------------------+---------------+---------------------+
| thr_abc123-xxxx-xxxx-xxxx-xxxxxxxxxx | learn for english | 1             | we work for english |
+--------------------------------------+-------------------+---------------+---------------------+
```
