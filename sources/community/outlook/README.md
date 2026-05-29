# Microsoft Outlook Community Source

Query Outlook mail folders, messages, attachments, and contacts via the
[Microsoft Graph API v1.0](https://learn.microsoft.com/en-us/graph/overview).
Works with personal Microsoft accounts (Outlook.com) and work/school accounts
(Microsoft 365).

## Authentication

This source uses **OAuth 2.0 authorization code flow** with PKCE — the same
delegated-permission flow documented in the
[Microsoft Graph auth guide](https://learn.microsoft.com/en-us/graph/auth-v2-user).

You need to register an app in the Azure portal to obtain a `client_id`, then
use the guided **Sign in with Microsoft** flow inside Coral, or paste an
existing token manually.

---

### Step 1 — Register an Azure AD application

1. Go to [portal.azure.com](https://portal.azure.com) → **Azure Active
   Directory** → **App registrations** → **New registration**.
2. Give it a name (e.g. `Coral Outlook`).
3. Under **Supported account types**, choose the option matching your mailbox:
   - **Personal Microsoft accounts only** — for Outlook.com / Hotmail
   - **Accounts in this organizational directory only** — for Microsoft 365
     work/school accounts
   - **Any Azure AD directory + personal accounts** — if you want both
4. Under **Redirect URI**, select platform **Mobile and desktop applications**
   and enter: `http://localhost`
   > Per RFC 8252, Microsoft ignores the port when matching a `localhost`
   > loopback redirect URI, so registering `http://localhost` (without a port)
   > lets Coral bind any ephemeral port at runtime. This port-agnostic matching
   > applies only to `localhost` — not `127.0.0.1`, which the portal also won't
   > accept with the `http` scheme.
5. Click **Register** and copy the **Application (client) ID**.

No client secret is needed — this is a public native client.

---

### Step 2 — Add API permissions

In your app registration, go to **API permissions** → **Add a permission** →
**Microsoft Graph** → **Delegated permissions**, and add:

| Permission | Used by |
|---|---|
| `Mail.Read` | `outlook.mail_folders`, `outlook.messages`, `outlook.folder_messages`, `outlook.attachments` |
| `Contacts.Read` | `outlook.contacts` |
| `offline_access` | Allows token refresh without re-authenticating |

Click **Grant admin consent** if you are on an organizational account and have
admin rights. For personal accounts, consent is granted interactively during
the OAuth flow.

---

### Step 3 — Install the source

```bash
coral source add --file sources/community/outlook/manifest.yaml --interactive
```

You'll be prompted for three inputs:

- **`OUTLOOK_TENANT_ID`** — must match the account type you chose in Step 1:
  - `consumers` — personal Outlook.com / Hotmail accounts
  - `organizations` (or your tenant ID GUID) — Microsoft 365 work/school accounts
  - `common` — both
- **`OUTLOOK_OAUTH_CLIENT_ID`** — the Application (client) ID from Step 1.
- **`OUTLOOK_ACCESS_TOKEN`** — choose **Sign in with Microsoft**. Coral opens a
  browser window for you to authenticate and consent; the token is then stored.
  (Coral does not auto-refresh expired tokens yet — see Limitations.)

> If `OUTLOOK_TENANT_ID` doesn't match the app's supported account types, sign-in
> fails — e.g. a "Personal Microsoft accounts only" app used with `common` returns
> `AADSTS9002346` and must use `consumers`.

To script the install instead, provide each input as an environment variable
(the guided OAuth sign-in itself requires `--interactive`):

```bash
OUTLOOK_TENANT_ID=consumers \
OUTLOOK_OAUTH_CLIENT_ID=<your-application-client-id> \
OUTLOOK_ACCESS_TOKEN=<an-existing-access-token> \
coral source add --file sources/community/outlook/manifest.yaml
```

When prompted interactively, you can also choose **Paste access token** instead
of signing in — useful in CI or when a token was obtained by other means.

---

### Step 4 — Verify

```bash
coral source test outlook
```

The built-in test queries read `outlook.mail_folders` and `outlook.messages`
to confirm auth and column mapping are working.

## Tables

### `outlook.mail_folders`

All top-level mail folders in the user's mailbox.

Use `id` values from this table as the `folder_id` filter on
`outlook.folder_messages`.

**Optional filter:** `include_hidden` — set to `true` to include
system-managed hidden folders.

Well-known folder names usable as `folder_id` without looking up an ID:
`Inbox`, `Drafts`, `SentItems`, `DeletedItems`, `JunkEmail`, `Archive`,
`Outbox`.

| Column | Type | Description |
|---|---|---|
| `id` | Utf8 | Folder ID — use as `folder_id` on `outlook.folder_messages` |
| `display_name` | Utf8 | Folder display name |
| `parent_folder_id` | Utf8 | Parent folder ID; null for root-level folders |
| `child_folder_count` | Int64 | Number of child folders |
| `unread_item_count` | Int64 | Unread message count |
| `total_item_count` | Int64 | Total message count |
| `is_hidden` | Boolean | Whether the folder is hidden from the Outlook UI |

---

### `outlook.messages`

The most recent messages across all folders (a single page, ~50), ordered by
`received_date_time` descending. Use this table for cross-folder reads.

**Result scope:** this table returns only the first page from Microsoft Graph.
Graph paginates mail by following the `@odata.nextLink` URL it returns, which
this source does not follow yet, so messages beyond the most-recent page are
not returned. Column predicates (e.g. `is_read`, `has_attachments`,
`from_address`) are applied by Coral over that page only — they are **not**
pushed to Graph and do not search the whole mailbox. To read a specific folder,
use `outlook.folder_messages`.

`body_preview` contains the first 255 characters of the message body.
`to_recipients` and `cc_recipients` are JSON arrays — each element has
`emailAddress.address` and `emailAddress.name` fields.

| Column | Type | Description |
|---|---|---|
| `id` | Utf8 | Message ID — use as `message_id` on `outlook.attachments` |
| `subject` | Utf8 | Subject line |
| `from_address` | Utf8 | Sender email address |
| `from_name` | Utf8 | Sender display name |
| `to_recipients` | Json | Array of To recipient objects |
| `cc_recipients` | Json | Array of CC recipient objects |
| `received_date_time` | Timestamp | Time the message was received |
| `sent_date_time` | Timestamp | Time the message was sent |
| `body_preview` | Utf8 | First 255 characters of the message body |
| `importance` | Utf8 | `low`, `normal`, or `high` |
| `is_read` | Boolean | Whether the message has been read |
| `is_draft` | Boolean | Whether the message is a draft |
| `has_attachments` | Boolean | Whether the message has attachments |
| `conversation_id` | Utf8 | Conversation thread ID |
| `internet_message_id` | Utf8 | RFC 2822 Message-ID header |
| `parent_folder_id` | Utf8 | ID of the containing folder |
| `web_link` | Utf8 | URL to open the message in Outlook Web App |

---

### `outlook.folder_messages`

Messages in a specific folder. The `folder_id` filter is **required**.

Pass a folder UUID from `outlook.mail_folders.id` or a well-known name:
`Inbox`, `Drafts`, `SentItems`, `DeletedItems`, `JunkEmail`, `Archive`,
`Outbox`.

Same columns as `outlook.messages`, plus:

| Column | Type | Description |
|---|---|---|
| `folder_id` | Utf8 | Folder ID or well-known name used to scope this query |

---

### `outlook.attachments`

File and item attachments on a specific message. The `message_id` filter is
**required** — obtain message IDs from `outlook.messages.id` or
`outlook.folder_messages.id`.

`attachment_type` is the raw OData type string — `#microsoft.graph.fileAttachment`,
`#microsoft.graph.itemAttachment`, or `#microsoft.graph.referenceAttachment`.

File content (`contentBytes`) is intentionally excluded — fetch it directly
from the Graph API when needed.

| Column | Type | Description |
|---|---|---|
| `message_id` | Utf8 | Message ID this attachment belongs to |
| `id` | Utf8 | Attachment ID |
| `name` | Utf8 | File name or display name |
| `content_type` | Utf8 | MIME type (e.g. `application/pdf`, `image/png`) |
| `size` | Int64 | Size in bytes |
| `is_inline` | Boolean | Whether the attachment is embedded in the body |
| `last_modified_date_time` | Timestamp | Last modification time |
| `attachment_type` | Utf8 | OData type string indicating attachment kind |

---

### `outlook.contacts`

Personal contacts in the user's default Contacts folder.

`primary_email` extracts the first address from `email_addresses` for
convenience. `business_phones` is a JSON array of phone number strings.

| Column | Type | Description |
|---|---|---|
| `id` | Utf8 | Contact ID |
| `display_name` | Utf8 | Display name |
| `given_name` | Utf8 | First name |
| `surname` | Utf8 | Last name |
| `primary_email` | Utf8 | First email address |
| `email_addresses` | Json | All email addresses as an array of address/name objects |
| `business_phones` | Json | Business phone numbers as an array of strings |
| `mobile_phone` | Utf8 | Mobile phone number |
| `job_title` | Utf8 | Job title |
| `company_name` | Utf8 | Company name |
| `department` | Utf8 | Department |
| `office_location` | Utf8 | Office location |
| `birthday` | Utf8 | Birthday in ISO 8601 date format |
| `created_date_time` | Timestamp | Time the contact was created |
| `last_modified_date_time` | Timestamp | Time the contact was last modified |

## Example Queries

> `messages` and `folder_messages` return a single most-recent page (~50).
> Predicates other than `folder_id` filter within that page (client-side) —
> they don't search the whole mailbox.

```sql
-- Unread messages within the most recent page of the Inbox
SELECT subject, from_address, received_date_time, body_preview
FROM outlook.folder_messages
WHERE folder_id = 'Inbox'
  AND is_read = false
ORDER BY received_date_time DESC
LIMIT 20;

-- Attachment-bearing messages within the most recent page
SELECT subject, from_address, received_date_time, parent_folder_id
FROM outlook.messages
WHERE has_attachments = true
ORDER BY received_date_time DESC
LIMIT 50;

-- All attachments on a specific message
SELECT name, content_type, size, is_inline
FROM outlook.attachments
WHERE message_id = '<message-id>';

-- Folder summary — unread counts and sizes
SELECT display_name, unread_item_count, total_item_count
FROM outlook.mail_folders
ORDER BY unread_item_count DESC;

-- Find a contact by company
SELECT display_name, primary_email, job_title, mobile_phone
FROM outlook.contacts
WHERE company_name = 'Contoso';

-- Messages joined to attachment counts (client-side)
SELECT m.subject, m.from_address, m.received_date_time,
       a.name AS attachment_name, a.content_type, a.size
FROM outlook.messages m
JOIN outlook.attachments a ON a.message_id = m.id
WHERE m.has_attachments = true
LIMIT 20;
```

## Limitations

- **Read-only.** This source does not send, move, or modify any messages,
  contacts, or folders.
- **Token expiry / no auto-refresh.** Access tokens expire after ~1 hour.
  Coral currently stores the refresh-token metadata from the OAuth flow but does
  **not** automatically refresh expired access tokens yet. When the token
  expires, re-run **Sign in with Microsoft** (or, for pasted tokens, reinstall
  the source with a fresh token).
- **`offline_access` scope.** Included so the flow obtains a refresh token and
  stores its metadata for when automatic refresh is supported. Without it, no
  refresh token is issued.
- **Delegated permissions only.** This source uses delegated (user-context)
  permissions and can only access the signed-in user's own mailbox. Application
  permissions (accessing other users' mailboxes as an admin) are not supported.
- **Single-page message reads.** `messages` and `folder_messages` return only
  the most recent page (~50, ordered by received date). Microsoft Graph
  paginates mail by following the `@odata.nextLink` URL it returns, which this
  source does not follow yet, so older messages aren't returned and column
  predicates filter only within that page rather than being pushed to Graph.
  Use `folder_messages` to scope to a folder.
- **`contacts` returns default folder only.** Contacts in non-default contact
  folders are not returned. Use the Graph API directly if you need contacts
  from a specific folder.
