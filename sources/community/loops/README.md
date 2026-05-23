# Loops

**Version:** 0.1.0
**Backend:** HTTP
**Tables:** 5
**Base URL:** `https://app.loops.so/api/v1`

Query mailing lists, contact properties, contacts, transactional email
templates, and campaigns from Loops via the Loops REST API v1.

## Authentication

Requires a `LOOPS_API_KEY`. Generate one from your Loops dashboard:

1. Go to **Settings → API**
2. Click **Create API Key**
3. Copy the generated key

```bash
LOOPS_API_KEY=your_key_here \
  coral source add --file sources/community/loops/manifest.yaml
```

Run from the repo root. Or interactively:

```bash
coral source add --file sources/community/loops/manifest.yaml --interactive
```

See [Loops API reference](https://loops.so/docs/api-reference/intro) for full documentation.

## Tables

| Table | Description | Required filters | Optional filters |
|---|---|---|---|
| `mailing_lists` | All mailing lists in the account | — | — |
| `contact_properties` | All contact property definitions | — | `list` |
| `contacts` | Look up a single contact by email | `email` | — |
| `transactional_emails` | Published transactional email templates | — | — |
| `campaigns` | All campaigns with lifecycle status | — | — |

### `mailing_lists`

Returns one row per mailing list configured in the account. Use `id` values
(e.g. `list_123`) to interpret the `mailing_lists` JSON column on the
`contacts` table.

Key columns: `id`, `name`, `description`, `is_public`.

### `contact_properties`

Returns one row per contact property definition — both Loops built-in fields
and any custom properties your team has created. Use this to discover which
property keys are available before querying contacts.

Pass `list = 'custom'` to see only user-defined properties.

Key columns: `key`, `label`, `type`.

### `contacts`

**This is a lookup table, not a bulk list.** The Loops API does not support
listing all contacts in bulk. You must supply the `email` filter to perform
a point lookup. Returns one row if found, zero rows if not.

Key columns: `id`, `email`, `first_name`, `last_name`, `subscribed`,
`user_group`, `mailing_lists` (JSON), `opt_in_status`.

### `transactional_emails`

Returns one row per published transactional email template. `id` is the
`transactionalId` to pass when triggering a transactional send. `data_variables`
lists the variable names the template accepts.

Key columns: `id`, `name`, `last_updated`, `data_variables`.

### `campaigns`

Returns all campaigns ordered most recently created first. `status` reflects
lifecycle: `Draft` → `Scheduled` → `Sending` → `Sent`.

Key columns: `campaign_id`, `name`, `subject`, `status`, `created_at`.

## Quick start

```bash
# List all mailing lists in the account
coral sql "SELECT id, name, description, is_public FROM loops.mailing_lists"

# Discover all contact properties (built-in and custom)
coral sql "SELECT key, label, type FROM loops.contact_properties ORDER BY type"

# Discover only custom contact properties
coral sql "
  SELECT key, label, type
  FROM loops.contact_properties
  WHERE list = 'custom'
"

# Look up a specific contact by email
coral sql "
  SELECT id, email, first_name, last_name, subscribed,
         user_group, opt_in_status, mailing_lists
  FROM loops.contacts
  WHERE email = 'user@example.com'
"

# List recent campaigns with their status
coral sql "
  SELECT campaign_id, name, subject, status, created_at, updated_at
  FROM loops.campaigns
  ORDER BY created_at DESC
  LIMIT 20
"

# Count campaigns by lifecycle status
coral sql "
  SELECT status, COUNT(*) AS count
  FROM loops.campaigns
  GROUP BY status
  ORDER BY count DESC
"

# List all transactional email templates and their data variables
coral sql "
  SELECT id, name, last_updated, data_variables
  FROM loops.transactional_emails
  ORDER BY last_updated DESC
"

# Find campaigns that have been sent in a single query
coral sql "
  SELECT campaign_id, name, subject, created_at
  FROM loops.campaigns
  WHERE status = 'Sent'
  ORDER BY created_at DESC
"
```

## Discovery order

```text
mailing_lists
  → id (list ID)
    → contacts.mailing_lists (JSON — keys are list IDs, values are booleans)

contact_properties
  → key (property name, camelCase), label, type
    → discovery-only: inspect what property keys exist in the account
    → query loops.contact_properties WHERE list = 'custom' to list
       only user-defined properties
    → note: custom property values are not exposed in contacts columns
       in v1; contact_properties is a schema-inspection table only

transactional_emails
  → id (transactionalId)
    → use when triggering sends via Loops API

campaigns
  → campaign_id
  → email_message_id → linked email message content
```

## API reference

- [Loops REST API v1](https://loops.so/docs/api-reference/intro)
- [Mailing Lists](https://loops.so/docs/api-reference/lists)
- [Contacts — Find](https://loops.so/docs/api-reference/contacts/get)
- [Contact Properties](https://loops.so/docs/api-reference/contact-properties)
- [Transactional Emails](https://loops.so/docs/api-reference/transactional/list)
- [Campaigns](https://loops.so/docs/api-reference/campaigns/list)
