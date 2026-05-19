# Typeform

**Version:** 0.1.0
**Backend:** HTTP
**Tables:** 5
**Base URL:** `https://api.typeform.com`

Query forms, workspaces, themes, responses, and webhooks from Typeform.

## Authentication

Requires a `TYPEFORM_PERSONAL_ACCESS_TOKEN`. Generate one in your Typeform
account under **Settings → Personal Tokens**.

```bash
TYPEFORM_PERSONAL_ACCESS_TOKEN=tfp_... coral source add --file sources/community/typeform/manifest.yaml
```

Or interactively:

```bash
coral source add --file sources/community/typeform/manifest.yaml --interactive
```

## Tables

| Table | Description | Required filters | Optional filters |
|---|---|---|---|
| `forms` | Forms in the account | — | `workspace_id`, `search` |
| `workspaces` | Workspaces in the account | — | `search` |
| `themes` | Themes available in the account | — | — |
| `responses` | Submitted responses for a form | `form_id` | `since`, `until`, `query` |
| `webhooks` | Webhook subscriptions for a form | `form_id` | — |

### Responses pagination note

The Typeform Responses API uses cursor-based pagination via response tokens,
which cannot be expressed in the Coral source-spec DSL. This source fetches
up to **1000 responses** per request (the Typeform API maximum). For forms
with more than 1000 responses, use `since` / `until` time-window filters to
paginate through results in date ranges.

## Quick start

```bash
# Confirm connectivity — list forms
coral sql "SELECT id, title, type, is_public, display_url FROM typeform.forms LIMIT 5"

# List all workspaces with form counts
coral sql "SELECT id, name, forms_count, shared, default FROM typeform.workspaces"

# Search for forms by title
coral sql "
  SELECT id, title, is_public, created_at
  FROM typeform.forms
  WHERE search = 'feedback'
"

# List forms in a specific workspace
coral sql "
  SELECT id, title, created_at
  FROM typeform.forms
  WHERE workspace_id = 'abc123'
"

# Get responses for a specific form
coral sql "
  SELECT response_id, submitted_at, answers, calculated_score, platform
  FROM typeform.responses
  WHERE form_id = 'xyz789'
"

# Get responses in a date range
coral sql "
  SELECT response_id, submitted_at, browser, platform
  FROM typeform.responses
  WHERE form_id = 'xyz789'
    AND since = '2026-01-01T00:00:00Z'
    AND until = '2026-02-01T00:00:00Z'
"

# Audit themes
coral sql "
  SELECT id, name, visibility, font, color_question, color_background
  FROM typeform.themes
"

# Check webhook subscriptions for a form
coral sql "
  SELECT tag, url, enabled, verify_ssl, created_at
  FROM typeform.webhooks
  WHERE form_id = 'xyz789'
"
```

## Discovery order

```text
workspaces
  → id (workspace_id)
    → forms (WHERE workspace_id = '...')

forms
  → id (form_id)
    → responses (WHERE form_id = '...')
    → webhooks  (WHERE form_id = '...')

themes
  (standalone — used by forms for styling)
```
