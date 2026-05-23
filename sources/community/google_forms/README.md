# Google Forms

Query Google Forms metadata, questions, and response data using SQL via the [Google Forms REST API v1](https://developers.google.com/forms/api/reference/rest).

**Version:** 0.1.0
**Backend:** HTTP
**Tables:** 3
**Base URL:** `https://forms.googleapis.com`

## Authentication

Requires a `GOOGLE_FORMS_ACCESS_TOKEN`. Generate an OAuth 2.0 access token with the following scopes:

- `https://www.googleapis.com/auth/forms.body.readonly`
- `https://www.googleapis.com/auth/forms.responses.readonly`

```bash
GOOGLE_FORMS_ACCESS_TOKEN=ya29.xxx coral source add --file sources/community/google_forms/manifest.yaml
```

Or interactively:

```bash
coral source add --file sources/community/google_forms/manifest.yaml --interactive
```

To generate an access token:

1. Go to [Google Cloud Console](https://console.cloud.google.com/apis/credentials).
2. Create a project and enable the **Google Forms API**.
3. Create an **OAuth 2.0 Client ID** credential (Desktop or Web app).
4. Authorize the scopes above.
5. Obtain an access token via OAuth 2.0 flow or use the [OAuth 2.0 Playground](https://developers.google.com/oauthplayground/).

Note: OAuth 2.0 access tokens expire after 1 hour. Refresh and update the secret as needed.

## Finding Your Form ID

The form ID is in the Google Forms URL:

```
https://docs.google.com/forms/d/{form_id}/edit
```

## Tables

| Table | Description | Required filters | Optional filters |
|---|---|---|---|
| `form` | Form title, description, and metadata | `form_id` | — |
| `questions` | Question items in the form | `form_id` | — |
| `responses` | Submitted responses | `form_id` | `after_timestamp` |

## Quick Start

```sql
-- Get form metadata
SELECT form_id, title, description
FROM google_forms.form
WHERE form_id = 'your-form-id';

-- List all questions
SELECT item_id, title, required
FROM google_forms.questions
WHERE form_id = 'your-form-id';

-- List required questions only
SELECT item_id, title
FROM google_forms.questions
WHERE form_id = 'your-form-id'
  AND required = true;

-- Get all responses
SELECT response_id, create_time, respondent_email, answers
FROM google_forms.responses
WHERE form_id = 'your-form-id';

-- Count total responses
SELECT COUNT(*) AS total_responses
FROM google_forms.responses
WHERE form_id = 'your-form-id';

-- Responses after a specific time
SELECT response_id, create_time, answers
FROM google_forms.responses
WHERE form_id = 'your-form-id'
  AND after_timestamp = 'timestamp=2024-01-01T00:00:00Z';
```

## Discovery Order

```text
form
  → form_id
    → questions (WHERE form_id = '...')
    → responses (WHERE form_id = '...')
questions
  → item_id → matches keys in responses.answers
```

## Limitations

- **Read-only**: This source only supports `SELECT` operations.
- **Access token expiry**: OAuth 2.0 access tokens expire after 1 hour.
- **Answers as JSON**: The `answers` column is a raw JSON object. Join `item_id` from the `questions` table to resolve question titles.
- **after_timestamp filter**: The Google Forms API expects the filter value in the format `timestamp=<ISO8601>` (e.g. `timestamp=2024-01-01T00:00:00Z`).

## Notes

- The `form_id` is stable and does not change when the form is renamed.
- Use the `questions` table to map `item_id` values in `answers` to human-readable question titles.
