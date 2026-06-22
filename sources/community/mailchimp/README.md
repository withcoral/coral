# Mailchimp (Community)

**Version:** 0.1.0
**Backend:** HTTP (Mailchimp Marketing API v3)
**Tables:** 4
**Base URL:** `https://{server-prefix}.api.mailchimp.com`

Query Mailchimp audiences, campaigns, subscribers, and campaign performance
reports as SQL tables. Join with Salesforce contacts or Linear issues for
cross-source marketing and engineering intelligence.

## Install

```bash
coral source add --file sources/community/mailchimp/manifest.yaml
```

Or copy `manifest.yaml` into your workspace and pass that path to
`coral source add --file`.

## Authentication and setup

Requires a Mailchimp API key and your account's server prefix.

1. In Mailchimp, go to **Profile > Extras > API keys > Create A Key**.
2. Note the server prefix from the end of your API key (e.g. if the key ends
   in `-us6`, your prefix is `us6`).
3. Run:

```bash
MAILCHIMP_SERVER_PREFIX=us6 \
MAILCHIMP_API_KEY=your-api-key \
coral source add --file sources/community/mailchimp/manifest.yaml
```

Or pass them interactively when prompted.

### Permissions

Mailchimp API keys carry the full permissions of the account that created
them — Mailchimp has no key-level scopes. All four tables (`lists`,
`campaigns`, `members`, `reports`) still depend on the authorizing user's
role and the account's plan. A 403 response means the account's plan or user
role does not allow access to that endpoint, not that the key is invalid.
Check the user's access level and the error response details. See the
[API fundamentals](https://mailchimp.com/developer/marketing/docs/fundamentals/#connecting-to-the-api)
and [error reference](https://mailchimp.com/developer/marketing/docs/errors/)
for details.

## Tables

| Table | Required filters | Optional filters | Description |
| --- | --- | --- | --- |
| `lists` | none | — | All audiences with subscriber counts and engagement averages |
| `campaigns` | none | `status`, `list_id`, `since_send_time`, `before_send_time`, `sort_field`, `sort_dir` | Campaigns with status, subject, send time, and summary stats |
| `members` | `list_id` | `status` | Subscribers in an audience with email, status, and signup times |
| `reports` | none | `since_send_time`, `before_send_time` | Per-campaign performance: opens, clicks, bounces, unsubscribes |

## Example queries

### Audiences ranked by subscriber count

```sql
SELECT id, name, subscriber_count, open_rate, click_rate, date_created
FROM mailchimp.lists
ORDER BY subscriber_count DESC
LIMIT 20;
```

This ranks the audiences within the table's 100-row default fetch limit.

### Most recent sent campaigns

Use `sort_field` and `sort_dir` to push the sort to the Mailchimp API so
`LIMIT` returns the globally most-recent rows, not just the first page.

```sql
SELECT id, subject_line, emails_sent, open_rate, click_rate, send_time
FROM mailchimp.campaigns
WHERE status = 'sent'
  AND sort_field = 'send_time'
  AND sort_dir = 'DESC'
LIMIT 25;
```

### Active subscribers in an audience

```sql
SELECT email_address, full_name, source, timestamp_signup
FROM mailchimp.members
WHERE list_id = 'YOUR_LIST_ID'
  AND status = 'subscribed'
ORDER BY timestamp_signup DESC
LIMIT 50;
```

### Campaigns by open rate within a fetched time window

Use `since_send_time` to bound the result set so `ORDER BY open_rate DESC`
ranks the fetched reports from a relevant time window. Mailchimp does not
support server-side report sorting, and this table fetches at most 100 rows by
default, so the result is not a global ranking when the window contains more
than 100 reports.

```sql
SELECT campaign_title, emails_sent, open_rate, click_rate,
       hard_bounces, unsubscribed, send_time
FROM mailchimp.reports
WHERE since_send_time = '2025-01-01T00:00:00Z'
ORDER BY open_rate DESC
LIMIT 20;
```

### Campaigns joined with full report stats

```sql
SELECT c.subject_line, c.send_time, r.open_rate, r.click_rate,
       r.hard_bounces, r.unsubscribed
FROM mailchimp.campaigns c
JOIN mailchimp.reports r ON c.id = r.id
WHERE c.status = 'sent'
  AND c.since_send_time = '2025-01-01T00:00:00Z'
  AND r.since_send_time = '2025-01-01T00:00:00Z'
ORDER BY r.open_rate DESC
LIMIT 20;
```

## Cross-source examples

### Subscribers who also have open Linear issues (requires `linear` source)

```sql
SELECT m.email_address, m.full_name, li.title AS issue_title, li.state_type
FROM mailchimp.members m
JOIN linear.issues li ON LOWER(li.assignee_email) = LOWER(m.email_address)
WHERE m.list_id = 'YOUR_LIST_ID'
  AND m.status = 'subscribed'
  AND li.state_type != 'completed'
LIMIT 30;
```

### Salesforce contacts who are active subscribers (requires `salesforce` source)

```sql
SELECT c.first_name, c.last_name, c.email, m.status AS mailchimp_status
FROM salesforce.contacts c
JOIN mailchimp.members m ON LOWER(m.email_address) = LOWER(c.email)
WHERE m.list_id = 'YOUR_LIST_ID'
  AND m.status = 'subscribed'
LIMIT 50;
```

## Validation

```bash
# YAML style check
make lint-sources

# Add the source (output sanitized — real IDs and key redacted)
coral source add --file sources/community/mailchimp/manifest.yaml
```

```text
  ✓ mailchimp connected successfully
  Secrets: keyring

    mailchimp (4 tables)
    ├─ campaigns
    ├─ lists
    ├─ members
    └─ reports

    Query tests
    1 declared · 1 passed · 0 failed

    ✓ SELECT id, name FROM mailchimp.lists LIMIT 1
      1 row
```

```bash
# Run the built-in test query against the live account
coral source test mailchimp
```

```text
  ✓ mailchimp connected successfully
  Secrets: keyring

    mailchimp (4 tables)
    ├─ campaigns
    ├─ lists
    ├─ members
    └─ reports

    Query tests
    1 declared · 1 passed · 0 failed

    ✓ SELECT id, name FROM mailchimp.lists LIMIT 1
      1 row
```

```bash
# Representative query — list audiences (output sanitized)
coral sql "SELECT id, name, subscriber_count, open_rate FROM mailchimp.lists LIMIT 3"
```

```text
+------------------+---------------------+------------------+-----------+
| id               | name                | subscriber_count | open_rate |
+------------------+---------------------+------------------+-----------+
| a1b2c3d4e5f67890 | Main Newsletter     | 4821             | 28.407    |
| b2c3d4e5f6789012 | Product Updates     | 1203             | 34.118    |
| c3d4e5f678901234 | Customer Onboarding | 892              | 41.729    |
+------------------+---------------------+------------------+-----------+
```

## Limitations

- **Read-only** — this source exposes read-only Marketing API endpoints only.
- **members requires list_id** — get a list ID from `mailchimp.lists` first.
- **report_summary columns are null for unsent campaigns** — `opens`, `clicks`,
  `open_rate`, and `click_rate` on the `campaigns` table are only populated
  after a campaign has been sent. Use `mailchimp.reports` for full stats.
- **timestamp_signup and timestamp_opt** — returned as strings; Mailchimp
  returns an empty string (not null) when these are not set, so they are typed
  as `Utf8` rather than `Timestamp`.
- **API key expiry** — Mailchimp API keys do not expire by default but can be
  revoked. Re-add the source if you regenerate your key.
- **Rate limits** — Mailchimp allows 10 simultaneous connections per account.
  Always use provider-side filters where available. Each table also has a
  100-row default fetch limit; SQL `LIMIT` alone does not increase that cap.
- Community sources are maintained separately from bundled core sources.

## Contributing

Follow [CONTRIBUTING.md](../../../CONTRIBUTING.md): discuss on the linked issue
first, sign the CLA if this is your first contribution, run `make lint-sources`,
and open a focused PR titled
`feat(sources/community/mailchimp): add mailchimp community source`.
