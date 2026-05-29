# Buttondown Coral source

Query Buttondown newsletter data with Coral. The source exposes subscribers,
emails, tags, and surveys for creator analytics and launch tracking.

## Setup

Create a Buttondown API key, then add the source. The key needs read access for
subscribers, emails, tags, and surveys; write permissions are not needed.

```bash
BUTTONDOWN_API_KEY=... coral source add --file sources/community/buttondown/manifest.yaml
```

Run validation:

```bash
coral source test buttondown
```

## Tables

| Table | Description |
| --- | --- |
| `buttondown.subscribers` | Subscriber status, acquisition, engagement, referrer, tags, and metadata. |
| `buttondown.emails` | Newsletter emails, drafts, and published sends. |
| `buttondown.tags` | Subscriber tags. |
| `buttondown.surveys` | Newsletter surveys. |

This source pins `X-API-Version: 2026-04-01` so field names and date filters
match the manifest.

## Example queries

```sql
SELECT email_address, type, source, open_rate, click_rate, creation_date
FROM buttondown.subscribers
WHERE ordering = '-creation_date'
LIMIT 20;
```

```sql
SELECT subject, status, publish_date, canonical_url
FROM buttondown.emails
WHERE ordering = '-publish_date'
LIMIT 20;
```

```sql
SELECT identifier, question, status, response_count
FROM buttondown.surveys
WHERE ordering = '-response_count'
LIMIT 20;
```

## Notes

- This source is read-only.
- Buttondown uses `Authorization: Token <api_key>`.
- Subscribers, emails, and surveys expose Buttondown's provider-side
  `ordering` filter for recency/top-N queries. Tags keep the documented
  `page_size` parameter; the other list endpoints rely on `Link` pagination
  without sending an undocumented page size.

## Validation evidence

Static validation run locally:

```bash
coral source lint sources/community/buttondown/manifest.yaml
make lint-sources
yamllint sources/community/buttondown/manifest.yaml
git diff --check origin/main..HEAD
gitleaks detect --no-banner --redact --source . --log-opts=origin/main..HEAD
```

Credentialed `coral source add --file`, `coral source test buttondown`, and
representative live queries require a Buttondown API key and were not run in
this workspace.

## API references

- https://docs.buttondown.com/api-subscribers-list
- https://docs.buttondown.com/api-emails-list
- https://docs.buttondown.com/api-tags-list
- https://docs.buttondown.com/api-surveys-list
