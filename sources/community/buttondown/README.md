# Buttondown Coral source

Query Buttondown newsletter data with Coral. The source exposes subscribers,
emails, tags, and surveys for creator analytics and launch tracking.

## Setup

Create a Buttondown API key, then add the source:

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

## Example queries

```sql
SELECT email_address, type, source, open_rate, click_rate, creation_date
FROM buttondown.subscribers
ORDER BY creation_date DESC
LIMIT 20;
```

```sql
SELECT subject, status, publish_date, canonical_url
FROM buttondown.emails
ORDER BY publish_date DESC
LIMIT 20;
```

```sql
SELECT identifier, question, status, response_count
FROM buttondown.surveys
ORDER BY response_count DESC
LIMIT 20;
```

## Notes

- This source is read-only.
- Buttondown uses `Authorization: Token <api_key>`.
- Buttondown list endpoints return `results` and expose standard `Link`
  headers for pagination.

## API references

- https://docs.buttondown.com/api-subscribers-list
- https://docs.buttondown.com/api-emails-list
- https://docs.buttondown.com/api-tags-list
- https://docs.buttondown.com/api-surveys-list
