# Devpost

**Version:** 0.1.0
**Backend:** HTTP
**Tables:** 1 table + 1 search function
**Base URL:** `https://devpost.com`

Query public hackathons from [Devpost](https://devpost.com) via its public JSON
API (`/api/hackathons`). No authentication required.

```bash
coral source add --file sources/community/devpost/manifest.yaml
coral source test devpost
```

## Tables & functions

| Name | Kind | Description | Filters / Args |
|---|---|---|---|
| `events` | table | Browse hackathons in Devpost's default ranking | `status` |
| `search` | search function | Keyword search, preserving provider ranking | `q` (required), `status` |

---

### `events`

Browse the hackathon catalog. Results paginate 9 per page, so always use a
`LIMIT` (default fetch limit is 100). Avoid an unbounded `ORDER BY`, which would
force fetching the entire ~13k-row catalog; for ranked keyword results use
`search()` instead.

#### Filters

| Filter | Type | Description |
|---|---|---|
| `status` | string | `open`, `upcoming`, or `ended`, mapped to `?status[]=` |

### `search`

```sql
SELECT title, organizer, prize_amount_raw, registrations_count
FROM devpost.search(q => 'ai')
ORDER BY registrations_count DESC
LIMIT 10;
```

#### Arguments

| Arg | Required | Description |
|---|---|---|
| `q` | yes | Keyword query, mapped to `?search=` |
| `status` | no | Optional `open` / `upcoming` / `ended` |

## Columns (both surfaces)

| Column | Type | Description |
|---|---|---|
| `id` | Int64 | Devpost numeric hackathon id |
| `title` | Utf8 | Hackathon title |
| `url` | Utf8 | Canonical hackathon URL |
| `organizer` | Utf8 | Hosting organization name |
| `location` | Utf8 | Displayed location (e.g. "Online") |
| `status` | Utf8 | Open state (open/upcoming/ended) |
| `tracks` | Utf8 | Comma-joined theme names |
| `registrations_count` | Int64 | Number of registered participants |
| `featured` | Boolean | Whether Devpost features this hackathon |
| `invite_only` | Boolean | Whether participation is invite-only |
| `prize_amount_raw` | Utf8 | Raw prize markup (HTML); parse downstream |
| `submission_period_dates_raw` | Utf8 | Human-readable window; parse downstream |
| `time_left_to_submission` | Utf8 | Human-readable time remaining |
| `thumbnail_url` | Utf8 | Thumbnail image URL |
| `submission_gallery_url` | Utf8 | Project submission gallery URL |

## Notes

Two fields are returned un-normalized by Devpost and exposed as `_raw`:

- `prize_amount_raw` — HTML, e.g. `"$<span data-currency-value>60,000</span>"`.
- `submission_period_dates_raw` — text, e.g. `"May 05 - Jun 11, 2026"`.

They are left raw because Coral's `format_timestamp` helper only accepts
epoch/ISO-8601 inputs; parse them in your own layer.
