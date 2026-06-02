# Crossref — Coral Community Source

This source connects [Coral](https://withcoral.com) to the **Crossref REST API**, a bibliographic database covering over 150 million scholarly works. It lets you query academic metadata — citations, publishers, funders, journals — using plain SQL.

---

## What You Can Do

Crossref indexes:

- **Works** — journal articles, book chapters, conference papers, preprints, datasets, grants
- **Members** — publishers and learned societies that deposit works
- **Journals** — every ISSN-identified title in the registry
- **Funders** — funding bodies from the Crossref Funder Registry
- **Licenses** — open-access metadata, embargo periods, content versions
- **Types** — controlled vocabulary for work types

---

## Setup

No API token needed. All endpoints are public.

```bash
coral source add --file sources/community/crossref/manifest.yaml
```

It's recommended to set your email address. Crossref routes requests with a contact email into their **"polite pool"**, which has better rate limits and stability.

```bash
export CROSSREF_EMAIL=you@yourorg.org
```

---

## Tables

| Table | Description |
|-------|-------------|
| `crossref.works` | Scholarly works — articles, books, preprints, datasets, and more |
| `crossref.members` | Publisher and repository members of Crossref |
| `crossref.journals` | ISSN-identified journal titles |
| `crossref.funders` | Research funding bodies (Crossref Funder Registry) |
| `crossref.licenses` | Content licenses declared on works |
| `crossref.types` | Controlled vocabulary of work types |

## Functions

| Function | Description |
|----------|-------------|
| `crossref.search_works(q)` | Full-text relevance search across all works |
| `crossref.search_funders(q)` | Search the Funder Registry by name |

---

## Schema

### `crossref.works`

Each row is one scholarly work.

| Column | Type | Description |
|--------|------|-------------|
| `doi` | text | Digital Object Identifier |
| `title` | text | Primary title |
| `type` | text | Work type (e.g. `journal-article`, `book-chapter`) |
| `publisher` | text | Publisher name as deposited |
| `container_title` | text | Journal or book series title |
| `is_referenced_by_count` | integer | Number of Crossref works that cite this DOI |
| `references_count` | integer | Number of references listed in this work |
| `score` | float | Relevance score (present when `query=` is used) |
| `published_print_year` | text | Print publication year |
| `published_online_year` | text | Online publication year |
| `issn` | text | ISSNs of the containing journal |
| `subject` | text | Discipline tags (comma-separated) |
| `language` | text | BCP-47 language code |
| `abstract` | text | Abstract text (may contain JATS XML) |
| `license_url` | text | License URLs (comma-separated) |
| `member` | text | Crossref member ID |
| `prefix` | text | DOI prefix, e.g. `10.1038` |

**Available filters on `crossref.works`:**

| Filter | Description |
|--------|-------------|
| `query` | Keyword search across titles, authors, and metadata |
| `filter` | Raw Crossref filter string, e.g. `type:journal-article,from-pub-date:2024-01` |
| `sort` | Sort field: `score`, `relevance`, `updated`, `deposited`, `published` |
| `order` | `asc` or `desc` |

---

### `crossref.members`

| Column | Type | Description |
|--------|------|-------------|
| `id` | integer | Numeric Crossref member ID |
| `primary_name` | text | Name of the member organisation |
| `location` | text | Country or city |
| `total_dois` | integer | Total DOIs registered |
| `current_dois` | integer | DOIs for content in the last two years |
| `backfile_dois` | integer | DOIs for older backfile content |
| `prefixes` | text | DOI prefixes owned (comma-separated) |
| `coverage_orcids_current` | float | Fraction of current works with ORCID metadata |
| `coverage_references_current` | float | Fraction of current works with reference lists |

---

### `crossref.journals`

| Column | Type | Description |
|--------|------|-------------|
| `issn` | text | ISSNs (comma-separated) |
| `title` | text | Journal title |
| `publisher` | text | Publisher name |
| `total_dois` | integer | Total DOIs registered for this journal |
| `current_dois` | integer | DOIs for recent content |
| `backfile_dois` | integer | DOIs for older content |
| `coverage_abstracts_current` | float | Fraction of current articles with abstracts |

---

### `crossref.funders`

| Column | Type | Description |
|--------|------|-------------|
| `id` | text | Funder DOI (e.g. `10.13039/100000001`) |
| `name` | text | Primary name of the funder |
| `alt_names` | text | Alternative names (comma-separated) |
| `location` | text | Country where the funder is based |
| `work_count` | integer | Total works linked to this funder |
| `descendant_work_count` | integer | Works linked to this funder and all descendants |

---

### `crossref.licenses`

| Column | Type | Description |
|--------|------|-------------|
| `url` | text | Canonical URL of the license |
| `work_count` | integer | Number of works using this license |
| `content_version` | text | `vor`, `am`, `tdm`, or `unspecified` |
| `delay_in_days` | integer | Embargo period before the license applies |

---

### `crossref.types`

| Column | Type | Description |
|--------|------|-------------|
| `id` | text | Machine-readable type ID (e.g. `journal-article`) |
| `label` | text | Human-readable label (e.g. `Journal Article`) |

---

## Example Queries

### Most-cited machine learning works

```sql
SELECT doi, title, publisher, is_referenced_by_count
FROM crossref.works
WHERE query = 'machine learning'
ORDER BY is_referenced_by_count DESC
LIMIT 10;
```

---

### Browse all work types

```sql
SELECT id, label
FROM crossref.types
LIMIT 20;
```

---

### Filter to journal articles only

```sql
SELECT doi, title, type
FROM crossref.works
WHERE filter = 'type:journal-article'
LIMIT 5;
```

---

### Search works by relevance score

```sql
SELECT doi, title, score, is_referenced_by_count
FROM crossref.search_works(q => 'quantum computing')
LIMIT 5;
```

---

### Look up a publisher

```sql
SELECT id, primary_name, total_dois, current_dois
FROM crossref.members
WHERE query = 'Elsevier'
LIMIT 5;
```

---

### Find funders by country

```sql
SELECT id, name, location, work_count
FROM crossref.search_funders(q => 'Japan')
LIMIT 5;
```

---

### Check Creative Commons license coverage

```sql
SELECT url, work_count, delay_in_days
FROM crossref.licenses
WHERE query = 'creative commons'
LIMIT 10;
```

---

## Rate Limiting & Polite Pool

Crossref has two request pools:

- **Anonymous pool** — no email provided; lower throughput
- **Polite pool** — email provided; higher stability and rate limits

Set your email to use the polite pool:

```bash
export CROSSREF_EMAIL=you@yourorg.org
```

The source automatically includes your email in the `User-Agent` header on every request.

---

## Validation

```bash
coral source lint sources/community/crossref/manifest.yaml
coral source test crossref
```

Quick checks:

```bash
coral sql "SELECT doi, title FROM crossref.works LIMIT 1"
coral sql "SELECT id, label FROM crossref.types LIMIT 5"
coral sql "SELECT id, name FROM crossref.funders WHERE query = 'NIH' LIMIT 3"
```

---

## Limitations

- **Read-only.** No writes or mutations.
- `work_count` on `crossref.funders` may return null for some entries — this is a Crossref API quirk.
- The `abstract` field may contain raw JATS XML instead of plain text.
- `is_referenced_by_count` only reflects works registered within Crossref, not all citations globally.
- Large scans without filters can be slow. Use `query=`, `filter=`, or `LIMIT` to keep things efficient.

---

## References

- [Crossref REST API Documentation](https://api.crossref.org)
- [Crossref REST API GitHub](https://github.com/CrossRef/rest-api-doc)
- [Crossref Funder Registry](https://www.crossref.org/services/funder-registry/)
- [Polite Pool Info](https://github.com/CrossRef/rest-api-doc#etiquette)