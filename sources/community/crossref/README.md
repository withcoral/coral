# Crossref — Coral Community Source

This source connects [Coral](https://withcoral.com) to the **Crossref REST API**, a bibliographic database covering over 150 million scholarly works. It lets you query academic metadata — citations, publishers, funders, journals — using plain SQL.

---

## What You Can Do

Crossref indexes:

- **Works** — journal articles, book chapters, conference papers, preprints, datasets, grants
- **Members** — publishers and learned societies that deposit works
- **Journals** — every ISSN-identified title in the registry
- **Funders** — funding bodies from the Crossref Funder Registry
- **Licenses** — license URLs and aggregate work counts across all works
- **Types** — controlled vocabulary for work types
- **Authors** — author metadata surfaced from works (names, ORCIDs, affiliations)
- **References** — reference lists surfaced from works (DOIs, titles, citation strings)

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
| `crossref.licenses` | License URLs and aggregate work counts declared on works |
| `crossref.types` | Controlled vocabulary of work types |
| `crossref.authors` | Author metadata surfaced from works (one row per work) |
| `crossref.references` | Reference lists surfaced from works (one row per work) |

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
| `score` | float | Relevance score; only populated when a `query=` filter is supplied, null otherwise |
| `created_date_parts` | text | Deposit date as a date-parts string, e.g. `2024;3;15` |
| `indexed_date_parts` | text | Last-indexed date as a date-parts string |
| `issn` | text | ISSNs of the containing journal (comma-separated) |
| `subject` | text | Discipline tags (comma-separated) |
| `url` | text | Canonical URL for the work |

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

---

### `crossref.journals`

| Column | Type | Description |
|--------|------|-------------|
| `issn` | text | ISSNs (comma-separated) |
| `title` | text | Journal title |
| `publisher` | text | Publisher name |

---

### `crossref.funders`

Each row is one funding body from the Crossref Funder Registry. The `/funders` list endpoint returns id, name, location, and uri only — it does not expose work counts or descendant counts. Use `crossref.search_funders()` to search funders by name.

| Column | Type | Description |
|--------|------|-------------|
| `id` | text | Numeric funder ID (e.g. `100000001`) |
| `name` | text | Primary name of the funder |
| `location` | text | Country where the funder is based |
| `uri` | text | Registry URI for this funder |

---

### `crossref.licenses`

Each row is one distinct license URL with an aggregate work count. `content_version` and `delay_in_days` are per-work sub-fields and are not available at this aggregate endpoint.

| Column | Type | Description |
|--------|------|-------------|
| `url` | text | License URL as returned by the API (casing varies — may be uppercase or mixed case) |
| `work_count` | integer | Number of works using this license |

---

### `crossref.types`

| Column | Type | Description |
|--------|------|-------------|
| `id` | text | Machine-readable type ID (e.g. `journal-article`) |
| `label` | text | Human-readable label (e.g. `Journal Article`) |

---

### `crossref.authors`

Each row is one work. Author fields contain comma-separated values across all authors on that work. The `query` and `filter` columns echo the filter values you supplied — they are not fields returned by the Crossref API.

| Column | Type | Description |
|--------|------|-------------|
| `query` | text | Echoes the `query` filter value you supplied |
| `filter` | text | Echoes the `filter` filter value you supplied |
| `doi` | text | DOI of the work |
| `given` | text | Given (first) names of all authors, comma-separated |
| `family` | text | Family (last) names of all authors, comma-separated |
| `sequence` | text | Author order values (`first`, `additional`), comma-separated |
| `orcid` | text | ORCID identifier URLs, comma-separated, if deposited |
| `affiliation` | text | Affiliation names across all authors, comma-separated |

**Available filters on `crossref.authors`:**

| Filter | Description |
|--------|-------------|
| `query` | Keyword search across titles, authors, and metadata |
| `filter` | Raw Crossref filter string, e.g. `from-pub-date:2024-01` |

---

### `crossref.references`

Each row is one work. Reference fields contain pipe-separated values across all references in that work. The `query` and `filter` columns echo the filter values you supplied — they are not fields returned by the Crossref API.

`source_doi` is the DOI of the **citing** work (the work whose reference list is being unpacked). The `doi` column contains the DOIs of the **cited** works, pipe-separated where resolved.

| Column | Type | Description |
|--------|------|-------------|
| `query` | text | Echoes the `query` filter value you supplied |
| `filter` | text | Echoes the `filter` filter value you supplied |
| `source_doi` | text | DOI of the citing work whose reference list is being unpacked |
| `doi` | text | DOIs of the cited (referenced) works, pipe-separated, where resolved |
| `article_title` | text | Titles of referenced articles, pipe-separated, where deposited |
| `author` | text | First authors of referenced works, pipe-separated, where deposited |
| `journal_title` | text | Journal titles of referenced works, pipe-separated, where deposited |
| `year` | text | Publication years of referenced works, pipe-separated, where deposited |
| `unstructured` | text | Raw citation strings, pipe-separated, where structured fields unavailable |

**Available filters on `crossref.references`:**

| Filter | Description |
|--------|-------------|
| `query` | Keyword search across titles, authors, and metadata |
| `filter` | Raw Crossref filter string, e.g. `from-pub-date:2024-01` |

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
SELECT id, primary_name, total_dois
FROM crossref.members
WHERE query = 'Elsevier'
LIMIT 5;
```

---

### Find authors on machine learning works

```sql
SELECT doi, given, family, sequence, orcid
FROM crossref.authors
WHERE query = 'machine learning'
LIMIT 10;
```

---

### Browse reference lists for recent works

```sql
SELECT source_doi, doi, article_title, year
FROM crossref.references
WHERE filter = 'from-pub-date:2024-01'
LIMIT 10;
```

---

### Check Creative Commons license coverage

```sql
SELECT url, work_count
FROM crossref.licenses
WHERE query = 'creative commons'
LIMIT 10;
```

---

### Search funders by name

```sql
SELECT id, name, location
FROM crossref.search_funders(q => 'Japan')
LIMIT 5;
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
coral sql "SELECT doi, given, family FROM crossref.authors WHERE query = 'machine learning' LIMIT 3"
coral sql "SELECT source_doi, article_title FROM crossref.references WHERE query = 'machine learning' LIMIT 3"
```

---

## Limitations

- **Read-only.** No writes or mutations.
- `is_referenced_by_count` only reflects works registered within Crossref, not all citations globally.
- `score` in `crossref.works` is only populated when a `query=` filter is supplied; it is null for unfiltered or filter-only scans.
- `crossref.funders` and `crossref.search_funders()` both expose id, name, location, and uri only. The `/funders` list endpoint does not return work counts or descendant counts; those are available only via individual funder detail pages (`/funders/{id}`), which this source does not expose.
- `crossref.licenses` aggregates by license URL only; `content_version` and `delay_in_days` are per-work sub-fields not available at this endpoint.
- Large scans without filters can be slow. Use a `query` filter, a `filter` filter, or `LIMIT` to keep things efficient.
- Crossref caps offset-based pagination at 10,000 results on `/works`-backed tables (`crossref.works`, `crossref.authors`, `crossref.references`).

---

## References

- [Crossref REST API Documentation](https://api.crossref.org)
- [Crossref REST API GitHub](https://github.com/CrossRef/rest-api-doc)
- [Crossref Funder Registry](https://www.crossref.org/services/funder-registry/)
- [Polite Pool Info](https://github.com/CrossRef/rest-api-doc#etiquette)