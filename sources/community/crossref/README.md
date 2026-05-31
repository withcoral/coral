# Crossref — Coral Community Source

> *"I have learned, in whatsoever state I am, therewith to be content."*
> — Philippians 4:11

Greetings, fellow builder. Whether you have come to this source in search of scholarly metadata, citation counts, or the names of those who fund the great work of human knowledge — you have come to the right place. This source will not fail you, if you use it faithfully.

---

## The Problem We Are Solving

The world of academic publishing is vast, scattered, and — if we are honest — difficult to navigate programmatically. You want to ask a simple question: *how many times has this paper been cited? Who published it? What funder made this research possible?* And yet, to answer these questions, you once had to read API documentation, craft HTTP requests by hand, and wrestle JSON into something usable.

We take no pleasure in unnecessary complexity. So we built this.

This source connects [Coral](https://withcoral.com) to the **Crossref REST API** — one of the most comprehensive bibliographic databases on earth, covering over **150 million scholarly works** — and exposes it as clean, queryable SQL tables. You write a query. You get rows. It is that simple.

---

## Why This Is Useful

Crossref is not merely a DOI registry. It is a living index of the scholarly record:

- **Works** — journal articles, book chapters, conference papers, preprints, datasets, grants
- **Members** — the publishers and learned societies who deposit those works
- **Journals** — every ISSN-identified title in the registry
- **Funders** — the funding bodies whose money made the research possible
- **Licenses** — open-access metadata, embargo periods, content versions
- **Types** — a controlled vocabulary of what kind of thing a work actually is

With this source, a single SQL query can answer questions that used to require hours of scripting. You can audit open-access coverage. You can find the most-cited papers in a field. You can discover which funders are behind quantum computing research. The power is yours.

---

## Setup

No API token is required. All endpoints are public.

```bash
coral source add --file sources/community/crossref/manifest.yaml
```

We strongly recommend providing your email address. Crossref routes requests with a contact email into their **"polite pool"** — a more stable lane with better rate limits. Set it as an environment variable or pass it at runtime:

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
| `crossref.licenses` | Content licences declared on works |
| `crossref.types` | Controlled vocabulary of work types |

## Functions

| Function | Description |
|----------|-------------|
| `crossref.search_works(q)` | Full-text relevance search across all works |
| `crossref.search_funders(q)` | Search the Funder Registry by name |

---

## Schema

### `crossref.works`

The heart of the registry. Each row is one scholarly work.

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
| `published_print_year` | text | Print publication date |
| `published_online_year` | text | Online publication date |
| `issn` | text | ISSNs of the containing journal |
| `subject` | text | Discipline tags (comma-separated) |
| `language` | text | BCP-47 language code |
| `abstract` | text | Abstract text (may contain JATS XML) |
| `license_url` | text | Licence URLs (comma-separated) |
| `member` | text | Crossref member ID |
| `prefix` | text | DOI prefix, e.g. `10.1038` |

**Filters available on `crossref.works`:**

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
| `url` | text | Canonical URL of the licence |
| `work_count` | integer | Number of works using this licence |
| `content_version` | text | `vor`, `am`, `tdm`, or `unspecified` |
| `delay_in_days` | integer | Embargo period before the licence applies |

---

### `crossref.types`

| Column | Type | Description |
|--------|------|-------------|
| `id` | text | Machine-readable type ID (e.g. `journal-article`) |
| `label` | text | Human-readable label (e.g. `Journal Article`) |

---

## Example Queries

### Find the most-cited machine learning works

```sql
SELECT doi, title, publisher, is_referenced_by_count
FROM crossref.works
WHERE query = 'machine learning'
ORDER BY is_referenced_by_count DESC
LIMIT 10;
```

```
+------------------------------------+---------------------------------------+------------------+------------------------+
| doi                                | title                                 | publisher        | is_referenced_by_count |
+------------------------------------+---------------------------------------+------------------+------------------------+
| 10.1093/oso/9780198828044.003.0003 | Machine learning with sklearn         | Oxford Univ Prs  | 19                     |
| 10.1093/oso/9780190941659.003.0001 | Why Use Automated Machine Learning?   | Oxford Univ Prs  | 10                     |
...
```

---

### Browse all work types

```sql
SELECT id, label
FROM crossref.types
LIMIT 20;
```

```
+------------------+------------------+
| id               | label            |
+------------------+------------------+
| journal-article  | Journal Article  |
| book-chapter     | Book Section     |
| posted-content   | Posted Content   |
| dataset          | Dataset          |
...
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

```
+--------------------------------------+------------------------------------------+-----------+
| doi                                  | title                                    | score     |
+--------------------------------------+------------------------------------------+-----------+
| 10.1093/oso/9780198854227.003.0015   | Quantum Computing I                      | 18.67     |
| 10.1093/oso/9780198854227.003.0016   | Quantum Computing II                     | 18.66     |
...
```

---

### Look up a publisher's footprint

```sql
SELECT id, primary_name, total_dois, current_dois
FROM crossref.members
WHERE query = 'Elsevier'
LIMIT 5;
```

```
+-------+------------------+------------+--------------+
| id    | primary_name     | total_dois | current_dois |
+-------+------------------+------------+--------------+
| 78    | Elsevier BV      | 24925608   | 3364598      |
...
```

---

### Find funders by country

```sql
SELECT id, name, location, work_count
FROM crossref.search_funders(q => 'Japan')
LIMIT 5;
```

---

### Audit Creative Commons licence coverage

```sql
SELECT url, work_count, delay_in_days
FROM crossref.licenses
WHERE query = 'creative commons'
LIMIT 10;
```

---

## Rate Limiting & The Polite Pool

Crossref operates two request pools:

- **Anonymous pool** — no email provided; lower throughput, less stable
- **Polite pool** — email provided via `CROSSREF_EMAIL`; higher stability and rate limits

Set your email. It costs nothing and gains you much:

```bash
export CROSSREF_EMAIL=you@yourorg.org
```

The source automatically passes your email in the `User-Agent` header on every request.

---

## Validation

Run the smoke tests to confirm the source is wired correctly:

```bash
coral source lint sources/community/crossref/manifest.yaml
coral source test crossref
```

Ad-hoc checks:

```bash
coral sql "SELECT doi, title FROM crossref.works LIMIT 1"
coral sql "SELECT id, label FROM crossref.types LIMIT 5"
coral sql "SELECT id, name FROM crossref.funders WHERE query = 'NIH' LIMIT 3"
```

---

## Limitations

- **Read-only.** No writes, deposits, or mutations of any kind.
- `work_count` on `crossref.funders` may return null for some entries; this is a Crossref API quirk.
- The `abstract` field may contain raw JATS XML rather than plain text.
- `is_referenced_by_count` reflects only works registered within Crossref, not all citations in existence.
- Very large scans without filters may be slow. Use `query=`, `filter=`, or explicit `LIMIT` clauses to stay efficient.

---

## References

- [Crossref REST API Documentation](https://api.crossref.org)
- [Crossref REST API GitHub](https://github.com/CrossRef/rest-api-doc)
- [Crossref Funder Registry](https://www.crossref.org/services/funder-registry/)
- [Polite Pool Information](https://github.com/CrossRef/rest-api-doc#etiquette)

---

## A Final Word

We did not build this source so that you might be impressed by it. We built it so that it might serve you — faithfully, reliably, and without complaint. The knowledge of the world's scholars is indexed here. Ask of it freely.

Go now. Query well. And let the data speak plainly.

> *"Now unto him that is able to do exceeding abundantly above all that we ask or think — be glory."*
> — Ephesians 3:20