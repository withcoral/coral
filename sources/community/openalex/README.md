# OpenAlex

Query the [OpenAlex](https://openalex.org) scholarly database — a free, open
catalog of 250 M+ academic works, 90 M+ authors, 250 K+ sources, 100 K+
institutions, and 4 500+ research topics.

## Setup

No authentication is required. For higher rate limits (10 req/s instead of
1 req/s), set the `OPENALEX_EMAIL` input to your email address to join the
[polite pool](https://docs.openalex.org/how-to-use-the-api/rate-limits-and-authentication#the-polite-pool).

## Tables

| Table          | Description                                           |
| -------------- | ----------------------------------------------------- |
| `works`        | Scholarly works — articles, preprints, books, etc.    |
| `authors`      | Researchers with ORCID, citation counts, and topics.  |
| `sources`      | Journals, repositories, and conference proceedings.   |
| `institutions` | Universities, companies, and research organizations.  |
| `topics`       | Research topic taxonomy (domain/field/subfield/topic). |

## Filters

Every table supports:

| Filter   | Description                                                                        |
| -------- | ---------------------------------------------------------------------------------- |
| `search` | Full-text keyword search across names and titles.                                  |
| `filter` | [Structured filtering](https://docs.openalex.org/how-to-use-the-api/get-lists-of-entities/filter-entity-lists) using OpenAlex filter syntax. |
| `sort`   | Sort results (e.g. `cited_by_count:desc`, `publication_year:asc`).                 |

## Example queries

```sql
-- Search for works about "CRISPR" published in 2024, sorted by citations
SELECT title, publication_year, cited_by_count, is_oa
FROM openalex.works
WHERE search = 'CRISPR'
  AND filter = 'publication_year:2024'
  AND sort = 'cited_by_count:desc'
LIMIT 20;

-- Find an author by name
SELECT display_name, works_count, cited_by_count, orcid
FROM openalex.authors
WHERE search = 'Yann LeCun'
LIMIT 5;

/*
+--------------+-------------+----------------+
| display_name | works_count | cited_by_count |
+--------------+-------------+----------------+
| Yann LeCun   | 480         | 249194         |
| Yann Lecun   | 45          | 1932           |
| Yann LeCun   | 1           | 0              |
| Yann LeCun   | 2           | 0              |
| Yann LeCun   | 1           | 0              |
+--------------+-------------+----------------+
*/

-- List open-access journals with the most works
SELECT display_name, works_count, cited_by_count, apc_usd
FROM openalex.sources
WHERE filter = 'is_oa:true,type:journal'
  AND sort = 'works_count:desc'
LIMIT 10;

-- Find US universities sorted by citation count
SELECT display_name, geo__city, geo__region, works_count, cited_by_count
FROM openalex.institutions
WHERE filter = 'country_code:US,type:education'
  AND sort = 'cited_by_count:desc'
LIMIT 10;

-- Browse research topics in Computer Science
SELECT display_name, subfield__name, works_count
FROM openalex.topics
WHERE filter = 'field.id:fields/17'
  AND sort = 'works_count:desc'
LIMIT 10;
```

## Links

- [OpenAlex API documentation](https://docs.openalex.org/)
- [OpenAlex filter reference](https://docs.openalex.org/how-to-use-the-api/get-lists-of-entities/filter-entity-lists)
- [OpenAlex entity schemas](https://docs.openalex.org/api-entities)
