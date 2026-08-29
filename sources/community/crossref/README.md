# Crossref

Query scholarly works, DOI metadata, journal publications, members, and funders
from the public [Crossref REST API](https://api.crossref.org/). No
authentication is required.

Crossref is useful for agents that need to resolve DOIs, inspect publication
metadata, check citation counts, discover works from a journal ISSN, or connect
research outputs to publishers and funders.

## Tables

- `works`: search scholarly works by keyword and optional Crossref filters.
- `work_by_doi`: resolve one DOI to its Crossref metadata.
- `journal_works`: list works for a journal by ISSN.
- `members`: browse Crossref member organizations.
- `funders`: search funding organizations.

## Example Queries

Search works:

```sql
SELECT doi, title, publisher, container_title, is_referenced_by_count
FROM crossref.works
WHERE query = 'machine learning'
LIMIT 5;
```

Resolve a DOI:

```sql
SELECT doi, title, publisher, container_title, issued, author
FROM crossref.work_by_doi
WHERE doi = '10.1038/nphys1170';
```

List recent journal works:

```sql
SELECT doi, title, publisher, container_title
FROM crossref.journal_works
WHERE issn = '1932-6203'
  AND filter = 'from-pub-date:2024-01-01,type:journal-article'
LIMIT 5;
```

Find funders:

```sql
SELECT id, name, location, alt_names
FROM crossref.funders
WHERE query = 'National Science Foundation'
LIMIT 5;
```

## Notes

Crossref asks API clients to identify themselves. This source sends a descriptive
`User-Agent` header with every request.
