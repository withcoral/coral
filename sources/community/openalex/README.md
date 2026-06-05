# OpenAlex

Query the [OpenAlex](https://openalex.org) scholarly database — a free, open
catalog of 250 M+ academic works, 90 M+ authors, 250 K+ sources, 100 K+
institutions, and 4 500+ research topics.

```bash
coral source add --file sources/community/openalex/manifest.yaml
```

## Setup

OpenAlex recommends a free API key for reliable access. Get one at [openalex.org/settings/api](https://openalex.org/settings/api).
The key provides a $1/day free allowance, with different effective capacities for search versus list/filter calls.

Provide the key during setup:

```bash
coral source add --file sources/community/openalex/manifest.yaml --interactive
```
*(When prompted for `OPENALEX_API_KEY`, paste your key.)*

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
| `filter_value` | [Structured filtering](https://developers.openalex.org/guides/filtering) using OpenAlex filter syntax. |
| `sort_value`   | Sort results (e.g. `cited_by_count:desc`, `publication_year:asc`).                 |

## Example queries

```sql
-- Search for works about "CRISPR" published in 2024, sorted by citations
SELECT title, publication_year, cited_by_count, is_oa
FROM openalex.search_works(q => 'CRISPR', filter => 'publication_year:2024', sort => 'cited_by_count:desc')
LIMIT 5;

/*
+------------------------------------------------------------------------------------+------------------+----------------+-------+
| title                                                                              | publication_year | cited_by_count | is_oa |
+------------------------------------------------------------------------------------+------------------+----------------+-------+
| NF-κB in biology and targeted therapy: new insights and translational implications | 2024             | 1640           | true  |
| The cell biology of ferroptosis                                                    | 2024             | 1129           | true  |
| Exploring treatment options in cancer: tumor treatment strategies                  | 2024             | 933            | true  |
| Glucagon-like peptide-1 receptor: mechanisms and advances in therapy               | 2024             | 754            | true  |
| Ferroptosis in cancer: from molecular mechanisms to therapeutic strategies         | 2024             | 672            | true  |
+------------------------------------------------------------------------------------+------------------+----------------+-------+
*/

-- Find an author by name
SELECT display_name, works_count, cited_by_count, orcid
FROM openalex.search_authors(q => 'Yann LeCun')
LIMIT 5;

/*
+--------------+-------------+----------------+-------+
| display_name | works_count | cited_by_count | orcid |
+--------------+-------------+----------------+-------+
| Yann LeCun   | 480         | 249194         |       |
| Yann Lecun   | 45          | 1932           |       |
| Yann LeCun   | 1           | 0              |       |
| Yann LeCun   | 2           | 0              |       |
| Yann LeCun   | 1           | 0              |       |
+--------------+-------------+----------------+-------+
*/

-- List open-access journals with the most works
SELECT display_name, works_count, cited_by_count, apc_usd
FROM openalex.sources
WHERE filter_value = 'is_oa:true,type:journal'
  AND sort_value = 'works_count:desc'
LIMIT 5;

/*
+--------------------------------------+-------------+----------------+---------+
| display_name                         | works_count | cited_by_count | apc_usd |
+--------------------------------------+-------------+----------------+---------+
| Medical Entomology and Zoology       | 1875288     | 13152095       |         |
| PLoS ONE                             | 339206      | 12328711       | 1805    |
| Scientific Reports                   | 296504      | 7718666        | 2190    |
| Socio-Environmental Systems Modeling | 217660      | 230604         |         |
| Journal of Physics Conference Series | 215906      | 793399         |         |
+--------------------------------------+-------------+----------------+---------+
*/

-- Find US universities sorted by citation count
SELECT display_name, geo__city, geo__region, works_count, cited_by_count
FROM openalex.institutions
WHERE filter_value = 'country_code:US,type:education'
  AND sort_value = 'cited_by_count:desc'
LIMIT 5;

/*
+--------------------------+-----------+---------------+-------------+----------------+
| display_name             | geo__city | geo__region   | works_count | cited_by_count |
+--------------------------+-----------+---------------+-------------+----------------+
| Harvard University       | Cambridge | Massachusetts | 696902      | 142092461      |
| University of Washington | Seattle   | Washington    | 515470      | 92495233       |
| Stanford University      | Stanford  | California    | 520581      | 85197652       |
| Johns Hopkins University | Baltimore | Maryland      | 489825      | 74260647       |
| University of Michigan   | Ann Arbor | Michigan      | 974426      | 64969674       |
+--------------------------+-----------+---------------+-------------+----------------+
*/

-- Browse research topics in Computer Science
SELECT display_name, subfield__name, works_count
FROM openalex.topics
WHERE filter_value = 'field.id:fields/17'
  AND sort_value = 'works_count:desc'
LIMIT 5;

/*
+-----------------------------------------------+-------------------------------+-------------+
| display_name                                  | subfield__name                | works_count |
+-----------------------------------------------+-------------------------------+-------------+
| Geochemistry and Geologic Mapping             | Artificial Intelligence       | 3956270     |
| Computational Physics and Python Applications | Artificial Intelligence       | 413037      |
| Research Data Management Practices            | Information Systems           | 373169      |
| History of Computing Technologies             | Computer Science Applications | 346754      |
| Educational Methods and Media Use             | Information Systems           | 311168      |
+-----------------------------------------------+-------------------------------+-------------+
*/
```

## Links

- [OpenAlex API documentation](https://developers.openalex.org/)
- [OpenAlex filter reference](https://developers.openalex.org/guides/filtering)
- [OpenAlex authentication](https://developers.openalex.org/guides/authentication)

## Local Testing

```bash
OPENALEX_API_KEY=<key> coral source add --file sources/community/openalex/manifest.yaml
# Added source openalex
#
#   ✓ openalex connected successfully
#
#     openalex (5 tables)
#     ├─ authors
#     ├─ institutions
#     ├─ sources
#     ├─ topics
#     └─ works
#     Query tests
#     3 declared · 3 passed · 0 failed
#
#     ✓ SELECT title, publication_year, cited_by_count FROM openalex.search_works(q => 'machine learning') LIMIT 1
#       1 row
#
#     ✓ SELECT display_name, works_count, cited_by_count FROM openalex.search_authors(q => 'einstein') LIMIT 1
#       1 row
#
#     ✓ SELECT display_name, works_count FROM openalex.topics LIMIT 1
#       1 row

coral source test openalex
#   ✓ openalex connected successfully
#
#     openalex (5 tables)
#     ├─ authors
#     ├─ institutions
#     ├─ sources
#     ├─ topics
#     └─ works
#     Query tests
#     3 declared · 3 passed · 0 failed
#
#     ✓ SELECT title, publication_year, cited_by_count FROM openalex.search_works(q => 'machine learning') LIMIT 1
#       1 row
#
#     ✓ SELECT display_name, works_count, cited_by_count FROM openalex.search_authors(q => 'einstein') LIMIT 1
#       1 row
#
#     ✓ SELECT display_name, works_count FROM openalex.topics LIMIT 1
#       1 row

coral sql "SELECT title, publication_year, cited_by_count FROM openalex.search_works(q => 'machine learning') LIMIT 3"
# +------------------------------------------------------------------+------------------+----------------+
# | title                                                            | publication_year | cited_by_count |
# +------------------------------------------------------------------+------------------+----------------+
# | Scikit-learn: Machine Learning in Python                         | 2012             | 63647          |
# | Genetic algorithms in search, optimization, and machine learning | 1989             | 49332          |
# | C4.5: Programs for Machine Learning                              | 1992             | 23695          |
# +------------------------------------------------------------------+------------------+----------------+
```

