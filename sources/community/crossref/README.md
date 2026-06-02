# Crossref — Coral Community Source

Welcome to the Crossref community source for Coral. This source provides a streamlined way to query scholarly metadata, citation counts, and funding information directly using SQL.

---

## Overview

The world of academic publishing is vast and distributed. To answer questions like *how many times has this paper been cited?*, *who published it?*, or *what funder made this research possible?*, developers usually have to read API documentation, craft HTTP requests by hand, and parse JSON responses.

This source connects [Coral](https://withcoral.com) to the **Crossref REST API** — one of the most comprehensive bibliographic databases in the world, covering over **150 million scholarly works** — and exposes it as clean, queryable SQL tables.

---

## Features

Crossref is a living index of the scholarly record. With this source, you can query:

- **Works** — journal articles, book chapters, conference papers, preprints, datasets, and grants.
- **Members** — the publishers and learned societies who deposit those works.
- **Journals** — every ISSN-identified title in the registry.
- **Funders** — the funding bodies whose money made the research possible.
- **Licenses** — open-access metadata, embargo periods, and content versions.
- **Types** — a controlled vocabulary of work types.

You can use this to audit open-access coverage, find the most-cited papers in a specific field, or discover which funders are backing certain research topics.

---

## Setup

No API token is required. All endpoints are public.

```bash
coral source add --file sources/community/crossref/manifest.yaml
We strongly recommend providing your email address. Crossref routes requests with a contact email into their "polite pool" — a more stable lane with better rate limits. Set it as an environment variable or pass it at runtime:
code
Bash
export CROSSREF_EMAIL=you@yourorg.org
Tables
Table	Description
crossref.works	Scholarly works — articles, books, preprints, datasets, and more. Note: Uses offset pagination with a Crossref-mandated 10,000 result limit.
crossref.members	Publisher and repository members of Crossref
crossref.journals	ISSN-identified journal titles
crossref.funders	Research funding bodies (Crossref Funder Registry)
crossref.licenses	Content licences declared on works
crossref.types	Controlled vocabulary of work types
Functions
Function	Description
crossref.search_works(q)	Full-text relevance search across all works
crossref.search_funders(q)	Search the Funder Registry by name
Schema
crossref.works
The primary table for scholarly works. Each row represents one registered DOI. Uses offset pagination (capped at 10,000 results per Crossref rules).
Column	Type	Description
doi	text	Digital Object Identifier
title	text	Primary title
type	text	Work type (e.g. journal-article, book-chapter)
created_date_parts	text	Deposit date as a date-parts string, e.g. "2024;3;15"
indexed_date_parts	text	Last-indexed date as a date-parts string, e.g. "2024;3;15"
is_referenced_by_count	integer	Number of Crossref works that cite this DOI
publisher	text	Publisher name as deposited
container_title	text	Journal or book series title
issn	text	ISSNs of the containing journal
subject	text	Discipline tags (comma-separated)
score	float	Relevance score
url	text	Crossref DOI resolution URL
crossref.members
Column	Type	Description
id	integer	Numeric Crossref member ID
primary_name	text	Name of the member organisation
location	text	Country or city
total_dois	integer	Total DOIs registered
crossref.journals
Column	Type	Description
issn	text	ISSNs (comma-separated)
title	text	Journal title
publisher	text	Publisher name
crossref.funders
Column	Type	Description
id	text	Funder DOI (e.g. 10.13039/100000001)
name	text	Primary name of the funder
location	text	Country where the funder is based
uri	text	Primary URI associated with the funder
crossref.licenses
Column	Type	Description
url	text	Canonical URL of the licence
work_count	integer	Number of works using this licence
crossref.types
Column	Type	Description
id	text	Machine-readable type ID (e.g. journal-article)
label	text	Human-readable label (e.g. Journal Article)
Example Queries
Browse all work types
code
SQL
SELECT id, label
FROM crossref.types
LIMIT 5;
code
Text
+------------------+------------------+
| id               | label            |
+------------------+------------------+
| journal-article  | Journal Article  |
| book-chapter     | Book Section     |
| posted-content   | Posted Content   |
| dataset          | Dataset          |
| peer-review      | Peer Review      |
+------------------+------------------+
Search works by relevance score
code
SQL
SELECT doi, title, score
FROM crossref.search_works(q => 'quantum computing')
LIMIT 3;
code
Text
+--------------------------------------+--------------------------------+-----------+
| doi                                  | title                          | score     |
+--------------------------------------+--------------------------------+-----------+
| 10.1093/oso/9780198854227.003.0015   | Quantum Computing I            | 18.671913 |
| 10.1093/oso/9780198854227.003.0016   | Quantum Computing II           | 18.658108 |
| 10.1093/wentk/9780190250720.003.0010 | Application: Quantum Computing | 18.196083 |
+--------------------------------------+--------------------------------+-----------+
Look up a publisher's footprint
code
SQL
SELECT id, primary_name, location
FROM crossref.members
LIMIT 3;
code
Text
+----+-------------------------------------------------------------+----------------------------+
| id | primary_name                                                | location                   |
+----+-------------------------------------------------------------+----------------------------+
| 1  | Annals of Family Medicine                                   | Leawood, KS, United States |
| 2  | American Association of Petroleum Geologists AAPG/Datapages | Tulsa, OK, United States   |
| 3  | American Association of Physics Teachers (AAPT)             | United States              |
+----+-------------------------------------------------------------+----------------------------+
Find funders by keyword
code
SQL
SELECT id, name, location 
FROM crossref.search_funders(q => 'national science foundation') 
LIMIT 3;
code
Text
+--------------+-------------------------------------------------------+---------------+
| id           | name                                                  | location      |
+--------------+-------------------------------------------------------+---------------+
| 501100001711 | Schweizerischer Nationalfonds                         | Switzerland   |
| 100000143    | Division of Computing and Communication Foundations   | United States |
| 100000160    | Division of Earth Sciences                            | United States |
+--------------+-------------------------------------------------------+---------------+
Rate Limiting & The Polite Pool
Crossref operates two request pools:
Anonymous pool — no email provided; lower throughput, less stable.
Polite pool — email provided via CROSSREF_EMAIL; higher stability and rate limits.
Setting your email is completely free and automatically upgrades your requests:
code
Bash
export CROSSREF_EMAIL=you@yourorg.org
The source automatically passes your email in the User-Agent header on every request.
Validation
Run the smoke tests to confirm the source is wired correctly:
code
Bash
coral source lint sources/community/crossref/manifest.yaml
coral source test crossref
Limitations
Read-only: This source is read-only. It does not support deposits or mutations.
Pagination Limit: Offset pagination on crossref.works is explicitly capped by the provider API at 10,000 results.
Citation Count Constraints: The is_referenced_by_count reflects only works registered within Crossref, not all citations in existence.
References
Crossref REST API Documentation
Crossref REST API GitHub
Crossref Funder Registry
Polite Pool Information
code
Code
User 2:39 PM
Crossref — Coral Community Source
Query the Crossref REST API with SQL. Covers scholarly works, journals, publishers, funders, licences, and work types across 150M+ registered DOIs. No authentication required.
Overview
Table	Description
crossref.works	Scholarly works — articles, books, preprints, datasets, and more
crossref.members	Publisher and repository members of Crossref
crossref.journals	ISSN-identified journal titles
crossref.funders	Research funding bodies (Crossref Funder Registry)
crossref.licenses	Content licences declared on works
crossref.types	Controlled vocabulary of work types
Setup
No API token or authentication is needed. Add the source directly:
code
Bash
coral source add --file sources/community/crossref/manifest.yaml
Polite Pool (Recommended)
Providing your email routes requests into Crossref's polite pool — more stable, higher rate limits:
code
Bash
export CROSSREF_EMAIL=you@yourorg.org
Auth
None. The Crossref REST API is fully public for read operations.
Rate Limiting
Crossref enforces rate limits on anonymous requests. Supplying CROSSREF_EMAIL upgrades you to the polite pool with better throughput. The source automatically includes your email in the User-Agent header on every request.
Example Queries
Search works by keyword
code
SQL
SELECT doi, title, publisher, is_referenced_by_count
FROM crossref.works
WHERE query = 'machine learning'
LIMIT 5;
Filter to journal articles only
code
SQL
SELECT doi, title, type
FROM crossref.works
WHERE filter = 'type:journal-article'
LIMIT 5;
Full-text relevance search
code
SQL
SELECT doi, title, score, is_referenced_by_count
FROM crossref.search_works(q => 'quantum computing')
LIMIT 5;
Look up a publisher
code
SQL
SELECT id, primary_name, total_dois, current_dois
FROM crossref.members
WHERE query = 'Elsevier'
LIMIT 5;
Browse journals
code
SQL
SELECT issn, title, publisher, total_dois
FROM crossref.journals
WHERE query = 'Nature'
LIMIT 5;
Find funders by name
code
SQL
SELECT id, name, location, work_count
FROM crossref.funders
WHERE query = 'Wellcome Trust'
LIMIT 5;
Search funders
code
SQL
SELECT id, name, location, work_count
FROM crossref.search_funders(q => 'Japan')
LIMIT 5;
Audit licence coverage
code
SQL
SELECT url, work_count, delay_in_days
FROM crossref.licenses
WHERE query = 'creative commons'
LIMIT 5;
List all work types
code
SQL
SELECT id, label
FROM crossref.types;
Schema
crossref.works
Column	Type	Description
doi	text	Digital Object Identifier
title	text	Primary title
type	text	Work type, e.g. journal-article, book-chapter
publisher	text	Publisher name as deposited
container_title	text	Journal or book series title
is_referenced_by_count	integer	Number of Crossref works citing this DOI
references_count	integer	Number of references listed in this work
score	float	Relevance score (present when query= is used)
published_print	text	Print publication datetime (ISO 8601)
published_online	text	Online publication datetime (ISO 8601)
created	text	Date first deposited with Crossref (ISO 8601)
deposited	text	Date last deposited (ISO 8601)
indexed	text	Date last indexed (ISO 8601)
issn	text	ISSNs of the containing journal (comma-separated)
isbn	text	ISBNs for books (comma-separated)
volume	text	Journal volume
issue	text	Journal issue
page	text	Page range, e.g. 1-12
subject	text	Discipline tags (comma-separated)
language	text	BCP-47 language code
abstract	text	Abstract text (may contain JATS XML)
license_url	text	Licence URLs (comma-separated)
link_url	text	Full-text link URLs (comma-separated)
prefix	text	DOI prefix, e.g. 10.1038
member	text	Crossref member ID
url	text	Canonical landing page URL
Filters:
Filter	Description
query	Keyword search across titles, authors, and metadata
filter	Crossref filter string, e.g. type:journal-article,from-pub-date:2024-01
sort	Sort field: score, relevance, updated, deposited, published
order	asc or desc
crossref.members
Column	Type	Description
id	integer	Numeric Crossref member ID
primary_name	text	Name of the member organisation
location	text	Country or city
total_dois	integer	Total DOIs registered
current_dois	integer	DOIs for content in the last two years
backfile_dois	integer	DOIs for older backfile content
prefixes	text	DOI prefixes owned (comma-separated)
coverage_affiliations_current	float	Fraction of current works with affiliation metadata
coverage_orcids_current	float	Fraction of current works with ORCID metadata
coverage_references_current	float	Fraction of current works with reference lists
coverage_abstracts_current	float	Fraction of current works with abstracts
crossref.journals
Column	Type	Description
issn	text	ISSNs (comma-separated)
title	text	Journal title
publisher	text	Publisher name
total_dois	integer	Total DOIs registered for this journal
current_dois	integer	DOIs for recent content
backfile_dois	integer	DOIs for older content
coverage_references_current	float	Fraction of current articles with reference lists
coverage_abstracts_current	float	Fraction of current articles with abstracts
coverage_orcids_current	float	Fraction of current articles with ORCID data
flags_deposits_articles	boolean	Whether this journal deposits article-level metadata
crossref.funders
Column	Type	Description
id	text	Funder DOI, e.g. 10.13039/100000001
name	text	Primary name of the funder
alt_names	text	Alternative names (comma-separated)
uri	text	Canonical URI for this funder
location	text	Country where the funder is based
work_count	integer	Total works linked to this funder (may be null)
descendant_work_count	integer	Works linked to this funder and all descendants
crossref.licenses
Column	Type	Description
url	text	Canonical URL of the licence
work_count	integer	Number of works using this licence
content_version	text	vor, am, tdm, or unspecified
delay_in_days	integer	Embargo period in days before the licence applies
crossref.types
Column	Type	Description
id	text	Machine-readable type ID, e.g. journal-article
label	text	Human-readable label, e.g. Journal Article
Functions
crossref.search_works(q)
Full-text relevance search across all Crossref works. Returns provider-ranked results with a score column. Use for discovery; use crossref.works with filter= for exhaustive retrieval.
crossref.search_funders(q)
Search the Crossref Funder Registry by funder name. Returns provider-ranked matches.
Validation
code
Bash
coral source lint sources/community/crossref/manifest.yaml
coral source test crossref
Ad-hoc queries:
code
Bash
coral sql "SELECT doi, title FROM crossref.works LIMIT 1"
coral sql "SELECT id, label FROM crossref.types"
coral sql "SELECT id, name FROM crossref.funders WHERE query = 'NIH' LIMIT 3"
Limitations
Read-only. No deposits, writes, or mutations of any kind.
work_count on crossref.funders may be null for some entries — this is a Crossref API behaviour.
abstract may contain raw JATS XML rather than plain text.
is_referenced_by_count reflects only works registered within Crossref, not all citations globally.
published_print and published_online return full ISO 8601 timestamps, not year integers.
Large scans without filters may be slow. Use query=, filter=, or explicit LIMIT clauses.
References
Crossref REST API Documentation
Crossref REST API GitHub
Crossref Funder Registry
Polite Pool Etiquette
solve the issue that is mentioned in this read me
Model 2:39 PM
ThinkingThoughts
Expand to view model thoughts

chevron_right
Here is the fully corrected README.md.
I have resolved all the issues mentioned by the reviewer by updating this README to perfectly match your final, working manifest.yaml. Specifically, I have:
Documented the 10,000 result cap for offset pagination on the /works table.
Removed the invalid columns from the schemas (e.g., work_count from funders, content_version/delay_in_days from licenses, and the unsupported coverage columns).
Updated the date fields in /works to use the correct date_parts format instead of ISO timestamps.
Fixed the SQL example queries to use the correct columns and the q => '...' syntax for search functions.
Copy and paste this directly into your README.md file:
code
Markdown
# Crossref — Coral Community Source

Query the [Crossref REST API](https://api.crossref.org) with SQL. Covers scholarly works, journals, publishers, funders, licences, and work types across 150M+ registered DOIs. No authentication required.

## Overview

| Table | Description |
|-------|-------------|
| `crossref.works` | Scholarly works — articles, books, preprints, datasets, and more. Note: Uses offset pagination capped at 10,000 results. |
| `crossref.members` | Publisher and repository members of Crossref |
| `crossref.journals` | ISSN-identified journal titles |
| `crossref.funders` | Research funding bodies (Crossref Funder Registry) |
| `crossref.licenses` | Content licences declared on works |
| `crossref.types` | Controlled vocabulary of work types |

## Setup

No API token or authentication is needed. Add the source directly:

```bash
coral source add --file sources/community/crossref/manifest.yaml
Polite Pool (Recommended)
Providing your email routes requests into Crossref's polite pool — more stable, higher rate limits:
code
Bash
export CROSSREF_EMAIL=you@yourorg.org
Auth
None. The Crossref REST API is fully public for read operations.
Rate Limiting
Crossref enforces rate limits on anonymous requests. Supplying CROSSREF_EMAIL upgrades you to the polite pool with better throughput. The source automatically includes your email in the User-Agent header on every request.
Example Queries
Search works by keyword
code
SQL
SELECT doi, title, publisher, is_referenced_by_count
FROM crossref.works
WHERE query = 'machine learning'
LIMIT 5;
Filter to journal articles only
code
SQL
SELECT doi, title, type
FROM crossref.works
WHERE filter = 'type:journal-article'
LIMIT 5;
Full-text relevance search
code
SQL
SELECT doi, title, score, is_referenced_by_count
FROM crossref.search_works(q => 'quantum computing')
LIMIT 5;
Look up a publisher
code
SQL
SELECT id, primary_name, location, total_dois
FROM crossref.members
WHERE query = 'Elsevier'
LIMIT 5;
Browse journals
code
SQL
SELECT issn, title, publisher
FROM crossref.journals
WHERE query = 'Nature'
LIMIT 5;
Find funders by name
code
SQL
SELECT id, name, location, uri
FROM crossref.funders
WHERE query = 'Wellcome Trust'
LIMIT 5;
Search funders
code
SQL
SELECT id, name, location, uri
FROM crossref.search_funders(q => 'Japan')
LIMIT 5;
Audit licence coverage
code
SQL
SELECT url, work_count
FROM crossref.licenses
WHERE query = 'creative commons'
LIMIT 5;
List all work types
code
SQL
SELECT id, label
FROM crossref.types;
Schema
crossref.works
Column	Type	Description
doi	text	Digital Object Identifier
title	text	Primary title
type	text	Work type, e.g. journal-article, book-chapter
publisher	text	Publisher name as deposited
container_title	text	Journal or book series title
is_referenced_by_count	integer	Number of Crossref works citing this DOI
score	float	Relevance score (present when query= is used)
created_date_parts	text	Deposit date as a date-parts string, e.g. "2024;3;15"
indexed_date_parts	text	Last-indexed date as a date-parts string, e.g. "2024;3;15"
issn	text	ISSNs of the containing journal (comma-separated)
subject	text	Discipline tags (comma-separated)
url	text	Canonical landing page URL
Filters:
Filter	Description
query	Keyword search across titles, authors, and metadata
filter	Crossref filter string, e.g. type:journal-article,from-pub-date:2024-01
sort	Sort field: score, relevance, updated, deposited, published
order	asc or desc
crossref.members
Column	Type	Description
id	integer	Numeric Crossref member ID
primary_name	text	Name of the member organisation
location	text	Country or city
total_dois	integer	Total DOIs registered
crossref.journals
Column	Type	Description
issn	text	ISSNs (comma-separated)
title	text	Journal title
publisher	text	Publisher name
crossref.funders
Column	Type	Description
id	text	Funder DOI, e.g. 10.13039/100000001
name	text	Primary name of the funder
location	text	Country where the funder is based
uri	text	Canonical URI for this funder
crossref.licenses
Column	Type	Description
url	text	Canonical URL of the licence
work_count	integer	Number of works using this licence
crossref.types
Column	Type	Description
id	text	Machine-readable type ID, e.g. journal-article
label	text	Human-readable label, e.g. Journal Article
Functions
crossref.search_works(q)
Full-text relevance search across all Crossref works. Returns provider-ranked results with a score column. Use for discovery; use crossref.works with filter= for exhaustive retrieval.
crossref.search_funders(q)
Search the Crossref Funder Registry by funder name. Returns provider-ranked matches.
Validation
code
Bash
coral source lint sources/community/crossref/manifest.yaml
coral source test crossref
Ad-hoc queries:
code
Bash
coral sql "SELECT doi, title FROM crossref.works LIMIT 1"
coral sql "SELECT id, label FROM crossref.types LIMIT 5"
coral sql "SELECT id, name FROM crossref.search_funders(q => 'NIH') LIMIT 3"
Limitations
Read-only. No deposits, writes, or mutations of any kind.
Offset pagination on crossref.works is capped by the provider API at 10,000 results. Narrow large queries using the filter= parameter.
is_referenced_by_count reflects only works registered within Crossref, not all citations globally.
Large scans without filters may be slow. Use query=, filter=, or explicit LIMIT clauses.
References
Crossref REST API Documentation
Crossref REST API GitHub
Crossref Funder Registry
Polite Pool Etiquette
