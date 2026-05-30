# LinkedIn (data export)

Exposes a **LinkedIn account data export** — the CSV archive any member can
download of their own data — as queryable SQL tables. No API key, no scraping:
it reads the CSV files LinkedIn gives you directly.

## Setup

### 1. Export your LinkedIn data
1. Go to **LinkedIn → Settings → Data Privacy → Get a copy of your data**.
2. Select at least: **Profile, Skills, Positions** (and **Connections** if you
   want the network table).
3. Request the archive. LinkedIn prepares it and **emails you a download link
   when it is ready** (see LinkedIn's help page on downloading your data:
   https://www.linkedin.com/help/linkedin/answer/a1339364).
4. Extract the ZIP into a directory, e.g. `./linkedin_export/`.

### 2. Register the source
```bash
coral source add --file ./linkedin/manifest.yaml
```

Point the source at your export directory with the `LINKEDIN_EXPORT_PATH` input
(defaults to `./linkedin_export`).

### 3. Verify
```bash
coral source test linkedin
coral sql "SELECT * FROM linkedin.skills LIMIT 5"
coral sql "SELECT company_name, title FROM linkedin.positions ORDER BY started_on DESC LIMIT 3"
```

## Tables

| Table | File | Description |
|---|---|---|
| `linkedin.profile` | `Profile.csv` | Name, headline, summary, industry, location (13 export columns) |
| `linkedin.skills` | `Skills.csv` | Skill name (current exports are name-only) |
| `linkedin.positions` | `Positions.csv` | Work history: company, title, description, dates |
| `linkedin.connections` | `Connections.csv` | Network: name, profile URL, email, company, position, date — **requires preprocessing, see below** |

## Connections preprocessing

LinkedIn ships `Connections.csv` with a short **notes preamble** (a few lines
explaining the file) *before* the real header row. Coral's CSV backend reads
from the first line and cannot skip a preamble, so strip everything before the
header once, after extracting:

```bash
# Delete all lines before the "First Name,..." header so it becomes line 1
sed -i '/^First Name,/,$!d' Connections.csv
```

The other three tables (`profile`, `skills`, `positions`) need no preprocessing.

## Example queries

```sql
-- Your skills
SELECT name FROM linkedin.skills ORDER BY name;
```

```sql
-- Cross-source: skills required by rejected job applications
-- that are absent from your LinkedIn profile (the missing row is the signal)
SELECT required.skill, COUNT(*) AS times_required
FROM (
  SELECT UNNEST(required_skills) AS skill
  FROM sheets.applications
  WHERE status = 'rejected'
) required
LEFT JOIN linkedin.skills l ON l.name = required.skill
WHERE l.name IS NULL
GROUP BY required.skill
ORDER BY times_required DESC;
```

## Validated against exported archive

Validated end-to-end against a real LinkedIn account data export (Coral 0.4.1,
file backend) containing a real `Profile.csv` (1 row), `Skills.csv` (54 rows),
and `Positions.csv` (5 rows). Output below is verbatim Coral output with
personal values sanitized.

```text
$ export LINKEDIN_EXPORT_PATH=./linkedin_export
$ coral source add --file ./linkedin/manifest.yaml
Added source linkedin (secrets: none)

$ coral source test linkedin

  ✓ linkedin connected successfully
  Secrets: none

    linkedin (4 tables)
    ├─ connections
    ├─ positions
    ├─ profile
    └─ skills
    Query tests
    3 declared · 3 passed · 0 failed

    ✓ SELECT * FROM linkedin.skills LIMIT 1
      1 row
    ✓ SELECT * FROM linkedin.profile LIMIT 1
      1 row
    ✓ SELECT * FROM linkedin.positions LIMIT 1
      1 row

$ coral sql "SELECT * FROM linkedin.skills LIMIT 5"
+--------------------------+
| name                     |
+--------------------------+
| Full-Stack Development   |
| Back-End Web Development |
| Back-end Operations      |
| Automation               |
| Python                   |
+--------------------------+

$ coral sql "SELECT first_name, headline, industry, geo_location FROM linkedin.profile LIMIT 1"
+------------+----------------------------+---------------------+---------------+
| first_name | headline                   | industry            | geo_location  |
+------------+----------------------------+---------------------+---------------+
| Jordan     | Software Developer at Acme | Software Development | Bengaluru, IN |
+------------+----------------------------+---------------------+---------------+

$ coral sql "SELECT company_name, title, started_on FROM linkedin.positions LIMIT 3"
+---------------+-------------------+------------+
| company_name  | title             | started_on |
+---------------+-------------------+------------+
| Acme Corp     | Software Engineer | Apr 2026   |
| Globex        | System Engineer   | Oct 2024   |
| State College | Student           | Jun 2020   |
+---------------+-------------------+------------+
```

(The `connections` table is listed as a declared table but is not exercised by
the test queries above, because this export contained no `Connections.csv`; see
the preprocessing note above for how that file is handled when present.)

Live validation note: Coral 0.4.1 has an intermittent tokio panic that prints
to stderr *after* returning correct results; it does not affect query output.

## Notes
- Uses the official LinkedIn data export — legitimate, member-initiated data access.
- To refresh, request a new export from LinkedIn and re-extract over the same directory.
- Column names follow the headers in a standard English-locale export. Declared
  columns are mapped positionally; older or localized exports may differ, so
  adjust `columns` in the manifest to match your CSV if needed.
