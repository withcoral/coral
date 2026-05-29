# LinkedIn (account data export)

Exposes a LinkedIn account data export as queryable SQL tables. No API key,
no scraping - it reads the CSV files LinkedIn gives you directly.

## Setup

### 1. Export your LinkedIn data
1. Go to **LinkedIn -> Settings -> Data Privacy -> Get a copy of your data**.
2. Select at least: **Connections, Skills, Profile, Positions**.
3. Request the archive.
4. Extract the ZIP into a directory, e.g. `./linkedin_export/`.

LinkedIn's help center says selected data categories can arrive within minutes,
while larger exports can arrive within 24 hours, and the download link remains
available for 72 hours:
https://www.linkedin.com/help/linkedin/answer/a1339364

### 2. Normalize Connections.csv if needed
Some LinkedIn exports include explanatory note/preamble lines before the
`Connections.csv` header row. This manifest reads CSV files with `has_header:
true`, so `Connections.csv` must start with the header row before you run
validation.

If your file starts with notes, keep a backup and remove everything before:

```text
First Name,Last Name,Email Address,Company,Position,Connected On
```

For example:

```bash
cp "$LINKEDIN_EXPORT_PATH/Connections.csv" "$LINKEDIN_EXPORT_PATH/Connections.raw.csv"
python - <<'PY'
from pathlib import Path
import os

path = Path(os.environ["LINKEDIN_EXPORT_PATH"]) / "Connections.csv"
rows = path.read_text(encoding="utf-8-sig").splitlines()
header = next(
    i for i, row in enumerate(rows)
    if row.startswith("First Name,Last Name,Email Address,Company,Position,Connected On")
)
path.write_text("\n".join(rows[header:]) + "\n", encoding="utf-8")
PY
```

### 3. Register the source
```bash
coral source add --file ./sources/community/linkedin/manifest.yaml
```

Point the source at your export directory with the `LINKEDIN_EXPORT_PATH` input
(defaults to `./linkedin_export`).

### 4. Verify
```bash
export LINKEDIN_EXPORT_PATH=/absolute/path/to/linkedin_export
coral source test linkedin
coral sql "SELECT * FROM linkedin.skills LIMIT 5"
coral sql "SELECT * FROM linkedin.positions ORDER BY started_on DESC LIMIT 3"
coral sql "SELECT first_name, last_name, company, position FROM linkedin.connections LIMIT 3"
```

## Validated against exported archive

Status: pending maintainer/local validation against a real sanitized LinkedIn
account data export. A sample fixture is not sufficient for the Coral community
source PR; run the commands below against a user-owned export and paste the
sanitized output before requesting re-review.

```bash
export LINKEDIN_EXPORT_PATH=/absolute/path/to/sanitized/linkedin_export
coral source add --file ./sources/community/linkedin/manifest.yaml
coral source test linkedin
coral sql "SELECT name, endorsements FROM linkedin.skills ORDER BY endorsements DESC LIMIT 3"
coral sql "SELECT company_name, title, started_on FROM linkedin.positions ORDER BY started_on DESC LIMIT 3"
coral sql "SELECT first_name, last_name, company, position FROM linkedin.connections LIMIT 3"
```

Paste sanitized output here:

```text
$ coral source test linkedin
<paste output>

$ coral sql "SELECT name, endorsements FROM linkedin.skills ORDER BY endorsements DESC LIMIT 3"
<paste output with personal data redacted>

$ coral sql "SELECT company_name, title, started_on FROM linkedin.positions ORDER BY started_on DESC LIMIT 3"
<paste output with personal data redacted>

$ coral sql "SELECT first_name, last_name, company, position FROM linkedin.connections LIMIT 3"
<paste output with personal data redacted>
```

## Tables

| Table | File | Description |
|---|---|---|
| `linkedin.profile` | `Profile.csv` | First/last name, headline, summary, location, industry |
| `linkedin.skills` | `Skills.csv` | Skill name and endorsement count |
| `linkedin.positions` | `Positions.csv` | Work history: company, title, description, dates |
| `linkedin.connections` | `Connections.csv` | Network: name, email, company, position, connected date |

## Example queries

```sql
-- Most endorsed skills
SELECT name, endorsements
FROM linkedin.skills
ORDER BY endorsements DESC
LIMIT 10;
```

```sql
-- Cross-source: skills required by rejected job applications
-- that are absent from your LinkedIn profile (the missing row is the signal)
SELECT required.skill, COUNT(*) AS times_required
FROM (
  SELECT UNNEST(required_skills) AS skill
  FROM notion.applications
  WHERE status = 'rejected'
) required
LEFT JOIN linkedin.skills l ON l.name = required.skill
WHERE l.name IS NULL
GROUP BY required.skill
ORDER BY times_required DESC;
```

## Notes
- Uses the official LinkedIn account data export - legitimate, member-initiated data access.
- Re-download the account data export to refresh local CSVs.
- Column names follow the headers in a standard English-locale LinkedIn export.
  Older or localized exports may differ slightly; adjust `columns` to match your CSV headers.
