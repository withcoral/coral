# LinkedIn (data export)

Exposes a LinkedIn data export (the GDPR archive LinkedIn lets any member
download) as queryable SQL tables. No API key, no scraping — it reads the CSV
files LinkedIn gives you directly.

## Setup

### 1. Export your LinkedIn data
1. Go to **LinkedIn → Settings → Data Privacy → Get a copy of your data**.
2. Select at least: **Connections, Skills, Profile, Positions**.
3. Request the archive (LinkedIn emails it within ~24 hours).
4. Extract the ZIP into `~/linkedin_export/` (or edit `location` in `manifest.yaml`
   to point at wherever you extracted it).

### 2. Register the source
```bash
coral source add --file ./sources/community/linkedin/manifest.yaml
```

### 3. Verify
```bash
coral sql "SELECT * FROM linkedin.skills LIMIT 5"
coral sql "SELECT * FROM linkedin.positions ORDER BY started_on DESC LIMIT 3"
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
-- Total years of experience inferred from positions
SELECT title, company_name, started_on, finished_on
FROM linkedin.positions
ORDER BY started_on DESC;
```

## Notes
- Uses the official LinkedIn data export — legitimate, member-initiated data access.
- LinkedIn permits one export roughly every 24 hours; re-download to refresh.
- Column names follow the headers in a standard English-locale LinkedIn export.
  Older or localized exports may differ slightly; adjust the `columns` in
  `manifest.yaml` to match your CSV headers.
