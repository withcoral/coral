# NIST National Vulnerability Database (nvd)

**Version:** 0.1.0
**Backend:** HTTP
**Tables:** 6
**Base URL:** `https://services.nvd.nist.gov`

Query CVE records, CVSS severity scores, and advisory references from the [NIST National Vulnerability Database](https://nvd.nist.gov/) — the U.S. government repository of standards-based vulnerability management data. No authentication required.

```bash
coral source add --file sources/community/nvd/manifest.yaml
```

## Rate Limits

Without an API key: 5 requests per 30 seconds. With an API key: 50 requests per 30 seconds. The NVD API key is not currently wired into this source manifest. All queries run unauthenticated.

## Tables

| Table             | Description                                                              |
| ----------------- | ------------------------------------------------------------------------ |
| `vulnerabilities` | Core CVE records — id, status, published date, description               |
| `cvss_v3_1`       | CVSS v3.1 scores and metrics (most CVEs published after 2019)            |
| `cvss_v3_0`       | CVSS v3.0 scores and metrics (CVEs scored before v3.1 was adopted)       |
| `cvss_v2`         | CVSS v2 fallback scores for older CVEs                                   |
| `references`      | Advisory and patch links as a JSON array per CVE                         |

---

### `vulnerabilities`

Core CVE records. Each row is one CVE.

| Column              | Type   | Description                                      |
| ------------------- | ------ | ------------------------------------------------ |
| `cve_id`            | `Utf8` | CVE identifier (e.g. `CVE-2021-44228`)           |
| `source_identifier` | `Utf8` | Source organization that assigned the CVE        |
| `published`         | `Utf8` | Publication date and time (ISO 8601)             |
| `last_modified`     | `Utf8` | Last modified date and time (ISO 8601)           |
| `vuln_status`       | `Utf8` | Analysis status (Analyzed, Awaiting Analysis...) |
| `description`       | `Utf8` | English-language vulnerability description       |

### `cvss_v3_1`

CVSS v3.1 scores. Each row is one CVE; metric columns are **null** when NVD has not assigned v3.1 metrics. Use `WHERE base_score IS NOT NULL` to restrict to scored rows.

| Column                   | Type      | Description                                  |
| ------------------------ | --------- | -------------------------------------------- |
| `cve_id`                 | `Utf8`    | CVE identifier                               |
| `base_score`             | `Float64` | Base score (0.0 – 10.0), null if unscored    |
| `base_severity`          | `Utf8`    | Severity label (LOW, MEDIUM, HIGH, CRITICAL) |
| `exploitability_score`   | `Float64` | Exploitability sub-score                     |
| `impact_score`           | `Float64` | Impact sub-score                             |
| `vector_string`          | `Utf8`    | Full CVSS v3.1 vector string                 |
| `attack_vector`          | `Utf8`    | NETWORK, ADJACENT_NETWORK, LOCAL, PHYSICAL   |
| `attack_complexity`      | `Utf8`    | LOW or HIGH                                  |
| `privileges_required`    | `Utf8`    | NONE, LOW, or HIGH                           |
| `user_interaction`       | `Utf8`    | NONE or REQUIRED                             |
| `scope`                  | `Utf8`    | UNCHANGED or CHANGED                         |
| `confidentiality_impact` | `Utf8`    | NONE, LOW, or HIGH                           |
| `integrity_impact`       | `Utf8`    | NONE, LOW, or HIGH                           |
| `availability_impact`    | `Utf8`    | NONE, LOW, or HIGH                           |

### `cvss_v3_0`

CVSS v3.0 scores. Same columns as `cvss_v3_1`. Use this for CVEs that were scored before CVSS v3.1 was adopted.

### `cvss_v2`

CVSS v2 scores for older CVEs. Metric columns are null when not scored under v2.

| Column                 | Type      | Description                        |
| ---------------------- | --------- | ---------------------------------- |
| `cve_id`               | `Utf8`    | CVE identifier                     |
| `base_score`           | `Float64` | Base score (0.0 – 10.0)            |
| `severity`             | `Utf8`    | LOW, MEDIUM, or HIGH               |
| `vector_string`        | `Utf8`    | Full CVSS v2 vector string         |
| `exploitability_score` | `Float64` | Exploitability sub-score           |
| `impact_score`         | `Float64` | Impact sub-score                   |

### `references`

One row per CVE with a JSON array of all reference links.

| Column       | Type   | Description                                    |
| ------------ | ------ | ---------------------------------------------- |
| `cve_id`     | `Utf8` | CVE identifier                                 |
| `references` | `Json` | Array of objects with url, source, tags fields |

---

## Quick Start

```bash
coral sql "SELECT cve_id, description, vuln_status FROM nvd.vulnerabilities WHERE cve_id = 'CVE-2021-44228' LIMIT 1"
```

## Example Queries

Lookup a specific CVE:

```sql
SELECT cve_id, description, vuln_status
FROM nvd.vulnerabilities
WHERE cve_id = 'CVE-2021-44228'
LIMIT 1;
```

Critical CVEs with CVSS v3.1 scores:

```sql
SELECT v.cve_id, v.description, c.base_score, c.base_severity
FROM nvd.vulnerabilities v
JOIN nvd.cvss_v3_1 c ON c.cve_id = v.cve_id
WHERE c.base_severity = 'CRITICAL'
AND c.base_score IS NOT NULL
LIMIT 10;
```

CVEs published in a 30-day window (use pub_start_date + pub_end_date together, max 120 days):

```sql
SELECT cve_id, published, vuln_status
FROM nvd.vulnerabilities
WHERE pub_start_date = '2024-01-01T00:00:00.000'
AND pub_end_date = '2024-01-31T23:59:59.999'
ORDER BY published DESC
LIMIT 20;
```

CVEs modified in the last 7 days:

```sql
SELECT cve_id, last_modified, vuln_status
FROM nvd.vulnerabilities
WHERE last_mod_start_date = '2024-05-01T00:00:00.000'
AND last_mod_end_date = '2024-05-07T23:59:59.999'
LIMIT 20;
```

CVE with all scoring details:

```sql
SELECT v.cve_id, v.description,
       c3.base_score AS cvss_v3_score, c3.base_severity,
       c2.base_score AS cvss_v2_score
FROM nvd.vulnerabilities v
LEFT JOIN nvd.cvss_v3_1 c3 ON c3.cve_id = v.cve_id
LEFT JOIN nvd.cvss_v2 c2 ON c2.cve_id = v.cve_id
WHERE v.cve_id = 'CVE-2021-44228';
```

## Notes

- The NVD contains 350,000+ CVE records. Always scope queries with `cve_id`, a `pub_start_date`/`pub_end_date` pair, or `cvss_v3_severity`. Unscoped queries page through the full corpus.
- Date filters (`pub_start_date`/`pub_end_date` and `last_mod_start_date`/`last_mod_end_date`) must be supplied in pairs. NVD caps any date window at 120 consecutive days.
- Date values use ISO 8601 format: `2024-01-01T00:00:00.000`.
- CVSS metric columns are nullable — a row is returned for every fetched CVE, but score fields are null when NVD has not assigned that metric version. Use `WHERE base_score IS NOT NULL` to filter to scored rows.
- Most CVEs published after 2019 use CVSS v3.1 (`cvss_v3_1`). Older CVEs may only have CVSS v3.0 (`cvss_v3_0`) or v2 (`cvss_v2`) scores.
- The `references` column is a JSON array. Use `json_get(references, 0)` to get the first reference object, then `json_get_str(json_get(references, 0), 'url')` to extract the URL.
- No authentication is required. The NVD API key is not wired into this source.
