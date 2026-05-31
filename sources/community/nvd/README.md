# NIST National Vulnerability Database (nvd)

**Version:** 0.1.0
**Backend:** HTTP
**Tables:** 5
**Base URL:** `https://services.nvd.nist.gov`

Query CVE records, CVSS severity scores, and advisory references from the [NIST National Vulnerability Database](https://nvd.nist.gov/) — the U.S. government repository of standards-based vulnerability management data. No authentication required.

API docs: [CVE API 2.0](https://nvd.nist.gov/developers/vulnerabilities) · [Rate limits and best practices](https://nvd.nist.gov/developers/start-here)

> **CVSS v4.0 out of scope:** This source covers CVSS v2, v3.0, and v3.1 metrics only. CVSS v4.0 metrics are not yet included.

```bash
coral source add --file sources/community/nvd/manifest.yaml
```

## Rate Limits

All queries run unauthenticated at 5 requests per 30 seconds. This source is unauthenticated only — no API key is wired. Auth support can be contributed if higher throughput is needed.

## Tables

| Table                  | Description                                                              |
| ---------------------- | ------------------------------------------------------------------------ |
| `vulnerabilities`      | Core CVE records — id, status, published date, description               |
| `cvss_v3_1_metrics`    | CVSS v3.1 metric array as JSON (most CVEs published after 2019)          |
| `cvss_v3_0_metrics`    | CVSS v3.0 metric array as JSON (CVEs scored before v3.1 was adopted)     |
| `cvss_v2_metrics`      | CVSS v2 metric array as JSON for older CVEs                              |
| `references`           | Advisory and patch links as a JSON array per CVE                         |

---

### `vulnerabilities`

Core CVE records. Each row is one CVE.

| Column              | Type   | Description                                      |
| ------------------- | ------ | ------------------------------------------------ |
| `cve_id`            | `Utf8` | CVE identifier (e.g. `CVE-2021-44228`)           |
| `source_identifier` | `Utf8` | Source organization that assigned the CVE        |
| `published`         | `Timestamp` | Publication date and time (ISO 8601)             |
| `last_modified`     | `Timestamp` | Last modified date and time (ISO 8601)           |
| `vuln_status`       | `Utf8` | Analysis status (Analyzed, Awaiting Analysis...) |
| `description`       | `Utf8` | English-language vulnerability description       |

### `cvss_v3_1_metrics`

CVSS v3.1 metric array per CVE. The `metrics` column is the full `cvssMetricV31` array from NVD as JSON. Each array element contains `source`, `type`, `cvssData` (baseScore, baseSeverity, vectorString, and sub-metrics), `exploitabilityScore`, and `impactScore`. Filter to `type = 'Primary'` and `source = 'nvd@nist.gov'` for the authoritative NVD score. Column is **null** when NVD has not assigned v3.1 metrics.

| Column    | Type   | Description                                                  |
| --------- | ------ | ------------------------------------------------------------ |
| `cve_id`  | `Utf8` | CVE identifier                                               |
| `metrics` | `Json` | Full cvssMetricV31 array; null when no v3.1 metrics assigned |

### `cvss_v3_0_metrics`

CVSS v3.0 metric array per CVE. Same structure as `cvss_v3_1_metrics`. Use this for CVEs that were scored before CVSS v3.1 was adopted (typically CVEs published before 2019).

| Column    | Type   | Description                                                  |
| --------- | ------ | ------------------------------------------------------------ |
| `cve_id`  | `Utf8` | CVE identifier                                               |
| `metrics` | `Json` | Full cvssMetricV30 array; null when no v3.0 metrics assigned |

### `cvss_v2_metrics`

CVSS v2 metric array per CVE. Each array element contains `source`, `type`, `cvssData` (baseScore, vectorString), `baseSeverity`, `exploitabilityScore`, and `impactScore`.

| Column    | Type   | Description                                                 |
| --------- | ------ | ----------------------------------------------------------- |
| `cve_id`  | `Utf8` | CVE identifier                                              |
| `metrics` | `Json` | Full cvssMetricV2 array; null when no v2 metrics assigned   |

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

Critical CVEs pushed server-side via the `cvss_v3_severity` filter:

```sql
SELECT cve_id, description
FROM nvd.vulnerabilities
WHERE cvss_v3_severity = 'CRITICAL'
LIMIT 10;
```

Inspect the raw CVSS v3.1 metric array for a specific CVE:

```sql
SELECT cve_id, metrics
FROM nvd.cvss_v3_1_metrics
WHERE cve_id = 'CVE-2021-44228'
LIMIT 1;
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

CVE with all metric arrays:

```sql
SELECT v.cve_id, v.description,
       m31.metrics AS cvss_v3_1,
       m30.metrics AS cvss_v3_0,
       m2.metrics  AS cvss_v2
FROM nvd.vulnerabilities v
LEFT JOIN nvd.cvss_v3_1_metrics m31 ON m31.cve_id = v.cve_id
LEFT JOIN nvd.cvss_v3_0_metrics m30 ON m30.cve_id = v.cve_id
LEFT JOIN nvd.cvss_v2_metrics   m2  ON m2.cve_id  = v.cve_id
WHERE v.cve_id = 'CVE-2021-44228';
```

## Notes

- The NVD contains 350,000+ CVE records. Always scope queries with `cve_id`, a `pub_start_date`/`pub_end_date` pair, or `cvss_v3_severity`. Unscoped queries page through the full corpus.
- Date filters (`pub_start_date`/`pub_end_date` and `last_mod_start_date`/`last_mod_end_date`) must be supplied in pairs. NVD caps any date window at 120 consecutive days.
- Date values use ISO 8601 format: `2024-01-01T00:00:00.000`.
- `keyword_search` uses NVD's provider keyword search with implicit expansion — results may include suffix and plural variants, so matches can be broader than an exact word.
- CVSS metric columns are nullable — a row is returned for every fetched CVE, but the `metrics` column is null when NVD has not assigned that metric version.
- Most CVEs published after 2019 use CVSS v3.1 (`cvss_v3_1_metrics`). Older CVEs may only have CVSS v3.0 (`cvss_v3_0_metrics`) or v2 (`cvss_v2_metrics`) scores.
- The `metrics` column is a JSON array. Each element has `source`, `type`, `cvssData`, `exploitabilityScore`, and `impactScore` fields. Do not rely on array position — NVD may return multiple entries per CVE. Select the authoritative score by checking `source = 'nvd@nist.gov'` and `type = 'Primary'` on each element.
- The `references` column is a JSON array. Use `json_get(references, 0)` to get the first reference object, then `json_get_str(json_get(references, 0), 'url')` to extract the URL.
- No authentication is required. The NVD API key is not wired into this source.
- **CVSS v4.0 is out of scope for this version.** Only CVSS v2, v3.0, and v3.1 metrics are covered.
