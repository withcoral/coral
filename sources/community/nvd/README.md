# NIST National Vulnerability Database (nvd)

**Version:** 0.1.0
**Backend:** HTTP
**Tables:** 4
**Base URL:** `https://services.nvd.nist.gov`

Query CVE records, CVSS severity scores, and advisory references from the [NIST National Vulnerability Database](https://nvd.nist.gov/) — the U.S. government repository of standards-based vulnerability management data. No authentication required.

```bash
coral source add --file sources/community/nvd/manifest.yaml
```

## Rate Limits

Without an API key: 5 requests per 30 seconds.
With an API key: 50 requests per 30 seconds.

Request a free API key at https://nvd.nist.gov/developers/request-an-api-key.

## Tables

| Table             | Description                                                                 |
| ----------------- | --------------------------------------------------------------------------- |
| `vulnerabilities` | Core CVE records with identifiers, publication dates, status, descriptions |
| `cvss_v3`         | CVSS v3.x severity scores and metrics                                      |
| `cvss_v2`         | CVSS v2 severity scores (fallback for older CVEs)                          |
| `references`      | Advisory and patch reference links                                         |

---

### `vulnerabilities`

Core CVE records from the NVD. Each row is one CVE with its identifier, publication dates, status, and English description.

| Column              | Type   | Description                                      |
| ------------------- | ------ | ------------------------------------------------ |
| `cve_id`            | `Utf8` | CVE identifier (e.g. `CVE-2021-44228`)           |
| `source_identifier` | `Utf8` | Source organization that assigned the CVE        |
| `published`         | `Utf8` | Date and time the CVE was published (ISO 8601)   |
| `last_modified`     | `Utf8` | Date and time last modified (ISO 8601)           |
| `vuln_status`       | `Utf8` | Analysis status (Analyzed, Awaiting Analysis...) |
| `description`       | `Utf8` | English-language vulnerability description       |

## Quick Start

```bash
# Lookup a specific CVE
coral sql "SELECT cve_id, description, vuln_status FROM nvd.vulnerabilities WHERE cve_id = 'CVE-2021-44228' LIMIT 1"
```

### `cvss_v3`

CVSS v3.x severity scores for CVE records. Each row contains the primary NVD scoring for one CVE.

| Column                   | Type      | Description                                  |
| ------------------------ | --------- | -------------------------------------------- |
| `cve_id`                 | `Utf8`    | CVE identifier                               |
| `base_score`             | `Float64` | CVSS v3 base score (0.0 – 10.0)              |
| `base_severity`          | `Utf8`    | Severity label (LOW, MEDIUM, HIGH, CRITICAL) |
| `exploitability_score`   | `Float64` | Exploitability sub-score                     |
| `impact_score`           | `Float64` | Impact sub-score                             |
| `vector_string`          | `Utf8`    | Full CVSS v3 vector string                   |
| `attack_vector`          | `Utf8`    | Attack vector (NETWORK, LOCAL...)            |
| `attack_complexity`      | `Utf8`    | Attack complexity (LOW, HIGH)                |
| `privileges_required`    | `Utf8`    | Privileges required (NONE, LOW, HIGH)        |
| `user_interaction`       | `Utf8`    | User interaction (NONE, REQUIRED)            |
| `scope`                  | `Utf8`    | Scope (UNCHANGED, CHANGED)                   |
| `confidentiality_impact` | `Utf8`    | Confidentiality impact (NONE, LOW, HIGH)     |
| `integrity_impact`       | `Utf8`    | Integrity impact (NONE, LOW, HIGH)           |
| `availability_impact`    | `Utf8`    | Availability impact (NONE, LOW, HIGH)        |

### `cvss_v2`

CVSS v2 severity scores for CVE records. Provided as a fallback for older CVEs that pre-date CVSS v3.

| Column                 | Type      | Description                          |
| ---------------------- | --------- | ------------------------------------ |
| `cve_id`               | `Utf8`    | CVE identifier                       |
| `base_score`           | `Float64` | CVSS v2 base score (0.0 – 10.0)      |
| `severity`             | `Utf8`    | Severity label (LOW, MEDIUM, HIGH)   |
| `vector_string`        | `Utf8`    | Full CVSS v2 vector string           |
| `exploitability_score` | `Float64` | Exploitability sub-score             |
| `impact_score`         | `Float64` | Impact sub-score                     |

### `references`

Advisory and patch reference links for CVE records. Each row is one CVE with its full references array — not one row per URL. The `references` column is a JSON array where each element has `url`, `source`, and `tags` fields.

| Column       | Type   | Description                                     |
| ------------ | ------ | ----------------------------------------------- |
| `cve_id`     | `Utf8` | CVE identifier                                  |
| `references` | `Json` | Array of reference objects (url, source, tags)  |

Each element in the array is an object. Use `json_get(references, 0)` to extract the first reference object, or `json_get_str(json_get(references, 0), 'url')` to extract a specific field such as the URL.

---

## Example Queries

Critical CVEs with CVSS v3 scores:

```sql
SELECT v.cve_id, v.description, c.base_score, c.base_severity
FROM nvd.vulnerabilities v
JOIN nvd.cvss_v3 c ON c.cve_id = v.cve_id
WHERE c.base_severity = 'CRITICAL'
LIMIT 10;
```

Recent CVEs published in 2024:

```sql
SELECT cve_id, published, vuln_status
FROM nvd.vulnerabilities
WHERE published >= '2024-01-01T00:00:00.000'
ORDER BY published DESC
LIMIT 10;
```

CVE with all scoring details:

```sql
SELECT v.cve_id, v.description, v.vuln_status,
       c3.base_score AS cvss_v3_score, c3.base_severity,
       c2.base_score AS cvss_v2_score, c2.severity AS cvss_v2_severity
FROM nvd.vulnerabilities v
LEFT JOIN nvd.cvss_v3 c3 ON c3.cve_id = v.cve_id
LEFT JOIN nvd.cvss_v2 c2 ON c2.cve_id = v.cve_id
WHERE v.cve_id = 'CVE-2021-44228';
```

## Notes

- The NVD contains 350,000+ CVE records. Always use filters (`cve_id`, `pub_start_date`, `cvss_v3_severity`) or `LIMIT` for interactive queries.
- Date filters use ISO 8601 format: `2024-01-01T00:00:00.000`.
- The `references` column is a JSON array. Use `json_get(references, 0)` to get the first reference object, or `json_get_str(json_get(references, 0), 'url')` to extract the URL field.
- CVSS v3 scores are preferred for modern CVEs. Use CVSS v2 only for older records.
