# CISA Known Exploited Vulnerabilities (cisa_kev)

**Version:** 0.1.0
**Backend:** HTTP
**Tables:** 2
**Base URL:** `https://www.cisa.gov`

Query vulnerabilities and feed metadata from the [CISA Known Exploited Vulnerabilities catalog](https://www.cisa.gov/known-exploited-vulnerabilities-catalog) — a public feed of vulnerabilities confirmed as actively exploited in the wild. No authentication required.

```bash
coral source add --file sources/community/cisa_kev/manifest.yaml
```

## Rate Limits

This is a static JSON file served by CISA. There is no documented rate limit, but the feed is refreshed periodically (typically daily). Avoid polling it at high frequency.

## Tables

| Table             | Description                                                                      |
| ----------------- | -------------------------------------------------------------------------------- |
| `catalog`         | Single-row feed metadata for the KEV catalog, including version and release date |
| `vulnerabilities` | All entries in the KEV catalog — one row per CVE                                 |

---

### `catalog`

Single-row feed metadata for the CISA KEV catalog. Use this table to check feed freshness before joining with vulnerability rows.

| Column           | Type    | Description                              |
| ---------------- | ------- | ---------------------------------------- |
| `catalog_version` | `Utf8`  | CISA catalog version string              |
| `date_released`   | `Utf8`  | Date the catalog feed was released       |
| `count`          | `Int64` | Number of vulnerability rows in the feed |

## Quick Start

```bash
# Confirm connectivity and freshness metadata
coral sql "SELECT catalog_version, date_released, count FROM cisa_kev.catalog LIMIT 1"
```

### `vulnerabilities`

All entries in the CISA KEV catalog. Each row is one CVE that CISA has confirmed as actively exploited.

| Column                          | Type   | Description                                        |
| ------------------------------- | ------ | -------------------------------------------------- |
| `cve_id`                        | `Utf8` | CVE identifier (e.g. `CVE-2021-44228`)             |
| `vendor_project`                | `Utf8` | Vendor or project name                             |
| `product`                       | `Utf8` | Affected product name                              |
| `vulnerability_name`            | `Utf8` | Short human-readable vulnerability name            |
| `date_added`                    | `Utf8` | Date added to the KEV catalog (`YYYY-MM-DD`)       |
| `short_description`             | `Utf8` | Brief description of the vulnerability             |
| `required_action`               | `Utf8` | Remediation action required by CISA                |
| `due_date`                      | `Utf8` | Federal agency remediation due date (`YYYY-MM-DD`) |
| `known_ransomware_campaign_use` | `Utf8` | `Known` or `Unknown` ransomware campaign use       |
| `notes`                         | `Utf8` | Additional references and notes                    |
| `cwes`                          | `Json` | Array of associated CWE identifiers                |

---

## Vulnerability Queries

```bash
# Confirm connectivity
coral sql "SELECT * FROM cisa_kev.vulnerabilities LIMIT 1"
```

## Example Queries

Browse the full catalog:

```sql
SELECT * FROM cisa_kev.vulnerabilities LIMIT 10;
```

10 most recently added vulnerabilities:

```sql
SELECT cve_id, vendor_project, product, vulnerability_name, date_added
FROM cisa_kev.vulnerabilities
ORDER BY date_added DESC
LIMIT 10;
```

Vulnerabilities linked to known ransomware campaigns:

```sql
SELECT cve_id, vendor_project, product
FROM cisa_kev.vulnerabilities
WHERE known_ransomware_campaign_use = 'Known';
```

Vulnerabilities for a specific vendor:

```sql
SELECT cve_id, product, vulnerability_name, due_date
FROM cisa_kev.vulnerabilities
WHERE vendor_project = 'Microsoft'
ORDER BY date_added DESC
LIMIT 20;
```

Inspect CWE data for a specific CVE:

```sql
SELECT cve_id, vulnerability_name, cwes
FROM cisa_kev.vulnerabilities
WHERE cve_id = 'CVE-2021-44228';
```

## Notes

- The catalog is a single static JSON file (~1600 entries as of mid-2026). All rows are fetched in one request; no pagination is needed.
- `date_added` and `due_date` are plain `YYYY-MM-DD` strings. Cast with `CAST(date_added AS DATE)` if your SQL engine supports it.
- `cwes` is a JSON array of CWE strings (e.g. `["CWE-89"]`). Query individual entries with `json_get_str(cwes, 0)`.
- No authentication or API key is required.
