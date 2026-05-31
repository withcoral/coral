# ip_api

**Version:** 0.1.0
**Backend:** HTTP
**Tables:** 1
**Base URL:** `http://ip-api.com`

Query geolocation details for an IP address or domain using the free [ip-api](http://ip-api.com/) JSON API.

```bash
coral source add --file sources/community/ip_api/manifest.yaml
```

## Setup

No authentication is required for the free endpoint. However, the free API is restricted to non-commercial use and only available over HTTP (not HTTPS). Commercial use or HTTPS support requires an [ip-api Pro](https://ip-api.com/) subscription.

**Rate Limits:** The free endpoint allows up to 45 requests per minute. If exceeded, requests will return a 429 status and your IP may be banned for 1 hour for repeated overuse. Limits can be tracked via the `X-Rl` and `X-Ttl` response headers. Note that agent-driven queries can easily exhaust this shared IP limit.

## Tables

| Table | Description | Filters |
|---|---|---|
| `location` | Fetch geolocation details for the current IP, or a specific IP/domain | `query` (optional) |

---

### `location`

Fetch geolocation details. By default, it fetches the details for the IP from which the query is originating. If `query` is provided, it fetches details for that specific IP address or domain.

#### Filters

| Filter | Type | Required | Description |
|---|---|---|---|
| `query` | Utf8 | No | The IPv4, IPv6 address, or domain name to look up. |

#### Columns

| Column | Type | Description |
|---|---|---|
| `query` | Utf8 | The queried IP address or domain |
| `status` | Utf8 | Response status (e.g., `success`, `fail`) |
| `message` | Utf8 | Error message if status is `fail` |
| `country` | Utf8 | Country name |
| `country_code` | Utf8 | Two-letter country code (ISO 3166-1 alpha-2, mapped from API's `countryCode`) |
| `region` | Utf8 | Region/state code |
| `region_name` | Utf8 | Region/state name (mapped from API's `regionName`) |
| `city` | Utf8 | City name |
| `zip` | Utf8 | Zip/postal code |
| `lat` | Float64 | Latitude |
| `lon` | Float64 | Longitude |
| `timezone` | Utf8 | Timezone (e.g., `America/New_York`) |
| `isp` | Utf8 | Internet Service Provider name |
| `org` | Utf8 | Organization name |
| `asn` | Utf8 | AS number and organization (mapped from the API's `as` field) |

---

## Quick start

```bash
# Fetch geolocation details for your current IP address
coral sql "
  SELECT country, region_name, city, lat, lon, isp
  FROM ip_api.location
  LIMIT 1
"

/*
+---------+-------------------------------------+-----------+---------+---------+-------------------------------+
| country | region_name                         | city      | lat     | lon     | isp                           |
+---------+-------------------------------------+-----------+---------+---------+-------------------------------+
| India   | National Capital Territory of Delhi | New Delhi | 28.6327 | 77.2198 | Reliance Jio Infocomm Limited |
+---------+-------------------------------------+-----------+---------+---------+-------------------------------+
*/

# Fetch geolocation details for a specific IP (e.g., Google DNS)
coral sql "
  SELECT query, country, city, isp, asn
  FROM ip_api.location
  WHERE query = '8.8.8.8'
"

/*
+---------+---------------+---------+------------+--------------------+
| query   | country       | city    | isp        | asn                |
+---------+---------------+---------+------------+--------------------+
| 8.8.8.8 | United States | Ashburn | Google LLC | AS15169 Google LLC |
+---------+---------------+---------+------------+--------------------+
*/

# Fetch geolocation details for a domain name
coral sql "
  SELECT query, country, city, isp
  FROM ip_api.location
  WHERE query = 'github.com'
"

/*
+----------------+-----------+-----------+-----------------------+
| query          | country   | city      | isp                   |
+----------------+-----------+-----------+-----------------------+
| 20.205.243.166 | Singapore | Singapore | Microsoft Corporation |
+----------------+-----------+-----------+-----------------------+
*/
```

## Links

- [ip-api documentation](http://ip-api.com/docs/api:json)
