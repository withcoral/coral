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

No authentication is required to use the ip-api API. 
*Note: The free API allows up to 45 HTTP requests per minute. If you exceed this limit, your requests will be throttled.*

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
| `countryCode` | Utf8 | Two-letter country code (ISO 3166-1 alpha-2) |
| `region` | Utf8 | Region/state code |
| `regionName` | Utf8 | Region/state name |
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
  SELECT country, regionName, city, lat, lon, isp
  FROM ip_api.location
  LIMIT 1
"

# Fetch geolocation details for a specific IP (e.g., Google DNS)
coral sql "
  SELECT query, country, city, isp, asn
  FROM ip_api.location
  WHERE query = '8.8.8.8'
"

# Fetch geolocation details for a domain name
coral sql "
  SELECT query, country, city, isp
  FROM ip_api.location
  WHERE query = 'github.com'
"
```

## Links

- [ip-api documentation](http://ip-api.com/docs/api:json)
