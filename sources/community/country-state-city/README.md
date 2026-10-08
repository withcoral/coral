# Country State City

Retrieve geographic data about countries, states, and cities worldwide using the [Country State City API](https://countrystatecity.in/).

## Setup

This source requires a free API key from countrystatecity.in.
Provide it during setup:

```bash
coral source add --file sources/community/country-state-city/manifest.yaml --interactive
```
*(When prompted for `api_key`, paste your `X-CSCAPI-KEY` value.)*

## Local Testing

```bash
coral sql "
  SELECT name 
  FROM country_state_city.cities 
  WHERE ciso = 'US' AND siso = 'CA' 
  LIMIT 2
"

/*
+----------------+
| name           |
+----------------+
| Acalanes Ridge |
| Acton          |
+----------------+
*/
```

## Tables

| Table | Description |
|-------|-------------|
| `countries` | List of all countries globally. |
| `states` | List of states for a specific country (filtered by `ciso`). |
| `cities` | List of cities for a specific state and country (filtered by `ciso` and `siso`). |
