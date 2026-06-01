# NREL & NLR Community Data Source

A high-performance community source specification for **NREL (National Renewable Energy Laboratory) / NLR (National Laboratory of the Rockies)**. This connector exposes physics-based photovoltaic performance calculators, Typical Meteorological Year (TMY) solar resource matrices, and the US national alternative fuel station registry directly as queryable SQL tables.

By translating complex, nested energy data structures into relational models, this source enables AI agents, analytical platforms, and optimization pipelines (like the hybrid ML battery dispatch systems in **SuryaOS**) to interact with renewable energy data seamlessly using SQL.

---

## ⚡ Domain Transition Context

> [!IMPORTANT]
> **API Server Domain Migration:**
> The legacy NREL developer domain (`developer.nrel.gov`) is undergoing a complete retirement and service shutdown on **May 29, 2026**.
>
> All requests must transition to the new **National Laboratory of the Rockies (NLR)** domain: **`developer.nlr.gov`**.
>
> **How this connector protects you:**
> This source is built with future-proof defaults:
> - **`NREL_API_BASE`** defaults automatically to `https://developer.nlr.gov`.
> - If you must maintain compatibility with legacy local mocks or older integrations during the final transition week, simply override the base URL by setting the `NREL_API_BASE` input variable to `https://developer.nrel.gov`.

---

## 🔑 Authentication Setup

NLR/NREL relies on a secure API key authentication system.

1. Sign up for a free developer key at [developer.nlr.gov/signup](https://developer.nlr.gov/signup/).
2. Provide your API key to Coral by configuring the `NREL_API_KEY` secret.
3. The connector attaches this token under the recommended **`X-Api-Key`** HTTP header dynamically.

---

## 🚀 Install

Run the following from the **repository root**:

```bash
# Register the source with your API key
NREL_API_KEY=your-nlr-api-key coral source add --file sources/community/nrel/manifest.yaml

# Verify the source connects successfully
coral source test nrel
```

Or interactively (Coral will prompt for the API key):

```bash
coral source add --file sources/community/nrel/manifest.yaml --interactive
```

To override the base URL (e.g. to use the legacy domain during transition):

```bash
NREL_API_KEY=your-nlr-api-key NREL_API_BASE=https://developer.nrel.gov \
  coral source add --file sources/community/nrel/manifest.yaml
```

---

## 📊 Exposed Tables

### 1. `nrel.pvwatts`
Runs a photovoltaic system performance simulation using the industry-standard **PVWatts V8** compute module. It models fixed or tracking flat-plate systems using current TMY solar resource climate stations.

* **Required Filters**:
  - `lat` (Float64): Latitude coordinate of the simulated system.
  - `lon` (Float64): Longitude coordinate of the simulated system.
  - `system_capacity` (Float64): Nameplate DC capacity rating in kW.
  - `module_type` (Int64): `0` = Standard, `1` = Premium, `2` = Thin Film.
  - `array_type` (Int64): `0` = Fixed Open, `1` = Fixed Roof, `2` = 1-Axis Tracker, `3` = 1-Axis Backtracking, `4` = 2-Axis Tracker.
  - `losses` (Float64): System percentage losses (-5.0 to 99.0). Typically `14.0`.
  - `tilt` (Float64): Module tilt angle in degrees (0 to 90).
  - `azimuth` (Float64): Module azimuth orientation in degrees (0 <= azimuth < 360).
* **Optional Configuration Filters**:
  - `bifacial` (Float64): Bifaciality ratio (decimal between 0 and 1, typically 0.65 to 0.9).
  - `albedo` (Float64): Ground reflectance ratio (0 to 1).
  - `soiling` (Utf8): Pipe-delimited array of 12 monthly percentage values in the 0 to 100 range (e.g., `5|5|...` for 5% monthly soiling).

### 2. `nrel.solar_resource`
Fetches Typical Meteorological Year (TMY) averages for a coordinate. Useful for high-level site planning and physical GHI/DNI evaluations before designing actual arrays.

* **Primary Filters**:
  - `lat` (Float64, **Required**): Target latitude coordinate.
  - `lon` (Float64, **Required**): Target longitude coordinate.

### 3. `nrel.alt_fuel_stations`
Integrates the complete US Clean Transportation database, exposing the location, network provider, connector types, and real-time operational status of alternative fuel stations (e.g. EV charging stations, CNG, E85, Hydrogen).

* **Primary Filters**:
  - `fuel_type` (Utf8, Optional): Fuel filter (e.g., `ELEC` for EV charging, `CNG`, `LPG`, `E85`).
  - `state` (Utf8, Optional): Filter by state abbreviation (e.g., `CO`, `CA`, `NY`).
  - `zip` (Utf8, Optional): Filter by exact ZIP code.
  - `status` (Utf8, Optional): Operational status (`E` for open, `P` for planned, `T` for temporary closure).
  - `access` (Utf8, Optional): Access type (`public` or `private`).
  - `ev_network` (Utf8, Optional): Charging provider network (e.g., `Tesla`, `ChargePoint`).

---

## 💡 Practical SQL Examples

### A. PV Solar System Output Simulation
Determine how much annual AC energy is generated and calculate the system capacity factor for a 10 kW premium rooftop array in Boulder, Colorado ($40.0^\circ\text{ N}, 105.2^\circ\text{ W}$):
```sql
SELECT
  ac_annual,
  capacity_factor,
  station_elevation
FROM nrel.pvwatts
WHERE lat = 40.0
  AND lon = -105.2
  AND system_capacity = 10.0
  AND module_type = 1
  AND array_type = 1
  AND tilt = 30.0
  AND azimuth = 180.0
  AND losses = 14.0;
```

### B. Typical Meteorological Year Irradiance Scan
Fetch monthly Direct Normal Irradiance (DNI) and Global Horizontal Irradiance (GHI) month-keyed JSON objects to analyze winter-vs-summer seasonality:
```sql
SELECT
  avg_dni_annual,
  avg_ghi_annual,
  avg_ghi_monthly
FROM nrel.solar_resource
WHERE lat = 34.05
  AND lon = -118.24;
```

### C. Find Open Public Tesla Fast Chargers in a Specific State
Audit regional charging infrastructure by filtering the national stations directory for public Superchargers in Colorado:
```sql
SELECT
  station_name,
  street_address,
  city,
  ev_dc_fast_num,
  ev_connector_types
FROM nrel.alt_fuel_stations
WHERE state = 'CO'
  AND fuel_type = 'ELEC'
  AND access = 'public'
  AND ev_network = 'Tesla'
  AND status = 'E'
LIMIT 10;
```

## 📈 Rate Limits & Telemetry

NLR/NREL enforces standard provider rate limits based on your developer API key:
- **Default Limit**: 1,000 requests per hour.
- **Breaching the Limit**: If you exceed the hourly request allotment, the API will return HTTP `429 Too Many Requests` errors. These errors are cleanly surfaced and propagated by Coral directly to your query interface, allowing your application logic to handle retries.
- **Monitoring**: NLR returns rate limit metadata in these standard response headers:
  - `X-RateLimit-Limit`: Hourly request allotment.
  - `X-RateLimit-Remaining`: Requests remaining in the current hour window.
