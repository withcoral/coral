# Google Ads community source

The `google_ads` community source exposes read-only Google Ads campaign, ad group, ad, and performance report data using Google Ads Query Language (GAQL) templates through Coral SQL.

## Setup

Google Ads API requires OAuth 2.0 user authentication with appropriate developer credentials.

To use the Google Ads source:
1. Ensure the Google Ads API is enabled in your Google Cloud Console.
2. Obtain a **Developer Token** from your Google Ads Manager account.
3. Obtain your **Customer ID** (a 10-digit number, e.g. `1234567890`).
4. Generate an OAuth 2.0 access token requesting the read-only or read/write scope:
   * `https://www.googleapis.com/auth/adwords`
5. Install the source:

```sh
export GOOGLE_ADS_CUSTOMER_ID="<your-customer-id>"
export GOOGLE_ADS_DEVELOPER_TOKEN="<your-developer-token>"
export GOOGLE_ADS_ACCESS_TOKEN="<your-access-token>"
coral source add --file sources/community/google_ads/manifest.yaml
```

Alternatively, you can run the interactive setup flow in the Coral UI by selecting **Connect Google Ads** and providing your Google Cloud OAuth Client ID and Secret.

## Tables

| Table | Purpose | Required Filters |
| --- | --- | --- |
| `google_ads.campaigns` | Lists all campaigns in the account. | None |
| `google_ads.ad_groups` | Lists ad groups, including implicit campaign details. | None |
| `google_ads.ads` | Lists ads (ad group ads), including associated ad group and campaign IDs. | None |
| `google_ads.campaign_performance` | Lists campaign performance report metrics by date. | `start_date`, `end_date` |

All tables are read-only. This source does not mutate ads or modify budgets.

### Important Design Quirks

* **Currency "Micros" Multiplier**: Financial columns like `cost_micros` are multiplied by one million to avoid floating-point math issues. To extract the actual currency value, you must divide the value by `1,000,000` (e.g. `cost_micros / 1000000.0`).
* **Required Date Range Filters**: The `campaign_performance` table requires `start_date` and `end_date` filters in the `YYYY-MM-DD` format. These filter values are pushed directly down into the Google Ads Query Language (GAQL) `WHERE` clause.
* **Relational Joins**: To make joins simple, the `ad_groups` and `ads` tables include native calculated `campaign_id` and `ad_group_id` columns, fetched directly from the GAQL relations.

## Example queries

List enabled campaigns:

```sql
SELECT id, name, status, advertising_channel_type
FROM google_ads.campaigns
WHERE status = 'ENABLED'
LIMIT 50;
```

List ad groups and their campaigns:

```sql
SELECT id, name, status, campaign_name, campaign_id
FROM google_ads.ad_groups
LIMIT 50;
```

Calculate Click-Through Rate (CTR) and Cost-Per-Click (CPC) from performance reports:

```sql
SELECT
  campaign_name,
  SUM(impressions) AS total_impressions,
  SUM(clicks) AS total_clicks,
  SUM(cost_micros) / 1000000.0 AS total_cost,
  CASE
    WHEN SUM(impressions) > 0 THEN (SUM(clicks) * 1.0 / SUM(impressions)) * 100
    ELSE 0.0
  END AS ctr_percentage,
  CASE
    WHEN SUM(clicks) > 0 THEN (SUM(cost_micros) / 1000000.0) / SUM(clicks)
    ELSE 0.0
  END AS cpc
FROM google_ads.campaign_performance
WHERE start_date = '2026-05-01' AND end_date = '2026-05-20'
GROUP BY campaign_name;
```

## Validation

Lint the manifest:

```sh
coral source lint sources/community/google_ads/manifest.yaml
```

Install and test with a real or mock token:

```sh
export GOOGLE_ADS_CUSTOMER_ID="1234567890"
export GOOGLE_ADS_DEVELOPER_TOKEN="mock_dev_token"
export GOOGLE_ADS_ACCESS_TOKEN="mock_token"
export GOOGLE_ADS_API_BASE="http://127.0.0.1:8899"
coral source add --file sources/community/google_ads/manifest.yaml
coral sql "SELECT * FROM google_ads.campaigns LIMIT 1"
```
