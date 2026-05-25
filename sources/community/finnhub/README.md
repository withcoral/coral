# Finnhub community source

The `finnhub` community source exposes an incredibly comprehensive suite of 50 tables covering real-time market news, stock quotes, forex rates, earnings calendars, institutional ownership, ETFs, ESG scores, and alternative data like Congressional trading from the Finnhub API through Coral SQL.

## Setup

Create or copy a Finnhub API token:

- Create a free account at [Finnhub.io](https://finnhub.io/)
- Copy the API Key from your dashboard.

Then install the source:

```sh
export FINNHUB_API_TOKEN="<token>"
cargo run -p coral-cli -- source add --file sources/community/finnhub/manifest.yaml
```

## Tables

| Table | Purpose |
| --- | --- |
| `finnhub.market_news` | General market news headlines. |
| `finnhub.company_news` | News specific to a company symbol (requires `symbol`, `from`, and `to` filters). |
| `finnhub.news_sentiment` | News sentiment and buzz scores for a company (requires `symbol` filter). |
| `finnhub.economic_calendar` | Upcoming global economic events and indicators. |
| `finnhub.quote` | Real-time quote data for US stocks (requires `symbol` filter). |
| `finnhub.forex_rates` | Live forex exchange rates (requires `base` filter). |
| `finnhub.market_status` | Current open/close status of global exchanges (requires `exchange` filter). |
| `finnhub.company_profile` | General information of a company (requires `symbol` filter). |
| `finnhub.basic_financials` | Basic financial metrics for a company (requires `symbol` filter). |
| `finnhub.insider_transactions` | Company insider transactions (requires `symbol` filter). |
| `finnhub.recommendation_trends` | Latest analyst recommendation trends (requires `symbol` filter). |
| `finnhub.earnings_calendar` | Historical and upcoming earnings calendar (requires `from`, `to` filters). |
| `finnhub.market_holiday` | List of market holidays for an exchange (requires `exchange` filter). |
| `finnhub.ipo_calendar` | Recent and upcoming IPO calendar (requires `from`, `to` filters). |
| `finnhub.forex_symbols` | List of supported forex symbols for an exchange (requires `exchange` filter). |
| `finnhub.stock_price_target` | Latest price target consensus (requires `symbol` filter). |
| `finnhub.stock_upgrade_downgrade` | Analyst upgrades and downgrades (requires `symbol` filter). |
| `finnhub.stock_ownership` | Institutional shareholders list (requires `symbol` filter). |
| `finnhub.stock_fund_ownership` | Fund shareholders list (requires `symbol` filter). |
| `finnhub.eps_estimates` | Company EPS estimates and consensus (requires `symbol` filter). |
| `finnhub.stock_revenue_estimate` | Company revenue estimates (requires `symbol` filter). |
| `finnhub.stock_ebitda_estimate` | Company EBITDA estimates (requires `symbol` filter). |
| `finnhub.stock_ebit_estimate` | Company EBIT estimates (requires `symbol` filter). |
| `finnhub.stock_net_income_estimate` | Company Net Income estimates (requires `symbol` filter). |
| `finnhub.stock_pretax_income_estimate` | Company Pretax Income estimates (requires `symbol` filter). |
| `finnhub.stock_dps_estimate` | Company Dividends Per Share estimates (requires `symbol` filter). |
| `finnhub.stock_dividend` | Company dividend history (requires `symbol`, `from`, `to` filters). |
| `finnhub.stock_split` | Company stock split history (requires `symbol`, `from`, `to` filters). |
| `finnhub.stock_executives` | Company executives list (requires `symbol` filter). |
| `finnhub.symbol_search` | Search for best-matching stock symbols (requires `q` filter). |
| `finnhub.etf_profile` | General information about an ETF (requires `symbol` filter). |
| `finnhub.etf_sector` | ETF sector exposure (requires `symbol` filter). |
| `finnhub.etf_country` | ETF country exposure (requires `symbol` filter). |
| `finnhub.mutual_fund_profile` | General information about a mutual fund (requires `symbol` filter). |
| `finnhub.crypto_profile` | General information about a cryptocurrency (requires `symbol` filter). |
| `finnhub.stock_esg` | Company ESG scores (requires `symbol` filter). |
| `finnhub.stock_congressional_trading` | US Congressional trading activities (requires `symbol` filter). |
| `finnhub.stock_visa_application` | Company H1-B visa applications (requires `symbol` filter). |
| `finnhub.stock_lobbying` | Company lobbying activities (requires `symbol` filter). |
| `finnhub.stock_usa_spending` | Company USA spending and government contracts (requires `symbol` filter). |
| `finnhub.stock_uspto_patent` | Company USPTO patents (requires `symbol` filter). |
| `finnhub.stock_earnings_quality_score` | Company earnings quality score (requires `symbol` filter). |
| `finnhub.stock_historical_market_cap` | Historical market cap (requires `symbol` filter). |
| `finnhub.stock_historical_employee_count` | Historical employee count (requires `symbol` filter). |
| `finnhub.stock_price_metric` | Historical price metrics by date (requires `symbol`, `date` filters). |
| `finnhub.stock_symbols` | List of supported stock symbols for an exchange (requires `exchange` filter). |
| `finnhub.crypto_symbols` | List of supported crypto symbols for an exchange (requires `exchange` filter). |
| `finnhub.transcripts_list` | List of earnings call transcripts (requires `symbol` filter). |
| `finnhub.bond_profile` | General information about a bond (requires `isin` filter). |
| `finnhub.financials_reported` | As-reported financial statements (requires `symbol` filter). |

All tables are read-only. 

## Example queries

Get the latest general market news:

```sql
SELECT datetime, headline, summary, url 
FROM finnhub.market_news 
LIMIT 10;
```

Get real-time quotes for a stock:

```sql
SELECT c AS current_price, h AS high, l AS low, pc AS previous_close 
FROM finnhub.quote 
WHERE symbol = 'AAPL';
```

Track upcoming high-impact economic events:

```sql
SELECT time, country, event, impact, actual, estimate 
FROM finnhub.economic_calendar 
WHERE impact = 'High';
```

Check US Congressional trading to see what politicians are buying:

```sql
SELECT name, position, transactionDate, amount
FROM finnhub.stock_congressional_trading 
WHERE symbol = 'NVDA';
```

Check the latest Environmental, Social, and Governance (ESG) scores:

```sql
SELECT totalESGScore, environmentScore, socialScore, governanceScore
FROM finnhub.stock_esg
WHERE symbol = 'MSFT';
```

Find all available earnings call transcripts for a company:

```sql
SELECT year, quarter, title, time
FROM finnhub.transcripts_list
WHERE symbol = 'AAPL'
ORDER BY year DESC, quarter DESC;
```

Search for a stock symbol by name:

```sql
SELECT description, displaySymbol, type 
FROM finnhub.symbol_search 
WHERE q = 'apple';
```

## Validation

Lint the manifest:

```sh
cargo run -p coral-cli -- source lint sources/community/finnhub/manifest.yaml
```

Install and test with a real token:

```sh
export FINNHUB_API_TOKEN="<token>"
cargo run -p coral-cli -- source add --file sources/community/finnhub/manifest.yaml
cargo run -p coral-cli -- source test finnhub
```
