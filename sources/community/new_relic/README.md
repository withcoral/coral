# New Relic source

Query New Relic NerdGraph accounts, user context, monitored entities, and NRQL
results from Coral SQL.

## Credentials

Create a New Relic user API key with NerdGraph access, then add the source:

```bash
export NEW_RELIC_USER_KEY="..."
coral source add --file sources/community/new_relic/manifest.yaml
```

For EU accounts:

```bash
export NEW_RELIC_GRAPHQL_URL="https://api.eu.newrelic.com/graphql"
```

## Start here

```sql
SELECT id, name
FROM new_relic.accounts;
```

Run NRQL through Coral:

```sql
SELECT raw
FROM new_relic.nrql(
  account_id => 123456,
  query => 'SELECT count(*) FROM Transaction SINCE 1 HOUR AGO'
);
```

Search entities with New Relic entity search syntax:

```sql
SELECT guid, name, domain, type, reporting, alert_severity
FROM new_relic.search_entities(query => 'name LIKE `%api%`')
LIMIT 50;
```

Inspect a specific entity:

```sql
SELECT guid, name, account_id, tags, permalink
FROM new_relic.entity
WHERE guid = '...';
```

## Notes

- This source is read-only and uses NerdGraph queries only.
- NRQL result rows are dynamic, so the `raw` JSON column is the canonical
  output. Common aggregate and timeseries fields are exposed as convenience
  columns when present.
- Use the EU NerdGraph endpoint for New Relic accounts hosted in the EU data
  center.

## References

- NerdGraph introduction: <https://docs.newrelic.com/docs/apis/nerdgraph/get-started/introduction-new-relic-nerdgraph/>
- NRQL through NerdGraph: <https://docs.newrelic.com/docs/apis/nerdgraph/examples/nerdgraph-nrql-tutorial/>
- New Relic API keys: <https://docs.newrelic.com/docs/apis/intro-apis/new-relic-api-keys/>
