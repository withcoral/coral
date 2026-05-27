# Cat Facts

Fetch random and paginated cat facts from the [CatFact.ninja API](https://catfact.ninja/).

## Setup

No authentication is required. Add the source:

```bash
coral source add --file sources/community/cat-facts/manifest.yaml
```

## Local Testing

```bash
coral sql "
  SELECT fact, length 
  FROM cat_facts.random_fact 
  LIMIT 1
"

/*
+-----------------------------------------------------------------------------+--------+
| fact                                                                        | length |
+-----------------------------------------------------------------------------+--------+
| In multi-cat households, cats of the opposite sex usually get along better. | 75     |
+-----------------------------------------------------------------------------+--------+
*/
```

## Tables

| Table | Description |
|-------|-------------|
| `random_fact` | Get a single random cat fact. |
| `facts` | List paginated cat facts. Supports querying up to maximum facts available. |
