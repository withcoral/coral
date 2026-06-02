# Advice Slip

Generate random advice or search for specific advice using the [Advice Slip API](https://api.adviceslip.com/).

## Setup

No authentication is required. Add the source:

```bash
coral source add --file sources/community/advice-slip/manifest.yaml
```

## Local Testing

```bash
coral sql "
  SELECT id, advice 
  FROM advice_slip.advices 
  WHERE query = 'spiders' 
  LIMIT 1
"

/*
+----+---------------------------------------------------------------------+
| id | advice                                                              |
+----+---------------------------------------------------------------------+
| 1  | Remember that spiders are more afraid of you, than you are of them. |
+----+---------------------------------------------------------------------+
*/
```

## Tables

| Table | Description |
|-------|-------------|
| `random_advice` | Get a single random piece of advice. |
| `advices` | Search for advice containing a specific keyword via the `query` filter. |
