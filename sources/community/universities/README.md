# Universities List

Search universities worldwide by name or country via the [HipoLabs Universities API](https://github.com/Hipo/university-domains-list).

## Setup

No authentication is required. Add the source:

```bash
coral source add --file sources/community/universities/manifest.yaml
```

## Local Testing

```bash
coral sql "
  SELECT name, country 
  FROM universities.universities 
  WHERE country = 'United States' 
  LIMIT 2
"

/*
+-----------------------+---------------+
| name                  | country       |
+-----------------------+---------------+
| Marywood University   | United States |
| Lindenwood University | United States |
+-----------------------+---------------+
*/
```

## Tables

| Table | Description |
|-------|-------------|
| `universities` | List of universities filtered by `name` or `country`. |
