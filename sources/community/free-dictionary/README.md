# Free Dictionary API

Query English word definitions, phonetics, and meanings using the [Free Dictionary API](https://dictionaryapi.dev/).

## Setup

No API key or authentication is needed. Add the source directly:

```bash
coral source add --file sources/community/free-dictionary/manifest.yaml
```

## Tables

### `entries`
Fetch the dictionary entries for a specific English word. Requires the `word` filter.

**Example:**
```sql
SELECT word, meanings, phonetics
FROM free_dictionary.entries
WHERE word = 'hello';
```
