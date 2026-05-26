# Free Dictionary API

Query English word definitions, phonetics, and meanings using the [Free Dictionary API](https://dictionaryapi.dev/).

## Setup

No API key or authentication is needed. Add the source directly:

```bash
coral source add --file sources/community/free-dictionary/manifest.yaml
```

## Unknown Words

If you query for a word that does not exist in the dictionary, the API returns a 404 response. Coral is configured to gracefully handle this and will return an empty result set rather than throwing an error.

## Tables

### `entries`
Fetch the dictionary entries for a specific English word. Requires the `word` filter.

**Example:**
```sql
SELECT word, meanings, phonetics
FROM free_dictionary.entries
WHERE word = 'hello';
```
