# Gemini

Query Google Gemini models and run LLM inference using the Gemini API via Coral SQL.

```bash
coral source add --file sources/community/gemini/manifest.yaml
```

## Setup

A Gemini API key is required. You can obtain a free API key at [Google AI Studio](https://aistudio.google.com/app/apikey).

Provide the key during setup:

```bash
coral source add --file sources/community/gemini/manifest.yaml --interactive
```
*(When prompted for `GEMINI_API_KEY`, paste your key.)*

## Tables

| Table          | Description                                           |
| -------------- | ----------------------------------------------------- |
| `models`       | List all available Gemini models and their capabilities. |
| `generate`     | Run prompt inference against a specific Gemini model. |

## Filters

The `generate` table requires specific filters to execute prompts:

| Filter        | Required | Description                                                                        |
| ------------- | -------- | ---------------------------------------------------------------------------------- |
| `model`       | **Yes**  | The Gemini model ID to use (e.g., `gemini-1.5-flash`, `gemini-1.5-pro`).           |
| `prompt`      | **Yes**  | The text prompt to send to the model.                                              |
| `system`      | No       | Optional system instruction to guide the model's behavior.                         |
| `temperature` | No       | Controls randomness (e.g., `0.0` for deterministic, `1.0` for maximum creativity). |

## Example queries

```sql
-- List available models
SELECT name, version, display_name, input_token_limit, output_token_limit
FROM gemini.models
LIMIT 5;

-- Generate text using gemini-1.5-flash
SELECT response, prompt_token_count, candidates_token_count
FROM gemini.generate
WHERE model = 'gemini-1.5-flash' 
  AND prompt = 'Explain how a SQL JOIN works in one short paragraph.';

-- Generate text with a custom system prompt and temperature
SELECT response
FROM gemini.generate
WHERE model = 'gemini-1.5-flash'
  AND prompt = 'How do I center a div?'
  AND system = 'You are a pirate front-end developer. Always speak like a pirate.'
  AND temperature = 1.0;
```

## Links

- [Gemini API Documentation](https://ai.google.dev/api)
- [Get an API Key](https://aistudio.google.com/app/apikey)
- [Gemini Models List](https://ai.google.dev/models/gemini)

## Local Testing

```bash
GEMINI_API_KEY=<key> coral source add --file sources/community/gemini/manifest.yaml
# Added source gemini
#
#   ✓ gemini connected successfully
#
#     gemini (2 tables)
#     ├─ generate
#     └─ models
#     Query tests
#     1 declared · 1 passed · 0 failed
#
#     ✓ SELECT name, version FROM gemini.models LIMIT 5
#       5 rows

coral source test gemini
#   ✓ gemini connected successfully
#
#     gemini (2 tables)
#     ├─ generate
#     └─ models
#     Query tests
#     1 declared · 1 passed · 0 failed
#
#     ✓ SELECT name, version FROM gemini.models LIMIT 5
#       5 rows

coral sql "SELECT response FROM gemini.generate(model => 'gemini-flash-latest', prompt => 'What is SQL?') LIMIT 1"
# +-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
# | response                                                                                                                                                                                                                                |
# +-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
# | **SQL** (pronounced either **"S-Q-L"** or **"Sequel"**) stands for **Structured Query Language**.                                                                                                                                       |
# |                                                                                                                                                                                                                                         |
# | In simple terms, SQL is the standard language used to communicate with, manage, and manipulate **relational databases**.                                                                                                                |
# |                                                                                                                                                                                                                                         |
# | If you think of a database as a giant, highly organized digital filing cabinet, SQL is the language you use to ask the filing cabinet clerk to store, retrieve, change, or delete information...                                        |
# +-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
```
