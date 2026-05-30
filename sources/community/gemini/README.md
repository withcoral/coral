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

## Functions

| Function       | Description                                           |
| -------------- | ----------------------------------------------------- |
| `generate`     | Run prompt inference against a specific Gemini model. |

## Arguments

The `generate` function requires specific arguments to execute prompts:

| Argument      | Required | Description                                                                        |
| ------------- | -------- | ---------------------------------------------------------------------------------- |
| `model`       | **Yes**  | The base model ID to use (e.g., `gemini-2.5-flash`, `gemini-1.5-pro`).             |
| `prompt`      | **Yes**  | The text prompt to send to the model.                                              |
| `temperature` | No       | Optional temperature controlling randomness (e.g., `0.7`).                         |

## Example queries

```sql
-- List available models
SELECT name, base_model_id, display_name, supported_generation_methods
FROM gemini.models
LIMIT 5;
/*
+----------------------------------+---------------+---------------------------+--------------------------------------------------------------------------------+
| name                             | base_model_id | display_name              | supported_generation_methods                                                   |
+----------------------------------+---------------+---------------------------+--------------------------------------------------------------------------------+
| models/gemini-2.5-flash          |               | Gemini 2.5 Flash          | ["generateContent","countTokens","createCachedContent","batchGenerateContent"] |
| models/gemini-2.5-pro            |               | Gemini 2.5 Pro            | ["generateContent","countTokens","createCachedContent","batchGenerateContent"] |
| models/gemini-2.0-flash          |               | Gemini 2.0 Flash          | ["generateContent","countTokens","createCachedContent","batchGenerateContent"] |
| models/gemini-2.0-flash-001      |               | Gemini 2.0 Flash 001      | ["generateContent","countTokens","createCachedContent","batchGenerateContent"] |
| models/gemini-2.0-flash-lite-001 |               | Gemini 2.0 Flash-Lite 001 | ["generateContent","countTokens","createCachedContent","batchGenerateContent"] |
+----------------------------------+---------------+---------------------------+--------------------------------------------------------------------------------+
*/

-- Generate text using gemini-2.5-flash
SELECT response, prompt_token_count, candidates_token_count
FROM gemini.generate(
  model => 'gemini-2.5-flash',
  prompt => 'Explain how a SQL JOIN works in one short paragraph.'
);
/*
+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------------+------------------------+
| response                                                                                                                                                                                                                                                                                                                      | prompt_token_count | candidates_token_count |
+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------------+------------------------+
| A SQL JOIN combines rows from two or more tables into a single result set by matching values in a specified common column (or columns) between them. This allows you to retrieve a comprehensive view of related data that is logically separated across different tables, linking them based on their defined relationships. | 12                 | 57                     |
+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------------+------------------------+
*/

-- Generate text with a custom temperature
SELECT response
FROM gemini.generate(
  model => 'gemini-2.5-flash',
  prompt => 'Write a haiku about databases.',
  temperature => '0.9'
);
/*
+-----------------------------------+
| response                          |
+-----------------------------------+
| Data kept so safe,                |
| Rows and tables, structured vast, |
| Ready for your call.              |
+-----------------------------------+
*/
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
#     gemini (1 table)
#     └─ models
#     Query tests
#     1 declared · 1 passed · 0 failed
#
#     ✓ SELECT name, version FROM gemini.models LIMIT 5
#       5 rows

coral sql "SELECT response FROM gemini.generate(model => 'gemini-2.5-flash', prompt => 'What is 2+2? Reply with just the number.')"
# +----------+
# | response |
# +----------+
# | 4        |
# +----------+
```
