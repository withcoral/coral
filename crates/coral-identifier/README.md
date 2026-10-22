# Coral identifiers

A new Task or Program Run is named with four words from `words.txt`, drawn at
random and joined by hyphens: `height-word-database-library`. The words are the
identifier. [ADR 0023](../../docs/adr/0023-tasks-and-runs-named-with-four-words.md)
records the shape, the uniqueness rule and the growth path.

## The word list

`words.txt` holds 246 lowercase words, one per line, sorted. Every word is one
token on the tokenizers of Claude, GPT, Llama 3, Qwen3, Gemma 3, Mistral and
DeepSeek, so an agent copies an identifier for about eight tokens on Claude and
four on GPT. The list was curated by hand from the 713 words that pass that
rule: no stop words, numbers, ordinals, directions, months, names, places,
nationalities, plurals, participles, code tokens, Coral terms, or sensitive,
political or instruction-like words.

The list is read only at generation. A word removed later never invalidates a
stored identifier.

## Validate the list

`validate_words.py` checks every word against the nine tokenizers. It is the
script behind the list, and it is not a CI gate: the list changes by hand,
rarely, and the check downloads tokenizers once.

```sh
uv run crates/coral-identifier/validate_words.py
uv run crates/coral-identifier/validate_words.py candidates.txt
ANTHROPIC_API_KEY=… uv run crates/coral-identifier/validate_words.py --api
```

The script exits with status 1 and lists every word that fails, with the
tokenizers it fails on. `--api` repeats the Claude check against the official
`count_tokens` API. The tokenizers are a 2026 snapshot: a new model family
needs a new run and may shrink the list.

## Change the list

1. Edit `words.txt`. Keep it sorted, one word per line.
2. Run `validate_words.py`.
3. Update the word count in the `list_is_sorted_unique_lowercase_and_denies_nothing_hazardous`
   test in `src/lib.rs`, and add any new hazard to its deny list.
4. Run `cargo nextest run -p coral-identifier`.
