#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "ctok==1.3.0",
#   "tiktoken>=0.8",
#   "tokenizers>=0.21",
#   "huggingface_hub>=0.30",
# ]
# ///
"""Check that every word in words.txt is one token on the nine tokenizers.

    uv run crates/coral-identifier/validate_words.py                 # words.txt beside this file
    uv run crates/coral-identifier/validate_words.py candidates.txt  # a list under review
    uv run crates/coral-identifier/validate_words.py --api           # also ask Claude's count_tokens API

The rule behind the list (issue #2412, ADR 0023). The tokenizers are a 2026
snapshot; a new model family needs a new run and may shrink the list.

- Claude 4.7 and later: inside the identifier frame {"run_id":"x-WORD-x"} the
  word costs no more than the reference word "round", which is one token on
  every tokenizer here. ctok 4.7 is the offline judge. It agreed with the
  official count_tokens API on all 14,787 candidate words on 2026-10-19.
  --api repeats this check against the API. The key comes from the
  ANTHROPIC_API_KEY environment variable and is never printed.
- The eight other tokenizers (o200k, cl100k, Llama 3, Qwen3, Gemma 3, Mistral
  Tekken, DeepSeek V3, Claude 4.6 and earlier): the bare word costs no more
  than "round".

The Hugging Face tokenizers download once into the Hugging Face cache. A gated
repository falls back to an open mirror that ships the same tokenizer.json.

Exit status 1 lists every word that fails, with the tokenizers it fails on.
This is not a CI gate: the list changes by hand, rarely, and the check needs
the network once.
"""

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from pathlib import Path

import ctok
import tiktoken
from huggingface_hub import hf_hub_download
from huggingface_hub.errors import HfHubHTTPError
from tokenizers import Tokenizer

FRAME = '{"run_id":"x-%s-x"}'
REFERENCE = "round"
API_MODEL = "claude-sonnet-5-5"
HUGGING_FACE = {
    "llama3": ("meta-llama/Meta-Llama-3-8B", "NousResearch/Meta-Llama-3-8B"),
    "qwen3": ("Qwen/Qwen3-8B", None),
    "gemma3": ("google/gemma-3-1b-it", "unsloth/gemma-3-1b-it"),
    "mistral": ("mistralai/Mistral-Nemo-Instruct-2407", "unsloth/Mistral-Nemo-Instruct-2407"),
    "deepseek": ("deepseek-ai/DeepSeek-V3", None),
}

Counter = Callable[[str], int]


def hugging_face_counter(repository: str, mirror: str | None) -> tuple[str, Counter]:
    """Loads tokenizer.json from the repository, or from its open mirror when access is refused."""
    try:
        path = hf_hub_download(repository, "tokenizer.json")
        source = repository
    except HfHubHTTPError:
        if mirror is None:
            raise
        path = hf_hub_download(mirror, "tokenizer.json")
        source = mirror
    tokenizer = Tokenizer.from_file(path)
    return source, lambda text: len(tokenizer.encode(text, add_special_tokens=False).ids)


def tiktoken_counter(encoding_name: str) -> Counter:
    encoding = tiktoken.get_encoding(encoding_name)
    return lambda text: len(encoding.encode(text, disallowed_special=()))


def bare_word_counters() -> dict[str, Counter]:
    """The eight tokenizers judged on the bare word, with the source of each."""
    counters: dict[str, Counter] = {}
    for name, (repository, mirror) in HUGGING_FACE.items():
        source, counter = hugging_face_counter(repository, mirror)
        print(f"{name}: {source}")
        counters[name] = counter
    for encoding_name in ("o200k_base", "cl100k_base"):
        counters[encoding_name.removesuffix("_base")] = tiktoken_counter(encoding_name)
        print(f"{encoding_name.removesuffix('_base')}: tiktoken {encoding_name}")
    counters["claude<=4.6"] = ctok_counter("3")
    print("claude<=4.6: ctok 3")
    return counters


def ctok_counter(version: str) -> Counter:
    """ctok counts a message overhead with the text, and an empty message costs
    the same as a one-token one, so its counts are only compared, never read
    as absolute token counts."""
    return lambda text: ctok.token_count(text, version)


def claude_frame_counter() -> Counter:
    print("claude4.7+: ctok 4.7, judged inside " + FRAME % "WORD")
    return ctok_counter("4.7")


def api_frame_counter() -> Counter:
    """Claude's official count_tokens, with retries on rate limits and outages."""
    key = os.environ.get("ANTHROPIC_API_KEY")
    if not key:
        sys.exit("--api needs ANTHROPIC_API_KEY in the environment")
    print(f"api: count_tokens for {API_MODEL}, judged inside " + FRAME % "WORD")

    def count(text: str) -> int:
        body = json.dumps(
            {"model": API_MODEL, "messages": [{"role": "user", "content": text}]}
        ).encode()
        for attempt in range(8):
            request = urllib.request.Request(
                "https://api.anthropic.com/v1/messages/count_tokens",
                data=body,
                method="POST",
                headers={
                    "x-api-key": key,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
            )
            try:
                with urllib.request.urlopen(request, timeout=30) as response:
                    return json.load(response)["input_tokens"]
            except urllib.error.HTTPError as error:
                detail = error.read().decode(errors="replace")[:300]
                if error.code in (429, 500, 502, 503, 529) and attempt < 7:
                    time.sleep(float(error.headers.get("retry-after") or 2 * (attempt + 1)))
                    continue
                sys.exit(f"count_tokens failed: HTTP {error.code}: {detail}")
            except (urllib.error.URLError, TimeoutError) as error:
                if attempt < 7:
                    time.sleep(2 * (attempt + 1))
                    continue
                sys.exit(f"count_tokens failed: {error}")
        sys.exit("count_tokens: still failing after 8 attempts")

    return count


def read_words(path: Path) -> list[str]:
    words = [line.strip() for line in path.read_text().splitlines() if line.strip()]
    if not words:
        sys.exit(f"{path} holds no words")
    return words


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument(
        "words",
        nargs="?",
        type=Path,
        default=Path(__file__).with_name("words.txt"),
        help="one lowercase word per line (default: words.txt beside this script)",
    )
    parser.add_argument(
        "--api",
        action="store_true",
        help="also judge each word with Claude's count_tokens API",
    )
    arguments = parser.parse_args()
    words = read_words(arguments.words)

    # Each judge is a counter and the cost of the reference word under it. A
    # word passes a judge when it costs no more than the reference.
    judges: dict[str, tuple[Counter, Callable[[str], str]]] = {}
    for name, counter in bare_word_counters().items():
        judges[name] = (counter, lambda word: word)
    judges["claude4.7+"] = (claude_frame_counter(), lambda word: FRAME % word)
    if arguments.api:
        judges["api"] = (api_frame_counter(), lambda word: FRAME % word)

    # A judge that counts the empty string as zero must count the bare
    # reference word as one token, or the rule it anchors is void.
    reference: dict[str, int] = {}
    for name, (counter, frame) in judges.items():
        reference[name] = counter(frame(REFERENCE))
        if frame(REFERENCE) == REFERENCE and counter("") == 0 and reference[name] != 1:
            sys.exit(f'{name}: the reference word "{REFERENCE}" is {reference[name]} tokens; the rule is void')

    failures: dict[str, list[str]] = {}
    for word in words:
        for name, (counter, frame) in judges.items():
            cost = counter(frame(word))
            if cost > reference[name]:
                failures.setdefault(word, []).append(f"{name} ({cost} > {reference[name]})")

    if failures:
        print(f"\n{len(failures)} of {len(words)} words fail:")
        for word, names in failures.items():
            print(f"  {word}: {', '.join(names)}")
        return 1
    print(f"\n{len(words)} words: every word is one token under all {len(judges)} judges")
    return 0


if __name__ == "__main__":
    sys.exit(main())
