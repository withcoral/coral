#!/usr/bin/env python3
"""Detect likely-truncated descriptions in Coral source manifests.

A manifest's column/table/source descriptions feed documentation, MCP surfaces,
and the coral.columns catalog. When upstream generation (e.g. OpenAPI -> YAML)
applies a character cap to each description, sentences get cut mid-phrase.
This tool scans every `sources/*/manifest.y{a,}ml` and flags descriptions that
exhibit one or more deterministic truncation signals.

Signals (ordered from least to most likely false-positive):
  - ends-with-mid-punctuation: text ends with `,`, `;`, `:`, or `-`
  - unbalanced-brackets: more `(` than `)` (or `[`/`]`, `{`/`}`)
  - unbalanced-backticks: odd count of backticks
  - ends-with-open-bracket: final char is `(`, `[`, or `{`
  - ends-with-stopword: final word is an article, aux verb, conjunction,
    relative pronoun, or distributive determiner — categories that require a
    grammatical complement
  - suspicious-length: single-line description with a 120-130 char trailing
    clause that doesn't end in a sentence-terminating character, matching the
    LLM-generation caps observed in SOURCE-465

A small number of legitimate short descriptions ending with auxiliary verbs
(e.g. "...what X does", "...roles Y can have") may surface as false positives.
Reviewers should cross-reference with the upstream source spec before rewriting.

Exit code:
  0 - no suspected truncations found
  1 - suspected truncations found
  2 - usage error

Usage:
  python3 scripts/detect-truncated-descriptions.py [sources/...] [--verbose]
  python3 scripts/detect-truncated-descriptions.py --self-test
"""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path


# Tokens that rarely, if ever, terminate a complete English description.
# If a description ends on one of these, it almost certainly got chopped.
#
# Prepositions are intentionally excluded: English relative clauses often end
# with a preposition ("the team this user belongs to"), so flagging them
# yields mostly false positives. Similarly ambiguous tokens ("not", "any",
# "all", "these") are omitted because they appear naturally in phrases like
# "enabled or not", "if any", or "created_by_me or all".
SENTENCE_TAIL_STOPWORDS: frozenset[str] = frozenset(
    {
        # Articles — almost always followed by a noun
        "a", "an", "the",
        # Auxiliary verbs — almost always followed by a complement
        "is", "are", "was", "were", "be", "been", "being", "am",
        "has", "have", "had", "having",
        "do", "does", "did",
        "will", "would", "shall", "should",
        "can", "could", "may", "might", "must",
        # Subordinating conjunctions — introduce a subordinate clause
        "if", "when", "whenever", "while", "until", "unless",
        "although", "though", "because", "since", "whether",
        # Relative pronouns / interrogatives (without "where" — can end a clause)
        "that", "which", "who", "whom", "whose", "why", "how",
        # Coordinating conjunctions — "X and Y" is complete, "X and" is not
        "and", "or", "but", "nor", "yet",
        # Distributive determiners — awaiting a noun ("on every", "for each")
        "every", "each",
        # Possessive adjectives — awaiting a noun
        "its", "their", "his", "her", "my", "your", "our",
    }
)

# Characters that can legitimately end a description.
SENTENCE_ENDERS: frozenset[str] = frozenset(".!?)]}\"'`")
# Characters that are a strong truncation signal when they're the last glyph.
MID_SENTENCE_PUNCTUATION: frozenset[str] = frozenset(",;:-")


@dataclass(frozen=True)
class Finding:
    file: Path
    line: int
    reason: str
    description: str

    def format(self, max_desc: int = 120) -> str:
        snippet = self.description
        if len(snippet) > max_desc:
            snippet = snippet[: max_desc - 3] + "..."
        return f"{self.file}:{self.line}: [{self.reason}] {snippet}"


def iter_manifests(paths: list[Path]) -> list[Path]:
    """Expand the caller's path list into concrete manifest files."""
    out: list[Path] = []
    for p in paths:
        if p.is_file() and p.suffix in (".yaml", ".yml"):
            out.append(p)
            continue
        if p.is_dir():
            for name in ("manifest.yaml", "manifest.yml"):
                nested = p / name
                if nested.is_file():
                    out.append(nested)
                    break
            else:
                # No direct manifest — recurse for sources/*/manifest.y{a,}ml
                out.extend(sorted(p.glob("*/manifest.yaml")))
                out.extend(sorted(p.glob("*/manifest.yml")))
    # Dedup while preserving order.
    seen: set[Path] = set()
    unique: list[Path] = []
    for p in out:
        rp = p.resolve()
        if rp not in seen:
            seen.add(rp)
            unique.append(p)
    return unique


_DESCRIPTION_KEY_RE = re.compile(r"^(?P<indent>\s*)description:\s*(?P<value>.*)$")


def extract_descriptions(
    path: Path,
) -> list[tuple[int, str]]:
    """Walk a manifest file and yield (1-based line number, resolved description).

    Supports the four YAML scalar forms used in the bundled manifests:
      - plain scalars (possibly folded across indented continuation lines)
      - single-quoted scalars
      - double-quoted scalars
      - block scalars (`>-`, `>`, `|-`, `|`)
    """
    lines = path.read_text().splitlines()
    results: list[tuple[int, str]] = []
    i = 0
    while i < len(lines):
        m = _DESCRIPTION_KEY_RE.match(lines[i])
        if not m:
            i += 1
            continue
        key_indent = len(m.group("indent"))
        value = m.group("value").rstrip()
        start_line = i + 1  # 1-based

        if not value:
            # Block-scalar with empty header or empty plain scalar.
            i += 1
            text, i = _consume_block_scalar(lines, i, key_indent, chomp=None)
            if text is not None:
                results.append((start_line, text))
            continue

        if value.startswith(("|", ">")):
            indicator = value[0]
            chomp = value[1] if len(value) > 1 and value[1] in ("-", "+") else None
            i += 1
            text, i = _consume_block_scalar(lines, i, key_indent, chomp=chomp, fold=(indicator == ">"))
            if text is not None:
                results.append((start_line, text))
            continue

        if value.startswith("'"):
            text, i = _consume_single_quoted(lines, i, value)
            results.append((start_line, text))
            continue

        if value.startswith('"'):
            text, i = _consume_double_quoted(lines, i, value)
            results.append((start_line, text))
            continue

        text, i = _consume_plain_scalar(lines, i, value, key_indent)
        results.append((start_line, text))
    return results


def _consume_plain_scalar(
    lines: list[str], start: int, first_value: str, key_indent: int
) -> tuple[str, int]:
    """A YAML plain scalar may wrap onto indented continuation lines. Lines that
    are indented more than the mapping key are folded together as a single
    space-separated string."""
    pieces = [first_value.strip()]
    i = start + 1
    while i < len(lines):
        cont = lines[i]
        stripped = cont.strip()
        if not stripped:
            break
        cont_indent = len(cont) - len(cont.lstrip(" "))
        if cont_indent <= key_indent:
            break
        pieces.append(stripped)
        i += 1
    return " ".join(pieces), i


def _consume_single_quoted(
    lines: list[str], start: int, first_value: str
) -> tuple[str, int]:
    """Consume a possibly multi-line single-quoted scalar. Inside single quotes
    `''` escapes a literal single quote; everything else is literal."""
    buf = first_value[1:]  # strip opening quote
    i = start
    while True:
        # Scan buf for a closing quote that is not part of a doubled pair.
        j = 0
        closed = -1
        while j < len(buf):
            if buf[j] == "'":
                if j + 1 < len(buf) and buf[j + 1] == "'":
                    j += 2
                    continue
                closed = j
                break
            j += 1
        if closed >= 0:
            text = buf[:closed].replace("''", "'")
            return text, i + 1
        # No closing quote on this line — append next continuation line.
        i += 1
        if i >= len(lines):
            return buf.replace("''", "'"), i
        buf += " " + lines[i].strip()


def _consume_double_quoted(
    lines: list[str], start: int, first_value: str
) -> tuple[str, int]:
    """Consume a possibly multi-line double-quoted scalar. Supports `\\"` escape."""
    buf = first_value[1:]  # strip opening quote
    i = start
    while True:
        j = 0
        closed = -1
        while j < len(buf):
            ch = buf[j]
            if ch == "\\" and j + 1 < len(buf):
                j += 2
                continue
            if ch == '"':
                closed = j
                break
            j += 1
        if closed >= 0:
            text = _unescape_double_quoted(buf[:closed])
            return text, i + 1
        i += 1
        if i >= len(lines):
            return _unescape_double_quoted(buf), i
        buf += " " + lines[i].strip()


def _unescape_double_quoted(s: str) -> str:
    out: list[str] = []
    it = iter(range(len(s)))
    i = 0
    while i < len(s):
        if s[i] == "\\" and i + 1 < len(s):
            nxt = s[i + 1]
            if nxt == "n":
                out.append("\n")
            elif nxt == "t":
                out.append("\t")
            elif nxt == "r":
                out.append("\r")
            elif nxt == "\\":
                out.append("\\")
            elif nxt == '"':
                out.append('"')
            elif nxt == "0":
                out.append("\0")
            else:
                out.append(nxt)
            i += 2
            continue
        out.append(s[i])
        i += 1
    return "".join(out)


def _consume_block_scalar(
    lines: list[str],
    start: int,
    key_indent: int,
    chomp: str | None = None,
    fold: bool = False,
) -> tuple[str | None, int]:
    """Consume a block scalar (`|` literal or `>` folded). Terminates on the
    first line indented at or below `key_indent + 1`."""
    i = start
    content_lines: list[str] = []
    block_indent: int | None = None
    while i < len(lines):
        raw = lines[i]
        if not raw.strip():
            content_lines.append("")
            i += 1
            continue
        indent = len(raw) - len(raw.lstrip(" "))
        if indent <= key_indent:
            break
        if block_indent is None:
            block_indent = indent
        content_lines.append(raw[block_indent:])
        i += 1
    if not content_lines and block_indent is None:
        return None, i
    if fold:
        text = _fold_block(content_lines)
    else:
        text = "\n".join(content_lines)
    if chomp == "-":
        text = text.rstrip("\n")
    elif chomp is None:
        text = text.rstrip("\n") + ("\n" if text.endswith("\n") else "")
    return text, i


def _fold_block(content_lines: list[str]) -> str:
    """Apply folded-scalar semantics: single newlines collapse to spaces,
    blank lines become one newline."""
    if not content_lines:
        return ""
    out: list[str] = []
    for i, line in enumerate(content_lines):
        if i == 0:
            out.append(line)
            continue
        prev = content_lines[i - 1]
        if prev == "" or line == "":
            out.append("\n")
            out.append(line)
        else:
            out.append(" ")
            out.append(line)
    return "".join(out)


def classify(description: str) -> list[str]:
    """Return a list of truncation-signal reasons for a description. Empty list
    means no suspicion."""
    text = description.strip()
    if not text:
        return []
    reasons: list[str] = []

    # Rule: ends with a mid-sentence punctuation mark.
    if text[-1] in MID_SENTENCE_PUNCTUATION:
        reasons.append(f"ends-with-mid-punctuation({text[-1]!r})")

    # Rule: last token is a stopword that rarely ends a sentence.
    tail_word = _trailing_word(text)
    if tail_word and tail_word.lower() in SENTENCE_TAIL_STOPWORDS:
        reasons.append(f"ends-with-stopword({tail_word!r})")

    # Rule: odd number of backticks (unterminated inline code).
    if text.count("`") % 2 == 1:
        reasons.append("unbalanced-backticks")

    # Rule: unbalanced brackets/parens/braces. A genuinely truncated
    # description often leaves an opening delimiter unmatched.
    for open_ch, close_ch in (("(", ")"), ("[", "]"), ("{", "}")):
        if text.count(open_ch) > text.count(close_ch):
            reasons.append(f"unbalanced-brackets({open_ch}{close_ch})")

    # Rule: ends with an opening delimiter (cut mid-construct).
    # Backticks are intentionally excluded — the count check above handles
    # them — and a text ending with a matched closing backtick is fine.
    if text[-1] in "([{":
        reasons.append("ends-with-open-bracket")

    # Rule: the final clause (after the last sentence-terminating punct) is
    # long and unterminated. Targets the observed LLM-generation cap
    # (120-128 chars in the PagerDuty regression tracked by SOURCE-465).
    # Skipped for multi-line descriptions (markdown bullet lists, code
    # blocks) because dots inside identifiers (e.g. MIME types, versioned
    # package names) confuse last-clause detection in those structures.
    if "\n" not in text:
        last_clause = _last_clause(text)
        if (
            120 <= len(last_clause) <= 130
            and last_clause[-1] not in SENTENCE_ENDERS
            and not reasons
        ):
            reasons.append(f"suspicious-length({len(last_clause)})")

    return reasons


def _last_clause(text: str) -> str:
    """Return the final clause: the trailing substring after the last `.`, `!`
    or `?`. Used to avoid flagging multi-sentence descriptions whose last
    sentence is short but complete (or is itself a short phrase)."""
    for i in range(len(text) - 1, -1, -1):
        if text[i] in ".!?":
            return text[i + 1 :].strip()
    return text


_TRAILING_WORD_RE = re.compile(r"([A-Za-z][A-Za-z'_-]*)\s*[`'\")\]\}]*\s*$")


def _trailing_word(text: str) -> str | None:
    m = _TRAILING_WORD_RE.search(text)
    return m.group(1) if m else None


def scan(path: Path) -> list[Finding]:
    findings: list[Finding] = []
    for line, desc in extract_descriptions(path):
        reasons = classify(desc)
        if not reasons:
            continue
        findings.append(
            Finding(
                file=path,
                line=line,
                reason=",".join(reasons),
                description=desc.replace("\n", " "),
            )
        )
    return findings


_SELF_TESTS: list[tuple[str, bool, str]] = [
    # (description, should_flag, label)
    # Known truncations from SOURCE-465 and the PagerDuty OpenAPI-generated
    # manifest. Each must be flagged by at least one rule.
    (
        "A short-form, server-generated string that provides succinct, "
        "important information about an object suitable for primary",
        True, "pd-primary-120"
    ),
    (
        "The user role. Account must have the `read_only_users` ability to "
        "set a user as a `read_only_user` or a",
        True, "pd-role-ends-a"
    ),
    (
        "Whether or not the incident resolved automatically, either via an "
        "integration  or [auto-resolved in",
        True, "pd-unbalanced-bracket"
    ),
    (
        "The list of payment method types (e.g.",
        True, "stripe-e-dot-g-dot"  # unbalanced paren
    ),
    ("The list of pending_actions... can be escalate,", True, "ends-with-comma"),
    ("Filter syntax:", True, "ends-with-colon"),
    ("The attributes the user has are", True, "ends-with-aux-are"),
    ("The level of privacy this team should have", True, "ends-with-have"),
    # Natural descriptions — must NOT be flagged.
    (
        "The type of repositories in the organization that the secret is visible to",
        False, "ends-with-to-preposition"
    ),
    ("List dependencies an issue is blocked by", False, "ends-with-by"),
    ("Whether this alert route is enabled or not", False, "ends-with-not"),
    ("The registry resource this type is synced from, if any", False, "ends-with-any"),
    ("New alert state (alert annotations only)", False, "ends-with-only"),
    (
        "The visibility of newly created repositories for which the code "
        "security configuration will be applied to by default",
        False, "116-char-legit"
    ),
    ("Event action to filter on", False, "short-ends-with-on"),
]


def run_self_tests() -> int:
    failures = 0
    for text, should_flag, label in _SELF_TESTS:
        reasons = classify(text)
        flagged = bool(reasons)
        if flagged != should_flag:
            failures += 1
            verdict = "FLAGGED" if flagged else "NOT FLAGGED"
            expected = "should flag" if should_flag else "should NOT flag"
            print(f"  FAIL [{label}] {expected}, got {verdict} ({reasons})")
            print(f"    text: {text[:100]}...")
        else:
            print(f"  OK   [{label}]: {reasons or 'clean'}")
    if failures:
        print(f"\n{failures}/{len(_SELF_TESTS)} self-test(s) failed.")
        return 1
    print(f"\nAll {len(_SELF_TESTS)} self-tests passed.")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Detect likely-truncated descriptions in Coral source manifests."
    )
    parser.add_argument(
        "paths",
        nargs="*",
        type=Path,
        default=[Path("sources")],
        help="Manifest files or directories to scan (default: sources/)",
    )
    parser.add_argument(
        "--verbose", action="store_true", help="Print every description scanned"
    )
    parser.add_argument(
        "--self-test",
        action="store_true",
        help="Run built-in rule-coverage tests and exit",
    )
    args = parser.parse_args()

    if args.self_test:
        return run_self_tests()

    manifests = iter_manifests(args.paths)
    if not manifests:
        print("no manifests found", file=sys.stderr)
        return 2

    all_findings: list[Finding] = []
    for m in manifests:
        findings = scan(m)
        if args.verbose:
            print(f"{m}: {len(findings)} suspected truncations")
        all_findings.extend(findings)

    if not all_findings:
        print(f"OK — scanned {len(manifests)} manifest(s), no suspected truncations.")
        return 0

    print(f"Found {len(all_findings)} suspected truncation(s):\n")
    for f in all_findings:
        print(f.format())
    print()
    by_file: dict[Path, int] = {}
    for f in all_findings:
        by_file[f.file] = by_file.get(f.file, 0) + 1
    print("Summary:")
    for path, count in sorted(by_file.items(), key=lambda kv: (-kv[1], str(kv[0]))):
        print(f"  {path}: {count}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
