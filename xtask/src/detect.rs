//! Heuristic scanner for truncated column/table/source descriptions.
//!
//! A manifest's descriptions feed documentation, MCP surfaces, and the
//! `coral.columns` catalog. When an upstream generation step (e.g. `OpenAPI`
//! → YAML) applies a character cap, sentences get cut mid-phrase. This module
//! walks each `sources/*/manifest.y{a,}ml` and flags descriptions that exhibit
//! deterministic truncation signals.
//!
//! Signals (ordered from least to most likely false-positive):
//!   - `ends-with-mid-punctuation`: text ends with `,`, `;`, `:`, or `-`.
//!   - `unbalanced-brackets`: more `(` than `)` (or `[`/`]`, `{`/`}`).
//!   - `unbalanced-backticks`: odd count of backticks.
//!   - `ends-with-open-bracket`: final char is `(`, `[`, or `{`.
//!   - `ends-with-stopword`: final word is an article, aux verb, conjunction,
//!     relative pronoun, or distributive determiner — categories that require
//!     a grammatical complement.
//!   - `suspicious-length`: single-line description with a 120-130 char
//!     trailing clause that doesn't end in sentence-terminating punctuation,
//!     matching the LLM-generation caps observed in SOURCE-465.

use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

/// Tokens that rarely, if ever, terminate a complete English description.
/// If a description ends on one of these, it almost certainly got chopped.
///
/// Prepositions are intentionally excluded: English relative clauses often end
/// with a preposition ("the team this user belongs to"), so flagging them
/// yields mostly false positives. Similarly ambiguous tokens ("not", "any",
/// "all", "these") are omitted because they appear naturally in phrases like
/// "enabled or not", "if any", or "`created_by_me` or all".
#[rustfmt::skip]
const SENTENCE_TAIL_STOPWORDS: &[&str] = &[
    // Articles — almost always followed by a noun.
    "a", "an", "the",
    // Auxiliary verbs — almost always followed by a complement.
    "is", "are", "was", "were", "be", "been", "being", "am",
    "has", "have", "had", "having",
    "do", "does", "did",
    "will", "would", "shall", "should",
    "can", "could", "may", "might", "must",
    // Subordinating conjunctions — introduce a subordinate clause.
    "if", "when", "whenever", "while", "until", "unless",
    "although", "though", "because", "since", "whether",
    // Relative pronouns / interrogatives (without "where" — can end a clause).
    "that", "which", "who", "whom", "whose", "why", "how",
    // Coordinating conjunctions — "X and Y" is complete, "X and" is not.
    "and", "or", "but", "nor", "yet",
    // Distributive determiners — awaiting a noun ("on every", "for each").
    "every", "each",
    // Possessive adjectives — awaiting a noun.
    "its", "their", "his", "her", "my", "your", "our",
];

/// Characters that can legitimately end a description.
const SENTENCE_ENDERS: &[char] = &['.', '!', '?', ')', ']', '}', '"', '\'', '`'];

/// Characters that are a strong truncation signal when they're the last glyph.
const MID_SENTENCE_PUNCTUATION: &[char] = &[',', ';', ':', '-'];

/// A suspected-truncation hit recorded for later display.
#[derive(Debug, Clone)]
pub(crate) struct Finding {
    pub file: PathBuf,
    pub line: usize,
    pub reasons: Vec<String>,
    pub description: String,
}

impl Finding {
    fn display(&self, max_desc: usize) -> String {
        let chars: Vec<char> = self.description.chars().collect();
        let snippet = if chars.len() > max_desc {
            let truncated: String = chars.iter().take(max_desc.saturating_sub(3)).collect();
            format!("{truncated}...")
        } else {
            self.description.clone()
        };
        format!(
            "{}:{}: [{}] {snippet}",
            self.file.display(),
            self.line,
            self.reasons.join(","),
        )
    }
}

/// Expand the caller's path list into concrete manifest files, deduplicated.
pub(crate) fn iter_manifests(paths: &[PathBuf]) -> Vec<PathBuf> {
    let mut out: Vec<PathBuf> = Vec::new();
    for p in paths {
        if p.is_file() && matches!(p.extension().and_then(|s| s.to_str()), Some("yaml" | "yml")) {
            out.push(p.clone());
            continue;
        }
        if p.is_dir() {
            let direct = ["manifest.yaml", "manifest.yml"]
                .iter()
                .map(|name| p.join(name))
                .find(|path| path.is_file());
            if let Some(nested) = direct {
                out.push(nested);
                continue;
            }
            // No direct manifest — recurse for sources/*/manifest.y{a,}ml.
            if let Ok(entries) = fs::read_dir(p) {
                let mut children: Vec<PathBuf> = entries
                    .filter_map(std::result::Result::ok)
                    .map(|e| e.path())
                    .filter(|c| c.is_dir())
                    .collect();
                children.sort();
                for child in children {
                    for name in ["manifest.yaml", "manifest.yml"] {
                        let candidate = child.join(name);
                        if candidate.is_file() {
                            out.push(candidate);
                            break;
                        }
                    }
                }
            }
        }
    }
    let mut seen: std::collections::HashSet<PathBuf> = std::collections::HashSet::new();
    let mut unique: Vec<PathBuf> = Vec::new();
    for p in out {
        let resolved = p.canonicalize().unwrap_or_else(|_| p.clone());
        if seen.insert(resolved) {
            unique.push(p);
        }
    }
    unique
}

/// Walk a manifest file and yield (1-based line number, resolved description).
///
/// Supports the four YAML scalar forms used in the bundled manifests:
///   - plain scalars (possibly folded across indented continuation lines)
///   - single-quoted scalars
///   - double-quoted scalars
///   - block scalars (`>-`, `>`, `|-`, `|`)
pub(crate) fn extract_descriptions(content: &str) -> Vec<(usize, String)> {
    let lines: Vec<&str> = content.lines().collect();
    let mut results: Vec<(usize, String)> = Vec::new();
    let mut i = 0;
    while i < lines.len() {
        let line = lines
            .get(i)
            .expect("line index is bounded by loop condition");
        let Some((key_indent, value)) = parse_description_header(line) else {
            i += 1;
            continue;
        };
        let start_line = i + 1; // 1-based
        let value = value.trim_end().to_string();

        if value.is_empty() {
            // Block-scalar with empty header or empty plain scalar.
            i += 1;
            let (text, new_i) = consume_block_scalar(&lines, i, key_indent, None, false);
            i = new_i;
            if let Some(t) = text {
                results.push((start_line, t));
            }
            continue;
        }

        let first = value
            .chars()
            .next()
            .expect("empty values are handled before scalar dispatch");
        if first == '|' || first == '>' {
            let chomp = value.chars().nth(1).filter(|c| *c == '-' || *c == '+');
            let fold = first == '>';
            i += 1;
            let (text, new_i) = consume_block_scalar(&lines, i, key_indent, chomp, fold);
            i = new_i;
            if let Some(t) = text {
                results.push((start_line, t));
            }
            continue;
        }

        if first == '\'' {
            let (text, new_i) = consume_single_quoted(&lines, i, &value);
            i = new_i;
            results.push((start_line, text));
            continue;
        }

        if first == '"' {
            let (text, new_i) = consume_double_quoted(&lines, i, &value);
            i = new_i;
            results.push((start_line, text));
            continue;
        }

        let (text, new_i) = consume_plain_scalar(&lines, i, &value, key_indent);
        i = new_i;
        results.push((start_line, text));
    }
    results
}

/// Match lines of the form `<indent>description:<optional space><value>`.
/// Returns `(indent_width, trimmed_value)` on match.
fn parse_description_header(line: &str) -> Option<(usize, String)> {
    let indent = line.chars().take_while(|c| *c == ' ').count();
    let rest = line.get(indent..)?;
    let value_part = rest.strip_prefix("description:")?;
    // YAML allows zero or more spaces between the colon and the value. A line
    // like `description:foo` would be a parser error in strict YAML but our
    // input comes from emitters that always produce `description: foo` or
    // `description:` — we handle both defensively.
    let value = value_part.trim_start_matches([' ', '\t']);
    Some((indent, value.to_string()))
}

/// A YAML plain scalar may wrap onto indented continuation lines. Lines that
/// are indented more than the mapping key are folded together as a single
/// space-separated string.
fn consume_plain_scalar(
    lines: &[&str],
    start: usize,
    first_value: &str,
    key_indent: usize,
) -> (String, usize) {
    let mut pieces: Vec<String> = vec![first_value.trim().to_string()];
    let mut i = start + 1;
    while i < lines.len() {
        let cont = lines
            .get(i)
            .expect("line index is bounded by loop condition");
        let stripped = cont.trim();
        if stripped.is_empty() {
            break;
        }
        let cont_indent = cont.chars().take_while(|c| *c == ' ').count();
        if cont_indent <= key_indent {
            break;
        }
        pieces.push(stripped.to_string());
        i += 1;
    }
    (pieces.join(" "), i)
}

/// Consume a possibly multi-line single-quoted scalar. Inside single quotes
/// `''` escapes a literal single quote; everything else is literal.
fn consume_single_quoted(lines: &[&str], start: usize, first_value: &str) -> (String, usize) {
    // Strip the opening quote. `first_value` is guaranteed to start with `'`.
    let mut buf = first_value
        .strip_prefix('\'')
        .expect("single-quoted scalar starts with quote")
        .to_string();
    let mut i = start;
    loop {
        if let Some(pos) = find_unescaped_single_quote(&buf) {
            let text = buf
                .get(..pos)
                .expect("single-quote scanner returns a char boundary")
                .replace("''", "'");
            return (text, i + 1);
        }
        i += 1;
        if i >= lines.len() {
            return (buf.replace("''", "'"), i);
        }
        buf.push(' ');
        buf.push_str(
            lines
                .get(i)
                .expect("line index is bounded by prior length check")
                .trim(),
        );
    }
}

fn find_unescaped_single_quote(buf: &str) -> Option<usize> {
    let bytes = buf.as_bytes();
    let mut j = 0;
    while j < bytes.len() {
        if bytes.get(j).copied() == Some(b'\'') {
            if bytes.get(j + 1).copied() == Some(b'\'') {
                j += 2;
                continue;
            }
            return Some(j);
        }
        j += 1;
    }
    None
}

/// Consume a possibly multi-line double-quoted scalar. Supports `\"` escape.
fn consume_double_quoted(lines: &[&str], start: usize, first_value: &str) -> (String, usize) {
    let mut buf = first_value
        .strip_prefix('"')
        .expect("double-quoted scalar starts with quote")
        .to_string();
    let mut i = start;
    loop {
        if let Some(pos) = find_unescaped_double_quote(&buf) {
            let text = unescape_double_quoted(
                buf.get(..pos)
                    .expect("double-quote scanner returns a char boundary"),
            );
            return (text, i + 1);
        }
        i += 1;
        if i >= lines.len() {
            return (unescape_double_quoted(&buf), i);
        }
        buf.push(' ');
        buf.push_str(
            lines
                .get(i)
                .expect("line index is bounded by prior length check")
                .trim(),
        );
    }
}

fn find_unescaped_double_quote(buf: &str) -> Option<usize> {
    let bytes = buf.as_bytes();
    let mut j = 0;
    while j < bytes.len() {
        match bytes.get(j).copied() {
            Some(b'\\') if j + 1 < bytes.len() => j += 2,
            Some(b'"') => return Some(j),
            _ => j += 1,
        }
    }
    None
}

fn unescape_double_quoted(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.next() {
                Some('n') => out.push('\n'),
                Some('t') => out.push('\t'),
                Some('r') => out.push('\r'),
                Some('"') => out.push('"'),
                Some('0') => out.push('\0'),
                // Matches Python: `\<x>` preserves `<x>` for unknown escapes
                // and a trailing bare `\` is passed through verbatim.
                Some('\\') | None => out.push('\\'),
                Some(other) => out.push(other),
            }
        } else {
            out.push(c);
        }
    }
    out
}

/// Consume a block scalar (`|` literal or `>` folded). Terminates on the
/// first line indented at or below `key_indent`.
fn consume_block_scalar(
    lines: &[&str],
    start: usize,
    key_indent: usize,
    chomp: Option<char>,
    fold: bool,
) -> (Option<String>, usize) {
    let mut i = start;
    let mut content_lines: Vec<String> = Vec::new();
    let mut block_indent: Option<usize> = None;
    while i < lines.len() {
        let raw = lines
            .get(i)
            .expect("line index is bounded by loop condition");
        if raw.trim().is_empty() {
            content_lines.push(String::new());
            i += 1;
            continue;
        }
        let indent = raw.chars().take_while(|c| *c == ' ').count();
        if indent <= key_indent {
            break;
        }
        let bi = *block_indent.get_or_insert(indent);
        let stripped = raw.get(bi..).unwrap_or("");
        content_lines.push(stripped.to_string());
        i += 1;
    }
    if content_lines.is_empty() && block_indent.is_none() {
        return (None, i);
    }
    let text = if fold {
        fold_block(&content_lines)
    } else {
        content_lines.join("\n")
    };
    let text = match chomp {
        Some('-') => text.trim_end_matches('\n').to_string(),
        Some('+') => text,
        _ => {
            // Default chomp: keep at most one trailing newline.
            let ended = text.ends_with('\n');
            let stripped = text.trim_end_matches('\n').to_string();
            if ended { stripped + "\n" } else { stripped }
        }
    };
    (Some(text), i)
}

/// Apply folded-scalar semantics: single newlines collapse to spaces,
/// blank lines become one newline.
fn fold_block(content_lines: &[String]) -> String {
    if content_lines.is_empty() {
        return String::new();
    }
    let mut out = String::new();
    for (i, line) in content_lines.iter().enumerate() {
        if i == 0 {
            out.push_str(line);
            continue;
        }
        let prev = content_lines
            .get(i - 1)
            .expect("previous content line exists after first iteration");
        if prev.is_empty() || line.is_empty() {
            out.push('\n');
        } else {
            out.push(' ');
        }
        out.push_str(line);
    }
    out
}

/// Return the truncation-signal reasons for a description. Empty vec means
/// no suspicion.
pub(crate) fn classify(description: &str) -> Vec<String> {
    let text = description.trim();
    if text.is_empty() {
        return Vec::new();
    }
    let mut reasons: Vec<String> = Vec::new();

    let last_char = text.chars().last().expect("non-empty checked above");

    if MID_SENTENCE_PUNCTUATION.contains(&last_char) {
        reasons.push(format!("ends-with-mid-punctuation({last_char:?})"));
    }

    if let Some(word) = trailing_word(text) {
        let lower = word.to_ascii_lowercase();
        if SENTENCE_TAIL_STOPWORDS.contains(&lower.as_str()) {
            reasons.push(format!("ends-with-stopword('{word}')"));
        }
    }

    if text.chars().filter(|c| *c == '`').count() % 2 == 1 {
        reasons.push("unbalanced-backticks".to_string());
    }

    for (open_ch, close_ch) in [('(', ')'), ('[', ']'), ('{', '}')] {
        let opens = text.chars().filter(|c| *c == open_ch).count();
        let closes = text.chars().filter(|c| *c == close_ch).count();
        if opens > closes {
            reasons.push(format!("unbalanced-brackets({open_ch}{close_ch})"));
        }
    }

    if matches!(last_char, '(' | '[' | '{') {
        reasons.push("ends-with-open-bracket".to_string());
    }

    // Skipped for multi-line descriptions (markdown bullet lists, code
    // blocks) because dots inside identifiers confuse last-clause detection.
    if !text.contains('\n') && reasons.is_empty() {
        let clause = last_clause(text);
        let clause_len = clause.chars().count();
        if (120..=130).contains(&clause_len) {
            let clause_last = clause.chars().last().expect("non-empty clause");
            if !SENTENCE_ENDERS.contains(&clause_last) {
                reasons.push(format!("suspicious-length({clause_len})"));
            }
        }
    }

    reasons
}

/// Return the final clause: the trailing substring after the last `.`, `!`,
/// or `?`. Used to avoid flagging multi-sentence descriptions whose last
/// sentence is short but complete.
fn last_clause(text: &str) -> &str {
    for (i, c) in text.char_indices().rev() {
        if matches!(c, '.' | '!' | '?') {
            return text
                .get(i + c.len_utf8()..)
                .expect("char_indices yields valid UTF-8 boundaries")
                .trim();
        }
    }
    text
}

/// Return the trailing word, stripping any closing quotes/brackets and
/// whitespace. Mirrors the Python regex `([A-Za-z][A-Za-z'_-]*)\s*[...]*\s*$`:
/// the word must begin with an ASCII letter, but may contain apostrophes,
/// underscores, or hyphens.
fn trailing_word(text: &str) -> Option<&str> {
    let trimmed = text.trim_end_matches(|c: char| {
        c.is_whitespace() || matches!(c, '`' | '\'' | '"' | ')' | ']' | '}')
    });
    let bytes = trimmed.as_bytes();
    if bytes.is_empty() {
        return None;
    }
    let is_word_byte = |b: u8| b.is_ascii_alphabetic() || matches!(b, b'\'' | b'_' | b'-');
    let mut start = bytes.len();
    while start > 0 && bytes.get(start - 1).is_some_and(|b| is_word_byte(*b)) {
        start -= 1;
    }
    // Regex requires the first char to be [A-Za-z]. Advance past any leading
    // non-alpha word chars (e.g. leading apostrophe).
    while start < bytes.len()
        && bytes
            .get(start)
            .is_some_and(|byte| !byte.is_ascii_alphabetic())
    {
        start += 1;
    }
    if start >= bytes.len() {
        None
    } else {
        trimmed.get(start..)
    }
}

/// Scan a single manifest file, returning one `Finding` per suspected
/// truncation.
pub(crate) fn scan(path: &Path) -> Result<Vec<Finding>> {
    let content =
        fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let mut findings: Vec<Finding> = Vec::new();
    for (line, desc) in extract_descriptions(&content) {
        let reasons = classify(&desc);
        if reasons.is_empty() {
            continue;
        }
        findings.push(Finding {
            file: path.to_path_buf(),
            line,
            reasons,
            description: desc.replace('\n', " "),
        });
    }
    Ok(findings)
}

/// CLI entry point: scan the provided paths and write a report to stdout.
/// Returns `true` if no findings, `false` if any were reported.
pub(crate) fn run(paths: &[PathBuf], verbose: bool) -> Result<bool> {
    let manifests = iter_manifests(paths);
    if manifests.is_empty() {
        anyhow::bail!("no manifests found");
    }
    let mut all_findings: Vec<Finding> = Vec::new();
    for m in &manifests {
        let findings = scan(m)?;
        if verbose {
            println!("{}: {} suspected truncations", m.display(), findings.len());
        }
        all_findings.extend(findings);
    }
    if all_findings.is_empty() {
        println!(
            "OK — scanned {} manifest(s), no suspected truncations.",
            manifests.len()
        );
        return Ok(true);
    }
    println!("Found {} suspected truncation(s):\n", all_findings.len());
    for f in &all_findings {
        println!("{}", f.display(120));
    }
    println!();
    let mut by_file: std::collections::BTreeMap<PathBuf, usize> = std::collections::BTreeMap::new();
    for f in &all_findings {
        *by_file.entry(f.file.clone()).or_insert(0) += 1;
    }
    let mut rows: Vec<(PathBuf, usize)> = by_file.into_iter().collect();
    rows.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
    println!("Summary:");
    for (path, count) in rows {
        println!("  {}: {count}", path.display());
    }
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Known-good and known-bad descriptions that the rule set must separate
    /// correctly. Drawn from SOURCE-465 incidents and neighbouring manifests.
    const CASES: &[(&str, bool, &str)] = &[
        (
            "A short-form, server-generated string that provides succinct, important information about an object suitable for primary",
            true,
            "pd-primary-120",
        ),
        (
            "The user role. Account must have the `read_only_users` ability to set a user as a `read_only_user` or a",
            true,
            "pd-role-ends-a",
        ),
        (
            "Whether or not the incident resolved automatically, either via an integration  or [auto-resolved in",
            true,
            "pd-unbalanced-bracket",
        ),
        (
            "The list of payment method types (e.g.",
            true,
            "stripe-e-dot-g-dot",
        ),
        (
            "The list of pending_actions... can be escalate,",
            true,
            "ends-with-comma",
        ),
        ("Filter syntax:", true, "ends-with-colon"),
        ("The attributes the user has are", true, "ends-with-aux-are"),
        (
            "The level of privacy this team should have",
            true,
            "ends-with-have",
        ),
        (
            "The type of repositories in the organization that the secret is visible to",
            false,
            "ends-with-to-preposition",
        ),
        (
            "List dependencies an issue is blocked by",
            false,
            "ends-with-by",
        ),
        (
            "Whether this alert route is enabled or not",
            false,
            "ends-with-not",
        ),
        (
            "The registry resource this type is synced from, if any",
            false,
            "ends-with-any",
        ),
        (
            "New alert state (alert annotations only)",
            false,
            "ends-with-only",
        ),
        (
            "The visibility of newly created repositories for which the code security configuration will be applied to by default",
            false,
            "116-char-legit",
        ),
        ("Event action to filter on", false, "short-ends-with-on"),
    ];

    #[test]
    fn classify_rules_cover_known_truncations() {
        for (text, should_flag, label) in CASES {
            let reasons = classify(text);
            let flagged = !reasons.is_empty();
            assert_eq!(
                flagged, *should_flag,
                "[{label}] expected flag={should_flag}, got {flagged} ({reasons:?})",
            );
        }
    }

    #[test]
    fn extract_plain_scalar() {
        let yaml = "
tables:
  - name: foo
    description: A plain description
";
        let descs = extract_descriptions(yaml);
        assert_eq!(descs, vec![(4, "A plain description".to_string())]);
    }

    #[test]
    fn extract_multi_line_plain_scalar() {
        let yaml = "
tables:
  - name: foo
    description: A plain description
      that wraps onto a second line
";
        let descs = extract_descriptions(yaml);
        assert_eq!(
            descs,
            vec![(
                4,
                "A plain description that wraps onto a second line".to_string()
            )]
        );
    }

    #[test]
    fn extract_single_quoted() {
        let yaml = "
description: 'single ''quoted'' value'
";
        let descs = extract_descriptions(yaml);
        assert_eq!(descs, vec![(2, "single 'quoted' value".to_string())]);
    }

    #[test]
    fn extract_double_quoted() {
        let yaml = r#"
description: "quoted \"value\""
"#;
        let descs = extract_descriptions(yaml);
        assert_eq!(descs, vec![(2, "quoted \"value\"".to_string())]);
    }

    #[test]
    fn extract_literal_block_scalar() {
        let yaml = "
tables:
  - name: foo
    description: |
      First line.
      Second line.
";
        let descs = extract_descriptions(yaml);
        assert_eq!(descs, vec![(4, "First line.\nSecond line.".to_string())]);
    }

    #[test]
    fn extract_folded_block_scalar() {
        let yaml = "
tables:
  - name: foo
    description: >-
      Folded
      lines
      join.
";
        let descs = extract_descriptions(yaml);
        assert_eq!(descs, vec![(4, "Folded lines join.".to_string())]);
    }

    #[test]
    fn trailing_word_handles_punctuation() {
        assert_eq!(trailing_word("the big cat"), Some("cat"));
        assert_eq!(trailing_word("the big cat)"), Some("cat"));
        assert_eq!(trailing_word("the big `cat`"), Some("cat"));
        assert_eq!(trailing_word("it's"), Some("it's"));
        assert_eq!(trailing_word("  "), None);
    }

    #[test]
    fn suspicious_length_only_fires_without_other_signals() {
        // 125-char description ending mid-phrase with no other signal.
        let text = "A short-form, server-generated string that provides succinct, important information about an object suitable for primary";
        let reasons = classify(text);
        assert!(reasons.iter().any(|r| r.starts_with("suspicious-length")));
    }
}
