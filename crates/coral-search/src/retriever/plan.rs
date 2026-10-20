//! From the engine's normalized terms to what a ranked statement binds.
//!
//! Identifier variants belong to the retrievers, not the engine, so a
//! database retriever can tell typed words from synthesized ones. The rules
//! and constants follow the reference implementation recorded in the research
//! record "Postgres retrieval algorithm and its `SQLite` mirror".

use std::collections::BTreeSet;

use crate::indexer::compact_identifier_variant;

/// Terms shorter than this cannot meet a trigram index; both backends drop them.
pub(crate) const MIN_TERM_CHARS: usize = 3;
/// A near-spelling joins a query word to a catalog word only when both have
/// this many letters. Shorter words sit one edit from too many others:
/// `maya` from `may`, `chen` from `when` and `then`.
const NEAR_SPELLING_MIN_LETTERS: usize = 5;
/// A word in more than half the corpus scores this instead of a negative IDF.
const IDF_FLOOR: f64 = 1e-6;
/// What ends a word in the phrase test: any character but an ASCII letter or
/// digit. `_` ends one too, so `pulls.get` meets the escaped name `pulls._get`.
/// A letter outside ASCII also ends a word. Unicode classes would differ
/// between the backends: Postgres reads them from the database's locale.
const PHRASE_SEPARATOR: &str = "[^a-zA-Z0-9]";
/// Words the whole-field test skips, so `create pull request` is the whole
/// title `Create a pull request`.
const FILLER_WORDS: [&str; 3] = ["a", "an", "the"];

/// One term and where it came from.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Term {
    text: String,
    /// Synthesized by removing every non-alphanumeric character from a typed
    /// term (`deploy_url` gives `deployurl`). Rows index the same variant of
    /// their names, so its only designed match is a whole lexeme.
    compact_variant: bool,
}

/// The query as both backends' statements need it.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct QueryPlan {
    /// Every term of at least three characters, compact variants included,
    /// and every word: the `SQLite` `MATCH` phrases.
    pub(super) match_terms: Vec<String>,
    /// The patterns of the longest term, the whole query: the phrase-first
    /// order key.
    pub(super) phrase: PhrasePatterns,
    /// Runs of alphanumerics and `_` of at least three characters, typed or
    /// derived.
    pub(crate) words: Vec<String>,
    /// The words that only a compact variant produced.
    pub(super) variant_words: Vec<String>,
    /// Terms that yield no word, kept as substring patterns.
    pub(super) wordless: Vec<String>,
}

impl QueryPlan {
    /// Builds the plan, or `None` when no term has three characters or the
    /// longest term, the whole query, holds no letter or digit: the ranking
    /// is then empty, never a rejection.
    pub(crate) fn build(terms: &[String]) -> Option<Self> {
        let terms = with_compact_variants(terms)
            .into_iter()
            .filter(|term| term.text.chars().count() >= MIN_TERM_CHARS)
            .collect::<Vec<_>>();
        let phrase = PhrasePatterns::build(
            &terms
                .iter()
                .max_by_key(|term| term.text.chars().count())?
                .text,
        )?;
        let mut given_words = BTreeSet::new();
        let mut variant_words = BTreeSet::new();
        let mut wordless = BTreeSet::new();
        for term in &terms {
            let term_words = lexeme_words(&term.text)
                .into_iter()
                .filter(|word| word.chars().count() >= MIN_TERM_CHARS)
                .collect::<Vec<_>>();
            if term_words.is_empty() {
                wordless.insert(term.text.clone());
            } else if term.compact_variant {
                variant_words.extend(term_words);
            } else {
                given_words.extend(term_words);
            }
        }
        let words = given_words
            .union(&variant_words)
            .cloned()
            .collect::<Vec<_>>();
        // The words join the terms on SQLite, as they select candidates on
        // Postgres: `weather.getforecast` keeps `.` and is one term, but no
        // column holds it whole, since the name column stops below the
        // Provider namespace.
        let match_terms = terms
            .iter()
            .map(|term| term.text.clone())
            .chain(words.iter().cloned())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        Some(Self {
            match_terms,
            phrase,
            words,
            variant_words: variant_words.difference(&given_words).cloned().collect(),
            wordless: wordless.into_iter().collect(),
        })
    }

    /// The typed words that no row holds as an exact word and that are long
    /// enough for a near-spelling: the words whose catalog neighbours the
    /// Postgres statement reads.
    pub(super) fn near_spelling_words(&self, words: &[WordStatistics]) -> Vec<String> {
        words
            .iter()
            .filter(|stat| self.near_spelling_applies(stat))
            .map(|stat| stat.word.clone())
            .collect()
    }

    fn near_spelling_applies(&self, stat: &WordStatistics) -> bool {
        stat.ndoc == 0
            && !self.variant_words.contains(&stat.word)
            && letters(&stat.word) >= NEAR_SPELLING_MIN_LETTERS
    }

    /// The words the Postgres statement selects candidates by and scores,
    /// each with its spellings and their IDF from the Workspace's statistics.
    ///
    /// A compact variant the corpus does not hold as a lexeme leaves: it has
    /// nothing to meet. A typed word the corpus does not hold stays, since
    /// its substring pattern can still meet a mid-word match such as
    /// `enchmark` in `benchmark_runs`. It also gains as spellings the catalog
    /// words one edit away, each with its own IDF, so `colour` scores where
    /// `color` would. The reference implementation also drops words above a
    /// 5% document frequency from candidate selection, as a latency lever;
    /// that cap is left out until profiles ask for it.
    pub(super) fn scoring(&self, statistics: &Statistics) -> ScoringPlan {
        let stats = statistics
            .words
            .iter()
            .filter(|stat| stat.ndoc > 0 || !self.variant_words.contains(&stat.word))
            .collect::<Vec<_>>();
        let total = statistics.total_docs.max(0);
        let spelling = |stat: &WordStatistics| Spelling {
            word: stat.word.clone(),
            idf: inverse_document_frequency(total, stat.ndoc),
        };
        ScoringPlan {
            words: stats
                .iter()
                .map(|stat| {
                    let applies = self.near_spelling_applies(stat);
                    let near = statistics
                        .catalog_words
                        .iter()
                        .filter(|catalog| {
                            applies
                                && letters(&catalog.word) >= NEAR_SPELLING_MIN_LETTERS
                                && one_edit_apart(&stat.word, &catalog.word)
                        })
                        .map(spelling);
                    ScoredWord {
                        word: stat.word.clone(),
                        spellings: std::iter::once(spelling(stat)).chain(near).collect(),
                    }
                })
                .collect(),
            avgdl: if statistics.avgdl > 0.0 {
                statistics.avgdl
            } else {
                1.0
            },
        }
    }
}

/// The phrase test's two patterns over one field, in the regular-expression
/// syntax that Postgres's `~` and `SQLite`'s `REGEXP` (the `regex` crate)
/// read alike: no flags, no escapes, no lookaround. Letters match either case
/// through a class per letter. The test ignores separators, so `pulls.create`
/// and `pulls create` are the same phrase.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PhrasePatterns {
    /// The field is the phrase, apart from separators and filler words. A
    /// phrase of filler words alone keeps them all.
    pub(super) whole: String,
    /// The field holds the phrase's words in order, each a whole word, apart
    /// from filler words, which it may also hold between them. A capital after
    /// a lowercase letter or a digit also starts a word and ends the last, so
    /// `issues` meets `openIssues`, `get review` meets `getReview`, and
    /// `pulls.get` meets `pulls.getReview`, but `request` meets neither
    /// `requests` nor `REQUESTS`.
    pub(super) contained: String,
}

impl PhrasePatterns {
    fn build(phrase: &str) -> Option<Self> {
        let words = phrase
            .split(|character: char| !character.is_alphanumeric())
            .filter(|word| !word.is_empty())
            .collect::<Vec<_>>();
        if words.is_empty() {
            return None;
        }
        let separator = PHRASE_SEPARATOR;
        let content = words
            .iter()
            .filter(|word| !FILLER_WORDS.contains(word))
            .collect::<Vec<_>>();
        let content = if content.is_empty() {
            words.iter().collect()
        } else {
            content
        };
        let filler = format!(
            "(?:{})",
            FILLER_WORDS
                .iter()
                .map(|word| either_case(word))
                .collect::<Vec<_>>()
                .join("|")
        );
        let whole = format!(
            "^{separator}*(?:{filler}{separator}+)*{}{separator}*$",
            content
                .iter()
                .map(|word| either_case(word))
                .collect::<Vec<_>>()
                .join(&format!("(?:{separator}+{filler})*{separator}+")),
        );
        let contained = contained_pattern(&content, &filler);
        Some(Self { whole, contained })
    }
}

/// The contained pattern: the content words in order, each a whole word.
/// The first starts the field or follows a separator; a later one follows
/// separators and any filler words. A camelCase edge also separates words:
/// a capital after a lowercase letter or a digit starts a word, or ends the
/// last. The patterns have no lookaround, so the edge after a word matches
/// that word's last character, to see that it is lowercase or a digit. A
/// word of one character ends no camelCase edge.
fn contained_pattern(content: &[&&str], filler: &str) -> String {
    let separator = PHRASE_SEPARATOR;
    let mut pattern = String::new();
    // The previous word's last character, which the next edge matches.
    let mut pending: Option<char> = None;
    for (position, word) in content.iter().enumerate() {
        let characters = word.chars().collect::<Vec<_>>();
        let Some((&first, rest)) = characters.split_first() else {
            continue;
        };
        let opening = if position == 0 {
            match capital(first) {
                Some(capital) => format!(
                    "(?:(?:^|{separator}){}|[a-z0-9]{capital})",
                    either_case_char(first)
                ),
                None => format!("(?:^|{separator}){}", either_case_char(first)),
            }
        } else {
            let after_separator = format!(
                "(?:{separator}+{filler})*{separator}+{}",
                either_case_char(first)
            );
            match pending.take() {
                Some(last) => match lowercase_or_digit(last).zip(capital(first)) {
                    Some((lower, capital)) => format!(
                        "(?:{}{after_separator}|{lower}{capital})",
                        either_case_char(last)
                    ),
                    None => format!("{}{after_separator}", either_case_char(last)),
                },
                None => after_separator,
            }
        };
        pattern.push_str(&opening);
        if let Some((&last, middle)) = rest.split_last() {
            pattern.push_str(&either_case(&middle.iter().collect::<String>()));
            pending = Some(last);
        }
    }
    let ending = match pending {
        Some(last) => match lowercase_or_digit(last) {
            Some(lower) => format!(
                "(?:{}(?:$|{separator})|{lower}[A-Z])",
                either_case_char(last)
            ),
            None => format!("{}(?:$|{separator})", either_case_char(last)),
        },
        None => format!("(?:$|{separator})"),
    };
    pattern.push_str(&ending);
    pattern
}

/// The capital that starts a camelCase word with this letter, for an ASCII
/// letter.
fn capital(character: char) -> Option<char> {
    character
        .is_ascii_alphabetic()
        .then(|| character.to_ascii_uppercase())
}

/// What a camelCase word may end with before the next capital: the letter in
/// lowercase, or the digit.
fn lowercase_or_digit(character: char) -> Option<char> {
    (character.is_ascii_alphabetic() || character.is_ascii_digit())
        .then(|| character.to_ascii_lowercase())
}

fn either_case_char(character: char) -> String {
    either_case(&character.to_string())
}

/// The word as a pattern that matches it in either case. A letter whose case
/// forms are one character each becomes a class of both; any other character
/// stays literal. Every character is a letter or digit, so none needs
/// escaping.
fn either_case(word: &str) -> String {
    word.chars()
        .map(|character| {
            let mut lower = character.to_lowercase();
            let mut upper = character.to_uppercase();
            match (lower.next(), lower.next(), upper.next(), upper.next()) {
                (Some(lower), None, Some(upper), None) if lower != upper => {
                    format!("[{lower}{upper}]")
                }
                _ => character.to_string(),
            }
        })
        .collect()
}

/// A Workspace's statistics for the plan's words.
pub(super) struct Statistics {
    pub(super) words: Vec<WordStatistics>,
    /// The Workspace's words that a near-spelling of the plan's words may
    /// reach, read by length only.
    pub(super) catalog_words: Vec<WordStatistics>,
    pub(super) total_docs: i64,
    pub(super) avgdl: f64,
}

/// How many of a Workspace's rows hold one word as an exact lexeme.
pub(super) struct WordStatistics {
    pub(super) word: String,
    pub(super) ndoc: i64,
}

/// What the Postgres statement scores and selects candidates by.
pub(super) struct ScoringPlan {
    pub(super) words: Vec<ScoredWord>,
    pub(super) avgdl: f64,
}

impl ScoringPlan {
    /// The catalog words that query words reach as near-spellings.
    pub(super) fn near_spellings(&self) -> Vec<String> {
        self.words
            .iter()
            .flat_map(|word| {
                word.spellings
                    .iter()
                    .filter(|spelling| spelling.word != word.word)
                    .map(|spelling| spelling.word.clone())
            })
            .collect()
    }
}

/// One query word that scores, and the spellings it scores by.
pub(super) struct ScoredWord {
    pub(super) word: String,
    pub(super) spellings: Vec<Spelling>,
}

/// One spelling a word scores by, with its inverse document frequency.
pub(super) struct Spelling {
    pub(super) word: String,
    pub(super) idf: f64,
}

/// FTS5's IDF, `ln((N - n + 0.5) / (n + 0.5))`, floored so a word in more
/// than half the corpus scores nothing instead of a negative value.
fn inverse_document_frequency(total_docs: i64, ndoc: i64) -> f64 {
    let total = as_f64(total_docs.max(0));
    let matched = as_f64(ndoc.clamp(0, total_docs.max(0)));
    ((total - matched + 0.5) / (matched + 0.5))
        .ln()
        .max(IDF_FLOOR)
}

/// Counts stay far below 2^53, so the conversion is exact.
#[expect(
    clippy::cast_precision_loss,
    reason = "document counts stay far below 2^53"
)]
const fn as_f64(value: i64) -> f64 {
    value as f64
}

fn letters(word: &str) -> usize {
    word.chars()
        .filter(|character| character.is_alphabetic())
        .count()
}

/// Whether one edit turns one word into the other: one letter added,
/// removed or changed, or two adjacent letters swapped. Every character the
/// edit touches is a letter, so `workspace1` reaches neither `workspace2` nor
/// `workspaces`. Equal words are no edit apart.
fn one_edit_apart(left: &str, right: &str) -> bool {
    let left = left.chars().collect::<Vec<_>>();
    let right = right.chars().collect::<Vec<_>>();
    let (shorter, longer) = if left.len() <= right.len() {
        (&left, &right)
    } else {
        (&right, &left)
    };
    match longer.len() - shorter.len() {
        0 => {
            let differing = shorter
                .iter()
                .zip(longer)
                .enumerate()
                .filter(|(_index, (shorter, longer))| shorter != longer)
                .collect::<Vec<_>>();
            match differing.as_slice() {
                [(_index, (changed, into))] => changed.is_alphabetic() && into.is_alphabetic(),
                [
                    (first, (left_first, right_first)),
                    (second, (left_second, right_second)),
                ] => {
                    *second == first + 1
                        && left_first == right_second
                        && left_second == right_first
                        && left_first.is_alphabetic()
                        && left_second.is_alphabetic()
                }
                _ => false,
            }
        }
        1 => {
            let common = shorter
                .iter()
                .zip(longer)
                .take_while(|(shorter, longer)| shorter == longer)
                .count();
            longer
                .get(common)
                .is_some_and(|added| added.is_alphabetic())
                && shorter.get(common..) == longer.get(common + 1..)
        }
        _ => false,
    }
}

/// Each typed term, then its compact variant when it adds one. A term both
/// typed and derived counts as typed.
fn with_compact_variants(terms: &[String]) -> Vec<Term> {
    let mut expanded: Vec<Term> = Vec::new();
    let mut push = |text: String, compact_variant: bool| match expanded
        .iter_mut()
        .find(|existing| existing.text == text)
    {
        Some(existing) => existing.compact_variant &= compact_variant,
        None => expanded.push(Term {
            text,
            compact_variant,
        }),
    };
    for term in terms {
        let term = term.trim().to_lowercase();
        if term.is_empty() {
            continue;
        }
        let compact = compact_identifier_variant(&term);
        push(term, false);
        if let Some(compact) = compact {
            push(compact, true);
        }
    }
    expanded
}

/// The words of `term`, as [`split_words`] splits them.
pub(crate) fn lexeme_words(term: &str) -> Vec<String> {
    split_words(term).map(str::to_string).collect()
}

/// Runs of alphanumerics and `_`, which the `simple` text-search parser
/// accepts without operators or quoting. The indexer splits schema text
/// with the same rule.
pub(crate) fn split_words(text: &str) -> impl Iterator<Item = &str> {
    text.split(|character: char| !(character.is_alphanumeric() || character == '_'))
        .filter(|word| !word.is_empty())
}

/// `%term%` for substring matches, with `\`, `%`, and `_` escaped so an
/// identifier such as `deploy_url` matches literally.
pub(super) fn contains_pattern(term: &str) -> String {
    format!("%{}%", escape_like(term))
}

/// `word%` for the lexeme-prefix join in the Postgres score.
pub(super) fn prefix_pattern(word: &str) -> String {
    format!("{}%", escape_like(word))
}

fn escape_like(term: &str) -> String {
    let mut escaped = String::with_capacity(term.len());
    for character in term.chars() {
        if matches!(character, '\\' | '%' | '_') {
            escaped.push('\\');
        }
        escaped.push(character);
    }
    escaped
}

#[cfg(test)]
mod tests;
