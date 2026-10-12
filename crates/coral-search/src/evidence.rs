//! Why a hit matched, computed at hydration for the page's items only.
//!
//! The database scores the whole row; evidence explains the match from the
//! authoritative fields, which the row joins together (description with
//! guidance, tags with declaration paths). The test is the substring rule of
//! the phrase key, applied per field to each query term and to each word the
//! backends match on, with the compact identifier spelling as well: so
//! `get_forecast` explains `getForecast`, and `issues.weekly` is explained by
//! its words where no field holds it whole. The catalog words that a query
//! word reached as a near-spelling on Postgres are tested the same way, so
//! `colour` explains `color`. Tags are tested one by one, so a hit shows only
//! the tags a term matched.

use std::collections::BTreeSet;

use crate::SearchEvidence;
use crate::retriever::plan::{MIN_TERM_CHARS, lexeme_words};

/// The query terms one hit's evidence tests fields against.
pub(crate) struct EvidenceTerms {
    terms: Vec<(String, String)>,
}

impl EvidenceTerms {
    /// The terms and their words, and the catalog words the ranked statement
    /// matched as near-spellings of them.
    pub(crate) fn new(terms: &[String], near_spellings: &[String]) -> Self {
        let words = terms
            .iter()
            .flat_map(|term| {
                let term = term.to_lowercase();
                let words = lexeme_words(&term);
                std::iter::once(term).chain(words)
            })
            .chain(near_spellings.iter().cloned())
            .filter(|term| term.chars().count() >= MIN_TERM_CHARS)
            .collect::<BTreeSet<_>>();
        Self {
            terms: words
                .into_iter()
                .map(|term| {
                    let compact = compact(&term);
                    (term, compact)
                })
                .collect(),
        }
    }

    /// The byte offset in `text` of the leftmost term as typed, compared
    /// without case. A match only in compact spelling has no offset.
    pub(crate) fn leftmost(&self, text: &str) -> Option<usize> {
        let mut lowercase = String::with_capacity(text.len());
        let mut offsets = Vec::with_capacity(text.len());
        for (offset, character) in text.char_indices() {
            for lower in character.to_lowercase() {
                lowercase.push(lower);
                offsets.resize(lowercase.len(), offset);
            }
        }
        self.terms
            .iter()
            .filter_map(|(term, _compact)| lowercase.find(term.as_str()))
            .min()
            .and_then(|index| offsets.get(index).copied())
    }

    /// Whether any term occurs in the text, as typed or in compact spelling.
    fn match_text(&self, text: &str) -> bool {
        let lowercase = text.to_lowercase();
        let compact_text = compact(text);
        self.terms.iter().any(|(term, compact_term)| {
            lowercase.contains(term.as_str())
                || (!compact_term.is_empty() && compact_text.contains(compact_term.as_str()))
        })
    }
}

/// What one piece of evidence names.
#[derive(Clone, PartialEq, Eq)]
enum Named {
    Field(String),
    Path(String),
    /// One tag; a matched tag names the `tags` field.
    Tag(String),
}

/// The evidence of one hit, and the tags a term matched.
pub(crate) struct Matched {
    pub(crate) evidence: SearchEvidence,
    pub(crate) tags: Vec<String>,
}

/// Collects field names and declaration paths whose text a term matched.
pub(crate) struct EvidenceBuilder<'t> {
    terms: &'t EvidenceTerms,
    matched: Vec<Named>,
}

impl<'t> EvidenceBuilder<'t> {
    pub(crate) const fn new(terms: &'t EvidenceTerms) -> Self {
        Self {
            terms,
            matched: Vec::new(),
        }
    }

    /// Names `field` when any of its values matched.
    pub(crate) fn field<'a>(
        &mut self,
        field: &str,
        values: impl IntoIterator<Item = &'a str>,
    ) -> &mut Self {
        self.test(&Named::Field(field.to_string()), values);
        self
    }

    /// Names the `tags` field when any tag matched, and keeps each matched
    /// tag.
    pub(crate) fn tags<'a>(&mut self, tags: impl IntoIterator<Item = &'a str>) -> &mut Self {
        for tag in tags {
            self.test(&Named::Tag(tag.to_string()), [tag]);
        }
        self
    }

    /// Names a declaration path when its path or its text matched.
    pub(crate) fn path(&mut self, path: &str, text: &str) -> &mut Self {
        self.test(&Named::Path(path.to_string()), [path, text]);
        self
    }

    fn test<'a>(&mut self, named: &Named, values: impl IntoIterator<Item = &'a str>) {
        if values.into_iter().any(|value| self.terms.match_text(value)) {
            self.matched.push(named.clone());
        }
    }

    /// The bounded evidence, fields in name order, and the matched tags, or
    /// `None` when nothing matched.
    pub(crate) fn build(self) -> Option<Matched> {
        let mut fields = Vec::new();
        let mut paths = Vec::new();
        let mut tags = Vec::new();
        for named in self.matched {
            match named {
                Named::Field(field) => fields.push(field),
                Named::Path(path) => paths.push(path),
                Named::Tag(tag) => {
                    fields.push("tags".to_string());
                    tags.push(tag);
                }
            }
        }
        fields.sort();
        (!fields.is_empty() || !paths.is_empty()).then(|| Matched {
            evidence: SearchEvidence::new(fields, paths),
            tags,
        })
    }
}

fn compact(text: &str) -> String {
    text.chars()
        .filter(|character| character.is_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect()
}
