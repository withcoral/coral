//! Backend-neutral catalog projection types and index semantics.
//!
//! Every search backend stores the same projection documents and answers the
//! same retrieval questions. The types here are the contract both sides of the
//! storage seam share; the term normalization and searchable-text composition
//! define what "matches" means independently of the engine that indexes it.

#[derive(Debug, Clone)]
pub(crate) struct CatalogIndexSnapshot {
    pub(crate) documents: Vec<CatalogIndexDocument>,
    pub(crate) fingerprint: String,
}

#[derive(Debug, Clone)]
pub(crate) struct CatalogIndexDocument {
    pub(crate) doc_id: String,
    pub(crate) doc_kind: CatalogIndexDocumentKind,
    pub(crate) source_name: String,
    pub(crate) catalog_name: Option<String>,
    pub(crate) surface_kind: String,
    pub(crate) surface_name: String,
    pub(crate) field_name: String,
    pub(crate) field_role: String,
    pub(crate) qualified_name: String,
    pub(crate) title: String,
    pub(crate) description: String,
    pub(crate) searchable_text: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CatalogIndexDocumentKind {
    CatalogTable,
    CatalogTableFunction,
    ColumnHint,
}

impl CatalogIndexDocumentKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::CatalogTable => "catalog_table",
            Self::CatalogTableFunction => "catalog_table_function",
            Self::ColumnHint => "column_hint",
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct CatalogRefreshResult {
    pub(crate) refreshed: bool,
    pub(crate) document_count: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CatalogRebuildResult {
    pub(crate) old_document_count: u32,
    pub(crate) new_document_count: u32,
    pub(crate) projection_changed: bool,
    pub(crate) rebuild_performed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CatalogClearResult {
    pub(crate) deleted_document_count: u32,
}

#[derive(Debug, Clone)]
pub(crate) struct CatalogSearchHits {
    pub(crate) hits: Vec<CatalogSearchHit>,
    /// Whether the candidate window cut the result short. Surfaced so callers
    /// can report `has_more` rather than implying the index held nothing else.
    pub(crate) retrieval_limited: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct CatalogSearchHit {
    /// Storage identity of the matched document. Retrieval keys on the entry
    /// it resolves to, so this is carried for diagnostics rather than ranking.
    #[cfg_attr(not(test), expect(dead_code, reason = "read by index tests only"))]
    pub(crate) doc_id: String,
    pub(crate) source_name: String,
    pub(crate) catalog_name: Option<String>,
    pub(crate) surface_kind: String,
    pub(crate) surface_name: String,
    pub(crate) field_name: String,
    pub(crate) field_role: String,
}

/// Vocabulary a stored `surface_kind` may take; both backends enforce it as a
/// CHECK constraint and re-validate rows on read.
pub(crate) fn is_known_surface_kind(value: &str) -> bool {
    matches!(value, "" | "table" | "table_function")
}

/// Vocabulary a stored `field_role` may take.
pub(crate) fn is_known_field_role(value: &str) -> bool {
    matches!(
        value,
        "" | "table_column"
            | "table_filter"
            | "table_function_argument"
            | "table_function_result_column"
    )
}

/// Which population of documents a retriever is asking for.
///
/// Entry documents and field documents share an index, but not a candidate
/// window. Measured: in a 50-document window over one shared list, 45 slots go
/// to field documents and only 7 distinct entries survive — wide tables crowd
/// everything else out. Separate windows are what keep entry recall.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CatalogDocumentClass {
    Entries,
    Fields,
}

impl CatalogDocumentClass {
    /// The document kinds this class retrieves. Each backend renders its own
    /// predicate from them.
    pub(crate) fn document_kinds(self) -> &'static [CatalogIndexDocumentKind] {
        match self {
            Self::Entries => &[
                CatalogIndexDocumentKind::CatalogTable,
                CatalogIndexDocumentKind::CatalogTableFunction,
            ],
            Self::Fields => &[CatalogIndexDocumentKind::ColumnHint],
        }
    }
}

/// One normalized query term and where it came from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NormalizedSearchTerm {
    pub(crate) text: String,
    /// Synthesized by removing every non-alphanumeric character from a term
    /// the caller gave (`deploy_url` → `deployurl`); never typed by the
    /// caller. Documents index the same variant of each of their names, so
    /// the only designed match for one is a whole lexeme. A backend that can
    /// tell whether its corpus holds that lexeme may drop a variant it does
    /// not; the whole query's variant of a sentence is the common case.
    pub(crate) compact_variant: bool,
}

/// Lowercases, trims, and de-duplicates query terms, adding the compact
/// identifier variant of each term so `deploy_url` and `deployurl` meet.
pub(crate) fn normalized_search_terms(terms: &[String]) -> Vec<String> {
    normalized_search_term_variants(terms)
        .into_iter()
        .map(|term| term.text)
        .collect()
}

/// [`normalized_search_terms`] with each term's provenance kept.
pub(crate) fn normalized_search_term_variants(terms: &[String]) -> Vec<NormalizedSearchTerm> {
    let mut normalized = Vec::new();
    for term in terms {
        let term = term.trim().to_lowercase();
        if term.is_empty() {
            continue;
        }
        push_search_term(&mut normalized, term.clone(), false);
        if let Some(compact) = compact_identifier_variant(&term) {
            push_search_term(&mut normalized, compact, true);
        }
    }
    normalized
}

fn push_search_term(terms: &mut Vec<NormalizedSearchTerm>, text: String, compact_variant: bool) {
    match terms.iter_mut().find(|existing| existing.text == text) {
        // A term the caller typed is never demoted to a variant, whichever
        // of the two spellings arrived first.
        Some(existing) => existing.compact_variant &= compact_variant,
        None => terms.push(NormalizedSearchTerm {
            text,
            compact_variant,
        }),
    }
}

/// The text a backend indexes as the document's searchable body: the
/// snapshot's searchable text plus compact identifier variants of every name.
pub(crate) fn indexed_searchable_text(document: &CatalogIndexDocument) -> String {
    let mut parts = vec![document.searchable_text.clone()];
    for value in [
        document.source_name.as_str(),
        document.surface_name.as_str(),
        document.field_name.as_str(),
        document.qualified_name.as_str(),
        document.title.as_str(),
    ] {
        if let Some(compact) = compact_identifier_variant(value) {
            parts.push(compact);
        }
    }
    parts
        .into_iter()
        .filter(|part| !part.trim().is_empty())
        .collect::<Vec<_>>()
        .join(" ")
}

fn compact_identifier_variant(value: &str) -> Option<String> {
    let normalized = value.trim().to_lowercase();
    let compact = normalized
        .chars()
        .filter(|ch| ch.is_alphanumeric())
        .collect::<String>();
    (!compact.is_empty() && compact != normalized).then_some(compact)
}

/// Fetches one row past the limit so the caller can tell "exactly full" from
/// "cut short" without a second count query.
pub(crate) fn probe_limit(limit: usize) -> usize {
    limit.saturating_add(1).max(1)
}

pub(crate) fn truncate_probe_hits(hits: &mut Vec<CatalogSearchHit>, limit: usize) -> bool {
    if hits.len() > limit {
        hits.truncate(limit);
        true
    } else {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::{NormalizedSearchTerm, normalized_search_term_variants, normalized_search_terms};

    fn terms(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_string()).collect()
    }

    #[test]
    fn compact_variants_are_flagged_and_typed_terms_are_not() {
        let normalized = normalized_search_term_variants(&terms(&["Deploy_URL", "issue labels"]));

        assert_eq!(
            normalized,
            vec![
                NormalizedSearchTerm {
                    text: "deploy_url".to_string(),
                    compact_variant: false,
                },
                NormalizedSearchTerm {
                    text: "deployurl".to_string(),
                    compact_variant: true,
                },
                NormalizedSearchTerm {
                    text: "issue labels".to_string(),
                    compact_variant: false,
                },
                NormalizedSearchTerm {
                    text: "issuelabels".to_string(),
                    compact_variant: true,
                },
            ]
        );
        assert_eq!(
            normalized_search_terms(&terms(&["Deploy_URL", "issue labels"])),
            terms(&["deploy_url", "deployurl", "issue labels", "issuelabels"])
        );
    }

    #[test]
    fn a_typed_term_is_never_demoted_to_a_variant() {
        // The variant arrives first, then the same spelling typed by the caller.
        let first = normalized_search_term_variants(&terms(&["deploy_url", "deployurl"]));
        assert_eq!(
            first
                .iter()
                .map(|term| term.compact_variant)
                .collect::<Vec<_>>(),
            vec![false, false]
        );
        // And the other way round.
        let second = normalized_search_term_variants(&terms(&["deployurl", "deploy_url"]));
        assert_eq!(
            second
                .iter()
                .map(|term| term.compact_variant)
                .collect::<Vec<_>>(),
            vec![false, false]
        );
    }
}
