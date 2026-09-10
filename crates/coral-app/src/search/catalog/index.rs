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

/// Lowercases, trims, and de-duplicates query terms, adding the compact
/// identifier variant of each term so `deploy_url` and `deployurl` meet.
pub(crate) fn normalized_search_terms(terms: &[String]) -> Vec<String> {
    let mut normalized = Vec::new();
    for term in terms {
        let term = term.trim().to_lowercase();
        if term.is_empty() {
            continue;
        }
        push_search_term(&mut normalized, term.clone());
        if let Some(compact) = compact_identifier_variant(&term) {
            push_search_term(&mut normalized, compact);
        }
    }
    normalized
}

fn push_search_term(terms: &mut Vec<String>, term: String) {
    if !terms.iter().any(|existing| existing == &term) {
        terms.push(term);
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
