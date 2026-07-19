//! Transport-neutral Universal Search request and result models.

use std::cmp::Reverse;

use coral_engine::ColumnInfo;

use crate::bootstrap::AppError;
use crate::catalog::discovery::CatalogItem;
use crate::workspaces::WorkspaceName;

pub(crate) const DEFAULT_UNIVERSAL_SEARCH_LIMIT: u32 = 10;
pub(crate) const MAX_UNIVERSAL_SEARCH_LIMIT: u32 = 50;
const MAX_UNIVERSAL_SEARCH_QUERY_BYTES: usize = 512;

#[derive(Debug)]
pub(crate) enum SearchManagerError {
    App(AppError),
}

impl From<AppError> for SearchManagerError {
    fn from(error: AppError) -> Self {
        Self::App(error)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SearchRequest {
    pub(crate) workspace_name: WorkspaceName,
    pub(crate) query: String,
    pub(crate) limit: u32,
    pub(crate) terms: Vec<String>,
}

impl SearchRequest {
    pub(crate) fn new(
        workspace_name: WorkspaceName,
        query: &str,
        limit: u32,
    ) -> Result<Self, SearchManagerError> {
        let query = query.trim();
        if query.is_empty() {
            return Err(
                AppError::InvalidInput("search query must not be empty".to_string()).into(),
            );
        }
        if query.len() > MAX_UNIVERSAL_SEARCH_QUERY_BYTES {
            return Err(AppError::InvalidInput(format!(
                "search query must be at most {MAX_UNIVERSAL_SEARCH_QUERY_BYTES} bytes"
            ))
            .into());
        }
        let limit = normalized_search_limit(limit)?;
        let terms = query_terms(query);
        Ok(Self {
            workspace_name,
            query: query.to_string(),
            limit,
            terms,
        })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SearchResponse {
    pub(crate) workspace_name: WorkspaceName,
    pub(crate) results: Vec<SearchResult>,
    pub(crate) provider_statuses: Vec<ProviderStatus>,
    pub(crate) truncation: SearchTruncation,
}

#[derive(Debug, Clone)]
pub(crate) struct SearchTruncation {
    pub(crate) truncated: bool,
    pub(crate) returned_count: u32,
    pub(crate) max_results: u32,
    pub(crate) note: String,
}

#[derive(Debug, Clone)]
pub(crate) struct ProviderStatus {
    pub(crate) provider: SearchProviderKind,
    pub(crate) state: SearchProviderState,
    pub(crate) note: String,
    pub(crate) coverage: Option<ProviderCoverage>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SearchProviderKind {
    CatalogMetadata,
    ObservedValues,
    NativeFanout,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SearchProviderState {
    ResultsFound,
    Empty,
    NotEnabled,
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "provider-skip execution is introduced by a later branch in the search stack"
        )
    )]
    Skipped,
    Partial,
    Error,
}

#[derive(Debug, Clone, Default)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "provider coverage is a compact transport/domain status record"
)]
pub(crate) struct ProviderCoverage {
    pub(crate) eligible_units: u32,
    pub(crate) searched_units: u32,
    pub(crate) failed_units: u32,
    pub(crate) returned_count: u32,
    pub(crate) has_more: bool,
    pub(crate) budget_exhausted: bool,
    pub(crate) timed_out: bool,
    pub(crate) stale_index: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct SearchCandidate {
    pub(crate) key: String,
    pub(crate) score: u32,
    pub(crate) provider: SearchProviderKind,
    pub(crate) payload: SearchPayload,
}

impl SearchCandidate {
    pub(crate) fn type_order(&self) -> u8 {
        match &self.payload {
            SearchPayload::CatalogMetadata(_) => 0,
            SearchPayload::ColumnHint(_) => 1,
        }
    }
}

impl Eq for SearchCandidate {}

impl PartialEq for SearchCandidate {
    fn eq(&self, other: &Self) -> bool {
        self.score == other.score
            && self.type_order() == other.type_order()
            && self.key == other.key
    }
}

impl Ord for SearchCandidate {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (Reverse(self.score), self.type_order(), self.key.as_str()).cmp(&(
            Reverse(other.score),
            other.type_order(),
            other.key.as_str(),
        ))
    }
}

impl PartialOrd for SearchCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SearchResult {
    pub(crate) provider: SearchProviderKind,
    pub(crate) payload: SearchPayload,
}

#[derive(Debug, Clone)]
pub(crate) enum SearchPayload {
    CatalogMetadata(CatalogMetadataResult),
    ColumnHint(ColumnHintResult),
}

#[derive(Debug, Clone)]
pub(crate) struct CatalogMetadataResult {
    pub(crate) item: CatalogItem,
    pub(crate) matched_fields: Vec<String>,
    pub(crate) table_column_preview: Option<TableColumnPreview>,
}

#[derive(Debug, Clone)]
pub(crate) struct TableColumnPreview {
    pub(crate) column_count: u32,
    pub(crate) columns: Vec<TableColumnPreviewColumn>,
    pub(crate) omitted_column_count: u32,
}

#[derive(Debug, Clone)]
pub(crate) struct TableColumnPreviewColumn {
    pub(crate) column: ColumnInfo,
    pub(crate) matched_fields: Vec<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct ColumnHintResult {
    pub(crate) schema_name: String,
    pub(crate) surface_name: String,
    pub(crate) surface_kind: SearchSurfaceKind,
    pub(crate) name: String,
    pub(crate) data_type: String,
    pub(crate) required: bool,
    pub(crate) description: String,
    pub(crate) matched_fields: Vec<String>,
    pub(crate) field_role: SearchFieldRole,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum SearchSurfaceKind {
    Table,
    TableFunction,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[expect(
    clippy::enum_variant_names,
    reason = "field roles intentionally mirror the protobuf role names"
)]
pub(crate) enum SearchFieldRole {
    TableColumn,
    TableFilter,
    TableFunctionArgument,
    TableFunctionResultColumn,
}

fn normalized_search_limit(limit: u32) -> Result<u32, AppError> {
    if limit == 0 {
        return Ok(DEFAULT_UNIVERSAL_SEARCH_LIMIT);
    }
    if limit > MAX_UNIVERSAL_SEARCH_LIMIT {
        return Err(AppError::InvalidInput(format!(
            "search limit must be between 1 and {MAX_UNIVERSAL_SEARCH_LIMIT}"
        )));
    }
    Ok(limit)
}

fn query_terms(query: &str) -> Vec<String> {
    let normalized_query = normalize_query_part(query);
    let mut terms = query
        .split(|ch: char| !is_query_token_char(ch))
        .filter_map(|part| {
            let part = normalize_query_part(part);
            (!part.is_empty()).then_some(part)
        })
        .collect::<Vec<_>>();
    if !terms.iter().any(|term| term == &normalized_query) {
        terms.push(normalized_query);
    }
    terms.sort();
    terms.dedup();
    terms
}

fn is_query_token_char(ch: char) -> bool {
    ch.is_alphanumeric() || matches!(ch, '_' | '-' | '.' | '#' | '/' | '@')
}

fn normalize_query_part(value: &str) -> String {
    value.trim().to_lowercase()
}

#[cfg(test)]
mod tests {
    use super::{
        DEFAULT_UNIVERSAL_SEARCH_LIMIT, MAX_UNIVERSAL_SEARCH_LIMIT,
        MAX_UNIVERSAL_SEARCH_QUERY_BYTES, SearchRequest,
    };
    use crate::workspaces::WorkspaceName;

    #[test]
    fn default_limit_applies_when_limit_is_zero() {
        let request = SearchRequest::new(WorkspaceName::default(), "github issue", 0)
            .expect("search request");

        assert_eq!(request.limit, DEFAULT_UNIVERSAL_SEARCH_LIMIT);
    }

    #[test]
    fn query_terms_preserve_common_identifier_punctuation() {
        let request =
            SearchRequest::new(WorkspaceName::default(), "payments-api #eng acme/repo", 10)
                .expect("search request");

        assert!(request.terms.iter().any(|term| term == "payments-api"));
        assert!(request.terms.iter().any(|term| term == "#eng"));
        assert!(request.terms.iter().any(|term| term == "acme/repo"));
    }

    #[test]
    fn query_terms_keep_single_character_identifiers() {
        let request =
            SearchRequest::new(WorkspaceName::default(), "github q", 10).expect("search request");

        assert!(request.terms.iter().any(|term| term == "github"));
        assert!(request.terms.iter().any(|term| term == "q"));
    }

    #[test]
    fn query_terms_use_unicode_lowercasing() {
        let request =
            SearchRequest::new(WorkspaceName::default(), "ÜBER café", 10).expect("search request");

        assert!(request.terms.iter().any(|term| term == "über"));
        assert!(request.terms.iter().any(|term| term == "café"));
    }

    #[test]
    fn oversized_limit_is_rejected() {
        SearchRequest::new(
            WorkspaceName::default(),
            "github issue",
            MAX_UNIVERSAL_SEARCH_LIMIT + 1,
        )
        .expect_err("oversized limit should fail");
    }

    #[test]
    fn oversized_query_is_rejected() {
        let query = "x".repeat(MAX_UNIVERSAL_SEARCH_QUERY_BYTES + 1);

        SearchRequest::new(WorkspaceName::default(), &query, 10)
            .expect_err("oversized query should fail");
    }
}
