//! Transport-neutral Universal Search request and result models.

use crate::bootstrap::AppError;
use crate::workspaces::WorkspaceName;

pub(crate) const DEFAULT_UNIVERSAL_SEARCH_LIMIT: u32 = 10;
pub(crate) const MAX_UNIVERSAL_SEARCH_LIMIT: u32 = 50;
/// Shortest term the catalog index can match.
///
/// The FTS table is tokenized as trigrams, so a term under three characters has
/// no representation in the index at all — `id` matches nothing. Rejecting the
/// query is honest; returning an empty result set reads as "no such thing"
/// rather than "ask me differently".
pub(crate) const MIN_SEARCHABLE_TERM_CHARS: usize = 3;
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
        if !query
            .split(|ch: char| !is_query_token_char(ch))
            .map(normalize_query_part)
            .any(|term| term.chars().count() >= MIN_SEARCHABLE_TERM_CHARS)
        {
            return Err(AppError::InvalidInput(format!(
                "search query must contain a term of at least {MIN_SEARCHABLE_TERM_CHARS} characters"
            ))
            .into());
        }
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
    /// Ranked catalog entries. Every element is something the caller can query;
    /// matched fields and values are evidence nested under the owning entry.
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
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
    Skipped,
    Partial,
    Error,
}

const _: SearchProviderState = SearchProviderState::Skipped;

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

/// Identity of one queryable catalog entry.
///
/// This is both the fusion key and the SQL reference. `kind` is load-bearing:
/// `CatalogInfo` keeps tables and table functions in separate collections, so
/// `(schema_name, name)` alone does not resolve to an entry.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct SearchSurfaceId {
    pub(crate) catalog_name: Option<String>,
    pub(crate) schema_name: String,
    pub(crate) name: String,
    pub(crate) kind: SearchSurfaceKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum SearchSurfaceKind {
    Table,
    TableFunction,
}

/// What the catalog knows about an entry. Carries no query-dependent state, so
/// it is resolved once per surviving entry rather than repeated per match.
#[derive(Debug, Clone)]
pub(crate) struct CatalogSurface {
    pub(crate) id: SearchSurfaceId,
    pub(crate) description: String,
    pub(crate) guide: String,
    pub(crate) shape: SurfaceShape,
}

/// A table's columns are both selectable and filterable, so they share one
/// list. A function separates the values you supply from the ones you get back.
#[derive(Debug, Clone)]
pub(crate) enum SurfaceShape {
    Table {
        fields: Vec<Field>,
    },
    Function {
        arguments: Vec<Field>,
        returns: Vec<Field>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Field {
    pub(crate) name: String,
    pub(crate) data_type: String,
    pub(crate) required: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FieldValues {
    pub(crate) field: String,
    pub(crate) values: Vec<String>,
}

/// Names one field on an entry. The role disambiguates a function argument
/// from a result column that happens to share its name.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct FieldRef {
    pub(crate) name: String,
    pub(crate) role: FieldRole,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum FieldRole {
    Column,
    Filter,
    Argument,
    ResultColumn,
}

/// What a retriever found: an entry identity plus the evidence for it. A
/// retriever cannot emit shape, which is why catalog metadata is resolved once
/// downstream instead of being repeated by every provider.
#[derive(Debug, Clone)]
pub(crate) struct SurfaceMatch {
    pub(crate) id: SearchSurfaceId,
    pub(crate) evidence: MatchEvidence,
}

/// Accumulates across retrievers. Merging is a fold, so union must not depend
/// on the order retrievers ran in.
#[derive(Debug, Clone, Default)]
pub(crate) struct MatchEvidence {
    pub(crate) matched_fields: Vec<FieldRef>,
    pub(crate) matching_values: Vec<FieldValues>,
}

impl MatchEvidence {
    pub(crate) fn merge(&mut self, other: Self) {
        for field in other.matched_fields {
            if !self.matched_fields.contains(&field) {
                self.matched_fields.push(field);
            }
        }
        for values in other.matching_values {
            if let Some(existing) = self
                .matching_values
                .iter_mut()
                .find(|existing| existing.field == values.field)
            {
                for value in values.values {
                    if !existing.values.contains(&value) {
                        existing.values.push(value);
                    }
                }
            } else {
                self.matching_values.push(values);
            }
        }
        // Values are literals a caller pastes into a query, so their order must
        // not depend on which retriever happened to run first.
        for values in &mut self.matching_values {
            values.values.sort();
            values.values.dedup();
        }
        self.matching_values
            .sort_by(|left, right| left.field.cmp(&right.field));
    }
}

/// Names one ranked list so a failing retriever can be reported in its
/// provider's status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RetrieverId {
    CatalogEntries,
    CatalogFields,
    ObservedValues,
}

impl RetrieverId {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::CatalogEntries => "catalog.entries",
            Self::CatalogFields => "catalog.fields",
            Self::ObservedValues => "observed.values",
        }
    }

    pub(crate) fn provider(self) -> SearchProviderKind {
        match self {
            Self::CatalogEntries | Self::CatalogFields => SearchProviderKind::CatalogMetadata,
            Self::ObservedValues => SearchProviderKind::ObservedValues,
        }
    }
}

/// One retriever's ranked output. Vector position is the rank fusion uses; no
/// score crosses this boundary.
#[derive(Debug, Clone)]
pub(crate) struct Ranking {
    pub(crate) retriever: RetrieverId,
    pub(crate) matches: Vec<SurfaceMatch>,
}

/// What we return: catalog knowledge plus what this query found.
#[derive(Debug, Clone)]
pub(crate) struct SearchResult {
    pub(crate) surface: CatalogSurface,
    pub(crate) providers: Vec<SearchProviderKind>,
    pub(crate) matching_values: Vec<FieldValues>,
    pub(crate) omitted_matching_field_count: u32,
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
        AppError, DEFAULT_UNIVERSAL_SEARCH_LIMIT, MAX_UNIVERSAL_SEARCH_LIMIT,
        MAX_UNIVERSAL_SEARCH_QUERY_BYTES, SearchManagerError, SearchRequest,
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
    fn a_query_the_index_cannot_represent_is_rejected() {
        // Both catalog and observed-value indexes are tokenized as trigrams, so
        // a query of only short terms retrieves nothing from either. Failing
        // loudly beats an empty result set, which reads as "no such thing".
        let error = SearchRequest::new(WorkspaceName::default(), "id", 10)
            .expect_err("a two-character query cannot be served");
        let SearchManagerError::App(error) = error;
        let AppError::InvalidInput(message) = error else {
            panic!("a query the index cannot represent is invalid input");
        };
        assert!(
            message.contains("at least 3 characters"),
            "message should say what to do instead, got {message:?}"
        );

        SearchRequest::new(WorkspaceName::default(), "ts", 10)
            .expect_err("a two-character query cannot be served");

        SearchRequest::new(WorkspaceName::default(), "ts of", 10)
            .expect_err("every token is too short for the index");
    }

    #[test]
    fn a_short_term_is_kept_when_another_term_can_be_searched() {
        let request = SearchRequest::new(WorkspaceName::default(), "slack message id", 10)
            .expect("search request");

        assert!(request.terms.iter().any(|term| term == "id"));
        assert!(request.terms.iter().any(|term| term == "message"));
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
