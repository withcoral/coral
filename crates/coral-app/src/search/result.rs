//! Transport-neutral Universal Search result models.

use crate::bootstrap::AppError;
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
        Ok(Self {
            workspace_name,
            query: query.to_string(),
            limit,
        })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SearchResponse {
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
    NotEnabled,
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "reserved for request-level provider gating in concrete provider PRs"
        )
    )]
    Skipped,
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
