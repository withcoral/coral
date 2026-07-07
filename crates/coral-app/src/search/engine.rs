//! Universal Search provider orchestration.

use crate::query::QueryAttribution;
use crate::search::catalog::provider::CatalogMetadataProvider;
use crate::search::result::{
    ProviderStatus, SearchProviderKind, SearchProviderState, SearchRequest, SearchResponse,
    SearchResult, SearchTruncation,
};

#[derive(Clone)]
pub(crate) struct UniversalSearchEngine {
    catalog: CatalogMetadataProvider,
}

impl UniversalSearchEngine {
    pub(crate) fn new(catalog: CatalogMetadataProvider) -> Self {
        Self { catalog }
    }

    pub(crate) fn search(
        &self,
        request: &SearchRequest,
        attribution: &QueryAttribution,
    ) -> SearchResponse {
        tracing::debug!(
            workspace = %request.workspace_name,
            query_len_bytes = request.query.len(),
            limit = request.limit,
            "running Universal Search"
        );
        let catalog = self.catalog.search(request, attribution);
        let provider_has_more = catalog
            .status
            .coverage
            .as_ref()
            .is_some_and(|coverage| coverage.has_more);
        let mut candidates = catalog.candidates;
        candidates.sort();

        let total_count = candidates.len();
        let max_results = usize::try_from(request.limit).unwrap_or(usize::MAX);
        let truncated = total_count > max_results || provider_has_more;
        let results = candidates
            .into_iter()
            .take(max_results)
            .map(|candidate| SearchResult {
                provider: candidate.provider,
                payload: candidate.payload,
            })
            .collect::<Vec<_>>();
        let returned_count = u32::try_from(results.len()).unwrap_or(u32::MAX);

        SearchResponse {
            workspace_name: request.workspace_name.clone(),
            results,
            provider_statuses: vec![
                catalog.status,
                ProviderStatus {
                    provider: SearchProviderKind::ObservedValues,
                    state: SearchProviderState::NotEnabled,
                    note: "observed value search is not wired yet".to_string(),
                    coverage: None,
                },
                ProviderStatus {
                    provider: SearchProviderKind::NativeFanout,
                    state: SearchProviderState::NotEnabled,
                    note: "provider-native fanout is not wired yet".to_string(),
                    coverage: None,
                },
            ],
            truncation: SearchTruncation {
                truncated,
                returned_count,
                max_results: request.limit,
                note: truncation_note(truncated, provider_has_more, total_count, max_results),
            },
        }
    }
}

fn truncation_note(
    truncated: bool,
    provider_has_more: bool,
    total_count: usize,
    max_results: usize,
) -> String {
    if provider_has_more {
        "one or more search providers had more matches than were returned".to_string()
    } else if total_count > max_results {
        format!("returned {max_results} of {total_count} search hints")
    } else if truncated {
        "search results were truncated".to_string()
    } else {
        String::new()
    }
}

#[cfg(test)]
mod tests {
    use super::truncation_note;

    #[test]
    fn truncation_note_does_not_report_retrieved_count_as_total_when_provider_has_more() {
        let note = truncation_note(true, true, 50, 10);

        assert_eq!(
            note,
            "one or more search providers had more matches than were returned"
        );
    }
}
