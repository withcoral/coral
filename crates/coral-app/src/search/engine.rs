//! Universal Search provider orchestration.

use crate::bootstrap::AppError;
use crate::catalog::model::CatalogResolution;
use crate::query::QueryAttribution;
use crate::query::manager::QueryManagerError;
use crate::search::catalog::provider::CatalogMetadataProvider;
use crate::search::observed::ObservedValuesRetrievalPolicy;
use crate::search::observed::provider::ObservedValuesProvider;
use crate::search::provider::ProviderSearchOutcome;
use crate::search::result::{
    ProviderStatus, SearchProviderKind, SearchProviderState, SearchRequest, SearchResponse,
    SearchResult, SearchTruncation,
};

#[derive(Clone)]
pub(crate) struct UniversalSearchEngine {
    catalog: CatalogMetadataProvider,
    observed: ObservedValuesProvider,
}

impl UniversalSearchEngine {
    pub(crate) fn new(catalog: CatalogMetadataProvider, observed: ObservedValuesProvider) -> Self {
        Self { catalog, observed }
    }

    pub(crate) fn search(
        &self,
        request: &SearchRequest,
        _attribution: &QueryAttribution,
        catalog_resolution: Result<&CatalogResolution, &QueryManagerError>,
        observed_policy: Option<Result<&ObservedValuesRetrievalPolicy, &AppError>>,
    ) -> SearchResponse {
        tracing::debug!(
            workspace = %request.workspace_name,
            query_len_bytes = request.query.len(),
            limit = request.limit,
            "running Universal Search"
        );
        let catalog = self.catalog.search(request, catalog_resolution);
        let observed = observed_policy.map_or_else(observed_not_enabled_outcome, |policy| {
            self.observed.search(request, policy)
        });
        let provider_has_more = providers_have_more(&[&catalog.status, &observed.status]);
        let mut candidates = catalog.candidates;
        candidates.extend(observed.candidates);
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
                observed.status,
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

fn observed_not_enabled_outcome() -> ProviderSearchOutcome {
    ProviderSearchOutcome {
        candidates: Vec::new(),
        status: ProviderStatus {
            provider: SearchProviderKind::ObservedValues,
            state: SearchProviderState::NotEnabled,
            note: "observed value search is disabled; enable `observed_values_search` to include values from earlier queries".to_string(),
            coverage: None,
        },
    }
}

fn providers_have_more(statuses: &[&ProviderStatus]) -> bool {
    statuses.iter().any(|status| {
        status
            .coverage
            .as_ref()
            .is_some_and(|coverage| coverage.has_more)
    })
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
    use super::{observed_not_enabled_outcome, providers_have_more, truncation_note};
    use crate::search::result::{
        ProviderCoverage, ProviderStatus, SearchProviderKind, SearchProviderState,
    };

    #[test]
    fn disabled_observed_search_reports_not_enabled_without_candidates_or_coverage() {
        let outcome = observed_not_enabled_outcome();

        assert!(outcome.candidates.is_empty());
        assert_eq!(outcome.status.provider, SearchProviderKind::ObservedValues);
        assert_eq!(outcome.status.state, SearchProviderState::NotEnabled);
        assert!(outcome.status.coverage.is_none());
        assert!(outcome.status.note.contains("`observed_values_search`"));
    }

    #[test]
    fn truncation_note_does_not_report_retrieved_count_as_total_when_provider_has_more() {
        let note = truncation_note(true, true, 50, 10);

        assert_eq!(
            note,
            "one or more search providers had more matches than were returned"
        );
    }

    #[test]
    fn truncation_note_reports_global_fusion_truncation_without_provider_overflow() {
        let note = truncation_note(true, false, 12, 10);

        assert_eq!(note, "returned 10 of 12 search hints");
    }

    #[test]
    fn stale_or_budget_exhausted_provider_does_not_imply_more_results() {
        let status = ProviderStatus {
            provider: SearchProviderKind::ObservedValues,
            state: SearchProviderState::Partial,
            note: String::new(),
            coverage: Some(ProviderCoverage {
                budget_exhausted: true,
                stale_index: true,
                ..ProviderCoverage::default()
            }),
        };

        assert!(!providers_have_more(&[&status]));
    }
}
