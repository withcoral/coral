//! Universal Search provider orchestration.

use crate::bootstrap::AppError;
use crate::query::QueryAttribution;
use crate::search::catalog::provider::CatalogMetadataProvider;
use crate::search::observed::ObservedValuesRetrievalPolicy;
use crate::search::observed::provider::ObservedValuesProvider;
use crate::search::provider::ProviderSearchOutcome;
use crate::search::result::{
    ProviderStatus, SearchProviderKind, SearchProviderState, SearchRequest, SearchResponse,
    SearchResult, SearchTruncation,
};

const CATALOG_ONLY_OBSERVED_NOTE: &str = "observed-value provider disabled by CORAL_SEARCH_PROVIDER_MODE=catalog_only for benchmark ablation";

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum SearchProviderMode {
    #[default]
    CatalogAndObserved,
    CatalogOnly,
}

#[derive(Clone)]
pub(crate) struct UniversalSearchEngine {
    catalog: CatalogMetadataProvider,
    observed: ObservedValuesProvider,
    provider_mode: SearchProviderMode,
}

impl UniversalSearchEngine {
    pub(crate) fn new(
        catalog: CatalogMetadataProvider,
        observed: ObservedValuesProvider,
        provider_mode: SearchProviderMode,
    ) -> Self {
        Self {
            catalog,
            observed,
            provider_mode,
        }
    }

    pub(crate) fn search(
        &self,
        request: &SearchRequest,
        attribution: &QueryAttribution,
        observed_policy: Option<Result<&ObservedValuesRetrievalPolicy, &AppError>>,
    ) -> SearchResponse {
        tracing::debug!(
            workspace = %request.workspace_name,
            query_len_bytes = request.query.len(),
            limit = request.limit,
            "running Universal Search"
        );
        let catalog = self.catalog.search(request, attribution);
        let observed = search_observed_provider(self.provider_mode, || {
            let missing_policy = AppError::Internal(
                "observed-value retrieval policy was not loaded in default provider mode"
                    .to_string(),
            );
            let observed_policy = observed_policy.unwrap_or(Err(&missing_policy));
            self.observed.search(request, observed_policy)
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

fn search_observed_provider(
    mode: SearchProviderMode,
    search: impl FnOnce() -> ProviderSearchOutcome,
) -> ProviderSearchOutcome {
    match mode {
        SearchProviderMode::CatalogAndObserved => search(),
        SearchProviderMode::CatalogOnly => ProviderSearchOutcome {
            candidates: Vec::new(),
            status: ProviderStatus {
                provider: SearchProviderKind::ObservedValues,
                state: SearchProviderState::NotEnabled,
                note: CATALOG_ONLY_OBSERVED_NOTE.to_string(),
                coverage: None,
            },
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
    use super::{
        CATALOG_ONLY_OBSERVED_NOTE, SearchProviderMode, providers_have_more,
        search_observed_provider, truncation_note,
    };
    use crate::search::provider::ProviderSearchOutcome;
    use crate::search::result::{
        ProviderCoverage, ProviderStatus, SearchProviderKind, SearchProviderState,
    };

    fn observed_outcome() -> ProviderSearchOutcome {
        ProviderSearchOutcome {
            candidates: Vec::new(),
            status: ProviderStatus {
                provider: SearchProviderKind::ObservedValues,
                state: SearchProviderState::Empty,
                note: "observed provider ran".to_string(),
                coverage: Some(ProviderCoverage::default()),
            },
        }
    }

    #[test]
    fn default_mode_calls_observed_values_provider() {
        let outcome = search_observed_provider(SearchProviderMode::CatalogAndObserved, || {
            observed_outcome()
        });

        assert_eq!(outcome.status.state, SearchProviderState::Empty);
        assert_eq!(outcome.status.note, "observed provider ran");
        assert!(outcome.status.coverage.is_some());
    }

    #[test]
    fn catalog_only_mode_never_calls_observed_values_provider() {
        let outcome = search_observed_provider(SearchProviderMode::CatalogOnly, || {
            panic!("observed-value provider must not run in catalog-only mode")
        });

        assert!(outcome.candidates.is_empty());
        assert_eq!(outcome.status.provider, SearchProviderKind::ObservedValues);
        assert_eq!(outcome.status.state, SearchProviderState::NotEnabled);
        assert_eq!(outcome.status.note, CATALOG_ONLY_OBSERVED_NOTE);
        assert!(outcome.status.coverage.is_none());
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
