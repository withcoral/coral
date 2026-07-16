//! Universal Search provider orchestration.

use std::sync::Arc;

use crate::search::fusion;
use crate::search::provider::{
    ProviderSearchOutcome, SearchExecutionContext, SearchProviderRegistry, provider_error_outcome,
};
use crate::search::result::{
    ProviderStatus, SearchProviderKind, SearchProviderState, SearchResponse, SearchResult,
    SearchTruncation,
};

#[derive(Clone)]
pub(crate) struct UniversalSearchEngine {
    providers: SearchProviderRegistry,
}

impl UniversalSearchEngine {
    pub(crate) fn new(providers: SearchProviderRegistry) -> Self {
        Self { providers }
    }

    pub(crate) async fn search(&self, context: SearchExecutionContext) -> SearchResponse {
        tracing::debug!(
            workspace = %context.request.workspace_name,
            query_len_bytes = context.request.query.len(),
            limit = context.request.limit,
            elapsed_ms = context.request_started_at.elapsed().as_millis(),
            "running Universal Search"
        );
        let context = Arc::new(context);
        // Start every provider before awaiting any outcome. The registry still
        // defines deterministic response order without serialising provider work.
        let tasks = self
            .providers
            .iter()
            .map(|provider| {
                let provider = Arc::clone(provider);
                let provider_kind = provider.kind();
                let context = Arc::clone(&context);
                let task = tokio::spawn(async move { provider.search(context).await });
                (provider_kind, task)
            })
            .collect::<Vec<_>>();
        let mut outcomes = Vec::with_capacity(tasks.len());
        for (provider_kind, task) in tasks {
            let outcome = match task.await {
                Ok(outcome) => outcome,
                Err(error) => {
                    tracing::error!(
                        provider = ?provider_kind,
                        ?error,
                        "Universal Search provider future failed"
                    );
                    provider_error_outcome(provider_kind)
                }
            };
            outcomes.push(outcome);
        }

        assemble_response(&context, outcomes)
    }
}

fn assemble_response(
    context: &SearchExecutionContext,
    mut outcomes: Vec<ProviderSearchOutcome>,
) -> SearchResponse {
    let provider_has_more = providers_have_more(&outcomes);
    let mut provider_statuses = outcomes
        .iter()
        .map(|outcome| outcome.status.clone())
        .collect::<Vec<_>>();
    if !provider_statuses
        .iter()
        .any(|status| status.provider == SearchProviderKind::NativeFanout)
    {
        provider_statuses.push(native_not_enabled_status());
    }

    let candidates = fusion::order_candidates(&mut outcomes);
    let total_count = candidates.len();
    let max_results = usize::try_from(context.request.limit).unwrap_or(usize::MAX);
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
        workspace_name: context.request.workspace_name.clone(),
        results,
        provider_statuses,
        truncation: SearchTruncation {
            truncated,
            returned_count,
            max_results: context.request.limit,
            note: truncation_note(truncated, provider_has_more, total_count, max_results),
        },
    }
}

fn native_not_enabled_status() -> ProviderStatus {
    ProviderStatus {
        provider: SearchProviderKind::NativeFanout,
        state: SearchProviderState::NotEnabled,
        note: "provider-native fanout is not wired yet".to_string(),
        coverage: None,
    }
}

fn providers_have_more(outcomes: &[ProviderSearchOutcome]) -> bool {
    outcomes.iter().any(|outcome| {
        outcome
            .status
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
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use tokio::sync::Barrier;
    use tokio::time::timeout;

    use super::{UniversalSearchEngine, providers_have_more, truncation_note};
    use crate::bootstrap::AppError;
    use crate::query::manager::QueryManagerError;
    use crate::search::provider::{
        ObservedValuesPolicyInput, ProviderSearchFuture, ProviderSearchOutcome,
        SearchExecutionContext, SearchProvider, SearchProviderRegistry, provider_error_outcome,
    };
    use crate::search::result::{
        ObservedValueResult, ProviderCoverage, ProviderStatus, SearchCandidate, SearchPayload,
        SearchProviderKind, SearchProviderState, SearchRequest, SearchSurfaceKind,
    };
    use crate::workspaces::WorkspaceName;

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
        let outcome = ProviderSearchOutcome {
            candidates: Vec::new(),
            status: ProviderStatus {
                provider: SearchProviderKind::ObservedValues,
                state: SearchProviderState::Partial,
                note: String::new(),
                coverage: Some(ProviderCoverage {
                    budget_exhausted: true,
                    stale_index: true,
                    ..ProviderCoverage::default()
                }),
            },
        };

        assert!(!providers_have_more(&[outcome]));
    }

    #[tokio::test]
    async fn provider_failure_keeps_other_provider_candidates_and_status_order() {
        let registry = SearchProviderRegistry::from_ordered(vec![
            Arc::new(PanickingProvider {
                kind: SearchProviderKind::CatalogMetadata,
            }),
            Arc::new(StaticProvider {
                outcome: ProviderSearchOutcome {
                    candidates: vec![observed_candidate("survivor")],
                    status: ProviderStatus {
                        provider: SearchProviderKind::ObservedValues,
                        state: SearchProviderState::ResultsFound,
                        note: String::new(),
                        coverage: None,
                    },
                },
            }),
        ]);
        let response = UniversalSearchEngine::new(registry)
            .search(test_context())
            .await;

        assert_eq!(response.results.len(), 1);
        assert_eq!(
            response
                .provider_statuses
                .iter()
                .map(|status| (status.provider, status.state))
                .collect::<Vec<_>>(),
            [
                (
                    SearchProviderKind::CatalogMetadata,
                    SearchProviderState::Error,
                ),
                (
                    SearchProviderKind::ObservedValues,
                    SearchProviderState::ResultsFound,
                ),
                (
                    SearchProviderKind::NativeFanout,
                    SearchProviderState::NotEnabled,
                ),
            ]
        );
    }

    #[tokio::test]
    async fn providers_start_concurrently_but_keep_registry_response_order() {
        let start_barrier = Arc::new(Barrier::new(3));
        let registry = SearchProviderRegistry::from_ordered(vec![
            Arc::new(BarrierProvider {
                kind: SearchProviderKind::CatalogMetadata,
                start_barrier: Arc::clone(&start_barrier),
            }),
            Arc::new(BarrierProvider {
                kind: SearchProviderKind::ObservedValues,
                start_barrier: Arc::clone(&start_barrier),
            }),
        ]);
        let engine = UniversalSearchEngine::new(registry);
        let search = tokio::spawn(async move { engine.search(test_context()).await });

        timeout(Duration::from_secs(1), start_barrier.wait())
            .await
            .expect("all providers should start before any outcome is awaited");
        let response = search.await.expect("search task");

        assert_eq!(
            response
                .provider_statuses
                .iter()
                .map(|status| status.provider)
                .collect::<Vec<_>>(),
            [
                SearchProviderKind::CatalogMetadata,
                SearchProviderKind::ObservedValues,
                SearchProviderKind::NativeFanout,
            ]
        );
    }

    struct StaticProvider {
        outcome: ProviderSearchOutcome,
    }

    impl SearchProvider for StaticProvider {
        fn kind(&self) -> SearchProviderKind {
            self.outcome.status.provider
        }

        fn search(&self, _context: Arc<SearchExecutionContext>) -> ProviderSearchFuture {
            let outcome = self.outcome.clone();
            Box::pin(async move { outcome })
        }
    }

    struct PanickingProvider {
        kind: SearchProviderKind,
    }

    impl SearchProvider for PanickingProvider {
        fn kind(&self) -> SearchProviderKind {
            self.kind
        }

        fn search(&self, _context: Arc<SearchExecutionContext>) -> ProviderSearchFuture {
            Box::pin(async move { panic!("provider panic") })
        }
    }

    struct BarrierProvider {
        kind: SearchProviderKind,
        start_barrier: Arc<Barrier>,
    }

    impl SearchProvider for BarrierProvider {
        fn kind(&self) -> SearchProviderKind {
            self.kind
        }

        fn search(&self, _context: Arc<SearchExecutionContext>) -> ProviderSearchFuture {
            let kind = self.kind;
            let start_barrier = Arc::clone(&self.start_barrier);
            Box::pin(async move {
                start_barrier.wait().await;
                provider_error_outcome(kind)
            })
        }
    }

    fn test_context() -> SearchExecutionContext {
        SearchExecutionContext::new(
            Instant::now(),
            SearchRequest::new(WorkspaceName::default(), "issue", 10).expect("search request"),
            Err(QueryManagerError::App(AppError::Internal(
                "catalog resolution is unused by test providers".to_string(),
            ))),
            ObservedValuesPolicyInput::Disabled,
        )
    }

    fn observed_candidate(key: &str) -> SearchCandidate {
        SearchCandidate {
            key: key.to_string(),
            score: 1,
            provider: SearchProviderKind::ObservedValues,
            payload: SearchPayload::ObservedValue(ObservedValueResult {
                value: key.to_string(),
                schema_name: "github".to_string(),
                surface_name: "issues".to_string(),
                column_name: "title".to_string(),
                surface_kind: SearchSurfaceKind::Table,
                field_path: "title".to_string(),
                observed_count: 1,
                last_observed_at: "2026-07-16T00:00:00Z".to_string(),
            }),
        }
    }
}
