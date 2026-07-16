//! Universal Search provider orchestration.

use std::sync::Arc;

use tracing::Instrument as _;

use crate::search::fusion;
use crate::search::provider::{
    ProviderSearchOutcome, SearchExecutionContext, SearchProviderRegistration,
    SearchProviderRegistry, provider_error_outcome,
};
use crate::search::result::{
    ProviderStatus, SearchProviderKind, SearchResponse, SearchResult, SearchTruncation,
};

#[derive(Clone)]
pub(crate) struct UniversalSearchEngine {
    providers: SearchProviderRegistry,
}

enum PendingProviderSearch {
    Provider {
        kind: SearchProviderKind,
        task: tokio::task::JoinHandle<ProviderSearchOutcome>,
    },
    StaticStatus(ProviderStatus),
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
            .map(|registration| match registration {
                SearchProviderRegistration::Provider(provider) => {
                    let provider = Arc::clone(provider);
                    let kind = provider.kind();
                    let context = Arc::clone(&context);
                    let span = tracing::Span::current();
                    let task = tokio::spawn(
                        async move { provider.search(context).await }.instrument(span),
                    );
                    PendingProviderSearch::Provider { kind, task }
                }
                SearchProviderRegistration::StaticStatus(status) => {
                    PendingProviderSearch::StaticStatus(status.clone())
                }
            })
            .collect::<Vec<_>>();
        let mut outcomes = Vec::with_capacity(tasks.len());
        for pending in tasks {
            let outcome = match pending {
                PendingProviderSearch::Provider { kind, task } => match task.await {
                    Ok(outcome) => outcome,
                    Err(error) => {
                        tracing::error!(
                            provider = ?kind,
                            ?error,
                            "Universal Search provider future failed"
                        );
                        provider_error_outcome(kind)
                    }
                },
                PendingProviderSearch::StaticStatus(status) => ProviderSearchOutcome {
                    candidates: Vec::new(),
                    status,
                },
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
    let provider_statuses = outcomes
        .iter()
        .map(|outcome| outcome.status.clone())
        .collect::<Vec<_>>();

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
    use std::sync::{Arc, Mutex, mpsc as std_mpsc};
    use std::time::{Duration, Instant};

    use tokio::sync::{Barrier, oneshot};
    use tokio::time::timeout;
    use tracing::Instrument as _;

    use super::{UniversalSearchEngine, providers_have_more, truncation_note};
    use crate::bootstrap::AppError;
    use crate::query::manager::QueryManagerError;
    use crate::search::provider::{
        ProviderSearchFuture, ProviderSearchOutcome, SearchExecutionContext, SearchProvider,
        SearchProviderRegistration, SearchProviderRegistry, provider_error_outcome,
    };
    use crate::search::result::{
        ObservedValueResult, ProviderCoverage, ProviderStatus, SearchCandidate, SearchPayload,
        SearchProviderKind, SearchProviderState, SearchRequest, SearchSurfaceKind,
    };
    use crate::workspaces::{WorkspaceLifecycleLock, WorkspaceLifecycleReadLease, WorkspaceName};

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
                diagnostics: Vec::new(),
                diagnostics_truncated: false,
                omitted_diagnostic_count: 0,
            },
        };

        assert!(!providers_have_more(&[outcome]));
    }

    #[tokio::test]
    async fn provider_failure_keeps_other_provider_candidates_and_status_order() {
        let registry = SearchProviderRegistry::from_ordered(vec![
            SearchProviderRegistration::Provider(Arc::new(PanickingProvider {
                kind: SearchProviderKind::CatalogMetadata,
            })),
            SearchProviderRegistration::Provider(Arc::new(StaticProvider {
                outcome: ProviderSearchOutcome {
                    candidates: vec![observed_candidate("survivor")],
                    status: ProviderStatus {
                        provider: SearchProviderKind::ObservedValues,
                        state: SearchProviderState::ResultsFound,
                        note: String::new(),
                        coverage: None,
                        diagnostics: Vec::new(),
                        diagnostics_truncated: false,
                        omitted_diagnostic_count: 0,
                    },
                },
            })),
            static_status_registration(
                SearchProviderKind::NativeFanout,
                SearchProviderState::NotEnabled,
            ),
        ]);
        let response = UniversalSearchEngine::new(registry)
            .search(test_context().await)
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
    async fn response_contains_only_registry_owned_statuses() {
        let registry = SearchProviderRegistry::from_ordered(vec![static_status_registration(
            SearchProviderKind::ObservedValues,
            SearchProviderState::NotEnabled,
        )]);

        let response = UniversalSearchEngine::new(registry)
            .search(test_context().await)
            .await;

        assert!(response.results.is_empty());
        assert_eq!(
            response
                .provider_statuses
                .iter()
                .map(|status| (status.provider, status.state))
                .collect::<Vec<_>>(),
            [(
                SearchProviderKind::ObservedValues,
                SearchProviderState::NotEnabled,
            )]
        );
    }

    #[tokio::test]
    async fn providers_start_concurrently_but_keep_registry_response_order() {
        let start_barrier = Arc::new(Barrier::new(3));
        let registry = SearchProviderRegistry::from_ordered(vec![
            SearchProviderRegistration::Provider(Arc::new(BarrierProvider {
                kind: SearchProviderKind::CatalogMetadata,
                start_barrier: Arc::clone(&start_barrier),
            })),
            SearchProviderRegistration::Provider(Arc::new(BarrierProvider {
                kind: SearchProviderKind::ObservedValues,
                start_barrier: Arc::clone(&start_barrier),
            })),
            static_status_registration(
                SearchProviderKind::NativeFanout,
                SearchProviderState::NotEnabled,
            ),
        ]);
        let engine = UniversalSearchEngine::new(registry);
        let search = tokio::spawn(async move { engine.search(test_context().await).await });

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

    #[tokio::test(flavor = "current_thread")]
    async fn provider_future_inherits_the_search_request_span() {
        let _subscriber = tracing::subscriber::set_default(tracing_subscriber::Registry::default());
        let observed_span = Arc::new(Mutex::new(None));
        let registry =
            SearchProviderRegistry::from_ordered(vec![SearchProviderRegistration::Provider(
                Arc::new(SpanCapturingProvider {
                    observed_span: Arc::clone(&observed_span),
                }),
            )]);
        let request_span = tracing::info_span!("universal_search_request");
        let expected_span = request_span.id().expect("request span id");

        UniversalSearchEngine::new(registry)
            .search(test_context().await)
            .instrument(request_span)
            .await;

        assert_eq!(
            observed_span.lock().expect("observed span lock").as_ref(),
            Some(&expected_span)
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_search_keeps_lifecycle_lease_until_blocking_provider_finishes() {
        let lifecycle = WorkspaceLifecycleLock::default();
        let workspace = WorkspaceName::default();
        let revision = lifecycle
            .revision_if_active_async(&workspace)
            .await
            .expect("workspace is active");
        let lifecycle_lease = lifecycle
            .read_lease_if_unchanged(revision, &workspace)
            .await
            .expect("current lifecycle lease");
        let (started_tx, started_rx) = oneshot::channel();
        let (release_tx, release_rx) = std_mpsc::channel();
        let registry =
            SearchProviderRegistry::from_ordered(vec![SearchProviderRegistration::Provider(
                Arc::new(PausingBlockingProvider {
                    kind: SearchProviderKind::CatalogMetadata,
                    started: Mutex::new(Some(started_tx)),
                    release: Mutex::new(Some(release_rx)),
                }),
            )]);
        let engine = UniversalSearchEngine::new(registry);
        let search = tokio::spawn(async move {
            engine
                .search(test_context_with_lease(lifecycle_lease))
                .await
        });

        started_rx.await.expect("blocking provider should start");
        search.abort();
        assert!(
            search
                .await
                .expect_err("search task should be cancelled")
                .is_cancelled()
        );

        assert!(
            timeout(Duration::from_millis(50), lifecycle.lock_async())
                .await
                .is_err(),
            "detached provider work must retain the lifecycle read lease"
        );

        release_tx.send(()).expect("release blocking provider");
        let guard = timeout(Duration::from_secs(1), lifecycle.lock_async())
            .await
            .expect("lifecycle writer should proceed after provider completion");
        drop(guard);
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

    struct PausingBlockingProvider {
        kind: SearchProviderKind,
        started: Mutex<Option<oneshot::Sender<()>>>,
        release: Mutex<Option<std_mpsc::Receiver<()>>>,
    }

    struct SpanCapturingProvider {
        observed_span: Arc<Mutex<Option<tracing::span::Id>>>,
    }

    impl SearchProvider for SpanCapturingProvider {
        fn kind(&self) -> SearchProviderKind {
            SearchProviderKind::CatalogMetadata
        }

        fn search(&self, _context: Arc<SearchExecutionContext>) -> ProviderSearchFuture {
            let observed_span = Arc::clone(&self.observed_span);
            Box::pin(async move {
                *observed_span.lock().expect("observed span lock") = tracing::Span::current().id();
                provider_error_outcome(SearchProviderKind::CatalogMetadata)
            })
        }
    }

    impl SearchProvider for PausingBlockingProvider {
        fn kind(&self) -> SearchProviderKind {
            self.kind
        }

        fn search(&self, context: Arc<SearchExecutionContext>) -> ProviderSearchFuture {
            let kind = self.kind;
            let started = self
                .started
                .lock()
                .expect("started sender lock")
                .take()
                .expect("provider starts once");
            let release = self
                .release
                .lock()
                .expect("release receiver lock")
                .take()
                .expect("provider starts once");
            Box::pin(async move {
                tokio::task::spawn_blocking(move || {
                    started.send(()).expect("signal provider start");
                    release.recv().expect("wait for provider release");
                    drop(context);
                    provider_error_outcome(kind)
                })
                .await
                .expect("blocking provider task")
            })
        }
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

    async fn test_context() -> SearchExecutionContext {
        let lifecycle = WorkspaceLifecycleLock::default();
        let workspace = WorkspaceName::default();
        let revision = lifecycle
            .revision_if_active_async(&workspace)
            .await
            .expect("workspace is active");
        let lifecycle_lease = lifecycle
            .read_lease_if_unchanged(revision, &workspace)
            .await
            .expect("current lifecycle lease");
        test_context_with_lease(lifecycle_lease)
    }

    fn test_context_with_lease(
        lifecycle_lease: WorkspaceLifecycleReadLease,
    ) -> SearchExecutionContext {
        SearchExecutionContext::new(
            Instant::now(),
            lifecycle_lease,
            SearchRequest::new(WorkspaceName::default(), "issue", 10).expect("search request"),
            Err(QueryManagerError::App(AppError::Internal(
                "catalog resolution is unused by test providers".to_string(),
            ))),
            None,
        )
    }

    fn static_status_registration(
        provider: SearchProviderKind,
        state: SearchProviderState,
    ) -> SearchProviderRegistration {
        SearchProviderRegistration::StaticStatus(ProviderStatus {
            provider,
            state,
            note: String::new(),
            coverage: None,
            diagnostics: Vec::new(),
            diagnostics_truncated: false,
            omitted_diagnostic_count: 0,
        })
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
