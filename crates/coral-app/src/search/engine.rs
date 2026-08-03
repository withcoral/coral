//! Universal Search provider orchestration.

use std::sync::Arc;

use tracing::Instrument as _;

use crate::search::catalog::provider::resolve_entry;
use crate::search::fusion;
use crate::search::provider::{
    ProviderSearchOutcome, SearchExecutionContext, SearchProviderRegistration,
    SearchProviderRegistry, provider_error_outcome, run_provider,
};
use crate::search::result::{ProviderStatus, SearchProviderKind, SearchResponse, SearchTruncation};

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
                    let task = tokio::spawn(run_provider(provider, context).instrument(span));
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
                    rankings: Vec::new(),
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
    outcomes: Vec<ProviderSearchOutcome>,
) -> SearchResponse {
    let provider_has_more = providers_have_more(&outcomes);
    let mut provider_statuses = outcomes
        .iter()
        .map(|outcome| outcome.status.clone())
        .collect::<Vec<_>>();
    let matched_providers = outcomes
        .iter()
        .filter(|outcome| {
            outcome
                .rankings
                .iter()
                .any(|ranking| !ranking.matches.is_empty())
        })
        .map(|outcome| outcome.status.provider)
        .collect::<Vec<_>>();

    // Every retriever from every provider fuses into one ordering. An entry
    // found by several of them accumulates, which is the whole point of keying
    // on identity rather than on who produced it.
    let rankings = outcomes
        .into_iter()
        .flat_map(|outcome| outcome.rankings)
        .collect::<Vec<_>>();
    tracing::debug!(
        workspace = %context.request.workspace_name,
        retrievers = ?rankings
            .iter()
            .map(|ranking| (ranking.retriever.as_str(), ranking.matches.len()))
            .collect::<Vec<_>>(),
        "fusing Universal Search rankings"
    );
    let fused = fusion::fuse(rankings);
    let total_count = fused.len();
    let max_results = usize::try_from(context.request.limit).unwrap_or(usize::MAX);
    let truncated = total_count > max_results || provider_has_more;

    let catalog = context
        .catalog_resolution
        .as_ref()
        .ok()
        .map(|resolution| &resolution.catalog);
    if catalog.is_none() {
        mark_catalog_unresolved(&mut provider_statuses, &matched_providers);
    }
    // An entry the catalog can no longer resolve is dropped rather than
    // returned half-formed; that is what keeps every result queryable.
    let results = catalog.map_or_else(Vec::new, |catalog| {
        fused
            .iter()
            .filter_map(|entry| {
                resolve_entry(catalog, &entry.id, &entry.evidence, &entry.providers)
            })
            .take(max_results)
            .collect::<Vec<_>>()
    });
    let returned_count = u32::try_from(results.len()).unwrap_or(u32::MAX);

    SearchResponse {
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

fn mark_catalog_unresolved(
    statuses: &mut [ProviderStatus],
    unresolved_providers: &[SearchProviderKind],
) {
    for status in statuses {
        if !unresolved_providers.contains(&status.provider) {
            continue;
        }
        status.state = crate::search::result::SearchProviderState::Error;
        if !status.note.is_empty() {
            status.note.push(' ');
        }
        status
            .note
            .push_str("matches could not be resolved because catalog metadata is unavailable");
        if let Some(coverage) = &mut status.coverage {
            coverage.returned_count = 0;
        }
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

    use tokio::sync::oneshot;
    use tokio::time::timeout;
    use tracing::Instrument as _;

    use std::collections::{BTreeMap, BTreeSet};

    use coral_engine::{CatalogInfo, TableInfo};

    use super::{
        UniversalSearchEngine, mark_catalog_unresolved, providers_have_more, truncation_note,
    };
    use crate::catalog::model::CatalogResolution;
    use crate::search::provider::{
        PreparedRetrievers, ProviderFailure, ProviderSearchOutcome, SearchExecutionContext,
        SearchProvider, SearchProviderRegistration, SearchProviderRegistry, provider_error_outcome,
    };
    use crate::search::result::{
        MatchEvidence, ProviderCoverage, ProviderStatus, Ranking, RetrieverId, SearchProviderKind,
        SearchProviderState, SearchRequest, SearchSurfaceId, SearchSurfaceKind, SurfaceMatch,
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
    fn unresolved_matches_do_not_report_returned_provider_results() {
        let mut statuses = vec![ProviderStatus {
            provider: SearchProviderKind::ObservedValues,
            state: SearchProviderState::ResultsFound,
            note: "found one match".to_string(),
            coverage: Some(ProviderCoverage {
                returned_count: 1,
                ..ProviderCoverage::default()
            }),
        }];

        mark_catalog_unresolved(&mut statuses, &[SearchProviderKind::ObservedValues]);

        let status = statuses.first().expect("observed status");
        assert_eq!(status.state, SearchProviderState::Error);
        assert_eq!(
            status
                .coverage
                .as_ref()
                .map(|coverage| coverage.returned_count),
            Some(0)
        );
        assert!(status.note.contains("catalog metadata is unavailable"));
    }

    #[test]
    fn stale_or_budget_exhausted_provider_does_not_imply_more_results() {
        let outcome = ProviderSearchOutcome {
            rankings: Vec::new(),
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
            SearchProviderRegistration::Provider(Arc::new(PanickingProvider {
                kind: SearchProviderKind::CatalogMetadata,
            })),
            SearchProviderRegistration::Provider(Arc::new(StaticProvider {
                outcome: ProviderSearchOutcome {
                    rankings: vec![observed_ranking("survivor")],
                    status: ProviderStatus {
                        provider: SearchProviderKind::ObservedValues,
                        state: SearchProviderState::ResultsFound,
                        note: String::new(),
                        coverage: None,
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
        // Only the two registered providers reach the barrier; the third
        // registration is a static status and never runs.
        let start_barrier = Arc::new(std::sync::Barrier::new(2));
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

        // Every provider must reach the barrier for any of them to return, so
        // the search completing at all proves they started concurrently.
        let response = timeout(Duration::from_secs(1), search)
            .await
            .expect("all providers should start before any outcome is awaited")
            .expect("search task");

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

    /// Providers under test override the provided `search` directly: these
    /// exercise orchestration, not retrieval, so they never build retrievers.
    fn no_retrievers() -> Result<PreparedRetrievers, ProviderFailure> {
        unreachable!("orchestration tests override search")
    }

    struct StaticProvider {
        outcome: ProviderSearchOutcome,
    }

    impl SearchProvider for StaticProvider {
        fn kind(&self) -> SearchProviderKind {
            self.outcome.status.provider
        }

        fn retrievers(
            &self,
            _context: &SearchExecutionContext,
        ) -> Result<PreparedRetrievers, ProviderFailure> {
            no_retrievers()
        }

        fn search(&self, _context: &SearchExecutionContext) -> ProviderSearchOutcome {
            self.outcome.clone()
        }
    }

    struct PanickingProvider {
        kind: SearchProviderKind,
    }

    impl SearchProvider for PanickingProvider {
        fn kind(&self) -> SearchProviderKind {
            self.kind
        }

        fn retrievers(
            &self,
            _context: &SearchExecutionContext,
        ) -> Result<PreparedRetrievers, ProviderFailure> {
            no_retrievers()
        }

        fn search(&self, _context: &SearchExecutionContext) -> ProviderSearchOutcome {
            panic!("provider panic")
        }
    }

    struct BarrierProvider {
        kind: SearchProviderKind,
        start_barrier: Arc<std::sync::Barrier>,
    }

    impl SearchProvider for BarrierProvider {
        fn kind(&self) -> SearchProviderKind {
            self.kind
        }

        fn retrievers(
            &self,
            _context: &SearchExecutionContext,
        ) -> Result<PreparedRetrievers, ProviderFailure> {
            no_retrievers()
        }

        fn search(&self, _context: &SearchExecutionContext) -> ProviderSearchOutcome {
            // Every provider runs on its own blocking thread, so a provider
            // that never reaches the barrier proves work was serialised.
            self.start_barrier.wait();
            provider_error_outcome(self.kind)
        }
    }

    struct PausingBlockingProvider {
        kind: SearchProviderKind,
        started: Mutex<Option<oneshot::Sender<()>>>,
        release: Mutex<Option<std_mpsc::Receiver<()>>>,
    }

    impl SearchProvider for PausingBlockingProvider {
        fn kind(&self) -> SearchProviderKind {
            self.kind
        }

        fn retrievers(
            &self,
            _context: &SearchExecutionContext,
        ) -> Result<PreparedRetrievers, ProviderFailure> {
            no_retrievers()
        }

        fn search(&self, _context: &SearchExecutionContext) -> ProviderSearchOutcome {
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
            started.send(()).expect("signal provider start");
            release.recv().expect("wait for provider release");
            provider_error_outcome(self.kind)
        }
    }

    struct SpanCapturingProvider {
        observed_span: Arc<Mutex<Option<tracing::span::Id>>>,
    }

    impl SearchProvider for SpanCapturingProvider {
        fn kind(&self) -> SearchProviderKind {
            SearchProviderKind::CatalogMetadata
        }

        fn retrievers(
            &self,
            _context: &SearchExecutionContext,
        ) -> Result<PreparedRetrievers, ProviderFailure> {
            no_retrievers()
        }

        fn search(&self, _context: &SearchExecutionContext) -> ProviderSearchOutcome {
            *self.observed_span.lock().expect("observed span lock") = tracing::Span::current().id();
            provider_error_outcome(SearchProviderKind::CatalogMetadata)
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
            Ok(test_catalog_resolution()),
            None,
        )
    }

    /// A catalog holding just the entry the test providers return, so a
    /// surviving ranking can actually resolve into a result.
    fn test_catalog_resolution() -> CatalogResolution {
        CatalogResolution {
            catalog: CatalogInfo {
                tables: vec![TableInfo {
                    catalog_name: None,
                    schema_name: "github".to_string(),
                    table_name: "survivor".to_string(),
                    description: "Surviving provider result".to_string(),
                    guide: String::new(),
                    require_guide_read: false,
                    columns: Vec::new(),
                    required_filters: Vec::new(),
                }],
                table_functions: Vec::new(),
            },
            failed_source_names: BTreeSet::new(),
            runtime_schema_owners: BTreeMap::new(),
        }
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
        })
    }

    fn observed_ranking(name: &str) -> Ranking {
        Ranking {
            retriever: RetrieverId::ObservedValues,
            matches: vec![SurfaceMatch {
                id: SearchSurfaceId {
                    catalog_name: None,
                    schema_name: "github".to_string(),
                    name: name.to_string(),
                    kind: SearchSurfaceKind::Table,
                },
                evidence: MatchEvidence::default(),
            }],
        }
    }
}
