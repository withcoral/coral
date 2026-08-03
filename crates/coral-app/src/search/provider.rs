//! Provider-facing Universal Search contracts and ordered registration.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, Weak};
use std::time::Instant;

use tokio::task;

use crate::bootstrap::AppError;
use crate::catalog::model::CatalogResolution;
use crate::query::manager::QueryManagerError;
use crate::search::catalog::provider::CatalogMetadataProvider;
use crate::search::observed::ObservedValuesRetrievalPolicy;
use crate::search::observed::provider::ObservedValuesProvider;
use crate::search::result::{
    ProviderCoverage, ProviderStatus, Ranking, RetrieverId, SearchProviderKind,
    SearchProviderState, SearchRequest, SurfaceMatch,
};
use crate::workspaces::{WorkspaceLifecycleReadLease, WorkspaceName};

pub(crate) type ProviderSearchFuture =
    Pin<Box<dyn Future<Output = ProviderSearchOutcome> + Send + 'static>>;

#[derive(Debug, Clone)]
pub(crate) struct ProviderSearchOutcome {
    /// One ranked list per retriever that succeeded. Fusion flattens these
    /// across every provider; positions are ranks and no score escapes.
    pub(crate) rankings: Vec<Ranking>,
    pub(crate) status: ProviderStatus,
}

/// A provider could not produce any ranking — shared setup failed, so no
/// retriever ran.
#[derive(Debug, Clone)]
pub(crate) struct ProviderFailure {
    pub(crate) state: SearchProviderState,
    pub(crate) note: String,
    pub(crate) coverage: Option<ProviderCoverage>,
}

impl ProviderFailure {
    pub(crate) fn into_outcome(self, provider: SearchProviderKind) -> ProviderSearchOutcome {
        ProviderSearchOutcome {
            rankings: Vec::new(),
            status: ProviderStatus {
                provider,
                state: self.state,
                note: self.note,
                coverage: self.coverage,
            },
        }
    }
}

/// One retriever failed. Its siblings still reach fusion.
#[derive(Debug, Clone)]
pub(crate) struct RetrieverError {
    pub(crate) note: String,
}

pub(crate) struct RetrieverOutcome {
    pub(crate) matches: Vec<SurfaceMatch>,
    pub(crate) retrieval_limited: bool,
}

pub(crate) type ObservedValuesPolicyInput = Result<ObservedValuesRetrievalPolicy, AppError>;

pub(crate) struct SearchExecutionContext {
    pub(crate) request_started_at: Instant,
    // Keep the workspace lifecycle read lease attached to every provider task.
    // Search request cancellation detaches spawned provider work, and blocking
    // provider tasks cannot be cancelled once started.
    _lifecycle_lease: WorkspaceLifecycleReadLease,
    pub(crate) request: SearchRequest,
    pub(crate) catalog_resolution: Result<CatalogResolution, QueryManagerError>,
    pub(crate) observed_values_policy: Option<ObservedValuesPolicyInput>,
}

impl SearchExecutionContext {
    pub(crate) fn new(
        request_started_at: Instant,
        lifecycle_lease: WorkspaceLifecycleReadLease,
        request: SearchRequest,
        catalog_resolution: Result<CatalogResolution, QueryManagerError>,
        observed_values_policy: Option<ObservedValuesPolicyInput>,
    ) -> Self {
        Self {
            request_started_at,
            _lifecycle_lease: lifecycle_lease,
            request,
            catalog_resolution,
            observed_values_policy,
        }
    }
}

/// Serializes local SQLite-backed provider work per workspace while leaving
/// providers that do not use the coordinator free to run concurrently.
#[derive(Debug, Clone, Default)]
pub(crate) struct LocalSearchWriteCoordinator {
    gates: Arc<Mutex<BTreeMap<WorkspaceName, Weak<Mutex<()>>>>>,
}

impl LocalSearchWriteCoordinator {
    pub(crate) fn run<T>(
        &self,
        workspace_name: &WorkspaceName,
        operation: impl FnOnce() -> T,
    ) -> T {
        let gate = {
            let mut gates = self
                .gates
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            gates.retain(|_, gate| gate.strong_count() > 0);
            if let Some(gate) = gates.get(workspace_name).and_then(Weak::upgrade) {
                gate
            } else {
                let gate = Arc::new(Mutex::new(()));
                gates.insert(workspace_name.clone(), Arc::downgrade(&gate));
                gate
            }
        };
        let _guard = gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        operation()
    }
}

/// One way to interrogate a provider.
///
/// Retrievers are constructed per request over whatever their provider prepared,
/// so they can share an expensive projection without the registry knowing about
/// it.
pub(crate) trait Retriever: Send {
    fn id(&self) -> RetrieverId;

    /// Best-first. Vector position is the rank fusion uses; no score escapes.
    fn retrieve(&self, request: &SearchRequest) -> Result<RetrieverOutcome, RetrieverError>;
}

/// A configured search provider.
///
/// Implementations supply their retrievers; the provided `search` drives them
/// and composes status, so no provider can forget to isolate a failing
/// retriever from its siblings. Provider work is synchronous — the registry
/// owns the blocking-task boundary.
pub(crate) trait SearchProvider: Send + Sync {
    fn kind(&self) -> SearchProviderKind;

    /// Acquire per-request resources once, then hand back the retrievers bound
    /// to them. Failing here fails the provider; failing inside one retriever
    /// does not.
    fn retrievers(
        &self,
        context: &SearchExecutionContext,
    ) -> Result<PreparedRetrievers, ProviderFailure>;

    fn search(&self, context: &SearchExecutionContext) -> ProviderSearchOutcome {
        let prepared = match self.retrievers(context) {
            Ok(prepared) => prepared,
            Err(failure) => return failure.into_outcome(self.kind()),
        };

        let mut rankings = Vec::new();
        let mut failed = Vec::new();
        let mut retrieval_limited = false;
        for retriever in prepared.retrievers {
            match retriever.retrieve(&context.request) {
                Ok(outcome) => {
                    retrieval_limited |= outcome.retrieval_limited;
                    rankings.push(Ranking {
                        retriever: retriever.id(),
                        matches: outcome.matches,
                    });
                }
                Err(error) => failed.push((retriever.id(), error)),
            }
        }

        let coverage = prepared.coverage.map(|coverage| ProviderCoverage {
            has_more: coverage.has_more || retrieval_limited,
            ..coverage
        });
        ProviderSearchOutcome {
            status: provider_status(
                self.kind(),
                &rankings,
                &failed,
                coverage,
                prepared.degraded.as_deref(),
            ),
            rankings,
        }
    }
}

/// What preparation produced: the retrievers, plus anything only preparation
/// could know.
pub(crate) struct PreparedRetrievers {
    pub(crate) retrievers: Vec<Box<dyn Retriever>>,
    /// Provider-specific coverage. `returned_count` is filled in centrally once
    /// the retrievers have run.
    pub(crate) coverage: Option<ProviderCoverage>,
    /// Degradation detected during preparation — a stale index, a drained
    /// queue, sources that failed to load. Independent of retriever failure,
    /// but reported the same way.
    pub(crate) degraded: Option<String>,
}

/// Every provider degrades the same way: surviving siblings keep their
/// rankings, and a provider only reports `Error` when nothing survived.
fn provider_status(
    provider: SearchProviderKind,
    rankings: &[Ranking],
    failed: &[(RetrieverId, RetrieverError)],
    coverage: Option<ProviderCoverage>,
    degraded: Option<&str>,
) -> ProviderStatus {
    let returned = rankings
        .iter()
        .map(|ranking| ranking.matches.len())
        .sum::<usize>();
    let has_more = coverage.as_ref().is_some_and(|coverage| coverage.has_more);
    let state = if !failed.is_empty() && rankings.is_empty() {
        SearchProviderState::Error
    } else if !failed.is_empty() || degraded.is_some() || has_more {
        // More matched than was returned, so the provider is reporting a
        // partial view rather than everything it holds.
        SearchProviderState::Partial
    } else if returned == 0 {
        SearchProviderState::Empty
    } else {
        SearchProviderState::ResultsFound
    };
    let coverage = coverage.map(|coverage| ProviderCoverage {
        returned_count: u32::try_from(returned).unwrap_or(u32::MAX),
        ..coverage
    });
    ProviderStatus {
        provider,
        state,
        note: status_note(failed, degraded),
        coverage,
    }
}

fn status_note(failed: &[(RetrieverId, RetrieverError)], degraded: Option<&str>) -> String {
    let mut notes = Vec::new();
    if let Some(degraded) = degraded {
        notes.push(degraded.to_string());
    }
    if !failed.is_empty() {
        notes.push(retriever_failure_note(failed));
    }
    notes.join(" ")
}

fn retriever_failure_note(failed: &[(RetrieverId, RetrieverError)]) -> String {
    if failed.is_empty() {
        return String::new();
    }
    let detail = failed
        .iter()
        .map(|(retriever, error)| format!("{}: {}", retriever.as_str(), error.note))
        .collect::<Vec<_>>()
        .join("; ");
    format!("some retrieval failed without discarding other results ({detail})")
}

#[derive(Clone)]
pub(crate) struct SearchProviderRegistry {
    ordered: Arc<[SearchProviderRegistration]>,
}

#[derive(Clone)]
pub(crate) enum SearchProviderRegistration {
    Provider(Arc<dyn SearchProvider>),
    StaticStatus(ProviderStatus),
}

impl SearchProviderRegistry {
    pub(crate) fn local(
        catalog: CatalogMetadataProvider,
        observed: Option<ObservedValuesProvider>,
    ) -> Self {
        let ordered = local_registrations(
            Arc::new(catalog),
            observed.map(|provider| Arc::new(provider) as Arc<dyn SearchProvider>),
        );
        Self {
            ordered: ordered.into(),
        }
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = &SearchProviderRegistration> {
        self.ordered.iter()
    }

    #[cfg(test)]
    pub(crate) fn from_ordered(ordered: Vec<SearchProviderRegistration>) -> Self {
        Self {
            ordered: ordered.into(),
        }
    }
}

fn local_registrations(
    catalog: Arc<dyn SearchProvider>,
    observed: Option<Arc<dyn SearchProvider>>,
) -> Vec<SearchProviderRegistration> {
    let observed = observed.map_or_else(
        || SearchProviderRegistration::StaticStatus(observed_not_enabled_status()),
        SearchProviderRegistration::Provider,
    );
    vec![
        SearchProviderRegistration::Provider(catalog),
        observed,
        SearchProviderRegistration::StaticStatus(native_not_enabled_status()),
    ]
}

fn native_not_enabled_status() -> ProviderStatus {
    ProviderStatus {
        provider: SearchProviderKind::NativeFanout,
        state: SearchProviderState::NotEnabled,
        note: "provider-native fanout is disabled".to_string(),
        coverage: None,
    }
}

/// Runs one provider's synchronous work on a blocking task.
///
/// Providers read `SQLite`, so their work must leave the async worker. Owning the
/// boundary here rather than in each provider is what lets `SearchProvider` stay
/// synchronous, which in turn lets retrievers borrow request-scoped state
/// without a `'static` future having to hold it.
pub(crate) fn run_provider(
    provider: Arc<dyn SearchProvider>,
    context: Arc<SearchExecutionContext>,
) -> ProviderSearchFuture {
    let kind = provider.kind();
    Box::pin(async move {
        let span = tracing::Span::current();
        // The blocking thread has no subscriber of its own, so carry the
        // caller's dispatcher across with the span or provider work is untraced.
        let dispatch = tracing::dispatcher::get_default(tracing::Dispatch::clone);
        match task::spawn_blocking(move || {
            tracing::dispatcher::with_default(&dispatch, || {
                span.in_scope(|| provider.search(&context))
            })
        })
        .await
        {
            Ok(outcome) => outcome,
            Err(error) => {
                tracing::error!(?kind, ?error, "Universal Search provider task failed");
                provider_error_outcome(kind)
            }
        }
    })
}

pub(crate) fn provider_error_outcome(provider: SearchProviderKind) -> ProviderSearchOutcome {
    ProviderSearchOutcome {
        rankings: Vec::new(),
        status: ProviderStatus {
            provider,
            state: SearchProviderState::Error,
            note: "search provider failed without affecting other providers".to_string(),
            coverage: None,
        },
    }
}

fn observed_not_enabled_status() -> ProviderStatus {
    ProviderStatus {
        provider: SearchProviderKind::ObservedValues,
        state: SearchProviderState::NotEnabled,
        note: "observed value search is disabled; enable `observed_values_search` to include values from earlier queries".to_string(),
        coverage: None,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc as std_mpsc;
    use std::thread;

    use super::{
        LocalSearchWriteCoordinator, PreparedRetrievers, ProviderFailure, SearchExecutionContext,
        SearchProvider, SearchProviderRegistration, local_registrations,
    };
    use crate::search::result::{SearchProviderKind, SearchProviderState};
    use crate::workspaces::WorkspaceName;

    #[test]
    fn disabled_observed_search_is_registered_as_a_static_status() {
        let registrations = local_registrations(
            Arc::new(UnusedProvider(SearchProviderKind::CatalogMetadata)),
            None,
        );

        let SearchProviderRegistration::StaticStatus(status) =
            registrations.get(1).expect("observed registration")
        else {
            panic!("disabled observed search must not register its provider");
        };
        assert_eq!(status.provider, SearchProviderKind::ObservedValues);
        assert_eq!(status.state, SearchProviderState::NotEnabled);
        assert!(status.coverage.is_none());
        assert!(status.note.contains("`observed_values_search`"));
    }

    #[test]
    fn shared_local_writers_never_overlap_and_independent_work_still_runs() {
        let coordinator = LocalSearchWriteCoordinator::default();
        let workspace = WorkspaceName::default();
        let active_writers = Arc::new(AtomicUsize::new(0));
        let max_active_writers = Arc::new(AtomicUsize::new(0));
        let (first_started_tx, first_started_rx) = std_mpsc::channel();
        let (release_first_tx, release_first_rx) = std_mpsc::channel();

        let first_coordinator = coordinator.clone();
        let first_workspace = workspace.clone();
        let first_active = Arc::clone(&active_writers);
        let first_max = Arc::clone(&max_active_writers);
        let first = thread::spawn(move || {
            first_coordinator.run(&first_workspace, || {
                record_active_writer(&first_active, &first_max);
                first_started_tx.send(()).expect("signal first writer");
                release_first_rx.recv().expect("release first writer");
                first_active.fetch_sub(1, Ordering::SeqCst);
            });
        });
        first_started_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .expect("first writer starts");

        let second_active = Arc::clone(&active_writers);
        let second_max = Arc::clone(&max_active_writers);
        let (second_started_tx, second_started_rx) = std_mpsc::channel();
        let second = thread::spawn(move || {
            coordinator.run(&workspace, || {
                record_active_writer(&second_active, &second_max);
                second_started_tx.send(()).expect("signal second writer");
                second_active.fetch_sub(1, Ordering::SeqCst);
            });
        });
        assert!(matches!(
            second_started_rx.recv_timeout(std::time::Duration::from_millis(100)),
            Err(std_mpsc::RecvTimeoutError::Timeout)
        ));

        let (independent_tx, independent_rx) = std_mpsc::channel();
        let independent = thread::spawn(move || independent_tx.send(()));
        independent_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .expect("independent provider work remains concurrent");

        release_first_tx.send(()).expect("release first writer");
        second_started_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .expect("second writer starts after first completes");
        first.join().expect("first writer");
        second.join().expect("second writer");
        independent
            .join()
            .expect("independent worker")
            .expect("signal independent work");
        assert_eq!(max_active_writers.load(Ordering::SeqCst), 1);
    }

    fn record_active_writer(active: &AtomicUsize, max_active: &AtomicUsize) {
        let current = active.fetch_add(1, Ordering::SeqCst) + 1;
        max_active.fetch_max(current, Ordering::SeqCst);
    }

    struct UnusedProvider(SearchProviderKind);

    impl SearchProvider for UnusedProvider {
        fn kind(&self) -> SearchProviderKind {
            self.0
        }

        fn retrievers(
            &self,
            _context: &SearchExecutionContext,
        ) -> Result<PreparedRetrievers, ProviderFailure> {
            unreachable!("static registration must not invoke disabled provider")
        }
    }
}
