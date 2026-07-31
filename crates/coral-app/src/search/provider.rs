//! Provider-facing Universal Search contracts and ordered registration.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, Weak};

use tokio::task;
use tokio::time::Instant;
use uuid::Uuid;

use crate::bootstrap::AppError;
use crate::catalog::model::CatalogResolution;
use crate::query::manager::QueryManagerError;
use crate::search::catalog::provider::CatalogMetadataProvider;
use crate::search::observed::ObservedValuesRetrievalPolicy;
use crate::search::observed::provider::ObservedValuesProvider;
use crate::search::result::{
    ProviderStatus, SearchCandidate, SearchProviderKind, SearchProviderState, SearchRequest,
};
use crate::workspaces::{WorkspaceLifecycleReadLease, WorkspaceName};

pub(crate) type ProviderSearchFuture =
    Pin<Box<dyn Future<Output = ProviderSearchOutcome> + Send + 'static>>;

#[derive(Debug, Clone)]
pub(crate) struct ProviderSearchOutcome {
    pub(crate) candidates: Vec<SearchCandidate>,
    pub(crate) status: ProviderStatus,
}

pub(crate) type ObservedValuesPolicyInput = Result<ObservedValuesRetrievalPolicy, AppError>;

pub(crate) struct SearchExecutionContext {
    pub(crate) request_started_at: Instant,
    // Keep the workspace lifecycle read lease attached to every provider task.
    // Search request cancellation detaches spawned provider work, and blocking
    // provider tasks cannot be cancelled once started.
    _lifecycle_lease: WorkspaceLifecycleReadLease,
    /// UTC wall-clock cutoff in `SQLite`'s millisecond timestamp format.
    ///
    /// Observed rows projected at or after this instant belong to this or a
    /// concurrent request and must not enter this response.
    pub(crate) observed_values_cutoff: Option<String>,
    pub(crate) search_origin: Uuid,
    pub(crate) request: SearchRequest,
    pub(crate) catalog_resolution: Result<CatalogResolution, QueryManagerError>,
    pub(crate) observed_values_policy: Option<ObservedValuesPolicyInput>,
}

impl SearchExecutionContext {
    pub(crate) fn new(
        request_started_at: Instant,
        lifecycle_lease: WorkspaceLifecycleReadLease,
        observed_values_cutoff: Option<String>,
        search_origin: Uuid,
        request: SearchRequest,
        catalog_resolution: Result<CatalogResolution, QueryManagerError>,
        observed_values_policy: Option<ObservedValuesPolicyInput>,
    ) -> Self {
        Self {
            request_started_at,
            _lifecycle_lease: lifecycle_lease,
            observed_values_cutoff,
            search_origin,
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

pub(crate) trait SearchProvider: Send + Sync {
    fn kind(&self) -> SearchProviderKind;

    /// Returns candidates in best-first provider-local order. When native
    /// fanout contributes candidates, fusion treats each vector position as
    /// that provider's rank instead of re-sorting provider-local evidence.
    fn search(&self, context: Arc<SearchExecutionContext>) -> ProviderSearchFuture;
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
        native: Option<Arc<dyn SearchProvider>>,
    ) -> Self {
        let ordered = local_registrations(
            Arc::new(catalog),
            observed.map(|provider| Arc::new(provider) as Arc<dyn SearchProvider>),
            native,
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
    native: Option<Arc<dyn SearchProvider>>,
) -> Vec<SearchProviderRegistration> {
    let observed = observed.map_or_else(
        || SearchProviderRegistration::StaticStatus(observed_not_enabled_status()),
        SearchProviderRegistration::Provider,
    );
    vec![
        SearchProviderRegistration::Provider(catalog),
        observed,
        native.map_or_else(
            || SearchProviderRegistration::StaticStatus(native_not_enabled_status()),
            SearchProviderRegistration::Provider,
        ),
    ]
}

fn native_not_enabled_status() -> ProviderStatus {
    ProviderStatus {
        provider: SearchProviderKind::NativeFanout,
        state: SearchProviderState::NotEnabled,
        note: "provider-native fanout is disabled".to_string(),
        coverage: None,
        diagnostics: Vec::new(),
        diagnostics_truncated: false,
        omitted_diagnostic_count: 0,
    }
}

impl SearchProvider for CatalogMetadataProvider {
    fn kind(&self) -> SearchProviderKind {
        SearchProviderKind::CatalogMetadata
    }

    fn search(&self, context: Arc<SearchExecutionContext>) -> ProviderSearchFuture {
        let provider = self.clone();
        Box::pin(async move {
            run_blocking_provider(SearchProviderKind::CatalogMetadata, move || {
                provider.search(&context.request, context.catalog_resolution.as_ref())
            })
            .await
        })
    }
}

impl SearchProvider for ObservedValuesProvider {
    fn kind(&self) -> SearchProviderKind {
        SearchProviderKind::ObservedValues
    }

    fn search(&self, context: Arc<SearchExecutionContext>) -> ProviderSearchFuture {
        let provider = self.clone();
        Box::pin(async move {
            if context.observed_values_policy.is_none() {
                tracing::error!("enabled observed-values provider received no retrieval policy");
                return provider_error_outcome(SearchProviderKind::ObservedValues);
            }
            run_blocking_provider(SearchProviderKind::ObservedValues, move || {
                let policy = context
                    .observed_values_policy
                    .as_ref()
                    .expect("registry enables observed provider only with a retrieval policy");
                provider.search_with_origin(
                    &context.request,
                    policy.as_ref(),
                    context
                        .observed_values_cutoff
                        .as_ref()
                        .map(|_| context.search_origin),
                    context.observed_values_cutoff.as_deref(),
                )
            })
            .await
        })
    }
}

async fn run_blocking_provider<F>(
    provider: SearchProviderKind,
    operation: F,
) -> ProviderSearchOutcome
where
    F: FnOnce() -> ProviderSearchOutcome + Send + 'static,
{
    let span = tracing::Span::current();
    match task::spawn_blocking(move || span.in_scope(operation)).await {
        Ok(outcome) => outcome,
        Err(error) => {
            tracing::error!(?provider, ?error, "Universal Search provider task failed");
            provider_error_outcome(provider)
        }
    }
}

pub(crate) fn provider_error_outcome(provider: SearchProviderKind) -> ProviderSearchOutcome {
    ProviderSearchOutcome {
        candidates: Vec::new(),
        status: ProviderStatus {
            provider,
            state: SearchProviderState::Error,
            note: "search provider failed without affecting other providers".to_string(),
            coverage: None,
            diagnostics: Vec::new(),
            diagnostics_truncated: false,
            omitted_diagnostic_count: 0,
        },
    }
}

fn observed_not_enabled_status() -> ProviderStatus {
    ProviderStatus {
        provider: SearchProviderKind::ObservedValues,
        state: SearchProviderState::NotEnabled,
        note: "observed value search is disabled; enable `observed_values_search` to include values from earlier queries".to_string(),
        coverage: None,
        diagnostics: Vec::new(),
        diagnostics_truncated: false,
        omitted_diagnostic_count: 0,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc as std_mpsc;
    use std::thread;

    use super::{
        LocalSearchWriteCoordinator, ProviderSearchFuture, SearchExecutionContext, SearchProvider,
        SearchProviderRegistration, local_registrations, provider_error_outcome,
        run_blocking_provider,
    };
    use crate::search::result::{SearchProviderKind, SearchProviderState};
    use crate::workspaces::WorkspaceName;

    #[test]
    fn disabled_observed_search_is_registered_as_a_static_status() {
        let registrations = local_registrations(
            Arc::new(UnusedProvider(SearchProviderKind::CatalogMetadata)),
            None,
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

    #[tokio::test(flavor = "current_thread")]
    async fn local_provider_work_runs_off_the_async_worker() {
        let async_thread = thread::current().id();
        let (thread_tx, thread_rx) = std_mpsc::sync_channel(1);
        let outcome = run_blocking_provider(SearchProviderKind::CatalogMetadata, move || {
            thread_tx
                .send(thread::current().id())
                .expect("record blocking thread");
            provider_error_outcome(SearchProviderKind::CatalogMetadata)
        })
        .await;

        assert_ne!(
            thread_rx.recv().expect("blocking thread id"),
            async_thread,
            "provider work must leave the async worker thread"
        );
        assert_eq!(outcome.status.provider, SearchProviderKind::CatalogMetadata);
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

        fn search(&self, _context: Arc<SearchExecutionContext>) -> ProviderSearchFuture {
            panic!("static registration must not invoke disabled provider")
        }
    }
}
