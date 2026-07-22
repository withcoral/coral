//! Provider-facing Universal Search contracts and ordered registration.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use tokio::task;

use crate::bootstrap::AppError;
use crate::catalog::model::CatalogResolution;
use crate::query::manager::QueryManagerError;
use crate::search::catalog::provider::CatalogMetadataProvider;
use crate::search::observed::ObservedValuesRetrievalPolicy;
use crate::search::observed::provider::ObservedValuesProvider;
use crate::search::result::{
    ProviderStatus, SearchCandidate, SearchProviderKind, SearchProviderState, SearchRequest,
};
use crate::workspaces::WorkspaceLifecycleReadLease;

pub(crate) type ProviderSearchFuture =
    Pin<Box<dyn Future<Output = ProviderSearchOutcome> + Send + 'static>>;

#[derive(Debug, Clone)]
pub(crate) struct ProviderSearchOutcome {
    pub(crate) candidates: Vec<SearchCandidate>,
    pub(crate) status: ProviderStatus,
}

pub(crate) enum ObservedValuesPolicyInput {
    Disabled,
    Enabled(Result<ObservedValuesRetrievalPolicy, AppError>),
}

pub(crate) struct SearchExecutionContext {
    pub(crate) request_started_at: Instant,
    // Keep the workspace lifecycle read lease attached to every provider task.
    // Search request cancellation detaches spawned provider work, and blocking
    // provider tasks cannot be cancelled once started.
    _lifecycle_lease: WorkspaceLifecycleReadLease,
    pub(crate) request: SearchRequest,
    pub(crate) catalog_resolution: Result<CatalogResolution, QueryManagerError>,
    pub(crate) observed_values_policy: ObservedValuesPolicyInput,
}

impl SearchExecutionContext {
    pub(crate) fn new(
        request_started_at: Instant,
        lifecycle_lease: WorkspaceLifecycleReadLease,
        request: SearchRequest,
        catalog_resolution: Result<CatalogResolution, QueryManagerError>,
        observed_values_policy: ObservedValuesPolicyInput,
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
        observed: ObservedValuesProvider,
    ) -> Self {
        let ordered = vec![
            SearchProviderRegistration::Provider(Arc::new(catalog)),
            SearchProviderRegistration::Provider(Arc::new(observed)),
            SearchProviderRegistration::StaticStatus(native_not_enabled_status()),
        ];
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

fn native_not_enabled_status() -> ProviderStatus {
    ProviderStatus {
        provider: SearchProviderKind::NativeFanout,
        state: SearchProviderState::NotEnabled,
        note: "provider-native fanout is disabled".to_string(),
        coverage: None,
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
            if matches!(
                &context.observed_values_policy,
                ObservedValuesPolicyInput::Disabled
            ) {
                return observed_not_enabled_outcome();
            }
            run_blocking_provider(SearchProviderKind::ObservedValues, move || {
                match &context.observed_values_policy {
                    ObservedValuesPolicyInput::Enabled(policy) => {
                        provider.search(&context.request, policy.as_ref())
                    }
                    ObservedValuesPolicyInput::Disabled => {
                        unreachable!("disabled observed search returns before blocking offload")
                    }
                }
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
        },
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

#[cfg(test)]
mod tests {
    use std::thread;

    use super::{observed_not_enabled_outcome, provider_error_outcome, run_blocking_provider};
    use crate::search::result::{SearchProviderKind, SearchProviderState};

    #[test]
    fn disabled_observed_search_reports_not_enabled_without_candidates_or_coverage() {
        let outcome = observed_not_enabled_outcome();

        assert!(outcome.candidates.is_empty());
        assert_eq!(outcome.status.provider, SearchProviderKind::ObservedValues);
        assert_eq!(outcome.status.state, SearchProviderState::NotEnabled);
        assert!(outcome.status.coverage.is_none());
        assert!(outcome.status.note.contains("`observed_values_search`"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn local_provider_work_runs_off_the_async_worker() {
        let async_thread = thread::current().id();
        let outcome = run_blocking_provider(SearchProviderKind::CatalogMetadata, move || {
            assert_ne!(thread::current().id(), async_thread);
            provider_error_outcome(SearchProviderKind::CatalogMetadata)
        })
        .await;

        assert_eq!(outcome.status.provider, SearchProviderKind::CatalogMetadata);
    }
}
