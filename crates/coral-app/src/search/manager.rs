//! App-level Universal Search manager.

use std::time::Duration;

use chrono::Utc;
use tokio::task;
use tokio::time::Instant;

use crate::bootstrap::AppError;
use crate::catalog::discovery::CatalogDiscovery;
use crate::catalog::model::CatalogResolution;
use crate::query::QueryAttribution;
use crate::query::manager::QueryManagerError;
use crate::search::catalog::provider::{CatalogMetadataProvider, catalog_clear_provider_result};
use crate::search::engine::UniversalSearchEngine;
use crate::search::maintenance::{
    ClearSearchDataRequest, ClearSearchDataResponse, DrainSearchQueueRequest,
    DrainSearchQueueResponse, RebuildSearchIndexRequest, RebuildSearchIndexResponse,
    SearchClearTarget, SearchDataScope, SearchIndexProvider, SearchMaintenanceResult,
    SearchMaintenanceState, SearchProviderClearRequest, SearchProviderRebuildRequest,
    SearchStorageCleanupResult,
};
use crate::search::native::provider::NativeFanoutRegistration;
use crate::search::observed::provider::{ObservedValuesProvider, observed_clear_provider_result};
use crate::search::observed::{
    ObservedValuesDrainBudget, ObservedValuesLiveScopeLoad, ObservedValuesLiveScopeLoader,
    ObservedValuesRetrievalPolicy,
};
use crate::search::provider::{
    LocalSearchWriteCoordinator, SearchExecutionContext, SearchProviderRegistry,
};
use crate::search::result::{
    SearchManagerError, SearchProviderKind, SearchRequest, SearchResponse,
};
use crate::search::sqlite_store::{
    SqliteSearchCompactionResult, SqliteSearchError, SqliteSearchStore,
};
use crate::sources::materialization::SourceDiagnosticReporter;
use crate::state::{AppStateLayout, ConfigStore};
use crate::workspaces::{
    WorkspaceLifecycleLock, WorkspaceLifecycleRevision, WorkspaceManager, WorkspaceName,
};

#[derive(Clone)]
pub(crate) struct SearchManager {
    catalog_discovery: CatalogDiscovery,
    catalog: CatalogMetadataProvider,
    observed: ObservedValuesProvider,
    observed_scope_loader: ObservedValuesLiveScopeLoader,
    observed_values_search_enabled: bool,
    native_fanout_present: bool,
    engine: UniversalSearchEngine,
    workspaces: WorkspaceManager,
    lifecycle_lock: WorkspaceLifecycleLock,
    layout: AppStateLayout,
}

const DEFAULT_MANUAL_DRAIN_BUDGET_MS: u32 = 1_000;
const MAX_MANUAL_DRAIN_BUDGET_MS: u32 = 60_000;
const MANUAL_DRAIN_MAX_JOBS: usize = 10_000;
const SHUTDOWN_DRAIN_SOFT_BUDGET: Duration = Duration::from_secs(1);
const WORKSPACE_SNAPSHOT_ATTEMPTS: usize = 2;
const OBSERVED_STALE_AFTER_LAST_OBSERVED_DAYS: u32 = 365;
const OBSERVED_VALUES_SEARCH_DISABLED_MAINTENANCE_NOTE: &str = "observed value search maintenance is disabled; enable `observed_values_search` to rebuild or drain observed values";

enum CatalogPreload {
    Ready {
        revision: WorkspaceLifecycleRevision,
        resolution: Result<CatalogResolution, QueryManagerError>,
    },
    WorkspaceChanged,
}

impl SearchManager {
    #[cfg(test)]
    pub(crate) fn new(
        layout: AppStateLayout,
        config_store: &ConfigStore,
        workspace_manager: WorkspaceManager,
        observed_values_search_enabled: bool,
        catalog_discovery: CatalogDiscovery,
        lifecycle_lock: WorkspaceLifecycleLock,
        native_fanout: Option<NativeFanoutRegistration>,
    ) -> Self {
        Self::with_diagnostic_reporter(
            layout,
            config_store,
            workspace_manager,
            observed_values_search_enabled,
            SourceDiagnosticReporter::default(),
            catalog_discovery,
            lifecycle_lock,
            native_fanout,
        )
    }

    pub(crate) fn with_diagnostic_reporter(
        layout: AppStateLayout,
        config_store: &ConfigStore,
        workspace_manager: WorkspaceManager,
        observed_values_search_enabled: bool,
        diagnostic_reporter: SourceDiagnosticReporter,
        catalog_discovery: CatalogDiscovery,
        lifecycle_lock: WorkspaceLifecycleLock,
        native_fanout: Option<NativeFanoutRegistration>,
    ) -> Self {
        let write_coordinator = LocalSearchWriteCoordinator::default();
        let catalog = CatalogMetadataProvider::with_write_coordinator(
            layout.clone(),
            write_coordinator.clone(),
        );
        let observed =
            ObservedValuesProvider::with_write_coordinator(layout.clone(), write_coordinator);
        let native_fanout_present = native_fanout.is_some();
        let native_provider = native_fanout.map(|registration| registration.provider);
        let observed_scope_loader = ObservedValuesLiveScopeLoader::new(
            layout.clone(),
            config_store.clone(),
            diagnostic_reporter,
        );
        Self {
            catalog_discovery,
            catalog: catalog.clone(),
            observed: observed.clone(),
            observed_scope_loader,
            observed_values_search_enabled,
            native_fanout_present,
            engine: UniversalSearchEngine::new(SearchProviderRegistry::local(
                catalog,
                observed_values_search_enabled.then(|| observed.clone()),
                native_provider,
            )),
            workspaces: workspace_manager,
            lifecycle_lock,
            layout,
        }
    }

    pub(crate) async fn search(
        &self,
        request: &SearchRequest,
        attribution: &QueryAttribution,
    ) -> Result<SearchResponse, SearchManagerError> {
        let request_started_at = Instant::now();
        let observed_values_cutoff = self
            .native_fanout_present
            .then(|| Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string());
        let search_origin = uuid::Uuid::new_v4();
        for _ in 0..WORKSPACE_SNAPSHOT_ATTEMPTS {
            let CatalogPreload::Ready {
                revision,
                resolution,
            } = self
                .preload_catalog(&request.workspace_name, attribution)
                .await?
            else {
                continue;
            };
            let Some(lifecycle_lease) = self
                .lifecycle_lock
                .read_lease_if_unchanged(revision, &request.workspace_name)
                .await
            else {
                continue;
            };
            let (observed_values_policy, lifecycle_lease) = if self.observed_values_search_enabled {
                let search = self.clone();
                let workspace_name = request.workspace_name.clone();
                run_blocking_search_operation(move || {
                    Ok((
                        Some(search.observed_retrieval_policy(&workspace_name)),
                        lifecycle_lease,
                    ))
                })
                .await?
            } else {
                (None, lifecycle_lease)
            };
            let context = SearchExecutionContext::new(
                request_started_at,
                lifecycle_lease,
                observed_values_cutoff.clone(),
                search_origin,
                request.clone(),
                resolution,
                observed_values_policy,
            );
            return Ok(self.engine.search(context).await);
        }
        Err(workspace_changed_error("searching"))
    }

    pub(crate) async fn rebuild_index(
        &self,
        request: &RebuildSearchIndexRequest,
    ) -> Result<RebuildSearchIndexResponse, SearchManagerError> {
        let attribution = QueryAttribution::default();
        let needs_catalog = matches!(
            request.provider,
            SearchIndexProvider::Catalog | SearchIndexProvider::All
        );
        for _ in 0..WORKSPACE_SNAPSHOT_ATTEMPTS {
            let (revision, resolution) = if needs_catalog {
                let CatalogPreload::Ready {
                    revision,
                    resolution,
                } = self
                    .preload_catalog(&request.workspace_name, &attribution)
                    .await?
                else {
                    continue;
                };
                (revision, Some(resolution))
            } else {
                let Some(revision) = self
                    .lifecycle_lock
                    .revision_if_active_async(&request.workspace_name)
                    .await
                else {
                    continue;
                };
                self.workspaces
                    .require_workspace(&request.workspace_name)
                    .await?;
                (revision, None)
            };
            let Some(lifecycle_lease) = self
                .lifecycle_lock
                .read_lease_if_unchanged(revision, &request.workspace_name)
                .await
            else {
                continue;
            };
            let search = self.clone();
            let request = request.clone();
            let response = run_blocking_search_operation(move || {
                let _lifecycle_lease = lifecycle_lease;
                let resolution = resolution
                    .map(|resolution| resolution.map_err(catalog_resolution_error))
                    .transpose()?;
                let results = match request.provider {
                    SearchIndexProvider::Catalog => vec![
                        search.rebuild_catalog_index(
                            &request,
                            resolution
                                .as_ref()
                                .expect("catalog rebuild preloads the catalog resolution"),
                        )?,
                    ],
                    SearchIndexProvider::ObservedValues => {
                        vec![search.rebuild_observed_index(&request)]
                    }
                    SearchIndexProvider::All => vec![
                        search.rebuild_catalog_index(
                            &request,
                            resolution
                                .as_ref()
                                .expect("catalog rebuild preloads the catalog resolution"),
                        )?,
                        search.rebuild_observed_index(&request),
                    ],
                };
                Ok(RebuildSearchIndexResponse { results })
            })
            .await?;
            return Ok(response);
        }
        Err(workspace_changed_error("rebuilding the search index"))
    }

    fn rebuild_catalog_index(
        &self,
        request: &RebuildSearchIndexRequest,
        resolution: &CatalogResolution,
    ) -> Result<SearchMaintenanceResult, SearchManagerError> {
        self.catalog
            .rebuild_index(&request.workspace_name, resolution, request.force)
    }

    pub(crate) async fn drain_queue(
        &self,
        request: &DrainSearchQueueRequest,
    ) -> Result<DrainSearchQueueResponse, SearchManagerError> {
        self.workspaces
            .require_workspace(&request.workspace_name)
            .await?;
        let search = self.clone();
        let request = request.clone();
        run_blocking_search_operation(move || search.drain_queue_blocking(&request)).await
    }

    fn drain_queue_blocking(
        &self,
        request: &DrainSearchQueueRequest,
    ) -> Result<DrainSearchQueueResponse, SearchManagerError> {
        Ok(DrainSearchQueueResponse {
            results: vec![
                self.drain_observed_queue_with_budget(&request.workspace_name, request.budget_ms)?,
            ],
        })
    }

    pub(crate) async fn clear_data(
        &self,
        request: &ClearSearchDataRequest,
    ) -> Result<ClearSearchDataResponse, SearchManagerError> {
        for _ in 0..WORKSPACE_SNAPSHOT_ATTEMPTS {
            let Some(revision) = self
                .lifecycle_lock
                .revision_if_active_async(&request.workspace_name)
                .await
            else {
                continue;
            };
            self.workspaces
                .require_workspace(&request.workspace_name)
                .await?;
            let Some(lifecycle_lease) = self
                .lifecycle_lock
                .read_lease_if_unchanged(revision, &request.workspace_name)
                .await
            else {
                continue;
            };
            let search = self.clone();
            let request = request.clone();
            let response = run_blocking_search_operation(move || {
                let _lifecycle_lease = lifecycle_lease;
                search.clear_data_blocking(&request)
            })
            .await?;
            return Ok(response);
        }
        Err(workspace_changed_error("clearing search data"))
    }

    fn clear_data_blocking(
        &self,
        request: &ClearSearchDataRequest,
    ) -> Result<ClearSearchDataResponse, SearchManagerError> {
        if request.scope == SearchDataScope::All {
            return match &request.target {
                SearchClearTarget::Workspace => self.clear_workspace_all(&request.workspace_name),
                SearchClearTarget::Source(owner_source_name) => {
                    self.clear_source_all(&request.workspace_name, owner_source_name)
                }
            };
        }
        let provider_outcomes = match request.scope {
            SearchDataScope::ObservedValues => {
                vec![self.observed.clear_data(SearchProviderClearRequest {
                    workspace_name: &request.workspace_name,
                    scope: request.scope,
                    target: &request.target,
                    compact_after_clear: true,
                })?]
            }
            SearchDataScope::All => {
                vec![
                    self.catalog.clear_data(SearchProviderClearRequest {
                        workspace_name: &request.workspace_name,
                        scope: request.scope,
                        target: &request.target,
                        compact_after_clear: false,
                    })?,
                    self.observed.clear_data(SearchProviderClearRequest {
                        workspace_name: &request.workspace_name,
                        scope: request.scope,
                        target: &request.target,
                        compact_after_clear: true,
                    })?,
                ]
            }
        };
        let mut results = Vec::with_capacity(provider_outcomes.len());
        let mut storage_cleanup = None;
        for outcome in provider_outcomes {
            results.push(outcome.result);
            if let Some(cleanup) = outcome.storage_cleanup
                && storage_cleanup.replace(cleanup).is_some()
            {
                return Err(AppError::Internal(
                    "multiple providers attempted shared search storage cleanup".to_string(),
                )
                .into());
            }
        }
        let storage_cleanup = storage_cleanup.ok_or_else(|| {
            AppError::Internal("no provider performed shared search storage cleanup".to_string())
        })?;
        Ok(ClearSearchDataResponse {
            results,
            storage_cleanup,
        })
    }

    fn clear_source_all(
        &self,
        workspace_name: &WorkspaceName,
        owner_source_name: &crate::sources::SourceName,
    ) -> Result<ClearSearchDataResponse, SearchManagerError> {
        let store = SqliteSearchStore::open_workspace(&self.layout, workspace_name)
            .map_err(|error| search_clear_sqlite_app_error(&error))?;
        let (catalog, observed) = store
            .clear_source_all(owner_source_name.as_str())
            .map_err(|error| search_clear_sqlite_app_error(&error))?;
        let compaction = store.compact_after_clear();
        Ok(ClearSearchDataResponse {
            results: vec![
                catalog_clear_provider_result(catalog.deleted_document_count),
                observed_clear_provider_result(observed),
            ],
            storage_cleanup: search_storage_cleanup_result(&compaction),
        })
    }

    fn clear_workspace_all(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<ClearSearchDataResponse, SearchManagerError> {
        let store = SqliteSearchStore::open_workspace(&self.layout, workspace_name)
            .map_err(|error| search_clear_sqlite_app_error(&error))?;
        let (catalog, observed) = store
            .clear_workspace_all()
            .map_err(|error| search_clear_sqlite_app_error(&error))?;
        let compaction = store.compact_after_clear();
        Ok(ClearSearchDataResponse {
            results: vec![
                catalog_clear_provider_result(catalog.deleted_document_count),
                observed_clear_provider_result(observed),
            ],
            storage_cleanup: search_storage_cleanup_result(&compaction),
        })
    }

    pub(crate) async fn drain_before_shutdown(&self) -> Result<(), SearchManagerError> {
        if !self.observed_values_search_enabled {
            return Ok(());
        }
        let workspaces = self.workspaces.list_workspaces().await?;
        let observed = self.observed.clone();
        run_blocking_search_operation(move || {
            let deadline = Instant::now() + SHUTDOWN_DRAIN_SOFT_BUDGET;
            for workspace in workspaces {
                let remaining_budget = deadline.saturating_duration_since(Instant::now());
                if remaining_budget.is_zero() {
                    tracing::debug!(
                        workspace = %workspace.name,
                        "skipping observed-value shutdown drain because budget expired"
                    );
                    break;
                }
                match observed.drain_queue(
                    &workspace.name,
                    ObservedValuesDrainBudget::new(MANUAL_DRAIN_MAX_JOBS, remaining_budget),
                ) {
                    Ok(result) => {
                        tracing::debug!(
                            workspace = %workspace.name,
                            state = ?result.state,
                            note = %result.note,
                            remaining_soft_budget_ms = remaining_budget.as_millis(),
                            "drained observed-value queue before shutdown"
                        );
                    }
                    Err(error) => {
                        tracing::debug!(
                            workspace = %workspace.name,
                            error = ?error,
                            "failed to drain observed-value queue before shutdown"
                        );
                    }
                }
            }
            Ok(())
        })
        .await
    }

    async fn preload_catalog(
        &self,
        workspace_name: &WorkspaceName,
        attribution: &QueryAttribution,
    ) -> Result<CatalogPreload, SearchManagerError> {
        let Some(revision) = self
            .lifecycle_lock
            .revision_if_active_async(workspace_name)
            .await
        else {
            return Ok(CatalogPreload::WorkspaceChanged);
        };
        self.workspaces.require_workspace(workspace_name).await?;
        let resolution = self
            .catalog_discovery
            .resolve_catalog(workspace_name, attribution)
            .await;
        self.workspaces.require_workspace(workspace_name).await?;
        Ok(CatalogPreload::Ready {
            revision,
            resolution,
        })
    }

    fn rebuild_observed_index(
        &self,
        request: &RebuildSearchIndexRequest,
    ) -> SearchMaintenanceResult {
        if !self.observed_values_search_enabled {
            return observed_values_search_disabled_maintenance_result();
        }
        match self.try_rebuild_observed_index(request) {
            Ok(result) => result,
            Err(error) => observed_rebuild_error_provider_result(&error),
        }
    }

    fn try_rebuild_observed_index(
        &self,
        request: &RebuildSearchIndexRequest,
    ) -> Result<SearchMaintenanceResult, SearchManagerError> {
        let policy = self.observed_retrieval_policy(&request.workspace_name)?;
        self.observed.rebuild_index(
            SearchProviderRebuildRequest {
                workspace_name: &request.workspace_name,
            },
            &policy,
        )
    }

    fn drain_observed_queue_with_budget(
        &self,
        workspace_name: &WorkspaceName,
        budget_ms: u32,
    ) -> Result<SearchMaintenanceResult, SearchManagerError> {
        let budget_ms = manual_drain_budget_ms(budget_ms)?;
        if !self.observed_values_search_enabled {
            return Ok(observed_values_search_disabled_maintenance_result());
        }
        self.observed.drain_queue(
            workspace_name,
            ObservedValuesDrainBudget::new(
                MANUAL_DRAIN_MAX_JOBS,
                Duration::from_millis(u64::from(budget_ms)),
            ),
        )
    }

    fn observed_retrieval_policy(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<ObservedValuesRetrievalPolicy, AppError> {
        let load = self.observed_scope_loader.load(workspace_name)?;
        Ok(observed_retrieval_policy_from_load(
            load,
            OBSERVED_STALE_AFTER_LAST_OBSERVED_DAYS,
        ))
    }
}

fn catalog_resolution_error(error: QueryManagerError) -> SearchManagerError {
    match error {
        QueryManagerError::App(error) => error.into(),
        QueryManagerError::Core(error) => {
            AppError::Internal(format!("workspace catalog resolution failed: {error}")).into()
        }
    }
}

fn workspace_changed_error(operation: &str) -> SearchManagerError {
    AppError::FailedPrecondition(format!(
        "workspace changed repeatedly while {operation}; retry the request"
    ))
    .into()
}

async fn run_blocking_search_operation<T, F>(operation: F) -> Result<T, SearchManagerError>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, SearchManagerError> + Send + 'static,
{
    let span = tracing::Span::current();
    task::spawn_blocking(move || span.in_scope(operation))
        .await
        .map_err(AppError::from)?
}

fn manual_drain_budget_ms(requested_budget_ms: u32) -> Result<u32, SearchManagerError> {
    let budget_ms = if requested_budget_ms == 0 {
        DEFAULT_MANUAL_DRAIN_BUDGET_MS
    } else {
        requested_budget_ms
    };
    if budget_ms > MAX_MANUAL_DRAIN_BUDGET_MS {
        return Err(AppError::InvalidInput(format!(
            "search queue drain budget must be at most {MAX_MANUAL_DRAIN_BUDGET_MS}ms"
        ))
        .into());
    }
    Ok(budget_ms)
}

fn observed_retrieval_policy_from_load(
    load: ObservedValuesLiveScopeLoad,
    stale_after_last_observed_days: u32,
) -> ObservedValuesRetrievalPolicy {
    ObservedValuesRetrievalPolicy::with_load_failures(
        load.live_scopes,
        load.failed_sources,
        stale_after_last_observed_days,
    )
}

fn observed_rebuild_error_provider_result(error: &SearchManagerError) -> SearchMaintenanceResult {
    SearchMaintenanceResult {
        provider: SearchProviderKind::ObservedValues,
        state: SearchMaintenanceState::Failed,
        note: format!(
            "observed-value search index rebuild failed: {}",
            search_manager_error_message(error)
        ),
        detail: None,
    }
}

fn observed_values_search_disabled_maintenance_result() -> SearchMaintenanceResult {
    SearchMaintenanceResult {
        provider: SearchProviderKind::ObservedValues,
        state: SearchMaintenanceState::Skipped,
        note: OBSERVED_VALUES_SEARCH_DISABLED_MAINTENANCE_NOTE.to_string(),
        detail: None,
    }
}

fn search_manager_error_message(error: &SearchManagerError) -> String {
    match error {
        SearchManagerError::App(error) => error.to_string(),
    }
}

fn search_clear_sqlite_app_error(error: &SqliteSearchError) -> AppError {
    if error.is_lock_contention() {
        AppError::Unavailable(format!("search maintenance storage is busy: {error}"))
    } else if error.is_storage_exhaustion() {
        AppError::ResourceExhausted(format!("search maintenance storage is exhausted: {error}"))
    } else if matches!(
        error,
        SqliteSearchError::UnsupportedCapability { .. }
            | SqliteSearchError::UnsupportedSchemaVersion { .. }
    ) {
        AppError::FailedPrecondition(format!("search maintenance is not supported: {error}"))
    } else {
        AppError::Internal(format!("search maintenance storage failed: {error}"))
    }
}

fn search_storage_cleanup_result(
    result: &SqliteSearchCompactionResult,
) -> SearchStorageCleanupResult {
    let (state, note) = match (
        result.wal_checkpoint_truncate_completed,
        result.vacuum_completed,
    ) {
        (true, true) => (
            SearchMaintenanceState::Completed,
            "local search storage cleanup completed",
        ),
        (true, false) | (false, true) => (
            SearchMaintenanceState::Partial,
            "local search storage cleanup partially completed",
        ),
        (false, false) => (
            SearchMaintenanceState::Failed,
            "local search storage cleanup did not complete",
        ),
    };
    if state != SearchMaintenanceState::Completed {
        tracing::warn!(
            wal_checkpoint_truncate_completed = result.wal_checkpoint_truncate_completed,
            vacuum_completed = result.vacuum_completed,
            detail = %result.note,
            "local search storage cleanup did not fully complete"
        );
    }
    SearchStorageCleanupResult {
        state,
        note: note.to_string(),
    }
}
