//! App-level Universal Search manager.

use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use opentelemetry::trace::Status as OtelStatus;
use tokio::task;
use tracing::{Instrument as _, field};
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

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
use crate::state::db::CoralDb;
use crate::state::{AppStateLayout, ConfigStore};
use crate::task::id::TaskId;
use crate::telemetry::{app_error_type, record_local_only_span_attribute};
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
const SEARCH_TELEMETRY_ERROR_MESSAGE: &str = "Search operation failed";
const SEARCH_MAINTENANCE_TELEMETRY_ERROR_MESSAGE: &str = "Search maintenance operation failed";
const SEARCH_MAINTENANCE_PROVIDER_FAILURE_ERROR_TYPE: &str = "PROVIDER_FAILURE";
const REBUILD_SEARCH_INDEX_OPERATION: &str = "rebuild_search_index";
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
        db: Arc<CoralDb>,
        observed_values_search_enabled: bool,
        catalog_discovery: CatalogDiscovery,
        lifecycle_lock: WorkspaceLifecycleLock,
    ) -> Self {
        Self::with_diagnostic_reporter(
            layout,
            config_store,
            workspace_manager,
            db,
            observed_values_search_enabled,
            SourceDiagnosticReporter::default(),
            catalog_discovery,
            lifecycle_lock,
        )
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "database handle joins the existing collaborator set the search manager owns"
    )]
    pub(crate) fn with_diagnostic_reporter(
        layout: AppStateLayout,
        config_store: &ConfigStore,
        workspace_manager: WorkspaceManager,
        db: Arc<CoralDb>,
        observed_values_search_enabled: bool,
        diagnostic_reporter: SourceDiagnosticReporter,
        catalog_discovery: CatalogDiscovery,
        lifecycle_lock: WorkspaceLifecycleLock,
    ) -> Self {
        let write_coordinator = LocalSearchWriteCoordinator::default();
        let catalog = CatalogMetadataProvider::with_write_coordinator(
            layout.clone(),
            write_coordinator.clone(),
        );
        let observed =
            ObservedValuesProvider::with_write_coordinator(layout.clone(), write_coordinator);
        let observed_scope_loader = ObservedValuesLiveScopeLoader::new(
            layout.clone(),
            config_store.clone(),
            db,
            diagnostic_reporter,
        );
        Self {
            catalog_discovery,
            catalog: catalog.clone(),
            observed: observed.clone(),
            observed_scope_loader,
            observed_values_search_enabled,
            engine: UniversalSearchEngine::new(SearchProviderRegistry::local(
                catalog,
                observed_values_search_enabled.then(|| observed.clone()),
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
        // The retry/preload path makes this future large enough to trigger
        // Clippy's `large_futures` lint when it is awaited inline.
        Box::pin(run_search_operation(
            request,
            attribution.task_id.as_ref(),
            async {
                let request_started_at = Instant::now();
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
                    let observed_values_policy = if self.observed_values_search_enabled {
                        Some(
                            self.observed_retrieval_policy(&request.workspace_name)
                                .await,
                        )
                    } else {
                        None
                    };
                    let context = SearchExecutionContext::new(
                        request_started_at,
                        lifecycle_lease,
                        request.clone(),
                        resolution,
                        observed_values_policy,
                    );
                    return Ok(self.engine.search(context).await);
                }
                Err(workspace_changed_error("searching"))
            },
        ))
        .await
    }

    pub(crate) async fn rebuild_index(
        &self,
        request: &RebuildSearchIndexRequest,
    ) -> Result<RebuildSearchIndexResponse, SearchManagerError> {
        Box::pin(run_search_maintenance_operation(
            &request.workspace_name,
            REBUILD_SEARCH_INDEX_OPERATION,
            self.rebuild_index_inner(request),
        ))
        .await
    }

    async fn rebuild_index_inner(
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
            let observed_policy = match request.provider {
                SearchIndexProvider::ObservedValues | SearchIndexProvider::All
                    if self.observed_values_search_enabled =>
                {
                    Some(
                        self.observed_retrieval_policy(&request.workspace_name)
                            .await,
                    )
                }
                SearchIndexProvider::Catalog
                | SearchIndexProvider::ObservedValues
                | SearchIndexProvider::All => None,
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
                        vec![search.rebuild_observed_index(
                            &request,
                            observed_policy.as_ref().map(Result::as_ref),
                        )]
                    }
                    SearchIndexProvider::All => vec![
                        search.rebuild_catalog_index(
                            &request,
                            resolution
                                .as_ref()
                                .expect("catalog rebuild preloads the catalog resolution"),
                        )?,
                        search.rebuild_observed_index(
                            &request,
                            observed_policy.as_ref().map(Result::as_ref),
                        ),
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
                SearchClearTarget::Source(source_name) => {
                    self.clear_source_all(&request.workspace_name, source_name)
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
        source_name: &crate::sources::SourceName,
    ) -> Result<ClearSearchDataResponse, SearchManagerError> {
        let store = SqliteSearchStore::open_workspace(&self.layout, workspace_name)
            .map_err(|error| search_clear_sqlite_app_error(&error))?;
        let (catalog, observed) = store
            .clear_source_all(source_name.as_str())
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
        observed_policy: Option<Result<&ObservedValuesRetrievalPolicy, &AppError>>,
    ) -> SearchMaintenanceResult {
        if !self.observed_values_search_enabled {
            return observed_values_search_disabled_maintenance_result();
        }
        let policy = match observed_policy {
            Some(Ok(policy)) => policy,
            Some(Err(error)) => return observed_rebuild_error_provider_result(error),
            None => {
                return observed_rebuild_error_provider_result(&AppError::Internal(
                    "observed-value retrieval policy was not loaded for rebuild".to_string(),
                ));
            }
        };
        match self.try_rebuild_observed_index(request, policy) {
            Ok(result) => result,
            Err(SearchManagerError::App(error)) => observed_rebuild_error_provider_result(&error),
        }
    }

    fn try_rebuild_observed_index(
        &self,
        request: &RebuildSearchIndexRequest,
        policy: &ObservedValuesRetrievalPolicy,
    ) -> Result<SearchMaintenanceResult, SearchManagerError> {
        self.observed.rebuild_index(
            SearchProviderRebuildRequest {
                workspace_name: &request.workspace_name,
            },
            policy,
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

    async fn observed_retrieval_policy(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<ObservedValuesRetrievalPolicy, AppError> {
        let load = self.observed_scope_loader.load(workspace_name).await?;
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

async fn run_search_maintenance_operation<F>(
    workspace_name: &WorkspaceName,
    operation_name: &'static str,
    operation: F,
) -> Result<RebuildSearchIndexResponse, SearchManagerError>
where
    F: Future<Output = Result<RebuildSearchIndexResponse, SearchManagerError>>,
{
    let span = tracing::info_span!(
        "coral.search.maintenance",
        coral.stream.entry = true,
        coral.stream.kind = coral_telemetry::QUERY_STREAM_KIND_OTHER,
        coral.stream.name = operation_name,
        otel.name = "coral.search.maintenance",
        operation = operation_name,
        workspace = field::Empty,
        status = field::Empty,
        error.type = field::Empty,
        exception.message = field::Empty,
    );
    span.record(
        coral_telemetry::WORKSPACE_SPAN_ATTRIBUTE,
        workspace_name.as_str(),
    );
    let result = operation.instrument(span.clone()).await;
    match &result {
        Ok(response) => {
            if response
                .results
                .iter()
                .any(|result| result.state == SearchMaintenanceState::Failed)
            {
                coral_telemetry::record_failure(
                    &span,
                    SEARCH_MAINTENANCE_PROVIDER_FAILURE_ERROR_TYPE,
                    SEARCH_MAINTENANCE_TELEMETRY_ERROR_MESSAGE,
                );
            } else {
                span.record("status", "ok");
                span.set_status(OtelStatus::Ok);
            }
        }
        Err(SearchManagerError::App(error)) => {
            coral_telemetry::record_failure(
                &span,
                app_error_type(error),
                SEARCH_MAINTENANCE_TELEMETRY_ERROR_MESSAGE,
            );
        }
    }
    drop(span);
    result
}

async fn run_search_operation<F>(
    request: &SearchRequest,
    task_id: Option<&TaskId>,
    operation: F,
) -> Result<SearchResponse, SearchManagerError>
where
    F: Future<Output = Result<SearchResponse, SearchManagerError>>,
{
    let span = create_search_span(request, task_id);
    let result = operation.instrument(span.clone()).await;
    match &result {
        Ok(response) => {
            span.record(
                "result_count",
                u64::try_from(response.results.len()).unwrap_or(u64::MAX),
            );
            span.record("status", "ok");
            span.set_status(OtelStatus::Ok);
        }
        Err(SearchManagerError::App(error)) => {
            coral_telemetry::record_failure(
                &span,
                app_error_type(error),
                SEARCH_TELEMETRY_ERROR_MESSAGE,
            );
        }
    }
    result
}

fn create_search_span(request: &SearchRequest, task_id: Option<&TaskId>) -> tracing::Span {
    let span = tracing::info_span!(
        "coral.search",
        coral.stream.entry = true,
        coral.stream.kind = coral_telemetry::QUERY_STREAM_KIND_SEARCH,
        coral.stream.name = "search",
        otel.name = "coral.search",
        operation = "search",
        workspace = field::Empty,
        query_len_bytes = request.query.len(),
        limit = request.limit,
        task.id = field::Empty,
        result_count = field::Empty,
        status = field::Empty,
        error.type = field::Empty,
        exception.message = field::Empty,
    );
    record_local_only_span_attribute(
        &span,
        coral_telemetry::QUERY_STREAM_SEARCH_QUERY_ATTRIBUTE,
        request.query.as_str(),
    );
    span.record(
        coral_telemetry::WORKSPACE_SPAN_ATTRIBUTE,
        request.workspace_name.as_str(),
    );
    if let Some(task_id) = task_id {
        span.record("task.id", field::display(task_id));
    }
    span
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

fn observed_rebuild_error_provider_result(error: &AppError) -> SearchMaintenanceResult {
    SearchMaintenanceResult {
        provider: SearchProviderKind::ObservedValues,
        state: SearchMaintenanceState::Failed,
        note: format!("observed-value search index rebuild failed: {error}"),
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

#[cfg(test)]
mod tests {
    use opentelemetry::trace::{Status as OtelStatus, TracerProvider as _};
    use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
    use tracing::Instrument as _;
    use tracing_subscriber::layer::SubscriberExt as _;

    use super::{
        REBUILD_SEARCH_INDEX_OPERATION, SEARCH_MAINTENANCE_PROVIDER_FAILURE_ERROR_TYPE,
        SEARCH_MAINTENANCE_TELEMETRY_ERROR_MESSAGE, SEARCH_TELEMETRY_ERROR_MESSAGE,
        run_search_maintenance_operation, run_search_operation,
    };
    use crate::bootstrap::AppError;
    use crate::search::maintenance::{
        RebuildSearchIndexResponse, SearchMaintenanceResult, SearchMaintenanceState,
    };
    use crate::search::result::{
        SearchManagerError, SearchProviderKind, SearchRequest, SearchResponse, SearchTruncation,
    };
    use crate::task::id::TaskId;
    use crate::workspaces::WorkspaceName;

    #[tokio::test(flavor = "current_thread")]
    async fn search_maintenance_operation_marks_the_outer_query_stream_entry() {
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let tracer = provider.tracer("search-maintenance-summary-test");
        let subscriber = tracing_subscriber::Registry::default()
            .with(tracing_opentelemetry::layer().with_tracer(tracer));
        let _guard = tracing::subscriber::set_default(subscriber);

        run_search_maintenance_operation(
            &WorkspaceName::default(),
            REBUILD_SEARCH_INDEX_OPERATION,
            async {
                async {}
                    .instrument(tracing::info_span!(
                        "coral.query",
                        coral.stream.entry = true,
                        coral.stream.kind = coral_telemetry::QUERY_STREAM_KIND_QUERY,
                        coral.stream.name = "LIST CATALOG",
                    ))
                    .await;
                Ok::<_, SearchManagerError>(RebuildSearchIndexResponse {
                    results: vec![SearchMaintenanceResult {
                        provider: SearchProviderKind::ObservedValues,
                        state: SearchMaintenanceState::Partial,
                        note: "maintenance partially completed".to_string(),
                        detail: None,
                    }],
                })
            },
        )
        .await
        .expect("search maintenance operation");

        provider.force_flush().expect("flush spans");
        let spans = exporter.get_finished_spans().expect("finished spans");
        let maintenance_span = spans
            .iter()
            .find(|span| span.name == "coral.search.maintenance")
            .expect("search maintenance span recorded");
        let query_span = spans
            .iter()
            .find(|span| span.name == "coral.query")
            .expect("nested query span recorded");
        assert!(query_span.attributes.iter().any(|attribute| {
            attribute.key.as_str() == coral_telemetry::QUERY_STREAM_ENTRY_ATTRIBUTE
                && attribute.value == opentelemetry::Value::Bool(true)
        }));
        let attribute = |name: &str| {
            maintenance_span
                .attributes
                .iter()
                .find(|attribute| attribute.key.as_str() == name)
                .unwrap_or_else(|| panic!("missing {name} attribute"))
        };

        assert_eq!(
            attribute(coral_telemetry::QUERY_STREAM_ENTRY_ATTRIBUTE).value,
            opentelemetry::Value::Bool(true)
        );
        assert_eq!(
            attribute(coral_telemetry::QUERY_STREAM_KIND_ATTRIBUTE)
                .value
                .as_str(),
            coral_telemetry::QUERY_STREAM_KIND_OTHER
        );
        assert_eq!(
            attribute(coral_telemetry::QUERY_STREAM_NAME_ATTRIBUTE)
                .value
                .as_str(),
            REBUILD_SEARCH_INDEX_OPERATION
        );
        assert_eq!(attribute("workspace").value.as_str(), "default");
        assert_eq!(attribute("status").value.as_str(), "ok");
        assert_eq!(
            query_span.parent_span_id,
            maintenance_span.span_context.span_id()
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn search_maintenance_operation_records_failed_provider_as_an_error() {
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let tracer = provider.tracer("search-maintenance-provider-failure-test");
        let subscriber = tracing_subscriber::Registry::default()
            .with(tracing_opentelemetry::layer().with_tracer(tracer));
        let _guard = tracing::subscriber::set_default(subscriber);
        let failure_detail = "SENSITIVE_SEARCH_MAINTENANCE_FAILURE";

        let response = run_search_maintenance_operation(
            &WorkspaceName::default(),
            REBUILD_SEARCH_INDEX_OPERATION,
            async {
                Ok::<_, SearchManagerError>(RebuildSearchIndexResponse {
                    results: vec![SearchMaintenanceResult {
                        provider: SearchProviderKind::ObservedValues,
                        state: SearchMaintenanceState::Failed,
                        note: failure_detail.to_string(),
                        detail: None,
                    }],
                })
            },
        )
        .await
        .expect("maintenance response should preserve provider results");

        let provider_result = response
            .results
            .first()
            .expect("failed provider result preserved");
        assert_eq!(provider_result.state, SearchMaintenanceState::Failed);
        assert_eq!(provider_result.note, failure_detail);

        provider.force_flush().expect("flush spans");
        let spans = exporter.get_finished_spans().expect("finished spans");
        let maintenance_span = spans
            .iter()
            .find(|span| span.name == "coral.search.maintenance")
            .expect("search maintenance span recorded");
        let attribute = |name: &str| {
            maintenance_span
                .attributes
                .iter()
                .find(|attribute| attribute.key.as_str() == name)
                .unwrap_or_else(|| panic!("missing {name} attribute"))
        };

        assert_eq!(attribute("status").value.as_str(), "error");
        assert_eq!(
            attribute("error.type").value.as_str(),
            SEARCH_MAINTENANCE_PROVIDER_FAILURE_ERROR_TYPE
        );
        assert_eq!(
            attribute("exception.message").value.as_str(),
            SEARCH_MAINTENANCE_TELEMETRY_ERROR_MESSAGE
        );
        assert_eq!(
            maintenance_span.status,
            OtelStatus::error(SEARCH_MAINTENANCE_TELEMETRY_ERROR_MESSAGE)
        );
        assert!(!format!("{maintenance_span:?}").contains(failure_detail));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn search_operation_records_safe_summary_metadata() {
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let tracer = provider.tracer("search-summary-test");
        let subscriber = tracing_subscriber::Registry::default()
            .with(tracing_opentelemetry::layer().with_tracer(tracer));
        let _guard = tracing::subscriber::set_default(subscriber);

        let raw_query = "sensitive search marker";
        let request = SearchRequest::new(WorkspaceName::default(), raw_query, 7)
            .expect("valid search request");
        let task_id = TaskId::parse("550e8400-e29b-41d4-a716-446655440000").expect("valid task id");
        let response = SearchResponse {
            results: Vec::new(),
            provider_statuses: Vec::new(),
            truncation: SearchTruncation {
                truncated: false,
                returned_count: 0,
                max_results: request.limit,
                note: "all results returned".to_string(),
            },
        };

        run_search_operation(&request, Some(&task_id), async { Ok(response) })
            .await
            .expect("search operation");

        provider.force_flush().expect("flush spans");
        let spans = exporter.get_finished_spans().expect("finished spans");
        let search_span = spans
            .iter()
            .find(|span| span.name == "coral.search")
            .expect("coral.search span recorded");
        let attribute = |name: &str| {
            search_span
                .attributes
                .iter()
                .find(|attribute| attribute.key.as_str() == name)
                .unwrap_or_else(|| panic!("missing {name} attribute"))
        };

        assert_eq!(attribute("operation").value.as_str(), "search");
        assert_eq!(
            attribute(coral_telemetry::QUERY_STREAM_ENTRY_ATTRIBUTE).value,
            opentelemetry::Value::Bool(true)
        );
        assert_eq!(
            attribute(coral_telemetry::QUERY_STREAM_KIND_ATTRIBUTE)
                .value
                .as_str(),
            coral_telemetry::QUERY_STREAM_KIND_SEARCH
        );
        assert_eq!(
            attribute(coral_telemetry::QUERY_STREAM_NAME_ATTRIBUTE)
                .value
                .as_str(),
            "search"
        );
        assert_eq!(attribute("workspace").value.as_str(), "default");
        assert_eq!(
            attribute("task.id").value.as_str(),
            "550e8400-e29b-41d4-a716-446655440000"
        );
        assert_eq!(attribute("status").value.as_str(), "ok");
        assert!(
            search_span
                .attributes
                .iter()
                .any(|attribute| attribute.key.as_str() == "query_len_bytes")
        );
        assert!(
            search_span
                .attributes
                .iter()
                .any(|attribute| attribute.key.as_str() == "limit")
        );
        assert!(
            search_span
                .attributes
                .iter()
                .any(|attribute| attribute.key.as_str() == "result_count")
        );
        assert!(
            search_span.attributes.iter().all(|attribute| {
                !attribute
                    .key
                    .as_str()
                    .starts_with(coral_telemetry::LOCAL_ONLY_SPAN_ATTRIBUTE_PREFIX)
            }),
            "a subscriber not installed by Coral must not receive local-only attributes"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn search_operation_redacts_caller_visible_error_details_from_telemetry() {
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let tracer = provider.tracer("search-error-privacy-test");
        let subscriber = tracing_subscriber::Registry::default()
            .with(tracing_opentelemetry::layer().with_tracer(tracer));
        let _guard = tracing::subscriber::set_default(subscriber);

        let query = "local search text";
        let error_sentinel = "SENSITIVE_SEARCH_ERROR_PATH_MARKER";
        let request =
            SearchRequest::new(WorkspaceName::default(), query, 7).expect("valid search request");
        let error = run_search_operation(&request, None, async {
            Err::<SearchResponse, SearchManagerError>(
                AppError::Internal(format!("failed while handling {error_sentinel}")).into(),
            )
        })
        .await
        .expect_err("search operation should return its detailed error");

        let SearchManagerError::App(app_error) = &error;
        assert!(app_error.to_string().contains(error_sentinel));

        provider.force_flush().expect("flush spans");
        let spans = exporter.get_finished_spans().expect("finished spans");
        let search_span = spans
            .iter()
            .find(|span| span.name == "coral.search")
            .expect("coral.search span recorded");
        let attribute = |name: &str| {
            search_span
                .attributes
                .iter()
                .find(|attribute| attribute.key.as_str() == name)
                .unwrap_or_else(|| panic!("missing {name} attribute"))
        };

        assert_eq!(attribute("status").value.as_str(), "error");
        assert_eq!(attribute("error.type").value.as_str(), "INTERNAL");
        assert_eq!(
            attribute("exception.message").value.as_str(),
            SEARCH_TELEMETRY_ERROR_MESSAGE
        );
        assert_eq!(
            search_span.status,
            OtelStatus::error(SEARCH_TELEMETRY_ERROR_MESSAGE)
        );
        assert!(!format!("{search_span:?}").contains(query));
        assert!(!format!("{search_span:?}").contains(error_sentinel));
    }
}
