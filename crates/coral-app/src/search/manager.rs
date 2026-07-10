//! App-level Universal Search manager.

use std::time::{Duration, Instant};

use tokio::task;

use crate::bootstrap::AppError;
use crate::query::QueryAttribution;
use crate::search::catalog::local_snapshot::CatalogSnapshotLoader;
use crate::search::catalog::provider::{CatalogMetadataProvider, catalog_clear_provider_result};
use crate::search::engine::UniversalSearchEngine;
use crate::search::maintenance::{
    ClearSearchDataRequest, ClearSearchDataResponse, DrainSearchQueueRequest,
    DrainSearchQueueResponse, RebuildSearchIndexRequest, RebuildSearchIndexResponse,
    SearchClearTarget, SearchDataScope, SearchIndexProvider, SearchMaintenanceResult,
    SearchMaintenanceState, SearchProviderClearRequest, SearchProviderMaintenance,
    SearchProviderRebuildRequest, SearchStorageCleanupResult,
};
use crate::search::observed::provider::{ObservedValuesProvider, observed_clear_provider_result};
use crate::search::observed::{
    ObservedValuesDrainBudget, ObservedValuesLiveScopeLoad, ObservedValuesLiveScopeLoader,
    ObservedValuesRetrievalPolicy,
};
use crate::search::result::{
    SearchManagerError, SearchProviderKind, SearchRequest, SearchResponse,
};
use crate::search::sqlite_store::{
    SqliteSearchCompactionResult, SqliteSearchError, SqliteSearchStore,
};
use crate::state::{AppStateLayout, ConfigStore};
use crate::workspaces::{WorkspaceManager, WorkspaceName};

#[derive(Clone)]
pub(crate) struct SearchManager {
    catalog: CatalogMetadataProvider,
    observed: ObservedValuesProvider,
    observed_scope_loader: ObservedValuesLiveScopeLoader,
    observed_values_search_enabled: bool,
    engine: UniversalSearchEngine,
    workspaces: WorkspaceManager,
    layout: AppStateLayout,
}

const DEFAULT_MANUAL_DRAIN_BUDGET_MS: u32 = 1_000;
const MAX_MANUAL_DRAIN_BUDGET_MS: u32 = 60_000;
const MANUAL_DRAIN_MAX_JOBS: usize = 10_000;
const SHUTDOWN_DRAIN_BUDGET: Duration = Duration::from_secs(1);
const OBSERVED_STALE_AFTER_LAST_OBSERVED_DAYS: u32 = 365;

impl SearchManager {
    pub(crate) fn new(
        layout: AppStateLayout,
        config_store: &ConfigStore,
        workspace_manager: WorkspaceManager,
        observed_values_search_enabled: bool,
    ) -> Self {
        let catalog_loader = CatalogSnapshotLoader::new(config_store.clone(), layout.clone());
        let catalog = CatalogMetadataProvider::new(layout.clone(), catalog_loader);
        let observed = ObservedValuesProvider::new(layout.clone());
        let observed_scope_loader =
            ObservedValuesLiveScopeLoader::new(layout.clone(), config_store.clone());
        Self {
            catalog: catalog.clone(),
            observed: observed.clone(),
            observed_scope_loader,
            observed_values_search_enabled,
            engine: UniversalSearchEngine::new(catalog, observed),
            workspaces: workspace_manager,
            layout,
        }
    }

    pub(crate) async fn search(
        &self,
        request: &SearchRequest,
        attribution: &QueryAttribution,
    ) -> Result<SearchResponse, SearchManagerError> {
        self.workspaces
            .require_workspace(&request.workspace_name)
            .await?;
        let search = self.clone();
        let request = request.clone();
        let attribution = attribution.clone();
        run_blocking_search_operation(move || {
            let observed_policy = search
                .observed_values_search_enabled
                .then(|| search.observed_retrieval_policy(&request.workspace_name));
            Ok(search.engine.search(
                &request,
                &attribution,
                observed_policy.as_ref().map(Result::as_ref),
            ))
        })
        .await
    }

    pub(crate) async fn rebuild_index(
        &self,
        request: &RebuildSearchIndexRequest,
    ) -> Result<RebuildSearchIndexResponse, SearchManagerError> {
        self.workspaces
            .require_workspace(&request.workspace_name)
            .await?;
        let search = self.clone();
        let request = request.clone();
        run_blocking_search_operation(move || search.rebuild_index_blocking(&request)).await
    }

    fn rebuild_index_blocking(
        &self,
        request: &RebuildSearchIndexRequest,
    ) -> Result<RebuildSearchIndexResponse, SearchManagerError> {
        let results = match request.provider {
            SearchIndexProvider::Catalog => vec![self.rebuild_catalog_index(request)?],
            SearchIndexProvider::ObservedValues => vec![self.rebuild_observed_index(request)],
            SearchIndexProvider::All => vec![
                self.rebuild_catalog_index(request)?,
                self.rebuild_observed_index(request),
            ],
        };
        Ok(RebuildSearchIndexResponse { results })
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
        self.workspaces
            .require_workspace(&request.workspace_name)
            .await?;
        let search = self.clone();
        let request = request.clone();
        run_blocking_search_operation(move || search.clear_data_blocking(&request)).await
    }

    fn clear_data_blocking(
        &self,
        request: &ClearSearchDataRequest,
    ) -> Result<ClearSearchDataResponse, SearchManagerError> {
        if request.scope == SearchDataScope::All
            && let SearchClearTarget::Source(source_name) = &request.target
        {
            return self.clear_source_all(&request.workspace_name, source_name);
        }
        let provider_outcomes = match request.scope {
            SearchDataScope::Observed => {
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
            if let Some(cleanup) = outcome.storage_cleanup {
                if storage_cleanup.replace(cleanup).is_some() {
                    return Err(AppError::Internal(
                        "multiple providers attempted shared search storage cleanup".to_string(),
                    )
                    .into());
                }
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
        source_name: &str,
    ) -> Result<ClearSearchDataResponse, SearchManagerError> {
        let store = SqliteSearchStore::open_workspace(&self.layout, workspace_name)
            .map_err(|error| search_clear_sqlite_app_error(&error))?;
        let (catalog, observed) = store
            .clear_source_all(source_name)
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
            let deadline = Instant::now() + SHUTDOWN_DRAIN_BUDGET;
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
                            remaining_budget_ms = remaining_budget.as_millis(),
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

    fn rebuild_catalog_index(
        &self,
        request: &RebuildSearchIndexRequest,
    ) -> Result<SearchMaintenanceResult, SearchManagerError> {
        self.catalog.rebuild_index(SearchProviderRebuildRequest {
            workspace_name: &request.workspace_name,
            force: request.force,
        })
    }

    fn rebuild_observed_index(
        &self,
        request: &RebuildSearchIndexRequest,
    ) -> SearchMaintenanceResult {
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
                force: request.force,
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
