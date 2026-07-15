//! App-level Universal Search manager.

use std::time::{Duration, Instant};

use tokio::task;

use crate::bootstrap::AppError;
use crate::query::QueryAttribution;
use crate::search::catalog::local_snapshot::CatalogSnapshotLoader;
use crate::search::catalog::provider::CatalogMetadataProvider;
use crate::search::engine::UniversalSearchEngine;
use crate::search::maintenance::{
    ClearSearchDataRequest, ClearSearchDataResponse, RebuildSearchIndexRequest,
    RebuildSearchIndexResponse, SearchMaintenanceResult, SearchProviderClearRequest,
    SearchProviderMaintenance, SearchProviderRebuildRequest,
};
use crate::search::observed::provider::ObservedValuesProvider;
use crate::search::observed::{
    ObservedValuesDrainBudget, ObservedValuesLiveScopeLoad, ObservedValuesLiveScopeLoader,
    ObservedValuesRetrievalPolicy,
};
use crate::search::result::{SearchManagerError, SearchRequest, SearchResponse};
use crate::sources::materialization::SourceDiagnosticReporter;
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
}

const MANUAL_DRAIN_MAX_JOBS: usize = 10_000;
const SHUTDOWN_DRAIN_SOFT_BUDGET: Duration = Duration::from_secs(1);
const OBSERVED_STALE_AFTER_LAST_OBSERVED_DAYS: u32 = 365;

impl SearchManager {
    #[cfg(test)]
    pub(crate) fn new(
        layout: AppStateLayout,
        config_store: &ConfigStore,
        workspace_manager: WorkspaceManager,
        observed_values_search_enabled: bool,
    ) -> Self {
        Self::with_diagnostic_reporter(
            layout,
            config_store,
            workspace_manager,
            observed_values_search_enabled,
            SourceDiagnosticReporter::default(),
        )
    }

    pub(crate) fn with_diagnostic_reporter(
        layout: AppStateLayout,
        config_store: &ConfigStore,
        workspace_manager: WorkspaceManager,
        observed_values_search_enabled: bool,
        diagnostic_reporter: SourceDiagnosticReporter,
    ) -> Self {
        let catalog_loader = CatalogSnapshotLoader::with_diagnostic_reporter(
            config_store.clone(),
            layout.clone(),
            diagnostic_reporter.clone(),
        );
        let catalog = CatalogMetadataProvider::new(layout.clone(), catalog_loader);
        let observed = ObservedValuesProvider::new(layout.clone());
        let observed_scope_loader =
            ObservedValuesLiveScopeLoader::new(layout, config_store.clone(), diagnostic_reporter);
        Self {
            catalog: catalog.clone(),
            observed: observed.clone(),
            observed_scope_loader,
            observed_values_search_enabled,
            engine: UniversalSearchEngine::new(catalog, observed),
            workspaces: workspace_manager,
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
        let result = self.rebuild_catalog_index(request)?;
        Ok(RebuildSearchIndexResponse {
            results: vec![result],
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
        let outcome = self.catalog.clear_data(SearchProviderClearRequest {
            workspace_name: &request.workspace_name,
            scope: request.scope,
            target: &request.target,
        })?;
        Ok(ClearSearchDataResponse {
            results: vec![outcome.result],
            storage_cleanup: outcome.storage_cleanup,
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
                            queue_jobs_processed = result.queue_jobs_processed,
                            stale_jobs_skipped = result.stale_jobs_skipped,
                            failed_jobs = result.failed_jobs,
                            storage_jobs_dropped = result.storage_jobs_dropped,
                            stale_rows_purged = result.stale_rows_purged,
                            evicted_rows = result.evicted_rows,
                            storage_limit_reached = result.storage_limit_reached,
                            remaining_queue_depth = result.remaining_queue_depth,
                            budget_exhausted = result.budget_exhausted,
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

    fn rebuild_catalog_index(
        &self,
        request: &RebuildSearchIndexRequest,
    ) -> Result<SearchMaintenanceResult, SearchManagerError> {
        self.catalog.rebuild_index(SearchProviderRebuildRequest {
            workspace_name: &request.workspace_name,
            force: request.force,
        })
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
