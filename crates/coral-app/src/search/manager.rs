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
use crate::search::observed::{ObservedValuesDrainBudget, ObservedValuesProjection};
use crate::search::result::{SearchManagerError, SearchRequest, SearchResponse};
use crate::state::{AppStateLayout, ConfigStore};
use crate::workspaces::WorkspaceManager;

#[derive(Clone)]
pub(crate) struct SearchManager {
    catalog: CatalogMetadataProvider,
    observed_projection: ObservedValuesProjection,
    engine: UniversalSearchEngine,
    workspaces: WorkspaceManager,
}

const MANUAL_DRAIN_MAX_JOBS: usize = 10_000;
const SHUTDOWN_DRAIN_BUDGET: Duration = Duration::from_secs(1);

impl SearchManager {
    pub(crate) fn new(
        layout: AppStateLayout,
        config_store: &ConfigStore,
        workspace_manager: WorkspaceManager,
    ) -> Self {
        let catalog_loader = CatalogSnapshotLoader::new(config_store.clone(), layout.clone());
        let catalog = CatalogMetadataProvider::new(layout.clone(), catalog_loader);
        let observed_projection = ObservedValuesProjection::new(layout);
        Self {
            catalog: catalog.clone(),
            observed_projection,
            engine: UniversalSearchEngine::new(catalog),
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
        run_blocking_search_operation(move || Ok(search.engine.search(&request, &attribution)))
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
        let workspaces = self.workspaces.list_workspaces().await?;
        let observed_projection = self.observed_projection.clone();
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
                match observed_projection.drain_queue(
                    &workspace.name,
                    ObservedValuesDrainBudget::new(MANUAL_DRAIN_MAX_JOBS, remaining_budget),
                ) {
                    Ok(result) => {
                        tracing::debug!(
                            workspace = %workspace.name,
                            queue_jobs_processed = result.queue_jobs_processed,
                            stale_jobs_skipped = result.stale_jobs_skipped,
                            failed_jobs = result.failed_jobs,
                            remaining_queue_depth = result.remaining_queue_depth,
                            budget_exhausted = result.budget_exhausted,
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
