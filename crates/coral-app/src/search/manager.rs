//! App-level Universal Search manager.

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
use crate::search::result::{SearchManagerError, SearchRequest, SearchResponse};
use crate::state::{AppStateLayout, ConfigStore};
use crate::workspaces::WorkspaceManager;

#[derive(Clone)]
pub(crate) struct SearchManager {
    catalog: CatalogMetadataProvider,
    engine: UniversalSearchEngine,
    workspaces: WorkspaceManager,
}

impl SearchManager {
    pub(crate) fn new(
        layout: AppStateLayout,
        config_store: &ConfigStore,
        workspace_manager: WorkspaceManager,
    ) -> Self {
        let catalog_loader = CatalogSnapshotLoader::new(config_store.clone(), layout.clone());
        let catalog = CatalogMetadataProvider::new(layout, catalog_loader);
        Self {
            catalog: catalog.clone(),
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
        Ok(self.engine.search(request, attribution))
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
