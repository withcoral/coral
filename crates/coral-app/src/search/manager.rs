//! App-level Universal Search manager.

use crate::query::QueryAttribution;
use crate::search::catalog::local_snapshot::CatalogSnapshotLoader;
use crate::search::catalog::provider::CatalogMetadataProvider;
use crate::search::engine::UniversalSearchEngine;
use crate::search::result::{SearchManagerError, SearchRequest, SearchResponse};
use crate::state::{AppStateLayout, ConfigStore};
use crate::workspaces::WorkspaceManager;

#[derive(Clone)]
pub(crate) struct SearchManager {
    engine: UniversalSearchEngine,
    workspaces: WorkspaceManager,
}

impl SearchManager {
    pub(crate) fn new(
        layout: AppStateLayout,
        config_store: &ConfigStore,
        workspace_manager: WorkspaceManager,
    ) -> Self {
        let catalog_loader = CatalogSnapshotLoader::new(config_store, layout.clone());
        let catalog = CatalogMetadataProvider::new(layout, catalog_loader);
        Self {
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
}
