//! App-level Universal Search manager.

use crate::query::QueryAttribution;
use crate::search::catalog::local_snapshot::CatalogSnapshotLoader;
use crate::search::catalog::provider::CatalogMetadataProvider;
use crate::search::engine::UniversalSearchEngine;
use crate::search::result::{SearchManagerError, SearchRequest, SearchResponse};
use crate::sources::materialization::SourceDiagnosticReporter;
use crate::state::{AppStateLayout, ConfigStore};
use crate::workspaces::WorkspaceManager;

#[derive(Clone)]
pub(crate) struct SearchManager {
    engine: UniversalSearchEngine,
    workspaces: WorkspaceManager,
}

impl SearchManager {
    #[cfg(test)]
    pub(crate) fn new(
        layout: AppStateLayout,
        config_store: &ConfigStore,
        workspace_manager: WorkspaceManager,
    ) -> Self {
        Self::with_diagnostic_reporter(
            layout,
            config_store,
            workspace_manager,
            SourceDiagnosticReporter::default(),
        )
    }

    pub(crate) fn with_diagnostic_reporter(
        layout: AppStateLayout,
        config_store: &ConfigStore,
        workspace_manager: WorkspaceManager,
        diagnostic_reporter: SourceDiagnosticReporter,
    ) -> Self {
        let catalog_loader = CatalogSnapshotLoader::with_diagnostic_reporter(
            config_store.clone(),
            layout.clone(),
            diagnostic_reporter,
        );
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
