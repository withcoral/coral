//! App-level Universal Search manager.

use crate::query::QueryAttribution;
use crate::search::catalog::local_snapshot::CatalogSnapshotLoader;
use crate::search::catalog::provider::CatalogMetadataProvider;
use crate::search::engine::UniversalSearchEngine;
use crate::search::maintenance::{
    ClearSearchDataRequest, ClearSearchDataResponse, RebuildSearchIndexRequest,
    RebuildSearchIndexResponse, SearchIndexProvider, SearchMaintenanceProviderResult,
    SearchProviderClearRequest, SearchProviderMaintenance, SearchProviderRebuildRequest,
};
use crate::search::result::{
    ProviderCoverage, SearchManagerError, SearchProviderKind, SearchProviderState, SearchRequest,
    SearchResponse,
};
use crate::state::{AppStateLayout, ConfigStore};
use crate::workspaces::WorkspaceName;

#[derive(Clone)]
pub(crate) struct SearchManager {
    catalog: CatalogMetadataProvider,
    engine: UniversalSearchEngine,
    config_store: ConfigStore,
}

impl SearchManager {
    pub(crate) fn new(layout: AppStateLayout, config_store: ConfigStore) -> Self {
        let catalog_loader = CatalogSnapshotLoader::new(config_store.clone(), layout.clone());
        let catalog = CatalogMetadataProvider::new(layout, catalog_loader);
        Self {
            catalog: catalog.clone(),
            engine: UniversalSearchEngine::new(catalog),
            config_store,
        }
    }

    pub(crate) fn search(
        &self,
        request: &SearchRequest,
        attribution: &QueryAttribution,
    ) -> Result<SearchResponse, SearchManagerError> {
        self.require_workspace(&request.workspace_name)?;
        Ok(self.engine.search(request, attribution))
    }

    pub(crate) fn rebuild_index(
        &self,
        request: &RebuildSearchIndexRequest,
    ) -> Result<RebuildSearchIndexResponse, SearchManagerError> {
        self.require_workspace(&request.workspace_name)?;
        let provider_results = match request.provider {
            SearchIndexProvider::Catalog => vec![self.rebuild_catalog_index(request)?],
            SearchIndexProvider::ObservedValues => vec![skipped_rebuild_provider_result(
                SearchProviderKind::ObservedValues,
                "observed-value search index rebuild is not implemented yet",
            )],
            SearchIndexProvider::All => vec![
                self.rebuild_catalog_index(request)?,
                skipped_rebuild_provider_result(
                    SearchProviderKind::ObservedValues,
                    "observed-value search index rebuild is not implemented yet",
                ),
            ],
        };
        Ok(RebuildSearchIndexResponse { provider_results })
    }

    pub(crate) fn clear_data(
        &self,
        request: &ClearSearchDataRequest,
    ) -> Result<ClearSearchDataResponse, SearchManagerError> {
        self.require_workspace(&request.workspace_name)?;
        let outcome = self.catalog.clear_data(SearchProviderClearRequest {
            workspace_name: &request.workspace_name,
            scope: request.scope,
            target: &request.target,
        })?;
        Ok(ClearSearchDataResponse {
            provider_results: vec![outcome.provider_result],
            compaction: outcome.compaction,
        })
    }

    fn require_workspace(&self, workspace_name: &WorkspaceName) -> Result<(), SearchManagerError> {
        let _state_lock = self.config_store.state_lock_shared()?;
        let config = self.config_store.load_config_unlocked()?;
        config.require_workspace(workspace_name)?;
        Ok(())
    }

    fn rebuild_catalog_index(
        &self,
        request: &RebuildSearchIndexRequest,
    ) -> Result<SearchMaintenanceProviderResult, SearchManagerError> {
        self.catalog.rebuild_index(SearchProviderRebuildRequest {
            workspace_name: &request.workspace_name,
            force: request.force,
        })
    }
}

fn skipped_rebuild_provider_result(
    provider: SearchProviderKind,
    note: &'static str,
) -> SearchMaintenanceProviderResult {
    SearchMaintenanceProviderResult {
        provider,
        state: SearchProviderState::Skipped,
        note: note.to_string(),
        coverage: ProviderCoverage::default(),
        detail: None,
    }
}
