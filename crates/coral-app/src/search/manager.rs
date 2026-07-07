//! App-level Universal Search manager.

use crate::query::QueryAttribution;
use crate::search::catalog::local_snapshot::CatalogSnapshotLoader;
use crate::search::catalog::provider::CatalogMetadataProvider;
use crate::search::engine::UniversalSearchEngine;
use crate::search::result::{SearchManagerError, SearchRequest, SearchResponse};
use crate::state::{AppStateLayout, ConfigStore};

#[derive(Clone)]
pub(crate) struct SearchManager {
    engine: UniversalSearchEngine,
    config_store: ConfigStore,
}

impl SearchManager {
    pub(crate) fn new(layout: AppStateLayout, config_store: ConfigStore) -> Self {
        let catalog_loader = CatalogSnapshotLoader::new(config_store.clone(), layout.clone());
        let catalog = CatalogMetadataProvider::new(layout, catalog_loader);
        Self {
            engine: UniversalSearchEngine::new(catalog),
            config_store,
        }
    }

    pub(crate) fn search(
        &self,
        request: &SearchRequest,
        attribution: &QueryAttribution,
    ) -> Result<SearchResponse, SearchManagerError> {
        {
            let _state_lock = self.config_store.state_lock_shared()?;
            let config = self.config_store.load_config_unlocked()?;
            config.require_workspace(&request.workspace_name)?;
        }
        Ok(self.engine.search(request, attribution))
    }
}
