//! App-level Universal Search manager.

use crate::query::QueryAttribution;
use crate::search::catalog::local_snapshot::CatalogSnapshotLoader;
use crate::search::catalog::provider::CatalogMetadataProvider;
use crate::search::engine::UniversalSearchEngine;
use crate::search::result::{SearchRequest, SearchResponse};
use crate::state::{AppStateLayout, ConfigStore};

#[derive(Clone)]
pub(crate) struct SearchManager {
    engine: UniversalSearchEngine,
}

impl SearchManager {
    pub(crate) fn new(layout: AppStateLayout, config_store: ConfigStore) -> Self {
        let catalog_loader = CatalogSnapshotLoader::new(config_store, layout.clone());
        let catalog = CatalogMetadataProvider::new(layout, catalog_loader);
        Self {
            engine: UniversalSearchEngine::new(catalog),
        }
    }

    pub(crate) fn search(
        &self,
        request: &SearchRequest,
        attribution: &QueryAttribution,
    ) -> SearchResponse {
        self.engine.search(request, attribution)
    }
}
