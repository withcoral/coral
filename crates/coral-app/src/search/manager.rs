//! App-level Universal Search manager.

use crate::query::QueryAttribution;
use crate::query::manager::QueryManager;
use crate::search::catalog::provider::CatalogMetadataProvider;
use crate::search::engine::UniversalSearchEngine;
use crate::search::result::{SearchRequest, SearchResponse};
use crate::state::AppStateLayout;

#[derive(Clone)]
pub(crate) struct SearchManager {
    engine: UniversalSearchEngine,
}

impl SearchManager {
    pub(crate) fn new(layout: AppStateLayout, query_manager: QueryManager) -> Self {
        let catalog = CatalogMetadataProvider::new(layout, query_manager);
        Self {
            engine: UniversalSearchEngine::new(catalog),
        }
    }

    pub(crate) async fn search(
        &self,
        request: &SearchRequest,
        attribution: &QueryAttribution,
    ) -> SearchResponse {
        self.engine.search(request, attribution).await
    }
}
