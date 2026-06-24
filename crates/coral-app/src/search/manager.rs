//! App-level Universal Search manager.

use crate::search::result::{
    ProviderStatus, SearchManagerError, SearchProviderKind, SearchProviderState, SearchRequest,
    SearchResponse, SearchTruncation,
};
use crate::state::ConfigStore;

#[derive(Clone)]
pub(crate) struct SearchManager {
    config_store: ConfigStore,
}

impl SearchManager {
    pub(crate) fn new(config_store: ConfigStore) -> Self {
        Self { config_store }
    }

    pub(crate) fn search(
        &self,
        request: &SearchRequest,
    ) -> Result<SearchResponse, SearchManagerError> {
        {
            let _state_lock = self.config_store.state_lock_shared()?;
            let config = self.config_store.load_config_unlocked()?;
            config.require_workspace(&request.workspace_name)?;
        }
        tracing::debug!(
            workspace = %request.workspace_name,
            query_len_bytes = request.query.len(),
            limit = request.limit,
            "running Universal Search shell without concrete providers"
        );
        Ok(SearchResponse {
            provider_statuses: vec![
                ProviderStatus {
                    provider: SearchProviderKind::CatalogMetadata,
                    state: SearchProviderState::NotEnabled,
                    note: "catalog metadata search is not wired yet".to_string(),
                    coverage: None,
                },
                ProviderStatus {
                    provider: SearchProviderKind::ObservedValues,
                    state: SearchProviderState::NotEnabled,
                    note: "observed value search is not wired yet".to_string(),
                    coverage: None,
                },
                ProviderStatus {
                    provider: SearchProviderKind::NativeFanout,
                    state: SearchProviderState::NotEnabled,
                    note: "provider-native fanout is not wired yet".to_string(),
                    coverage: None,
                },
            ],
            truncation: SearchTruncation {
                truncated: false,
                returned_count: 0,
                max_results: request.limit,
                note: "no search providers are wired yet".to_string(),
            },
        })
    }
}
