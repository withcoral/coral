//! App-level Universal Search manager.

use std::time::{Duration, Instant};

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
use crate::state::{AppStateLayout, ConfigStore};
use crate::workspaces::WorkspaceName;

#[derive(Clone)]
pub(crate) struct SearchManager {
    catalog: CatalogMetadataProvider,
    observed: ObservedValuesProvider,
    observed_scope_loader: ObservedValuesLiveScopeLoader,
    engine: UniversalSearchEngine,
    config_store: ConfigStore,
}

const MANUAL_DRAIN_MAX_JOBS: usize = 10_000;
const SHUTDOWN_DRAIN_BUDGET: Duration = Duration::from_secs(1);
const OBSERVED_STALE_AFTER_LAST_OBSERVED_DAYS: u32 = 365;

impl SearchManager {
    pub(crate) fn new(layout: AppStateLayout, config_store: ConfigStore) -> Self {
        let catalog_loader = CatalogSnapshotLoader::new(config_store.clone(), layout.clone());
        let catalog = CatalogMetadataProvider::new(layout.clone(), catalog_loader);
        let observed = ObservedValuesProvider::new(layout.clone());
        let observed_scope_loader =
            ObservedValuesLiveScopeLoader::new(layout, config_store.clone());
        Self {
            catalog: catalog.clone(),
            observed: observed.clone(),
            observed_scope_loader,
            engine: UniversalSearchEngine::new(catalog, observed),
            config_store,
        }
    }

    pub(crate) fn search(
        &self,
        request: &SearchRequest,
        attribution: &QueryAttribution,
    ) -> Result<SearchResponse, SearchManagerError> {
        self.require_workspace(&request.workspace_name)?;
        let observed_policy = self.observed_retrieval_policy(&request.workspace_name);
        Ok(self
            .engine
            .search(request, attribution, observed_policy.as_ref()))
    }

    pub(crate) fn rebuild_index(
        &self,
        request: &RebuildSearchIndexRequest,
    ) -> Result<RebuildSearchIndexResponse, SearchManagerError> {
        self.require_workspace(&request.workspace_name)?;
        let result = self.rebuild_catalog_index(request)?;
        Ok(RebuildSearchIndexResponse {
            results: vec![result],
        })
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
            results: vec![outcome.result],
            storage_cleanup: outcome.storage_cleanup,
        })
    }

    pub(crate) fn drain_before_shutdown(&self) -> Result<(), SearchManagerError> {
        let workspaces = {
            let _state_lock = self.config_store.state_lock_shared()?;
            self.config_store.load_config_unlocked()?.workspaces()
        };
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
            match self.observed.drain_queue(
                &workspace.name,
                ObservedValuesDrainBudget::new(MANUAL_DRAIN_MAX_JOBS, remaining_budget),
            ) {
                Ok(result) => {
                    tracing::debug!(
                        workspace = %workspace.name,
                        queue_jobs_processed = result.queue_jobs_processed,
                        stale_jobs_skipped = result.stale_jobs_skipped,
                        failed_jobs = result.failed_jobs,
                        stale_rows_purged = result.stale_rows_purged,
                        evicted_rows = result.evicted_rows,
                        storage_limit_reached = result.storage_limit_reached,
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
    }

    fn require_workspace(&self, workspace_name: &WorkspaceName) -> Result<(), SearchManagerError> {
        let _state_lock = self.config_store.state_lock_shared()?;
        let config = self.config_store.load_config_unlocked()?;
        config.require_workspace(workspace_name)?;
        Ok(())
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
