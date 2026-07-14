//! App-level Universal Search manager.

use crate::query::QueryAttribution;
use crate::search::catalog::local_snapshot::CatalogSnapshotLoader;
use crate::search::catalog::provider::CatalogMetadataProvider;
use crate::search::engine::UniversalSearchEngine;
use crate::search::result::{SearchManagerError, SearchRequest, SearchResponse};
use crate::state::{AppStateLayout, ConfigStore};
use crate::trajectory_memory::{RawTrajectoryStep, TrajectoryMemoryManager};
use crate::workspaces::WorkspaceManager;

#[derive(Clone)]
pub(crate) struct SearchManager {
    engine: UniversalSearchEngine,
    workspaces: WorkspaceManager,
    trajectory_memory: Option<TrajectoryMemoryManager>,
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
            engine: UniversalSearchEngine::new(catalog),
            workspaces: workspace_manager,
            trajectory_memory: None,
        }
    }

    pub(crate) fn with_trajectory_memory(
        mut self,
        trajectory_memory: TrajectoryMemoryManager,
    ) -> Self {
        self.trajectory_memory = Some(trajectory_memory);
        self
    }

    pub(crate) async fn search(
        &self,
        request: &SearchRequest,
        attribution: &QueryAttribution,
    ) -> Result<SearchResponse, SearchManagerError> {
        let started_at_unix_nanos = trajectory_timestamp();
        let result = match self
            .workspaces
            .require_workspace(&request.workspace_name)
            .await
        {
            Ok(()) => Ok(self.engine.search(request, attribution)),
            Err(error) => Err(error.into()),
        };
        if let (Some(memory), Some(task_id)) =
            (self.trajectory_memory.as_ref(), attribution.task_id)
        {
            let (status, row_count, error_message) = match &result {
                Ok(response) => (
                    "success",
                    Some(u64::try_from(response.results.len()).unwrap_or(u64::MAX)),
                    None,
                ),
                Err(SearchManagerError::App(error)) => ("error", None, Some(error.to_string())),
            };
            if let Err(error) = memory
                .record_raw_step(
                    &request.workspace_name,
                    RawTrajectoryStep {
                        task_id,
                        started_at_unix_nanos,
                        completed_at_unix_nanos: trajectory_timestamp(),
                        operation: "search".to_string(),
                        input: request.query.clone(),
                        status,
                        row_count,
                        output_summary: None,
                        error_kind: error_message.as_ref().map(|_| "app".to_string()),
                        error_type: error_message.as_ref().map(|_| "SEARCH".to_string()),
                        error_message,
                    },
                )
                .await
            {
                tracing::warn!(%error, task_id = %task_id, "failed to capture raw trajectory step");
            }
        }
        result
    }
}

fn trajectory_timestamp() -> i64 {
    match crate::state::db::now_unix_nanos_i64() {
        Ok(timestamp) => timestamp,
        Err(error) => {
            tracing::warn!(%error, "failed to timestamp raw trajectory step");
            0
        }
    }
}
