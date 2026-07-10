//! App-owned observed-value queue projection and bounded drainage.

use crate::search::observed::sqlite_projection::{
    ObservedValuesDrainBudget, ObservedValuesDrainResult,
};
use crate::search::observed::sqlite_store::SqliteObservedValuesStore;
use crate::search::sqlite_store::SqliteSearchError;
use crate::state::AppStateLayout;
use crate::workspaces::WorkspaceName;

#[derive(Debug, Clone)]
pub(crate) struct ObservedValuesProjection {
    store: SqliteObservedValuesStore,
}

impl ObservedValuesProjection {
    pub(crate) fn new(layout: AppStateLayout) -> Self {
        Self {
            store: SqliteObservedValuesStore::new(layout),
        }
    }

    pub(crate) fn drain_queue(
        &self,
        workspace_name: &WorkspaceName,
        budget: ObservedValuesDrainBudget,
    ) -> Result<ObservedValuesDrainResult, SqliteSearchError> {
        let result = self.store.drain_queue(workspace_name, budget)?;
        if result.budget_exhausted {
            tracing::debug!(
                workspace = %workspace_name,
                remaining_queue_depth = result.remaining_queue_depth,
                queue_jobs_processed = result.queue_jobs_processed,
                stale_jobs_skipped = result.stale_jobs_skipped,
                failed_jobs = result.failed_jobs,
                stale_rows_purged = result.stale_rows_purged,
                evicted_rows = result.evicted_rows,
                storage_limit_reached = result.storage_limit_reached,
                "observed-value queue drain budget expired"
            );
        }
        Ok(result)
    }
}
