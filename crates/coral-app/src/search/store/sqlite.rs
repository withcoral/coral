//! `SQLite` side of the storage seam: one sidecar file per Workspace.

use crate::search::catalog::index::{
    CatalogClearResult, CatalogDocumentClass, CatalogIndexSnapshot, CatalogRebuildResult,
    CatalogRefreshResult, CatalogSearchHits,
};
use crate::search::maintenance::{SearchMaintenanceState, SearchStorageCleanupResult};
use crate::search::observed::{
    ObservedValuesClearResult, ObservedValuesDrainBudget, ObservedValuesDrainResult,
    ObservedValuesRebuildResult, ObservedValuesRetrievalPolicy, ObservedValuesSearchHits,
    SqliteObservedValuesStore,
};
use crate::search::sqlite_store::{
    SqliteSearchCompactionResult, SqliteSearchError, SqliteSearchStore,
};
use crate::search::store::{CatalogStore, ObservedValuesStore, SearchStoreError};
use crate::state::AppStateLayout;
use crate::workspaces::WorkspaceName;

#[derive(Debug, Clone)]
pub(super) struct SqliteSearchStorage {
    layout: AppStateLayout,
}

impl SqliteSearchStorage {
    pub(super) fn new(layout: AppStateLayout) -> Self {
        Self { layout }
    }

    pub(super) fn open_workspace(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<SqliteSearchStore, SearchStoreError> {
        let store = SqliteSearchStore::open_workspace(&self.layout, workspace_name)?;
        let capabilities = store.capabilities();
        tracing::debug!(
            workspace = %workspace_name,
            sqlite_version = %capabilities.sqlite_version,
            fts5 = capabilities.fts5,
            trigram = capabilities.trigram,
            "opened SQLite search store"
        );
        Ok(store)
    }

    pub(super) fn open_existing_workspace(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<Option<SqliteSearchStore>, SearchStoreError> {
        let exists = self
            .layout
            .search_sqlite_file(workspace_name)
            .try_exists()
            .map_err(SqliteSearchError::from)?;
        if !exists {
            return Ok(None);
        }
        self.open_workspace(workspace_name).map(Some)
    }

    pub(super) fn observed_values(&self) -> SqliteObservedValuesStore {
        SqliteObservedValuesStore::new(self.layout.clone())
    }
}

impl CatalogStore for SqliteSearchStore {
    fn projection_is_current(&self, fingerprint: &str) -> Result<bool, SearchStoreError> {
        Ok(self.catalog_projection_is_current(fingerprint)?)
    }

    fn refresh_projection(
        &self,
        snapshot: &CatalogIndexSnapshot,
    ) -> Result<CatalogRefreshResult, SearchStoreError> {
        Ok(self.refresh_catalog_projection(snapshot)?)
    }

    fn rebuild_projection(
        &self,
        snapshot: &CatalogIndexSnapshot,
        force: bool,
    ) -> Result<CatalogRebuildResult, SearchStoreError> {
        Ok(self.rebuild_catalog_projection(snapshot, force)?)
    }

    fn document_count(&self) -> Result<u32, SearchStoreError> {
        Ok(self.catalog_document_count()?)
    }

    fn search(
        &self,
        terms: &[String],
        limit: usize,
        class: CatalogDocumentClass,
    ) -> Result<CatalogSearchHits, SearchStoreError> {
        Ok(self.search_catalog(terms, limit, class)?)
    }

    fn clear_source(&self, source_name: &str) -> Result<CatalogClearResult, SearchStoreError> {
        Ok(self.clear_catalog_source(source_name)?)
    }

    fn clear_workspace(&self) -> Result<CatalogClearResult, SearchStoreError> {
        Ok(self.clear_catalog_workspace()?)
    }
}

impl ObservedValuesStore for SqliteObservedValuesStore {
    fn search(
        &self,
        workspace_name: &WorkspaceName,
        terms: &[String],
        limit: usize,
        policy: &ObservedValuesRetrievalPolicy,
    ) -> Result<ObservedValuesSearchHits, SearchStoreError> {
        Ok(Self::search(self, workspace_name, terms, limit, policy)?)
    }

    fn drain_queue(
        &self,
        workspace_name: &WorkspaceName,
        budget: ObservedValuesDrainBudget,
    ) -> Result<ObservedValuesDrainResult, SearchStoreError> {
        Ok(Self::drain_queue(self, workspace_name, budget)?)
    }

    fn rebuild_fts(
        &self,
        workspace_name: &WorkspaceName,
        policy: &ObservedValuesRetrievalPolicy,
    ) -> Result<ObservedValuesRebuildResult, SearchStoreError> {
        Ok(Self::rebuild_fts(self, workspace_name, policy)?)
    }

    fn pending_queue_job_count(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<usize, SearchStoreError> {
        Ok(Self::pending_queue_job_count(self, workspace_name)?)
    }

    fn clear_workspace_and_advance_epoch(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<ObservedValuesClearResult, SearchStoreError> {
        Ok(Self::clear_workspace_and_advance_epoch(
            self,
            workspace_name,
        )?)
    }

    fn clear_source_and_advance_epoch(
        &self,
        workspace_name: &WorkspaceName,
        source_name: &str,
    ) -> Result<ObservedValuesClearResult, SearchStoreError> {
        Ok(Self::clear_source_and_advance_epoch(
            self,
            workspace_name,
            source_name,
        )?)
    }

    fn compact_after_clear(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<SearchStorageCleanupResult, SearchStoreError> {
        let compaction = Self::compact_after_clear(self, workspace_name)?;
        Ok(storage_cleanup_result(&compaction))
    }
}

/// Projects the sidecar's checkpoint + `VACUUM` outcome onto the maintenance
/// result the RPC reports.
pub(super) fn storage_cleanup_result(
    result: &SqliteSearchCompactionResult,
) -> SearchStorageCleanupResult {
    let (state, note) = match (
        result.wal_checkpoint_truncate_completed,
        result.vacuum_completed,
    ) {
        (true, true) => (
            SearchMaintenanceState::Completed,
            "local search storage cleanup completed",
        ),
        (true, false) | (false, true) => (
            SearchMaintenanceState::Partial,
            "local search storage cleanup partially completed",
        ),
        (false, false) => (
            SearchMaintenanceState::Failed,
            "local search storage cleanup did not complete",
        ),
    };
    if state != SearchMaintenanceState::Completed {
        tracing::warn!(
            wal_checkpoint_truncate_completed = result.wal_checkpoint_truncate_completed,
            vacuum_completed = result.vacuum_completed,
            detail = %result.note,
            "local search storage cleanup did not fully complete"
        );
    }
    SearchStorageCleanupResult {
        state,
        note: note.to_string(),
    }
}
