//! Transport-neutral Universal Search maintenance models.

use crate::search::result::{SearchManagerError, SearchProviderKind};
use crate::sources::SourceName;
use crate::workspaces::WorkspaceName;

#[derive(Debug, Clone)]
pub(crate) struct RebuildSearchIndexRequest {
    pub(crate) workspace_name: WorkspaceName,
    pub(crate) provider: SearchIndexProvider,
    pub(crate) force: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct RebuildSearchIndexResponse {
    pub(crate) results: Vec<SearchMaintenanceResult>,
}

#[derive(Debug, Clone)]
pub(crate) struct DrainSearchQueueRequest {
    pub(crate) workspace_name: WorkspaceName,
    pub(crate) budget_ms: u32,
}

#[derive(Debug, Clone)]
pub(crate) struct DrainSearchQueueResponse {
    pub(crate) results: Vec<SearchMaintenanceResult>,
}

#[derive(Debug, Clone)]
pub(crate) struct ClearSearchDataRequest {
    pub(crate) workspace_name: WorkspaceName,
    pub(crate) scope: SearchDataScope,
    pub(crate) target: SearchClearTarget,
}

#[derive(Debug, Clone)]
pub(crate) struct ClearSearchDataResponse {
    pub(crate) results: Vec<SearchMaintenanceResult>,
    pub(crate) storage_cleanup: SearchStorageCleanupResult,
}

pub(crate) trait SearchProviderMaintenance {
    fn rebuild_index(
        &self,
        request: SearchProviderRebuildRequest<'_>,
    ) -> Result<SearchMaintenanceResult, SearchManagerError>;

    fn clear_data(
        &self,
        request: SearchProviderClearRequest<'_>,
    ) -> Result<SearchProviderClearOutcome, SearchManagerError>;
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct SearchProviderRebuildRequest<'a> {
    pub(crate) workspace_name: &'a WorkspaceName,
    pub(crate) force: bool,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct SearchProviderClearRequest<'a> {
    pub(crate) workspace_name: &'a WorkspaceName,
    pub(crate) scope: SearchDataScope,
    pub(crate) target: &'a SearchClearTarget,
    pub(crate) compact_after_clear: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct SearchProviderClearOutcome {
    pub(crate) result: SearchMaintenanceResult,
    pub(crate) storage_cleanup: Option<SearchStorageCleanupResult>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SearchIndexProvider {
    Catalog,
    ObservedValues,
    All,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SearchDataScope {
    ObservedValues,
    All,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SearchClearTarget {
    Workspace,
    Source(SourceName),
}

#[derive(Debug, Clone)]
pub(crate) struct SearchMaintenanceResult {
    pub(crate) provider: SearchProviderKind,
    pub(crate) state: SearchMaintenanceState,
    pub(crate) note: String,
    pub(crate) detail: Option<SearchMaintenanceDetail>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SearchMaintenanceState {
    Completed,
    Noop,
    Skipped,
    Partial,
    Failed,
}

#[derive(Debug, Clone)]
pub(crate) enum SearchMaintenanceDetail {
    CatalogRebuild(CatalogRebuildMaintenanceResult),
    CatalogClear(CatalogClearMaintenanceResult),
    ObservedDrain(ObservedDrainMaintenanceResult),
    ObservedRebuild(ObservedRebuildMaintenanceResult),
    ObservedClear(ObservedClearMaintenanceResult),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CatalogRebuildMaintenanceResult {
    pub(crate) old_document_count: u32,
    pub(crate) new_document_count: u32,
    pub(crate) projection_changed: bool,
    pub(crate) rebuild_performed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CatalogClearMaintenanceResult {
    pub(crate) deleted_document_count: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ObservedDrainMaintenanceResult {
    pub(crate) queue_jobs_processed: u32,
    pub(crate) stale_jobs_skipped: u32,
    pub(crate) failed_jobs: u32,
    pub(crate) canonical_rows_upserted: u32,
    pub(crate) fts_rows_written: u32,
    pub(crate) remaining_queue_depth: u32,
    pub(crate) budget_exhausted: bool,
    pub(crate) stale_rows_purged: u32,
    pub(crate) evicted_rows: u32,
    pub(crate) storage_limit_reached: bool,
    pub(crate) storage_jobs_dropped: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ObservedRebuildMaintenanceResult {
    pub(crate) canonical_rows_scanned: u32,
    pub(crate) fts_rows_rebuilt: u32,
    pub(crate) drain: ObservedDrainMaintenanceResult,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ObservedClearMaintenanceResult {
    pub(crate) values: u32,
    pub(crate) fts_rows: u32,
    pub(crate) queue_jobs: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SearchStorageCleanupResult {
    pub(crate) state: SearchMaintenanceState,
    pub(crate) note: String,
}
