//! Transport-neutral Universal Search maintenance models.

use crate::search::result::{SearchManagerError, SearchProviderKind};
use crate::workspaces::WorkspaceName;

#[derive(Debug, Clone)]
pub(crate) struct RebuildSearchIndexRequest {
    pub(crate) workspace_name: WorkspaceName,
    pub(crate) force: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct RebuildSearchIndexResponse {
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
}

#[derive(Debug, Clone)]
pub(crate) struct SearchProviderClearOutcome {
    pub(crate) result: SearchMaintenanceResult,
    pub(crate) storage_cleanup: SearchStorageCleanupResult,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SearchDataScope {
    All,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SearchClearTarget {
    Workspace,
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
    Partial,
    Failed,
}

#[derive(Debug, Clone)]
pub(crate) enum SearchMaintenanceDetail {
    CatalogRebuild(CatalogRebuildMaintenanceResult),
    CatalogClear(CatalogClearMaintenanceResult),
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SearchStorageCleanupResult {
    pub(crate) state: SearchMaintenanceState,
    pub(crate) note: String,
}
