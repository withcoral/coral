//! Transport-neutral Universal Search maintenance models.

use crate::search::result::{
    ProviderCoverage, SearchManagerError, SearchProviderKind, SearchProviderState,
};
use crate::workspaces::WorkspaceName;

#[derive(Debug, Clone)]
pub(crate) struct RebuildSearchIndexRequest {
    pub(crate) workspace_name: WorkspaceName,
    pub(crate) provider: SearchIndexProvider,
    pub(crate) force: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct RebuildSearchIndexResponse {
    pub(crate) provider_results: Vec<SearchMaintenanceProviderResult>,
}

#[derive(Debug, Clone)]
pub(crate) struct ClearSearchDataRequest {
    pub(crate) workspace_name: WorkspaceName,
    pub(crate) scope: SearchDataScope,
    pub(crate) target: SearchClearTarget,
}

#[derive(Debug, Clone)]
pub(crate) struct ClearSearchDataResponse {
    pub(crate) provider_results: Vec<SearchMaintenanceProviderResult>,
    pub(crate) compaction: SearchCompactionStatus,
}

pub(crate) trait SearchProviderMaintenance {
    fn rebuild_index(
        &self,
        request: SearchProviderRebuildRequest<'_>,
    ) -> Result<SearchMaintenanceProviderResult, SearchManagerError>;

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
    pub(crate) provider_result: SearchMaintenanceProviderResult,
    pub(crate) compaction: SearchCompactionStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SearchIndexProvider {
    Catalog,
    ObservedValues,
    All,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SearchDataScope {
    Observed,
    All,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SearchClearTarget {
    Workspace,
    Source(String),
}

#[derive(Debug, Clone)]
pub(crate) struct SearchMaintenanceProviderResult {
    pub(crate) provider: SearchProviderKind,
    pub(crate) state: SearchProviderState,
    pub(crate) note: String,
    pub(crate) coverage: ProviderCoverage,
    pub(crate) detail: Option<SearchMaintenanceDetail>,
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
pub(crate) struct SearchCompactionStatus {
    pub(crate) wal_checkpoint_truncate_completed: bool,
    pub(crate) vacuum_completed: bool,
    pub(crate) note: String,
}
