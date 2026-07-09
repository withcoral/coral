//! Implements the gRPC `SearchService`.

use coral_api::v1::search_clear_target::Target as ProtoSearchClearTargetKind;
use coral_api::v1::search_maintenance_result::Detail as ProtoMaintenanceDetail;
use coral_api::v1::search_result::Payload;
use coral_api::v1::search_service_server::SearchService as SearchServiceApi;
use coral_api::v1::{
    CatalogClearResult as ProtoCatalogClearResult, CatalogMetadata,
    CatalogRebuildResult as ProtoCatalogRebuildResult,
    ClearSearchDataRequest as ProtoClearSearchDataRequest,
    ClearSearchDataResponse as ProtoClearSearchDataResponse, ColumnHint,
    DrainSearchQueueRequest as ProtoDrainSearchQueueRequest,
    DrainSearchQueueResponse as ProtoDrainSearchQueueResponse,
    ObservedClearResult as ProtoObservedClearResult,
    ObservedDrainResult as ProtoObservedDrainResult,
    ObservedRebuildResult as ProtoObservedRebuildResult, ObservedValue as ProtoObservedValue,
    RebuildSearchIndexRequest as ProtoRebuildSearchIndexRequest,
    RebuildSearchIndexResponse as ProtoRebuildSearchIndexResponse,
    SearchDataScope as ProtoSearchDataScope, SearchFieldRole as ProtoSearchFieldRole,
    SearchIndexProvider as ProtoSearchIndexProvider,
    SearchMaintenanceResult as ProtoSearchMaintenanceResult,
    SearchMaintenanceState as ProtoSearchMaintenanceState, SearchProvider as ProtoSearchProvider,
    SearchProviderCoverage, SearchProviderState, SearchProviderStatus,
    SearchRequest as ProtoSearchRequest, SearchResponse as ProtoSearchResponse,
    SearchResult as ProtoSearchResult, SearchResultTruncation,
    SearchStorageCleanupResult as ProtoSearchStorageCleanupResult,
    SearchSurfaceKind as ProtoSearchSurfaceKind, SearchTableColumnPreview,
    SearchTableColumnPreviewColumn,
};
use tokio::task;
use tonic::{Request, Response, Status};

use crate::bootstrap::{AppError, app_status};
use crate::query::QueryAttribution;
use crate::search::maintenance::{
    CatalogClearMaintenanceResult, CatalogRebuildMaintenanceResult,
    ClearSearchDataRequest as DomainClearSearchDataRequest,
    ClearSearchDataResponse as DomainClearSearchDataResponse,
    DrainSearchQueueRequest as DomainDrainSearchQueueRequest,
    DrainSearchQueueResponse as DomainDrainSearchQueueResponse, ObservedClearMaintenanceResult,
    ObservedDrainMaintenanceResult, ObservedRebuildMaintenanceResult,
    RebuildSearchIndexRequest as DomainRebuildSearchIndexRequest,
    RebuildSearchIndexResponse as DomainRebuildSearchIndexResponse, SearchClearTarget,
    SearchDataScope, SearchIndexProvider, SearchMaintenanceDetail, SearchMaintenanceResult,
    SearchMaintenanceState, SearchStorageCleanupResult,
};
use crate::search::manager::SearchManager;
use crate::search::result::{
    CatalogMetadataResult, ColumnHintResult, ObservedValueResult, ProviderCoverage, ProviderStatus,
    SearchFieldRole, SearchManagerError, SearchPayload, SearchProviderKind,
    SearchProviderState as DomainProviderState, SearchRequest, SearchResponse, SearchSurfaceKind,
    TableColumnPreview as DomainTableColumnPreview,
};
use crate::transport::{
    catalog_item_to_proto, grpc_span, instrument_grpc, workspace_name_from_proto,
};

#[derive(Clone)]
pub(crate) struct SearchService {
    search: SearchManager,
}

impl SearchService {
    pub(crate) fn new(search_manager: SearchManager) -> Self {
        Self {
            search: search_manager,
        }
    }
}

#[tonic::async_trait]
impl SearchServiceApi for SearchService {
    async fn search(
        &self,
        request: Request<ProtoSearchRequest>,
    ) -> Result<Response<ProtoSearchResponse>, Status> {
        let span = grpc_span(&request);
        let search = self.search.clone();
        instrument_grpc(span, async move {
            let attribution = QueryAttribution::from_extensions(request.extensions());
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            let request = SearchRequest::new(workspace_name, &request.query, request.limit)
                .map_err(search_status)?;
            let response =
                run_blocking_search_operation(move || search.search(&request, &attribution))
                    .await?;
            Ok(Response::new(search_response_to_proto(response)))
        })
        .await
    }

    async fn rebuild_search_index(
        &self,
        request: Request<ProtoRebuildSearchIndexRequest>,
    ) -> Result<Response<ProtoRebuildSearchIndexResponse>, Status> {
        let span = grpc_span(&request);
        let search = self.search.clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            let request = DomainRebuildSearchIndexRequest {
                workspace_name,
                provider: index_provider_from_proto(proto_index_provider(request.provider)?),
                force: request.force,
            };
            let response =
                run_blocking_search_operation(move || search.rebuild_index(&request)).await?;
            Ok(Response::new(rebuild_response_to_proto(response)))
        })
        .await
    }

    async fn drain_search_queue(
        &self,
        request: Request<ProtoDrainSearchQueueRequest>,
    ) -> Result<Response<ProtoDrainSearchQueueResponse>, Status> {
        let span = grpc_span(&request);
        let search = self.search.clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            let request = DomainDrainSearchQueueRequest {
                workspace_name,
                budget_ms: request.budget_ms,
            };
            let response =
                run_blocking_search_operation(move || search.drain_queue(&request)).await?;
            Ok(Response::new(drain_response_to_proto(response)))
        })
        .await
    }

    async fn clear_search_data(
        &self,
        request: Request<ProtoClearSearchDataRequest>,
    ) -> Result<Response<ProtoClearSearchDataResponse>, Status> {
        let span = grpc_span(&request);
        let search = self.search.clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            let request = DomainClearSearchDataRequest {
                workspace_name,
                scope: data_scope_from_proto(proto_data_scope(request.scope)?)?,
                target: clear_target_from_proto(request.target)?,
            };
            let response =
                run_blocking_search_operation(move || search.clear_data(&request)).await?;
            Ok(Response::new(clear_response_to_proto(response)))
        })
        .await
    }
}

async fn run_blocking_search_operation<T, F>(operation: F) -> Result<T, Status>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, SearchManagerError> + Send + 'static,
{
    let span = tracing::Span::current();
    task::spawn_blocking(move || span.in_scope(operation))
        .await
        .map_err(|error| Status::internal(format!("search operation task failed: {error}")))?
        .map_err(search_status)
}

fn search_status(error: SearchManagerError) -> Status {
    match error {
        SearchManagerError::App(error) => app_status(error),
    }
}

fn search_response_to_proto(response: SearchResponse) -> ProtoSearchResponse {
    ProtoSearchResponse {
        results: response
            .results
            .into_iter()
            .map(|result| ProtoSearchResult {
                provider: provider_kind_to_proto(result.provider) as i32,
                payload: Some(search_payload_to_proto(
                    &response.workspace_name,
                    result.payload,
                )),
            })
            .collect(),
        provider_statuses: response
            .provider_statuses
            .into_iter()
            .map(provider_status_to_proto)
            .collect(),
        truncation: Some(SearchResultTruncation {
            truncated: response.truncation.truncated,
            returned_count: response.truncation.returned_count,
            max_results: response.truncation.max_results,
            note: response.truncation.note,
        }),
    }
}

fn rebuild_response_to_proto(
    response: DomainRebuildSearchIndexResponse,
) -> ProtoRebuildSearchIndexResponse {
    ProtoRebuildSearchIndexResponse {
        results: response
            .results
            .into_iter()
            .map(maintenance_result_to_proto)
            .collect(),
    }
}

fn drain_response_to_proto(
    response: DomainDrainSearchQueueResponse,
) -> ProtoDrainSearchQueueResponse {
    ProtoDrainSearchQueueResponse {
        results: response
            .results
            .into_iter()
            .map(maintenance_result_to_proto)
            .collect(),
    }
}

fn clear_response_to_proto(
    response: DomainClearSearchDataResponse,
) -> ProtoClearSearchDataResponse {
    ProtoClearSearchDataResponse {
        results: response
            .results
            .into_iter()
            .map(maintenance_result_to_proto)
            .collect(),
        storage_cleanup: Some(storage_cleanup_to_proto(response.storage_cleanup)),
    }
}

fn maintenance_result_to_proto(result: SearchMaintenanceResult) -> ProtoSearchMaintenanceResult {
    let SearchMaintenanceResult {
        provider,
        state,
        note,
        detail,
    } = result;
    ProtoSearchMaintenanceResult {
        provider: provider_kind_to_proto(provider) as i32,
        state: maintenance_state_to_proto(state) as i32,
        note,
        detail: detail.as_ref().map(maintenance_detail_to_proto),
    }
}

fn maintenance_detail_to_proto(detail: &SearchMaintenanceDetail) -> ProtoMaintenanceDetail {
    match detail {
        SearchMaintenanceDetail::CatalogRebuild(result) => {
            ProtoMaintenanceDetail::CatalogRebuild(catalog_rebuild_to_proto(result))
        }
        SearchMaintenanceDetail::CatalogClear(result) => {
            ProtoMaintenanceDetail::CatalogClear(catalog_clear_to_proto(*result))
        }
        SearchMaintenanceDetail::ObservedDrain(result) => {
            ProtoMaintenanceDetail::ObservedDrain(observed_drain_to_proto(*result))
        }
        SearchMaintenanceDetail::ObservedRebuild(result) => {
            ProtoMaintenanceDetail::ObservedRebuild(observed_rebuild_to_proto(*result))
        }
        SearchMaintenanceDetail::ObservedClear(result) => {
            ProtoMaintenanceDetail::ObservedClear(observed_clear_to_proto(*result))
        }
    }
}

fn catalog_rebuild_to_proto(result: &CatalogRebuildMaintenanceResult) -> ProtoCatalogRebuildResult {
    ProtoCatalogRebuildResult {
        old_document_count: result.old_document_count,
        new_document_count: result.new_document_count,
        projection_changed: result.projection_changed,
        rebuild_performed: result.rebuild_performed,
    }
}

fn catalog_clear_to_proto(result: CatalogClearMaintenanceResult) -> ProtoCatalogClearResult {
    ProtoCatalogClearResult {
        deleted_document_count: result.deleted_document_count,
    }
}

fn storage_cleanup_to_proto(result: SearchStorageCleanupResult) -> ProtoSearchStorageCleanupResult {
    ProtoSearchStorageCleanupResult {
        state: maintenance_state_to_proto(result.state) as i32,
        note: result.note,
    }
}

fn maintenance_state_to_proto(state: SearchMaintenanceState) -> ProtoSearchMaintenanceState {
    match state {
        SearchMaintenanceState::Completed => ProtoSearchMaintenanceState::Completed,
        SearchMaintenanceState::Noop => ProtoSearchMaintenanceState::Noop,
        SearchMaintenanceState::Partial => ProtoSearchMaintenanceState::Partial,
        SearchMaintenanceState::Failed => ProtoSearchMaintenanceState::Failed,
    }
}

fn observed_drain_to_proto(result: ObservedDrainMaintenanceResult) -> ProtoObservedDrainResult {
    ProtoObservedDrainResult {
        queue_jobs_processed: result.queue_jobs_processed,
        stale_jobs_skipped: result.stale_jobs_skipped,
        failed_jobs: result.failed_jobs,
        canonical_rows_upserted: result.canonical_rows_upserted,
        fts_rows_written: result.fts_rows_written,
        remaining_queue_depth: result.remaining_queue_depth,
        budget_exhausted: result.budget_exhausted,
    }
}

fn observed_rebuild_to_proto(
    result: ObservedRebuildMaintenanceResult,
) -> ProtoObservedRebuildResult {
    ProtoObservedRebuildResult {
        canonical_rows_scanned: result.canonical_rows_scanned,
        fts_rows_rebuilt: result.fts_rows_rebuilt,
    }
}

fn observed_clear_to_proto(result: ObservedClearMaintenanceResult) -> ProtoObservedClearResult {
    ProtoObservedClearResult {
        deleted_value_count: result.values,
        deleted_fts_count: result.fts_rows,
        deleted_queue_job_count: result.queue_jobs,
    }
}

fn proto_index_provider(value: i32) -> Result<ProtoSearchIndexProvider, Status> {
    ProtoSearchIndexProvider::try_from(value).map_err(|_error| {
        app_status(AppError::InvalidInput(format!(
            "invalid search index provider value {value}"
        )))
    })
}

fn index_provider_from_proto(provider: ProtoSearchIndexProvider) -> SearchIndexProvider {
    match provider {
        ProtoSearchIndexProvider::Catalog => SearchIndexProvider::Catalog,
        ProtoSearchIndexProvider::ObservedValues => SearchIndexProvider::ObservedValues,
        ProtoSearchIndexProvider::All | ProtoSearchIndexProvider::Unspecified => {
            SearchIndexProvider::All
        }
    }
}

fn proto_data_scope(value: i32) -> Result<ProtoSearchDataScope, Status> {
    ProtoSearchDataScope::try_from(value).map_err(|_error| {
        app_status(AppError::InvalidInput(format!(
            "invalid search data scope value {value}"
        )))
    })
}

fn data_scope_from_proto(scope: ProtoSearchDataScope) -> Result<SearchDataScope, Status> {
    match scope {
        ProtoSearchDataScope::Observed => Ok(SearchDataScope::Observed),
        ProtoSearchDataScope::All => Ok(SearchDataScope::All),
        ProtoSearchDataScope::Unspecified => Err(app_status(AppError::InvalidInput(
            "search data scope is required".to_string(),
        ))),
    }
}

fn clear_target_from_proto(
    target: Option<coral_api::v1::SearchClearTarget>,
) -> Result<SearchClearTarget, Status> {
    match target.and_then(|target| target.target) {
        Some(ProtoSearchClearTargetKind::Workspace(true)) => Ok(SearchClearTarget::Workspace),
        Some(ProtoSearchClearTargetKind::Workspace(false)) => Err(app_status(
            AppError::InvalidInput("search clear workspace target must be true".to_string()),
        )),
        Some(ProtoSearchClearTargetKind::SourceName(source_name))
            if !source_name.trim().is_empty() =>
        {
            Ok(SearchClearTarget::Source(source_name))
        }
        Some(ProtoSearchClearTargetKind::SourceName(_)) => Err(app_status(AppError::InvalidInput(
            "search clear source_name target must not be empty".to_string(),
        ))),
        None => Err(app_status(AppError::InvalidInput(
            "search clear target is required".to_string(),
        ))),
    }
}

fn search_payload_to_proto(
    workspace_name: &crate::workspaces::WorkspaceName,
    payload: SearchPayload,
) -> Payload {
    match payload {
        SearchPayload::CatalogMetadata(result) => {
            Payload::CatalogMetadata(catalog_metadata_to_proto(workspace_name, result))
        }
        SearchPayload::ColumnHint(result) => Payload::ColumnHint(column_hint_to_proto(result)),
        SearchPayload::ObservedValue(result) => {
            Payload::ObservedValue(observed_value_to_proto(result))
        }
    }
}

fn catalog_metadata_to_proto(
    workspace_name: &crate::workspaces::WorkspaceName,
    result: CatalogMetadataResult,
) -> CatalogMetadata {
    CatalogMetadata {
        item: Some(catalog_item_to_proto(workspace_name, result.item)),
        matched_fields: result.matched_fields,
        table_column_preview: result
            .table_column_preview
            .map(table_column_preview_to_proto),
    }
}

fn table_column_preview_to_proto(preview: DomainTableColumnPreview) -> SearchTableColumnPreview {
    SearchTableColumnPreview {
        column_count: preview.column_count,
        columns: preview
            .columns
            .into_iter()
            .map(|column| SearchTableColumnPreviewColumn {
                name: column.column.name,
                data_type: column.column.data_type,
                is_required_filter: column.column.is_required_filter,
                description: column.column.description,
                matched_fields: column.matched_fields,
            })
            .collect(),
        omitted_column_count: preview.omitted_column_count,
    }
}

fn column_hint_to_proto(result: ColumnHintResult) -> ColumnHint {
    ColumnHint {
        schema_name: result.schema_name,
        surface_name: result.surface_name,
        surface_kind: surface_kind_to_proto(result.surface_kind) as i32,
        name: result.name,
        data_type: result.data_type,
        required: result.required,
        description: result.description,
        matched_fields: result.matched_fields,
        field_role: field_role_to_proto(result.field_role) as i32,
    }
}

fn observed_value_to_proto(result: ObservedValueResult) -> ProtoObservedValue {
    ProtoObservedValue {
        value: result.value,
        schema_name: result.schema_name,
        surface_name: result.surface_name,
        column_name: result.column_name,
        surface_kind: surface_kind_to_proto(result.surface_kind) as i32,
        field_path: result.field_path,
        observed_count: result.observed_count,
        last_observed_at: result.last_observed_at,
    }
}

fn provider_status_to_proto(status: ProviderStatus) -> SearchProviderStatus {
    let coverage = if matches!(
        status.state,
        DomainProviderState::NotEnabled | DomainProviderState::Skipped
    ) {
        None
    } else {
        status.coverage.as_ref().map(provider_coverage_to_proto)
    };
    SearchProviderStatus {
        provider: provider_kind_to_proto(status.provider) as i32,
        state: provider_state_to_proto(status.state) as i32,
        note: status.note,
        coverage,
    }
}

fn provider_kind_to_proto(provider: SearchProviderKind) -> ProtoSearchProvider {
    match provider {
        SearchProviderKind::CatalogMetadata => ProtoSearchProvider::CatalogMetadata,
        SearchProviderKind::ObservedValues => ProtoSearchProvider::ObservedValues,
        SearchProviderKind::NativeFanout => ProtoSearchProvider::NativeFanout,
    }
}

fn provider_state_to_proto(state: DomainProviderState) -> SearchProviderState {
    match state {
        DomainProviderState::ResultsFound => SearchProviderState::ResultsFound,
        DomainProviderState::Empty => SearchProviderState::Empty,
        DomainProviderState::NotEnabled => SearchProviderState::NotEnabled,
        DomainProviderState::Skipped => SearchProviderState::Skipped,
        DomainProviderState::Partial => SearchProviderState::Partial,
        DomainProviderState::Error => SearchProviderState::Error,
    }
}

fn provider_coverage_to_proto(coverage: &ProviderCoverage) -> SearchProviderCoverage {
    SearchProviderCoverage {
        eligible_units: coverage.eligible_units,
        searched_units: coverage.searched_units,
        failed_units: coverage.failed_units,
        returned_count: coverage.returned_count,
        has_more: coverage.has_more,
        budget_exhausted: coverage.budget_exhausted,
        timed_out: coverage.timed_out,
        stale_index: coverage.stale_index,
    }
}

fn surface_kind_to_proto(surface_kind: SearchSurfaceKind) -> ProtoSearchSurfaceKind {
    match surface_kind {
        SearchSurfaceKind::Table => ProtoSearchSurfaceKind::Table,
        SearchSurfaceKind::TableFunction => ProtoSearchSurfaceKind::TableFunction,
    }
}

fn field_role_to_proto(field_role: SearchFieldRole) -> ProtoSearchFieldRole {
    match field_role {
        SearchFieldRole::TableColumn => ProtoSearchFieldRole::TableColumn,
        SearchFieldRole::TableFilter => ProtoSearchFieldRole::TableFilter,
        SearchFieldRole::TableFunctionArgument => ProtoSearchFieldRole::TableFunctionArgument,
        SearchFieldRole::TableFunctionResultColumn => {
            ProtoSearchFieldRole::TableFunctionResultColumn
        }
    }
}

#[cfg(test)]
mod tests {
    use coral_api::v1::SearchProviderState as ProtoSearchProviderState;

    use super::provider_status_to_proto;
    use crate::search::result::{
        ProviderCoverage, ProviderStatus, SearchProviderKind,
        SearchProviderState as DomainProviderState,
    };

    #[test]
    fn skipped_provider_status_maps_without_coverage() {
        let proto = provider_status_to_proto(ProviderStatus {
            provider: SearchProviderKind::NativeFanout,
            state: DomainProviderState::Skipped,
            note: "not eligible for this request".to_string(),
            coverage: Some(ProviderCoverage::default()),
        });

        assert_eq!(
            ProtoSearchProviderState::try_from(proto.state).expect("provider state"),
            ProtoSearchProviderState::Skipped
        );
        assert!(
            proto.coverage.is_none(),
            "skipped provider coverage should be absent"
        );
    }
}
