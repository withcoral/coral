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
    GetSearchCapabilitiesRequest as ProtoGetSearchCapabilitiesRequest,
    GetSearchCapabilitiesResponse as ProtoGetSearchCapabilitiesResponse,
    NativeSearchAttribute as ProtoNativeSearchAttribute,
    NativeSearchDiagnostic as ProtoNativeSearchDiagnostic,
    NativeSearchDiagnosticReason as ProtoNativeSearchDiagnosticReason,
    NativeSearchDiagnosticState as ProtoNativeSearchDiagnosticState,
    NativeSearchResult as ProtoNativeSearchResult, ObservedClearResult as ProtoObservedClearResult,
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
    SearchRouteIdentity as ProtoSearchRouteIdentity,
    SearchStorageCleanupResult as ProtoSearchStorageCleanupResult,
    SearchSurfaceKind as ProtoSearchSurfaceKind, SearchTableColumnPreview,
    SearchTableColumnPreviewColumn,
};
use tonic::{Request, Response, Status};

use crate::bootstrap::{AppError, app_status};
use crate::query::QueryAttribution;
use crate::search::capabilities::SearchCapabilities;
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
    CatalogMetadataResult, ColumnHintResult, NativeSearchDiagnostic, NativeSearchDiagnosticReason,
    NativeSearchDiagnosticState, NativeSearchResult, ObservedValueResult, ProviderCoverage,
    ProviderStatus, SearchFieldRole, SearchManagerError, SearchPayload, SearchProviderKind,
    SearchProviderState as DomainProviderState, SearchRequest, SearchResponse, SearchSurfaceKind,
    TableColumnPreview as DomainTableColumnPreview,
};
use crate::sources::SourceName;
use crate::task::manager::TaskManager;
use crate::task::service::task_manager_status;
use crate::transport::{
    catalog_item_to_proto, grpc_span, instrument_grpc, request_context, workspace_name_from_proto,
};

#[derive(Clone)]
pub(crate) struct SearchService {
    search: SearchManager,
    tasks: TaskManager,
}

impl SearchService {
    pub(crate) fn new(search_manager: SearchManager, task_manager: TaskManager) -> Self {
        Self {
            search: search_manager,
            tasks: task_manager,
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
        let tasks = self.tasks.clone();
        let request_context = request_context(&request)?.clone();
        Box::pin(instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            let attribution = QueryAttribution::new(
                tasks
                    .validate_attribution(&workspace_name, request_context.task_id())
                    .await
                    .map_err(task_manager_status)?,
            );
            let request = SearchRequest::new(workspace_name, &request.query, request.limit)
                .map_err(search_status)?;
            let response = search
                .search(&request, &attribution)
                .await
                .map_err(search_status)?;
            Ok(Response::new(search_response_to_proto(response)))
        }))
        .await
    }

    async fn get_search_capabilities(
        &self,
        request: Request<ProtoGetSearchCapabilitiesRequest>,
    ) -> Result<Response<ProtoGetSearchCapabilitiesResponse>, Status> {
        let span = grpc_span(&request);
        let search = self.search.clone();
        Box::pin(instrument_grpc(span, async move {
            let attribution = QueryAttribution::from_extensions(request.extensions());
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            let capabilities = search
                .capabilities(&workspace_name, &attribution)
                .await
                .map_err(search_status)?;
            Ok(Response::new(search_capabilities_to_proto(capabilities)))
        }))
        .await
    }

    async fn rebuild_search_index(
        &self,
        request: Request<ProtoRebuildSearchIndexRequest>,
    ) -> Result<Response<ProtoRebuildSearchIndexResponse>, Status> {
        let span = grpc_span(&request);
        let search = self.search.clone();
        Box::pin(instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            let request = DomainRebuildSearchIndexRequest {
                workspace_name,
                provider: index_provider_from_proto(proto_index_provider(request.provider)?),
                force: request.force,
            };
            let response = search
                .rebuild_index(&request)
                .await
                .map_err(search_status)?;
            Ok(Response::new(rebuild_response_to_proto(response)))
        }))
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
            let response = search.drain_queue(&request).await.map_err(search_status)?;
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
            let response = search.clear_data(&request).await.map_err(search_status)?;
            Ok(Response::new(clear_response_to_proto(response)))
        })
        .await
    }
}

fn search_capabilities_to_proto(
    capabilities: SearchCapabilities,
) -> ProtoGetSearchCapabilitiesResponse {
    ProtoGetSearchCapabilitiesResponse {
        provider_fanout_enabled: capabilities.provider_fanout_enabled,
        eligible_routes: capabilities
            .eligible_routes
            .into_iter()
            .map(|route| ProtoSearchRouteIdentity {
                source_name: route.source_name,
                function_name: route.function_name,
                authored_route_id: route.authored_route_id,
            })
            .collect(),
        truncated: capabilities.truncated,
        omitted_route_count: capabilities.omitted_route_count,
    }
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
        SearchMaintenanceState::Skipped => ProtoSearchMaintenanceState::Skipped,
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
        stale_rows_purged: result.stale_rows_purged,
        evicted_rows: result.evicted_rows,
        storage_limit_reached: result.storage_limit_reached,
        storage_jobs_dropped: result.storage_jobs_dropped,
    }
}

fn observed_rebuild_to_proto(
    result: ObservedRebuildMaintenanceResult,
) -> ProtoObservedRebuildResult {
    ProtoObservedRebuildResult {
        canonical_rows_scanned: result.canonical_rows_scanned,
        fts_rows_rebuilt: result.fts_rows_rebuilt,
        drain: Some(observed_drain_to_proto(result.drain)),
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
        ProtoSearchDataScope::ObservedValues => Ok(SearchDataScope::ObservedValues),
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
        Some(ProtoSearchClearTargetKind::SourceName(source_name)) => {
            let parsed = SourceName::parse(&source_name).map_err(app_status)?;
            if parsed.as_str() != source_name {
                return Err(app_status(AppError::InvalidInput(
                    "search clear source_name target must not contain surrounding whitespace"
                        .to_string(),
                )));
            }
            Ok(SearchClearTarget::Source(parsed))
        }
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
        SearchPayload::NativeResult(result) => {
            Payload::NativeResult(native_result_to_proto(result))
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

fn native_result_to_proto(result: NativeSearchResult) -> ProtoNativeSearchResult {
    ProtoNativeSearchResult {
        schema_name: result.schema_name,
        function_name: result.function_name,
        row_ordinal: result.row_ordinal,
        entity_type: result.entity_type,
        provider_id: result.provider_id,
        title: result.title,
        url: result.url,
        snippet: result.snippet,
        attributes: result
            .attributes
            .into_iter()
            .map(|attribute| ProtoNativeSearchAttribute {
                name: attribute.name,
                display_value: attribute.display_value,
            })
            .collect(),
        omitted_attribute_count: result.omitted_attribute_count,
        content_truncated: result.content_truncated,
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
        diagnostics: status
            .diagnostics
            .into_iter()
            .map(native_diagnostic_to_proto)
            .collect(),
        diagnostics_truncated: status.diagnostics_truncated,
        omitted_diagnostic_count: status.omitted_diagnostic_count,
    }
}

fn native_diagnostic_to_proto(diagnostic: NativeSearchDiagnostic) -> ProtoNativeSearchDiagnostic {
    ProtoNativeSearchDiagnostic {
        source_name: diagnostic.source_name,
        function_name: diagnostic.function_name,
        authored_route_id: diagnostic.authored_route_id,
        state: native_diagnostic_state_to_proto(diagnostic.state) as i32,
        reason: native_diagnostic_reason_to_proto(diagnostic.reason) as i32,
        elapsed_ms: diagnostic.elapsed_ms,
        safe_candidate_count: diagnostic.safe_candidate_count,
        has_more: diagnostic.has_more,
    }
}

fn native_diagnostic_state_to_proto(
    state: NativeSearchDiagnosticState,
) -> ProtoNativeSearchDiagnosticState {
    match state {
        NativeSearchDiagnosticState::ResultsFound => ProtoNativeSearchDiagnosticState::ResultsFound,
        NativeSearchDiagnosticState::Empty => ProtoNativeSearchDiagnosticState::Empty,
        NativeSearchDiagnosticState::Skipped => ProtoNativeSearchDiagnosticState::Skipped,
        NativeSearchDiagnosticState::TimedOut => ProtoNativeSearchDiagnosticState::TimedOut,
        NativeSearchDiagnosticState::Cancelled => ProtoNativeSearchDiagnosticState::Cancelled,
        NativeSearchDiagnosticState::Error => ProtoNativeSearchDiagnosticState::Error,
    }
}

fn native_diagnostic_reason_to_proto(
    reason: NativeSearchDiagnosticReason,
) -> ProtoNativeSearchDiagnosticReason {
    match reason {
        NativeSearchDiagnosticReason::Unspecified => ProtoNativeSearchDiagnosticReason::Unspecified,
        NativeSearchDiagnosticReason::NotAuthorized => {
            ProtoNativeSearchDiagnosticReason::NotAuthorized
        }
        NativeSearchDiagnosticReason::AmbiguousRoute => {
            ProtoNativeSearchDiagnosticReason::AmbiguousRoute
        }
        NativeSearchDiagnosticReason::InvalidSearchLimits => {
            ProtoNativeSearchDiagnosticReason::InvalidSearchLimits
        }
        NativeSearchDiagnosticReason::QueryInputUnmappable => {
            ProtoNativeSearchDiagnosticReason::QueryInputUnmappable
        }
        NativeSearchDiagnosticReason::MissingArgumentDefault => {
            ProtoNativeSearchDiagnosticReason::MissingArgumentDefault
        }
        NativeSearchDiagnosticReason::RouteStale => ProtoNativeSearchDiagnosticReason::RouteStale,
        NativeSearchDiagnosticReason::UnsafeOperation => {
            ProtoNativeSearchDiagnosticReason::UnsafeOperation
        }
        NativeSearchDiagnosticReason::NoSafeDisplayFields => {
            ProtoNativeSearchDiagnosticReason::NoSafeDisplayFields
        }
        NativeSearchDiagnosticReason::FanoutLimitReached => {
            ProtoNativeSearchDiagnosticReason::FanoutLimitReached
        }
        NativeSearchDiagnosticReason::InsufficientBudget => {
            ProtoNativeSearchDiagnosticReason::InsufficientBudget
        }
        NativeSearchDiagnosticReason::GlobalBudgetExhausted => {
            ProtoNativeSearchDiagnosticReason::GlobalBudgetExhausted
        }
        NativeSearchDiagnosticReason::CallTimeout => ProtoNativeSearchDiagnosticReason::CallTimeout,
        NativeSearchDiagnosticReason::Cancelled => ProtoNativeSearchDiagnosticReason::Cancelled,
        NativeSearchDiagnosticReason::RateLimited => ProtoNativeSearchDiagnosticReason::RateLimited,
        NativeSearchDiagnosticReason::AuthOrPermissionFailed => {
            ProtoNativeSearchDiagnosticReason::AuthOrPermissionFailed
        }
        NativeSearchDiagnosticReason::UpstreamUnavailable => {
            ProtoNativeSearchDiagnosticReason::UpstreamUnavailable
        }
        NativeSearchDiagnosticReason::InvalidResponse => {
            ProtoNativeSearchDiagnosticReason::InvalidResponse
        }
        NativeSearchDiagnosticReason::ExecutionFailed => {
            ProtoNativeSearchDiagnosticReason::ExecutionFailed
        }
        NativeSearchDiagnosticReason::UnsupportedCancellation => {
            ProtoNativeSearchDiagnosticReason::UnsupportedCancellation
        }
        NativeSearchDiagnosticReason::InternalError => {
            ProtoNativeSearchDiagnosticReason::InternalError
        }
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
    use coral_api::v1::{
        NativeSearchDiagnostic as ProtoNativeSearchDiagnostic,
        NativeSearchDiagnosticReason as ProtoNativeSearchDiagnosticReason,
        NativeSearchDiagnosticState as ProtoNativeSearchDiagnosticState,
        SearchClearTarget as ProtoSearchClearTarget,
        SearchProviderState as ProtoSearchProviderState,
        SearchRouteIdentity as ProtoSearchRouteIdentity, search_clear_target,
    };
    use prost::Message as _;
    use tonic::Code;

    use super::{
        Payload, clear_target_from_proto, native_diagnostic_reason_to_proto,
        native_diagnostic_state_to_proto, provider_status_to_proto, search_capabilities_to_proto,
        search_payload_to_proto,
    };
    use crate::search::capabilities::{
        SearchCapabilities, SearchRouteIdentity as DomainSearchRouteIdentity,
    };
    use crate::search::maintenance::SearchClearTarget;
    use crate::search::result::{
        NativeSearchAttribute, NativeSearchDiagnostic, NativeSearchDiagnosticReason,
        NativeSearchDiagnosticState, NativeSearchResult, ProviderCoverage, ProviderStatus,
        SearchPayload, SearchProviderKind, SearchProviderState as DomainProviderState,
    };

    fn round_trip_diagnostic(state: i32, reason: i32) -> ProtoNativeSearchDiagnostic {
        let diagnostic = ProtoNativeSearchDiagnostic {
            state,
            reason,
            ..ProtoNativeSearchDiagnostic::default()
        };
        ProtoNativeSearchDiagnostic::decode(diagnostic.encode_to_vec().as_slice())
            .expect("native diagnostic protobuf round trip")
    }

    #[test]
    fn skipped_provider_status_maps_without_coverage() {
        let proto = provider_status_to_proto(ProviderStatus {
            provider: SearchProviderKind::NativeFanout,
            state: DomainProviderState::Skipped,
            note: "not eligible for this request".to_string(),
            coverage: Some(ProviderCoverage::default()),
            diagnostics: Vec::new(),
            diagnostics_truncated: false,
            omitted_diagnostic_count: 0,
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

    #[test]
    fn capability_route_mapping_exposes_one_source_identity() {
        let response = search_capabilities_to_proto(SearchCapabilities {
            provider_fanout_enabled: true,
            eligible_routes: vec![DomainSearchRouteIdentity {
                source_name: "github".to_string(),
                function_name: "search_issues".to_string(),
                authored_route_id: Some("issues".to_string()),
            }],
            truncated: false,
            omitted_route_count: 0,
        });
        let route = response.eligible_routes.first().expect("capability route");
        let route = ProtoSearchRouteIdentity::decode(route.encode_to_vec().as_slice())
            .expect("capability route protobuf round trip");

        assert_eq!(route.source_name, "github");
        assert_eq!(route.function_name, "search_issues");
        assert_eq!(route.authored_route_id.as_deref(), Some("issues"));
    }

    #[test]
    fn clear_source_target_is_parsed_into_source_identity() {
        for source_name in ["github_v4", "github issues"] {
            let target = clear_target_from_proto(Some(ProtoSearchClearTarget {
                target: Some(search_clear_target::Target::SourceName(
                    source_name.to_string(),
                )),
            }))
            .expect("valid source target");

            let SearchClearTarget::Source(parsed) = target else {
                panic!("expected source target");
            };
            assert_eq!(parsed.as_str(), source_name);
        }
    }

    #[test]
    fn native_result_mapping_preserves_optional_fields_and_attribute_order() {
        let payload = search_payload_to_proto(
            &crate::workspaces::WorkspaceName::default(),
            SearchPayload::NativeResult(NativeSearchResult {
                schema_name: "github".to_string(),
                function_name: "search_issues".to_string(),
                row_ordinal: 2,
                entity_type: Some("issue".to_string()),
                provider_id: None,
                title: Some("Fix native search".to_string()),
                url: None,
                snippet: Some("Compact preview".to_string()),
                attributes: vec![
                    NativeSearchAttribute {
                        name: "state".to_string(),
                        display_value: "open".to_string(),
                    },
                    NativeSearchAttribute {
                        name: "author".to_string(),
                        display_value: "octocat".to_string(),
                    },
                ],
                omitted_attribute_count: 3,
                content_truncated: true,
            }),
        );
        let Payload::NativeResult(proto) = payload else {
            panic!("native result should map to the native protobuf payload");
        };
        let proto = coral_api::v1::NativeSearchResult::decode(proto.encode_to_vec().as_slice())
            .expect("native result protobuf round trip");

        assert_eq!(proto.schema_name, "github");
        assert_eq!(proto.function_name, "search_issues");
        assert_eq!(proto.row_ordinal, 2);
        assert_eq!(proto.entity_type.as_deref(), Some("issue"));
        assert_eq!(proto.provider_id, None);
        assert_eq!(proto.title.as_deref(), Some("Fix native search"));
        assert_eq!(proto.url, None);
        assert_eq!(proto.snippet.as_deref(), Some("Compact preview"));
        assert_eq!(
            proto
                .attributes
                .iter()
                .map(|attribute| attribute.name.as_str())
                .collect::<Vec<_>>(),
            ["state", "author"]
        );
        assert_eq!(proto.omitted_attribute_count, 3);
        assert!(proto.content_truncated);
    }

    #[test]
    fn every_native_diagnostic_state_maps_to_the_stable_wire_value() {
        let cases = [
            (
                NativeSearchDiagnosticState::ResultsFound,
                ProtoNativeSearchDiagnosticState::ResultsFound,
            ),
            (
                NativeSearchDiagnosticState::Empty,
                ProtoNativeSearchDiagnosticState::Empty,
            ),
            (
                NativeSearchDiagnosticState::Skipped,
                ProtoNativeSearchDiagnosticState::Skipped,
            ),
            (
                NativeSearchDiagnosticState::TimedOut,
                ProtoNativeSearchDiagnosticState::TimedOut,
            ),
            (
                NativeSearchDiagnosticState::Cancelled,
                ProtoNativeSearchDiagnosticState::Cancelled,
            ),
            (
                NativeSearchDiagnosticState::Error,
                ProtoNativeSearchDiagnosticState::Error,
            ),
        ];

        for (index, (domain, expected)) in cases.into_iter().enumerate() {
            let actual = native_diagnostic_state_to_proto(domain);
            assert_eq!(actual, expected);
            assert_eq!(actual as i32, i32::try_from(index + 1).expect("wire value"));
            assert_eq!(round_trip_diagnostic(actual as i32, 0).state, actual as i32);
        }
    }

    #[test]
    fn clear_source_target_rejects_unsafe_source_identities() {
        for source_name in [
            "",
            " github",
            "github ",
            "github/child",
            r"github\child",
            ".",
            "..",
        ] {
            let status = clear_target_from_proto(Some(ProtoSearchClearTarget {
                target: Some(search_clear_target::Target::SourceName(
                    source_name.to_string(),
                )),
            }))
            .expect_err("unsafe source target should fail");

            assert_eq!(
                status.code(),
                Code::InvalidArgument,
                "source={source_name:?}"
            );
        }
    }

    #[test]
    fn every_native_diagnostic_reason_maps_to_the_stable_wire_value() {
        let cases = [
            (
                NativeSearchDiagnosticReason::NotAuthorized,
                ProtoNativeSearchDiagnosticReason::NotAuthorized,
            ),
            (
                NativeSearchDiagnosticReason::AmbiguousRoute,
                ProtoNativeSearchDiagnosticReason::AmbiguousRoute,
            ),
            (
                NativeSearchDiagnosticReason::InvalidSearchLimits,
                ProtoNativeSearchDiagnosticReason::InvalidSearchLimits,
            ),
            (
                NativeSearchDiagnosticReason::QueryInputUnmappable,
                ProtoNativeSearchDiagnosticReason::QueryInputUnmappable,
            ),
            (
                NativeSearchDiagnosticReason::MissingArgumentDefault,
                ProtoNativeSearchDiagnosticReason::MissingArgumentDefault,
            ),
            (
                NativeSearchDiagnosticReason::RouteStale,
                ProtoNativeSearchDiagnosticReason::RouteStale,
            ),
            (
                NativeSearchDiagnosticReason::UnsafeOperation,
                ProtoNativeSearchDiagnosticReason::UnsafeOperation,
            ),
            (
                NativeSearchDiagnosticReason::NoSafeDisplayFields,
                ProtoNativeSearchDiagnosticReason::NoSafeDisplayFields,
            ),
            (
                NativeSearchDiagnosticReason::FanoutLimitReached,
                ProtoNativeSearchDiagnosticReason::FanoutLimitReached,
            ),
            (
                NativeSearchDiagnosticReason::InsufficientBudget,
                ProtoNativeSearchDiagnosticReason::InsufficientBudget,
            ),
            (
                NativeSearchDiagnosticReason::GlobalBudgetExhausted,
                ProtoNativeSearchDiagnosticReason::GlobalBudgetExhausted,
            ),
            (
                NativeSearchDiagnosticReason::CallTimeout,
                ProtoNativeSearchDiagnosticReason::CallTimeout,
            ),
            (
                NativeSearchDiagnosticReason::Cancelled,
                ProtoNativeSearchDiagnosticReason::Cancelled,
            ),
            (
                NativeSearchDiagnosticReason::RateLimited,
                ProtoNativeSearchDiagnosticReason::RateLimited,
            ),
            (
                NativeSearchDiagnosticReason::AuthOrPermissionFailed,
                ProtoNativeSearchDiagnosticReason::AuthOrPermissionFailed,
            ),
            (
                NativeSearchDiagnosticReason::UpstreamUnavailable,
                ProtoNativeSearchDiagnosticReason::UpstreamUnavailable,
            ),
            (
                NativeSearchDiagnosticReason::InvalidResponse,
                ProtoNativeSearchDiagnosticReason::InvalidResponse,
            ),
            (
                NativeSearchDiagnosticReason::ExecutionFailed,
                ProtoNativeSearchDiagnosticReason::ExecutionFailed,
            ),
            (
                NativeSearchDiagnosticReason::UnsupportedCancellation,
                ProtoNativeSearchDiagnosticReason::UnsupportedCancellation,
            ),
            (
                NativeSearchDiagnosticReason::InternalError,
                ProtoNativeSearchDiagnosticReason::InternalError,
            ),
        ];

        for (index, (domain, expected)) in cases.into_iter().enumerate() {
            let actual = native_diagnostic_reason_to_proto(domain);
            assert_eq!(actual, expected);
            assert_eq!(actual as i32, i32::try_from(index + 1).expect("wire value"));
            assert_eq!(
                round_trip_diagnostic(0, actual as i32).reason,
                actual as i32
            );
        }
    }

    #[test]
    fn protobuf_round_trip_preserves_unknown_native_diagnostic_enums() {
        let diagnostic = round_trip_diagnostic(999, 998);

        assert_eq!(diagnostic.state, 999);
        assert_eq!(diagnostic.reason, 998);
    }

    #[test]
    fn native_diagnostic_tag_one_decodes_as_source_and_retired_tag_two_is_ignored() {
        let diagnostic =
            ProtoNativeSearchDiagnostic::decode(b"\x0a\x06github\x12\x06github".as_slice())
                .expect("decode source name plus retired schema field");

        assert_eq!(diagnostic.source_name, "github");
        assert_eq!(
            diagnostic.encode_to_vec(),
            b"\x0a\x06github",
            "prost should discard the retired tag-2 schema field"
        );
    }

    #[test]
    fn native_diagnostics_preserve_resolution_absence_and_elapsed_width() {
        let proto = provider_status_to_proto(ProviderStatus {
            provider: SearchProviderKind::NativeFanout,
            state: DomainProviderState::Partial,
            note: "one route was unresolved".to_string(),
            coverage: Some(ProviderCoverage::default()),
            diagnostics: vec![NativeSearchDiagnostic {
                source_name: "github".to_string(),
                function_name: None,
                authored_route_id: Some("issues".to_string()),
                state: NativeSearchDiagnosticState::Skipped,
                reason: NativeSearchDiagnosticReason::AmbiguousRoute,
                elapsed_ms: u64::MAX,
                safe_candidate_count: 0,
                has_more: false,
            }],
            diagnostics_truncated: true,
            omitted_diagnostic_count: 7,
        });

        let diagnostic = proto.diagnostics.first().expect("diagnostic");
        assert_eq!(diagnostic.source_name, "github");
        assert_eq!(diagnostic.function_name, None);
        assert_eq!(diagnostic.authored_route_id.as_deref(), Some("issues"));
        assert_eq!(diagnostic.elapsed_ms, u64::MAX);
        assert!(proto.diagnostics_truncated);
        assert_eq!(proto.omitted_diagnostic_count, 7);
    }
}
