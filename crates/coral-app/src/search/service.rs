//! Implements the gRPC `SearchService`.

use coral_api::v1::search_clear_target::Target as ProtoSearchClearTargetKind;
use coral_api::v1::search_maintenance_result::Detail as ProtoMaintenanceDetail;
use coral_api::v1::search_result::Shape as ProtoShape;
use coral_api::v1::search_service_server::SearchService as SearchServiceApi;
use coral_api::v1::{
    CatalogClearResult as ProtoCatalogClearResult,
    CatalogRebuildResult as ProtoCatalogRebuildResult,
    ClearSearchDataRequest as ProtoClearSearchDataRequest,
    ClearSearchDataResponse as ProtoClearSearchDataResponse,
    DrainSearchQueueRequest as ProtoDrainSearchQueueRequest,
    DrainSearchQueueResponse as ProtoDrainSearchQueueResponse,
    ObservedClearResult as ProtoObservedClearResult,
    ObservedDrainResult as ProtoObservedDrainResult,
    ObservedRebuildResult as ProtoObservedRebuildResult,
    RebuildSearchIndexRequest as ProtoRebuildSearchIndexRequest,
    RebuildSearchIndexResponse as ProtoRebuildSearchIndexResponse,
    SearchDataScope as ProtoSearchDataScope, SearchField, SearchFieldValues, SearchFunctionShape,
    SearchIndexProvider as ProtoSearchIndexProvider,
    SearchMaintenanceResult as ProtoSearchMaintenanceResult,
    SearchMaintenanceState as ProtoSearchMaintenanceState, SearchProvider as ProtoSearchProvider,
    SearchProviderCoverage, SearchProviderState, SearchProviderStatus,
    SearchRequest as ProtoSearchRequest, SearchResponse as ProtoSearchResponse,
    SearchResult as ProtoSearchResult, SearchResultTruncation,
    SearchStorageCleanupResult as ProtoSearchStorageCleanupResult, SearchSurfaceRef,
    SearchTableShape,
};
use tonic::{Request, Response, Status};

use crate::bootstrap::{AppError, app_status};
use crate::query::QueryAttribution;
use crate::request_context::RequestContext;
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
    Field, FieldValues, ProviderCoverage, ProviderStatus, SearchManagerError, SearchProviderKind,
    SearchProviderState as DomainProviderState, SearchRequest, SearchResponse,
    SearchResult as DomainSearchResult, SearchSurfaceId, SurfaceShape,
};
use crate::sources::SourceName;
use crate::task::manager::TaskManager;
use crate::task::service::task_manager_status;
use crate::transport::{grpc_span, instrument_grpc, request_context, workspace_name_from_proto};
use crate::workspaces::WorkspaceName;
use crate::workspaces::authorization::{WorkspaceAction, WorkspaceAuthorizer};

#[derive(Clone)]
pub(crate) struct SearchService {
    search: SearchManager,
    tasks: TaskManager,
    authorizer: WorkspaceAuthorizer,
}

impl SearchService {
    pub(crate) const fn new(
        search_manager: SearchManager,
        task_manager: TaskManager,
        authorizer: WorkspaceAuthorizer,
    ) -> Self {
        Self {
            search: search_manager,
            tasks: task_manager,
            authorizer,
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
        let authorizer = self.authorizer.clone();
        let request_context = request_context(&request)?.clone();
        Box::pin(instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            // Settled before anything else is read from the request: a caller
            // who may not reach this workspace must not be able to learn from
            // it whether their task id exists or their query is searchable.
            authorizer
                .authorize(
                    request_context.principal(),
                    &workspace_name,
                    WorkspaceAction::Read,
                )
                .await
                .map_err(app_status)?;
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

    async fn rebuild_search_index(
        &self,
        request: Request<ProtoRebuildSearchIndexRequest>,
    ) -> Result<Response<ProtoRebuildSearchIndexResponse>, Status> {
        let span = grpc_span(&request);
        let search = self.search.clone();
        let authorizer = self.authorizer.clone();
        let request_context = request_context(&request)?.clone();
        Box::pin(instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            authorize_maintenance(&authorizer, &workspace_name, &request_context).await?;
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
        let authorizer = self.authorizer.clone();
        let request_context = request_context(&request)?.clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            authorize_maintenance(&authorizer, &workspace_name, &request_context).await?;
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
        let authorizer = self.authorizer.clone();
        let request_context = request_context(&request)?.clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            authorize_maintenance(&authorizer, &workspace_name, &request_context).await?;
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

/// Settles owner access to `workspace` before any maintenance work.
///
/// The order is the point: rebuilding, draining, and clearing all reach this
/// immediately after parsing their workspace, so a caller who may not manage
/// it never causes an index to be read or a stored row to be removed, and
/// never learns from the request's own validation that the workspace is there.
async fn authorize_maintenance(
    authorizer: &WorkspaceAuthorizer,
    workspace: &WorkspaceName,
    request_context: &RequestContext,
) -> Result<(), Status> {
    authorizer
        .authorize(
            request_context.principal(),
            workspace,
            WorkspaceAction::Manage,
        )
        .await
        .map_err(app_status)
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
            .map(search_result_to_proto)
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

fn search_result_to_proto(result: DomainSearchResult) -> ProtoSearchResult {
    let entry = result.surface;
    ProtoSearchResult {
        surface: Some(surface_ref_to_proto(&entry.id)),
        description: entry.description,
        guide: entry.guide,
        shape: Some(shape_to_proto(entry.shape)),
        matching_values: result
            .matching_values
            .into_iter()
            .map(field_values_to_proto)
            .collect(),
        omitted_matching_field_count: result.omitted_matching_field_count,
        providers: result
            .providers
            .into_iter()
            .map(provider_kind_to_proto)
            .map(|provider| provider as i32)
            .collect(),
    }
}

fn surface_ref_to_proto(id: &SearchSurfaceId) -> SearchSurfaceRef {
    SearchSurfaceRef {
        catalog_name: id.catalog_name.clone().unwrap_or_default(),
        schema_name: id.schema_name.clone(),
        name: id.name.clone(),
    }
}

fn shape_to_proto(shape: SurfaceShape) -> ProtoShape {
    match shape {
        SurfaceShape::Table { fields } => ProtoShape::Table(SearchTableShape {
            fields: fields.into_iter().map(field_to_proto).collect(),
        }),
        SurfaceShape::Function { arguments, returns } => {
            ProtoShape::Function(SearchFunctionShape {
                arguments: arguments.into_iter().map(field_to_proto).collect(),
                returns: returns.into_iter().map(field_to_proto).collect(),
            })
        }
    }
}

fn field_to_proto(field: Field) -> SearchField {
    SearchField {
        name: field.name,
        data_type: field.data_type,
        required: field.required,
    }
}

fn field_values_to_proto(values: FieldValues) -> SearchFieldValues {
    SearchFieldValues {
        field: values.field,
        values: values.values,
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use coral_api::v1::{
        SearchClearTarget as ProtoSearchClearTarget,
        SearchProviderState as ProtoSearchProviderState, search_clear_target,
    };
    use coral_engine::QueryRuntimeContext;
    use tempfile::TempDir;
    use tonic::{Code, Request};

    use super::{
        ProtoClearSearchDataRequest, ProtoDrainSearchQueueRequest, ProtoRebuildSearchIndexRequest,
        ProtoSearchDataScope, ProtoSearchRequest, SearchService, SearchServiceApi,
        clear_target_from_proto, provider_status_to_proto,
    };
    use crate::catalog::discovery::CatalogDiscovery;
    use crate::identity::Principal;
    use crate::query::manager::QueryManager;
    use crate::request_context::RequestContext;
    use crate::search::maintenance::SearchClearTarget;
    use crate::search::manager::SearchManager;
    use crate::search::result::{
        ProviderCoverage, ProviderStatus, SearchProviderKind,
        SearchProviderState as DomainProviderState,
    };
    use crate::state::db::CoralDb;
    use crate::task::manager::TaskManager;
    use crate::task::store::TaskStore;
    use crate::test_support::{create_workspace, migrated_deployment, seed_principal};
    use crate::workspaces::authorization::WorkspaceAuthorizer;
    use crate::workspaces::{MemberRole, WorkspaceName};

    /// This suite's login issuer. Each suite provisions under its own, so a
    /// subject seeded here is a different person from the same subject
    /// seeded elsewhere.
    const ISSUER: &str = "https://issuer.test/search-authorization";

    /// A provider value no enum admits. Reaching the maintenance request build
    /// with it answers `InvalidArgument`, so a refusal that answers anything
    /// else proves the request was never interpreted.
    const UNDECODABLE_PROVIDER: i32 = 9_999;

    struct Fixture {
        _temp: TempDir,
        service: SearchService,
        db: Arc<CoralDb>,
    }

    /// The workspace these fixtures run in.
    ///
    /// An install provisions none, so [`fixture`] creates it explicitly. The
    /// name is ordinary on purpose: a fixture that leaned on a well-known one
    /// would prove the workspace was resolved by name rather than created.
    fn test_workspace() -> WorkspaceName {
        WorkspaceName::parse("work").expect("workspace name")
    }

    /// A shared deployment over one migrated database holding one created
    /// workspace, so every caller's authority comes from a membership row.
    async fn fixture() -> Fixture {
        let deployment = migrated_deployment().await;
        create_workspace(&deployment.db, &test_workspace()).await;
        let (temp, layout, config_store, db, workspaces) = (
            deployment.temp,
            deployment.layout,
            deployment.config_store,
            deployment.db,
            deployment.workspaces,
        );
        let queries = QueryManager::new_for_tests(
            config_store.clone(),
            workspaces.clone(),
            deployment.credentials,
            QueryRuntimeContext::default(),
            layout.clone(),
            Vec::new(),
        );
        let lifecycle_lock = workspaces.lifecycle_lock();
        let search = SearchManager::new(
            layout,
            &config_store,
            workspaces,
            true,
            CatalogDiscovery::new(queries),
            lifecycle_lock,
        );
        Fixture {
            _temp: temp,
            service: SearchService::new(
                search,
                TaskManager::new(TaskStore::new(Arc::clone(&db))),
                WorkspaceAuthorizer::new(Arc::clone(&db)),
            ),
            db,
        }
    }

    fn request<T>(message: T, principal: &Principal) -> Request<T> {
        let mut request = Request::new(message);
        request
            .extensions_mut()
            .insert(RequestContext::new(principal.clone()));
        request
    }

    fn workspace() -> coral_api::v1::Workspace {
        crate::transport::workspace_to_proto(&test_workspace())
    }

    /// An empty query is what search preparation rejects, so it stands as the
    /// probe for whether preparation was reached at all.
    fn search_request() -> ProtoSearchRequest {
        ProtoSearchRequest {
            workspace: Some(workspace()),
            query: String::new(),
            limit: 0,
        }
    }

    fn rebuild_request() -> ProtoRebuildSearchIndexRequest {
        ProtoRebuildSearchIndexRequest {
            workspace: Some(workspace()),
            provider: UNDECODABLE_PROVIDER,
            force: true,
        }
    }

    fn clear_request() -> ProtoClearSearchDataRequest {
        ProtoClearSearchDataRequest {
            workspace: Some(workspace()),
            scope: ProtoSearchDataScope::Unspecified as i32,
            target: None,
        }
    }

    /// Searching reads the workspace; rebuilding, draining, and clearing its
    /// index are maintenance of it.
    ///
    /// Every request here carries input the work itself rejects, so each
    /// refusal is an absence rather than an error code: the caller who is
    /// allowed through is told what is wrong with the request, and the caller
    /// who is not never gets that far.
    #[tokio::test]
    async fn members_search_while_only_owners_maintain_the_index() {
        let fixture = fixture().await;
        let owner = seed_principal(&fixture.db, ISSUER, &test_workspace(), "owner", Some(MemberRole::Owner)).await;
        let member = seed_principal(&fixture.db, ISSUER, &test_workspace(), "member", Some(MemberRole::Member)).await;
        let outsider = seed_principal(&fixture.db, ISSUER, &test_workspace(), "outsider", None).await;

        assert_eq!(
            fixture
                .service
                .search(request(search_request(), &member))
                .await
                .expect_err("the query is what the member is stopped by")
                .code(),
            Code::InvalidArgument
        );
        assert_eq!(
            fixture
                .service
                .search(request(search_request(), &outsider))
                .await
                .expect_err("a non-member searches nothing")
                .code(),
            Code::NotFound
        );

        for status in [
            fixture
                .service
                .rebuild_search_index(request(rebuild_request(), &member))
                .await
                .expect_err("a member rebuilds nothing"),
            fixture
                .service
                .clear_search_data(request(clear_request(), &member))
                .await
                .expect_err("a member clears nothing"),
            fixture
                .service
                .drain_search_queue(request(
                    ProtoDrainSearchQueueRequest {
                        workspace: Some(workspace()),
                        budget_ms: 1,
                    },
                    &member,
                ))
                .await
                .expect_err("a member drains nothing"),
        ] {
            assert_eq!(status.code(), Code::PermissionDenied);
        }

        for status in [
            fixture
                .service
                .rebuild_search_index(request(rebuild_request(), &owner))
                .await
                .expect_err("the provider is what the owner is stopped by"),
            fixture
                .service
                .clear_search_data(request(clear_request(), &owner))
                .await
                .expect_err("the scope is what the owner is stopped by"),
        ] {
            assert_eq!(status.code(), Code::InvalidArgument);
        }
    }

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
}
