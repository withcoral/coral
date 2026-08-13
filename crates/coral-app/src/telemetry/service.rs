//! Implements the gRPC `TraceService` for trace inspection.

use coral_api::v1::trace_service_server::TraceService as TraceServiceApi;
use coral_api::v1::{
    GetTraceRequest, GetTraceResponse, ListTracesRequest, ListTracesResponse, TraceInvocationKind,
    TraceOperationKind, TraceSpan, TraceStatus, TraceSummary, TraceView, Workspace,
};
use tonic::{Code, Request, Response, Status};

use crate::bootstrap::app_status;
use crate::identity::Principal;
use crate::telemetry::local_store::{
    OwnedWorkspaceScope, StoredTraceInvocationKind, StoredTraceOperationKind, StoredTraceStatus,
    TraceDetailRecord, TraceSpanRecord, TraceSummaryRecord,
};
use crate::telemetry::manager::{
    GetTraceQuery, ListTracesQuery, TraceAccessScope, TraceListView, TraceManager,
    TraceManagerError,
};
use crate::transport::{grpc_span, instrument_grpc, request_context};
use crate::workspaces::{
    LocalPrincipalPolicy, WorkspaceAction, WorkspaceAuthorizer, WorkspaceName,
};

const DEFAULT_TRACE_PAGE_SIZE: usize = 50;
const MAX_TRACE_PAGE_SIZE: usize = 200;
const OWNED_WORKSPACE_PAGE_SIZE: usize = 200;

#[derive(Clone)]
pub(crate) struct TraceService {
    traces: TraceManager,
    workspace_authorizer: WorkspaceAuthorizer,
}

impl TraceService {
    pub(crate) fn new(
        trace_manager: TraceManager,
        workspace_authorizer: WorkspaceAuthorizer,
    ) -> Self {
        Self {
            traces: trace_manager,
            workspace_authorizer,
        }
    }
}

#[tonic::async_trait]
impl TraceServiceApi for TraceService {
    async fn list_traces(
        &self,
        request: Request<ListTracesRequest>,
    ) -> Result<Response<ListTracesResponse>, Status> {
        let span = grpc_span(&request);
        let traces = self.traces.clone();
        let workspace_authorizer = self.workspace_authorizer.clone();
        let principal = request_context(&request)?.principal().clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace_name = workspace_filter_from_proto(request.workspace.as_ref())?;
            let scope =
                trace_access_scope(workspace_name, &workspace_authorizer, &principal).await?;
            let page_size = normalize_page_size(request.page_size);
            let offset = parse_page_token(&request.page_token)?;
            let view = trace_list_view_from_proto(request.view)?;
            let page = traces
                .list_traces(ListTracesQuery {
                    view,
                    scope,
                    page_size,
                    offset,
                })
                .await
                .map_err(trace_manager_status)?;
            Ok(Response::new(ListTracesResponse {
                traces: page
                    .traces
                    .into_iter()
                    .map(trace_summary_to_proto)
                    .collect(),
                next_page_token: page
                    .next_offset
                    .map_or_else(String::new, |offset| offset.to_string()),
            }))
        })
        .await
    }

    async fn get_trace(
        &self,
        request: Request<GetTraceRequest>,
    ) -> Result<Response<GetTraceResponse>, Status> {
        let span = grpc_span(&request);
        let traces = self.traces.clone();
        let workspace_authorizer = self.workspace_authorizer.clone();
        let principal = request_context(&request)?.principal().clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace_name = workspace_filter_from_proto(request.workspace.as_ref())?;
            let scope =
                trace_access_scope(workspace_name, &workspace_authorizer, &principal).await?;
            if request.trace_id.trim().is_empty() {
                return Err(Status::new(
                    Code::InvalidArgument,
                    "invalid input: missing trace_id",
                ));
            }
            let view = trace_list_view_from_proto(request.view)?;
            let trace = traces
                .get_trace(GetTraceQuery {
                    trace_id: request.trace_id,
                    scope,
                    view,
                })
                .await
                .map_err(trace_manager_status)?;
            Ok(Response::new(trace_detail_to_proto(trace)))
        })
        .await
    }
}

/// Resolves what the caller may read before any trace leaves the store.
///
/// A named workspace is authorized for `Manage`; the local principal reads
/// everything; every other caller is confined to the workspaces they own.
async fn trace_access_scope(
    workspace_name: Option<WorkspaceName>,
    authorizer: &WorkspaceAuthorizer,
    principal: &Principal,
) -> Result<TraceAccessScope, Status> {
    match workspace_name {
        Some(workspace_name) => {
            authorizer
                .authorize(principal, &workspace_name, WorkspaceAction::Manage)
                .await
                .map_err(app_status)?;
            Ok(TraceAccessScope::Workspace(workspace_name))
        }
        None if authorizer.local_principal_policy() == LocalPrincipalPolicy::ImplicitOwner
            && principal.is_local() =>
        {
            Ok(TraceAccessScope::Unrestricted)
        }
        None => Ok(TraceAccessScope::Owned(
            owned_workspace_scope(authorizer, principal).await?,
        )),
    }
}

async fn owned_workspace_scope(
    authorizer: &WorkspaceAuthorizer,
    principal: &Principal,
) -> Result<OwnedWorkspaceScope, Status> {
    let mut after_workspace = None;
    let mut workspaces = Vec::new();
    loop {
        let page = authorizer
            .owned_workspace_page_for_user(
                principal,
                after_workspace.as_ref(),
                OWNED_WORKSPACE_PAGE_SIZE,
            )
            .await
            .map_err(app_status)?;
        let page_is_complete = page.len() < OWNED_WORKSPACE_PAGE_SIZE;
        after_workspace = page.last().cloned();
        workspaces.extend(page.into_iter().map(|workspace| workspace.to_string()));
        if page_is_complete {
            break;
        }
    }
    Ok(OwnedWorkspaceScope::new(workspaces))
}

fn normalize_page_size(page_size: i32) -> usize {
    if page_size <= 0 {
        DEFAULT_TRACE_PAGE_SIZE
    } else {
        usize::try_from(page_size)
            .unwrap_or(MAX_TRACE_PAGE_SIZE)
            .min(MAX_TRACE_PAGE_SIZE)
    }
}

fn parse_page_token(page_token: &str) -> Result<usize, Status> {
    if page_token.is_empty() {
        return Ok(0);
    }
    page_token.parse().map_err(|_parse_error| {
        Status::new(
            Code::InvalidArgument,
            "invalid input: page_token must be returned by ListTraces",
        )
    })
}

fn trace_list_view_from_proto(view: i32) -> Result<TraceListView, Status> {
    match TraceView::try_from(view) {
        Ok(TraceView::Unspecified) => Ok(TraceListView::All),
        Ok(TraceView::QueryStream) => Ok(TraceListView::QueryStream),
        Err(_unknown_view) => Err(Status::new(
            Code::InvalidArgument,
            "invalid input: unknown trace view",
        )),
    }
}

fn workspace_filter_from_proto(
    workspace: Option<&Workspace>,
) -> Result<Option<WorkspaceName>, Status> {
    workspace
        .map(|workspace| WorkspaceName::parse(&workspace.name).map_err(app_status))
        .transpose()
}

fn trace_manager_status(error: TraceManagerError) -> Status {
    match error {
        TraceManagerError::NotFound { trace_id } => {
            Status::new(Code::NotFound, format!("trace '{trace_id}' not found"))
        }
        TraceManagerError::Store(error) => Status::new(Code::Internal, error.to_string()),
    }
}

fn trace_detail_to_proto(trace: TraceDetailRecord) -> GetTraceResponse {
    GetTraceResponse {
        summary: Some(trace_summary_to_proto(trace.summary)),
        spans: trace.spans.into_iter().map(trace_span_to_proto).collect(),
    }
}

fn trace_summary_to_proto(summary: TraceSummaryRecord) -> TraceSummary {
    TraceSummary {
        trace_id: summary.trace_id,
        root_span_id: summary.root_span_id,
        name: summary.name,
        query: summary.query,
        status: trace_status_to_proto(summary.status) as i32,
        start_time_unix_nanos: summary.start_time_unix_nanos,
        end_time_unix_nanos: summary.end_time_unix_nanos,
        duration_nanos: summary.duration_nanos,
        span_count: summary.span_count,
        row_count: summary.row_count,
        row_count_recorded: summary.row_count_recorded,
        operation_kind: trace_operation_kind_to_proto(summary.operation_kind) as i32,
        operation_name: summary.operation_name,
        invocation_kind: trace_invocation_kind_to_proto(summary.invocation_kind) as i32,
    }
}

fn trace_span_to_proto(span: TraceSpanRecord) -> TraceSpan {
    TraceSpan {
        trace_id: span.trace_id,
        span_id: span.span_id,
        parent_span_id: span.parent_span_id.unwrap_or_default(),
        parent_span_is_remote: span.parent_span_is_remote,
        name: span.name,
        kind: span.kind,
        status: trace_status_to_proto(span.status) as i32,
        status_message: span.status_message.unwrap_or_default(),
        start_time_unix_nanos: span.start_time_unix_nanos,
        end_time_unix_nanos: span.end_time_unix_nanos,
        duration_nanos: span.duration_nanos,
        attributes_json: span.attributes_json,
        events_json: span.events_json,
        links_json: span.links_json,
        resource_json: span.resource_json,
        scope_name: span.scope_name,
        scope_version: span.scope_version.unwrap_or_default(),
        scope_schema_url: span.scope_schema_url.unwrap_or_default(),
        scope_attributes_json: span.scope_attributes_json,
        trace_flags: span.trace_flags,
        trace_state: span.trace_state,
        is_remote: span.is_remote,
    }
}

fn trace_status_to_proto(status: StoredTraceStatus) -> TraceStatus {
    match status {
        StoredTraceStatus::Unspecified => TraceStatus::Unspecified,
        StoredTraceStatus::Ok => TraceStatus::Ok,
        StoredTraceStatus::Error => TraceStatus::Error,
    }
}

fn trace_operation_kind_to_proto(kind: StoredTraceOperationKind) -> TraceOperationKind {
    match kind {
        StoredTraceOperationKind::Unspecified => TraceOperationKind::Unspecified,
        StoredTraceOperationKind::Query => TraceOperationKind::Query,
        StoredTraceOperationKind::Search => TraceOperationKind::Search,
        StoredTraceOperationKind::Tool => TraceOperationKind::Tool,
        StoredTraceOperationKind::Other => TraceOperationKind::Other,
    }
}

fn trace_invocation_kind_to_proto(kind: StoredTraceInvocationKind) -> TraceInvocationKind {
    match kind {
        StoredTraceInvocationKind::Unspecified => TraceInvocationKind::Unspecified,
        StoredTraceInvocationKind::Direct => TraceInvocationKind::Direct,
        StoredTraceInvocationKind::Mcp => TraceInvocationKind::Mcp,
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;
    use std::time::Duration;

    use coral_api::v1::trace_service_server::TraceService as TraceServiceApi;
    use coral_api::v1::{
        GetTraceRequest, ListTracesRequest, TraceInvocationKind, TraceOperationKind, TraceView,
        Workspace,
    };
    use serde_json::json;
    use tempfile::TempDir;
    use tonic::{Code, Request};

    use super::{
        TraceService, normalize_page_size, parse_page_token, trace_invocation_kind_to_proto,
    };
    use crate::identity::{Principal, PrincipalKind};
    use crate::request_context::RequestContext;
    use crate::state::AppStateLayout;
    use crate::state::db::{
        CoralDb, DatabaseConfig, DbRepos, ResolvedDatabaseConfig, UpsertLoginOutcome,
    };
    use crate::telemetry::{TraceManager, local_store::StoredTraceInvocationKind};
    use crate::workspaces::{MemberRole, WorkspaceAuthorizer};

    #[test]
    fn page_size_defaults_and_caps() {
        assert_eq!(normalize_page_size(0), super::DEFAULT_TRACE_PAGE_SIZE);
        assert_eq!(normalize_page_size(-1), super::DEFAULT_TRACE_PAGE_SIZE);
        assert_eq!(normalize_page_size(10), 10);
        assert_eq!(normalize_page_size(10_000), super::MAX_TRACE_PAGE_SIZE);
    }

    #[test]
    fn page_token_is_offset() {
        assert_eq!(parse_page_token("").expect("empty token"), 0);
        assert_eq!(parse_page_token("25").expect("offset token"), 25);
        parse_page_token("not-an-offset").unwrap_err();
    }

    #[test]
    fn invocation_kinds_map_to_wire_values() {
        assert_eq!(
            trace_invocation_kind_to_proto(StoredTraceInvocationKind::Unspecified),
            TraceInvocationKind::Unspecified
        );
        assert_eq!(
            trace_invocation_kind_to_proto(StoredTraceInvocationKind::Direct),
            TraceInvocationKind::Direct
        );
        assert_eq!(
            trace_invocation_kind_to_proto(StoredTraceInvocationKind::Mcp),
            TraceInvocationKind::Mcp
        );
    }

    #[tokio::test]
    async fn named_trace_calls_require_owner_manage_access() {
        let fixture = service_fixture().await;
        let response = TraceServiceApi::list_traces(
            &fixture.service,
            list_request(&fixture.owner, Some("alpha"), 10, ""),
        )
        .await
        .expect("owner lists alpha traces")
        .into_inner();
        assert_eq!(trace_ids(&response), vec!["alpha-trace"]);

        let denied = TraceServiceApi::list_traces(
            &fixture.service,
            list_request(
                &fixture.member,
                Some("alpha"),
                10,
                "invalid-before-authorization",
            ),
        )
        .await
        .expect_err("workspace members cannot inspect traces");
        assert_eq!(denied.code(), Code::PermissionDenied);

        let concealed = TraceServiceApi::get_trace(
            &fixture.service,
            get_request(&fixture.nonmember, "", Some("alpha")),
        )
        .await
        .expect_err("nonmember workspace must be concealed before trace validation");
        assert_eq!(concealed.code(), Code::NotFound);
    }

    #[tokio::test]
    async fn global_trace_calls_page_owned_scope_and_keep_host_rows_local() {
        let fixture = service_fixture().await;
        let first = TraceServiceApi::list_traces(
            &fixture.service,
            list_request(&fixture.owner, None, 1, ""),
        )
        .await
        .expect("first owned trace page")
        .into_inner();
        assert_eq!(trace_ids(&first), vec!["beta-trace"]);
        assert_eq!(first.next_page_token, "1");

        let second = TraceServiceApi::list_traces(
            &fixture.service,
            list_request(&fixture.owner, None, 1, &first.next_page_token),
        )
        .await
        .expect("second owned trace page")
        .into_inner();
        assert_eq!(trace_ids(&second), vec!["alpha-trace"]);
        assert!(second.next_page_token.is_empty());

        let member_only = TraceServiceApi::list_traces(
            &fixture.service,
            list_request(&fixture.member, None, 10, ""),
        )
        .await
        .expect("global listing includes only owned workspaces")
        .into_inner();
        assert!(member_only.traces.is_empty());

        for concealed_trace_id in ["gamma-trace", "host-trace"] {
            let status = TraceServiceApi::get_trace(
                &fixture.service,
                get_request(&fixture.owner, concealed_trace_id, None),
            )
            .await
            .expect_err("unowned global trace must be concealed");
            assert_eq!(status.code(), Code::NotFound);
        }

        let strict_local = TraceServiceApi::list_traces(
            &fixture.service,
            list_request(
                &Principal::local(),
                None,
                10,
                "invalid-before-authorization",
            ),
        )
        .await
        .expect_err("shared mode rejects the local principal before request validation");
        assert_eq!(strict_local.code(), Code::PermissionDenied);

        let agent = Principal::parse(fixture.owner.id().as_str(), PrincipalKind::Agent)
            .expect("owner agent");
        let agent_denied = TraceServiceApi::list_traces(
            &fixture.service,
            list_request(&agent, None, 10, "invalid-before-authorization"),
        )
        .await
        .expect_err("global trace access requires a user principal");
        assert_eq!(agent_denied.code(), Code::PermissionDenied);

        let local = TraceServiceApi::list_traces(
            &fixture.local_service,
            list_request(&Principal::local(), None, 10, ""),
        )
        .await
        .expect("local unrestricted traces")
        .into_inner();
        assert_eq!(
            trace_ids(&local),
            vec!["beta-trace", "gamma-trace", "alpha-trace", "host-trace"]
        );
    }

    #[tokio::test]
    async fn query_stream_view_projects_entries_for_an_authorized_workspace() {
        let fixture = fixture_with_traces(write_query_stream_trace_fixture).await;
        let response = TraceServiceApi::list_traces(
            &fixture.service,
            view_list_request(&fixture.owner, Some("alpha"), TraceView::QueryStream),
        )
        .await
        .expect("list query stream")
        .into_inner();
        assert_eq!(response.traces.len(), 1);
        let summary = response.traces.first().expect("tool summary");
        assert_eq!(summary.root_span_id, "tool-span");
        assert_eq!(summary.operation_kind, TraceOperationKind::Query as i32);
        assert_eq!(summary.operation_name, "sql");
        assert_eq!(summary.invocation_kind, TraceInvocationKind::Mcp as i32);
        assert_eq!(summary.query, "SELECT 42");
        assert_eq!(summary.start_time_unix_nanos, 10);
        assert_eq!(summary.end_time_unix_nanos, 40);

        let detail = TraceServiceApi::get_trace(
            &fixture.service,
            view_get_request(
                &fixture.owner,
                "shared-trace",
                Some("alpha"),
                TraceView::QueryStream,
            ),
        )
        .await
        .expect("get query stream trace")
        .into_inner();
        let detail_summary = detail.summary.expect("query stream detail summary");
        assert_eq!(detail_summary, *summary);
        assert_eq!(detail.spans.len(), 2);
        let mixed = TraceServiceApi::get_trace(
            &fixture.service,
            view_get_request(
                &fixture.owner,
                "mixed-trace",
                Some("alpha"),
                TraceView::QueryStream,
            ),
        )
        .await
        .expect_err("mixed-workspace trace must be concealed");
        assert_eq!(mixed.code(), Code::NotFound);

        let unknown_view = TraceServiceApi::list_traces(
            &fixture.service,
            authenticated_request(
                ListTracesRequest {
                    page_size: 10,
                    page_token: String::new(),
                    workspace: None,
                    view: 999,
                },
                &fixture.owner,
            ),
        )
        .await
        .expect_err("unknown view is rejected");
        assert_eq!(unknown_view.code(), Code::InvalidArgument);
        let mut global_request = view_list_request(&fixture.owner, None, TraceView::QueryStream);
        global_request.get_mut().page_size = 10;
        let global = TraceServiceApi::list_traces(&fixture.service, global_request)
            .await
            .expect("owner-scoped query stream")
            .into_inner();
        assert_eq!(trace_ids(&global), vec!["mixed-trace", "shared-trace"]);

        let mixed = TraceServiceApi::get_trace(
            &fixture.service,
            view_get_request(&fixture.owner, "mixed-trace", None, TraceView::QueryStream),
        )
        .await
        .expect("owner reads a trace attributed only to owned workspaces")
        .into_inner();
        assert_eq!(mixed.spans.len(), 2);

        for principal in [&fixture.member, &fixture.nonmember] {
            let response = TraceServiceApi::list_traces(
                &fixture.service,
                view_list_request(principal, None, TraceView::QueryStream),
            )
            .await
            .expect("a caller without owned workspaces gets an empty page")
            .into_inner();
            assert!(response.traces.is_empty());
            let concealed = TraceServiceApi::get_trace(
                &fixture.service,
                view_get_request(principal, "shared-trace", None, TraceView::QueryStream),
            )
            .await
            .expect_err("a caller without owned workspaces cannot read a trace");
            assert_eq!(concealed.code(), Code::NotFound);
        }

        let mut local_request =
            view_list_request(&Principal::local(), None, TraceView::QueryStream);
        local_request.get_mut().page_size = 10;
        let local = TraceServiceApi::list_traces(&fixture.local_service, local_request)
            .await
            .expect("local principal reads the unrestricted query stream")
            .into_inner();
        assert_eq!(trace_ids(&local), vec!["mixed-trace", "shared-trace"]);
    }

    struct ServiceFixture {
        _temp: TempDir,
        service: TraceService,
        local_service: TraceService,
        owner: Principal,
        member: Principal,
        nonmember: Principal,
    }

    async fn fixture_with_traces(write_traces: impl FnOnce(&Path)) -> ServiceFixture {
        let temp = TempDir::new().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        let DatabaseConfig::Sqlite { path } = DatabaseConfig::load(&layout).expect("config") else {
            panic!("default test database must be SQLite")
        };
        let db = Arc::new(
            CoralDb::open(ResolvedDatabaseConfig::Sqlite { path })
                .await
                .expect("open sqlite"),
        );
        db.migrate().await.expect("migrate sqlite");
        let owner_id = provision_user(&db, "trace-owner").await;
        let member_id = provision_user(&db, "trace-member").await;
        let nonmember_id = provision_user(&db, "trace-nonmember").await;
        let gamma_owner_id = provision_user(&db, "gamma-owner").await;

        let mut session = db.as_ref();
        for (workspace, owner_id) in [
            ("alpha", owner_id.as_str()),
            ("beta", owner_id.as_str()),
            ("gamma", gamma_owner_id.as_str()),
        ] {
            session
                .workspaces()
                .create_with_owner(workspace, owner_id, 1)
                .await
                .expect("create owned workspace");
        }
        session
            .workspaces()
            .add_member("alpha", &member_id, MemberRole::Member, 2)
            .await
            .expect("add workspace member");

        let trace_store = temp.path().join("trace-store");
        std::fs::create_dir_all(&trace_store).expect("trace store dir");
        write_traces(&trace_store);
        let manager = TraceManager::new(trace_store, Duration::from_mins(1));
        ServiceFixture {
            service: TraceService::new(manager.clone(), WorkspaceAuthorizer::new(Arc::clone(&db))),
            local_service: TraceService::new(
                manager,
                WorkspaceAuthorizer::trusting_local_principal(db),
            ),
            owner: Principal::parse(&owner_id, PrincipalKind::User).expect("owner"),
            member: Principal::parse(&member_id, PrincipalKind::User).expect("member"),
            nonmember: Principal::parse(&nonmember_id, PrincipalKind::User).expect("nonmember"),
            _temp: temp,
        }
    }

    async fn service_fixture() -> ServiceFixture {
        fixture_with_traces(write_scoped_trace_records).await
    }

    fn write_scoped_trace_records(trace_store: &Path) {
        let mut host_trace = trace_record_json("host-trace", "host-span", "host", 10, 20);
        *host_trace
            .get_mut("attributes_json")
            .expect("trace attributes") = json!(r#"{"sql":"SELECT host","status":"ok"}"#);
        write_trace_records(
            trace_store,
            &[
                trace_record_json("alpha-trace", "alpha-span", "alpha", 10, 30),
                trace_record_json("beta-trace", "beta-span", "beta", 40, 60),
                trace_record_json("gamma-trace", "gamma-span", "gamma", 30, 50),
                host_trace,
            ],
        );
    }

    async fn provision_user(db: &CoralDb, subject: &str) -> String {
        let mut session = db;
        let UpsertLoginOutcome::Upserted(user) = session
            .users()
            .upsert_login("issuer", subject, None, 1)
            .await
            .expect("provision user")
        else {
            panic!("new subject should create user")
        };
        user.user_id
    }

    fn authenticated_request<T>(message: T, principal: &Principal) -> Request<T> {
        let mut request = Request::new(message);
        request
            .extensions_mut()
            .insert(RequestContext::new(principal.clone()));
        request
    }

    fn list_request(
        principal: &Principal,
        workspace_name: Option<&str>,
        page_size: i32,
        page_token: &str,
    ) -> Request<ListTracesRequest> {
        authenticated_request(
            ListTracesRequest {
                page_size,
                page_token: page_token.to_string(),
                workspace: workspace_name.map(workspace),
                view: TraceView::Unspecified as i32,
            },
            principal,
        )
    }

    fn view_list_request(
        principal: &Principal,
        workspace_name: Option<&str>,
        view: TraceView,
    ) -> Request<ListTracesRequest> {
        let mut request = list_request(principal, workspace_name, 1, "");
        request.get_mut().view = view as i32;
        request
    }

    fn get_request(
        principal: &Principal,
        trace_id: &str,
        workspace_name: Option<&str>,
    ) -> Request<GetTraceRequest> {
        authenticated_request(
            GetTraceRequest {
                trace_id: trace_id.to_string(),
                workspace: workspace_name.map(workspace),
                view: TraceView::Unspecified as i32,
            },
            principal,
        )
    }

    fn view_get_request(
        principal: &Principal,
        trace_id: &str,
        workspace_name: Option<&str>,
        view: TraceView,
    ) -> Request<GetTraceRequest> {
        let mut request = get_request(principal, trace_id, workspace_name);
        request.get_mut().view = view as i32;
        request
    }

    fn trace_ids(response: &coral_api::v1::ListTracesResponse) -> Vec<&str> {
        response
            .traces
            .iter()
            .map(|trace| trace.trace_id.as_str())
            .collect()
    }

    fn workspace(name: &str) -> Workspace {
        Workspace {
            name: name.to_string(),
        }
    }

    fn write_trace_records(dir: &std::path::Path, records: &[serde_json::Value]) {
        let mut lines = String::new();
        for record in records {
            lines.push_str(&record.to_string());
            lines.push('\n');
        }
        std::fs::write(dir.join("spans-test.jsonl"), lines).expect("write trace records");
    }

    fn write_query_stream_trace_fixture(trace_store: &Path) {
        let mut tool = trace_record_json("shared-trace", "tool-span", "alpha", 10, 40);
        let tool_object = tool.as_object_mut().expect("tool record object");
        tool_object.insert("parent_span_id".to_string(), json!("remote-parent"));
        tool_object.insert("parent_span_is_remote".to_string(), json!(true));
        tool_object.insert("name".to_string(), json!("coral.mcp.call_tool"));
        tool_object.insert(
            "attributes_json".to_string(),
            json!(
                json!({
                    "coral.stream.entry": true,
                    "coral.stream.kind": "tool",
                    "coral.stream.name": "sql",
                    "mcp.method": "tools/call",
                    "mcp.tool.name": "sql",
                    "workspace": "alpha",
                    "status": "ok",
                })
                .to_string()
            ),
        );

        let mut nested = trace_record_json("shared-trace", "nested-query", "alpha", 20, 30);
        let nested_object = nested.as_object_mut().expect("nested record object");
        nested_object.insert("parent_span_id".to_string(), json!("tool-span"));
        nested_object.insert(
            "attributes_json".to_string(),
            json!(
                json!({
                    "coral.stream.entry": true,
                    "coral.stream.kind": "query",
                    "coral.stream.name": "sql",
                    "sql": "SELECT 42",
                    "row_count": 1,
                    "status": "ok",
                })
                .to_string()
            ),
        );
        let mut mixed_tool = tool.clone();
        let mixed_tool_object = mixed_tool.as_object_mut().expect("mixed tool object");
        mixed_tool_object.insert("trace_id".to_string(), json!("mixed-trace"));
        mixed_tool_object.insert("span_id".to_string(), json!("mixed-tool"));
        let mut sentinel = trace_record_json("mixed-trace", "beta-query", "beta", 20, 30);
        let sentinel_object = sentinel.as_object_mut().expect("sentinel record object");
        sentinel_object.insert("parent_span_id".to_string(), json!("remote-parent"));
        sentinel_object.insert("parent_span_is_remote".to_string(), json!(true));
        write_trace_records(trace_store, &[tool, nested, mixed_tool, sentinel]);
    }

    fn trace_record_json(
        trace_id: &str,
        span_id: &str,
        workspace: &str,
        start_time_unix_nanos: i64,
        end_time_unix_nanos: i64,
    ) -> serde_json::Value {
        json!({
            "trace_id": trace_id,
            "span_id": span_id,
            "parent_span_id": null,
            "parent_span_is_remote": false,
            "name": "coral.query",
            "kind": "internal",
            "status": "ok",
            "status_message": null,
            "start_time_unix_nanos": start_time_unix_nanos,
            "end_time_unix_nanos": end_time_unix_nanos,
            "duration_nanos": end_time_unix_nanos - start_time_unix_nanos,
            "attributes_json": json!({
                "workspace": workspace,
                "sql": format!("SELECT {workspace}"),
                "status": "ok",
            }).to_string(),
            "events_json": "[]",
            "links_json": "[]",
            "resource_json": "{}",
            "scope_name": "test",
            "scope_version": null,
            "scope_schema_url": null,
            "scope_attributes_json": "{}",
            "trace_flags": 0,
            "trace_state": "",
            "is_remote": false
        })
    }
}
