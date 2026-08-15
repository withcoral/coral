//! Implements the gRPC `TraceService` for local trace inspection.

use std::collections::HashSet;

use coral_api::v1::trace_service_server::TraceService as TraceServiceApi;
use coral_api::v1::{
    GetTraceRequest, GetTraceResponse, ListTracesRequest, ListTracesResponse, TraceInvocationKind,
    TraceOperationKind, TraceSpan, TraceStatus, TraceSummary, TraceView, Workspace,
};
use tonic::{Code, Request, Response, Status};

use crate::bootstrap::{AppError, app_status};
use crate::identity::{LOCAL_PRINCIPAL_ID, Principal, PrincipalKind};
use crate::telemetry::local_store::{
    StoredTraceInvocationKind, StoredTraceOperationKind, StoredTraceStatus, TraceDetailRecord,
    TraceSpanRecord, TraceSummaryRecord,
};
use crate::telemetry::manager::{
    GetTraceQuery, ListTracesQuery, TraceListPage, TraceListView, TraceManager, TraceManagerError,
};
use crate::transport::{grpc_span, instrument_grpc, request_context};
use crate::workspaces::authorization::{WorkspaceAction, WorkspaceAuthorizer};
use crate::workspaces::{MemberRole, WorkspaceManager, WorkspaceName};

const DEFAULT_TRACE_PAGE_SIZE: usize = 50;
const MAX_TRACE_PAGE_SIZE: usize = 200;

/// What one caller may read, settled before any trace leaves the store.
enum TraceAccessScope {
    /// One workspace the caller manages.
    Workspace(WorkspaceName),
    /// Every trace this host recorded, including rows no workspace claims.
    Host,
    /// Only the workspaces the caller owns, read one at a time.
    Owned(Vec<WorkspaceName>),
}

#[derive(Clone)]
pub(crate) struct TraceService {
    traces: TraceManager,
    workspaces: WorkspaceManager,
    authorizer: WorkspaceAuthorizer,
}

impl TraceService {
    pub(crate) const fn new(
        trace_manager: TraceManager,
        workspaces: WorkspaceManager,
        authorizer: WorkspaceAuthorizer,
    ) -> Self {
        Self {
            traces: trace_manager,
            workspaces,
            authorizer,
        }
    }

    /// Settles what `principal` may read before any trace is fetched.
    ///
    /// A trace carries the query text, arguments, and errors of whoever ran it,
    /// so reading one is an owner's act rather than a member's: a named
    /// workspace is authorized for `Manage`, and a request that names none is
    /// confined to the workspaces the caller owns. Only the built-in local
    /// principal reads the host's own rows — the spans no workspace claims —
    /// and only where the deployment admits that principal at all.
    async fn trace_access_scope(
        &self,
        principal: &Principal,
        workspace: Option<WorkspaceName>,
    ) -> Result<TraceAccessScope, Status> {
        if let Some(workspace) = workspace {
            self.authorizer
                .authorize(principal, &workspace, WorkspaceAction::Manage)
                .await
                .map_err(app_status)?;
            return Ok(TraceAccessScope::Workspace(workspace));
        }

        self.authorizer.admit(principal).map_err(app_status)?;
        if principal.id().as_str() == LOCAL_PRINCIPAL_ID {
            return Ok(TraceAccessScope::Host);
        }
        // An unnamed request reads every workspace the caller owns at once, so
        // an agent credential is refused it for the same reason `Manage`
        // refuses it one workspace at a time.
        if principal.kind() == PrincipalKind::Agent {
            return Err(app_status(AppError::PermissionDenied(
                "agent credentials cannot inspect traces".to_string(),
            )));
        }
        let owned = self
            .workspaces
            .list_memberships_for_user(principal.id().as_str())
            .await
            .map_err(app_status)?
            .into_iter()
            .filter(|membership| membership.role == MemberRole::Owner)
            .map(|membership| membership.workspace.name)
            .collect();
        Ok(TraceAccessScope::Owned(owned))
    }
}

#[tonic::async_trait]
impl TraceServiceApi for TraceService {
    async fn list_traces(
        &self,
        request: Request<ListTracesRequest>,
    ) -> Result<Response<ListTracesResponse>, Status> {
        let span = grpc_span(&request);
        let principal = request_context(&request)?.principal().clone();
        let service = self.clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace = workspace_filter_from_proto(request.workspace.as_ref())?;
            // Settled before the rest of the request is parsed, so a caller who
            // may not read these traces cannot learn anything from the
            // request's own validation.
            let scope = service.trace_access_scope(&principal, workspace).await?;
            let page_size = normalize_page_size(request.page_size);
            let offset = parse_page_token(&request.page_token)?;
            let view = trace_list_view_from_proto(request.view)?;
            let page = list_scoped_traces(&service.traces, scope, view, page_size, offset)
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
        let principal = request_context(&request)?.principal().clone();
        let service = self.clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace = workspace_filter_from_proto(request.workspace.as_ref())?;
            let scope = service.trace_access_scope(&principal, workspace).await?;
            if request.trace_id.trim().is_empty() {
                return Err(Status::new(
                    Code::InvalidArgument,
                    "invalid input: missing trace_id",
                ));
            }
            let view = trace_list_view_from_proto(request.view)?;
            let trace = get_scoped_trace(&service.traces, scope, request.trace_id, view)
                .await
                .map_err(trace_manager_status)?;
            Ok(Response::new(trace_detail_to_proto(trace)))
        })
        .await
    }
}

/// Reads one page of traces under `scope`.
async fn list_scoped_traces(
    traces: &TraceManager,
    scope: TraceAccessScope,
    view: TraceListView,
    page_size: usize,
    offset: usize,
) -> Result<TraceListPage, TraceManagerError> {
    let workspace = match scope {
        TraceAccessScope::Workspace(workspace) => Some(workspace),
        TraceAccessScope::Host => None,
        TraceAccessScope::Owned(workspaces) => {
            return list_owned_traces(traces, &workspaces, view, page_size, offset).await;
        }
    };
    traces
        .list_traces(ListTracesQuery {
            view,
            workspace,
            page_size,
            offset,
        })
        .await
}

/// Merges one page out of the workspaces the caller owns.
///
/// The store scopes a read to a single workspace, so an unnamed request is
/// answered by reading each owned workspace from the top and merging. Reading
/// `offset + page_size` from each is enough: a trace cannot rank higher in the
/// merged page than it does in its own workspace's page, so nothing below that
/// depth in a workspace can reach this page.
async fn list_owned_traces(
    traces: &TraceManager,
    workspaces: &[WorkspaceName],
    view: TraceListView,
    page_size: usize,
    offset: usize,
) -> Result<TraceListPage, TraceManagerError> {
    let depth = offset.saturating_add(page_size);
    let mut merged = Vec::new();
    let mut merged_ids = HashSet::new();
    let mut deeper_traces_exist = false;
    for workspace in workspaces {
        let page = traces
            .list_traces(ListTracesQuery {
                view,
                workspace: Some(workspace.clone()),
                page_size: depth,
                offset: 0,
            })
            .await?;
        deeper_traces_exist |= page.next_offset.is_some();
        for summary in page.traces {
            // A trace whose spans span two owned workspaces is one trace.
            if merged_ids.insert(summary.trace_id.clone()) {
                merged.push(summary);
            }
        }
    }
    // Newest first, ties broken by trace id: the order each workspace's own
    // page already arrives in, so the merge preserves it.
    merged.sort_by(|left, right| {
        right
            .end_time_unix_nanos
            .cmp(&left.end_time_unix_nanos)
            .then_with(|| left.trace_id.cmp(&right.trace_id))
    });
    let next_offset = (deeper_traces_exist || merged.len() > depth).then_some(depth);
    Ok(TraceListPage {
        traces: merged.into_iter().skip(offset).take(page_size).collect(),
        next_offset,
    })
}

/// Reads one trace under `scope`.
async fn get_scoped_trace(
    traces: &TraceManager,
    scope: TraceAccessScope,
    trace_id: String,
    view: TraceListView,
) -> Result<TraceDetailRecord, TraceManagerError> {
    let workspace = match scope {
        TraceAccessScope::Workspace(workspace) => Some(workspace),
        TraceAccessScope::Host => None,
        TraceAccessScope::Owned(workspaces) => {
            return get_owned_trace(traces, &workspaces, trace_id, view).await;
        }
    };
    traces
        .get_trace(GetTraceQuery {
            trace_id,
            workspace,
            view,
        })
        .await
}

/// Looks one trace up in each workspace the caller owns.
///
/// A trace that belongs to none of them is reported absent rather than
/// refused: which workspace it does belong to is not the caller's to learn.
async fn get_owned_trace(
    traces: &TraceManager,
    workspaces: &[WorkspaceName],
    trace_id: String,
    view: TraceListView,
) -> Result<TraceDetailRecord, TraceManagerError> {
    for workspace in workspaces {
        let found = traces
            .get_trace(GetTraceQuery {
                trace_id: trace_id.clone(),
                workspace: Some(workspace.clone()),
                view,
            })
            .await;
        match found {
            Err(TraceManagerError::NotFound { .. }) => {}
            found => return found,
        }
    }
    Err(TraceManagerError::NotFound { trace_id })
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
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Duration;

    use coral_api::v1::trace_service_server::TraceService as TraceServiceApi;
    use coral_api::v1::{
        GetTraceRequest, ListTracesRequest, ListTracesResponse, TraceInvocationKind,
        TraceOperationKind, TraceView, Workspace,
    };
    use serde_json::json;
    use tempfile::TempDir;
    use tonic::{Code, Request};

    use super::{
        TraceService, normalize_page_size, parse_page_token, trace_invocation_kind_to_proto,
    };
    use crate::credentials::{CredentialManager, CredentialStore};
    use crate::identity::{Principal, PrincipalKind};
    use crate::request_context::RequestContext;
    use crate::state::db::{
        CoralDb, DbRepos, LoginIdentity, LoginProvisioning, ResolvedDatabaseConfig,
    };
    use crate::state::{AppStateLayout, ConfigStore};
    use crate::telemetry::{TraceManager, local_store::StoredTraceInvocationKind};
    use crate::workspaces::authorization::{LocalPrincipalPolicy, WorkspaceAuthorizer};
    use crate::workspaces::{MemberRole, WorkspaceManager, WorkspaceName};

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
    async fn trace_service_scopes_list_and_get_by_workspace() {
        let deployment = Deployment::new().await;
        deployment.write_traces(&[
            trace_record_json("alpha-trace", "alpha-span", Some("alpha"), 10, 20),
            trace_record_json("beta-trace", "beta-span", Some("beta"), 30, 40),
        ]);
        let service = deployment.service(LocalPrincipalPolicy::ImplicitOwner);

        let response = TraceServiceApi::list_traces(
            &service,
            local(ListTracesRequest {
                page_size: 10,
                page_token: String::new(),
                workspace: Some(workspace_proto("alpha")),
                view: TraceView::Unspecified as i32,
            }),
        )
        .await
        .expect("list alpha traces")
        .into_inner();

        assert_eq!(response.traces.len(), 1);
        assert_eq!(
            response.traces.first().expect("alpha trace").trace_id,
            "alpha-trace"
        );
        assert_eq!(
            response.traces.first().expect("alpha trace").operation_kind,
            TraceOperationKind::Unspecified as i32
        );
        assert!(
            response
                .traces
                .first()
                .expect("alpha trace")
                .operation_name
                .is_empty()
        );
        assert_eq!(
            response
                .traces
                .first()
                .expect("alpha trace")
                .invocation_kind,
            TraceInvocationKind::Unspecified as i32
        );

        let detail = TraceServiceApi::get_trace(
            &service,
            local(GetTraceRequest {
                trace_id: "alpha-trace".to_string(),
                workspace: Some(workspace_proto("alpha")),
                view: TraceView::Unspecified as i32,
            }),
        )
        .await
        .expect("get alpha trace")
        .into_inner();
        assert_eq!(detail.spans.len(), 1);

        let status = TraceServiceApi::get_trace(
            &service,
            local(GetTraceRequest {
                trace_id: "beta-trace".to_string(),
                workspace: Some(workspace_proto("alpha")),
                view: TraceView::Unspecified as i32,
            }),
        )
        .await
        .expect_err("beta trace should not match alpha workspace");
        assert_eq!(status.code(), Code::NotFound);
    }

    #[tokio::test]
    async fn trace_service_projects_query_stream_entries() {
        let deployment = Deployment::new().await;
        deployment.write_query_stream_traces();
        let service = deployment.service(LocalPrincipalPolicy::ImplicitOwner);

        let response = TraceServiceApi::list_traces(
            &service,
            local(ListTracesRequest {
                page_size: 10,
                page_token: String::new(),
                workspace: Some(workspace_proto("alpha")),
                view: TraceView::QueryStream as i32,
            }),
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
            &service,
            local(GetTraceRequest {
                trace_id: "shared-trace".to_string(),
                workspace: Some(workspace_proto("alpha")),
                view: TraceView::QueryStream as i32,
            }),
        )
        .await
        .expect("get query stream trace")
        .into_inner();
        let detail_summary = detail.summary.expect("query stream detail summary");
        assert_eq!(detail_summary, *summary);
        assert_eq!(detail.spans.len(), 2);

        let unknown_view = TraceServiceApi::list_traces(
            &service,
            local(ListTracesRequest {
                page_size: 10,
                page_token: String::new(),
                workspace: None,
                view: 999,
            }),
        )
        .await
        .expect_err("unknown view is rejected");
        assert_eq!(unknown_view.code(), Code::InvalidArgument);
    }

    /// A named workspace is an owner's to inspect and nobody else's. The
    /// member is the case that matters: they read the workspace's data all day
    /// and still may not read the trace of somebody else's query in it.
    #[tokio::test]
    async fn named_trace_calls_require_workspace_ownership() {
        let deployment = Deployment::new().await;
        deployment.write_traces(&fan_out_trace_records());
        let owner = deployment
            .seed_member("owner", "alpha", MemberRole::Owner)
            .await;
        let member = deployment
            .seed_member("member", "alpha", MemberRole::Member)
            .await;
        let outsider = deployment.seed_user("outsider").await;
        let service = deployment.service(LocalPrincipalPolicy::NoLocalPrincipal);

        let listed = TraceServiceApi::list_traces(&service, request(list_alpha(), owner.clone()))
            .await
            .expect("an owner inspects their own workspace")
            .into_inner();
        assert_eq!(trace_ids(&listed), vec!["alpha-new", "alpha-old"]);

        // A member is denied and an outsider is concealed: the workspace they
        // already know about stays distinguishable from the one they do not.
        for (principal, expected) in [
            (member, Code::PermissionDenied),
            (outsider, Code::NotFound),
            (as_agent(&owner), Code::PermissionDenied),
        ] {
            assert_eq!(
                TraceServiceApi::list_traces(&service, request(list_alpha(), principal))
                    .await
                    .expect_err("only an owner inspects traces")
                    .code(),
                expected
            );
        }
    }

    /// An unnamed request is the widest read this service offers, so it is the
    /// one that must not widen past what the caller owns: not another tenant's
    /// workspace, and not the host rows no workspace claims.
    #[tokio::test]
    async fn global_listing_fans_out_across_owned_workspaces_only() {
        let deployment = Deployment::new().await;
        deployment.write_traces(&fan_out_trace_records());
        let owner = deployment
            .seed_member("owner", "alpha", MemberRole::Owner)
            .await;
        deployment.grant(&owner, "beta", MemberRole::Owner).await;
        deployment.grant(&owner, "gamma", MemberRole::Member).await;
        let member = deployment
            .seed_member("member", "alpha", MemberRole::Member)
            .await;
        let service = deployment.service(LocalPrincipalPolicy::NoLocalPrincipal);

        // Paged one at a time, so the merge across owned workspaces is proven
        // to order and page as one list rather than as three.
        let mut page_token = String::new();
        let mut listed = Vec::new();
        loop {
            let page = TraceServiceApi::list_traces(
                &service,
                request(list_global(1, &page_token), owner.clone()),
            )
            .await
            .expect("owner lists the workspaces they own")
            .into_inner();
            listed.extend(trace_ids(&page).into_iter().map(str::to_string));
            page_token = page.next_page_token;
            if page_token.is_empty() {
                break;
            }
        }
        assert_eq!(listed, vec!["beta-trace", "alpha-new", "alpha-old"]);

        for (trace_id, expected) in [
            ("alpha-old", Code::Ok),
            ("gamma-trace", Code::NotFound),
            ("host-trace", Code::NotFound),
        ] {
            let found = TraceServiceApi::get_trace(
                &service,
                request(
                    GetTraceRequest {
                        trace_id: trace_id.to_string(),
                        workspace: None,
                        view: TraceView::Unspecified as i32,
                    },
                    owner.clone(),
                ),
            )
            .await;
            assert_eq!(
                found.err().map_or(Code::Ok, |status| status.code()),
                expected
            );
        }

        let member_page =
            TraceServiceApi::list_traces(&service, request(list_global(10, ""), member))
                .await
                .expect("a member owns nothing to fan out over")
                .into_inner();
        assert!(member_page.traces.is_empty());

        for principal in [as_agent(&owner), Principal::local()] {
            assert_eq!(
                TraceServiceApi::list_traces(&service, request(list_global(10, ""), principal))
                    .await
                    .expect_err("neither an agent nor an unadmitted principal reads traces")
                    .code(),
                Code::PermissionDenied
            );
        }
    }

    /// The single-user deployment keeps the unrestricted read it has always
    /// had, host rows included: there is nobody there to conceal them from.
    #[tokio::test]
    async fn the_implicit_owner_still_reads_every_trace_this_host_recorded() {
        let deployment = Deployment::new().await;
        deployment.write_traces(&fan_out_trace_records());
        let service = deployment.service(LocalPrincipalPolicy::ImplicitOwner);

        let response = TraceServiceApi::list_traces(&service, local(list_global(10, "")))
            .await
            .expect("local unrestricted traces")
            .into_inner();

        assert_eq!(
            trace_ids(&response),
            vec![
                "host-trace",
                "gamma-trace",
                "beta-trace",
                "alpha-new",
                "alpha-old"
            ]
        );
    }

    /// A deployment fixture: one migrated database, the workspace manager over
    /// it, and the trace store the service reads.
    struct Deployment {
        _temp: TempDir,
        db: Arc<CoralDb>,
        workspaces: WorkspaceManager,
        trace_store: PathBuf,
    }

    impl Deployment {
        async fn new() -> Self {
            let temp = TempDir::new().expect("temp dir");
            let layout =
                AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
            layout.ensure().expect("layout dirs");
            let db = CoralDb::open(ResolvedDatabaseConfig::Sqlite {
                path: temp.path().join("coral.sqlite"),
            })
            .await
            .expect("open sqlite");
            db.migrate().await.expect("migrate sqlite");
            let db = Arc::new(db);
            let workspaces = WorkspaceManager::new_for_tests(
                ConfigStore::new(layout.clone()),
                CredentialManager::new(CredentialStore::new(layout.clone())),
                layout,
                None,
                Arc::clone(&db),
            );
            let trace_store = temp.path().join("trace-store");
            std::fs::create_dir_all(&trace_store).expect("trace store dir");
            Self {
                _temp: temp,
                db,
                workspaces,
                trace_store,
            }
        }

        fn service(&self, policy: LocalPrincipalPolicy) -> TraceService {
            TraceService::new(
                TraceManager::new(self.trace_store.clone(), Duration::from_mins(1)),
                self.workspaces.clone(),
                WorkspaceAuthorizer::with_local_principal_policy(Arc::clone(&self.db), policy),
            )
        }

        fn write_traces(&self, records: &[serde_json::Value]) {
            write_trace_records(&self.trace_store, records);
        }

        fn write_query_stream_traces(&self) {
            write_query_stream_trace_fixture(&self.trace_store);
        }

        /// Provisions one directory user through the production login seam, so
        /// the `user_id` the service is handed is the one a real login carries.
        async fn seed_user(&self, subject: &str) -> Principal {
            let provisioned = self
                .db
                .user_state()
                .provision_login(LoginIdentity {
                    issuer: "https://issuer.test/traces",
                    subject,
                    display_name: None,
                    principal_claim: subject,
                    now_unix_nanos: 1,
                })
                .await
                .expect("provision user");
            let LoginProvisioning::Provisioned(user) = provisioned else {
                panic!("expected a provisioned user");
            };
            Principal::parse(&user.user_id, PrincipalKind::User).expect("principal")
        }

        async fn seed_member(&self, subject: &str, workspace: &str, role: MemberRole) -> Principal {
            let principal = self.seed_user(subject).await;
            self.grant(&principal, workspace, role).await;
            principal
        }

        async fn grant(&self, principal: &Principal, workspace: &str, role: MemberRole) {
            let name = WorkspaceName::parse(workspace).expect("workspace name");
            let mut session = self.db.as_ref();
            session
                .workspaces()
                .ensure(name.as_str(), 1)
                .await
                .expect("workspace row");
            session
                .workspace_members()
                .upsert(name.as_str(), principal.id().as_str(), role, 2)
                .await
                .expect("grant membership");
        }
    }

    /// The traces every fan-out case reads: two workspaces the owner holds,
    /// one they only belong to, and one host row no workspace claims.
    fn fan_out_trace_records() -> Vec<serde_json::Value> {
        vec![
            trace_record_json("alpha-old", "alpha-old-span", Some("alpha"), 10, 20),
            trace_record_json("alpha-new", "alpha-new-span", Some("alpha"), 20, 30),
            trace_record_json("beta-trace", "beta-span", Some("beta"), 30, 40),
            trace_record_json("gamma-trace", "gamma-span", Some("gamma"), 40, 50),
            trace_record_json("host-trace", "host-span", None, 50, 60),
        ]
    }

    fn list_alpha() -> ListTracesRequest {
        ListTracesRequest {
            page_size: 10,
            page_token: String::new(),
            workspace: Some(workspace_proto("alpha")),
            view: TraceView::Unspecified as i32,
        }
    }

    fn list_global(page_size: i32, page_token: &str) -> ListTracesRequest {
        ListTracesRequest {
            page_size,
            page_token: page_token.to_string(),
            workspace: None,
            view: TraceView::Unspecified as i32,
        }
    }

    fn trace_ids(response: &ListTracesResponse) -> Vec<&str> {
        response
            .traces
            .iter()
            .map(|summary| summary.trace_id.as_str())
            .collect()
    }

    fn as_agent(principal: &Principal) -> Principal {
        Principal::parse(principal.id().as_str(), PrincipalKind::Agent).expect("agent")
    }

    fn request<T>(message: T, principal: Principal) -> Request<T> {
        let mut request = Request::new(message);
        request
            .extensions_mut()
            .insert(RequestContext::new(principal));
        request
    }

    fn local<T>(message: T) -> Request<T> {
        request(message, Principal::local())
    }

    fn workspace_proto(name: &str) -> Workspace {
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

    fn write_query_stream_trace_fixture(trace_store: &std::path::Path) {
        let mut tool = trace_record_json("shared-trace", "tool-span", Some("alpha"), 10, 40);
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

        let mut nested = trace_record_json("shared-trace", "nested-query", Some("alpha"), 20, 30);
        let nested_object = nested.as_object_mut().expect("nested record object");
        nested_object.insert("parent_span_id".to_string(), json!("tool-span"));
        nested_object.insert(
            "attributes_json".to_string(),
            json!(
                json!({
                    "coral.stream.entry": true,
                    "coral.stream.kind": "query",
                    "coral.stream.name": "sql",
                    "workspace": "alpha",
                    "sql": "SELECT 42",
                    "row_count": 1,
                    "status": "ok",
                })
                .to_string()
            ),
        );
        write_trace_records(trace_store, &[tool, nested]);
    }

    /// Builds one stored span. A `None` workspace is a host row: work this
    /// server did that no workspace claims.
    fn trace_record_json(
        trace_id: &str,
        span_id: &str,
        workspace: Option<&str>,
        start_time_unix_nanos: i64,
        end_time_unix_nanos: i64,
    ) -> serde_json::Value {
        let attributes = workspace.map_or_else(
            || json!({ "status": "ok" }),
            |workspace| {
                json!({
                    "workspace": workspace,
                    "sql": format!("SELECT {workspace}"),
                    "status": "ok",
                })
            },
        );
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
            "attributes_json": attributes.to_string(),
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
