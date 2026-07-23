//! Implements the gRPC `TraceService` for local trace inspection.

use coral_api::v1::trace_service_server::TraceService as TraceServiceApi;
use coral_api::v1::{
    GetTraceRequest, GetTraceResponse, ListTracesRequest, ListTracesResponse, TraceOperationKind,
    TraceSpan, TraceStatus, TraceSummary, TraceView, Workspace,
};
use tonic::{Code, Request, Response, Status};

use crate::bootstrap::app_status;
use crate::telemetry::local_store::{
    StoredTraceOperationKind, StoredTraceStatus, TraceDetailRecord, TraceSpanRecord,
    TraceSummaryRecord,
};
use crate::telemetry::manager::{
    GetTraceQuery, ListTracesQuery, TraceDetailSelection, TraceListView, TraceManager,
    TraceManagerError,
};
use crate::transport::{grpc_span, instrument_grpc};
use crate::workspaces::WorkspaceName;

const DEFAULT_TRACE_PAGE_SIZE: usize = 50;
const MAX_TRACE_PAGE_SIZE: usize = 200;

#[derive(Clone)]
pub(crate) struct TraceService {
    traces: TraceManager,
}

impl TraceService {
    pub(crate) fn new(trace_manager: TraceManager) -> Self {
        Self {
            traces: trace_manager,
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
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let page_size = normalize_page_size(request.page_size);
            let offset = parse_page_token(&request.page_token)?;
            let workspace = workspace_filter_from_proto(request.workspace.as_ref())?;
            let view = trace_list_view_from_proto(request.view)?;
            let page = traces
                .list_traces(ListTracesQuery {
                    view,
                    workspace,
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
        instrument_grpc(span, async move {
            let request = request.into_inner();
            if request.trace_id.trim().is_empty() {
                return Err(Status::new(
                    Code::InvalidArgument,
                    "invalid input: missing trace_id",
                ));
            }
            let workspace = workspace_filter_from_proto(request.workspace.as_ref())?;
            let selection = trace_detail_selection_from_proto(&request.root_span_id);
            let trace = traces
                .get_trace(GetTraceQuery {
                    trace_id: request.trace_id,
                    workspace,
                    selection,
                })
                .await
                .map_err(trace_manager_status)?;
            Ok(Response::new(trace_detail_to_proto(trace)))
        })
        .await
    }
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

fn trace_detail_selection_from_proto(root_span_id: &str) -> TraceDetailSelection {
    let root_span_id = root_span_id.trim();
    if root_span_id.is_empty() {
        TraceDetailSelection::Full
    } else {
        TraceDetailSelection::QueryStreamOperation {
            root_span_id: root_span_id.to_string(),
        }
    }
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use coral_api::v1::trace_service_server::TraceService as TraceServiceApi;
    use coral_api::v1::{
        GetTraceRequest, ListTracesRequest, TraceOperationKind, TraceView, Workspace,
    };
    use serde_json::json;
    use tempfile::TempDir;
    use tonic::{Code, Request};

    use super::{TraceService, normalize_page_size, parse_page_token};
    use crate::telemetry::TraceManager;

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

    #[tokio::test]
    async fn trace_service_scopes_list_and_get_by_workspace() {
        let temp = TempDir::new().expect("temp dir");
        let trace_store = temp.path().join("trace-store");
        std::fs::create_dir_all(&trace_store).expect("trace store dir");
        write_trace_records(
            &trace_store,
            &[
                trace_record_json("alpha-trace", "alpha-span", "alpha", 10, 20),
                trace_record_json("beta-trace", "beta-span", "beta", 30, 40),
            ],
        );
        let service = TraceService::new(TraceManager::new(trace_store, Duration::from_mins(1)));

        let response = TraceServiceApi::list_traces(
            &service,
            Request::new(ListTracesRequest {
                page_size: 10,
                page_token: String::new(),
                workspace: Some(workspace("alpha")),
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

        let detail = TraceServiceApi::get_trace(
            &service,
            Request::new(GetTraceRequest {
                trace_id: "alpha-trace".to_string(),
                workspace: Some(workspace("alpha")),
                root_span_id: String::new(),
            }),
        )
        .await
        .expect("get alpha trace")
        .into_inner();
        assert_eq!(detail.spans.len(), 1);

        let status = TraceServiceApi::get_trace(
            &service,
            Request::new(GetTraceRequest {
                trace_id: "beta-trace".to_string(),
                workspace: Some(workspace("alpha")),
                root_span_id: String::new(),
            }),
        )
        .await
        .expect_err("beta trace should not match alpha workspace");
        assert_eq!(status.code(), Code::NotFound);
    }

    #[tokio::test]
    async fn trace_service_projects_and_selects_query_stream_entries() {
        let temp = TempDir::new().expect("temp dir");
        let trace_store = temp.path().join("trace-store");
        std::fs::create_dir_all(&trace_store).expect("trace store dir");
        write_query_stream_trace_fixture(&trace_store);
        let service = TraceService::new(TraceManager::new(trace_store, Duration::from_mins(1)));

        let response = TraceServiceApi::list_traces(
            &service,
            Request::new(ListTracesRequest {
                page_size: 10,
                page_token: String::new(),
                workspace: Some(workspace("alpha")),
                view: TraceView::QueryStream as i32,
            }),
        )
        .await
        .expect("list query stream")
        .into_inner();
        assert_eq!(response.traces.len(), 1);
        let summary = response.traces.first().expect("tool summary");
        assert_eq!(summary.root_span_id, "tool-span");
        assert_eq!(summary.operation_kind, TraceOperationKind::Tool as i32);
        assert_eq!(summary.operation_name, "sql");
        assert_eq!(summary.query, "SELECT 42");
        assert_eq!(summary.start_time_unix_nanos, 10);
        assert_eq!(summary.end_time_unix_nanos, 40);

        let detail = TraceServiceApi::get_trace(
            &service,
            Request::new(GetTraceRequest {
                trace_id: "shared-trace".to_string(),
                workspace: Some(workspace("alpha")),
                root_span_id: "tool-span".to_string(),
            }),
        )
        .await
        .expect("get selected query stream entry")
        .into_inner();
        assert_eq!(
            detail.summary.expect("selected summary").root_span_id,
            "tool-span"
        );

        let internal_status = TraceServiceApi::get_trace(
            &service,
            Request::new(GetTraceRequest {
                trace_id: "shared-trace".to_string(),
                workspace: Some(workspace("alpha")),
                root_span_id: "nested-query".to_string(),
            }),
        )
        .await
        .expect_err("collapsed nested entry is not addressable");
        assert_eq!(internal_status.code(), Code::NotFound);

        let unknown_view = TraceServiceApi::list_traces(
            &service,
            Request::new(ListTracesRequest {
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

    fn write_query_stream_trace_fixture(trace_store: &std::path::Path) {
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
