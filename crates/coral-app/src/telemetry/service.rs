//! Implements the gRPC `TraceService` for local trace inspection.

use std::path::PathBuf;
use std::time::Duration;

use coral_api::v1::trace_service_server::TraceService as TraceServiceApi;
use coral_api::v1::{
    GetTraceRequest, GetTraceResponse, ListTracesRequest, ListTracesResponse, TraceSpan,
    TraceStatus, TraceSummary, Workspace,
};
use tonic::{Code, Request, Response, Status};

use crate::bootstrap::app_status;
use crate::telemetry::local_store::{
    StoredTraceStatus, TraceDetailRecord, TraceSpanRecord, TraceStore, TraceStoreError,
    TraceSummaryRecord,
};
use crate::transport::{grpc_span, instrument_grpc};
use crate::workspaces::WorkspaceName;

const DEFAULT_TRACE_PAGE_SIZE: usize = 50;
const MAX_TRACE_PAGE_SIZE: usize = 200;

#[derive(Clone)]
pub(crate) struct TraceService {
    traces: TraceStore,
}

impl TraceService {
    pub(crate) fn new(trace_store_file: PathBuf, retention: Duration) -> Self {
        Self {
            traces: TraceStore::with_retention(trace_store_file, retention),
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
            let workspace_name = workspace_filter_from_proto(request.workspace.as_ref())?;
            let mut summaries = match workspace_name {
                Some(workspace_name) => {
                    traces
                        .list_traces_for_workspace(
                            page_size.saturating_add(1),
                            offset,
                            workspace_name,
                        )
                        .await
                }
                None => {
                    traces
                        .list_traces(page_size.saturating_add(1), offset)
                        .await
                }
            }
            .map_err(trace_store_status)?;
            let next_page_token = if summaries.len() > page_size {
                summaries.truncate(page_size);
                offset.saturating_add(page_size).to_string()
            } else {
                String::new()
            };
            Ok(Response::new(ListTracesResponse {
                traces: summaries.into_iter().map(trace_summary_to_proto).collect(),
                next_page_token,
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
            let workspace_name = workspace_filter_from_proto(request.workspace.as_ref())?;
            let trace = match workspace_name {
                Some(workspace_name) => {
                    traces
                        .get_trace_for_workspace(request.trace_id, workspace_name)
                        .await
                }
                None => traces.get_trace(request.trace_id).await,
            }
            .map_err(trace_store_status)?;
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

fn workspace_filter_from_proto(workspace: Option<&Workspace>) -> Result<Option<String>, Status> {
    workspace
        .map(|workspace| {
            WorkspaceName::parse(&workspace.name)
                .map(|workspace_name| workspace_name.as_str().to_string())
                .map_err(app_status)
        })
        .transpose()
}

fn trace_store_status(error: TraceStoreError) -> Status {
    match error {
        TraceStoreError::NotFound(trace_id) => {
            Status::new(Code::NotFound, format!("trace '{trace_id}' not found"))
        }
        TraceStoreError::ReadDir { .. }
        | TraceStoreError::OpenFile { .. }
        | TraceStoreError::FileMetadata { .. }
        | TraceStoreError::WriteFile { .. }
        | TraceStoreError::RemoveFile { .. }
        | TraceStoreError::RestoreFile { .. }
        | TraceStoreError::WriterRegistryPoisoned
        | TraceStoreError::WriterPoisoned
        | TraceStoreError::CloseActiveWriter { .. }
        | TraceStoreError::ReadFile { .. }
        | TraceStoreError::PruneExpired { .. }
        | TraceStoreError::Worker { .. } => Status::new(Code::Internal, error.to_string()),
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use coral_api::v1::trace_service_server::TraceService as TraceServiceApi;
    use coral_api::v1::{GetTraceRequest, ListTracesRequest, Workspace};
    use serde_json::json;
    use tempfile::TempDir;
    use tonic::{Code, Request};

    use super::{TraceService, normalize_page_size, parse_page_token};

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
        let service = TraceService::new(trace_store, Duration::from_mins(1));

        let response = TraceServiceApi::list_traces(
            &service,
            Request::new(ListTracesRequest {
                page_size: 10,
                page_token: String::new(),
                workspace: Some(workspace("alpha")),
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

        let detail = TraceServiceApi::get_trace(
            &service,
            Request::new(GetTraceRequest {
                trace_id: "alpha-trace".to_string(),
                workspace: Some(workspace("alpha")),
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
            }),
        )
        .await
        .expect_err("beta trace should not match alpha workspace");
        assert_eq!(status.code(), Code::NotFound);
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
