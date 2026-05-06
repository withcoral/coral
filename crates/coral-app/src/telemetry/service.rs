//! Implements the gRPC `TraceService` for local trace inspection.

use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use coral_api::v1::trace_service_server::TraceService as TraceServiceApi;
use coral_api::v1::{
    GetTraceRequest, GetTraceResponse, ListTracesRequest, ListTracesResponse, RecreatedQueryPlan,
    TraceSpan, TraceStatus, TraceSummary,
};
use coral_engine::QueryPlan;
use serde_json::Value as JsonValue;
use tonic::{Code, Request, Response, Status};

use crate::query::manager::{QueryManager, QueryManagerError};
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
    queries: QueryManager,
}

impl TraceService {
    pub(crate) fn new(trace_store_file: PathBuf, queries: QueryManager) -> Self {
        Self {
            traces: TraceStore::new(trace_store_file),
            queries,
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
            let mut summaries = traces
                .list_traces(page_size.saturating_add(1), offset)
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
        let queries = self.queries.clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            if request.trace_id.trim().is_empty() {
                return Err(Status::new(
                    Code::InvalidArgument,
                    "invalid input: missing trace_id",
                ));
            }
            let trace = traces
                .get_trace(&request.trace_id)
                .map_err(trace_store_status)?;
            let recreated_plan = recreate_query_plan(&queries, &trace).await;
            Ok(Response::new(trace_detail_to_proto(trace, recreated_plan)))
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
    page_token.parse().map_err(|_| {
        Status::new(
            Code::InvalidArgument,
            "invalid input: page_token must be returned by ListTraces",
        )
    })
}

fn trace_store_status(error: TraceStoreError) -> Status {
    match error {
        TraceStoreError::NotFound(trace_id) => {
            Status::new(Code::NotFound, format!("trace '{trace_id}' not found"))
        }
        TraceStoreError::ReadDir { .. }
        | TraceStoreError::OpenFile { .. }
        | TraceStoreError::ReadFile { .. }
        | TraceStoreError::DecodeLine { .. } => Status::new(Code::Internal, error.to_string()),
    }
}

async fn recreate_query_plan(
    queries: &QueryManager,
    trace: &TraceDetailRecord,
) -> Option<RecreatedQueryPlan> {
    let (workspace, sql) = query_span_inputs(trace)?;
    let generated_at_unix_nanos = unix_nanos(SystemTime::now());
    let workspace_name = match WorkspaceName::parse(&workspace) {
        Ok(workspace_name) => workspace_name,
        Err(error) => {
            return Some(plan_error_proto(
                workspace,
                sql,
                generated_at_unix_nanos,
                error.to_string(),
            ));
        }
    };

    match queries.plan_sql(&workspace_name, &sql).await {
        Ok(plan) => Some(plan_to_proto(
            workspace,
            sql,
            generated_at_unix_nanos,
            &plan,
        )),
        Err(error) => Some(plan_error_proto(
            workspace,
            sql,
            generated_at_unix_nanos,
            query_manager_error_message(error),
        )),
    }
}

fn query_span_inputs(trace: &TraceDetailRecord) -> Option<(String, String)> {
    trace.spans.iter().find_map(|span| {
        if span.name != "coral.query" {
            return None;
        }
        let attributes = serde_json::from_str::<JsonValue>(&span.attributes_json).ok()?;
        let workspace = attr_string(&attributes, "workspace")?;
        let sql = attr_string(&attributes, "sql")?;
        Some((workspace, sql))
    })
}

fn attr_string(attributes: &JsonValue, key: &str) -> Option<String> {
    match attributes.get(key)? {
        JsonValue::String(value) => Some(value.clone()),
        JsonValue::Number(value) => Some(value.to_string()),
        JsonValue::Bool(value) => Some(value.to_string()),
        _ => None,
    }
}

fn plan_to_proto(
    workspace: String,
    sql: String,
    generated_at_unix_nanos: i64,
    plan: &QueryPlan,
) -> RecreatedQueryPlan {
    RecreatedQueryPlan {
        workspace,
        sql,
        unoptimized_logical_plan: plan.unoptimized_logical_plan().to_string(),
        optimized_logical_plan: plan.optimized_logical_plan().to_string(),
        physical_plan: plan.physical_plan().to_string(),
        generated_at_unix_nanos,
        error: String::new(),
    }
}

fn plan_error_proto(
    workspace: String,
    sql: String,
    generated_at_unix_nanos: i64,
    error: String,
) -> RecreatedQueryPlan {
    RecreatedQueryPlan {
        workspace,
        sql,
        unoptimized_logical_plan: String::new(),
        optimized_logical_plan: String::new(),
        physical_plan: String::new(),
        generated_at_unix_nanos,
        error,
    }
}

fn query_manager_error_message(error: QueryManagerError) -> String {
    match error {
        QueryManagerError::App(error) => error.to_string(),
        QueryManagerError::Core(error) => error.to_string(),
    }
}

fn trace_detail_to_proto(
    trace: TraceDetailRecord,
    recreated_plan: Option<RecreatedQueryPlan>,
) -> GetTraceResponse {
    GetTraceResponse {
        summary: Some(trace_summary_to_proto(trace.summary)),
        spans: trace.spans.into_iter().map(trace_span_to_proto).collect(),
        recreated_plan,
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

fn unix_nanos(time: SystemTime) -> i64 {
    let nanos = time
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    i64::try_from(nanos).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::{normalize_page_size, parse_page_token, query_span_inputs};
    use crate::telemetry::local_store::{
        StoredTraceStatus, TraceDetailRecord, TraceSpanRecord, TraceSummaryRecord,
    };

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
        assert!(parse_page_token("not-an-offset").is_err());
    }

    #[test]
    fn query_span_inputs_reads_workspace_and_sql() {
        let trace = TraceDetailRecord {
            summary: TraceSummaryRecord {
                trace_id: "trace".to_string(),
                root_span_id: "span".to_string(),
                name: "coral.query".to_string(),
                query: "SELECT 1".to_string(),
                status: StoredTraceStatus::Ok,
                start_time_unix_nanos: 1,
                end_time_unix_nanos: 2,
                duration_nanos: 1,
                span_count: 1,
                row_count: 1,
                row_count_recorded: true,
            },
            spans: vec![TraceSpanRecord {
                trace_id: "trace".to_string(),
                span_id: "span".to_string(),
                parent_span_id: None,
                parent_span_is_remote: false,
                name: "coral.query".to_string(),
                kind: "internal".to_string(),
                status: StoredTraceStatus::Ok,
                status_message: None,
                start_time_unix_nanos: 1,
                end_time_unix_nanos: 2,
                duration_nanos: 1,
                attributes_json: r#"{"workspace":"default","sql":"SELECT 1"}"#.to_string(),
                events_json: "{}".to_string(),
                links_json: "{}".to_string(),
                resource_json: "{}".to_string(),
                scope_name: "coral".to_string(),
                scope_version: None,
                scope_schema_url: None,
                scope_attributes_json: "{}".to_string(),
                trace_flags: 1,
                trace_state: String::new(),
                is_remote: false,
            }],
        };

        assert_eq!(
            query_span_inputs(&trace),
            Some(("default".to_string(), "SELECT 1".to_string()))
        );
    }
}
