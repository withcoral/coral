//! Implements the gRPC `TraceService` for local trace inspection.

use std::path::PathBuf;
use std::time::Duration;

use coral_api::v1::trace_service_server::TraceService as TraceServiceApi;
use coral_api::v1::{
    GetTraceRequest, GetTraceResponse, ListTracesRequest, ListTracesResponse, TraceSpan,
    TraceStatus, TraceSummary,
};
use tonic::{Code, Request, Response, Status};

use crate::telemetry::local_store::{
    StoredTraceStatus, TraceDetailRecord, TraceSpanRecord, TraceStore, TraceStoreError,
    TraceSummaryRecord,
};
use crate::transport::{grpc_span, instrument_grpc, json_value_to_proto};

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
            let mut summaries = traces
                .list_traces(page_size.saturating_add(1), offset)
                .await
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
            let trace = traces
                .get_trace(request.trace_id)
                .await
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

fn trace_store_status(error: TraceStoreError) -> Status {
    match error {
        TraceStoreError::NotFound(trace_id) => {
            Status::new(Code::NotFound, format!("trace '{trace_id}' not found"))
        }
        TraceStoreError::ReadDir { .. }
        | TraceStoreError::OpenFile { .. }
        | TraceStoreError::FileMetadata { .. }
        | TraceStoreError::ReadFile { .. }
        | TraceStoreError::DecodeLine { .. }
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
    let attributes_json = json_payload(&span.attributes);
    let events_json = json_payload(&span.events);
    let links_json = json_payload(&span.links);
    let resource_json = json_payload(&span.resource);
    let scope_attributes_json = json_payload(&span.scope_attributes);
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
        attributes_json,
        events_json,
        links_json,
        resource_json,
        attributes: Some(json_value_to_proto(span.attributes)),
        events: Some(json_value_to_proto(span.events)),
        links: Some(json_value_to_proto(span.links)),
        resource: Some(json_value_to_proto(span.resource)),
        scope_name: span.scope_name,
        scope_version: span.scope_version.unwrap_or_default(),
        scope_schema_url: span.scope_schema_url.unwrap_or_default(),
        scope_attributes_json,
        scope_attributes: Some(json_value_to_proto(span.scope_attributes)),
        trace_flags: span.trace_flags,
        trace_state: span.trace_state,
        is_remote: span.is_remote,
    }
}

fn json_payload(value: &serde_json::Value) -> String {
    serde_json::to_string(value).unwrap_or_default()
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
    use super::{normalize_page_size, parse_page_token};
    use serde_json::json;

    use crate::telemetry::local_store::{StoredTraceStatus, TraceSpanRecord};
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
    fn trace_span_proto_dual_populates_json_strings_and_structured_values() {
        let proto = super::trace_span_to_proto(TraceSpanRecord {
            trace_id: "trace".to_string(),
            span_id: "span".to_string(),
            parent_span_id: None,
            parent_span_is_remote: false,
            name: "span-name".to_string(),
            kind: "internal".to_string(),
            status: StoredTraceStatus::Ok,
            status_message: None,
            start_time_unix_nanos: 1,
            end_time_unix_nanos: 2,
            duration_nanos: 1,
            attributes: json!({ "http.method": "GET" }),
            events: json!([{ "name": "event" }]),
            links: json!([]),
            resource: json!({ "service.name": "coral" }),
            scope_name: "scope".to_string(),
            scope_version: None,
            scope_schema_url: None,
            scope_attributes: json!({ "scope.attr": true }),
            trace_flags: 1,
            trace_state: "state".to_string(),
            is_remote: false,
        });

        assert_eq!(proto.attributes_json, r#"{"http.method":"GET"}"#);
        assert_eq!(proto.events_json, r#"[{"name":"event"}]"#);
        assert_eq!(proto.links_json, "[]");
        assert_eq!(proto.resource_json, r#"{"service.name":"coral"}"#);
        assert_eq!(proto.scope_attributes_json, r#"{"scope.attr":true}"#);
        assert_eq!(
            proto
                .attributes
                .map(serde_json::Value::from)
                .expect("structured attributes"),
            json!({ "http.method": "GET" })
        );
        assert_eq!(
            proto
                .scope_attributes
                .map(serde_json::Value::from)
                .expect("structured scope attributes"),
            json!({ "scope.attr": true })
        );
    }
}
