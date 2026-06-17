//! Implements the gRPC `TraceService` for local trace inspection.

use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use coral_api::v1::trace_service_server::TraceService as TraceServiceApi;
use coral_api::v1::{
    GetTraceRequest, GetTraceResponse, ListTracesRequest, ListTracesResponse, TraceSpan,
    TraceStatus, TraceSummary,
};
use tonic::{Code, Request, Response, Status};

use crate::authorization::{
    AllowAllWorkspaceReadAuthorizer, AuthorizationError, WorkspaceReadAuthorizer,
    authorization_status,
};
use crate::identity::{SingleUserPrincipalProvider, UserPrincipal, UserPrincipalProvider};
use crate::telemetry::local_store::{
    StoredTraceStatus, TraceDetailRecord, TraceSpanRecord, TraceStore, TraceStoreError,
    TraceSummaryRecord,
};
use crate::transport::instrument_authenticated_grpc;

const DEFAULT_TRACE_PAGE_SIZE: usize = 50;
const MAX_TRACE_PAGE_SIZE: usize = 200;
const FILTERED_TRACE_SCAN_BATCHES: usize = 5;
const FILTERED_TRACE_SCAN_LIMIT: usize = MAX_TRACE_PAGE_SIZE * FILTERED_TRACE_SCAN_BATCHES;
const MAX_TRACE_PAGE_CURSORS: usize = 1024;
const TRACE_PAGE_CURSOR_PREFIX: &str = "cursor-";

#[derive(Clone)]
pub(crate) struct TraceService {
    traces: TraceStore,
    user_principal_provider: Arc<dyn UserPrincipalProvider>,
    workspace_read_authorizer: Arc<dyn WorkspaceReadAuthorizer>,
    page_cursors: Arc<TracePageCursorStore>,
}

impl TraceService {
    pub(crate) fn new(trace_store_file: PathBuf, retention: Duration) -> Self {
        Self {
            traces: TraceStore::with_retention(trace_store_file, retention),
            user_principal_provider: Arc::new(SingleUserPrincipalProvider),
            workspace_read_authorizer: Arc::new(AllowAllWorkspaceReadAuthorizer),
            page_cursors: Arc::new(TracePageCursorStore::default()),
        }
    }

    pub(crate) fn with_user_principal_provider(
        mut self,
        user_principal_provider: Arc<dyn UserPrincipalProvider>,
    ) -> Self {
        self.user_principal_provider = user_principal_provider;
        self
    }

    pub(crate) fn with_workspace_read_authorizer(
        mut self,
        workspace_read_authorizer: Arc<dyn WorkspaceReadAuthorizer>,
    ) -> Self {
        self.workspace_read_authorizer = workspace_read_authorizer;
        self
    }
}

#[derive(Debug, Default)]
struct TracePageCursorStore {
    next_id: AtomicU64,
    cursors: Mutex<TracePageCursors>,
}

#[derive(Debug, Default)]
struct TracePageCursors {
    offsets: HashMap<String, usize>,
    order: VecDeque<String>,
}

impl TracePageCursorStore {
    fn insert(&self, offset: usize) -> Result<String, Status> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let token = format!("{TRACE_PAGE_CURSOR_PREFIX}{id:x}");
        let mut cursors = self
            .cursors
            .lock()
            .map_err(|_poisoned| Status::internal("trace page cursor store is unavailable"))?;
        cursors.offsets.insert(token.clone(), offset);
        cursors.order.push_back(token.clone());
        while cursors.order.len() > MAX_TRACE_PAGE_CURSORS {
            if let Some(expired) = cursors.order.pop_front() {
                cursors.offsets.remove(&expired);
            }
        }
        Ok(token)
    }

    fn resolve(&self, page_token: &str) -> Result<usize, Status> {
        if page_token.is_empty() {
            return Ok(0);
        }
        self.cursors
            .lock()
            .map_err(|_poisoned| Status::internal("trace page cursor store is unavailable"))?
            .offsets
            .get(page_token)
            .copied()
            .ok_or_else(invalid_page_token_status)
    }
}

#[tonic::async_trait]
impl TraceServiceApi for TraceService {
    async fn list_traces(
        &self,
        request: Request<ListTracesRequest>,
    ) -> Result<Response<ListTracesResponse>, Status> {
        let traces = self.traces.clone();
        let workspace_read_authorizer = Arc::clone(&self.workspace_read_authorizer);
        let page_cursors = Arc::clone(&self.page_cursors);
        instrument_authenticated_grpc(
            &self.user_principal_provider,
            request,
            |principal, request| async move {
                let page_size = normalize_page_size(request.page_size);
                let response = if workspace_read_authorizer.allows_all_workspace_reads() {
                    list_traces_unfiltered(&traces, page_size, &request.page_token).await?
                } else {
                    list_traces_filtered(
                        &traces,
                        page_cursors.as_ref(),
                        workspace_read_authorizer.as_ref(),
                        &principal,
                        page_size,
                        &request.page_token,
                    )
                    .await?
                };
                Ok(Response::new(response))
            },
        )
        .await
    }

    async fn get_trace(
        &self,
        request: Request<GetTraceRequest>,
    ) -> Result<Response<GetTraceResponse>, Status> {
        let traces = self.traces.clone();
        let workspace_read_authorizer = Arc::clone(&self.workspace_read_authorizer);
        instrument_authenticated_grpc(
            &self.user_principal_provider,
            request,
            |principal, request| async move {
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
                authorize_trace_summary(
                    workspace_read_authorizer.as_ref(),
                    &principal,
                    &trace.summary,
                )
                .await?;
                Ok(Response::new(trace_detail_to_proto(trace)))
            },
        )
        .await
    }
}

async fn list_traces_unfiltered(
    traces: &TraceStore,
    page_size: usize,
    page_token: &str,
) -> Result<ListTracesResponse, Status> {
    let offset = parse_page_token(page_token)?;
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
    Ok(ListTracesResponse {
        traces: summaries.into_iter().map(trace_summary_to_proto).collect(),
        next_page_token,
    })
}

async fn list_traces_filtered(
    traces: &TraceStore,
    page_cursors: &TracePageCursorStore,
    authorizer: &dyn WorkspaceReadAuthorizer,
    principal: &UserPrincipal,
    page_size: usize,
    page_token: &str,
) -> Result<ListTracesResponse, Status> {
    let mut offset = page_cursors.resolve(page_token)?;
    let mut summaries = Vec::with_capacity(page_size);
    let mut has_more_traces = false;
    let mut scanned = 0usize;

    'scan: loop {
        let remaining_scan = FILTERED_TRACE_SCAN_LIMIT.saturating_sub(scanned);
        if remaining_scan == 0 {
            has_more_traces = trace_store_has_more(traces, offset).await?;
            break;
        }

        let batch_limit = remaining_scan.min(MAX_TRACE_PAGE_SIZE);
        let batch = traces
            .list_traces(batch_limit, offset)
            .await
            .map_err(trace_store_status)?;
        if batch.is_empty() {
            break;
        }

        let batch_len = batch.len();
        scanned = scanned.saturating_add(batch_len);
        for (index, summary) in batch.into_iter().enumerate() {
            offset = offset.saturating_add(1);
            if trace_summary_is_authorized(authorizer, principal, &summary).await? {
                summaries.push(summary);
                if summaries.len() == page_size {
                    has_more_traces = index + 1 < batch_len
                        || (batch_len == batch_limit
                            && trace_store_has_more(traces, offset).await?);
                    break 'scan;
                }
            }
        }

        if batch_len < batch_limit {
            break;
        }
    }

    let next_page_token = if has_more_traces {
        page_cursors.insert(offset)?
    } else {
        String::new()
    };

    Ok(ListTracesResponse {
        traces: summaries.into_iter().map(trace_summary_to_proto).collect(),
        next_page_token,
    })
}

async fn trace_store_has_more(traces: &TraceStore, offset: usize) -> Result<bool, Status> {
    Ok(!traces
        .list_traces(1, offset)
        .await
        .map_err(trace_store_status)?
        .is_empty())
}

async fn trace_summary_is_authorized(
    authorizer: &dyn WorkspaceReadAuthorizer,
    principal: &UserPrincipal,
    summary: &TraceSummaryRecord,
) -> Result<bool, Status> {
    if summary.workspaces.is_empty() {
        return Ok(authorizer.allows_unscoped_workspace_reads());
    }

    for workspace_id in &summary.workspaces {
        match authorizer
            .authorize_workspace_read(principal, workspace_id)
            .await
        {
            Ok(()) => {}
            Err(AuthorizationError::Forbidden(_)) => return Ok(false),
            Err(error) => return Err(authorization_status(error)),
        }
    }
    Ok(true)
}

async fn authorize_trace_summary(
    authorizer: &dyn WorkspaceReadAuthorizer,
    principal: &UserPrincipal,
    summary: &TraceSummaryRecord,
) -> Result<(), Status> {
    if summary.workspaces.is_empty() {
        if authorizer.allows_unscoped_workspace_reads() {
            return Ok(());
        }
        return Err(Status::permission_denied(
            "trace workspace metadata is unavailable",
        ));
    }

    for workspace_id in &summary.workspaces {
        authorizer
            .authorize_workspace_read(principal, workspace_id)
            .await
            .map_err(authorization_status)?;
    }
    Ok(())
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
    page_token
        .parse()
        .map_err(|_parse_error| invalid_page_token_status())
}

fn invalid_page_token_status() -> Status {
    Status::new(
        Code::InvalidArgument,
        "invalid input: page_token must be returned by ListTraces",
    )
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
    use std::fs;
    use std::path::Path;
    use std::sync::Arc;
    use std::time::Duration;

    use coral_api::v1::trace_service_server::TraceService as TraceServiceApi;
    use coral_api::v1::{GetTraceRequest, ListTracesRequest};
    use serde_json::json;
    use tempfile::TempDir;
    use tonic::{Code, Request};

    use super::{TraceService, normalize_page_size, parse_page_token};
    use crate::authorization::{AuthorizationError, WorkspaceReadAuthorizer};
    use crate::identity::UserPrincipal;
    use crate::telemetry::local_store::{StoredTraceStatus, TraceSpanRecord};

    #[derive(Debug)]
    struct AllowOneWorkspaceAuthorizer;

    #[tonic::async_trait]
    impl WorkspaceReadAuthorizer for AllowOneWorkspaceAuthorizer {
        async fn authorize_workspace_read(
            &self,
            _principal: &UserPrincipal,
            workspace_id: &str,
        ) -> Result<(), AuthorizationError> {
            if workspace_id == "allowed" {
                Ok(())
            } else {
                Err(AuthorizationError::forbidden(format!(
                    "workspace read rejected for {workspace_id}"
                )))
            }
        }
    }

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
    async fn trace_reads_enforce_workspace_read_authorizer() {
        let temp = TempDir::new().expect("temp dir");
        let trace_dir = temp.path().join("traces");
        write_trace_records(
            &trace_dir,
            &[
                trace_record("allowed-trace", "allowed", "SELECT allowed", 10),
                trace_record("denied-trace", "denied", "SELECT denied", 20),
                trace_record_without_workspace("missing-workspace-trace", "SELECT missing", 30),
                trace_record_with_span(
                    "mixed-trace",
                    "allowed-query-span",
                    Some("allowed"),
                    "SELECT mixed allowed",
                    40,
                ),
                trace_record_with_span(
                    "mixed-trace",
                    "denied-query-span",
                    Some("denied"),
                    "SELECT mixed denied",
                    41,
                ),
            ],
        );

        let service = TraceService::new(trace_dir, Duration::from_mins(1))
            .with_workspace_read_authorizer(Arc::new(AllowOneWorkspaceAuthorizer));

        let list = TraceServiceApi::list_traces(
            &service,
            Request::new(ListTracesRequest {
                page_size: 10,
                page_token: String::new(),
            }),
        )
        .await
        .expect("list traces")
        .into_inner();
        assert_eq!(list.traces.len(), 1);
        let summary = list.traces.first().expect("allowed trace summary");
        assert_eq!(summary.trace_id, "allowed-trace");
        assert_eq!(summary.query, "SELECT allowed");
        assert!(list.next_page_token.is_empty());

        let mixed = TraceServiceApi::get_trace(
            &service,
            Request::new(GetTraceRequest {
                trace_id: "mixed-trace".to_string(),
            }),
        )
        .await
        .expect_err("mixed-workspace trace should require every workspace");
        assert_eq!(mixed.code(), Code::PermissionDenied);

        let denied = TraceServiceApi::get_trace(
            &service,
            Request::new(GetTraceRequest {
                trace_id: "denied-trace".to_string(),
            }),
        )
        .await
        .expect_err("denied trace should require workspace read authorization");
        assert_eq!(denied.code(), Code::PermissionDenied);
        assert!(denied.message().contains("workspace read rejected"));

        let missing_workspace = TraceServiceApi::get_trace(
            &service,
            Request::new(GetTraceRequest {
                trace_id: "missing-workspace-trace".to_string(),
            }),
        )
        .await
        .expect_err("trace without workspace metadata should fail closed");
        assert_eq!(missing_workspace.code(), Code::PermissionDenied);

        let allowed = TraceServiceApi::get_trace(
            &service,
            Request::new(GetTraceRequest {
                trace_id: "allowed-trace".to_string(),
            }),
        )
        .await
        .expect("allowed trace")
        .into_inner();
        assert_eq!(allowed.summary.expect("summary").trace_id, "allowed-trace");
    }

    #[tokio::test]
    async fn filtered_trace_pagination_uses_opaque_cursors() {
        let temp = TempDir::new().expect("temp dir");
        let trace_dir = temp.path().join("traces");
        write_trace_records(
            &trace_dir,
            &[
                trace_record("allowed-new-trace", "allowed", "SELECT new", 30),
                trace_record("denied-trace", "denied", "SELECT denied", 20),
                trace_record("allowed-old-trace", "allowed", "SELECT old", 10),
            ],
        );
        let service = TraceService::new(trace_dir, Duration::from_mins(1))
            .with_workspace_read_authorizer(Arc::new(AllowOneWorkspaceAuthorizer));

        let first_page = TraceServiceApi::list_traces(
            &service,
            Request::new(ListTracesRequest {
                page_size: 1,
                page_token: String::new(),
            }),
        )
        .await
        .expect("first trace page")
        .into_inner();
        assert_eq!(
            first_page.traces.first().expect("first trace").trace_id,
            "allowed-new-trace"
        );
        assert!(!first_page.next_page_token.is_empty());
        first_page
            .next_page_token
            .parse::<usize>()
            .expect_err("filtered cursor should not be a raw offset");

        let second_page = TraceServiceApi::list_traces(
            &service,
            Request::new(ListTracesRequest {
                page_size: 1,
                page_token: first_page.next_page_token,
            }),
        )
        .await
        .expect("second trace page")
        .into_inner();
        assert_eq!(
            second_page.traces.first().expect("second trace").trace_id,
            "allowed-old-trace"
        );
    }

    #[tokio::test]
    async fn filtered_trace_pagination_continues_after_denied_scan_window() {
        let temp = TempDir::new().expect("temp dir");
        let trace_dir = temp.path().join("traces");
        let mut records = (0..super::FILTERED_TRACE_SCAN_LIMIT)
            .map(|index| {
                trace_record(
                    &format!("denied-trace-{index}"),
                    "denied",
                    "SELECT denied",
                    i64::try_from(index).expect("fixture index fits i64"),
                )
            })
            .collect::<Vec<_>>();
        records.push(trace_record(
            "allowed-old-trace",
            "allowed",
            "SELECT old",
            -1,
        ));
        write_trace_records(&trace_dir, &records);
        let service = TraceService::new(trace_dir, Duration::from_mins(1))
            .with_workspace_read_authorizer(Arc::new(AllowOneWorkspaceAuthorizer));

        let first_page = TraceServiceApi::list_traces(
            &service,
            Request::new(ListTracesRequest {
                page_size: 1,
                page_token: String::new(),
            }),
        )
        .await
        .expect("first trace page")
        .into_inner();
        assert!(first_page.traces.is_empty());
        assert!(!first_page.next_page_token.is_empty());

        let second_page = TraceServiceApi::list_traces(
            &service,
            Request::new(ListTracesRequest {
                page_size: 1,
                page_token: first_page.next_page_token,
            }),
        )
        .await
        .expect("second trace page")
        .into_inner();
        assert_eq!(
            second_page.traces.first().expect("second trace").trace_id,
            "allowed-old-trace"
        );
        assert!(second_page.next_page_token.is_empty());
    }

    fn write_trace_records(dir: &Path, records: &[TraceSpanRecord]) {
        fs::create_dir_all(dir).expect("create trace dir");
        let contents = records
            .iter()
            .map(|record| serde_json::to_string(record).expect("serialize trace record"))
            .collect::<Vec<_>>()
            .join("\n");
        fs::write(
            dir.join("spans-00000000000000000001-test-0000000000000000.jsonl"),
            format!("{contents}\n"),
        )
        .expect("write trace records");
    }

    fn trace_record(
        trace_id: &str,
        workspace: &str,
        sql: &str,
        end_time_unix_nanos: i64,
    ) -> TraceSpanRecord {
        trace_record_with_span(
            trace_id,
            "query-span",
            Some(workspace),
            sql,
            end_time_unix_nanos,
        )
    }

    fn trace_record_without_workspace(
        trace_id: &str,
        sql: &str,
        end_time_unix_nanos: i64,
    ) -> TraceSpanRecord {
        trace_record_with_span(trace_id, "query-span", None, sql, end_time_unix_nanos)
    }

    fn trace_record_with_span(
        trace_id: &str,
        span_id: &str,
        workspace: Option<&str>,
        sql: &str,
        end_time_unix_nanos: i64,
    ) -> TraceSpanRecord {
        let attributes = workspace.map_or_else(
            || {
                json!({
                    "sql": sql,
                    "status": "ok"
                })
            },
            |workspace| {
                json!({
                    "workspace": workspace,
                    "sql": sql,
                    "status": "ok"
                })
            },
        );

        TraceSpanRecord {
            trace_id: trace_id.to_string(),
            span_id: span_id.to_string(),
            parent_span_id: None,
            parent_span_is_remote: false,
            name: "coral.query".to_string(),
            kind: "internal".to_string(),
            status: StoredTraceStatus::Ok,
            status_message: None,
            start_time_unix_nanos: end_time_unix_nanos.saturating_sub(1),
            end_time_unix_nanos,
            duration_nanos: 1,
            attributes_json: attributes.to_string(),
            events_json: "[]".to_string(),
            links_json: "[]".to_string(),
            resource_json: "{}".to_string(),
            scope_name: "test".to_string(),
            scope_version: None,
            scope_schema_url: None,
            scope_attributes_json: "{}".to_string(),
            trace_flags: 0,
            trace_state: String::new(),
            is_remote: false,
        }
    }
}
