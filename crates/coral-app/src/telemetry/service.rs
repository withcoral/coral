//! Implements the gRPC `TraceService` for local trace inspection.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use coral_api::v1::trace_service_server::TraceService as TraceServiceApi;
use coral_api::v1::{
    GetTraceRequest, GetTraceResponse, ListTracesRequest, ListTracesResponse, TraceSpan,
    TraceStatus, TraceSummary,
};
use tonic::{Code, Request, Response, Status};

use crate::state::db::{CoralDb, DbError, DbRepos};
use crate::telemetry::local_store::{
    StoredTraceStatus, TraceDetailRecord, TraceSpanRecord, TraceStore, TraceStoreError,
    TraceSummaryRecord,
};
use crate::transport::{grpc_span, instrument_grpc};

const DEFAULT_TRACE_PAGE_SIZE: usize = 50;
const MAX_TRACE_PAGE_SIZE: usize = 200;
const TRACE_SUMMARY_INDEX_PAGE_SIZE: usize = 200;

#[derive(Debug, thiserror::Error)]
pub(crate) enum TraceSummaryIndexError {
    #[error(transparent)]
    Store(#[from] TraceStoreError),
    #[error(transparent)]
    Database(#[from] DbError),
}

#[derive(Clone)]
pub(crate) struct TraceService {
    traces: TraceStore,
    db: Option<Arc<CoralDb>>,
}

impl TraceService {
    #[cfg(test)]
    pub(crate) fn new(trace_store_file: PathBuf, retention: Duration) -> Self {
        Self {
            traces: TraceStore::with_retention(trace_store_file, retention),
            db: None,
        }
    }

    pub(crate) fn with_db(
        trace_store_file: PathBuf,
        retention: Duration,
        db: Arc<CoralDb>,
    ) -> Self {
        Self {
            traces: TraceStore::with_retention(trace_store_file, retention),
            db: Some(db),
        }
    }

    pub(crate) async fn sync_summaries(
        trace_store_file: PathBuf,
        retention: Duration,
        db: &CoralDb,
    ) -> Result<usize, TraceSummaryIndexError> {
        let traces = TraceStore::with_retention(trace_store_file, retention);
        sync_trace_summaries(db, &traces).await
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
        let db = self.db.clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let page_size = normalize_page_size(request.page_size);
            let offset = parse_page_token(&request.page_token)?;
            let mut summaries = if let Some(db) = db {
                let mut session = db.as_ref();
                session
                    .trace_summaries()
                    .list(page_size.saturating_add(1), offset)
                    .await
                    .map_err(trace_database_status)?
            } else {
                traces
                    .list_traces(page_size.saturating_add(1), offset)
                    .await
                    .map_err(trace_store_status)?
            };
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

async fn sync_trace_summaries(
    db: &CoralDb,
    traces: &TraceStore,
) -> Result<usize, TraceSummaryIndexError> {
    let summaries = current_trace_summaries(traces).await?;
    let imported = summaries.len();
    let mut tx = db.begin().await?;
    tx.trace_summaries().replace_all(&summaries).await?;
    tx.commit().await?;
    Ok(imported)
}

async fn current_trace_summaries(
    traces: &TraceStore,
) -> Result<Vec<TraceSummaryRecord>, TraceStoreError> {
    let mut summaries = Vec::new();
    loop {
        let page = traces
            .list_traces(TRACE_SUMMARY_INDEX_PAGE_SIZE, summaries.len())
            .await?;
        let page_len = page.len();
        summaries.extend(page);
        if page_len < TRACE_SUMMARY_INDEX_PAGE_SIZE {
            break;
        }
    }
    Ok(summaries)
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

#[expect(
    clippy::needless_pass_by_value,
    reason = "used directly as a map_err adapter across tonic service handlers"
)]
fn trace_database_status(error: DbError) -> Status {
    Status::new(Code::Internal, error.to_string())
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
    use std::fs;
    use std::time::Duration;

    use serde_json::json;
    use tempfile::tempdir;

    use super::{TraceStore, normalize_page_size, parse_page_token, sync_trace_summaries};
    use crate::state::AppStateLayout;
    use crate::state::db::{CoralDb, DatabaseConfig, DbRepos, ResolvedDatabaseConfig};

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
    async fn sync_summaries_rebuilds_database_index_from_jsonl() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        let db = open_sqlite(&layout).await;
        let trace_dir = temp.path().join("traces");
        write_trace(&trace_dir, "trace-1", 1, 2);

        let traces = TraceStore::with_retention(trace_dir.clone(), Duration::from_mins(1));
        assert_eq!(
            sync_trace_summaries(&db, &traces)
                .await
                .expect("sync trace summaries"),
            1
        );
        let mut session = &db;
        let summaries = session
            .trace_summaries()
            .list(10, 0)
            .await
            .expect("list trace summaries");
        assert_eq!(summaries.len(), 1);
        let summary = summaries.first().expect("trace summary");
        assert_eq!(summary.trace_id, "trace-1");
        assert_eq!(summary.query, "SELECT 1");
        assert_eq!(summary.row_count, 1);
        assert!(summary.row_count_recorded);

        fs::remove_dir_all(&trace_dir).expect("remove raw trace store");
        assert_eq!(
            sync_trace_summaries(&db, &traces)
                .await
                .expect("resync empty trace store"),
            0
        );
        assert!(
            session
                .trace_summaries()
                .list(10, 0)
                .await
                .expect("list empty trace summaries")
                .is_empty()
        );
    }

    async fn open_sqlite(layout: &AppStateLayout) -> CoralDb {
        let config = DatabaseConfig::load(layout).expect("db config");
        let DatabaseConfig::Sqlite { path } = config else {
            panic!("default test config should be sqlite");
        };
        let db = CoralDb::open(ResolvedDatabaseConfig::Sqlite { path })
            .await
            .expect("open sqlite");
        db.migrate().await.expect("migrate sqlite");
        db
    }

    fn write_trace(dir: &std::path::Path, trace_id: &str, start: i64, end: i64) {
        fs::create_dir_all(dir).expect("create trace dir");
        let record = json!({
            "trace_id": trace_id,
            "span_id": "span-1",
            "parent_span_id": null,
            "name": "coral.query",
            "status": "ok",
            "start_time_unix_nanos": start,
            "end_time_unix_nanos": end,
            "attributes_json": r#"{"sql":"SELECT 1","row_count":1}"#,
        });
        fs::write(
            dir.join(format!("spans-{start:020}-test-{trace_id}.jsonl")),
            format!("{record}\n"),
        )
        .expect("write trace");
    }
}
