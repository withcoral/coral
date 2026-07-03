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

use super::upsert_trace_summary;
use crate::state::db::{CoralDb, DbError, DbRepos, TraceSummaryRecord};
use crate::telemetry::local_store::{
    StoredTraceStatus, TraceDetailRecord, TraceSpanRecord, TraceStore, TraceStoreError,
};
use crate::transport::{grpc_span, instrument_grpc};

const DEFAULT_TRACE_PAGE_SIZE: usize = 50;
const MAX_TRACE_PAGE_SIZE: usize = 200;
const TRACE_SUMMARY_INDEX_PAGE_SIZE: usize = 200;

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

    pub(crate) async fn backfill_summaries(
        trace_store_file: PathBuf,
        retention: Duration,
        db: &CoralDb,
    ) -> Result<usize, DbError> {
        let traces = TraceStore::with_retention(trace_store_file, retention);
        backfill_trace_summaries(db, &traces).await
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
                list_database_traces(&traces, db.as_ref(), page_size.saturating_add(1), offset)
                    .await?
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

async fn backfill_trace_summaries(db: &CoralDb, traces: &TraceStore) -> Result<usize, DbError> {
    let summaries = traces.list_all_traces_tolerant().await;
    let summaries = match summaries {
        Ok(summaries) => summaries,
        Err(error) => {
            tracing::warn!("skipping local trace summary backfill: {error}");
            return Ok(0);
        }
    };
    let mut imported = 0;
    for summary in summaries {
        if summary.workspace_id.is_none() {
            tracing::warn!(
                trace_id = %summary.trace_id,
                "skipping local trace summary backfill without workspace"
            );
            continue;
        }
        upsert_trace_summary(db, &summary).await?;
        imported += 1;
    }
    Ok(imported)
}

async fn list_database_traces(
    traces: &TraceStore,
    db: &CoralDb,
    limit: usize,
    offset: usize,
) -> Result<Vec<TraceSummaryRecord>, Status> {
    let mut summaries = Vec::new();
    let mut db_offset = offset;
    while summaries.len() < limit {
        let mut session = db;
        let page = session
            .trace_summaries()
            .list(TRACE_SUMMARY_INDEX_PAGE_SIZE, db_offset)
            .await
            .map_err(trace_database_status)?;
        if page.is_empty() {
            break;
        }
        db_offset = db_offset.saturating_add(page.len());
        for summary in page {
            match traces.get_trace(summary.trace_id.clone()).await {
                Ok(_trace) => summaries.push(summary),
                Err(TraceStoreError::NotFound(_)) => {
                    if let Err(error) = delete_trace_summary(db, &summary).await {
                        tracing::warn!(
                            trace_id = %summary.trace_id,
                            "failed to prune stale database trace summary: {error}"
                        );
                    }
                }
                Err(error) => return Err(trace_store_status(error)),
            }
            if summaries.len() == limit {
                break;
            }
        }
    }
    Ok(summaries)
}

async fn delete_trace_summary(db: &CoralDb, summary: &TraceSummaryRecord) -> Result<(), DbError> {
    let Some(workspace_id) = summary.workspace_id.as_deref() else {
        return Ok(());
    };
    let mut tx = db.begin().await?;
    tx.trace_summaries()
        .delete(workspace_id, &summary.trace_id)
        .await?;
    tx.commit().await?;
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
    use std::sync::Arc;
    use std::time::Duration;

    use coral_api::v1::ListTracesRequest;
    use coral_api::v1::trace_service_server::TraceService as _;
    use serde_json::json;
    use tempfile::tempdir;
    use tonic::Request;

    use super::{
        TraceService, TraceStore, backfill_trace_summaries, normalize_page_size, parse_page_token,
    };
    use crate::state::AppStateLayout;
    use crate::state::db::{CoralDb, DatabaseConfig, DbRepos, ResolvedDatabaseConfig};
    use crate::telemetry::{StoredTraceStatus, TraceSummaryRecord};

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
    async fn backfill_summaries_adds_local_rows_without_replacing_database_rows() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        let db = open_sqlite(&layout).await;
        let trace_dir = temp.path().join("traces");
        write_trace(&trace_dir, "trace-1", "work", 1, 2);
        ensure_workspace(&db, "work").await;
        let remote = summary("remote-trace", "remote", 10);
        insert_summary(&db, &remote).await;

        let traces = TraceStore::with_retention(trace_dir.clone(), Duration::from_mins(1));
        assert_eq!(
            backfill_trace_summaries(&db, &traces)
                .await
                .expect("backfill trace summaries"),
            1
        );
        let mut session = &db;
        let summaries = session
            .trace_summaries()
            .list(10, 0)
            .await
            .expect("list trace summaries");
        assert_eq!(
            summaries
                .iter()
                .map(|summary| summary.trace_id.as_str())
                .collect::<Vec<_>>(),
            vec!["remote-trace", "trace-1"]
        );

        fs::remove_dir_all(&trace_dir).expect("remove raw trace store");
        assert_eq!(
            backfill_trace_summaries(&db, &traces)
                .await
                .expect("backfill empty trace store"),
            0
        );
        assert_eq!(
            session
                .trace_summaries()
                .list(10, 0)
                .await
                .expect("list preserved trace summaries")
                .len(),
            2
        );
    }

    #[tokio::test]
    async fn exported_summary_is_visible_and_pruned_without_retained_details() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        let db = Arc::new(open_sqlite(&layout).await);
        let trace_dir = temp.path().join("traces");
        write_trace(&trace_dir, "trace-1", "work", 1, 2);
        ensure_workspace(db.as_ref(), "work").await;
        let traces = TraceStore::with_retention(trace_dir.clone(), Duration::from_mins(1));
        let trace = traces
            .get_trace("trace-1".to_string())
            .await
            .expect("trace detail");
        super::super::upsert_trace_summary(db.as_ref(), &trace.summary)
            .await
            .expect("upsert exported summary");

        let service =
            TraceService::with_db(trace_dir.clone(), Duration::from_mins(1), Arc::clone(&db));
        let response = service
            .list_traces(Request::new(ListTracesRequest {
                page_size: 10,
                page_token: String::new(),
            }))
            .await
            .expect("list traces")
            .into_inner();

        assert_eq!(response.traces.len(), 1);
        let trace = response.traces.into_iter().next().expect("trace summary");
        assert_eq!(trace.trace_id, "trace-1");

        fs::remove_dir_all(&trace_dir).expect("remove raw trace store");
        let response = service
            .list_traces(Request::new(ListTracesRequest {
                page_size: 10,
                page_token: String::new(),
            }))
            .await
            .expect("list traces")
            .into_inner();

        assert!(response.traces.is_empty());
        let mut session = db.as_ref();
        assert!(
            session
                .trace_summaries()
                .list(10, 0)
                .await
                .expect("list trace summaries")
                .is_empty()
        );
    }

    #[tokio::test]
    #[ignore = "set CORAL_TEST_POSTGRES_URL to run the trace summary backfill harness against Postgres"]
    async fn backfill_summaries_preserves_existing_postgres_rows() {
        let Some(url) = crate::bootstrap::env_var("CORAL_TEST_POSTGRES_URL") else {
            return;
        };
        let db = CoralDb::open(ResolvedDatabaseConfig::Postgres { url })
            .await
            .expect("open postgres");
        db.migrate().await.expect("migrate postgres");
        let suffix = uuid::Uuid::new_v4().simple().to_string();
        let local_workspace = format!("trace_local_{suffix}");
        let remote_workspace = format!("trace_remote_{suffix}");
        let local_trace = format!("local_trace_{suffix}");
        let remote_trace = format!("remote_trace_{suffix}");
        let temp = tempdir().expect("temp dir");
        let trace_dir = temp.path().join("traces");
        write_trace(&trace_dir, &local_trace, &local_workspace, 1, 2);
        ensure_workspace(&db, &local_workspace).await;
        insert_summary(&db, &summary(&remote_trace, &remote_workspace, 10)).await;

        let traces = TraceStore::with_retention(trace_dir, Duration::from_mins(1));
        assert_eq!(
            backfill_trace_summaries(&db, &traces)
                .await
                .expect("backfill trace summaries"),
            1
        );

        let mut session = &db;
        assert!(
            session
                .trace_summaries()
                .get(&local_trace)
                .await
                .expect("get local trace")
                .is_some()
        );
        assert!(
            session
                .trace_summaries()
                .get(&remote_trace)
                .await
                .expect("get remote trace")
                .is_some()
        );

        cleanup_workspace(&db, &local_workspace).await;
        cleanup_workspace(&db, &remote_workspace).await;
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

    async fn insert_summary(db: &CoralDb, summary: &TraceSummaryRecord) {
        let mut tx = db.begin().await.expect("begin tx");
        ensure_workspace_in_session(&mut tx, summary.workspace_id.as_deref().expect("workspace"))
            .await;
        tx.trace_summaries()
            .upsert(summary)
            .await
            .expect("upsert trace summary");
        tx.commit().await.expect("commit trace summary");
    }

    async fn ensure_workspace(db: &CoralDb, workspace_id: &str) {
        let mut tx = db.begin().await.expect("begin tx");
        ensure_workspace_in_session(&mut tx, workspace_id).await;
        tx.commit().await.expect("commit workspace");
    }

    async fn cleanup_workspace(db: &CoralDb, workspace_id: &str) {
        let mut tx = db.begin().await.expect("begin tx");
        tx.workspaces()
            .remove(workspace_id)
            .await
            .expect("remove workspace");
        tx.commit().await.expect("commit cleanup");
    }

    async fn ensure_workspace_in_session<S>(session: &mut S, workspace_id: &str)
    where
        S: crate::state::db::DbSession,
    {
        session
            .workspaces()
            .ensure(workspace_id, 1)
            .await
            .expect("ensure workspace");
    }

    fn summary(trace_id: &str, workspace_id: &str, end_time_unix_nanos: i64) -> TraceSummaryRecord {
        TraceSummaryRecord {
            trace_id: trace_id.to_string(),
            workspace_id: Some(workspace_id.to_string()),
            root_span_id: "span-1".to_string(),
            name: "coral.query".to_string(),
            query: "SELECT 1".to_string(),
            status: StoredTraceStatus::Ok,
            start_time_unix_nanos: end_time_unix_nanos - 1,
            end_time_unix_nanos,
            duration_nanos: 1,
            span_count: 1,
            row_count: 1,
            row_count_recorded: true,
        }
    }

    fn write_trace(
        dir: &std::path::Path,
        trace_id: &str,
        workspace_id: &str,
        start: i64,
        end: i64,
    ) {
        fs::create_dir_all(dir).expect("create trace dir");
        let attributes_json = serde_json::to_string(
            &json!({
                "sql": "SELECT 1",
                "row_count": 1,
                "workspace": workspace_id,
            })
            .to_string(),
        )
        .expect("encode attributes");
        let duration = end - start;
        let record = format!(
            r#"{{"trace_id":"{trace_id}","span_id":"span-1","parent_span_id":null,"parent_span_is_remote":false,"name":"coral.query","kind":"internal","status":"ok","status_message":null,"start_time_unix_nanos":{start},"end_time_unix_nanos":{end},"duration_nanos":{duration},"attributes_json":{attributes_json},"events_json":"[]","links_json":"[]","resource_json":"{{}}","scope_name":"test","scope_version":null,"scope_schema_url":null,"scope_attributes_json":"{{}}","trace_flags":0,"trace_state":"","is_remote":false}}"#
        );
        fs::write(
            dir.join(format!("spans-{start:020}-test-{trace_id}.jsonl")),
            format!("{record}\n"),
        )
        .expect("write trace");
    }
}
