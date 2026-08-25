use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use serde_json::json;
use tempfile::TempDir;

use super::{
    READ_TIMEOUT, ReadError, SearchResponseHistoryReader, bounded_read, selected_search_execution,
};
use crate::state::AppStateLayout;
use crate::state::db::{
    CoralDb, DatabaseConfig, DbError, DbRepos, ResolvedDatabaseConfig, TraceSearchResponseCapture,
    TraceSearchResponseInsertResult, TraceSearchResponseOutcome, now_unix_nanos_i64,
};
use crate::telemetry::local_store::{
    StoredTraceInvocationKind, StoredTraceOperationKind, StoredTraceStatus, TraceDetailRecord,
    TraceSpanRecord, TraceSummaryRecord,
};
use crate::telemetry::manager::{
    GetTraceQuery, RetainedSearchResponse, TraceListView, TraceManager,
};
use crate::workspaces::WorkspaceName;

#[tokio::test(start_paused = true)]
async fn read_times_out_at_budget() {
    let started_at = tokio::time::Instant::now();
    let result = bounded_read(
        std::future::pending::<Result<Option<TraceSearchResponseOutcome>, DbError>>(),
        READ_TIMEOUT,
    )
    .await;

    assert!(matches!(result, Err(ReadError::TimedOut)));
    assert_eq!(
        tokio::time::Instant::now().duration_since(started_at),
        READ_TIMEOUT
    );
}

#[tokio::test(start_paused = true)]
async fn read_returns_completed_result() {
    let outcome = TraceSearchResponseOutcome::Response(vec![1, 2, 3]);
    let started_at = tokio::time::Instant::now();
    let result = bounded_read(std::future::ready(Ok(Some(outcome.clone()))), READ_TIMEOUT).await;

    assert_eq!(result.expect("completed history read"), Some(outcome));
    assert_eq!(tokio::time::Instant::now(), started_at);
}

#[test]
fn direct_search_uses_only_the_selected_root_and_requires_its_workspace() {
    let trace = search_trace(
        StoredTraceInvocationKind::Direct,
        "search-root",
        vec![search_span("search-root", None, 10, "alpha")],
    );
    assert_eq!(
        selected_search_execution(&trace, TraceListView::QueryStream, "alpha")
            .map(|span| span.span_id.as_str()),
        Some("search-root")
    );
    assert_eq!(
        selected_search_execution(&trace, TraceListView::QueryStream, "beta"),
        None
    );
    assert_eq!(
        selected_search_execution(&trace, TraceListView::All, "alpha"),
        None
    );
}

#[test]
fn mcp_search_selection_follows_parent_edges_and_is_deterministic() {
    let mut remote_bridge = operation_span("bridge", Some("tool-root"), "grpc.server", 2, "alpha");
    remote_bridge.parent_span_is_remote = true;
    let trace = search_trace(
        StoredTraceInvocationKind::Mcp,
        "tool-root",
        vec![
            operation_span(
                "tool-root",
                Some("remote-parent"),
                "coral.mcp.call_tool",
                1,
                "alpha",
            ),
            remote_bridge,
            search_span("z-search", Some("bridge"), 10, "alpha"),
            search_span("a-search", Some("bridge"), 10, "alpha"),
            search_span("earlier-search", Some("bridge"), 9, "alpha"),
            search_span("sibling-search", Some("other-root"), 0, "alpha"),
        ],
    );

    assert_eq!(
        selected_search_execution(&trace, TraceListView::QueryStream, "alpha")
            .map(|span| span.span_id.as_str()),
        Some("earlier-search")
    );
}

#[test]
fn nearest_mcp_search_workspace_mismatch_does_not_fall_through() {
    let trace = search_trace(
        StoredTraceInvocationKind::Mcp,
        "tool-root",
        vec![
            operation_span("tool-root", None, "coral.mcp.call_tool", 1, "alpha"),
            search_span("nearest-wrong-workspace", Some("tool-root"), 2, "beta"),
            operation_span("bridge", Some("tool-root"), "grpc.client", 3, "alpha"),
            search_span("deeper-right-workspace", Some("bridge"), 4, "alpha"),
        ],
    );

    assert_eq!(
        selected_search_execution(&trace, TraceListView::QueryStream, "alpha"),
        None
    );
}

#[tokio::test]
async fn mcp_remote_parent_reads_only_the_selected_search_response() {
    let temp = TempDir::new().expect("temp dir");
    let db = open_test_db(&temp).await;
    ensure_workspace(&db).await;
    let reader = SearchResponseHistoryReader::new(Arc::clone(&db), Duration::from_hours(1));

    let mut remote_trace = search_trace(
        StoredTraceInvocationKind::Mcp,
        "tool-root",
        vec![
            operation_span(
                "tool-root",
                Some("remote-parent"),
                "coral.mcp.call_tool",
                1,
                "alpha",
            ),
            search_span("selected-search", Some("tool-root"), 2, "alpha"),
        ],
    );
    remote_trace.summary.trace_id = "remote-trace".to_string();
    insert_history_row(&db, "remote-trace", "selected-search", vec![1, 2, 3]).await;
    assert_eq!(
        reader
            .read(&remote_trace, TraceListView::QueryStream, Some("alpha"))
            .await,
        Some(RetainedSearchResponse::Response(vec![1, 2, 3]))
    );

    let mut no_fallback_trace = search_trace(
        StoredTraceInvocationKind::Mcp,
        "tool-root",
        vec![
            operation_span("tool-root", None, "coral.mcp.call_tool", 1, "alpha"),
            search_span("selected-without-row", Some("tool-root"), 2, "alpha"),
            operation_span("bridge", Some("tool-root"), "grpc.client", 3, "alpha"),
            search_span("deeper-with-row", Some("bridge"), 4, "alpha"),
        ],
    );
    no_fallback_trace.summary.trace_id = "no-fallback-trace".to_string();
    insert_history_row(&db, "no-fallback-trace", "deeper-with-row", vec![4, 5, 6]).await;
    assert_eq!(
        reader
            .read(
                &no_fallback_trace,
                TraceListView::QueryStream,
                Some("alpha"),
            )
            .await,
        None
    );
}

#[tokio::test]
async fn future_search_response_is_hidden_without_latching_read_failure() {
    let temp = TempDir::new().expect("temp dir");
    let trace_store = temp.path().join("trace-store");
    std::fs::create_dir_all(&trace_store).expect("trace store dir");
    let record = search_trace_record("future-search-trace", "alpha", 10, 20);
    std::fs::write(
        trace_store.join("spans-search.jsonl"),
        format!("{record}\n"),
    )
    .expect("write search trace");

    let db = open_test_db(&temp).await;
    ensure_workspace(&db).await;
    let two_minutes_nanos =
        i64::try_from(Duration::from_mins(2).as_nanos()).expect("duration fits i64");
    insert_history_row_at(
        &db,
        "future-search-trace",
        "future-search-trace-span",
        vec![7, 8, 9],
        now_unix_nanos_i64()
            .expect("current time")
            .saturating_add(two_minutes_nanos),
    )
    .await;
    let manager = TraceManager::new(trace_store, Duration::from_hours(3))
        .with_search_response_history(Arc::clone(&db), Duration::from_hours(1));

    let detail = manager
        .get_trace(GetTraceQuery {
            trace_id: "future-search-trace".to_string(),
            workspace: Some(WorkspaceName::parse("alpha").expect("workspace")),
            view: TraceListView::QueryStream,
        })
        .await
        .expect("future response does not hide trace detail");

    assert_eq!(detail.search_response, None);
    assert_eq!(detail.trace.spans.len(), 1);
    assert!(
        !manager
            .search_response_history
            .as_ref()
            .expect("history reader")
            .warnings
            .read_failure
            .load(Ordering::Relaxed)
    );
}

#[tokio::test]
async fn retained_response_is_read_only_for_scoped_query_stream_search() {
    let temp = TempDir::new().expect("temp dir");
    let trace_store = temp.path().join("trace-store");
    std::fs::create_dir_all(&trace_store).expect("trace store dir");
    let record = search_trace_record("search-trace", "alpha", 10, 20);
    let expired_record = search_trace_record("expired-search-trace", "alpha", 30, 40);
    let oversized_record = search_trace_record("oversized-search-trace", "alpha", 50, 60);
    std::fs::write(
        trace_store.join("spans-search.jsonl"),
        format!("{record}\n{expired_record}\n{oversized_record}\n"),
    )
    .expect("write search trace");

    let db = open_test_db(&temp).await;
    ensure_workspace(&db).await;
    insert_history_row(&db, "search-trace", "search-trace-span", vec![1, 2, 3]).await;
    let two_hours_nanos =
        i64::try_from(Duration::from_hours(2).as_nanos()).expect("duration fits i64");
    insert_history_row_at(
        &db,
        "expired-search-trace",
        "expired-search-trace-span",
        vec![4, 5, 6],
        now_unix_nanos_i64()
            .expect("current time")
            .saturating_sub(two_hours_nanos),
    )
    .await;
    insert_oversized_history_row(
        &db,
        "oversized-search-trace",
        "oversized-search-trace-span",
        1_048_577,
    )
    .await;
    let manager = TraceManager::new(trace_store, Duration::from_hours(3))
        .with_search_response_history(Arc::clone(&db), Duration::from_hours(1));

    let scoped = manager
        .get_trace(GetTraceQuery {
            trace_id: "search-trace".to_string(),
            workspace: Some(WorkspaceName::parse("alpha").expect("workspace")),
            view: TraceListView::QueryStream,
        })
        .await
        .expect("scoped Query Stream detail");
    assert_eq!(
        scoped.search_response,
        Some(RetainedSearchResponse::Response(vec![1, 2, 3]))
    );

    let expired = manager
        .get_trace(GetTraceQuery {
            trace_id: "expired-search-trace".to_string(),
            workspace: Some(WorkspaceName::parse("alpha").expect("workspace")),
            view: TraceListView::QueryStream,
        })
        .await
        .expect("expired response does not hide trace detail");
    assert_eq!(expired.search_response, None);
    assert_eq!(expired.trace.spans.len(), 1);

    let oversized = manager
        .get_trace(GetTraceQuery {
            trace_id: "oversized-search-trace".to_string(),
            workspace: Some(WorkspaceName::parse("alpha").expect("workspace")),
            view: TraceListView::QueryStream,
        })
        .await
        .expect("oversized response does not hide trace detail");
    assert_eq!(
        oversized.search_response,
        Some(RetainedSearchResponse::TooLarge)
    );
    assert_eq!(oversized.trace.spans.len(), 1);

    assert_history_absent_for_unscoped_views(&manager).await;

    db.drop_trace_search_responses_for_test()
        .await
        .expect("force response history read failure");
    let read_failure = manager
        .get_trace(GetTraceQuery {
            trace_id: "search-trace".to_string(),
            workspace: Some(WorkspaceName::parse("alpha").expect("workspace")),
            view: TraceListView::QueryStream,
        })
        .await
        .expect("response history read failure does not hide trace detail");
    assert_eq!(read_failure.search_response, None);
    assert_eq!(read_failure.trace.spans.len(), 1);
    assert!(
        manager
            .search_response_history
            .as_ref()
            .expect("history reader")
            .warnings
            .read_failure
            .load(Ordering::Relaxed)
    );
}

async fn assert_history_absent_for_unscoped_views(manager: &TraceManager) {
    for (view, workspace) in [
        (TraceListView::QueryStream, None),
        (
            TraceListView::All,
            Some(WorkspaceName::parse("alpha").expect("workspace")),
        ),
    ] {
        let detail = manager
            .get_trace(GetTraceQuery {
                trace_id: "search-trace".to_string(),
                workspace,
                view,
            })
            .await
            .expect("detail remains available");
        assert_eq!(detail.search_response, None);
        assert_eq!(detail.trace.spans.len(), 1);
    }
}

async fn open_test_db(temp: &TempDir) -> Arc<CoralDb> {
    let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
    let DatabaseConfig::Sqlite { path } = DatabaseConfig::load(&layout).expect("db config") else {
        panic!("default test database must be SQLite");
    };
    let db = Arc::new(
        CoralDb::open(ResolvedDatabaseConfig::Sqlite { path })
            .await
            .expect("open SQLite"),
    );
    db.migrate().await.expect("migrate SQLite");
    db
}

async fn ensure_workspace(db: &CoralDb) {
    let mut tx = db.begin().await.expect("workspace tx");
    tx.workspaces().ensure("alpha", 1).await.expect("workspace");
    tx.commit().await.expect("commit workspace");
}

async fn insert_history_row(
    db: &CoralDb,
    trace_id: &str,
    search_span_id: &str,
    response_proto: Vec<u8>,
) {
    insert_history_row_at(
        db,
        trace_id,
        search_span_id,
        response_proto,
        now_unix_nanos_i64().expect("current time"),
    )
    .await;
}

async fn insert_history_row_at(
    db: &CoralDb,
    trace_id: &str,
    search_span_id: &str,
    response_proto: Vec<u8>,
    recorded_at_unix_nanos: i64,
) {
    assert_eq!(
        db.insert_trace_search_response(TraceSearchResponseCapture {
            workspace_id: "alpha".to_string(),
            trace_id: trace_id.to_string(),
            search_span_id: search_span_id.to_string(),
            recorded_at_unix_nanos,
            outcome: TraceSearchResponseOutcome::Response(response_proto),
        })
        .await
        .expect("insert Search response history"),
        TraceSearchResponseInsertResult::Inserted
    );
}

async fn insert_oversized_history_row(
    db: &CoralDb,
    trace_id: &str,
    search_span_id: &str,
    oversized_bytes: i64,
) {
    assert_eq!(
        db.insert_trace_search_response(TraceSearchResponseCapture {
            workspace_id: "alpha".to_string(),
            trace_id: trace_id.to_string(),
            search_span_id: search_span_id.to_string(),
            recorded_at_unix_nanos: now_unix_nanos_i64().expect("current time"),
            outcome: TraceSearchResponseOutcome::TooLarge {
                bytes: oversized_bytes,
            },
        })
        .await
        .expect("insert oversized Search response history"),
        TraceSearchResponseInsertResult::Inserted
    );
}

fn search_trace(
    invocation_kind: StoredTraceInvocationKind,
    root_span_id: &str,
    spans: Vec<TraceSpanRecord>,
) -> TraceDetailRecord {
    TraceDetailRecord {
        summary: TraceSummaryRecord {
            trace_id: "trace".to_string(),
            root_span_id: root_span_id.to_string(),
            name: "Search".to_string(),
            query: "repository search".to_string(),
            status: StoredTraceStatus::Ok,
            start_time_unix_nanos: 1,
            end_time_unix_nanos: 20,
            duration_nanos: 19,
            span_count: u32::try_from(spans.len()).expect("span count"),
            row_count: 0,
            row_count_recorded: false,
            operation_kind: StoredTraceOperationKind::Search,
            operation_name: "search".to_string(),
            invocation_kind,
        },
        spans,
    }
}

fn search_span(
    span_id: &str,
    parent_span_id: Option<&str>,
    start_time_unix_nanos: i64,
    workspace: &str,
) -> TraceSpanRecord {
    operation_span(
        span_id,
        parent_span_id,
        "coral.search",
        start_time_unix_nanos,
        workspace,
    )
}

fn operation_span(
    span_id: &str,
    parent_span_id: Option<&str>,
    name: &str,
    start_time_unix_nanos: i64,
    workspace: &str,
) -> TraceSpanRecord {
    TraceSpanRecord {
        trace_id: "trace".to_string(),
        span_id: span_id.to_string(),
        parent_span_id: parent_span_id.map(str::to_string),
        parent_span_is_remote: parent_span_id == Some("remote-parent"),
        name: name.to_string(),
        kind: "internal".to_string(),
        status: StoredTraceStatus::Ok,
        status_message: None,
        start_time_unix_nanos,
        end_time_unix_nanos: start_time_unix_nanos + 1,
        duration_nanos: 1,
        attributes_json: json!({ "workspace": workspace }).to_string(),
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

fn search_trace_record(
    trace_id: &str,
    workspace: &str,
    start_time_unix_nanos: i64,
    end_time_unix_nanos: i64,
) -> serde_json::Value {
    json!({
        "trace_id": trace_id,
        "span_id": format!("{trace_id}-span"),
        "parent_span_id": null,
        "parent_span_is_remote": false,
        "name": "coral.search",
        "kind": "internal",
        "status": "ok",
        "status_message": null,
        "start_time_unix_nanos": start_time_unix_nanos,
        "end_time_unix_nanos": end_time_unix_nanos,
        "duration_nanos": end_time_unix_nanos - start_time_unix_nanos,
        "attributes_json": json!({
            "coral.stream.entry": true,
            "coral.stream.kind": "search",
            "coral.stream.name": "search",
            "workspace": workspace,
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
