//! LIST storage scanning, pagination, and projector lifecycle tests.

use std::time::{Duration, SystemTime};

use serde_json::json;

use super::super::{StoredTraceStatus, TraceListSpanRecord, unix_nanos};
use super::test_support::{TraceFiles, span};
use super::{QueryStreamProjector, query_stream_root_is_visible};

#[test]
fn query_stream_search_detail_stops_before_unrelated_older_files() {
    let files = TraceFiles::new();
    let base_time = SystemTime::now() - Duration::from_secs(20);
    let root_start = base_time + Duration::from_secs(5);
    let root_end = base_time + Duration::from_secs(8);

    let root = span("bounded-detail-trace", "selected-tool")
        .named("coral.mcp.call_tool")
        .remote_root()
        .entry("tool", "search", "alpha")
        .attrs(json!({
            "mcp.method": "tools/call",
            "mcp.tool.name": "search",
        }))
        .times(unix_nanos(root_start), unix_nanos(root_end))
        .build();
    let child = span("bounded-detail-trace", "selected-search")
        .named("coral.search")
        .child_of(&root)
        .entry("search", "search", "alpha")
        .attrs(json!({"coral.local.search.query": "bounded search phrase"}))
        .times(
            unix_nanos(root_start + Duration::from_secs(1)),
            unix_nanos(root_start + Duration::from_secs(2)),
        )
        .build();
    files.write_records_at(&[child, root], root_end);

    // This trace-shaped directory fails if opened as JSONL. Its conservative
    // end bound is before the selected root, so detail must not touch it.
    files.write_directory_at("spans-unrelated-old.jsonl", base_time);

    let detail = files
        .get("bounded-detail-trace", "selected-tool", Some("alpha"))
        .expect("get bounded detail without opening unrelated older file");
    assert_eq!(detail.summary.root_span_id, "selected-tool");
    assert_eq!(detail.summary.query, "bounded search phrase");
    assert!(!detail.summary.row_count_recorded);
    assert_eq!(detail.spans.len(), 2);
}

#[test]
fn query_stream_remote_parent_visibility_stops_before_unrelated_newer_files() {
    let files = TraceFiles::new();
    let base_time = SystemTime::now() - Duration::from_secs(20);
    let root_end = base_time + Duration::from_secs(5);
    let newer_time = base_time + Duration::from_secs(10);

    let root = span("forward-bounded-trace", "selected-tool")
        .named("coral.mcp.call_tool")
        .remote_root()
        .entry("tool", "sql", "alpha")
        .attrs(json!({
            "mcp.method": "tools/call",
            "mcp.tool.name": "sql",
        }))
        .times(1, unix_nanos(root_end))
        .build();
    files.write_at(&root, root_end);

    // Root discovery has already selected `root`; its remote parent must stop
    // the forward ancestry scan before this unreadable trace-shaped directory.
    files.write_directory_at("spans-unrelated-new.jsonl", newer_time);

    let store = files.store();
    let trace_files = store
        .jsonl_files_by_modified()
        .expect("list trace store files");
    let root_path = files.timestamped_path(root_end);
    let root_file_index = trace_files
        .iter()
        .position(|file| file.path == root_path)
        .expect("root file index");
    assert!(
        query_stream_root_is_visible(&trace_files, root_file_index, &root)
            .expect("check remote-parent visibility")
    );
}

#[test]
fn query_stream_list_and_detail_resolve_equal_mtime_files_as_one_bucket() {
    let files = TraceFiles::new();
    let base_time = SystemTime::now() - Duration::from_secs(10);
    let common_modified = base_time + Duration::from_secs(1);

    let parent = span("equal-mtime-trace", "local-parent")
        .named("http.request")
        .times(
            unix_nanos(base_time),
            unix_nanos(base_time + Duration::from_millis(900)),
        )
        .build();
    let root = span("equal-mtime-trace", "selected-tool")
        .named("coral.mcp.call_tool")
        .child_of(&parent)
        .entry("tool", "sql", "alpha")
        .attrs(json!({
            "mcp.method": "tools/call",
            "mcp.tool.name": "sql",
        }))
        .times(
            unix_nanos(base_time + Duration::from_millis(100)),
            unix_nanos(base_time + Duration::from_millis(800)),
        )
        .build();
    let child = span("equal-mtime-trace", "nested-query")
        .child_of(&root)
        .entry("query", "sql", "alpha")
        .attrs(json!({"sql": "SELECT 42", "row_count": 1}))
        .times(
            unix_nanos(base_time + Duration::from_millis(200)),
            unix_nanos(base_time + Duration::from_millis(700)),
        )
        .build();

    for (name, span) in [
        ("spans-a-parent.jsonl", &parent),
        ("spans-b-root.jsonl", &root),
        ("spans-c-child.jsonl", &child),
    ] {
        files.write_named_at(name, span, common_modified);
    }

    let summaries = files.list_with_detail(10, 0, Some("alpha"));
    assert_eq!(summaries.len(), 1);
    let summary = summaries.first().expect("equal-mtime summary");
    assert_eq!(summary.root_span_id, "selected-tool");
    assert_eq!(summary.query, "SELECT 42");
    assert_eq!(summary.row_count, 1);
    assert!(summary.row_count_recorded);
    assert_eq!(summary.span_count, 2);

    let detail = files
        .get("equal-mtime-trace", "selected-tool", Some("alpha"))
        .expect("get equal-mtime operation detail");
    assert_eq!(detail.summary, *summary);
    assert_eq!(
        detail
            .spans
            .iter()
            .map(|span| span.span_id.as_str())
            .collect::<Vec<_>>(),
        vec!["selected-tool", "nested-query"]
    );
}

#[test]
fn query_stream_filters_workspace_before_pagination() {
    let files = TraceFiles::new();
    let mut records = Vec::new();
    for (span_id, workspace, end_time) in [
        ("alpha-new", "alpha", 30),
        ("beta", "beta", 20),
        ("alpha-old", "alpha", 10),
    ] {
        records.push(
            span("pagination-trace", span_id)
                .remote_root()
                .entry("query", "sql", workspace)
                .attrs(json!({"sql": format!("SELECT '{span_id}'")}))
                .times(end_time - 1, end_time)
                .build(),
        );
    }
    files.write("spans-pagination.jsonl", &records);

    let first = files.list(1, 0, Some("alpha"));
    let second = files.list(1, 1, Some("alpha"));
    assert_eq!(
        first.first().expect("first entry").root_span_id,
        "alpha-new"
    );
    assert_eq!(
        second.first().expect("second entry").root_span_id,
        "alpha-old"
    );
}

#[test]
fn query_stream_stops_after_a_complete_recent_page() {
    let files = TraceFiles::new();
    let now = SystemTime::now();
    let recent_time = now - Duration::from_secs(1);
    let old_time = now - Duration::from_hours(2);

    let recent = span("recent-trace", "recent-entry")
        .entry("query", "sql", "alpha")
        .attrs(json!({"sql": "SELECT 'recent'"}))
        .times(
            unix_nanos(recent_time),
            unix_nanos(recent_time + Duration::from_millis(1)),
        )
        .build();
    files.write_at(&recent, recent_time);
    files.write_invalid_at(old_time);

    let summaries = files.list(1, 0, Some("alpha"));
    assert_eq!(summaries.len(), 1);
    assert_eq!(
        summaries.first().expect("recent summary").root_span_id,
        "recent-entry"
    );
}

#[test]
fn query_stream_offset_page_reads_enough_recent_files_then_stops() {
    let files = TraceFiles::new();
    let now = SystemTime::now();
    let newest_time = now - Duration::from_secs(1);
    let second_time = now - Duration::from_secs(5);
    let unreadable_time = now - Duration::from_hours(2);

    let newest = span("newest-trace", "newest-entry")
        .entry("query", "sql", "alpha")
        .attrs(json!({"sql": "SELECT 'newest'"}))
        .times(
            unix_nanos(newest_time),
            unix_nanos(newest_time + Duration::from_millis(1)),
        )
        .build();
    files.write_at(&newest, newest_time);

    let second = span("second-trace", "second-entry")
        .entry("query", "sql", "alpha")
        .attrs(json!({"sql": "SELECT 'second'"}))
        .times(
            unix_nanos(second_time),
            unix_nanos(second_time + Duration::from_millis(1)),
        )
        .build();
    files.write_at(&second, second_time);
    files.write_invalid_at(unreadable_time);

    let summaries = files.list_with_detail(1, 1, Some("alpha"));
    assert_eq!(summaries.len(), 1);
    assert_eq!(
        summaries.first().expect("offset summary").root_span_id,
        "second-entry"
    );
}

#[test]
fn query_stream_completes_returned_operations_from_older_files() {
    let files = TraceFiles::new();
    let operation_start = SystemTime::now() - Duration::from_secs(20);
    let child_end = operation_start + Duration::from_secs(2);
    let operation_end = operation_start + Duration::from_secs(10);

    let root = span("split-trace", "tool-root")
        .named("coral.mcp.call_tool")
        .remote_root()
        .entry("tool", "sql", "alpha")
        .attrs(json!({
            "mcp.method": "tools/call",
            "mcp.tool.name": "sql",
        }))
        .times(unix_nanos(operation_start), unix_nanos(operation_end))
        .build();

    let child = span("split-trace", "query-child")
        .child_of(&root)
        .entry("query", "sql", "alpha")
        .attrs(json!({"sql": "SELECT 42", "row_count": 1}))
        .times(
            unix_nanos(operation_start + Duration::from_secs(1)),
            unix_nanos(child_end),
        )
        .build();
    files.write_at(&child, child_end);
    files.write_at(&root, operation_end);

    let summaries = files.list_with_detail(1, 0, Some("alpha"));
    assert_eq!(summaries.len(), 1);
    let summary = summaries.first().expect("completed operation summary");
    assert_eq!(summary.root_span_id, "tool-root");
    assert_eq!(summary.query, "SELECT 42");
    assert_eq!(summary.row_count, 1);
    assert!(summary.row_count_recorded);
    assert_eq!(summary.span_count, 2);
}

#[test]
fn query_stream_projector_releases_completed_discovery_trees() {
    let mut projector = QueryStreamProjector::new(1, Some("alpha"));

    for index in (0..128_i64).rev() {
        let base = index * 10;
        let protocol_span_id = format!("protocol-{index}");
        let protocol = TraceListSpanRecord {
            trace_id: "shared-discovery-trace".to_string(),
            span_id: protocol_span_id.clone(),
            parent_span_id: Some("remote-parent".to_string()),
            parent_span_is_remote: true,
            name: "coral.mcp.list_tools".to_string(),
            status: StoredTraceStatus::Ok,
            start_time_unix_nanos: base,
            end_time_unix_nanos: base + 4,
            attributes_json: r#"{"mcp.method":"tools/list"}"#.to_string(),
        };
        let child = TraceListSpanRecord {
            trace_id: "shared-discovery-trace".to_string(),
            span_id: format!("discovery-query-{index}"),
            parent_span_id: Some(protocol_span_id),
            parent_span_is_remote: false,
            name: "coral.query".to_string(),
            status: StoredTraceStatus::Ok,
            start_time_unix_nanos: base + 1,
            end_time_unix_nanos: base + 2,
            attributes_json: json!({
                "coral.stream.entry": true,
                "coral.stream.kind": "query",
                "coral.stream.name": "list_catalog",
                "workspace": "alpha",
                "sql": "LIST CATALOG",
            })
            .to_string(),
        };

        projector.record_file(vec![child, protocol]);
        projector.advance_watermark(base - 1);
        assert!(projector.nodes.is_empty());
        assert!(projector.aggregates.is_empty());
        assert!(projector.finalized.is_empty());
    }
}

#[test]
fn query_stream_keeps_newer_duplicate_span_across_files() {
    let files = TraceFiles::new();
    let base_time = SystemTime::now() - Duration::from_secs(10);

    let older = span("duplicate-query-stream", "duplicate-entry")
        .entry("query", "sql", "alpha")
        .attrs(json!({"sql": "SELECT 'old'"}))
        .times(
            unix_nanos(base_time + Duration::from_millis(500)),
            unix_nanos(base_time + Duration::from_millis(600)),
        )
        .build();
    files.write_at(&older, base_time);

    let newer_modified = base_time + Duration::from_millis(10);
    let newer = span("duplicate-query-stream", "duplicate-entry")
        .entry("query", "sql", "alpha")
        .attrs(json!({"sql": "SELECT 'new'"}))
        .times(
            unix_nanos(base_time + Duration::from_millis(500)),
            unix_nanos(base_time + Duration::from_millis(700)),
        )
        .build();
    files.write_at(&newer, newer_modified);

    let summaries = files.list_with_detail(1, 0, Some("alpha"));
    assert_eq!(summaries.len(), 1);
    assert_eq!(
        summaries.first().expect("newer duplicate summary").query,
        "SELECT 'new'"
    );
}

#[test]
fn query_stream_keeps_scanning_when_file_mtime_is_coarse() {
    let files = TraceFiles::new();
    let base_time = SystemTime::now() - Duration::from_secs(10);
    let hidden_modified = base_time;
    let visible_modified = base_time + Duration::from_millis(10);

    let hidden_newer = span("hidden-newer", "hidden-newer-entry")
        .entry("query", "sql", "alpha")
        .attrs(json!({"sql": "SELECT 'newer'"}))
        .times(
            unix_nanos(base_time),
            unix_nanos(base_time + Duration::from_millis(900)),
        )
        .build();
    files.write_at(&hidden_newer, hidden_modified);

    let visible_older = span("visible-older", "visible-older-entry")
        .entry("query", "sql", "alpha")
        .attrs(json!({"sql": "SELECT 'older'"}))
        .times(
            unix_nanos(base_time),
            unix_nanos(base_time + Duration::from_millis(100)),
        )
        .build();
    files.write_at(&visible_older, visible_modified);

    let summaries = files.list(1, 0, Some("alpha"));
    assert_eq!(summaries.len(), 1);
    assert_eq!(
        summaries.first().expect("newest summary").trace_id,
        "hidden-newer"
    );
}
