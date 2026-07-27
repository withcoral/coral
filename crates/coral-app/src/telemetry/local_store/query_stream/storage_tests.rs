//! LIST storage scanning, pagination, and projector lifecycle tests.

use std::fs;
use std::time::{Duration, SystemTime};

use serde_json::json;
use tempfile::TempDir;

use super::super::tests::{
    set_modified_time, timestamped_jsonl_path, trace_record, write_record_file,
    write_record_file_lines,
};
use super::super::{StoredTraceStatus, TraceListSpanRecord, TraceStore, unix_nanos};
use super::QueryStreamProjector;

#[test]
fn query_stream_resolves_equal_mtime_files_as_one_bucket() {
    let temp = TempDir::new().expect("temp dir");
    let dir = temp.path().join("telemetry").join("traces");
    fs::create_dir_all(&dir).expect("trace dir");
    let base_time = SystemTime::now() - Duration::from_secs(10);
    let common_modified = base_time + Duration::from_secs(1);

    let mut parent = trace_record("equal-mtime-trace", "local-parent");
    parent.name = "http.request".to_string();
    parent.start_time_unix_nanos = unix_nanos(base_time);
    parent.end_time_unix_nanos = unix_nanos(base_time + Duration::from_millis(900));

    let mut root = trace_record("equal-mtime-trace", "selected-tool");
    root.parent_span_id = Some(parent.span_id.clone());
    root.name = "coral.mcp.call_tool".to_string();
    root.attributes_json = json!({
        "coral.stream.entry": true,
        "coral.stream.kind": "tool",
        "coral.stream.name": "sql",
        "mcp.method": "tools/call",
        "mcp.tool.name": "sql",
        "workspace": "alpha",
    })
    .to_string();
    root.start_time_unix_nanos = unix_nanos(base_time + Duration::from_millis(100));
    root.end_time_unix_nanos = unix_nanos(base_time + Duration::from_millis(800));

    let mut child = trace_record("equal-mtime-trace", "nested-query");
    child.parent_span_id = Some(root.span_id.clone());
    child.attributes_json = json!({
        "coral.stream.entry": true,
        "coral.stream.kind": "query",
        "coral.stream.name": "sql",
        "workspace": "alpha",
        "sql": "SELECT 42",
        "row_count": 1,
    })
    .to_string();
    child.start_time_unix_nanos = unix_nanos(base_time + Duration::from_millis(200));
    child.end_time_unix_nanos = unix_nanos(base_time + Duration::from_millis(700));

    for (name, span) in [
        ("spans-a-parent.jsonl", &parent),
        ("spans-b-root.jsonl", &root),
        ("spans-c-child.jsonl", &child),
    ] {
        let path = dir.join(name);
        write_record_file(&path, span);
        set_modified_time(&path, common_modified);
    }

    let summaries = TraceStore::new(dir)
        .list_query_stream_sync(10, 0, Some("alpha"))
        .expect("list equal-mtime operation");
    assert_eq!(summaries.len(), 1);
    let summary = summaries.first().expect("equal-mtime summary");
    assert_eq!(summary.root_span_id, "selected-tool");
    assert_eq!(summary.query, "SELECT 42");
    assert_eq!(summary.row_count, 1);
    assert!(summary.row_count_recorded);
    assert_eq!(summary.span_count, 2);
}

#[test]
fn query_stream_filters_workspace_before_pagination() {
    let temp = TempDir::new().expect("temp dir");
    let dir = temp.path().join("telemetry").join("traces");
    fs::create_dir_all(&dir).expect("trace dir");

    let mut records = Vec::new();
    for (span_id, workspace, end_time) in [
        ("alpha-new", "alpha", 30),
        ("beta", "beta", 20),
        ("alpha-old", "alpha", 10),
    ] {
        let mut record = trace_record("pagination-trace", span_id);
        record.parent_span_id = Some("remote-parent".to_string());
        record.parent_span_is_remote = true;
        record.attributes_json = json!({
            "coral.stream.entry": true,
            "coral.stream.kind": "query",
            "coral.stream.name": "sql",
            "workspace": workspace,
            "sql": format!("SELECT '{span_id}'"),
        })
        .to_string();
        record.start_time_unix_nanos = end_time - 1;
        record.end_time_unix_nanos = end_time;
        records.push(record);
    }
    write_record_file_lines(&dir.join("spans-pagination.jsonl"), &records);

    let store = TraceStore::new(dir);
    let first = store
        .list_query_stream_sync(1, 0, Some("alpha"))
        .expect("first page");
    let second = store
        .list_query_stream_sync(1, 1, Some("alpha"))
        .expect("second page");
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
    let temp = TempDir::new().expect("temp dir");
    let dir = temp.path().join("telemetry").join("traces");
    fs::create_dir_all(&dir).expect("trace dir");
    let now = SystemTime::now();
    let recent_time = now - Duration::from_secs(1);
    let old_time = now - Duration::from_hours(2);

    let mut recent = trace_record("recent-trace", "recent-entry");
    recent.attributes_json = json!({
        "coral.stream.entry": true,
        "coral.stream.kind": "query",
        "coral.stream.name": "sql",
        "workspace": "alpha",
        "sql": "SELECT 'recent'",
    })
    .to_string();
    recent.start_time_unix_nanos = unix_nanos(recent_time);
    recent.end_time_unix_nanos = unix_nanos(recent_time + Duration::from_millis(1));
    let recent_path = dir.join(timestamped_jsonl_path(recent_time));
    write_record_file(&recent_path, &recent);
    set_modified_time(&recent_path, recent_time);

    let old_path = dir.join(timestamped_jsonl_path(old_time));
    fs::write(&old_path, [0xff]).expect("write unreadable old JSONL text");
    set_modified_time(&old_path, old_time);

    let summaries = TraceStore::new(dir)
        .list_query_stream_sync(1, 0, Some("alpha"))
        .expect("recent page does not read old file");
    assert_eq!(summaries.len(), 1);
    assert_eq!(
        summaries.first().expect("recent summary").root_span_id,
        "recent-entry"
    );
}

#[test]
fn query_stream_offset_page_reads_enough_recent_files_then_stops() {
    let temp = TempDir::new().expect("temp dir");
    let dir = temp.path().join("telemetry").join("traces");
    fs::create_dir_all(&dir).expect("trace dir");
    let now = SystemTime::now();
    let newest_time = now - Duration::from_secs(1);
    let second_time = now - Duration::from_secs(5);
    let unreadable_time = now - Duration::from_hours(2);

    let mut newest = trace_record("newest-trace", "newest-entry");
    newest.attributes_json = json!({
        "coral.stream.entry": true,
        "coral.stream.kind": "query",
        "coral.stream.name": "sql",
        "workspace": "alpha",
        "sql": "SELECT 'newest'",
    })
    .to_string();
    newest.start_time_unix_nanos = unix_nanos(newest_time);
    newest.end_time_unix_nanos = unix_nanos(newest_time + Duration::from_millis(1));
    let newest_path = dir.join(timestamped_jsonl_path(newest_time));
    write_record_file(&newest_path, &newest);
    set_modified_time(&newest_path, newest_time);

    let mut second = trace_record("second-trace", "second-entry");
    second.attributes_json = json!({
        "coral.stream.entry": true,
        "coral.stream.kind": "query",
        "coral.stream.name": "sql",
        "workspace": "alpha",
        "sql": "SELECT 'second'",
    })
    .to_string();
    second.start_time_unix_nanos = unix_nanos(second_time);
    second.end_time_unix_nanos = unix_nanos(second_time + Duration::from_millis(1));
    let second_path = dir.join(timestamped_jsonl_path(second_time));
    write_record_file(&second_path, &second);
    set_modified_time(&second_path, second_time);

    let unreadable_path = dir.join(timestamped_jsonl_path(unreadable_time));
    fs::write(&unreadable_path, [0xff]).expect("write unreadable old JSONL text");
    set_modified_time(&unreadable_path, unreadable_time);

    let store = TraceStore::new(dir);
    let summaries = store
        .list_query_stream_sync(1, 1, Some("alpha"))
        .expect("list query stream");
    assert_eq!(summaries.len(), 1);
    assert_eq!(
        summaries.first().expect("offset summary").root_span_id,
        "second-entry"
    );
}

#[test]
fn query_stream_completes_returned_operations_from_older_files() {
    let temp = TempDir::new().expect("temp dir");
    let dir = temp.path().join("telemetry").join("traces");
    fs::create_dir_all(&dir).expect("trace dir");
    let operation_start = SystemTime::now() - Duration::from_secs(20);
    let child_end = operation_start + Duration::from_secs(2);
    let operation_end = operation_start + Duration::from_secs(10);

    let mut child = trace_record("split-trace", "query-child");
    child.parent_span_id = Some("tool-root".to_string());
    child.attributes_json = json!({
        "coral.stream.entry": true,
        "coral.stream.kind": "query",
        "coral.stream.name": "sql",
        "workspace": "alpha",
        "sql": "SELECT 42",
        "row_count": 1,
    })
    .to_string();
    child.start_time_unix_nanos = unix_nanos(operation_start + Duration::from_secs(1));
    child.end_time_unix_nanos = unix_nanos(child_end);
    let child_path = dir.join(timestamped_jsonl_path(child_end));
    write_record_file(&child_path, &child);
    set_modified_time(&child_path, child_end);

    let mut root = trace_record("split-trace", "tool-root");
    root.parent_span_id = Some("remote-parent".to_string());
    root.parent_span_is_remote = true;
    root.name = "coral.mcp.call_tool".to_string();
    root.attributes_json = json!({
        "coral.stream.entry": true,
        "coral.stream.kind": "tool",
        "coral.stream.name": "sql",
        "mcp.method": "tools/call",
        "mcp.tool.name": "sql",
        "workspace": "alpha",
    })
    .to_string();
    root.start_time_unix_nanos = unix_nanos(operation_start);
    root.end_time_unix_nanos = unix_nanos(operation_end);
    let root_path = dir.join(timestamped_jsonl_path(operation_end));
    write_record_file(&root_path, &root);
    set_modified_time(&root_path, operation_end);

    let store = TraceStore::new(dir);
    let summaries = store
        .list_query_stream_sync(1, 0, Some("alpha"))
        .expect("list query stream");
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
    let temp = TempDir::new().expect("temp dir");
    let dir = temp.path().join("telemetry").join("traces");
    fs::create_dir_all(&dir).expect("trace dir");
    let base_time = SystemTime::now() - Duration::from_secs(10);

    let mut older = trace_record("duplicate-query-stream", "duplicate-entry");
    older.attributes_json = json!({
        "coral.stream.entry": true,
        "coral.stream.kind": "query",
        "coral.stream.name": "sql",
        "workspace": "alpha",
        "sql": "SELECT 'old'",
    })
    .to_string();
    older.start_time_unix_nanos = unix_nanos(base_time + Duration::from_millis(500));
    older.end_time_unix_nanos = unix_nanos(base_time + Duration::from_millis(600));
    let older_path = dir.join(timestamped_jsonl_path(base_time));
    write_record_file(&older_path, &older);
    set_modified_time(&older_path, base_time);

    let mut newer = older;
    newer.attributes_json = json!({
        "coral.stream.entry": true,
        "coral.stream.kind": "query",
        "coral.stream.name": "sql",
        "workspace": "alpha",
        "sql": "SELECT 'new'",
    })
    .to_string();
    newer.end_time_unix_nanos = unix_nanos(base_time + Duration::from_millis(700));
    let newer_modified = base_time + Duration::from_millis(10);
    let newer_path = dir.join(timestamped_jsonl_path(newer_modified));
    write_record_file(&newer_path, &newer);
    set_modified_time(&newer_path, newer_modified);

    let store = TraceStore::new(dir);
    let summaries = store
        .list_query_stream_sync(1, 0, Some("alpha"))
        .expect("list query stream");
    assert_eq!(summaries.len(), 1);
    assert_eq!(
        summaries.first().expect("newer duplicate summary").query,
        "SELECT 'new'"
    );
}

#[test]
fn query_stream_keeps_scanning_when_file_mtime_is_coarse() {
    let temp = TempDir::new().expect("temp dir");
    let dir = temp.path().join("telemetry").join("traces");
    fs::create_dir_all(&dir).expect("trace dir");
    let base_time = SystemTime::now() - Duration::from_secs(10);
    let hidden_modified = base_time;
    let visible_modified = base_time + Duration::from_millis(10);

    let mut hidden_newer = trace_record("hidden-newer", "hidden-newer-entry");
    hidden_newer.attributes_json = json!({
        "coral.stream.entry": true,
        "coral.stream.kind": "query",
        "coral.stream.name": "sql",
        "workspace": "alpha",
        "sql": "SELECT 'newer'",
    })
    .to_string();
    hidden_newer.start_time_unix_nanos = unix_nanos(base_time);
    hidden_newer.end_time_unix_nanos = unix_nanos(base_time + Duration::from_millis(900));
    let hidden_path = dir.join(timestamped_jsonl_path(hidden_modified));
    write_record_file(&hidden_path, &hidden_newer);
    set_modified_time(&hidden_path, hidden_modified);

    let mut visible_older = trace_record("visible-older", "visible-older-entry");
    visible_older.attributes_json = json!({
        "coral.stream.entry": true,
        "coral.stream.kind": "query",
        "coral.stream.name": "sql",
        "workspace": "alpha",
        "sql": "SELECT 'older'",
    })
    .to_string();
    visible_older.start_time_unix_nanos = unix_nanos(base_time);
    visible_older.end_time_unix_nanos = unix_nanos(base_time + Duration::from_millis(100));
    let visible_path = dir.join(timestamped_jsonl_path(visible_modified));
    write_record_file(&visible_path, &visible_older);
    set_modified_time(&visible_path, visible_modified);

    let summaries = TraceStore::new(dir)
        .list_query_stream_sync(1, 0, Some("alpha"))
        .expect("list query stream");
    assert_eq!(summaries.len(), 1);
    assert_eq!(
        summaries.first().expect("newest summary").trace_id,
        "hidden-newer"
    );
}
