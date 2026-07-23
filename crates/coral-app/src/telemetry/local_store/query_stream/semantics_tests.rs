use std::collections::HashSet;

use serde_json::json;

use super::super::tests::trace_record;
use super::super::{StoredTraceOperationKind, StoredTraceStatus, TraceStoreError};
use super::classification::{
    MAX_LEGACY_TOOL_OPERATION_NAME_LEN, UNKNOWN_TOOL_OPERATION_NAME,
    privacy_safe_legacy_tool_operation_name, query_stream_summaries,
};
use super::test_support::{TraceFiles, project, project_page, span};

// Shared classification and ownership behavior.

#[test]
fn query_stream_projects_outer_operations_and_selects_details_by_span() {
    let sql_tool = span("shared-trace", "sql-tool")
        .named("coral.mcp.call_tool")
        .remote_root()
        .entry("tool", "sql", "alpha")
        .attrs(json!({
            "mcp.method": "tools/call",
            "mcp.tool.name": "sql",
            "status": "error",
        }))
        .status(StoredTraceStatus::Error)
        .times(10, 40)
        .build();
    let nested_query = span("shared-trace", "nested-query")
        .child_of(&sql_tool)
        .entry("query", "sql", "alpha")
        .attrs(json!({"sql": "SELECT 42", "row_count": 7, "status": "ok"}))
        .times(20, 30)
        .build();

    let files = TraceFiles::with_records(&[sql_tool, nested_query]);
    let summaries = files.list_with_detail(10, 0, None);
    assert_eq!(summaries.len(), 1);
    let sql_summary = summaries.first().expect("SQL summary");
    assert_eq!(sql_summary.root_span_id, "sql-tool");
    assert_eq!(sql_summary.operation_kind, StoredTraceOperationKind::Tool);
    assert_eq!(sql_summary.operation_name, "sql");
    assert_eq!(sql_summary.query, "SELECT 42");
    assert_eq!(sql_summary.row_count, 7);
    assert!(sql_summary.row_count_recorded);
    assert_eq!(sql_summary.status, StoredTraceStatus::Error);
    assert_eq!(sql_summary.start_time_unix_nanos, 10);
    assert_eq!(sql_summary.end_time_unix_nanos, 40);
    assert_eq!(sql_summary.duration_nanos, 30);
    assert_eq!(sql_summary.span_count, 2);

    let detail = files
        .get("shared-trace", "sql-tool", Some("alpha"))
        .expect("get selected operation");
    assert_eq!(detail.summary, *sql_summary);
    assert_eq!(detail.spans.len(), 2);
    assert!(
        files.get("shared-trace", "nested-query", None).is_err(),
        "a collapsed nested entry is not independently addressable"
    );
    assert!(
        files.get("shared-trace", "sql-tool", Some("beta")).is_err(),
        "workspace filtering applies to the selected operation"
    );
}

#[test]
fn query_stream_projects_search_text_for_tool_operation() {
    let tool = span("search-trace", "search-tool")
        .named("coral.mcp.call_tool")
        .remote_root()
        .entry("tool", "search", "beta")
        .attrs(json!({
            "mcp.method": "tools/call",
            "mcp.tool.name": "search",
        }))
        .times(1, 30)
        .build();
    let search = span("search-trace", "search-operation")
        .named("coral.search")
        .child_of(&tool)
        .entry("search", "search", "beta")
        .attrs(json!({"coral.local.search.query": "find customer churn"}))
        .times(10, 20)
        .build();
    let internal_query = span("search-trace", "search-catalog-query")
        .child_of(&search)
        .entry("query", "list_catalog", "beta")
        .attrs(json!({"sql": "LIST CATALOG", "row_count": 99}))
        .times(12, 15)
        .build();

    let files = TraceFiles::with_records(&[tool, search, internal_query]);
    let summaries = files.list_with_detail(10, 0, Some("beta"));
    let summary = summaries.first().expect("tool search summary");
    assert_eq!(summaries.len(), 1);
    assert_eq!(summary.root_span_id, "search-tool");
    assert_eq!(summary.operation_kind, StoredTraceOperationKind::Tool);
    assert_eq!(summary.operation_name, "search");
    assert_eq!(summary.query, "find customer churn");
    assert!(!summary.row_count_recorded);
}

#[test]
fn query_stream_projects_search_text_for_direct_operation() {
    let search = span("direct-search-trace", "direct-search")
        .named("coral.search")
        .remote_root()
        .entry("search", "search", "gamma")
        .attrs(json!({"coral.local.search.query": "direct search phrase"}))
        .build();

    let files = TraceFiles::with_records(&[search]);
    let summaries = files.list_with_detail(10, 0, Some("gamma"));
    let summary = summaries.first().expect("direct search summary");
    assert_eq!(summaries.len(), 1);
    assert_eq!(summary.root_span_id, "direct-search");
    assert_eq!(summary.operation_kind, StoredTraceOperationKind::Search);
    assert_eq!(summary.query, "direct search phrase");
}

#[test]
fn query_stream_entry_false_overrides_legacy_tool_shape() {
    let mut suppressed = trace_record("suppressed-trace", "suppressed-tool");
    suppressed.parent_span_id = Some("remote-parent".to_string());
    suppressed.parent_span_is_remote = true;
    suppressed.name = "coral.mcp.call_tool".to_string();
    suppressed.attributes_json = json!({
        "coral.stream.entry": false,
        "mcp.method": "tools/call",
        "mcp.tool.name": "list_catalog",
        "workspace": "alpha",
    })
    .to_string();

    let summaries = project(&[suppressed], Some("alpha"));
    assert!(summaries.is_empty());
}

#[test]
fn query_stream_entry_without_kind_is_other() {
    let mut entry = trace_record("other-trace", "other-entry");
    entry.parent_span_id = Some("remote-parent".to_string());
    entry.parent_span_is_remote = true;
    entry.name = "coral.vector_lookup".to_string();
    entry.attributes_json = json!({
        "coral.stream.entry": true,
        "coral.stream.name": "semantic_lookup",
        "workspace": "alpha",
    })
    .to_string();

    let summaries = project(&[entry], Some("alpha"));
    assert_eq!(summaries.len(), 1);
    let summary = summaries.first().expect("other summary");
    assert_eq!(summary.operation_kind, StoredTraceOperationKind::Other);
    assert_eq!(summary.operation_name, "semantic_lookup");
    assert!(summary.query.is_empty());
}

#[test]
fn query_stream_hides_entries_with_unfinished_local_parents() {
    let mut unfinished_child = trace_record("unfinished-trace", "unfinished-query");
    unfinished_child.parent_span_id = Some("unfinished-protocol-parent".to_string());
    unfinished_child.parent_span_is_remote = false;
    unfinished_child.attributes_json = json!({
        "coral.stream.entry": true,
        "coral.stream.kind": "query",
        "coral.stream.name": "sql",
        "workspace": "alpha",
        "sql": "SELECT 'unfinished'",
    })
    .to_string();

    let mut remote_root = trace_record("remote-root-trace", "remote-query");
    remote_root.parent_span_id = Some("remote-parent".to_string());
    remote_root.parent_span_is_remote = true;
    remote_root.attributes_json = json!({
        "coral.stream.entry": true,
        "coral.stream.kind": "query",
        "coral.stream.name": "sql",
        "workspace": "alpha",
        "sql": "SELECT 'remote'",
    })
    .to_string();

    let files = TraceFiles::with_records(&[unfinished_child, remote_root]);
    let summaries = files.list_with_detail(10, 0, Some("alpha"));
    assert_eq!(summaries.len(), 1);
    assert_eq!(
        summaries.first().expect("remote summary").root_span_id,
        "remote-query"
    );

    assert!(matches!(
        files.get("unfinished-trace", "unfinished-query", Some("alpha")),
        Err(TraceStoreError::NotFound(_))
    ));
    assert!(
        files
            .get("remote-root-trace", "remote-query", Some("alpha"))
            .is_ok(),
        "a missing remote parent is a valid local operation root"
    );
}

#[test]
fn query_stream_hides_malformed_parent_cycles_in_list_and_detail() {
    let mut entry = trace_record("cyclic-trace", "cyclic-query");
    entry.parent_span_id = Some("cyclic-parent".to_string());
    entry.parent_span_is_remote = false;
    entry.name = "coral.query".to_string();
    entry.attributes_json = json!({
        "workspace": "alpha",
        "operation": "sql",
        "sql": "SELECT 'cycle'",
    })
    .to_string();

    let mut parent = trace_record("cyclic-trace", "cyclic-parent");
    parent.parent_span_id = Some(entry.span_id.clone());
    parent.parent_span_is_remote = false;
    parent.name = "internal.cycle".to_string();
    parent.attributes_json = json!({"workspace": "alpha"}).to_string();

    let span_refs = [&entry, &parent];
    assert!(
        query_stream_summaries(&span_refs, Some("alpha")).is_empty(),
        "batch projection hides malformed cycles"
    );

    let files = TraceFiles::with_records(&[entry, parent]);
    assert!(
        files.list(10, 0, Some("alpha")).is_empty(),
        "streaming projection hides malformed cycles"
    );
    assert!(matches!(
        files.get("cyclic-trace", "cyclic-query", Some("alpha")),
        Err(TraceStoreError::NotFound(_))
    ));
}

#[test]
fn query_stream_detail_excludes_nested_visible_operations() {
    let mut outer = trace_record("nested-trace", "outer-query");
    outer.parent_span_id = Some("remote-parent".to_string());
    outer.parent_span_is_remote = true;
    outer.attributes_json = json!({
        "workspace": "alpha",
        "operation": "sql",
        "sql": "SELECT 1",
    })
    .to_string();

    let mut nested = trace_record("nested-trace", "nested-operation");
    nested.parent_span_id = Some(outer.span_id.clone());
    nested.name = "coral.future.operation".to_string();
    nested.attributes_json = json!({
        "coral.stream.entry": true,
        "coral.stream.kind": "future_kind",
        "coral.stream.name": "future_operation",
        "workspace": "beta",
    })
    .to_string();

    let mut nested_child = trace_record("nested-trace", "nested-child");
    nested_child.parent_span_id = Some(nested.span_id.clone());
    nested_child.name = "coral.future.child".to_string();

    let files = TraceFiles::with_records(&[outer, nested, nested_child]);
    let summaries = files.list_with_detail(10, 0, None);
    assert_eq!(summaries.len(), 2);

    let detail = files
        .get("nested-trace", "outer-query", Some("alpha"))
        .expect("get outer operation");
    assert_eq!(detail.summary.root_span_id, "outer-query");
    assert_eq!(detail.spans.len(), detail.summary.span_count as usize);
    assert_eq!(
        detail
            .spans
            .iter()
            .map(|span| span.span_id.as_str())
            .collect::<Vec<_>>(),
        vec!["outer-query"]
    );
}

// LIST projection, pagination, and streaming behavior.

#[test]
fn query_stream_projects_many_operations_from_one_distributed_trace() {
    let mut records = Vec::new();

    for index in 0..128 {
        let tool_span_id = format!("tool-{index:03}");
        let tool = span("shared-trace", &tool_span_id)
            .named("coral.mcp.call_tool")
            .remote_root()
            .entry("tool", "sql", "alpha")
            .attrs(json!({
                "mcp.method": "tools/call",
                "mcp.tool.name": "sql",
            }))
            .times(index * 10, index * 10 + 9)
            .build();
        let query = span("shared-trace", &format!("query-{index:03}"))
            .child_of(&tool)
            .entry("query", "sql", "alpha")
            .attrs(json!({"sql": format!("SELECT {index}"), "row_count": index}))
            .times(index * 10 + 1, index * 10 + 8)
            .build();
        records.extend([tool, query]);
    }

    let summaries = project_page(&records, 200, 0, Some("alpha"));
    assert_eq!(summaries.len(), 128);
    assert!(summaries.iter().all(|summary| {
        summary.operation_kind == StoredTraceOperationKind::Tool
            && summary.span_count == 2
            && !summary.query.is_empty()
    }));
    assert_eq!(
        summaries
            .iter()
            .map(|summary| summary.root_span_id.as_str())
            .collect::<HashSet<_>>()
            .len(),
        128
    );
}

#[test]
fn query_stream_suppresses_protocol_descendants_without_method_enumeration() {
    let mut protocol = trace_record("future-trace", "protocol");
    protocol.name = "coral.mcp.future_protocol".to_string();
    protocol.attributes_json = r#"{"mcp.method":"future/negotiate"}"#.to_string();

    let mut protocol_child = trace_record("future-trace", "protocol-child");
    protocol_child.parent_span_id = Some(protocol.span_id.clone());
    protocol_child.name = "coral.vector_lookup".to_string();
    protocol_child.attributes_json = json!({
        "coral.stream.entry": true,
        "coral.stream.kind": "vector_lookup",
        "coral.stream.name": "semantic_lookup",
        "workspace": "alpha",
    })
    .to_string();

    let mut direct_future = trace_record("future-trace", "direct-future");
    direct_future.parent_span_id = Some("remote-parent".to_string());
    direct_future.parent_span_is_remote = true;
    direct_future.name = "coral.vector_lookup".to_string();
    direct_future.attributes_json = json!({
        "coral.stream.entry": true,
        "coral.stream.kind": "vector_lookup",
        "coral.stream.name": "semantic_lookup",
        "workspace": "alpha",
    })
    .to_string();
    direct_future.start_time_unix_nanos = 10;
    direct_future.end_time_unix_nanos = 11;

    let files = TraceFiles::with_records(&[protocol, protocol_child, direct_future]);
    let summaries = files.list_with_detail(10, 0, Some("alpha"));
    assert_eq!(summaries.len(), 1);
    let summary = summaries.first().expect("future summary");
    assert_eq!(summary.root_span_id, "direct-future");
    assert_eq!(summary.operation_kind, StoredTraceOperationKind::Other);
    assert_eq!(summary.operation_name, "semantic_lookup");
}

#[test]
fn query_stream_supports_legacy_operations_and_protocol_suppression() {
    let mut protocol = trace_record("legacy-trace", "protocol");
    protocol.name = "coral.mcp.protocol".to_string();
    protocol.attributes_json = r#"{"mcp.method":"some/future_method"}"#.to_string();

    let mut hidden_query = trace_record("legacy-trace", "hidden-query");
    hidden_query.parent_span_id = Some(protocol.span_id.clone());
    hidden_query.attributes_json = r#"{"workspace":"alpha","sql":"LIST CATALOG"}"#.to_string();

    let mut legacy_tool = trace_record("legacy-trace", "legacy-tool");
    legacy_tool.name = "coral.mcp.call_tool".to_string();
    legacy_tool.attributes_json =
        r#"{"mcp.method":"tools/call","mcp.tool.name":"list_catalog"}"#.to_string();
    legacy_tool.start_time_unix_nanos = 10;
    legacy_tool.end_time_unix_nanos = 13;

    let mut visible_query = trace_record("legacy-trace", "visible-query");
    visible_query.parent_span_id = Some(legacy_tool.span_id.clone());
    visible_query.attributes_json =
        r#"{"workspace":"alpha","operation":"list_catalog","sql":"LIST CATALOG"}"#.to_string();
    visible_query.start_time_unix_nanos = 11;
    visible_query.end_time_unix_nanos = 12;

    let mut search_tool = trace_record("search-trace", "legacy-search-tool");
    search_tool.name = "coral.mcp.call_tool".to_string();
    search_tool.attributes_json =
        r#"{"mcp.method":"tools/call","mcp.tool.name":"search"}"#.to_string();
    search_tool.start_time_unix_nanos = 20;
    search_tool.end_time_unix_nanos = 25;

    let mut search = trace_record("search-trace", "legacy-search");
    search.parent_span_id = Some(search_tool.span_id.clone());
    search.name = "coral.search".to_string();
    search.attributes_json = r#"{"workspace":"alpha","operation":"search"}"#.to_string();
    search.start_time_unix_nanos = 21;
    search.end_time_unix_nanos = 24;

    let mut search_catalog_query = trace_record("search-trace", "legacy-search-catalog-query");
    search_catalog_query.parent_span_id = Some(search.span_id.clone());
    search_catalog_query.attributes_json =
        r#"{"workspace":"alpha","operation":"list_catalog","sql":"LIST CATALOG"}"#.to_string();
    search_catalog_query.start_time_unix_nanos = 22;
    search_catalog_query.end_time_unix_nanos = 23;

    let files = TraceFiles::with_records(&[
        protocol,
        hidden_query,
        legacy_tool,
        visible_query,
        search_tool,
        search,
        search_catalog_query,
    ]);
    let summaries = files.list_with_detail(10, 0, Some("alpha"));
    assert_eq!(summaries.len(), 2);
    let search_summary = summaries.first().expect("legacy search summary");
    assert_eq!(search_summary.root_span_id, "legacy-search-tool");
    assert_eq!(
        search_summary.operation_kind,
        StoredTraceOperationKind::Tool
    );
    assert_eq!(search_summary.operation_name, "search");
    assert!(search_summary.query.is_empty());
    let tool_summary = summaries.get(1).expect("legacy tool summary");
    assert_eq!(tool_summary.root_span_id, "legacy-tool");
    assert_eq!(tool_summary.operation_kind, StoredTraceOperationKind::Tool);
    assert_eq!(tool_summary.operation_name, "list_catalog");
    assert_eq!(tool_summary.query, "LIST CATALOG");

    let search_detail = files
        .get("search-trace", "legacy-search-tool", Some("alpha"))
        .expect("get legacy search entry");
    assert_eq!(search_detail.summary, *search_summary);
    assert!(
        search_detail
            .spans
            .iter()
            .any(|span| span.span_id == "legacy-search-catalog-query"),
        "the internal catalog query remains available in detail spans"
    );
    assert!(
        search_detail.summary.query.is_empty(),
        "Tool -> Search -> Query must not present internal SQL as the Tool query"
    );
}

#[test]
fn query_stream_detail_uses_search_text_without_inheriting_internal_query() {
    let tool = span("marked-search-trace", "search-tool")
        .named("coral.mcp.call_tool")
        .remote_root()
        .entry("tool", "search", "alpha")
        .attrs(json!({
            "mcp.method": "tools/call",
            "mcp.tool.name": "search",
        }))
        .times(10, 40)
        .build();
    let search = span("marked-search-trace", "search-operation")
        .named("coral.search")
        .child_of(&tool)
        .entry("search", "search", "alpha")
        .attrs(json!({"coral.local.search.query": "find customer churn"}))
        .times(15, 35)
        .build();
    let query = span("marked-search-trace", "catalog-query")
        .child_of(&search)
        .entry("query", "sql", "alpha")
        .attrs(json!({"sql": "LIST CATALOG", "row_count": 7}))
        .times(20, 30)
        .build();

    let files = TraceFiles::with_records(&[tool, search, query]);
    let summaries = files.list_with_detail(10, 0, Some("alpha"));
    assert_eq!(summaries.len(), 1);
    let summary = summaries.first().expect("search tool summary");
    assert_eq!(summary.root_span_id, "search-tool");
    assert_eq!(summary.query, "find customer churn");
    assert_ne!(summary.query, "LIST CATALOG");
    assert!(!summary.row_count_recorded);
    assert_eq!(summary.row_count, 0);

    let detail = files
        .get("marked-search-trace", "search-tool", Some("alpha"))
        .expect("get marked search tool detail");
    assert_eq!(detail.summary, *summary);
    assert_eq!(detail.spans.len(), 3);
    assert!(
        detail
            .spans
            .iter()
            .any(|span| span.span_id == "catalog-query")
    );
    for hidden_span_id in ["search-operation", "catalog-query"] {
        assert!(matches!(
            files.get("marked-search-trace", hidden_span_id, Some("alpha")),
            Err(TraceStoreError::NotFound(_))
        ));
    }
}

#[test]
fn query_stream_legacy_tool_requires_consistent_descendant_workspace() {
    let mut tool = trace_record("legacy-conflict", "legacy-tool");
    tool.name = "coral.mcp.call_tool".to_string();
    tool.attributes_json =
        r#"{"mcp.method":"tools/call","mcp.tool.name":"future_tool"}"#.to_string();
    tool.start_time_unix_nanos = 10;
    tool.end_time_unix_nanos = 30;

    let mut alpha = trace_record("legacy-conflict", "alpha-child");
    alpha.parent_span_id = Some(tool.span_id.clone());
    alpha.name = "internal.alpha".to_string();
    alpha.attributes_json = r#"{"workspace":"alpha"}"#.to_string();
    alpha.start_time_unix_nanos = 15;
    alpha.end_time_unix_nanos = 16;

    let mut beta = trace_record("legacy-conflict", "beta-child");
    beta.parent_span_id = Some(tool.span_id.clone());
    beta.name = "internal.beta".to_string();
    beta.attributes_json = r#"{"workspace":"beta"}"#.to_string();
    beta.start_time_unix_nanos = 20;
    beta.end_time_unix_nanos = 21;

    let records = [tool, alpha, beta];
    let files = TraceFiles::with_records(&records);
    assert!(files.list(10, 0, Some("alpha")).is_empty());
    let global = files.list_with_detail(10, 0, None);
    assert_eq!(global.len(), 1);
    assert_eq!(
        global.first().expect("legacy tool").operation_name,
        "future_tool"
    );
}

#[test]
fn query_stream_redacts_prose_shaped_legacy_tool_name() {
    let sentinel = "SENSITIVE prose-shaped legacy tool";

    let mut tool = trace_record("legacy-private-trace", "legacy-private-tool");
    tool.name = "coral.mcp.call_tool".to_string();
    tool.attributes_json = json!({
        "mcp.method": "tools/call",
        "mcp.tool.name": sentinel,
    })
    .to_string();

    let summaries = project(&[tool], None);
    assert_eq!(summaries.len(), 1);
    let summary = summaries.first().expect("legacy tool summary");
    assert_eq!(summary.operation_kind, StoredTraceOperationKind::Tool);
    assert_eq!(summary.operation_name, UNKNOWN_TOOL_OPERATION_NAME);
    assert!(!format!("{summary:?}").contains(sentinel));
}

#[test]
fn legacy_tool_operation_name_policy_bounds_identifiers() {
    let longest_valid = format!("2.{}", "a".repeat(MAX_LEGACY_TOOL_OPERATION_NAME_LEN - 2));
    assert_eq!(
        privacy_safe_legacy_tool_operation_name(longest_valid.clone()),
        longest_valid
    );

    let overlong = format!("2.{}", "a".repeat(MAX_LEGACY_TOOL_OPERATION_NAME_LEN - 1));
    assert_eq!(
        privacy_safe_legacy_tool_operation_name(overlong),
        UNKNOWN_TOOL_OPERATION_NAME
    );
}
