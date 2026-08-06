use serde_json::json;

use super::super::tests::trace_record;
use super::super::{StoredTraceInvocationKind, StoredTraceOperationKind, StoredTraceStatus};
use super::classification::{
    MAX_LEGACY_TOOL_OPERATION_NAME_LEN, UNKNOWN_TOOL_OPERATION_NAME,
    privacy_safe_tool_operation_name,
};
use super::test_support::{project, span};

// Shared classification and ownership behavior.

#[test]
fn query_stream_projects_outer_operations() {
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

    let summaries = project(&[sql_tool, nested_query], None);
    assert_eq!(summaries.len(), 1);
    let sql_summary = summaries.first().expect("SQL summary");
    assert_eq!(sql_summary.root_span_id, "sql-tool");
    assert_eq!(sql_summary.operation_kind, StoredTraceOperationKind::Query);
    assert_eq!(sql_summary.operation_name, "sql");
    assert_eq!(sql_summary.invocation_kind, StoredTraceInvocationKind::Mcp);
    assert_eq!(sql_summary.query, "SELECT 42");
    assert_eq!(sql_summary.row_count, 7);
    assert!(sql_summary.row_count_recorded);
    assert_eq!(sql_summary.status, StoredTraceStatus::Error);
    assert_eq!(sql_summary.start_time_unix_nanos, 10);
    assert_eq!(sql_summary.end_time_unix_nanos, 40);
    assert_eq!(sql_summary.duration_nanos, 30);
    assert_eq!(sql_summary.span_count, 2);
}

#[test]
fn query_stream_projects_search_text_for_mcp_operation() {
    let tool = span("search-trace", "search-tool")
        .named("coral.mcp.call_tool")
        .remote_root()
        .entry("search", "search", "beta")
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

    let summaries = project(&[tool, search, internal_query], Some("beta"));
    let summary = summaries.first().expect("tool search summary");
    assert_eq!(summaries.len(), 1);
    assert_eq!(summary.root_span_id, "search-tool");
    assert_eq!(summary.operation_kind, StoredTraceOperationKind::Search);
    assert_eq!(summary.operation_name, "search");
    assert_eq!(summary.invocation_kind, StoredTraceInvocationKind::Mcp);
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

    let summaries = project(&[search], Some("gamma"));
    let summary = summaries.first().expect("direct search summary");
    assert_eq!(summaries.len(), 1);
    assert_eq!(summary.root_span_id, "direct-search");
    assert_eq!(summary.operation_kind, StoredTraceOperationKind::Search);
    assert_eq!(summary.invocation_kind, StoredTraceInvocationKind::Direct);
    assert_eq!(summary.query, "direct search phrase");
}

#[test]
fn query_stream_projects_bare_legacy_root_query() {
    let mut query = trace_record("legacy-query-trace", "legacy-query");
    query.attributes_json = json!({
        "workspace": "alpha",
        "sql": "SELECT 42",
        "row_count": 7,
    })
    .to_string();

    let summaries = project(&[query], Some("alpha"));
    assert_eq!(summaries.len(), 1);
    let summary = summaries.first().expect("legacy query summary");
    assert_eq!(summary.root_span_id, "legacy-query");
    assert_eq!(summary.operation_kind, StoredTraceOperationKind::Query);
    assert_eq!(summary.operation_name, "sql");
    assert_eq!(summary.invocation_kind, StoredTraceInvocationKind::Direct);
    assert_eq!(summary.query, "SELECT 42");
    assert_eq!(summary.row_count, 7);
    assert!(summary.row_count_recorded);
}

#[test]
fn query_stream_ignores_row_count_on_non_query_entry() {
    let search = span("search-row-count-trace", "search")
        .named("coral.search")
        .entry("search", "search", "alpha")
        .attrs(json!({
            "coral.local.search.query": "find customer churn",
            "row_count": 99,
        }))
        .build();

    let summaries = project(&[search], Some("alpha"));
    let summary = summaries.first().expect("search summary");
    assert_eq!(summary.operation_kind, StoredTraceOperationKind::Search);
    assert_eq!(summary.query, "find customer churn");
    assert_eq!(summary.row_count, 0);
    assert!(!summary.row_count_recorded);
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
    assert_eq!(summary.invocation_kind, StoredTraceInvocationKind::Direct);
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

    let summaries = project(&[unfinished_child, remote_root], Some("alpha"));
    assert_eq!(summaries.len(), 1);
    assert_eq!(
        summaries.first().expect("remote summary").root_span_id,
        "remote-query"
    );
}

#[test]
fn query_stream_keeps_one_operation_when_a_remote_parent_is_reused() {
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

    let summaries = project(&records, Some("alpha"));
    let summary = summaries.first().expect("newest operation");
    assert_eq!(summaries.len(), 1);
    assert_eq!(summary.root_span_id, "tool-127");
    assert_eq!(summary.operation_kind, StoredTraceOperationKind::Query);
    assert_eq!(summary.invocation_kind, StoredTraceInvocationKind::Mcp);
    assert_eq!(summary.span_count, 2);
    assert_eq!(summary.query, "SELECT 127");
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

    let summaries = project(&[protocol, protocol_child, direct_future], Some("alpha"));
    assert_eq!(summaries.len(), 1);
    let summary = summaries.first().expect("future summary");
    assert_eq!(summary.root_span_id, "direct-future");
    assert_eq!(summary.operation_kind, StoredTraceOperationKind::Other);
    assert_eq!(summary.operation_name, "semantic_lookup");
    assert_eq!(summary.invocation_kind, StoredTraceInvocationKind::Direct);
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

    let summaries = project(
        &[
            protocol,
            hidden_query,
            legacy_tool,
            visible_query,
            search_tool,
            search,
            search_catalog_query,
        ],
        Some("alpha"),
    );
    assert_eq!(summaries.len(), 2);
    let search_summary = summaries.first().expect("legacy search summary");
    assert_eq!(search_summary.root_span_id, "legacy-search-tool");
    assert_eq!(
        search_summary.operation_kind,
        StoredTraceOperationKind::Search
    );
    assert_eq!(search_summary.operation_name, "search");
    assert_eq!(
        search_summary.invocation_kind,
        StoredTraceInvocationKind::Mcp
    );
    assert!(search_summary.query.is_empty());
    let tool_summary = summaries.get(1).expect("legacy tool summary");
    assert_eq!(tool_summary.root_span_id, "legacy-tool");
    assert_eq!(tool_summary.operation_kind, StoredTraceOperationKind::Tool);
    assert_eq!(tool_summary.operation_name, "list_catalog");
    assert_eq!(tool_summary.invocation_kind, StoredTraceInvocationKind::Mcp);
    assert_eq!(tool_summary.query, "LIST CATALOG");
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
    assert!(project(&records, Some("alpha")).is_empty());
    let global = project(&records, None);
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
    assert_eq!(summary.invocation_kind, StoredTraceInvocationKind::Mcp);
    assert!(!format!("{summary:?}").contains(sentinel));
}

#[test]
fn query_stream_redacts_prose_shaped_explicit_tool_fallback_name() {
    let sentinel = "SENSITIVE prose-shaped explicit tool";

    let mut tool = trace_record("explicit-private-trace", "explicit-private-tool");
    tool.name = "coral.mcp.call_tool".to_string();
    tool.attributes_json = json!({
        "coral.stream.entry": true,
        "coral.stream.kind": "tool",
        "mcp.tool.name": sentinel,
    })
    .to_string();

    let summaries = project(&[tool], None);
    assert_eq!(summaries.len(), 1);
    let summary = summaries.first().expect("explicit tool summary");
    assert_eq!(summary.operation_kind, StoredTraceOperationKind::Tool);
    assert_eq!(summary.operation_name, UNKNOWN_TOOL_OPERATION_NAME);
    assert!(!format!("{summary:?}").contains(sentinel));
}

#[test]
fn tool_operation_name_policy_bounds_identifiers() {
    let longest_valid = format!("2.{}", "a".repeat(MAX_LEGACY_TOOL_OPERATION_NAME_LEN - 2));
    assert_eq!(
        privacy_safe_tool_operation_name(longest_valid.clone()),
        longest_valid
    );

    let overlong = format!("2.{}", "a".repeat(MAX_LEGACY_TOOL_OPERATION_NAME_LEN - 1));
    assert_eq!(
        privacy_safe_tool_operation_name(overlong),
        UNKNOWN_TOOL_OPERATION_NAME
    );
}
