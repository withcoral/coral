use super::*;
use crate::runtime::catalog;
use crate::runtime::registry::{CompiledQuerySource, register_sources_blocking};
use crate::runtime::source_functions::SourceFunctionRegistry;
use crate::{
    QuerySource, SourceInputResolutionContext, SourceInputResolver, SourceInputResolverError,
};
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::error::DataFusionError;
use datafusion::prelude::SessionContext;
use rmcp::model::JsonObject;
use serde_json::Value;
use serde_json::json;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug)]
struct FakeMcpCaller {
    calls: Mutex<Vec<(String, JsonObject)>>,
}

#[async_trait]
impl McpToolCaller for FakeMcpCaller {
    async fn call_tool(
        &self,
        _relation: &str,
        tool_name: &str,
        arguments: JsonObject,
    ) -> Result<Value> {
        self.calls
            .lock()
            .expect("calls lock")
            .push((tool_name.to_string(), arguments.clone()));
        let query = arguments
            .get("query")
            .and_then(Value::as_str)
            .unwrap_or_default();
        Ok(json!({
            "items": [
                { "title": format!("{query} one"), "url": "https://example.com/1" },
                { "title": format!("{query} two"), "url": "https://example.com/2" }
            ]
        }))
    }
}

/// Records each MCP tool call and returns a fixed payload for table tests.
#[derive(Debug)]
struct FakeMcpTableCaller {
    calls: Mutex<Vec<(String, JsonObject)>>,
}

#[async_trait]
impl McpToolCaller for FakeMcpTableCaller {
    async fn call_tool(
        &self,
        _relation: &str,
        tool_name: &str,
        arguments: JsonObject,
    ) -> Result<Value> {
        self.calls
            .lock()
            .expect("calls lock")
            .push((tool_name.to_string(), arguments.clone()));
        Ok(json!({
            "issues": [
                { "id": "1", "title": "Bug A", "state": "open" },
                { "id": "2", "title": "Bug B", "state": "open" },
                { "id": "3", "title": "Bug C", "state": "closed" }
            ]
        }))
    }
}

/// Returns two cursor-paginated pages and records each MCP tool call.
#[derive(Debug)]
struct FakePaginatedMcpTableCaller {
    calls: Mutex<Vec<(String, JsonObject)>>,
}

#[async_trait]
impl McpToolCaller for FakePaginatedMcpTableCaller {
    async fn call_tool(
        &self,
        _relation: &str,
        tool_name: &str,
        arguments: JsonObject,
    ) -> Result<Value> {
        self.calls
            .lock()
            .expect("calls lock")
            .push((tool_name.to_string(), arguments.clone()));
        let cursor = arguments.get("cursor").and_then(Value::as_str);
        match cursor {
            None => Ok(json!({
                "issues": [
                    { "id": "1", "title": "Bug A", "state": "open" }
                ],
                "meta": { "nextCursor": "page-2" }
            })),
            Some("page-2") => Ok(json!({
                "issues": [
                    { "id": "2", "title": "Bug B", "state": "open" },
                    { "id": "3", "title": "Bug C", "state": "closed" }
                ],
                "meta": {}
            })),
            Some(other) => panic!("unexpected cursor: {other}"),
        }
    }
}

#[derive(Debug)]
struct RotatingInputResolver {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl SourceInputResolver for RotatingInputResolver {
    async fn resolve_inputs(
        &self,
        _source: &SourceInputResolutionContext,
    ) -> std::result::Result<BTreeMap<String, String>, SourceInputResolverError> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        Ok(BTreeMap::from([(
            "API_TOKEN".to_string(),
            format!("fresh-token-{call}"),
        )]))
    }
}

fn mcp_manifest() -> coral_spec::ValidatedSourceManifest {
    coral_spec::parse_source_manifest_value(json!({
        "dsl_version": 3,
        "name": "test_mcp",
        "version": "0.1.0",
        "backend": "mcp",
        "server": {
            "transport": "stdio",
            "command": "unused"
        },
        "functions": [{
            "name": "search",
            "tool": "search_tool",
            "args": [{
                "name": "query",
                "required": true,
                "bind": { "arg": "query" }
            }],
            "response": {
                "rows_path": ["items"]
            },
            "columns": [
                { "name": "title", "type": "Utf8" },
                { "name": "url", "type": "Utf8" }
            ]
        }]
    }))
    .expect("mcp manifest should parse")
}

fn mcp_typed_args_manifest() -> coral_spec::ValidatedSourceManifest {
    coral_spec::parse_source_manifest_value(json!({
        "dsl_version": 3,
        "name": "test_mcp",
        "version": "0.1.0",
        "backend": "mcp",
        "server": {
            "transport": "stdio",
            "command": "unused"
        },
        "functions": [{
            "name": "typed_search",
            "tool": "typed_search_tool",
            "args": [
                {
                    "name": "query",
                    "required": true,
                    "bind": { "arg": "query" }
                },
                {
                    "name": "limit",
                    "required": true,
                    "bind": { "arg": "limit" }
                },
                {
                    "name": "include_archived",
                    "required": true,
                    "bind": { "arg": "include_archived" }
                },
                {
                    "name": "threshold",
                    "required": true,
                    "bind": { "arg": "threshold" }
                }
            ],
            "response": {
                "rows_path": ["items"]
            },
            "columns": [
                { "name": "title", "type": "Utf8" },
                { "name": "url", "type": "Utf8" }
            ]
        }]
    }))
    .expect("mcp typed args manifest should parse")
}

fn compile_sources(
    manifest: coral_spec::ValidatedSourceManifest,
    caller: Arc<dyn McpToolCaller>,
) -> Vec<CompiledQuerySource> {
    compile_sources_with_inputs(manifest, caller, BTreeMap::new(), None)
}

fn compile_sources_with_inputs(
    manifest: coral_spec::ValidatedSourceManifest,
    caller: Arc<dyn McpToolCaller>,
    secrets: BTreeMap<String, String>,
    source_input_resolver: Option<Arc<dyn SourceInputResolver>>,
) -> Vec<CompiledQuerySource> {
    let mcp_manifest = manifest.as_mcp().expect("mcp manifest").clone();
    let variables = BTreeMap::new();
    let source = QuerySource::new(manifest, variables.clone(), secrets);
    let source_input_resolution = SourceInputResolutionContext::from_query_source(&source);
    let resolved_inputs = Arc::new(coral_spec::resolve_inputs(
        &mcp_manifest.declared_inputs,
        source_input_resolution.secrets(),
        source_input_resolution.variables(),
    ));
    let source_inputs = match source_input_resolver {
        Some(resolver) => Arc::new(McpSourceInputs::with_resolver(
            Arc::clone(&resolved_inputs),
            source_input_resolution.clone(),
            resolver,
        )),
        None => Arc::new(McpSourceInputs::static_inputs(resolved_inputs)),
    };
    let compiled =
        compile_source_with_caller(mcp_manifest, source_input_resolution, source_inputs, caller);
    vec![CompiledQuerySource { source, compiled }]
}

#[tokio::test]
async fn executes_mcp_table_function_with_bound_args() {
    let ctx = SessionContext::new();
    let caller = Arc::new(FakeMcpCaller {
        calls: Mutex::new(Vec::new()),
    });
    register_test_sources(&ctx, compile_sources(mcp_manifest(), caller.clone()));

    let batches = ctx
        .sql("SELECT title, url FROM test_mcp.search(query => 'issue') ORDER BY title")
        .await
        .expect("query should plan")
        .collect()
        .await
        .expect("query should execute");

    let rendered = pretty_format_batches(&batches)
        .expect("batches should render")
        .to_string();
    assert!(rendered.contains("| issue one"));
    assert!(rendered.contains("| issue two"));

    let calls = caller.calls.lock().expect("calls lock");
    assert_eq!(calls.len(), 1);
    let call = calls.first().expect("one MCP call should be recorded");
    assert_eq!(call.0, "search_tool");
    assert_eq!(
        call.1.get("query"),
        Some(&Value::String("issue".to_string()))
    );
}

#[tokio::test]
async fn mcp_table_function_preserves_json_scalar_arg_types() {
    let ctx = SessionContext::new();
    let caller = Arc::new(FakeMcpCaller {
        calls: Mutex::new(Vec::new()),
    });
    register_test_sources(
        &ctx,
        compile_sources(mcp_typed_args_manifest(), caller.clone()),
    );

    let _ = ctx
        .sql(
            "SELECT title FROM test_mcp.typed_search(\
             query => 'issue', \
             limit => 10, \
             include_archived => true, \
             threshold => 0.75)",
        )
        .await
        .expect("typed function query should plan")
        .collect()
        .await
        .expect("typed function query should execute");

    let calls = caller.calls.lock().expect("calls lock");
    assert_eq!(calls.len(), 1);
    let call = calls.first().expect("one MCP call should be recorded");
    assert_eq!(call.0, "typed_search_tool");
    assert_eq!(
        call.1.get("query"),
        Some(&Value::String("issue".to_string()))
    );
    assert_eq!(call.1.get("limit"), Some(&Value::from(10)));
    assert_eq!(call.1.get("include_archived"), Some(&Value::Bool(true)));
    assert_eq!(call.1.get("threshold"), Some(&json!(0.75)));
}

#[tokio::test]
async fn missing_required_function_arg_fails_planning() {
    let ctx = SessionContext::new();
    let caller = Arc::new(FakeMcpCaller {
        calls: Mutex::new(Vec::new()),
    });
    register_test_sources(&ctx, compile_sources(mcp_manifest(), caller));

    let error = ctx
        .sql("SELECT title FROM test_mcp.search()")
        .await
        .expect_err("missing required arg should fail");

    assert!(
        error
            .to_string()
            .contains("test_mcp.search missing required argument(s): query"),
        "unexpected error: {error}"
    );
}

fn register_test_sources(ctx: &SessionContext, sources: Vec<CompiledQuerySource>) {
    let registration = register_sources_blocking(ctx, sources).expect("mcp source should register");
    let source_functions = SourceFunctionRegistry::new(
        registration
            .active_sources
            .iter()
            .flat_map(|source| source.table_functions.iter()),
    );
    ctx.register_relation_planner(Arc::new(source_functions))
        .expect("source function planner should register");
}

fn register_test_sources_with_catalog(ctx: &SessionContext, sources: Vec<CompiledQuerySource>) {
    let registration = register_sources_blocking(ctx, sources).expect("mcp source should register");
    catalog::register(ctx, &registration.active_sources).expect("catalog should register");
    let source_functions = SourceFunctionRegistry::new(
        registration
            .active_sources
            .iter()
            .flat_map(|source| source.table_functions.iter()),
    );
    ctx.register_relation_planner(Arc::new(source_functions))
        .expect("source function planner should register");
}

fn mcp_table_manifest() -> coral_spec::ValidatedSourceManifest {
    coral_spec::parse_source_manifest_value(json!({
        "dsl_version": 3,
        "name": "test_mcp",
        "version": "0.1.0",
        "backend": "mcp",
        "server": { "transport": "stdio", "command": "unused" },
        "tables": [{
            "name": "issues",
            "description": "Open issues",
            "tool": "list_issues",
            "tool_args": {
                "owner": { "from": "literal", "value": "acme" }
            },
            "filters": [{
                "name": "state",
                "required": false,
                "tool_arg": "state"
            }],
            "response": { "rows_path": ["issues"] },
            "columns": [
                { "name": "id", "type": "Utf8" },
                { "name": "title", "type": "Utf8" },
                { "name": "state", "type": "Utf8" }
            ]
        }]
    }))
    .expect("mcp table manifest should parse")
}

fn mcp_table_required_filter_manifest() -> coral_spec::ValidatedSourceManifest {
    coral_spec::parse_source_manifest_value(json!({
        "dsl_version": 3,
        "name": "test_mcp",
        "version": "0.1.0",
        "backend": "mcp",
        "server": { "transport": "stdio", "command": "unused" },
        "tables": [{
            "name": "issues",
            "description": "Open issues",
            "tool": "list_issues",
            "filters": [{
                "name": "state",
                "required": true,
                "tool_arg": "state"
            }],
            "response": { "rows_path": ["issues"] },
            "columns": [
                { "name": "id", "type": "Utf8" },
                { "name": "title", "type": "Utf8" },
                { "name": "state", "type": "Utf8" }
            ]
        }]
    }))
    .expect("required-filter manifest should parse")
}

#[tokio::test]
async fn scans_mcp_table_with_manifest_tool_args_and_no_filters() {
    let ctx = SessionContext::new();
    let caller = Arc::new(FakeMcpTableCaller {
        calls: Mutex::new(Vec::new()),
    });
    register_test_sources(&ctx, compile_sources(mcp_table_manifest(), caller.clone()));

    let batches = ctx
        .sql("SELECT id, title, state FROM test_mcp.issues ORDER BY id")
        .await
        .expect("table query should plan")
        .collect()
        .await
        .expect("table query should execute");

    let rendered = pretty_format_batches(&batches)
        .expect("batches should render")
        .to_string();
    assert!(rendered.contains("| Bug A"));
    assert!(rendered.contains("| Bug B"));
    assert!(rendered.contains("| Bug C"));

    let calls = caller.calls.lock().expect("calls lock");
    assert_eq!(calls.len(), 1);
    let call = calls.first().expect("one MCP call");
    assert_eq!(call.0, "list_issues");
    assert_eq!(
        call.1.get("owner"),
        Some(&Value::String("acme".to_string()))
    );
    assert!(
        call.1.get("state").is_none(),
        "unbound optional filter should not be passed: {:?}",
        call.1
    );
}

fn mcp_typed_filters_manifest() -> coral_spec::ValidatedSourceManifest {
    coral_spec::parse_source_manifest_value(json!({
        "dsl_version": 3,
        "name": "test_mcp",
        "version": "0.1.0",
        "backend": "mcp",
        "server": { "transport": "stdio", "command": "unused" },
        "tables": [{
            "name": "issues",
            "description": "Issues filtered by typed scalar values",
            "tool": "list_issues",
            "filters": [
                { "name": "limit", "type": "Int64", "tool_arg": "limit" },
                { "name": "include_archived", "type": "Boolean", "tool_arg": "include_archived" },
                { "name": "threshold", "type": "Float64", "tool_arg": "threshold" },
                { "name": "state", "type": "Utf8", "tool_arg": "state" },
                { "name": "metadata", "type": "Json", "tool_arg": "metadata" },
            ],
            "response": { "rows_path": ["issues"] },
            "columns": [
                { "name": "limit", "type": "Int64", "virtual": true,
                  "expr": { "kind": "from_filter", "key": "limit" } },
                { "name": "include_archived", "type": "Boolean", "virtual": true,
                  "expr": { "kind": "from_filter", "key": "include_archived" } },
                { "name": "threshold", "type": "Float64", "virtual": true,
                  "expr": { "kind": "from_filter", "key": "threshold" } },
                { "name": "state", "type": "Utf8", "virtual": true,
                  "expr": { "kind": "from_filter", "key": "state" } },
                { "name": "metadata", "type": "Json", "virtual": true,
                  "expr": { "kind": "from_filter", "key": "metadata" } },
                { "name": "id", "type": "Utf8" },
                { "name": "title", "type": "Utf8" },
            ],
        }],
    }))
    .expect("typed-filter manifest should parse")
}

#[tokio::test]
async fn pushes_typed_filter_values_with_declared_json_scalar_types() {
    let ctx = SessionContext::new();
    let caller = Arc::new(FakeMcpTableCaller {
        calls: Mutex::new(Vec::new()),
    });
    register_test_sources(
        &ctx,
        compile_sources(mcp_typed_filters_manifest(), caller.clone()),
    );

    let _ = ctx
        .sql(
            "SELECT id FROM test_mcp.issues \
             WHERE \"limit\" = 10 \
             AND include_archived = true \
             AND threshold = 0.75 \
             AND state = 'open' \
             AND metadata = '{\"tag\":\"alpha\",\"ids\":[1,2]}'",
        )
        .await
        .expect("typed-filter query should plan")
        .collect()
        .await
        .expect("typed-filter query should execute");

    let calls = caller.calls.lock().expect("calls lock");
    assert_eq!(calls.len(), 1);
    let call = calls.first().expect("one MCP call");
    assert_eq!(
        call.1.get("limit"),
        Some(&Value::from(10)),
        "Int64 filter must push as a JSON number, not a string"
    );
    assert_eq!(
        call.1.get("include_archived"),
        Some(&Value::Bool(true)),
        "Boolean filter must push as a JSON bool, not a string"
    );
    assert_eq!(
        call.1.get("threshold"),
        Some(&json!(0.75)),
        "Float64 filter must push as a JSON number, not a string"
    );
    assert_eq!(
        call.1.get("state"),
        Some(&Value::String("open".to_string())),
        "Utf8 filter must still push as a JSON string"
    );
    assert_eq!(
        call.1.get("metadata"),
        Some(&json!({ "tag": "alpha", "ids": [1, 2] })),
        "Json filter must push the parsed JSON value, not a quoted string"
    );
}

#[tokio::test]
async fn pushes_equality_filter_into_mcp_tool_arg() {
    let ctx = SessionContext::new();
    let caller = Arc::new(FakeMcpTableCaller {
        calls: Mutex::new(Vec::new()),
    });
    register_test_sources(&ctx, compile_sources(mcp_table_manifest(), caller.clone()));

    let _ = ctx
        .sql("SELECT id FROM test_mcp.issues WHERE state = 'open'")
        .await
        .expect("filter query should plan")
        .collect()
        .await
        .expect("filter query should execute");

    let calls = caller.calls.lock().expect("calls lock");
    assert_eq!(calls.len(), 1);
    let call = calls.first().expect("one MCP call");
    assert_eq!(
        call.1.get("state"),
        Some(&Value::String("open".to_string()))
    );
}

#[tokio::test]
async fn missing_required_filter_fails_planning() {
    let ctx = SessionContext::new();
    let caller = Arc::new(FakeMcpTableCaller {
        calls: Mutex::new(Vec::new()),
    });
    register_test_sources(
        &ctx,
        compile_sources(mcp_table_required_filter_manifest(), caller.clone()),
    );

    let error = ctx
        .sql("SELECT id FROM test_mcp.issues")
        .await
        .expect("planning succeeds before scan")
        .collect()
        .await
        .expect_err("missing required filter should fail");

    let message = error.to_string();
    assert!(
        message.contains("test_mcp.issues table requires a constant equality filter"),
        "unexpected error: {message}"
    );
    assert!(
        message.contains("WHERE state = <constant>"),
        "missing column hint in error: {message}"
    );

    let root = error.find_root();
    match root {
        DataFusionError::External(inner) => {
            let provider = inner
                .downcast_ref::<McpProviderQueryError>()
                .expect("error should downcast to McpProviderQueryError");
            match provider {
                McpProviderQueryError::MissingRequiredFilter {
                    schema,
                    table,
                    column,
                } => {
                    assert_eq!(schema, "test_mcp");
                    assert_eq!(table, "issues");
                    assert_eq!(column, "state");
                }
                other => panic!("unexpected MCP error variant: {other:?}"),
            }
            let structured = provider.to_structured();
            assert_eq!(structured.reason(), "MISSING_REQUIRED_FILTER");
        }
        other => panic!("expected External error, got {other:?}"),
    }

    assert!(
        caller.calls.lock().expect("calls lock").is_empty(),
        "no MCP call should be made when planning fails"
    );
}

#[tokio::test]
async fn applies_projection_and_limit_to_mcp_table_scan() {
    let ctx = SessionContext::new();
    let caller = Arc::new(FakeMcpTableCaller {
        calls: Mutex::new(Vec::new()),
    });
    register_test_sources(&ctx, compile_sources(mcp_table_manifest(), caller));

    let batches = ctx
        .sql("SELECT title FROM test_mcp.issues ORDER BY id LIMIT 1")
        .await
        .expect("limit query should plan")
        .collect()
        .await
        .expect("limit query should execute");

    let total_rows: usize = batches
        .iter()
        .map(datafusion::arrow::array::RecordBatch::num_rows)
        .sum();
    assert_eq!(total_rows, 1);
    let schema = batches.first().expect("at least one batch").schema();
    assert_eq!(schema.fields().len(), 1);
    assert_eq!(schema.field(0).name(), "title");
}

/// Returns the ClickHouse-style success/error union and records each MCP
/// tool call.
#[derive(Debug)]
struct FakeMcpUnionCaller {
    payload: Value,
    calls: Mutex<Vec<(String, JsonObject)>>,
}

#[async_trait]
impl McpToolCaller for FakeMcpUnionCaller {
    async fn call_tool(
        &self,
        _relation: &str,
        tool_name: &str,
        arguments: JsonObject,
    ) -> Result<Value> {
        self.calls
            .lock()
            .expect("calls lock")
            .push((tool_name.to_string(), arguments));
        Ok(self.payload.clone())
    }
}

fn mcp_table_with_error_path_manifest() -> coral_spec::ValidatedSourceManifest {
    coral_spec::parse_source_manifest_value(json!({
        "dsl_version": 3,
        "name": "test_mcp",
        "version": "0.1.0",
        "backend": "mcp",
        "server": { "transport": "stdio", "command": "unused" },
        "tables": [{
            "name": "issues",
            "description": "Open issues",
            "tool": "list_issues",
            "response": {
                "rows_path": ["result", "data"],
                "error_path": ["result", "message"],
            },
            "columns": [
                { "name": "id", "type": "Utf8" },
                { "name": "title", "type": "Utf8" },
            ],
        }],
    }))
    .expect("error-path manifest should parse")
}

fn mcp_table_with_ok_path_manifest() -> coral_spec::ValidatedSourceManifest {
    coral_spec::parse_source_manifest_value(json!({
        "dsl_version": 3,
        "name": "test_mcp",
        "version": "0.1.0",
        "backend": "mcp",
        "server": { "transport": "stdio", "command": "unused" },
        "tables": [{
            "name": "issues",
            "description": "Open issues",
            "tool": "list_issues",
            "response": {
                "rows_path": ["data"],
                "ok_path": ["ok"],
                "error_path": ["error"],
            },
            "columns": [
                { "name": "id", "type": "Utf8" },
                { "name": "title", "type": "Utf8" },
            ],
        }],
    }))
    .expect("ok-path manifest should parse")
}

#[tokio::test]
async fn error_path_surfaces_tool_returned_error_when_present() {
    let ctx = SessionContext::new();
    let caller = Arc::new(FakeMcpUnionCaller {
        payload: json!({
            "result": { "status": "error", "message": "Code: 62. Syntax error" }
        }),
        calls: Mutex::new(Vec::new()),
    });
    register_test_sources(
        &ctx,
        compile_sources(mcp_table_with_error_path_manifest(), caller.clone()),
    );

    let error = ctx
        .sql("SELECT id FROM test_mcp.issues")
        .await
        .expect("planning succeeds before scan")
        .collect()
        .await
        .expect_err("error payload should surface as engine error");

    let root = error.find_root();
    match root {
        DataFusionError::External(inner) => {
            let provider = inner
                .downcast_ref::<McpProviderQueryError>()
                .expect("error should downcast to McpProviderQueryError");
            match provider {
                McpProviderQueryError::ToolReturnedError {
                    source_schema,
                    relation,
                    tool,
                    detail,
                } => {
                    assert_eq!(source_schema, "test_mcp");
                    assert_eq!(relation, "issues");
                    assert_eq!(tool, "list_issues");
                    assert_eq!(detail, "Code: 62. Syntax error");
                }
                other => panic!("unexpected MCP error variant: {other:?}"),
            }
            assert_eq!(provider.to_structured().reason(), "MCP_TOOL_RETURNED_ERROR");
        }
        other => panic!("expected External error, got {other:?}"),
    }
}

#[tokio::test]
async fn ok_path_true_skips_error_path_even_when_present() {
    let ctx = SessionContext::new();
    let caller = Arc::new(FakeMcpUnionCaller {
        payload: json!({
            "ok": true,
            "error": "",
            "data": [
                { "id": "1", "title": "Bug A" },
                { "id": "2", "title": "Bug B" }
            ]
        }),
        calls: Mutex::new(Vec::new()),
    });
    register_test_sources(
        &ctx,
        compile_sources(mcp_table_with_ok_path_manifest(), caller.clone()),
    );

    let batches = ctx
        .sql("SELECT id, title FROM test_mcp.issues ORDER BY id")
        .await
        .expect("query should plan")
        .collect()
        .await
        .expect("ok_path=true must not be misclassified by non-null error_path");

    let rendered = pretty_format_batches(&batches)
        .expect("batches should render")
        .to_string();
    assert!(rendered.contains("| Bug A"));
    assert!(rendered.contains("| Bug B"));
}

#[tokio::test]
async fn ok_path_false_surfaces_tool_returned_error_with_error_path_detail() {
    let ctx = SessionContext::new();
    let caller = Arc::new(FakeMcpUnionCaller {
        payload: json!({
            "ok": false,
            "error": "rate_limited",
            "data": []
        }),
        calls: Mutex::new(Vec::new()),
    });
    register_test_sources(
        &ctx,
        compile_sources(mcp_table_with_ok_path_manifest(), caller.clone()),
    );

    let error = ctx
        .sql("SELECT id FROM test_mcp.issues")
        .await
        .expect("planning succeeds before scan")
        .collect()
        .await
        .expect_err("ok_path=false should surface as engine error");

    let root = error.find_root();
    match root {
        DataFusionError::External(inner) => {
            let provider = inner
                .downcast_ref::<McpProviderQueryError>()
                .expect("error should downcast to McpProviderQueryError");
            match provider {
                McpProviderQueryError::ToolReturnedError { detail, .. } => {
                    assert_eq!(detail, "rate_limited");
                }
                other => panic!("unexpected MCP error variant: {other:?}"),
            }
        }
        other => panic!("expected External error, got {other:?}"),
    }
}

#[tokio::test]
async fn error_path_does_not_trigger_on_success_payload() {
    let ctx = SessionContext::new();
    let caller = Arc::new(FakeMcpUnionCaller {
        payload: json!({
            "result": {
                "data": [
                    { "id": "1", "title": "Bug A" },
                    { "id": "2", "title": "Bug B" }
                ]
            }
        }),
        calls: Mutex::new(Vec::new()),
    });
    register_test_sources(
        &ctx,
        compile_sources(mcp_table_with_error_path_manifest(), caller.clone()),
    );

    let batches = ctx
        .sql("SELECT id, title FROM test_mcp.issues ORDER BY id")
        .await
        .expect("query should plan")
        .collect()
        .await
        .expect("query should execute");

    let rendered = pretty_format_batches(&batches)
        .expect("batches should render")
        .to_string();
    assert!(rendered.contains("| Bug A"));
    assert!(rendered.contains("| Bug B"));
}

fn mcp_table_with_limit_binding_manifest(
    max: Option<usize>,
) -> coral_spec::ValidatedSourceManifest {
    let mut binding = serde_json::Map::new();
    binding.insert(
        "tool_arg".to_string(),
        Value::String("page_size".to_string()),
    );
    if let Some(max) = max {
        binding.insert("max".to_string(), serde_json::json!(max));
    }
    let binding = Value::Object(binding);
    coral_spec::parse_source_manifest_value(json!({
        "dsl_version": 3,
        "name": "test_mcp",
        "version": "0.1.0",
        "backend": "mcp",
        "server": { "transport": "stdio", "command": "unused" },
        "tables": [{
            "name": "issues",
            "description": "issues with limit binding",
            "tool": "list_issues",
            "limit_binding": binding,
            "response": { "rows_path": ["issues"] },
            "columns": [
                { "name": "id", "type": "Utf8" },
                { "name": "title", "type": "Utf8" },
                { "name": "state", "type": "Utf8" }
            ]
        }]
    }))
    .expect("limit-binding manifest should parse")
}

fn mcp_table_with_cursor_pagination_manifest() -> coral_spec::ValidatedSourceManifest {
    coral_spec::parse_source_manifest_value(json!({
        "dsl_version": 3,
        "name": "test_mcp",
        "version": "0.1.0",
        "backend": "mcp",
        "server": { "transport": "stdio", "command": "unused" },
        "tables": [{
            "name": "issues",
            "description": "issues with cursor pagination",
            "tool": "list_issues",
            "pagination": {
                "cursor_arg": "cursor",
                "response_cursor_path": ["meta", "nextCursor"],
                "max_pages": 3
            },
            "response": { "rows_path": ["issues"] },
            "columns": [
                { "name": "id", "type": "Utf8" },
                { "name": "title", "type": "Utf8" },
                { "name": "state", "type": "Utf8" }
            ]
        }]
    }))
    .expect("pagination manifest should parse")
}

fn mcp_server_env_manifest() -> coral_spec::ValidatedSourceManifest {
    coral_spec::parse_source_manifest_value(json!({
        "dsl_version": 3,
        "name": "test_mcp",
        "version": "0.1.0",
        "backend": "mcp",
        "inputs": {
            "API_TOKEN": { "kind": "secret" }
        },
        "server": {
            "transport": "stdio",
            "command": "unused",
            "env": [{
                "name": "TOKEN",
                "from": "input",
                "key": "API_TOKEN"
            }]
        },
        "tables": [{
            "name": "issues",
            "description": "issues",
            "tool": "list_issues",
            "response": { "rows_path": ["issues"] },
            "columns": [
                { "name": "id", "type": "Utf8" }
            ]
        }]
    }))
    .expect("server env manifest should parse")
}

fn mcp_table_with_input_tool_arg_and_cursor_pagination_manifest()
-> coral_spec::ValidatedSourceManifest {
    coral_spec::parse_source_manifest_value(json!({
        "dsl_version": 3,
        "name": "test_mcp",
        "version": "0.1.0",
        "backend": "mcp",
        "inputs": {
            "API_TOKEN": { "kind": "secret" }
        },
        "server": { "transport": "stdio", "command": "unused" },
        "tables": [{
            "name": "issues",
            "description": "issues with cursor pagination",
            "tool": "list_issues",
            "tool_args": {
                "token": { "from": "input", "key": "API_TOKEN" }
            },
            "pagination": {
                "cursor_arg": "cursor",
                "response_cursor_path": ["meta", "nextCursor"],
                "max_pages": 3
            },
            "response": { "rows_path": ["issues"] },
            "columns": [
                { "name": "id", "type": "Utf8" },
                { "name": "title", "type": "Utf8" },
                { "name": "state", "type": "Utf8" }
            ]
        }]
    }))
    .expect("pagination manifest should parse")
}

#[tokio::test]
async fn stdio_env_resolves_source_inputs_for_each_tool_call() {
    let manifest = mcp_server_env_manifest();
    let mcp_manifest = manifest.as_mcp().expect("mcp manifest").clone();
    let variables = BTreeMap::new();
    let secrets = BTreeMap::from([("API_TOKEN".to_string(), "stale-token".to_string())]);
    let resolved_inputs = Arc::new(coral_spec::resolve_inputs(
        &mcp_manifest.declared_inputs,
        &secrets,
        &variables,
    ));
    let resolver_calls = Arc::new(AtomicUsize::new(0));
    let source = QuerySource::new(manifest, variables, secrets);
    let source_inputs = Arc::new(McpSourceInputs::with_resolver(
        resolved_inputs,
        SourceInputResolutionContext::from_query_source(&source),
        Arc::new(RotatingInputResolver {
            calls: Arc::clone(&resolver_calls),
        }),
    ));
    let caller = StdioMcpToolCaller {
        source_name: mcp_manifest.common.name.clone(),
        server: mcp_manifest.server,
        source_inputs,
    };

    let first = caller
        .resolved_server_env()
        .await
        .expect("first env render");
    let second = caller
        .resolved_server_env()
        .await
        .expect("second env render");

    assert_eq!(first, [("TOKEN".to_string(), "fresh-token-1".to_string())]);
    assert_eq!(second, [("TOKEN".to_string(), "fresh-token-2".to_string())]);
    assert_eq!(resolver_calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn limit_binding_pushes_sql_limit_into_tool_arg() {
    let ctx = SessionContext::new();
    let caller = Arc::new(FakeMcpTableCaller {
        calls: Mutex::new(Vec::new()),
    });
    register_test_sources(
        &ctx,
        compile_sources(mcp_table_with_limit_binding_manifest(None), caller.clone()),
    );

    let _ = ctx
        .sql("SELECT id FROM test_mcp.issues LIMIT 2")
        .await
        .expect("limit query should plan")
        .collect()
        .await
        .expect("limit query should execute");

    let calls = caller.calls.lock().expect("calls lock");
    let call = calls.first().expect("one MCP call");
    assert_eq!(call.1.get("page_size"), Some(&Value::from(2u64)));
}

#[tokio::test]
async fn limit_binding_caps_pushdown_value_at_manifest_max() {
    let ctx = SessionContext::new();
    let caller = Arc::new(FakeMcpTableCaller {
        calls: Mutex::new(Vec::new()),
    });
    register_test_sources(
        &ctx,
        compile_sources(
            mcp_table_with_limit_binding_manifest(Some(2)),
            caller.clone(),
        ),
    );

    let batches = ctx
        .sql("SELECT id FROM test_mcp.issues LIMIT 1000")
        .await
        .expect("limit query should plan")
        .collect()
        .await
        .expect("limit query should execute");

    let calls = caller.calls.lock().expect("calls lock");
    let call = calls.first().expect("one MCP call");
    assert_eq!(
        call.1.get("page_size"),
        Some(&Value::from(2u64)),
        "expected pushed page_size to be capped at manifest max"
    );

    let total_rows: usize = batches
        .iter()
        .map(datafusion::arrow::array::RecordBatch::num_rows)
        .sum();
    assert_eq!(
        total_rows, 3,
        "limit_binding.max must not cap the final row count — only the pushdown value"
    );
}

#[tokio::test]
async fn limit_binding_omits_arg_when_no_limit_set() {
    let ctx = SessionContext::new();
    let caller = Arc::new(FakeMcpTableCaller {
        calls: Mutex::new(Vec::new()),
    });
    register_test_sources(
        &ctx,
        compile_sources(mcp_table_with_limit_binding_manifest(None), caller.clone()),
    );

    let _ = ctx
        .sql("SELECT id FROM test_mcp.issues")
        .await
        .expect("query should plan")
        .collect()
        .await
        .expect("query should execute");

    let calls = caller.calls.lock().expect("calls lock");
    let call = calls.first().expect("one MCP call");
    assert!(
        call.1.get("page_size").is_none(),
        "unbounded scan should not pass page_size: {:?}",
        call.1
    );
}

#[tokio::test]
async fn mcp_table_tool_args_resolve_source_inputs_for_each_tool_call() {
    let ctx = SessionContext::new();
    let caller = Arc::new(FakePaginatedMcpTableCaller {
        calls: Mutex::new(Vec::new()),
    });
    let resolver_calls = Arc::new(AtomicUsize::new(0));
    register_test_sources(
        &ctx,
        compile_sources_with_inputs(
            mcp_table_with_input_tool_arg_and_cursor_pagination_manifest(),
            caller.clone(),
            BTreeMap::from([("API_TOKEN".to_string(), "stale-token".to_string())]),
            Some(Arc::new(RotatingInputResolver {
                calls: Arc::clone(&resolver_calls),
            })),
        ),
    );

    let _ = ctx
        .sql("SELECT id FROM test_mcp.issues ORDER BY id")
        .await
        .expect("pagination query should plan")
        .collect()
        .await
        .expect("pagination query should execute");

    let calls = caller.calls.lock().expect("calls lock");
    assert_eq!(calls.len(), 2);
    let first_call = calls.first().expect("first call");
    let second_call = calls.get(1).expect("second call");
    assert_eq!(
        first_call.1.get("token"),
        Some(&Value::String("fresh-token-1".to_string()))
    );
    assert_eq!(
        second_call.1.get("token"),
        Some(&Value::String("fresh-token-2".to_string()))
    );
    assert_eq!(resolver_calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn cursor_pagination_fetches_until_response_cursor_is_absent() {
    let ctx = SessionContext::new();
    let caller = Arc::new(FakePaginatedMcpTableCaller {
        calls: Mutex::new(Vec::new()),
    });
    register_test_sources(
        &ctx,
        compile_sources(mcp_table_with_cursor_pagination_manifest(), caller.clone()),
    );

    let batches = ctx
        .sql("SELECT id FROM test_mcp.issues ORDER BY id")
        .await
        .expect("pagination query should plan")
        .collect()
        .await
        .expect("pagination query should execute");

    let rendered = pretty_format_batches(&batches)
        .expect("batches should render")
        .to_string();
    assert!(rendered.contains("| 1"));
    assert!(rendered.contains("| 2"));
    assert!(rendered.contains("| 3"));

    let calls = caller.calls.lock().expect("calls lock");
    assert_eq!(calls.len(), 2);
    let first_call = calls.first().expect("first call");
    let second_call = calls.get(1).expect("second call");
    assert!(first_call.1.get("cursor").is_none());
    assert_eq!(
        second_call.1.get("cursor"),
        Some(&Value::String("page-2".to_string()))
    );
}

#[tokio::test]
async fn cursor_pagination_stops_when_sql_limit_is_satisfied() {
    let ctx = SessionContext::new();
    let caller = Arc::new(FakePaginatedMcpTableCaller {
        calls: Mutex::new(Vec::new()),
    });
    register_test_sources(
        &ctx,
        compile_sources(mcp_table_with_cursor_pagination_manifest(), caller.clone()),
    );

    let batches = ctx
        .sql("SELECT id FROM test_mcp.issues LIMIT 1")
        .await
        .expect("pagination query should plan")
        .collect()
        .await
        .expect("pagination query should execute");

    let total_rows: usize = batches
        .iter()
        .map(datafusion::arrow::array::RecordBatch::num_rows)
        .sum();
    assert_eq!(total_rows, 1);

    let calls = caller.calls.lock().expect("calls lock");
    assert_eq!(calls.len(), 1);
}

fn mcp_table_with_pagination_and_limit_binding_manifest() -> coral_spec::ValidatedSourceManifest {
    coral_spec::parse_source_manifest_value(json!({
        "dsl_version": 3,
        "name": "test_mcp",
        "version": "0.1.0",
        "backend": "mcp",
        "server": { "transport": "stdio", "command": "unused" },
        "tables": [{
            "name": "issues",
            "description": "paginated issues with a per-page cap",
            "tool": "list_issues",
            "limit_binding": { "tool_arg": "page_size", "max": 1 },
            "pagination": {
                "cursor_arg": "cursor",
                "response_cursor_path": ["meta", "nextCursor"],
                "max_pages": 5,
            },
            "response": { "rows_path": ["issues"] },
            "columns": [
                { "name": "id", "type": "Utf8" },
                { "name": "title", "type": "Utf8" },
                { "name": "state", "type": "Utf8" },
            ],
        }],
    }))
    .expect("paginated limit-binding manifest should parse")
}

#[tokio::test]
async fn limit_binding_max_does_not_cap_final_rows_in_paginated_table() {
    let ctx = SessionContext::new();
    let caller = Arc::new(FakePaginatedMcpTableCaller {
        calls: Mutex::new(Vec::new()),
    });
    register_test_sources(
        &ctx,
        compile_sources(
            mcp_table_with_pagination_and_limit_binding_manifest(),
            caller.clone(),
        ),
    );

    let batches = ctx
        .sql("SELECT id FROM test_mcp.issues ORDER BY id LIMIT 3")
        .await
        .expect("paginated limit query should plan")
        .collect()
        .await
        .expect("paginated limit query should execute");

    let total_rows: usize = batches
        .iter()
        .map(datafusion::arrow::array::RecordBatch::num_rows)
        .sum();
    assert_eq!(
        total_rows, 3,
        "rows past limit_binding.max must remain reachable via pagination"
    );

    let calls = caller.calls.lock().expect("calls lock");
    assert_eq!(calls.len(), 2, "must paginate past max to reach later rows");
    let first_call = calls.first().expect("first call");
    let second_call = calls.get(1).expect("second call");
    assert_eq!(
        first_call.1.get("page_size"),
        Some(&Value::from(1u64)),
        "page_size on first call must be capped at limit_binding.max"
    );
    assert_eq!(
        second_call.1.get("page_size"),
        Some(&Value::from(1u64)),
        "page_size on subsequent pages must stay at limit_binding.max"
    );
}

#[tokio::test]
async fn mcp_table_appears_in_catalog_metadata() {
    let ctx = SessionContext::new();
    let caller = Arc::new(FakeMcpTableCaller {
        calls: Mutex::new(Vec::new()),
    });
    register_test_sources_with_catalog(&ctx, compile_sources(mcp_table_manifest(), caller));

    let batches = ctx
        .sql(
            "SELECT column_name FROM coral.columns \
             WHERE schema_name = 'test_mcp' AND table_name = 'issues' \
             ORDER BY column_name",
        )
        .await
        .expect("metadata query should plan")
        .collect()
        .await
        .expect("metadata query should execute");

    let rendered = pretty_format_batches(&batches)
        .expect("batches should render")
        .to_string();
    assert!(rendered.contains("| id"));
    assert!(rendered.contains("| title"));
    assert!(rendered.contains("| state"));
}
