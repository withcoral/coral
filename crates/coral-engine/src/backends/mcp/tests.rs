use super::*;
use crate::runtime::catalog;
use crate::runtime::registry::{CompiledQuerySource, register_sources_blocking};
use crate::runtime::source_functions::SourceFunctionRegistry;
use crate::{
    QuerySource, SourceInputResolutionContext, SourceInputResolver, SourceInputResolverError,
};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::error::DataFusionError;
use datafusion::prelude::SessionContext;
use rmcp::model::JsonObject;
use serde_json::Value;
use serde_json::json;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug)]
enum RecordingMcpResponse {
    SearchItems,
    PaginatedIssues,
    Payload(Value),
}

#[derive(Debug)]
struct RecordingMcpCaller {
    calls: Mutex<Vec<RecordedToolCall>>,
    response: RecordingMcpResponse,
}

#[derive(Clone, Debug)]
struct RecordedToolCall {
    tool: String,
    arguments: JsonObject,
}

impl RecordingMcpCaller {
    fn search() -> Arc<Self> {
        Self::new(RecordingMcpResponse::SearchItems)
    }

    fn table() -> Arc<Self> {
        Self::payload(issue_table_payload())
    }

    fn paginated_table() -> Arc<Self> {
        Self::new(RecordingMcpResponse::PaginatedIssues)
    }

    fn payload(payload: Value) -> Arc<Self> {
        Self::new(RecordingMcpResponse::Payload(payload))
    }

    fn new(response: RecordingMcpResponse) -> Arc<Self> {
        Arc::new(Self {
            calls: Mutex::new(Vec::new()),
            response,
        })
    }

    fn recorded_calls(&self) -> Vec<RecordedToolCall> {
        self.calls.lock().expect("calls lock").clone()
    }

    fn only_call(&self) -> RecordedToolCall {
        let calls = self.recorded_calls();
        assert_eq!(calls.len(), 1);
        calls.into_iter().next().expect("one MCP call")
    }

    fn two_calls(&self) -> (RecordedToolCall, RecordedToolCall) {
        let calls = self.recorded_calls();
        assert_eq!(calls.len(), 2);
        let mut calls = calls.into_iter();
        (
            calls.next().expect("first MCP call"),
            calls.next().expect("second MCP call"),
        )
    }

    fn assert_no_calls(&self, reason: &str) {
        let calls = self.recorded_calls();
        assert!(calls.is_empty(), "{reason}: {calls:?}");
    }
}

impl RecordedToolCall {
    fn assert_tool(&self, expected: &str) {
        assert_eq!(self.tool, expected);
    }

    fn assert_arg(&self, name: &str, expected: impl Into<Value>) {
        let expected = expected.into();
        assert_eq!(self.arguments.get(name), Some(&expected));
    }

    fn assert_no_arg(&self, name: &str, reason: &str) {
        assert!(
            self.arguments.get(name).is_none(),
            "{reason}: {:?}",
            self.arguments
        );
    }
}

#[async_trait]
impl McpToolCaller for RecordingMcpCaller {
    async fn call_tool(
        &self,
        _relation: &str,
        tool_name: &str,
        arguments: JsonObject,
    ) -> Result<Value> {
        self.calls
            .lock()
            .expect("calls lock")
            .push(RecordedToolCall {
                tool: tool_name.to_string(),
                arguments: arguments.clone(),
            });
        Ok(match &self.response {
            RecordingMcpResponse::SearchItems => {
                let query = arguments
                    .get("query")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                json!({
                    "items": [
                        { "title": format!("{query} one"), "url": "https://example.com/1" },
                        { "title": format!("{query} two"), "url": "https://example.com/2" }
                    ]
                })
            }
            RecordingMcpResponse::PaginatedIssues => {
                match arguments.get("cursor").and_then(Value::as_str) {
                    None => json!({
                        "issues": [
                            { "id": "1", "title": "Bug A", "state": "open" }
                        ],
                        "meta": { "nextCursor": "page-2" }
                    }),
                    Some("page-2") => json!({
                    "issues": [
                        { "id": "2", "title": "Bug B", "state": "open" },
                        { "id": "3", "title": "Bug C", "state": "closed" }
                    ],
                    "meta": {}
                    }),
                    Some(other) => panic!("unexpected cursor: {other}"),
                }
            }
            RecordingMcpResponse::Payload(payload) => payload.clone(),
        })
    }
}

fn issue_table_payload() -> Value {
    json!({
        "issues": [
            { "id": "1", "title": "Bug A", "state": "open" },
            { "id": "2", "title": "Bug B", "state": "open" },
            { "id": "3", "title": "Bug C", "state": "closed" }
        ]
    })
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

fn rotating_input_resolver() -> (
    BTreeMap<String, String>,
    Arc<AtomicUsize>,
    Arc<dyn SourceInputResolver>,
) {
    let calls = Arc::new(AtomicUsize::new(0));
    (
        BTreeMap::from([("API_TOKEN".to_string(), "stale-token".to_string())]),
        Arc::clone(&calls),
        Arc::new(RotatingInputResolver { calls }),
    )
}

fn parse_mcp_manifest(body: Value, expectation: &str) -> coral_spec::ValidatedSourceManifest {
    let Value::Object(mut manifest) = body else {
        panic!("MCP test manifest fixture must be an object");
    };
    for (key, value) in [
        ("dsl_version", json!(3)),
        ("name", json!("test_mcp")),
        ("version", json!("0.1.0")),
        ("backend", json!("mcp")),
    ] {
        manifest.entry(key.to_string()).or_insert(value);
    }
    manifest
        .entry("server".to_string())
        .or_insert_with(mcp_stdio_server);
    coral_spec::parse_source_manifest_value(Value::Object(manifest)).expect(expectation)
}

fn required_arg(name: &str) -> Value {
    json!({
        "name": name,
        "required": true,
        "bind": { "arg": name }
    })
}

fn mcp_function_manifest(
    name: &str,
    tool: &str,
    args: &[Value],
    expectation: &str,
) -> coral_spec::ValidatedSourceManifest {
    parse_mcp_manifest(
        json!({
            "functions": [{
                "name": name,
                "tool": tool,
                "args": args,
                "response": {
                    "rows_path": ["items"]
                },
                "columns": [
                    { "name": "title", "type": "Utf8" },
                    { "name": "url", "type": "Utf8" }
                ]
            }]
        }),
        expectation,
    )
}

fn mcp_manifest() -> coral_spec::ValidatedSourceManifest {
    mcp_function_manifest(
        "search",
        "search_tool",
        &[required_arg("query")],
        "mcp manifest should parse",
    )
}

fn mcp_typed_args_manifest() -> coral_spec::ValidatedSourceManifest {
    mcp_function_manifest(
        "typed_search",
        "typed_search_tool",
        &[
            required_arg("query"),
            required_arg("limit"),
            required_arg("include_archived"),
            required_arg("threshold"),
        ],
        "mcp typed args manifest should parse",
    )
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

fn compile_sources_with_rotating_resolver(
    manifest: coral_spec::ValidatedSourceManifest,
    caller: Arc<dyn McpToolCaller>,
) -> (Vec<CompiledQuerySource>, Arc<AtomicUsize>) {
    let (secrets, resolver_calls, resolver) = rotating_input_resolver();
    (
        compile_sources_with_inputs(manifest, caller, secrets, Some(resolver)),
        resolver_calls,
    )
}

fn mcp_source_inputs_with_rotating_resolver(
    manifest: coral_spec::ValidatedSourceManifest,
) -> (
    coral_spec::McpSourceManifest,
    Arc<McpSourceInputs>,
    Arc<AtomicUsize>,
) {
    let mcp_manifest = manifest.as_mcp().expect("mcp manifest").clone();
    let variables = BTreeMap::new();
    let (secrets, resolver_calls, resolver) = rotating_input_resolver();
    let source = QuerySource::new(manifest, variables, secrets);
    let source_input_resolution = SourceInputResolutionContext::from_query_source(&source);
    let resolved_inputs = Arc::new(coral_spec::resolve_inputs(
        &mcp_manifest.declared_inputs,
        source_input_resolution.secrets(),
        source_input_resolution.variables(),
    ));
    let source_inputs = Arc::new(McpSourceInputs::with_resolver(
        resolved_inputs,
        source_input_resolution,
        resolver,
    ));
    (mcp_manifest, source_inputs, resolver_calls)
}

fn registered_context(
    manifest: coral_spec::ValidatedSourceManifest,
    caller: Arc<dyn McpToolCaller>,
) -> SessionContext {
    registered_context_with_catalog_enabled(manifest, caller, false)
}

fn registered_context_with_catalog(
    manifest: coral_spec::ValidatedSourceManifest,
    caller: Arc<dyn McpToolCaller>,
) -> SessionContext {
    registered_context_with_catalog_enabled(manifest, caller, true)
}

fn registered_context_with_catalog_enabled(
    manifest: coral_spec::ValidatedSourceManifest,
    caller: Arc<dyn McpToolCaller>,
    include_catalog: bool,
) -> SessionContext {
    let ctx = SessionContext::new();
    register_test_sources_with_catalog_enabled(
        &ctx,
        compile_sources(manifest, caller),
        include_catalog,
    );
    ctx
}

fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}

async fn collect_query(ctx: &SessionContext, sql: &str) -> Vec<RecordBatch> {
    ctx.sql(sql)
        .await
        .expect("query should plan")
        .collect()
        .await
        .expect("query should execute")
}

async fn collect_query_error(
    ctx: &SessionContext,
    sql: &str,
    expectation: &str,
) -> DataFusionError {
    ctx.sql(sql)
        .await
        .expect("planning succeeds before scan")
        .collect()
        .await
        .expect_err(expectation)
}

async fn render_query(ctx: &SessionContext, sql: &str) -> String {
    pretty_format_batches(&collect_query(ctx, sql).await)
        .expect("batches should render")
        .to_string()
}

async fn assert_query_contains(ctx: &SessionContext, sql: &str, expected: &[&str]) {
    assert_rendered_contains(&render_query(ctx, sql).await, expected);
}

fn assert_rendered_contains(rendered: &str, expected: &[&str]) {
    for value in expected {
        assert!(rendered.contains(value), "rendered rows: {rendered}");
    }
}

fn mcp_provider_error(error: &DataFusionError) -> &McpProviderQueryError {
    match error.find_root() {
        DataFusionError::External(inner) => inner
            .downcast_ref::<McpProviderQueryError>()
            .expect("error should downcast to McpProviderQueryError"),
        other => panic!("expected External error, got {other:?}"),
    }
}

#[tokio::test]
async fn executes_mcp_table_function_with_bound_args() {
    let caller = RecordingMcpCaller::search();
    let ctx = registered_context(mcp_manifest(), caller.clone());

    assert_query_contains(
        &ctx,
        "SELECT title, url FROM test_mcp.search(query => 'issue') ORDER BY title",
        &["| issue one", "| issue two"],
    )
    .await;

    let call = caller.only_call();
    call.assert_tool("search_tool");
    call.assert_arg("query", json!("issue"));
}

#[tokio::test]
async fn mcp_table_function_preserves_json_scalar_arg_types() {
    let caller = RecordingMcpCaller::search();
    let ctx = registered_context(mcp_typed_args_manifest(), caller.clone());

    collect_query(
        &ctx,
        "SELECT title FROM test_mcp.typed_search(\
         query => 'issue', \
         limit => 10, \
         include_archived => true, \
         threshold => 0.75)",
    )
    .await;

    let call = caller.only_call();
    call.assert_tool("typed_search_tool");
    for (name, expected) in [
        ("query", json!("issue")),
        ("limit", json!(10)),
        ("include_archived", json!(true)),
        ("threshold", json!(0.75)),
    ] {
        call.assert_arg(name, expected);
    }
}

#[tokio::test]
async fn missing_required_function_arg_fails_planning() {
    let caller = RecordingMcpCaller::search();
    let ctx = registered_context(mcp_manifest(), caller);

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

fn register_test_sources_with_catalog_enabled(
    ctx: &SessionContext,
    sources: Vec<CompiledQuerySource>,
    include_catalog: bool,
) {
    let registration = register_sources_blocking(ctx, sources).expect("mcp source should register");
    if include_catalog {
        catalog::register(ctx, &registration.active_sources).expect("catalog should register");
    }
    let source_functions = SourceFunctionRegistry::new(
        registration
            .active_sources
            .iter()
            .flat_map(|source| source.table_functions.iter()),
    );
    ctx.register_relation_planner(Arc::new(source_functions))
        .expect("source function planner should register");
}

fn mcp_issues_table(fields: Value) -> Value {
    let Value::Object(mut table) = json!({
        "name": "issues",
        "description": "Open issues",
        "tool": "list_issues",
        "response": { "rows_path": ["issues"] },
        "columns": issue_columns(),
    }) else {
        unreachable!("base table fixture is an object");
    };
    let Value::Object(fields) = fields else {
        unreachable!("table fixture overrides must be an object");
    };
    table.extend(fields);
    Value::Object(table)
}

fn parse_mcp_issues_table(fields: Value, expectation: &str) -> coral_spec::ValidatedSourceManifest {
    parse_mcp_manifest(json!({ "tables": [mcp_issues_table(fields)] }), expectation)
}

fn mcp_stdio_server() -> Value {
    json!({ "transport": "stdio", "command": "unused" })
}

fn issue_columns() -> Value {
    json!([
        { "name": "id", "type": "Utf8" },
        { "name": "title", "type": "Utf8" },
        { "name": "state", "type": "Utf8" },
    ])
}

fn id_title_columns() -> Value {
    json!([
        { "name": "id", "type": "Utf8" },
        { "name": "title", "type": "Utf8" },
    ])
}

fn mcp_table_manifest() -> coral_spec::ValidatedSourceManifest {
    parse_mcp_issues_table(
        json!({
            "tool_args": {
                "owner": { "from": "literal", "value": "acme" }
            },
            "filters": [{
                "name": "state",
                "required": false,
                "tool_arg": "state"
            }]
        }),
        "mcp table manifest should parse",
    )
}

fn mcp_table_required_filter_manifest() -> coral_spec::ValidatedSourceManifest {
    parse_mcp_issues_table(
        json!({
            "filters": [{
                "name": "state",
                "required": true,
                "tool_arg": "state"
            }]
        }),
        "required-filter manifest should parse",
    )
}

#[tokio::test]
async fn scans_mcp_table_with_manifest_tool_args_and_no_filters() {
    let caller = RecordingMcpCaller::table();
    let ctx = registered_context(mcp_table_manifest(), caller.clone());

    assert_query_contains(
        &ctx,
        "SELECT id, title, state FROM test_mcp.issues ORDER BY id",
        &["| Bug A", "| Bug B", "| Bug C"],
    )
    .await;

    let call = caller.only_call();
    call.assert_tool("list_issues");
    call.assert_arg("owner", json!("acme"));
    call.assert_no_arg("state", "unbound optional filter should not be passed");
}

fn mcp_typed_filters_manifest() -> coral_spec::ValidatedSourceManifest {
    parse_mcp_issues_table(
        json!({
            "description": "Issues filtered by typed scalar values",
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
            ]
        }),
        "typed-filter manifest should parse",
    )
}

#[tokio::test]
async fn pushes_typed_filter_values_with_declared_json_scalar_types() {
    let caller = RecordingMcpCaller::table();
    let ctx = registered_context(mcp_typed_filters_manifest(), caller.clone());

    collect_query(
        &ctx,
        "SELECT id FROM test_mcp.issues \
         WHERE \"limit\" = 10 \
         AND include_archived = true \
         AND threshold = 0.75 \
         AND state = 'open' \
         AND metadata = '{\"tag\":\"alpha\",\"ids\":[1,2]}'",
    )
    .await;

    let call = caller.only_call();
    for (name, expected) in [
        ("limit", json!(10)),
        ("include_archived", json!(true)),
        ("threshold", json!(0.75)),
        ("state", json!("open")),
        ("metadata", json!({ "tag": "alpha", "ids": [1, 2] })),
    ] {
        call.assert_arg(name, expected);
    }
}

#[tokio::test]
async fn pushes_equality_filter_into_mcp_tool_arg() {
    let caller = RecordingMcpCaller::table();
    let ctx = registered_context(mcp_table_manifest(), caller.clone());

    collect_query(&ctx, "SELECT id FROM test_mcp.issues WHERE state = 'open'").await;

    let call = caller.only_call();
    call.assert_arg("state", json!("open"));
}

#[tokio::test]
async fn missing_required_filter_fails_planning() {
    let caller = RecordingMcpCaller::table();
    let ctx = registered_context(mcp_table_required_filter_manifest(), caller.clone());

    let error = collect_query_error(
        &ctx,
        "SELECT id FROM test_mcp.issues",
        "missing required filter should fail",
    )
    .await;

    let message = error.to_string();
    assert!(
        message.contains("test_mcp.issues table requires a constant equality filter"),
        "unexpected error: {message}"
    );
    assert!(
        message.contains("WHERE state = <constant>"),
        "missing column hint in error: {message}"
    );

    let provider = mcp_provider_error(&error);
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
    assert_eq!(provider.to_structured().reason(), "MISSING_REQUIRED_FILTER");

    caller.assert_no_calls("no MCP call should be made when planning fails");
}

#[tokio::test]
async fn applies_projection_and_limit_to_mcp_table_scan() {
    let caller = RecordingMcpCaller::table();
    let ctx = registered_context(mcp_table_manifest(), caller);

    let batches = collect_query(
        &ctx,
        "SELECT title FROM test_mcp.issues ORDER BY id LIMIT 1",
    )
    .await;

    assert_eq!(total_rows(&batches), 1);
    let schema = batches.first().expect("at least one batch").schema();
    assert_eq!(schema.fields().len(), 1);
    assert_eq!(schema.field(0).name(), "title");
}

fn mcp_table_with_error_path_manifest() -> coral_spec::ValidatedSourceManifest {
    mcp_table_with_response_manifest(
        &json!({
            "rows_path": ["result", "data"],
            "error_path": ["result", "message"],
        }),
        "error-path manifest should parse",
    )
}

fn mcp_table_with_ok_path_manifest() -> coral_spec::ValidatedSourceManifest {
    mcp_table_with_response_manifest(
        &json!({
            "rows_path": ["data"],
            "ok_path": ["ok"],
            "error_path": ["error"],
        }),
        "ok-path manifest should parse",
    )
}

fn mcp_table_with_response_manifest(
    response: &Value,
    expectation: &str,
) -> coral_spec::ValidatedSourceManifest {
    parse_mcp_issues_table(
        json!({
            "response": response,
            "columns": id_title_columns(),
        }),
        expectation,
    )
}

async fn assert_mcp_tool_returned_error(
    manifest: coral_spec::ValidatedSourceManifest,
    payload: Value,
    expectation: &str,
    expected_detail: &str,
) {
    let caller = RecordingMcpCaller::payload(payload);
    let ctx = registered_context(manifest, caller);
    let error = collect_query_error(&ctx, "SELECT id FROM test_mcp.issues", expectation).await;
    let provider = mcp_provider_error(&error);
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
            assert_eq!(detail, expected_detail);
        }
        other => panic!("unexpected MCP error variant: {other:?}"),
    }
    assert_eq!(provider.to_structured().reason(), "MCP_TOOL_RETURNED_ERROR");
}

async fn assert_mcp_payload_rows(
    manifest: coral_spec::ValidatedSourceManifest,
    payload: Value,
    expected_rows: &[&str],
) {
    let caller = RecordingMcpCaller::payload(payload);
    let ctx = registered_context(manifest, caller);
    let rendered = render_query(&ctx, "SELECT id, title FROM test_mcp.issues ORDER BY id").await;
    assert_rendered_contains(&rendered, expected_rows);
}

async fn collect_limit_binding_query(
    max: Option<usize>,
    sql: &str,
) -> (Vec<RecordBatch>, RecordedToolCall) {
    let caller = RecordingMcpCaller::table();
    let ctx = registered_context(mcp_table_with_limit_binding_manifest(max), caller.clone());
    let batches = collect_query(&ctx, sql).await;
    (batches, caller.only_call())
}

async fn collect_paginated_query(
    manifest: coral_spec::ValidatedSourceManifest,
    sql: &str,
) -> (Vec<RecordBatch>, Arc<RecordingMcpCaller>) {
    let caller = RecordingMcpCaller::paginated_table();
    let ctx = registered_context(manifest, caller.clone());
    let batches = collect_query(&ctx, sql).await;
    (batches, caller)
}

#[tokio::test]
async fn response_path_cases() {
    for (name, manifest, payload, expected_error, expected_rows) in [
        (
            "error_path surfaces tool error",
            mcp_table_with_error_path_manifest(),
            json!({
                "result": { "status": "error", "message": "Code: 62. Syntax error" }
            }),
            Some((
                "error payload should surface as engine error",
                "Code: 62. Syntax error",
            )),
            None,
        ),
        (
            "ok_path true skips error_path",
            mcp_table_with_ok_path_manifest(),
            json!({
                "ok": true,
                "error": "",
                "data": [
                    { "id": "1", "title": "Bug A" },
                    { "id": "2", "title": "Bug B" }
                ]
            }),
            None,
            Some(&["| Bug A", "| Bug B"][..]),
        ),
        (
            "ok_path false surfaces error_path detail",
            mcp_table_with_ok_path_manifest(),
            json!({
                "ok": false,
                "error": "rate_limited",
                "data": []
            }),
            Some((
                "ok_path=false should surface as engine error",
                "rate_limited",
            )),
            None,
        ),
        (
            "error_path ignores success payload",
            mcp_table_with_error_path_manifest(),
            json!({
                "result": {
                    "data": [
                        { "id": "1", "title": "Bug A" },
                        { "id": "2", "title": "Bug B" }
                    ]
                }
            }),
            None,
            Some(&["| Bug A", "| Bug B"][..]),
        ),
    ] {
        if let Some((expectation, message)) = expected_error {
            assert_mcp_tool_returned_error(manifest, payload, expectation, message).await;
        } else {
            assert_mcp_payload_rows(manifest, payload, expected_rows.expect(name)).await;
        }
    }
}

fn mcp_table_with_limit_binding_manifest(
    max: Option<usize>,
) -> coral_spec::ValidatedSourceManifest {
    let binding = max.map_or_else(
        || json!({ "tool_arg": "page_size" }),
        |max| json!({ "tool_arg": "page_size", "max": max }),
    );
    parse_mcp_issues_table(
        json!({
            "description": "issues with limit binding",
            "limit_binding": binding,
        }),
        "limit-binding manifest should parse",
    )
}

fn mcp_table_with_cursor_pagination_manifest() -> coral_spec::ValidatedSourceManifest {
    parse_mcp_issues_table(
        json!({
            "description": "issues with cursor pagination",
            "pagination": {
                "cursor_arg": "cursor",
                "response_cursor_path": ["meta", "nextCursor"],
                "max_pages": 3
            }
        }),
        "pagination manifest should parse",
    )
}

fn mcp_server_env_manifest() -> coral_spec::ValidatedSourceManifest {
    parse_mcp_manifest(
        json!({
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
            "tables": [mcp_issues_table(json!({
                "description": "issues",
                "columns": [
                    { "name": "id", "type": "Utf8" }
                ]
            }))]
        }),
        "server env manifest should parse",
    )
}

fn mcp_table_with_input_tool_arg_and_cursor_pagination_manifest()
-> coral_spec::ValidatedSourceManifest {
    parse_mcp_manifest(
        json!({
            "inputs": {
                "API_TOKEN": { "kind": "secret" }
            },
            "tables": [mcp_issues_table(json!({
                "description": "issues with cursor pagination",
                "tool_args": {
                    "token": { "from": "input", "key": "API_TOKEN" }
                },
                "pagination": {
                    "cursor_arg": "cursor",
                    "response_cursor_path": ["meta", "nextCursor"],
                    "max_pages": 3
                }
            }))]
        }),
        "pagination manifest should parse",
    )
}

#[tokio::test]
async fn stdio_env_resolves_source_inputs_for_each_tool_call() {
    let manifest = mcp_server_env_manifest();
    let (mcp_manifest, source_inputs, resolver_calls) =
        mcp_source_inputs_with_rotating_resolver(manifest);
    let caller = StdioMcpToolCaller {
        source_name: mcp_manifest.common.name.clone(),
        server: mcp_manifest.server,
        source_inputs,
        body_capture: super::trace::McpBodyCapture::default(),
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
async fn limit_binding_pushdown_cases() {
    let (_, call) =
        collect_limit_binding_query(None, "SELECT id FROM test_mcp.issues LIMIT 2").await;
    call.assert_arg("page_size", json!(2u64));

    let (batches, call) =
        collect_limit_binding_query(Some(2), "SELECT id FROM test_mcp.issues LIMIT 1000").await;
    call.assert_arg("page_size", json!(2u64));
    assert_eq!(
        total_rows(&batches),
        3,
        "limit_binding.max must not cap the final row count — only the pushdown value"
    );

    let (_, call) = collect_limit_binding_query(None, "SELECT id FROM test_mcp.issues").await;
    call.assert_no_arg("page_size", "unbounded scan should not pass page_size");
}

#[tokio::test]
async fn mcp_table_tool_args_resolve_source_inputs_for_each_tool_call() {
    let ctx = SessionContext::new();
    let caller = RecordingMcpCaller::paginated_table();
    let (sources, resolver_calls) = compile_sources_with_rotating_resolver(
        mcp_table_with_input_tool_arg_and_cursor_pagination_manifest(),
        caller.clone(),
    );
    register_test_sources_with_catalog_enabled(&ctx, sources, false);

    collect_query(&ctx, "SELECT id FROM test_mcp.issues ORDER BY id").await;

    let (first_call, second_call) = caller.two_calls();
    first_call.assert_arg("token", json!("fresh-token-1"));
    second_call.assert_arg("token", json!("fresh-token-2"));
    assert_eq!(resolver_calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn cursor_pagination_fetches_until_response_cursor_is_absent() {
    let (batches, caller) = collect_paginated_query(
        mcp_table_with_cursor_pagination_manifest(),
        "SELECT id FROM test_mcp.issues ORDER BY id",
    )
    .await;
    let rendered = pretty_format_batches(&batches)
        .expect("batches should render")
        .to_string();
    assert_rendered_contains(&rendered, &["| 1", "| 2", "| 3"]);

    let (first_call, second_call) = caller.two_calls();
    first_call.assert_no_arg("cursor", "first page should omit cursor");
    second_call.assert_arg("cursor", json!("page-2"));
}

#[tokio::test]
async fn cursor_pagination_stops_when_sql_limit_is_satisfied() {
    let (batches, caller) = collect_paginated_query(
        mcp_table_with_cursor_pagination_manifest(),
        "SELECT id FROM test_mcp.issues LIMIT 1",
    )
    .await;

    assert_eq!(total_rows(&batches), 1);

    let calls = caller.recorded_calls();
    assert_eq!(calls.len(), 1);
}

fn mcp_table_with_pagination_and_limit_binding_manifest() -> coral_spec::ValidatedSourceManifest {
    parse_mcp_issues_table(
        json!({
            "description": "paginated issues with a per-page cap",
            "limit_binding": { "tool_arg": "page_size", "max": 1 },
            "pagination": {
                "cursor_arg": "cursor",
                "response_cursor_path": ["meta", "nextCursor"],
                "max_pages": 5,
            }
        }),
        "paginated limit-binding manifest should parse",
    )
}

#[tokio::test]
async fn limit_binding_max_does_not_cap_final_rows_in_paginated_table() {
    let (batches, caller) = collect_paginated_query(
        mcp_table_with_pagination_and_limit_binding_manifest(),
        "SELECT id FROM test_mcp.issues ORDER BY id LIMIT 3",
    )
    .await;

    assert_eq!(
        total_rows(&batches),
        3,
        "rows past limit_binding.max must remain reachable via pagination"
    );

    let (first_call, second_call) = caller.two_calls();
    first_call.assert_arg("page_size", json!(1u64));
    second_call.assert_arg("page_size", json!(1u64));
}

#[tokio::test]
async fn mcp_table_appears_in_catalog_metadata() {
    let caller = RecordingMcpCaller::table();
    let ctx = registered_context_with_catalog(mcp_table_manifest(), caller);

    assert_query_contains(
        &ctx,
        "SELECT column_name FROM coral.columns \
         WHERE schema_name = 'test_mcp' AND table_name = 'issues' \
         ORDER BY column_name",
        &["| id", "| title", "| state"],
    )
    .await;
}
