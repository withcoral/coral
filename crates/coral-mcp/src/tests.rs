#![allow(
    clippy::indexing_slicing,
    clippy::string_slice,
    reason = "test code: assertion-style indexing is idiomatic in tests"
)]

use std::fs;
use std::path::{Path, PathBuf};

use coral_api::v1::{ImportSourceRequest, import_source_response};
use coral_app::{RunningServer, ServerBuilder};
use coral_client::{AppClient, SourceClient, default_workspace};
use rmcp::{RoleClient, ServiceExt, model::CallToolRequestParams, service::RunningService};
use serde_json::{Map, Value, json};
use tempfile::TempDir;
use tonic::Request;

use crate::McpRuntimeExposure;
use crate::{CoralMcpServer, McpOptions};

fn write_fixture_manifest(root: &Path) -> PathBuf {
    let source_dir = root.join("fixture-source");
    let data_dir = root.join("fixture-data");
    fs::create_dir_all(&source_dir).expect("create source dir");
    fs::create_dir_all(&data_dir).expect("create data dir");
    fs::write(
        data_dir.join("messages.jsonl"),
        r#"{"type":"user","sessionId":"s1","text":"hello"}
{"type":"assistant","sessionId":"s1","text":"world"}
"#,
    )
    .expect("write jsonl");
    let manifest = format!(
        r#"
name: local_messages
spec_version: 1
kind: source
interfaces:
  - id: events
    type: file
    files: ["{}"]
    format:
      kind: jsonl
  - id: messages
    type: file
    files: ["{}"]
    format:
      kind: jsonl
  - id: sessions
    type: file
    files: ["{}"]
    format:
      kind: jsonl
"#,
        data_dir.join("messages.jsonl").display(),
        data_dir.join("messages.jsonl").display(),
        data_dir.join("messages.jsonl").display()
    );
    let manifest_path = source_dir.join("source.yaml");
    fs::write(&manifest_path, manifest).expect("write manifest");
    manifest_path
}

fn json_object(value: &Value) -> Map<String, Value> {
    value.as_object().cloned().expect("json object")
}

/// Linear-style recursive introspection fixture shared with the importer
/// payload gate in `crates/coral-importers/src/tests.rs`, so importer-level
/// and end-to-end byte budgets pin the same provider shape.
const LINEAR_RECURSIVE_INTROSPECTION_JSON: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../coral-importers/tests/fixtures/linear_recursive_introspection.json"
));

fn write_linear_fixture_manifest(root: &Path) -> PathBuf {
    let source_dir = root.join("linear-fixture-source");
    fs::create_dir_all(&source_dir).expect("create linear fixture dir");
    let introspection_path = source_dir.join("introspection.json");
    fs::write(&introspection_path, LINEAR_RECURSIVE_INTROSPECTION_JSON)
        .expect("write introspection fixture");
    let manifest = format!(
        r#"
name: linear_fixture
spec_version: 1
kind: source
interfaces:
  - id: graph
    type: graphql
    endpoint: https://linear.example.test/graphql
    schema:
      kind: introspection_json_file
      file: "{}"
"#,
        introspection_path.display()
    );
    let manifest_path = source_dir.join("source.yaml");
    fs::write(&manifest_path, manifest).expect("write manifest");
    manifest_path
}

async fn start_linear_fixture_session(temp: &TempDir) -> TestSession {
    let manifest_path = write_linear_fixture_manifest(temp.path());
    let manifest_yaml = fs::read_to_string(&manifest_path).expect("read manifest");
    let mut session = start_session(temp).await;
    add_demo_source(&mut session.source_client, manifest_yaml).await;
    session
}

async fn structured(client: &RunningService<RoleClient, ()>, name: &str, args: Value) -> Value {
    let result = client
        .call_tool(CallToolRequestParams::new(name.to_string()).with_arguments(json_object(&args)))
        .await
        .unwrap_or_else(|error| panic!("{name} call failed: {error}"));
    assert_eq!(result.is_error, Some(false), "{name} returned a tool error");
    result
        .structured_content
        .unwrap_or_else(|| panic!("{name} returned no structured content"))
}

fn assert_exact_keys(value: &Value, expected: &[&str]) {
    let keys = value
        .as_object()
        .unwrap_or_else(|| panic!("expected object, got {value:#}"))
        .keys()
        .map(String::as_str)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        keys,
        expected.iter().copied().collect(),
        "unexpected key set in {value:#}"
    );
}

fn serialized_len(value: &Value) -> usize {
    serde_json::to_string(value).expect("serialize json").len()
}

/// Serializes with recursively sorted object keys, so byte equality means
/// structural equality regardless of field ordering or map backing.
fn canonical_json(value: &Value) -> String {
    fn canonicalize(value: &Value) -> Value {
        match value {
            Value::Object(object) => Value::Object(
                object
                    .iter()
                    .map(|(key, value)| (key.clone(), canonicalize(value)))
                    .collect::<std::collections::BTreeMap<_, _>>()
                    .into_iter()
                    .collect(),
            ),
            Value::Array(values) => Value::Array(values.iter().map(canonicalize).collect()),
            other => other.clone(),
        }
    }
    serde_json::to_string(&canonicalize(value)).expect("serialize canonical json")
}

/// Finds the search item whose generated call path ends with `suffix`.
fn generated_item<'a>(search: &'a Value, suffix: &str) -> &'a Value {
    search["items"]
        .as_array()
        .expect("search items")
        .iter()
        .find(|item| {
            item.get("call")
                .and_then(Value::as_str)
                .is_some_and(|call| call.ends_with(suffix))
        })
        .unwrap_or_else(|| panic!("no search item with call suffix '{suffix}': {search:#}"))
}

async fn add_demo_source(source_client: &mut SourceClient, manifest_yaml: String) {
    let mut stream = source_client
        .import_source(Request::new(ImportSourceRequest {
            workspace: Some(default_workspace()),
            manifest_yaml,
            variables: Vec::new(),
            secrets: Vec::new(),
            oauth_credential_retrievals: Vec::new(),
            interface_ids: Vec::new(),
        }))
        .await
        .expect("add source")
        .into_inner();
    stream
        .message()
        .await
        .expect("add source stream")
        .and_then(|response| match response.event {
            Some(import_source_response::Event::Source(source)) => Some(source),
            _ => None,
        })
        .expect("add source response");
}

struct TestSession {
    source_client: SourceClient,
    client: RunningService<RoleClient, ()>,
    app_server: RunningServer,
    mcp_server_task: tokio::task::JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>>,
}

impl TestSession {
    async fn shutdown(self) {
        let Self {
            client,
            app_server,
            mcp_server_task,
            ..
        } = self;
        client.cancel().await.expect("cancel client");
        mcp_server_task
            .await
            .expect("join mcp task")
            .expect("mcp server result");
        app_server.shutdown().await.expect("shutdown app server");
    }
}

async fn start_session(temp: &TempDir) -> TestSession {
    start_session_with_options(temp, McpOptions::default()).await
}

async fn start_session_with_options(temp: &TempDir, options: McpOptions) -> TestSession {
    let server = ServerBuilder::new()
        .with_config_dir(temp.path().join("coral-config"))
        .with_noop_feedback_uploads()
        .start()
        .await
        .expect("start server");
    let app = AppClient::connect(server.endpoint_uri())
        .await
        .expect("connect client");
    let source_client = app.source_client();

    let (server_transport, client_transport) = tokio::io::duplex(4096);
    let mcp_server_task = tokio::spawn(async move {
        let server = Box::pin(CoralMcpServer::new(&app, options).serve(server_transport)).await?;
        server.waiting().await?;
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    });
    let client = ().serve(client_transport).await.expect("start rmcp client");
    TestSession {
        source_client,
        client,
        app_server: server,
        mcp_server_task,
    }
}

async fn assert_describe_exposes_entry(client: &RunningService<RoleClient, ()>) {
    let search = client
        .call_tool(
            CallToolRequestParams::new("search").with_arguments(json_object(&json!({
                "query": "events",
                "source_key": "local_messages"
            }))),
        )
        .await
        .expect("search demo source");
    let search = search.structured_content.expect("structured search");
    assert!(search["items"][0].get("deprecated").is_none());
    assert!(search["items"][0].get("support").is_none());
    let reference = search["items"][0]["ref"].as_str().expect("reference");
    let describe = client
        .call_tool(
            CallToolRequestParams::new("describe").with_arguments(json_object(&json!({
                "reference": reference
            }))),
        )
        .await
        .expect("describe demo source");
    let describe = describe.structured_content.expect("structured describe");
    assert!(describe.get("status").is_none());
    assert!(describe.get("capability").is_none());
    assert!(describe.get("code_mode_output_schema").is_none());
    assert!(describe.get("output_schema").is_none());
    assert!(describe["input_schema"].is_object());
    assert!(describe["value_schema"].is_object());
    // Small schemas come back whole: no truncation flags and no schema note.
    assert!(describe.get("input_schema_truncated").is_none());
    assert!(describe.get("value_schema_truncated").is_none());
    assert!(describe.get("schema_note").is_none());
    assert!(describe["examples"].is_array());
    assert!(describe.get("deprecated").is_none());
    assert!(describe.get("support").is_none());
    assert!(describe.get("runtime").is_none());
    assert_eq!(describe["ref"], search["items"][0]["ref"]);
    assert_eq!(describe["call"], search["items"][0]["call"]);
    assert!(
        describe["call"]
            .as_str()
            .expect("call path")
            .starts_with("tools.localMessages.events.")
    );

    assert_describe_schema_expansion(client, reference, &describe).await;

    let detailed = client
        .call_tool(
            CallToolRequestParams::new("describe").with_arguments(json_object(&json!({
                "reference": reference,
                "view": "detailed"
            }))),
        )
        .await
        .expect("detailed describe demo source");
    let detailed = detailed
        .structured_content
        .expect("structured detailed describe");
    assert!(detailed["entry"].get("capability").is_some());
    assert!(detailed["entry"].get("code_mode_output_schema").is_some());
    assert_eq!(detailed["runtime"]["exposure"], "both");
}

async fn assert_describe_schema_expansion(
    client: &RunningService<RoleClient, ()>,
    reference: &str,
    describe: &Value,
) {
    let full_schemas = client
        .call_tool(
            CallToolRequestParams::new("describe").with_arguments(json_object(&json!({
                "reference": reference,
                "schemas": "full"
            }))),
        )
        .await
        .expect("describe without renderer bounds");
    let full_schemas = full_schemas
        .structured_content
        .expect("structured full-schema describe");
    assert_eq!(full_schemas["input_schema"], describe["input_schema"]);
    assert_eq!(full_schemas["value_schema"], describe["value_schema"]);

    let drilled = client
        .call_tool(
            CallToolRequestParams::new("describe").with_arguments(json_object(&json!({
                "reference": reference,
                "path": "limit"
            }))),
        )
        .await
        .expect("describe with path");
    let drilled = drilled
        .structured_content
        .expect("structured path describe");
    assert_eq!(drilled["ref"], describe["ref"]);
    assert_eq!(drilled["call"], describe["call"]);
    assert_eq!(drilled["path"], "limit");
    assert_eq!(
        drilled["schema"],
        describe["input_schema"]["properties"]["limit"]
    );
    assert_eq!(drilled["elided"], json!([]));
    assert!(drilled.get("input_schema").is_none());

    let invalid_path = client
        .call_tool(
            CallToolRequestParams::new("describe").with_arguments(json_object(&json!({
                "reference": reference,
                "path": "limit.bogus"
            }))),
        )
        .await
        .expect("describe with invalid path returns tool error");
    assert_eq!(invalid_path.is_error, Some(true));
}

#[tokio::test]
async fn mcp_surface_exposes_discovery_and_code_mode_tools() {
    let temp = TempDir::new().expect("temp dir");
    let session = start_session(&temp).await;
    let client = &session.client;

    let tools = client.list_all_tools().await.expect("tools");
    assert_eq!(
        tools
            .iter()
            .map(|tool| tool.name.as_ref())
            .collect::<Vec<_>>(),
        vec!["search", "describe", "exec", "wait", "feedback"]
    );

    let search = client
        .call_tool(CallToolRequestParams::new("search"))
        .await
        .expect("search");
    assert_eq!(search.is_error, Some(false));
    let structured_search = search
        .structured_content
        .as_ref()
        .expect("structured search");
    assert!(structured_search.get("rows").is_none());
    assert!(structured_search.get("runtime").is_none());
    assert_eq!(structured_search["pagination"]["total"], 0);
    assert_eq!(
        structured_search["items"].as_array().expect("items").len(),
        0
    );

    let describe = client
        .call_tool(
            CallToolRequestParams::new("describe").with_arguments(json_object(&json!({
                "reference": "typescript:missing.capability"
            }))),
        )
        .await
        .expect("describe");
    assert_eq!(describe.is_error, Some(false));
    assert_eq!(
        describe
            .structured_content
            .as_ref()
            .expect("structured describe")["status"],
        "not_found"
    );

    let exec = client
        .call_tool(
            CallToolRequestParams::new("exec").with_arguments(json_object(&json!({
                "source": "return 1;"
            }))),
        )
        .await
        .expect("exec");
    assert_eq!(exec.is_error, Some(false));
    let exec = exec.structured_content.expect("structured exec");
    let run_id = exec["run"]["id"].as_str().expect("run id");
    assert_eq!(exec["run"]["status"], "completed");
    assert!(exec["run"].get("cell_id").is_none());
    assert_eq!(exec["result"], 1);
    assert!(exec.get("output").is_none());
    assert!(exec.get("events").is_none());
    assert!(exec.get("cursor").is_none());
    assert!(exec.get("error").is_none());

    let wait = client
        .call_tool(
            CallToolRequestParams::new("wait").with_arguments(json_object(&json!({
                "run_id": run_id,
                "cursor": 2
            }))),
        )
        .await
        .expect("wait");
    assert_eq!(wait.is_error, Some(false));
    let wait = wait.structured_content.expect("structured wait");
    assert_eq!(wait["run"]["id"], run_id);
    assert_eq!(wait["run"]["status"], "completed");
    // Resuming from cursor 2 re-delivers the result event that arrived later.
    assert_eq!(wait["result"], 1);
    assert!(wait.get("cursor").is_none());

    session.shutdown().await;
}

/// Console text joins into one `output` string in emission order, the result
/// is the script's return value directly, and silent scripts omit `output`
/// entirely.
#[tokio::test]
async fn mcp_exec_joins_console_output_and_returns_the_value_directly() {
    let temp = TempDir::new().expect("temp dir");
    let session = start_session(&temp).await;
    let client = &session.client;

    let exec = client
        .call_tool(
            CallToolRequestParams::new("exec").with_arguments(json_object(&json!({
                "source": "console.log(\"first\"); console.log(\"second\"); return { done: true };"
            }))),
        )
        .await
        .expect("exec");
    assert_eq!(exec.is_error, Some(false));
    let exec = exec.structured_content.expect("structured exec");
    assert_eq!(exec["run"]["status"], "completed");
    assert_eq!(exec["output"], "first\nsecond");
    assert_eq!(exec["result"], json!({ "done": true }));
    assert!(exec.get("events").is_none());
    assert!(exec.get("output_truncated").is_none());

    let silent = client
        .call_tool(
            CallToolRequestParams::new("exec").with_arguments(json_object(&json!({
                "source": "return 7;"
            }))),
        )
        .await
        .expect("silent exec");
    let silent = silent.structured_content.expect("structured silent exec");
    assert_eq!(silent["result"], 7);
    assert!(silent.get("output").is_none());

    session.shutdown().await;
}

/// Oversized results resolve to the truncated preview with a sibling
/// `result_truncated: { original_bytes, estimated_tokens, artifact }` instead
/// of the old status/format/truncated/preview wrapper.
#[tokio::test]
async fn mcp_exec_truncated_result_carries_result_truncated_metadata() {
    let temp = TempDir::new().expect("temp dir");
    let session = start_session(&temp).await;
    let client = &session.client;

    let exec = client
        .call_tool(
            CallToolRequestParams::new("exec").with_arguments(json_object(&json!({
                "source": "// @exec: {\"max_output_tokens\": 10}\nreturn { rows: Array.from({ length: 200 }, (_, i) => `row ${i}`) };"
            }))),
        )
        .await
        .expect("exec");
    assert_eq!(exec.is_error, Some(false));
    let exec = exec.structured_content.expect("structured exec");
    assert_eq!(exec["run"]["status"], "completed");
    assert!(
        exec["result"].is_string(),
        "unparseable truncated previews stay raw text: {:#?}",
        exec["result"]
    );
    let result_truncated = exec["result_truncated"]
        .as_object()
        .expect("result_truncated object");
    assert!(
        result_truncated["original_bytes"]
            .as_u64()
            .expect("original_bytes")
            > 0
    );
    assert!(
        result_truncated["estimated_tokens"]
            .as_u64()
            .expect("estimated_tokens")
            > 10
    );
    assert!(
        result_truncated["artifact"]["path"]
            .as_str()
            .is_some_and(|path| !path.is_empty())
    );
    assert_eq!(
        result_truncated["artifact"]["bytes"],
        result_truncated["original_bytes"]
    );
    assert_eq!(
        result_truncated.keys().collect::<Vec<_>>(),
        vec!["original_bytes", "estimated_tokens", "artifact"]
    );

    session.shutdown().await;
}

#[tokio::test]
async fn mcp_surface_refreshes_after_source_add() {
    let temp = TempDir::new().expect("temp dir");
    let manifest_path = write_fixture_manifest(temp.path());
    let manifest_yaml = fs::read_to_string(&manifest_path).expect("read manifest");
    let mut session = start_session(&temp).await;
    let client = &session.client;

    let initial_tools = client.list_all_tools().await.expect("initial tools");
    assert_eq!(
        initial_tools
            .iter()
            .map(|tool| tool.name.as_ref())
            .collect::<Vec<_>>(),
        vec!["search", "describe", "exec", "wait", "feedback"]
    );
    for tool in &initial_tools {
        let Some(output_schema) = &tool.output_schema else {
            continue;
        };
        assert_eq!(
            output_schema.get("type").and_then(Value::as_str),
            Some("object"),
            "tool '{}' output schema root type should be object",
            tool.name
        );
    }
    add_demo_source(&mut session.source_client, manifest_yaml).await;

    let updated_tools = client.list_all_tools().await.expect("updated tools");
    assert_eq!(
        updated_tools
            .iter()
            .map(|tool| tool.name.as_ref())
            .collect::<Vec<_>>(),
        vec!["search", "describe", "exec", "wait", "feedback"]
    );
    assert_describe_exposes_entry(client).await;

    session.shutdown().await;
}

/// The MCP `describe` compact entry and Code Mode's `coral.describe` entry are
/// rendered by one shared renderer; this guards their parity end to end.
#[tokio::test]
async fn mcp_and_code_mode_compact_describe_entries_match() {
    let temp = TempDir::new().expect("temp dir");
    let manifest_path = write_fixture_manifest(temp.path());
    let manifest_yaml = fs::read_to_string(&manifest_path).expect("read manifest");
    let mut session = start_session(&temp).await;
    add_demo_source(&mut session.source_client, manifest_yaml).await;
    let client = &session.client;

    let search = client
        .call_tool(
            CallToolRequestParams::new("search").with_arguments(json_object(&json!({
                "query": "events",
                "source_key": "local_messages"
            }))),
        )
        .await
        .expect("search demo source");
    let search = search.structured_content.expect("structured search");
    let reference = search["items"][0]["ref"].as_str().expect("reference");

    let mcp_entry = client
        .call_tool(
            CallToolRequestParams::new("describe").with_arguments(json_object(&json!({
                "reference": reference
            }))),
        )
        .await
        .expect("mcp describe")
        .structured_content
        .expect("structured mcp describe");

    let exec = client
        .call_tool(
            CallToolRequestParams::new("exec").with_arguments(json_object(&json!({
                "source": format!(
                    "return await coral.describe({});",
                    json!({ "reference": reference })
                )
            }))),
        )
        .await
        .expect("exec coral.describe");
    let exec = exec.structured_content.expect("structured exec");
    assert_eq!(exec["run"]["status"], "completed");
    let code_mode_describe = &exec["result"];
    assert_eq!(code_mode_describe["status"], "found");
    assert_eq!(
        code_mode_describe["entry"], mcp_entry,
        "MCP and Code Mode compact describe entries must be identical"
    );

    session.shutdown().await;
}

/// Search hits carry one-line call signatures, and the top hit's compact
/// describe entry is inlined as `top` for exact-reference queries or on
/// `expand_top: true` — identical to what `describe` would return.
#[tokio::test]
async fn mcp_search_inlines_signature_and_top_describe_entry() {
    let temp = TempDir::new().expect("temp dir");
    let manifest_path = write_fixture_manifest(temp.path());
    let manifest_yaml = fs::read_to_string(&manifest_path).expect("read manifest");
    let mut session = start_session(&temp).await;
    add_demo_source(&mut session.source_client, manifest_yaml).await;
    let client = &session.client;

    let fuzzy = client
        .call_tool(
            CallToolRequestParams::new("search").with_arguments(json_object(&json!({
                "query": "events",
                "source_key": "local_messages"
            }))),
        )
        .await
        .expect("fuzzy search")
        .structured_content
        .expect("structured fuzzy search");
    let item = &fuzzy["items"][0];
    let call = item["call"].as_str().expect("call path");
    let signature = item["signature"].as_str().expect("signature");
    assert!(
        signature.starts_with(&format!("{call}(")),
        "signature must be the call form of the generated path: {signature}"
    );
    assert!(
        fuzzy.get("top").is_none(),
        "fuzzy searches must not inline a top entry: {fuzzy:#?}"
    );

    let reference = item["ref"].as_str().expect("reference");
    let describe = client
        .call_tool(
            CallToolRequestParams::new("describe").with_arguments(json_object(&json!({
                "reference": reference
            }))),
        )
        .await
        .expect("describe top hit")
        .structured_content
        .expect("structured describe");

    let exact = client
        .call_tool(
            CallToolRequestParams::new("search").with_arguments(json_object(&json!({
                "query": reference
            }))),
        )
        .await
        .expect("exact-ref search")
        .structured_content
        .expect("structured exact search");
    assert_eq!(exact["items"][0]["rank_reason"], "exact typed ref");
    assert_eq!(
        exact["top"], describe,
        "exact-ref searches must inline the compact describe entry"
    );

    let expanded = client
        .call_tool(
            CallToolRequestParams::new("search").with_arguments(json_object(&json!({
                "query": "events",
                "source_key": "local_messages",
                "expand_top": true
            }))),
        )
        .await
        .expect("expand_top search")
        .structured_content
        .expect("structured expand_top search");
    assert_eq!(
        expanded["top"], describe,
        "expand_top must inline the top hit's compact describe entry"
    );

    session.shutdown().await;
}

#[tokio::test]
async fn mcp_removed_sql_tools_are_not_exposed() {
    let temp = TempDir::new().expect("temp dir");
    let session = start_session_with_options(
        &temp,
        McpOptions {
            runtime_exposure: McpRuntimeExposure::typescript_only(),
            ..McpOptions::default()
        },
    )
    .await;
    let client = &session.client;

    let tools = client.list_all_tools().await.expect("tools");
    assert_eq!(
        tools
            .iter()
            .map(|tool| tool.name.as_ref())
            .collect::<Vec<_>>(),
        vec!["search", "describe", "exec", "wait", "feedback"]
    );
    let sql = client
        .call_tool(
            CallToolRequestParams::new("sql").with_arguments(json_object(&json!({
                "sql": "select 1"
            }))),
        )
        .await
        .expect_err("sql tool should not be exposed");
    assert!(sql.to_string().contains("tool 'sql' not found"));

    session.shutdown().await;
}

fn assert_sql_only_search_hides_typescript(search: &Value) -> String {
    assert!(search.get("runtime").is_none());
    let items = search["items"].as_array().expect("search items");
    assert!(
        !items.is_empty(),
        "SQL-only search should return SQL projections"
    );
    for item in items {
        assert!(item.get("call").is_none());
        assert!(
            item.get("signature").is_none(),
            "generated call signature leaked in {item:?}"
        );
        assert!(
            item["ref"].as_str().is_some_and(
                |ref_| ref_.starts_with("sql_table:") || ref_.starts_with("sql_function:")
            ),
            "hidden TypeScript ref leaked in {item:?}"
        );
    }
    items[0]["ref"].as_str().expect("sql ref").to_string()
}

async fn assert_sql_only_describe_hides_typescript(
    client: &RunningService<RoleClient, ()>,
    sql_ref: &str,
) {
    let visible_describe = client
        .call_tool(
            CallToolRequestParams::new("describe").with_arguments(json_object(&json!({
                "reference": sql_ref
            }))),
        )
        .await
        .expect("describe visible SQL ref");
    let visible_describe = visible_describe
        .structured_content
        .expect("structured visible describe");
    assert!(visible_describe.get("status").is_none());
    assert!(visible_describe.get("call").is_none());
    assert!(
        !visible_describe["sql_bindings"]
            .as_array()
            .expect("sql bindings")
            .is_empty()
    );

    let hidden_describe = client
        .call_tool(
            CallToolRequestParams::new("describe").with_arguments(json_object(&json!({
                "reference": "typescript:localMessages.events.files"
            }))),
        )
        .await
        .expect_err("hidden TypeScript ref should be rejected");
    assert!(
        hidden_describe
            .to_string()
            .contains("hidden by runtime exposure")
    );
}

#[tokio::test]
async fn mcp_runtime_exposure_sql_only_hides_typescript_schema() {
    let temp = TempDir::new().expect("temp dir");
    let manifest_path = write_fixture_manifest(temp.path());
    let manifest_yaml = fs::read_to_string(&manifest_path).expect("read manifest");
    let mut session = start_session_with_options(
        &temp,
        McpOptions {
            runtime_exposure: McpRuntimeExposure::sql_only(),
            ..McpOptions::default()
        },
    )
    .await;
    add_demo_source(&mut session.source_client, manifest_yaml).await;
    let client = &session.client;

    let tools = client.list_all_tools().await.expect("tools");
    let search_schema = tools[0].input_schema.as_ref();
    let kind_enum = search_schema
        .get("properties")
        .and_then(|properties| properties.get("kind"))
        .and_then(|kind| kind.get("enum"))
        .and_then(Value::as_array)
        .expect("search kind enum")
        .iter()
        .filter_map(Value::as_str)
        .collect::<Vec<_>>();
    assert_eq!(kind_enum, vec!["sql_table", "sql_function"]);
    assert!(
        !tools[0]
            .description
            .as_deref()
            .expect("search description")
            .contains("TypeScript bindings")
    );
    assert!(
        tools[1]
            .input_schema
            .as_ref()
            .get("properties")
            .and_then(|properties| properties.get("reference"))
            .and_then(|reference| reference.get("description"))
            .and_then(Value::as_str)
            .expect("describe reference description")
            .contains("SQL ref")
    );
    let search = client
        .call_tool(CallToolRequestParams::new("search"))
        .await
        .expect("sql-only search");
    let search = search.structured_content.expect("structured search");
    let sql_ref = assert_sql_only_search_hides_typescript(&search);
    assert_sql_only_describe_hides_typescript(client, &sql_ref).await;

    let hidden_kind = client
        .call_tool(
            CallToolRequestParams::new("search").with_arguments(json_object(&json!({
                "kind": "typescript"
            }))),
        )
        .await
        .expect_err("typescript kind should be hidden in SQL-only exposure");
    assert!(
        hidden_kind
            .to_string()
            .contains("hidden by runtime exposure")
    );

    session.shutdown().await;
}

#[tokio::test]
async fn mcp_runtime_exposure_none_returns_empty_search_without_hidden_counts() {
    let temp = TempDir::new().expect("temp dir");
    let manifest_path = write_fixture_manifest(temp.path());
    let manifest_yaml = fs::read_to_string(&manifest_path).expect("read manifest");
    let mut session = start_session_with_options(
        &temp,
        McpOptions {
            runtime_exposure: McpRuntimeExposure {
                typescript_enabled: false,
                sql_enabled: false,
            },
            ..McpOptions::default()
        },
    )
    .await;
    add_demo_source(&mut session.source_client, manifest_yaml).await;
    let client = &session.client;

    let search = client
        .call_tool(CallToolRequestParams::new("search"))
        .await
        .expect("none-exposure search");
    let search = search.structured_content.expect("structured search");
    assert!(search.get("runtime").is_none());
    assert_eq!(search["items"].as_array().expect("search items").len(), 0);
    assert_eq!(search["pagination"]["total"], 0);
    assert!(search["pagination"]["next_offset"].is_null());

    session.shutdown().await;
}

#[tokio::test]
async fn mcp_feedback_tool_persists_blocked_agent_report() {
    let temp = TempDir::new().expect("temp dir");
    let session = start_session(&temp).await;
    let client = &session.client;

    let tools = client.list_all_tools().await.expect("tools");
    assert_eq!(
        tools
            .iter()
            .map(|tool| tool.name.as_ref())
            .collect::<Vec<_>>(),
        vec!["search", "describe", "exec", "wait", "feedback"]
    );
    let feedback_annotations = tools[4].annotations.as_ref().expect("feedback annotations");
    assert_eq!(feedback_annotations.read_only_hint, Some(false));
    assert_eq!(feedback_annotations.destructive_hint, Some(false));
    assert_eq!(feedback_annotations.idempotent_hint, Some(false));
    assert_eq!(feedback_annotations.open_world_hint, Some(true));

    let feedback = client
        .call_tool(
            CallToolRequestParams::new("feedback").with_arguments(json_object(&json!({
                "trying_to_do": "Fix failing tests",
                "tried": "Ran cargo test and inspected the failing assertion",
                "stuck": "The fixture shape does not match the documented contract"
            }))),
        )
        .await
        .expect("feedback");
    assert_eq!(feedback.is_error, Some(false));
    let structured = feedback.structured_content.expect("structured content");
    assert!(
        structured["feedback_id"]
            .as_str()
            .is_some_and(|id| !id.is_empty())
    );
    assert!(
        structured["created_at"]
            .as_str()
            .is_some_and(|created_at| !created_at.is_empty())
    );
    assert_eq!(structured["message"], "Feedback report stored.");
    assert!(structured.get("upload").is_none());

    let raw = fs::read_to_string(
        temp.path()
            .join("coral-config/workspaces/default/feedback/reports.jsonl"),
    )
    .expect("feedback file should exist");
    let records = raw.lines().collect::<Vec<_>>();
    assert_eq!(records.len(), 1);
    let record: Value = serde_json::from_str(records[0]).expect("feedback JSONL should parse");
    assert_eq!(record["id"], structured["feedback_id"]);
    assert_eq!(record["workspace"], "default");
    assert_eq!(record["trying_to_do"], "Fix failing tests");
    assert_eq!(
        record["tried"],
        "Ran cargo test and inspected the failing assertion"
    );
    assert_eq!(
        record["stuck"],
        "The fixture shape does not match the documented contract"
    );

    let blank_feedback = client
        .call_tool(
            CallToolRequestParams::new("feedback").with_arguments(json_object(&json!({
                "trying_to_do": "Fix failing tests",
                "tried": " ",
                "stuck": "The fixture shape does not match the documented contract"
            }))),
        )
        .await
        .expect_err("blank feedback should fail before persistence");
    assert!(
        blank_feedback
            .to_string()
            .contains("missing string argument 'tried'")
    );

    let raw_after_error = fs::read_to_string(
        temp.path()
            .join("coral-config/workspaces/default/feedback/reports.jsonl"),
    )
    .expect("feedback file should still exist");
    assert_eq!(raw_after_error.lines().count(), 1);

    session.shutdown().await;
}

#[tokio::test]
async fn mcp_feedback_tool_is_exposed_by_default() {
    let temp = TempDir::new().expect("temp dir");
    let session = start_session(&temp).await;
    let client = &session.client;

    let feedback = client
        .call_tool(
            CallToolRequestParams::new("feedback").with_arguments(json_object(&json!({
                "trying_to_do": "Fix failing tests",
                "tried": "Ran cargo test",
                "stuck": "Need more context"
            }))),
        )
        .await
        .expect("feedback should be exposed by default");
    assert_eq!(feedback.is_error, Some(false));
    assert!(
        temp.path()
            .join("coral-config/workspaces/default/feedback/reports.jsonl")
            .exists()
    );

    session.shutdown().await;
}

#[tokio::test]
async fn mcp_tool_error_does_not_end_session() {
    let temp = TempDir::new().expect("temp dir");
    let manifest_path = write_fixture_manifest(temp.path());
    let manifest_yaml = fs::read_to_string(&manifest_path).expect("read manifest");
    let mut session = start_session(&temp).await;
    let client = &session.client;

    add_demo_source(&mut session.source_client, manifest_yaml).await;

    let sql = client
        .call_tool(
            CallToolRequestParams::new("sql").with_arguments(json_object(&json!({
                "sql": "SELECT text FROM local_messages.messages ORDER BY text"
            }))),
        )
        .await
        .expect_err("removed sql tool should fail before dispatch");
    assert!(sql.to_string().contains("tool 'sql' not found"));

    let search_after_error = client
        .call_tool(
            CallToolRequestParams::new("search").with_arguments(json_object(&json!({
                "query": "local messages"
            }))),
        )
        .await
        .expect("search after error");
    assert_eq!(
        search_after_error
            .structured_content
            .expect("structured content")["items"][0]["source_key"],
        "local_messages"
    );
    assert_eq!(search_after_error.is_error, Some(false));

    session.shutdown().await;
}

/// End-to-end guard for the MCP JSON contract: a large `Int64` result must
/// arrive in `structured_content` as a JSON string, not a JSON number, so
/// clients that parse JSON via IEEE-754 doubles preserve the exact value.
#[tokio::test]
async fn mcp_sql_returns_large_int64_as_string() {
    let temp = TempDir::new().expect("temp dir");
    let session = start_session(&temp).await;
    let client = &session.client;

    let exec = client
        .call_tool(
            CallToolRequestParams::new("exec").with_arguments(json_object(&json!({
                "source": "return await coral.sql.query(\"SELECT CAST(-8504475857937456387 AS BIGINT) AS user_id\");"
            }))),
        )
        .await
        .expect("exec");
    assert_eq!(exec.is_error, Some(false));
    let exec = exec.structured_content.expect("structured exec");
    assert_eq!(exec["run"]["status"], "completed");
    let rows = &exec["result"]["rows"];
    assert_eq!(
        rows[0]["user_id"],
        Value::String("-8504475857937456387".to_string()),
    );

    session.shutdown().await;
}

/// Payload regression gate (fix pipeline gate 2): the compact describe
/// response for the recursive Linear-style GraphQL fixture stays under a hard
/// byte cap, truncation flags fire, `path` drill-downs recover
/// renderer-elided detail cheaply, and `schemas: "full"` skips renderer
/// bounding for this fixture.
#[expect(
    clippy::too_many_lines,
    reason = "one session pins every describe byte budget (bounded, drilled, and full) against the same fixture"
)]
#[tokio::test]
async fn payload_gate_recursive_graphql_describe_stays_within_byte_budgets() {
    const COMPACT_DESCRIBE_HARD_CAP_BYTES: usize = 12 * 1024;
    const PATH_DRILL_CAP_BYTES: usize = 4 * 1024;
    const COMPACT_INPUT_SCHEMA_BUDGET_BYTES: usize = 8192;
    let temp = TempDir::new().expect("temp dir");
    let session = start_linear_fixture_session(&temp).await;
    let client = &session.client;

    let search = structured(
        client,
        "search",
        json!({ "query": "issues", "source_key": "linear_fixture" }),
    )
    .await;
    let reference = generated_item(&search, ".issues")["ref"]
        .as_str()
        .expect("issues query ref")
        .to_string();

    let describe = structured(client, "describe", json!({ "reference": &reference })).await;
    let describe_bytes = serialized_len(&describe);
    assert!(
        describe_bytes < COMPACT_DESCRIBE_HARD_CAP_BYTES,
        "compact describe regressed to {describe_bytes} bytes (hard cap {COMPACT_DESCRIBE_HARD_CAP_BYTES})"
    );
    assert_eq!(describe["input_schema_truncated"], true);
    assert!(describe["schema_note"].is_string());
    let bounded_input = &describe["input_schema"];
    let bounded_input_bytes = serialized_len(bounded_input);
    assert!(
        bounded_input_bytes <= COMPACT_INPUT_SCHEMA_BUDGET_BYTES,
        "bounded input schema must meet its budget, got {bounded_input_bytes} bytes"
    );
    // Near types stay whole; refs into dropped far types become typed stubs.
    assert_eq!(
        bounded_input
            .pointer("/properties/filter/$ref")
            .and_then(Value::as_str),
        Some("#/$defs/IssueFilter")
    );
    assert_eq!(
        bounded_input
            .pointer("/$defs/IssueFilter/properties/and/items/$ref")
            .and_then(Value::as_str),
        Some("#/$defs/IssueFilter")
    );
    assert_eq!(
        bounded_input
            .pointer("/$defs/IssueFilter/properties/assignee/x-coral-truncated")
            .and_then(Value::as_bool),
        Some(true),
        "expected the far NullableUserFilter ref to be elided in the bounded view: {bounded_input:#}"
    );
    assert!(
        bounded_input.pointer("/$defs/NullableUserFilter").is_none(),
        "far defs must be dropped, not kept pruned: {bounded_input:#}"
    );
    // The provider value schema is small, present, and untruncated.
    assert!(describe["value_schema"].is_object());
    assert!(describe.get("value_schema_truncated").is_none());
    assert!(describe.get("output_schema").is_none());

    // Drilling into one subtree recovers full fidelity for a fraction of the
    // compact entry cost.
    let drilled = structured(
        client,
        "describe",
        json!({ "reference": &reference, "path": "filter.assignee" }),
    )
    .await;
    assert_exact_keys(&drilled, &["ref", "call", "path", "schema", "elided"]);
    assert_eq!(drilled["path"], "filter.assignee");
    let drill_bytes = serialized_len(&drilled["schema"]);
    assert!(
        drill_bytes < PATH_DRILL_CAP_BYTES,
        "path drill-down regressed to {drill_bytes} bytes (cap {PATH_DRILL_CAP_BYTES})"
    );
    assert_eq!(drilled["elided"], json!([]));
    assert_eq!(
        drilled["schema"]
            .pointer("/properties/name/$ref")
            .and_then(Value::as_str),
        Some("#/$defs/StringComparator")
    );
    assert_eq!(
        drilled["schema"]
            .pointer("/$defs/StringComparator/properties/contains/type")
            .and_then(Value::as_str),
        Some("string"),
        "path drill must recover detail the bounded view elided: {drilled:#}"
    );

    // Drilling at the graph hub stays within the schema budget and reports
    // what it re-elided.
    let hub = structured(
        client,
        "describe",
        json!({ "reference": &reference, "path": "filter" }),
    )
    .await;
    assert!(serialized_len(&hub["schema"]) <= COMPACT_INPUT_SCHEMA_BUDGET_BYTES);
    assert!(
        !hub["elided"].as_array().expect("elided paths").is_empty(),
        "hub drill must list re-elided subtrees: {hub:#}"
    );

    // schemas:"full" skips renderer bounding and has zero renderer truncation.
    let full = structured(
        client,
        "describe",
        json!({ "reference": &reference, "schemas": "full" }),
    )
    .await;
    assert!(full.get("input_schema_truncated").is_none());
    assert!(full.get("schema_note").is_none());
    let full_input = serde_json::to_string(&full["input_schema"]).expect("full input json");
    assert!(
        full_input.len() > COMPACT_INPUT_SCHEMA_BUDGET_BYTES,
        "full rendering must remain unbounded, got {} bytes",
        full_input.len()
    );
    assert!(!full_input.contains("x-coral-truncated"));
    assert_eq!(
        full["input_schema"]
            .pointer("/$defs/StringComparator/properties/contains/type")
            .and_then(Value::as_str),
        Some("string")
    );

    session.shutdown().await;
}

/// Payload regression gate (fix pipeline gate 3): the MCP compact describe
/// entry and Code Mode's `coral.describe` entry are byte-identical for the
/// recursive GraphQL fixture under stable (key-sorted) serialization.
#[tokio::test]
async fn payload_gate_mcp_and_code_mode_graphql_describe_entries_are_byte_identical() {
    let temp = TempDir::new().expect("temp dir");
    let session = start_linear_fixture_session(&temp).await;
    let client = &session.client;

    let search = structured(
        client,
        "search",
        json!({ "query": "issues", "source_key": "linear_fixture" }),
    )
    .await;
    let reference = generated_item(&search, ".issues")["ref"]
        .as_str()
        .expect("issues query ref")
        .to_string();

    let mcp_entry = structured(client, "describe", json!({ "reference": &reference })).await;
    let exec = structured(
        client,
        "exec",
        json!({
            "source": format!(
                "return await coral.describe({});",
                json!({ "reference": &reference })
            )
        }),
    )
    .await;
    assert_eq!(exec["run"]["status"], "completed");
    assert_eq!(exec["result"]["status"], "found");

    assert_eq!(
        canonical_json(&exec["result"]["entry"]),
        canonical_json(&mcp_entry),
        "MCP and Code Mode compact describe entries must serialize identically"
    );

    session.shutdown().await;
}

/// Payload regression gate (fix pipeline gate 5): read-intent search ranks
/// every read before every mutation, write intent surfaces the mutation,
/// explicit `intent` overrides verb detection, exact refs win regardless, and
/// every executable hit carries a bounded one-line signature.
#[tokio::test]
async fn payload_gate_search_orders_reads_first_and_carries_bounded_signatures() {
    const SIGNATURE_CHAR_CAP: usize = 250;
    let temp = TempDir::new().expect("temp dir");
    let session = start_linear_fixture_session(&temp).await;
    let client = &session.client;

    let search = structured(
        client,
        "search",
        json!({ "query": "issue", "source_key": "linear_fixture" }),
    )
    .await;
    let items = search["items"].as_array().expect("search items");
    let effects = items
        .iter()
        .map(|item| item["effect"].as_str().expect("item effect"))
        .collect::<Vec<_>>();
    assert!(
        effects.contains(&"write"),
        "mixed corpus must include the mutation: {effects:?}"
    );
    let first_write = effects
        .iter()
        .position(|effect| *effect == "write")
        .expect("write position");
    assert!(
        effects
            .iter()
            .skip(first_write)
            .all(|effect| *effect != "read"),
        "every read must rank before every mutation: {effects:?}"
    );
    assert_eq!(effects.first(), Some(&"read"));
    for item in items {
        let Some(call) = item.get("call").and_then(Value::as_str) else {
            continue;
        };
        let signature = item["signature"]
            .as_str()
            .unwrap_or_else(|| panic!("executable hit missing signature: {item:#}"));
        assert!(
            signature.chars().count() < SIGNATURE_CHAR_CAP,
            "signature too long ({} chars): {signature}",
            signature.chars().count()
        );
        assert!(
            signature.starts_with(&format!("{call}(")),
            "signature must be the call form: {signature}"
        );
    }

    // A write-intent verb surfaces the mutation first.
    let write_search = structured(client, "search", json!({ "query": "update issue" })).await;
    assert_eq!(write_search["items"][0]["effect"], "write");
    let mutation_ref = write_search["items"][0]["ref"]
        .as_str()
        .expect("mutation ref")
        .to_string();

    // Explicit read intent overrides write-verb detection.
    let read_intent = structured(
        client,
        "search",
        json!({ "query": "update issue", "intent": "read" }),
    )
    .await;
    assert_eq!(read_intent["items"][0]["effect"], "read");

    // An exact mutation ref ranks first despite read-intent tiering and
    // inlines its compact describe entry as `top`.
    let exact = structured(client, "search", json!({ "query": &mutation_ref })).await;
    assert_eq!(exact["items"][0]["ref"], mutation_ref.as_str());
    assert_eq!(exact["items"][0]["rank_reason"], "exact typed ref");
    assert_eq!(exact["top"]["ref"], mutation_ref.as_str());

    session.shutdown().await;
}

/// Payload regression gate (fix pipeline gate 6): exec/wait responses carry
/// ONLY the slim envelope keys, none of the removed fields appear anywhere,
/// and joined console output round-trips through a wait cursor resume.
#[tokio::test]
async fn payload_gate_exec_wait_envelope_is_slim_and_round_trips_output() {
    let temp = TempDir::new().expect("temp dir");
    let session = start_session(&temp).await;
    let client = &session.client;

    let exec = structured(
        client,
        "exec",
        json!({
            "source": "console.log(\"alpha\"); console.log(\"beta\"); return { ok: true, rows: [1, 2] };"
        }),
    )
    .await;
    assert_exact_keys(&exec, &["run", "result", "output"]);
    assert_exact_keys(&exec["run"], &["id", "status"]);
    assert_eq!(exec["run"]["status"], "completed");
    assert_eq!(exec["output"], "alpha\nbeta");
    assert_eq!(exec["result"], json!({ "ok": true, "rows": [1, 2] }));

    let run_id = exec["run"]["id"].as_str().expect("run id").to_string();
    let wait = structured(client, "wait", json!({ "run_id": &run_id, "cursor": 0 })).await;
    assert_exact_keys(&wait, &["run", "result", "output"]);
    assert_eq!(wait["output"], exec["output"]);
    assert_eq!(wait["result"], exec["result"]);

    for response in [&exec, &wait] {
        let serialized = serde_json::to_string(response).expect("serialize response");
        for legacy in [
            "next_after_event_id",
            "has_more",
            "preview",
            "cell_id",
            "source_status",
            "last_event_id",
        ] {
            assert!(
                !serialized.contains(legacy),
                "legacy envelope field '{legacy}' leaked: {serialized}"
            );
        }
    }

    session.shutdown().await;
}

/// Payload regression gate (fix pipeline gate 7): one full discovery loop
/// (search -> describe -> exec) against the recursive GraphQL fixture stays
/// under a generous total byte budget, so gross payload regressions fail CI
/// even when the per-surface gates pass individually.
#[tokio::test]
async fn payload_gate_search_describe_exec_loop_stays_under_byte_budget() {
    const LOOP_BYTE_BUDGET: usize = 32 * 1024;
    let temp = TempDir::new().expect("temp dir");
    let session = start_linear_fixture_session(&temp).await;
    let client = &session.client;

    let search = structured(
        client,
        "search",
        json!({ "query": "list issues", "source_key": "linear_fixture" }),
    )
    .await;
    let reference = generated_item(&search, ".issues")["ref"]
        .as_str()
        .expect("issues query ref")
        .to_string();

    let describe = structured(client, "describe", json!({ "reference": &reference })).await;

    let exec = structured(
        client,
        "exec",
        json!({
            "source": format!(
                "const described = await coral.describe({});\nreturn {{ call: described.entry.call, params: Object.keys(described.entry.input_schema.properties) }};",
                json!({ "reference": &reference })
            )
        }),
    )
    .await;
    assert_eq!(exec["run"]["status"], "completed");

    let total = serialized_len(&search) + serialized_len(&describe) + serialized_len(&exec);
    assert!(
        total < LOOP_BYTE_BUDGET,
        "search+describe+exec loop regressed to {total} bytes (budget {LOOP_BYTE_BUDGET}): search={}, describe={}, exec={}",
        serialized_len(&search),
        serialized_len(&describe),
        serialized_len(&exec)
    );

    session.shutdown().await;
}
