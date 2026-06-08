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
    assert_eq!(search["items"][0]["deprecated"], false);
    assert_eq!(search["items"][0]["support_status"], "generated");
    let reference = search["items"][0]["refs"]
        .as_array()
        .expect("refs")
        .iter()
        .find_map(Value::as_str)
        .expect("reference");
    let describe = client
        .call_tool(
            CallToolRequestParams::new("describe").with_arguments(json_object(&json!({
                "reference": reference
            }))),
        )
        .await
        .expect("describe demo source");
    let describe = describe.structured_content.expect("structured describe");
    assert!(describe.get("description").is_none());
    assert!(describe["entry"].get("capability").is_none());
    assert!(describe["entry"].get("code_mode_output_schema").is_none());
    assert_eq!(describe["entry"]["deprecated"], false);
    assert_eq!(describe["entry"]["support_status"], "generated");
    assert_eq!(describe["runtime"]["exposure"], "both");
    assert_eq!(describe["runtime"]["typescript_enabled"], true);
    assert_eq!(describe["runtime"]["sql_enabled"], true);
    assert!(
        describe["runtime"]["visible_bindings"]["typescript"]
            .as_u64()
            .expect("typescript binding count")
            > 0
    );
    assert_eq!(
        describe["entry"]["full_path"],
        search["items"][0]["full_path"]
    );
    assert!(
        describe["entry"]["full_path"]
            .as_str()
            .expect("full path")
            .starts_with("tools.localMessages.events.")
    );

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
    assert!(structured_search.get("pagination").is_none());
    assert_eq!(structured_search["runtime"]["exposure"], "both");
    assert_eq!(structured_search["runtime"]["installed_source_count"], 0);
    assert_eq!(
        structured_search["runtime"]["visible_bindings"]["typescript"],
        0
    );
    assert_eq!(
        structured_search["runtime"]["visible_bindings"]["sql_table"],
        0
    );
    assert_eq!(
        structured_search["runtime"]["visible_bindings"]["sql_function"],
        0
    );
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
            .expect("structured describe")["found"],
        false
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
    let run_id = exec["run_id"].as_str().expect("run id");
    assert_eq!(exec["status"]["name"], "CODE_MODE_RUN_STATUS_COMPLETED");

    let wait = client
        .call_tool(
            CallToolRequestParams::new("wait").with_arguments(json_object(&json!({
                "run_id": run_id,
                "after_event_id": 2
            }))),
        )
        .await
        .expect("wait");
    assert_eq!(wait.is_error, Some(false));
    let wait = wait.structured_content.expect("structured wait");
    assert_eq!(wait["run_id"], run_id);
    assert_eq!(wait["status"]["name"], "CODE_MODE_RUN_STATUS_COMPLETED");

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
    assert_eq!(search["runtime"]["exposure"], "sql");
    assert_eq!(search["runtime"]["typescript_enabled"], false);
    assert_eq!(search["runtime"]["sql_enabled"], true);
    assert_eq!(search["runtime"]["visible_bindings"]["typescript"], 0);
    let items = search["items"].as_array().expect("search items");
    assert!(
        !items.is_empty(),
        "SQL-only search should return SQL projections"
    );
    for item in items {
        assert_eq!(item["alias"], "");
        assert_eq!(item["full_path"], "");
        assert!(
            item["refs"]
                .as_array()
                .expect("refs")
                .iter()
                .filter_map(Value::as_str)
                .all(|ref_| ref_.starts_with("sql_table:") || ref_.starts_with("sql_function:")),
            "hidden TypeScript ref leaked in {item:?}"
        );
        assert!(
            item["available_bindings"]
                .as_array()
                .expect("available bindings")
                .iter()
                .all(|kind| kind.as_i64() != Some(1)),
            "hidden TypeScript binding kind leaked in {item:?}"
        );
    }
    items[0]["refs"]
        .as_array()
        .expect("refs")
        .iter()
        .find_map(Value::as_str)
        .expect("sql ref")
        .to_string()
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
    assert_eq!(visible_describe["found"], true);
    assert_eq!(visible_describe["entry"]["alias"], "");
    assert_eq!(visible_describe["entry"]["full_path"], "");
    assert!(visible_describe["entry"]["typescript_binding"].is_null());
    assert!(
        !visible_describe["entry"]["sql_bindings"]
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
    assert_eq!(search["runtime"]["exposure"], "none");
    assert_eq!(search["runtime"]["typescript_enabled"], false);
    assert_eq!(search["runtime"]["sql_enabled"], false);
    assert_eq!(search["items"].as_array().expect("search items").len(), 0);
    assert_eq!(search["total"], 0);
    assert_eq!(search["has_more"], false);
    assert_eq!(search["next_offset"], 0);

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
    let run_id = exec.structured_content.expect("structured exec")["run_id"]
        .as_str()
        .expect("run id")
        .to_string();

    let wait = client
        .call_tool(
            CallToolRequestParams::new("wait").with_arguments(json_object(&json!({
                "run_id": run_id
            }))),
        )
        .await
        .expect("wait");
    assert_eq!(wait.is_error, Some(false));
    let wait = wait.structured_content.expect("structured wait");
    let events = wait["events"].as_array().expect("events");
    let output = events
        .iter()
        .find_map(|event| event.pointer("/result_item/item").cloned())
        .expect("result event");
    let rows = &output["rows"];
    assert_eq!(
        rows[0]["user_id"],
        Value::String("-8504475857937456387".to_string()),
    );

    session.shutdown().await;
}
