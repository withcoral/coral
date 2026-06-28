#![allow(
    clippy::indexing_slicing,
    clippy::string_slice,
    reason = "test code: assertion-style indexing is idiomatic in tests"
)]

use std::fs;
use std::path::{Path, PathBuf};

use coral_api::{
    CORAL_EPISODE_ID_MAX_LEN, CORAL_EPISODE_INTENT_MAX_CHARS,
    v1::{ImportSourceRequest, import_source_response},
};
use coral_client::{
    AppClient, SourceClient, default_workspace,
    local::{RunningServer, ServerBuilder},
};
use jsonschema::JSONSchema;
use rmcp::{
    RoleClient, ServiceExt,
    model::{CallToolRequestParams, ReadResourceRequestParams, Tool},
    service::RunningService,
};
use serde_json::{Map, Value, json};
use tempfile::TempDir;
use tonic::Request;

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
version: 0.1.0
dsl_version: 3
backend: file
tables:
  - name: events
    description: Fixture events
    format: jsonl
    source:
      location: file://{}/
      glob: "**/*.jsonl"
    columns:
      - name: type
        type: Utf8
      - name: sessionId
        type: Utf8
      - name: text
        type: Utf8
  - name: messages
    description: Fixture messages
    format: jsonl
    source:
      location: file://{}/
      glob: "**/*.jsonl"
    columns:
      - name: type
        type: Utf8
      - name: sessionId
        type: Utf8
      - name: text
        type: Utf8
  - name: sessions
    description: Fixture sessions
    format: jsonl
    source:
      location: file://{}/
      glob: "**/*.jsonl"
    columns:
      - name: type
        type: Utf8
      - name: sessionId
        type: Utf8
      - name: text
        type: Utf8
"#,
        data_dir.display(),
        data_dir.display(),
        data_dir.display()
    );
    let manifest_path = source_dir.join("source.yaml");
    fs::write(&manifest_path, manifest).expect("write manifest");
    manifest_path
}

fn write_function_fixture_manifest(root: &Path) -> PathBuf {
    let source_dir = root.join("function-source");
    fs::create_dir_all(&source_dir).expect("create function source dir");
    let manifest = r"
name: searchy
version: 0.1.0
dsl_version: 3
backend: http
base_url: https://example.com
tables:
  - name: placeholder
    description: Placeholder table
    request:
      method: GET
      path: /placeholder
    columns:
      - name: id
        type: Utf8
functions:
  - name: lookup_issue
    description: Lookup issue
    args:
      - name: number
        required: true
        bind:
          arg: number
    request:
      method: GET
      path: /issues/{{arg.number}}
    columns:
      - name: title
        type: Utf8
        description: Issue title
  - name: search_issues
    description: Search issues
    args:
      - name: q
        required: true
        bind:
          arg: q
      - name: mode
        values: [lexical, semantic, hybrid]
        bind:
          arg: search_type
    request:
      method: GET
      path: /search/issues
      query:
        - name: q
          from: arg
          key: q
        - name: search_type
          from: arg
          key: search_type
    response:
      rows_path: [items]
    columns:
      - name: title
        type: Utf8
        description: Issue title
      - name: score
        type: Float64
";
    let manifest_path = source_dir.join("source.yaml");
    fs::write(&manifest_path, manifest).expect("write function manifest");
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

fn text_content(result: &rmcp::model::ReadResourceResult) -> &str {
    match &result.contents[0] {
        rmcp::model::ResourceContents::TextResourceContents { text, .. } => text,
        other @ rmcp::model::ResourceContents::BlobResourceContents { .. } => {
            panic!("unexpected resource contents: {other:?}")
        }
    }
}

fn tool_by_name<'a>(tools: &'a [Tool], name: &str) -> &'a Tool {
    tools
        .iter()
        .find(|tool| tool.name == name)
        .expect("tool should be listed")
}

fn tool_input_properties(tool: &Tool) -> &Map<String, Value> {
    tool.input_schema
        .get("properties")
        .and_then(Value::as_object)
        .unwrap_or_else(|| panic!("tool '{}' should advertise input properties", tool.name))
}

fn assert_tool_advertises_episode_id(tool: &Tool) {
    let episode_id_schema = tool_input_properties(tool)
        .get("episode_id")
        .unwrap_or_else(|| panic!("tool '{}' should advertise optional episode_id", tool.name));
    assert_nullable_episode_id_schema(episode_id_schema, tool.name.as_ref());
}

fn assert_tool_advertises_intent(tool: &Tool) {
    let intent_schema = tool_input_properties(tool)
        .get("intent")
        .unwrap_or_else(|| panic!("tool '{}' should advertise optional intent", tool.name));
    assert_eq!(intent_schema["type"], json!("string"));
    assert_eq!(intent_schema["minLength"], json!(1));
    assert_eq!(
        intent_schema["maxLength"],
        json!(CORAL_EPISODE_INTENT_MAX_CHARS)
    );
}

/// Read the per-workspace episode records (one JSON object per JSONL line) for the default workspace.
fn read_episode_records(temp: &TempDir) -> Vec<Value> {
    let episodes_path = temp
        .path()
        .join("coral-config/workspaces/default/episodes/episodes.jsonl");
    fs::read_to_string(&episodes_path)
        .expect("episode file should exist")
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).expect("episode JSONL should parse"))
        .collect()
}

fn assert_nullable_episode_id_schema(schema: &Value, label: &str) {
    let any_of = schema
        .get("anyOf")
        .and_then(Value::as_array)
        .unwrap_or_else(|| panic!("{label} episode id schema should use anyOf"));
    let string_schema = any_of
        .iter()
        .find(|schema| schema.get("type") == Some(&json!("string")))
        .unwrap_or_else(|| panic!("{label} episode id schema should accept strings"));
    assert!(
        any_of
            .iter()
            .any(|schema| schema.get("type") == Some(&json!("null"))),
        "{label} episode id schema should accept null"
    );
    assert_eq!(string_schema["minLength"], json!(1));
    assert_eq!(string_schema["maxLength"], json!(CORAL_EPISODE_ID_MAX_LEN));
    assert_eq!(string_schema["pattern"], json!("^[!-~]+$"));
}

fn assert_tool_omits_episode_id(tool: &Tool) {
    assert!(
        !tool_input_properties(tool).contains_key("episode_id"),
        "tool '{}' should not advertise episode_id by default",
        tool.name
    );
}

fn assert_matches_output_schema(tool: &Tool, value: &Value) {
    let schema = Value::Object(
        tool.output_schema
            .as_ref()
            .unwrap_or_else(|| panic!("tool '{}' should advertise output schema", tool.name))
            .as_ref()
            .clone(),
    );
    let compiled = JSONSchema::compile(&schema).expect("tool output schema should compile");
    if let Err(errors) = compiled.validate(value) {
        let details = errors
            .map(|error| error.to_string())
            .collect::<Vec<_>>()
            .join("; ");
        panic!(
            "tool '{}' structured content did not match output schema: {details}",
            tool.name
        );
    }
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "This end-to-end MCP session test verifies feature-gated tool advertisement, persistence, child lineage, tagged follow-up calls, and validation together."
)]
async fn mcp_episode_tool_persists_episode_and_tags_follow_up_calls() {
    let temp = TempDir::new().expect("temp dir");
    let session = start_session_with_options(
        &temp,
        McpOptions {
            episodes_enabled: true,
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
        vec![
            "sql",
            "list_catalog",
            "search_catalog",
            "describe_table",
            "list_columns",
            "open_episode"
        ]
    );
    for name in [
        "sql",
        "list_catalog",
        "search_catalog",
        "describe_table",
        "list_columns",
    ] {
        let tool = tool_by_name(&tools, name);
        assert_tool_advertises_intent(tool);
        assert_tool_advertises_episode_id(tool);
    }
    let open_episode_tool = tool_by_name(&tools, "open_episode");
    assert_tool_advertises_intent(open_episode_tool);
    assert!(!tool_input_properties(open_episode_tool).contains_key("episode_id"));
    let parent_episode_id_schema = tool_input_properties(open_episode_tool)
        .get("parent_episode_id")
        .expect("open_episode should accept an optional parent_episode_id");
    assert_nullable_episode_id_schema(parent_episode_id_schema, "open_episode parent_episode_id");
    let open_annotations = open_episode_tool
        .annotations
        .as_ref()
        .expect("open episode annotations");
    assert_eq!(open_annotations.read_only_hint, Some(false));
    assert_eq!(open_annotations.destructive_hint, Some(false));
    assert_eq!(open_annotations.idempotent_hint, Some(false));
    assert_eq!(open_annotations.open_world_hint, Some(false));

    let root = client
        .call_tool(
            CallToolRequestParams::new("open_episode").with_arguments(json_object(&json!({
                "intent": "Investigate customer renewal risk"
            }))),
        )
        .await
        .expect("open root episode");
    assert_eq!(root.is_error, Some(false));
    let root = root.structured_content.expect("root structured content");
    assert_matches_output_schema(open_episode_tool, &root);
    let root_episode_id = root["episode_id"]
        .as_str()
        .expect("root episode id")
        .to_string();
    assert!(root_episode_id.starts_with("ep_"));
    assert_eq!(root["parent_episode_id"], Value::Null);
    assert_eq!(root["message"], "Episode opened.");
    assert!(
        root["instructions"]
            .as_str()
            .expect("instructions")
            .contains("subsequent Coral MCP tool calls")
    );

    let child = client
        .call_tool(
            CallToolRequestParams::new("open_episode").with_arguments(json_object(&json!({
                "intent": "Check renewal table columns",
                "parent_episode_id": root_episode_id
            }))),
        )
        .await
        .expect("open child episode");
    assert_eq!(child.is_error, Some(false));
    let child = child.structured_content.expect("child structured content");
    assert_matches_output_schema(open_episode_tool, &child);
    let child_episode_id = child["episode_id"]
        .as_str()
        .expect("child episode id")
        .to_string();
    assert!(child_episode_id.starts_with("ep_"));
    assert_eq!(child["parent_episode_id"], root_episode_id.as_str());

    let sql = client
        .call_tool(
            CallToolRequestParams::new("sql").with_arguments(json_object(&json!({
                "sql": "SELECT 1 AS ok",
                "episode_id": child_episode_id,
                "intent": "Verify the child episode can run a SQL probe"
            }))),
        )
        .await
        .expect("tagged sql");
    assert_eq!(sql.is_error, Some(false));
    assert_eq!(
        sql.structured_content.expect("sql structured")["rows"][0]["ok"],
        "1"
    );

    let invalid_episode_id = client
        .call_tool(
            CallToolRequestParams::new("sql").with_arguments(json_object(&json!({
                "sql": "SELECT 1",
                "episode_id": "has space"
            }))),
        )
        .await
        .expect_err("invalid episode_id should fail before query dispatch");
    assert!(
        invalid_episode_id
            .to_string()
            .contains("argument 'episode_id' must be graphic ASCII")
    );

    let invalid_intent = client
        .call_tool(
            CallToolRequestParams::new("sql").with_arguments(json_object(&json!({
                "sql": "SELECT 1",
                "intent": " "
            }))),
        )
        .await
        .expect_err("blank tool intent should fail before query dispatch");
    assert!(
        invalid_intent
            .to_string()
            .contains("argument 'intent' must not be blank")
    );

    let invalid_open_episode_id = client
        .call_tool(
            CallToolRequestParams::new("open_episode").with_arguments(json_object(&json!({
                "intent": "Open a child task",
                "episode_id": "has space"
            }))),
        )
        .await
        .expect_err("invalid stray episode_id should fail before opening an episode");
    assert!(
        invalid_open_episode_id
            .to_string()
            .contains("argument 'episode_id' must be graphic ASCII")
    );

    let episodes_path = temp
        .path()
        .join("coral-config/workspaces/default/episodes/episodes.jsonl");
    let raw = fs::read_to_string(&episodes_path).expect("episode file should exist");
    let records = raw
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).expect("episode JSONL should parse"))
        .collect::<Vec<_>>();
    assert_eq!(records.len(), 2);
    let root_record = records
        .iter()
        .find(|record| record["id"] == root_episode_id.as_str())
        .expect("root episode record");
    assert_eq!(root_record["workspace"], "default");
    assert_eq!(root_record["intent"], "Investigate customer renewal risk");
    assert_eq!(root_record["parent_episode_id"], Value::Null);
    let child_record = records
        .iter()
        .find(|record| record["id"] == child_episode_id.as_str())
        .expect("child episode record");
    assert_eq!(child_record["workspace"], "default");
    assert_eq!(child_record["intent"], "Check renewal table columns");
    assert_eq!(child_record["parent_episode_id"], root_episode_id.as_str());

    let blank_intent = client
        .call_tool(
            CallToolRequestParams::new("open_episode").with_arguments(json_object(&json!({
                "intent": " "
            }))),
        )
        .await
        .expect_err("blank intent should fail before persistence");
    assert!(
        blank_intent
            .to_string()
            .contains("missing string argument 'intent'")
    );
    let raw_after_error = fs::read_to_string(&episodes_path).expect("episode file should exist");
    assert_eq!(raw_after_error.lines().count(), 2);

    session.shutdown().await;
}

#[tokio::test]
async fn mcp_episode_tool_is_disabled_by_default() {
    let temp = TempDir::new().expect("temp dir");
    let session = start_session(&temp).await;
    let client = &session.client;

    let tools = client.list_all_tools().await.expect("tools");
    assert!(
        tools
            .iter()
            .all(|tool| tool.name.as_ref() != "open_episode"),
        "open_episode should not be listed by default"
    );
    for tool in &tools {
        assert_tool_advertises_intent(tool);
        assert_tool_omits_episode_id(tool);
    }

    let open_episode = client
        .call_tool(
            CallToolRequestParams::new("open_episode").with_arguments(json_object(&json!({
                "intent": "Investigate customer renewal risk"
            }))),
        )
        .await
        .expect_err("open_episode should not be exposed by default");
    assert!(
        open_episode
            .to_string()
            .contains("tool 'open_episode' not found")
    );

    // A data-tool `intent` is accepted even when episodes are off — no segmentation, no store.
    let sql = client
        .call_tool(
            CallToolRequestParams::new("sql").with_arguments(json_object(&json!({
                "sql": "SELECT 1 AS ok",
                "intent": "Investigate customer renewal risk"
            }))),
        )
        .await
        .expect("sql should run with episodes disabled");
    assert_eq!(sql.is_error, Some(false));

    let invalid_intent = client
        .call_tool(
            CallToolRequestParams::new("sql").with_arguments(json_object(&json!({
                "sql": "SELECT 1",
                "intent": " "
            }))),
        )
        .await
        .expect_err("blank tool intent should fail before query dispatch");
    assert!(
        invalid_intent
            .to_string()
            .contains("argument 'intent' must not be blank")
    );
    assert!(
        !temp
            .path()
            .join("coral-config/workspaces/default/episodes/episodes.jsonl")
            .exists()
    );

    session.shutdown().await;
}

/// Open an episodes-enabled MCP session for the segmentation tests below.
async fn start_episodes_session(temp: &TempDir) -> TestSession {
    start_session_with_options(
        temp,
        McpOptions {
            episodes_enabled: true,
            ..McpOptions::default()
        },
    )
    .await
}

#[tokio::test]
async fn mcp_intent_opens_one_episode_then_reuses_while_stable() {
    let temp = TempDir::new().expect("temp dir");
    let session = start_episodes_session(&temp).await;
    let client = &session.client;

    for _ in 0..2 {
        let result = client
            .call_tool(
                CallToolRequestParams::new("sql").with_arguments(json_object(&json!({
                    "sql": "SELECT 1 AS ok",
                    "intent": "Investigate customer renewal risk"
                }))),
            )
            .await
            .expect("sql with intent");
        assert_eq!(result.is_error, Some(false));
    }

    // Same intent across both calls → a single root episode, opened once.
    let records = read_episode_records(&temp);
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["intent"], "Investigate customer renewal risk");
    assert_eq!(records[0]["parent_episode_id"], Value::Null);
    assert!(records[0]["id"].as_str().expect("id").starts_with("ep_"));

    session.shutdown().await;
}

#[tokio::test]
async fn mcp_changed_intent_opens_a_new_episode() {
    let temp = TempDir::new().expect("temp dir");
    let session = start_episodes_session(&temp).await;
    let client = &session.client;

    for intent in ["Investigate renewal risk", "Draft the renewal email"] {
        client
            .call_tool(
                CallToolRequestParams::new("sql").with_arguments(json_object(&json!({
                    "sql": "SELECT 1 AS ok",
                    "intent": intent
                }))),
            )
            .await
            .expect("sql with intent");
    }

    let records = read_episode_records(&temp);
    assert_eq!(records.len(), 2);
    let intents = records
        .iter()
        .map(|record| record["intent"].as_str().expect("intent").to_string())
        .collect::<Vec<_>>();
    assert!(intents.contains(&"Investigate renewal risk".to_string()));
    assert!(intents.contains(&"Draft the renewal email".to_string()));
    // Intent-segmented episodes are roots; lineage is not inferred from the intent stream.
    for record in &records {
        assert_eq!(record["parent_episode_id"], Value::Null);
    }

    session.shutdown().await;
}

#[tokio::test]
async fn mcp_intent_normalization_collapses_whitespace_into_one_episode() {
    let temp = TempDir::new().expect("temp dir");
    let session = start_episodes_session(&temp).await;
    let client = &session.client;

    for intent in ["renewal   risk", "renewal risk"] {
        client
            .call_tool(
                CallToolRequestParams::new("sql").with_arguments(json_object(&json!({
                    "sql": "SELECT 1 AS ok",
                    "intent": intent
                }))),
            )
            .await
            .expect("sql with intent");
    }

    // Internal-whitespace-only difference normalizes equal → reuse, so just one episode.
    let records = read_episode_records(&temp);
    assert_eq!(records.len(), 1);

    session.shutdown().await;
}

#[tokio::test]
async fn mcp_explicit_episode_id_wins_over_intent() {
    let temp = TempDir::new().expect("temp dir");
    let session = start_episodes_session(&temp).await;
    let client = &session.client;

    let root = client
        .call_tool(
            CallToolRequestParams::new("open_episode").with_arguments(json_object(&json!({
                "intent": "Investigate renewal risk"
            }))),
        )
        .await
        .expect("open episode");
    let episode_id = root.structured_content.expect("structured")["episode_id"]
        .as_str()
        .expect("episode id")
        .to_string();
    assert_eq!(read_episode_records(&temp).len(), 1);

    // A different intent alongside an explicit episode_id must not open a new episode.
    let sql = client
        .call_tool(
            CallToolRequestParams::new("sql").with_arguments(json_object(&json!({
                "sql": "SELECT 1 AS ok",
                "episode_id": episode_id,
                "intent": "A completely different task"
            }))),
        )
        .await
        .expect("tagged sql");
    assert_eq!(sql.is_error, Some(false));
    assert_eq!(read_episode_records(&temp).len(), 1);

    session.shutdown().await;
}

#[tokio::test]
async fn mcp_concurrent_same_intent_opens_single_episode() {
    let temp = TempDir::new().expect("temp dir");
    let session = start_episodes_session(&temp).await;
    let client = &session.client;

    let call = || {
        client.call_tool(
            CallToolRequestParams::new("sql").with_arguments(json_object(&json!({
                "sql": "SELECT 1 AS ok",
                "intent": "Investigate renewal risk"
            }))),
        )
    };
    // Two concurrent calls with the same new intent must mint exactly one episode (mint-under-lock).
    let (a, b) = tokio::join!(call(), call());
    a.expect("first concurrent sql");
    b.expect("second concurrent sql");

    assert_eq!(read_episode_records(&temp).len(), 1);

    session.shutdown().await;
}

#[tokio::test]
async fn mcp_catalog_helpers_expose_coral_system_tables_from_sql_catalog() {
    let temp = TempDir::new().expect("temp dir");
    let session = start_session(&temp).await;
    let client = &session.client;
    let expected_tables = ["columns", "filters", "inputs", "table_functions", "tables"];

    let sql = client
        .call_tool(
            CallToolRequestParams::new("sql").with_arguments(json_object(&json!({
                "sql": "SELECT table_name FROM coral.tables WHERE schema_name = 'coral' ORDER BY table_name"
            }))),
        )
        .await
        .expect("sql system catalog");
    let sql_rows = sql.structured_content.as_ref().expect("structured sql")["rows"]
        .as_array()
        .expect("sql rows");
    assert_eq!(
        sql_rows
            .iter()
            .map(|row| row["table_name"].as_str().expect("table_name"))
            .collect::<Vec<_>>(),
        expected_tables
    );

    let catalog = client
        .call_tool(
            CallToolRequestParams::new("list_catalog").with_arguments(json_object(&json!({
                "schema": "coral",
                "kind": "table"
            }))),
        )
        .await
        .expect("list system catalog")
        .structured_content
        .expect("structured catalog");
    assert_eq!(catalog["total"], expected_tables.len());
    assert_eq!(
        catalog["items"]
            .as_array()
            .expect("catalog items")
            .iter()
            .map(|item| item["table"]["table_name"].as_str().expect("table name"))
            .collect::<Vec<_>>(),
        expected_tables
    );

    let described = client
        .call_tool(
            CallToolRequestParams::new("describe_table").with_arguments(json_object(&json!({
                "schema": "coral",
                "table": "columns"
            }))),
        )
        .await
        .expect("describe system table")
        .structured_content
        .expect("structured describe");
    assert_eq!(described["found"], true);
    assert_eq!(described["name"], "coral.columns");
    assert_eq!(described["column_count"], 10);

    let columns = client
        .call_tool(
            CallToolRequestParams::new("list_columns").with_arguments(json_object(&json!({
                "schema": "coral",
                "table": "tables"
            }))),
        )
        .await
        .expect("list system columns")
        .structured_content
        .expect("structured columns");
    assert_eq!(columns["total"], 6);
    assert_eq!(columns["columns"][0]["column_name"], "schema_name");

    session.shutdown().await;
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "This focused session test still verifies multiple discovery and resource refresh assertions in one end-to-end flow."
)]
async fn mcp_surface_refreshes_and_renders_dynamic_guide() {
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
        vec![
            "sql",
            "list_catalog",
            "search_catalog",
            "describe_table",
            "list_columns"
        ]
    );
    assert!(
        initial_tools[0]
            .description
            .as_deref()
            .expect("sql description")
            .contains("5 table(s) are currently visible")
    );
    assert!(
        initial_tools[0]
            .description
            .as_deref()
            .expect("sql description")
            .contains("No connected user sources are currently configured")
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
    let initial_resources = client
        .list_all_resources()
        .await
        .expect("initial resources");
    assert_eq!(
        initial_resources
            .iter()
            .map(|resource| resource.uri.as_str())
            .collect::<Vec<_>>(),
        vec!["coral://guide", "coral://tables"]
    );
    assert!(
        initial_resources[0]
            .description
            .as_deref()
            .expect("guide description")
            .contains("5 visible table")
    );

    let initial_guide = client
        .read_resource(ReadResourceRequestParams::new("coral://guide"))
        .await
        .expect("initial guide");
    let initial_guide_text = text_content(&initial_guide);
    assert!(initial_guide_text.contains("## Available Schemas"));
    assert!(initial_guide_text.contains("- coral: System catalog schema."));
    assert!(initial_guide_text.contains("No user schemas are currently configured."));
    assert!(initial_guide_text.contains("read-only SQL database"));
    assert!(initial_guide_text.contains("CROSS JOIN"));
    assert!(initial_guide_text.contains("schema_name = '<schema>'"));

    add_demo_source(&mut session.source_client, manifest_yaml).await;

    let updated_tools = client.list_all_tools().await.expect("updated tools");
    let list_catalog_tool = tool_by_name(&updated_tools, "list_catalog");
    let search_catalog_tool = tool_by_name(&updated_tools, "search_catalog");
    let list_columns_tool = tool_by_name(&updated_tools, "list_columns");
    assert!(
        updated_tools[0]
            .description
            .as_deref()
            .expect("sql description")
            .contains("8 table(s) are currently visible")
    );
    assert!(
        updated_tools[0]
            .description
            .as_deref()
            .expect("sql description")
            .contains("Connected sources/schemas include: local_messages")
    );
    assert!(
        updated_tools[1]
            .description
            .as_deref()
            .expect("catalog description")
            .contains("8 table(s) and 0 table function(s) are currently visible")
    );
    assert!(
        updated_tools[1]
            .description
            .as_deref()
            .expect("catalog description")
            .contains("Connected sources/schemas include: local_messages")
    );
    assert!(
        updated_tools[2]
            .description
            .as_deref()
            .expect("catalog search description")
            .contains("8 table(s) and 0 table function(s) are currently visible")
    );
    assert!(
        updated_tools[2]
            .description
            .as_deref()
            .expect("catalog search description")
            .contains("Connected sources/schemas include: local_messages")
    );

    let updated_resources = client
        .list_all_resources()
        .await
        .expect("updated resources");
    assert!(
        updated_resources[0]
            .description
            .as_deref()
            .expect("guide description")
            .contains("1 configured connection")
    );

    let tables_resource = client
        .read_resource(ReadResourceRequestParams::new("coral://tables"))
        .await
        .expect("read tables resource");
    let tables_text = text_content(&tables_resource);
    let tables_json =
        serde_json::from_str::<serde_json::Value>(tables_text).expect("parse tables resource");
    assert_eq!(tables_json["tables"][0]["name"], "coral.columns");
    assert_eq!(tables_json["tables"][0]["sql_reference"], "coral.columns");
    assert!(
        tables_json["tables"]
            .as_array()
            .expect("tables")
            .iter()
            .any(|table| table["name"] == "local_messages.events")
    );

    let updated_guide = client
        .read_resource(ReadResourceRequestParams::new("coral://guide"))
        .await
        .expect("updated guide");
    let updated_guide_text = text_content(&updated_guide);
    assert!(updated_guide_text.contains("## Available Schemas"));
    assert!(updated_guide_text.contains("- coral: System catalog schema."));
    assert!(updated_guide_text.contains("- local_messages"));
    assert!(updated_guide_text.contains("Prefer one SQL statement with `JOIN`, `CROSS JOIN`"));
    assert!(!updated_guide_text.contains("## Visible SQL Schemas"));
    assert!(updated_guide_text.contains(
        "FROM coral.columns WHERE schema_name = 'local_messages' AND table_name = 'events'"
    ));

    let catalog = client
        .call_tool(CallToolRequestParams::new("list_catalog"))
        .await
        .expect("list catalog");
    let catalog = catalog.structured_content.expect("structured catalog");
    assert_eq!(catalog["total"], 8);
    assert_eq!(catalog["items"][0]["kind"], "table");
    assert_eq!(catalog["items"][0]["name"], "coral.columns");
    assert_eq!(catalog["items"][0]["sql_reference"], "coral.columns");
    assert_eq!(catalog["items"][0]["table"]["table_name"], "columns");
    assert_matches_output_schema(list_catalog_tool, &catalog);

    let catalog_page = client
        .call_tool(
            CallToolRequestParams::new("list_catalog").with_arguments(json_object(&json!({
                "schema": "local_messages",
                "kind": "table",
                "limit": 2,
                "offset": 0
            }))),
        )
        .await
        .expect("list paginated catalog");
    let catalog_page = catalog_page.structured_content.expect("structured content");
    assert_eq!(catalog_page["total"], 3);
    assert_eq!(catalog_page["limit"], 2);
    assert_eq!(catalog_page["has_more"], true);
    assert_eq!(catalog_page["next_offset"], 2);
    assert_eq!(catalog_page["items"].as_array().expect("items").len(), 2);
    assert_matches_output_schema(list_catalog_tool, &catalog_page);

    let unknown_catalog_schema = client
        .call_tool(
            CallToolRequestParams::new("list_catalog").with_arguments(json_object(&json!({
                "schema": "missing",
                "kind": "table",
                "limit": 2,
                "offset": 0
            }))),
        )
        .await
        .expect("list unknown catalog schema");
    let unknown_catalog_schema = unknown_catalog_schema
        .structured_content
        .expect("structured content");
    assert_eq!(unknown_catalog_schema["total"], 0);
    assert!(
        unknown_catalog_schema["items"]
            .as_array()
            .expect("items")
            .is_empty()
    );
    assert_matches_output_schema(list_catalog_tool, &unknown_catalog_schema);

    client
        .call_tool(
            CallToolRequestParams::new("list_catalog").with_arguments(json_object(&json!({
                "limit": 0
            }))),
        )
        .await
        .expect_err("limit zero should be invalid");

    client
        .call_tool(
            CallToolRequestParams::new("list_catalog").with_arguments(json_object(&json!({
                "kind": "invalid"
            }))),
        )
        .await
        .expect_err("invalid catalog kind should fail");

    let search = client
        .call_tool(
            CallToolRequestParams::new("search_catalog").with_arguments(json_object(&json!({
                "pattern": "^MESSAGES$",
                "schema": "local_messages",
                "kind": "table",
                "ignore_case": true
            }))),
        )
        .await
        .expect("search catalog");
    let search = search.structured_content.expect("structured content");
    assert_eq!(search["total"], 1);
    assert_eq!(search["items"][0]["name"], "local_messages.messages");
    assert_eq!(
        search["items"][0]["sql_reference"],
        "local_messages.messages"
    );
    assert!(
        search["items"][0]["table"]["guide"].is_string(),
        "search results should always expose guide text, even when empty"
    );
    assert!(
        search["items"][0]["matched_fields"]
            .as_array()
            .expect("matched fields")
            .iter()
            .any(|field| field == "table_name")
    );
    assert_matches_output_schema(search_catalog_tool, &search);

    let search_page = client
        .call_tool(
            CallToolRequestParams::new("search_catalog").with_arguments(json_object(&json!({
                "pattern": "Fixture",
                "schema": "local_messages",
                "limit": 2
            }))),
        )
        .await
        .expect("search table page");
    let search_page = search_page.structured_content.expect("structured content");
    assert_eq!(search_page["total"], 3);
    assert_eq!(search_page["limit"], 2);
    assert_eq!(search_page["has_more"], true);
    assert_eq!(search_page["next_offset"], 2);
    assert_matches_output_schema(search_catalog_tool, &search_page);

    client
        .call_tool(
            CallToolRequestParams::new("search_catalog").with_arguments(json_object(&json!({
                "pattern": "["
            }))),
        )
        .await
        .expect_err("invalid regex should fail");

    let described = client
        .call_tool(
            CallToolRequestParams::new("describe_table").with_arguments(json_object(&json!({
                "schema": "local_messages",
                "table": "messages"
            }))),
        )
        .await
        .expect("describe table");
    let described = described.structured_content.expect("structured content");
    assert_eq!(described["found"], true);
    assert_eq!(described["name"], "local_messages.messages");
    assert_eq!(described["column_count"], 3);
    assert!(described["columns_hint"].as_str().is_some());
    assert!(described["columns"].is_null());

    let missing_table = client
        .call_tool(
            CallToolRequestParams::new("describe_table").with_arguments(json_object(&json!({
                "schema": "local_messages",
                "table": "missing"
            }))),
        )
        .await
        .expect("describe missing table");
    assert_eq!(missing_table.is_error, Some(false));
    let missing_table = missing_table
        .structured_content
        .expect("structured content");
    assert_eq!(missing_table["found"], false);
    assert_eq!(missing_table["requested"]["schema"], "local_messages");
    assert_eq!(missing_table["requested"]["table"], "missing");
    assert_eq!(
        missing_table["same_schema_tables"][0]["name"],
        "local_messages.events"
    );
    assert_eq!(
        missing_table["suggestions"][0]["name"],
        "local_messages.events"
    );
    assert_eq!(
        missing_table["suggested_calls"][0]["tool"],
        "search_catalog"
    );
    assert_eq!(
        missing_table["suggested_calls"][0]["arguments"]["pattern"],
        "missing"
    );
    assert_eq!(
        missing_table["suggested_calls"][0]["arguments"]["schema"],
        "local_messages"
    );

    let missing_schema = client
        .call_tool(
            CallToolRequestParams::new("describe_table").with_arguments(json_object(&json!({
                "schema": "local_mesages",
                "table": "missing["
            }))),
        )
        .await
        .expect("describe missing schema");
    assert_eq!(missing_schema.is_error, Some(false));
    let missing_schema = missing_schema
        .structured_content
        .expect("structured content");
    assert_eq!(missing_schema["found"], false);
    assert_eq!(
        missing_schema["suggested_calls"][0]["arguments"]["pattern"],
        r"missing\["
    );
    assert!(
        missing_schema["suggested_calls"][0]["arguments"]["schema"].is_null(),
        "search suggestion should not constrain a missing schema"
    );

    client
        .call_tool(
            CallToolRequestParams::new("describe_table").with_arguments(json_object(&json!({
                "schema": "local_messages",
                "table": " "
            }))),
        )
        .await
        .expect_err("blank table should fail");

    let columns = client
        .call_tool(
            CallToolRequestParams::new("list_columns").with_arguments(json_object(&json!({
                "schema": "local_messages",
                "table": "messages",
                "limit": 2
            }))),
        )
        .await
        .expect("list columns");
    let columns = columns.structured_content.expect("structured content");
    assert_eq!(columns["schema_name"], "local_messages");
    assert_eq!(columns["table_name"], "messages");
    assert_eq!(columns["total"], 3);
    assert_eq!(columns["limit"], 2);
    assert_eq!(columns["has_more"], true);
    assert_eq!(columns["next_offset"], 2);
    assert_eq!(columns["columns"][0]["column_name"], "type");
    assert_eq!(columns["columns"][0]["data_type"], "Utf8");
    assert_matches_output_schema(list_columns_tool, &columns);

    let required_columns = client
        .call_tool(
            CallToolRequestParams::new("list_columns").with_arguments(json_object(&json!({
                "schema": "local_messages",
                "table": "sessions",
                "required_only": true
            }))),
        )
        .await
        .expect("list required columns");
    let required_columns = required_columns
        .structured_content
        .expect("structured content");
    assert_eq!(required_columns["total"], 0);
    assert_matches_output_schema(list_columns_tool, &required_columns);

    let filtered_columns = client
        .call_tool(
            CallToolRequestParams::new("list_columns").with_arguments(json_object(&json!({
                "schema": "local_messages",
                "table": "messages",
                "pattern": "SESSION"
            }))),
        )
        .await
        .expect("list filtered columns");
    let filtered_columns = filtered_columns
        .structured_content
        .expect("structured content");
    assert_eq!(filtered_columns["total"], 1);
    assert_eq!(filtered_columns["columns"][0]["column_name"], "sessionId");
    assert!(
        filtered_columns["columns"][0]["matched_fields"]
            .as_array()
            .expect("matched fields")
            .iter()
            .any(|field| field == "column_name")
    );
    assert_matches_output_schema(list_columns_tool, &filtered_columns);

    let empty_column_filter = client
        .call_tool(
            CallToolRequestParams::new("list_columns").with_arguments(json_object(&json!({
                "schema": "local_messages",
                "table": "messages",
                "pattern": "does-not-match"
            }))),
        )
        .await
        .expect("list filtered columns with no matches");
    let empty_column_filter = empty_column_filter
        .structured_content
        .expect("structured content");
    assert!(empty_column_filter["found"].is_null());
    assert_eq!(empty_column_filter["schema_name"], "local_messages");
    assert_eq!(empty_column_filter["table_name"], "messages");
    assert_eq!(empty_column_filter["total"], 0);
    assert!(
        empty_column_filter["columns"]
            .as_array()
            .expect("columns")
            .is_empty()
    );
    assert_matches_output_schema(list_columns_tool, &empty_column_filter);

    let missing_columns = client
        .call_tool(
            CallToolRequestParams::new("list_columns").with_arguments(json_object(&json!({
                "schema": "local_messages",
                "table": "missing"
            }))),
        )
        .await
        .expect("list columns for missing table");
    let missing_columns = missing_columns
        .structured_content
        .expect("structured content");
    assert_eq!(missing_columns["found"], false);
    assert_eq!(missing_columns["requested"]["schema"], "local_messages");
    assert_eq!(missing_columns["requested"]["table"], "missing");
    assert_eq!(
        missing_columns["same_schema_tables"][0]["name"],
        "local_messages.events"
    );
    assert_eq!(
        missing_columns["suggestions"][0]["name"],
        "local_messages.events"
    );
    assert_eq!(
        missing_columns["suggested_calls"][0]["arguments"]["schema"],
        "local_messages"
    );
    assert_matches_output_schema(list_columns_tool, &missing_columns);

    let missing_columns_with_bad_pattern = client
        .call_tool(
            CallToolRequestParams::new("list_columns").with_arguments(json_object(&json!({
                "schema": "local_messages",
                "table": "missing",
                "pattern": "["
            }))),
        )
        .await
        .expect("list columns for missing table with bad pattern");
    let missing_columns_with_bad_pattern = missing_columns_with_bad_pattern
        .structured_content
        .expect("structured content");
    assert_eq!(missing_columns_with_bad_pattern["found"], false);
    assert_matches_output_schema(list_columns_tool, &missing_columns_with_bad_pattern);

    client
        .call_tool(
            CallToolRequestParams::new("list_columns").with_arguments(json_object(&json!({
                "schema": "local_messages",
                "table": "messages",
                "pattern": ""
            }))),
        )
        .await
        .expect_err("empty column regex should fail");

    session.shutdown().await;
}

#[tokio::test]
async fn list_catalog_surfaces_table_functions() {
    let temp = TempDir::new().expect("temp dir");
    let manifest_path = write_function_fixture_manifest(temp.path());
    let manifest_yaml = fs::read_to_string(&manifest_path).expect("read manifest");
    let mut session = start_session(&temp).await;
    let client = &session.client;

    add_demo_source(&mut session.source_client, manifest_yaml).await;

    let tools = client.list_all_tools().await.expect("tools");
    assert!(
        tool_by_name(&tools, "list_catalog")
            .description
            .as_deref()
            .expect("catalog description")
            .contains("6 table(s) and 2 table function(s) are currently visible")
    );
    assert!(
        tool_by_name(&tools, "search_catalog")
            .description
            .as_deref()
            .expect("catalog search description")
            .contains("Connected sources/schemas include: searchy")
    );
    assert!(tools.iter().all(|tool| tool.name != "list_tables"));
    assert!(tools.iter().all(|tool| tool.name != "search_tables"));

    let catalog_tool = tool_by_name(&tools, "list_catalog");
    let search_tool = tool_by_name(&tools, "search_catalog");
    let catalog = client
        .call_tool(
            CallToolRequestParams::new("list_catalog").with_arguments(json_object(&json!({
                "schema": "searchy"
            }))),
        )
        .await
        .expect("list catalog")
        .structured_content
        .expect("structured catalog");
    assert_eq!(catalog["total"], 3);
    assert_eq!(catalog["items"][0]["kind"], "table_function");
    assert_eq!(catalog["items"][0]["name"], "searchy.lookup_issue");
    assert_eq!(catalog["items"][0]["sql_reference"], "searchy.lookup_issue");
    assert_eq!(
        catalog["items"][0]["sql_call_example"],
        "searchy.lookup_issue(number => '<value>')"
    );
    assert_eq!(
        catalog["items"][0]["table_function"]["arguments"][0]["name"],
        "number"
    );
    assert_eq!(
        catalog["items"][0]["table_function"]["result_columns"][0]["column_name"],
        "title"
    );
    assert_eq!(catalog["items"][1]["kind"], "table");
    assert_eq!(catalog["items"][1]["name"], "searchy.placeholder");
    assert_matches_output_schema(catalog_tool, &catalog);

    let functions = client
        .call_tool(
            CallToolRequestParams::new("list_catalog").with_arguments(json_object(&json!({
                "kind": "table_function",
                "limit": 1,
                "offset": 1
            }))),
        )
        .await
        .expect("list table functions")
        .structured_content
        .expect("structured functions");
    assert_eq!(functions["total"], 2);
    assert_eq!(functions["limit"], 1);
    assert_eq!(functions["offset"], 1);
    assert_eq!(functions["has_more"], false);
    assert_eq!(functions["items"][0]["name"], "searchy.search_issues");
    assert_eq!(
        functions["items"][0]["sql_call_example"],
        "searchy.search_issues(q => '<value>')"
    );
    assert_matches_output_schema(catalog_tool, &functions);

    let search = client
        .call_tool(
            CallToolRequestParams::new("search_catalog").with_arguments(json_object(&json!({
                "pattern": "hybrid",
                "kind": "table_function"
            }))),
        )
        .await
        .expect("search table functions")
        .structured_content
        .expect("structured search");
    assert_eq!(search["total"], 1);
    assert_eq!(search["items"][0]["kind"], "table_function");
    assert_eq!(search["items"][0]["name"], "searchy.search_issues");
    assert!(
        search["items"][0]["matched_fields"]
            .as_array()
            .expect("matched fields")
            .iter()
            .any(|field| field == "arguments")
    );
    assert_matches_output_schema(search_tool, &search);

    session.shutdown().await;
}

#[tokio::test]
async fn mcp_feedback_tool_persists_blocked_agent_report() {
    let temp = TempDir::new().expect("temp dir");
    let session = start_session_with_options(
        &temp,
        McpOptions {
            feedback_enabled: true,
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
        vec![
            "sql",
            "list_catalog",
            "search_catalog",
            "describe_table",
            "list_columns",
            "feedback"
        ]
    );
    let feedback_annotations = tools[5].annotations.as_ref().expect("feedback annotations");
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
async fn mcp_feedback_tool_accepts_episode_id_when_episodes_enabled() {
    let temp = TempDir::new().expect("temp dir");
    let session = start_session_with_options(
        &temp,
        McpOptions {
            episodes_enabled: true,
            feedback_enabled: true,
            ..McpOptions::default()
        },
    )
    .await;
    let client = &session.client;

    let tools = client.list_all_tools().await.expect("tools");
    assert_tool_advertises_episode_id(tool_by_name(&tools, "feedback"));

    let feedback = client
        .call_tool(
            CallToolRequestParams::new("feedback").with_arguments(json_object(&json!({
                "trying_to_do": "Finish an episode-scoped task",
                "tried": "Opened an episode and inspected failing output",
                "stuck": "The final step still needs user judgment",
                "episode_id": "ep_failed_followup"
            }))),
        )
        .await
        .expect("episode-tagged feedback");
    assert_eq!(feedback.is_error, Some(false));
    assert_eq!(
        feedback.structured_content.expect("structured content")["message"],
        "Feedback report stored."
    );

    let invalid_episode_id = client
        .call_tool(
            CallToolRequestParams::new("feedback").with_arguments(json_object(&json!({
                "trying_to_do": "Finish an episode-scoped task",
                "tried": "Opened an episode and inspected failing output",
                "stuck": "The final step still needs user judgment",
                "episode_id": "has space"
            }))),
        )
        .await
        .expect_err("invalid episode_id should fail before feedback dispatch");
    assert!(
        invalid_episode_id
            .to_string()
            .contains("argument 'episode_id' must be graphic ASCII")
    );

    session.shutdown().await;
}

#[tokio::test]
async fn mcp_feedback_tool_is_disabled_by_default() {
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
        .expect_err("feedback should not be exposed by default");
    assert!(feedback.to_string().contains("tool 'feedback' not found"));
    assert!(
        !temp
            .path()
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
        .expect("sql");
    assert_eq!(
        sql.structured_content.expect("structured content")["rows"][0]["text"],
        "hello"
    );
    assert_eq!(sql.is_error, Some(false));

    let invalid_sql = client
        .call_tool(
            CallToolRequestParams::new("sql").with_arguments(json_object(&json!({
                "sql": "DELETE FROM local_messages.messages"
            }))),
        )
        .await
        .expect("failing sql still returns tool result");
    assert_eq!(invalid_sql.is_error, Some(true));
    assert_eq!(
        invalid_sql.structured_content.expect("structured content")["error"]["summary"],
        "Query request is invalid"
    );
    assert!(
        invalid_sql.content[0]
            .as_text()
            .expect("text content")
            .text
            .contains("Detail:")
    );

    let catalog_after_error = client
        .call_tool(
            CallToolRequestParams::new("list_catalog").with_arguments(json_object(&json!({
                "schema": "local_messages"
            }))),
        )
        .await
        .expect("list catalog after error");
    let structured_catalog_after_error = catalog_after_error
        .structured_content
        .expect("structured content");
    assert_eq!(
        structured_catalog_after_error["items"][0]["name"],
        "local_messages.events"
    );
    assert_eq!(
        structured_catalog_after_error["items"][0]["sql_reference"],
        "local_messages.events"
    );
    assert_eq!(catalog_after_error.is_error, Some(false));

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

    let sql = client
        .call_tool(
            CallToolRequestParams::new("sql").with_arguments(json_object(&json!({
                "sql": "SELECT CAST(-8504475857937456387 AS BIGINT) AS user_id"
            }))),
        )
        .await
        .expect("sql");
    assert_eq!(sql.is_error, Some(false));

    let rows = &sql.structured_content.expect("structured content")["rows"];
    assert_eq!(
        rows[0]["user_id"],
        Value::String("-8504475857937456387".to_string()),
    );

    session.shutdown().await;
}
