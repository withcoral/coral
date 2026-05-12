#![allow(
    clippy::indexing_slicing,
    clippy::string_slice,
    reason = "test code: assertion-style indexing is idiomatic in tests"
)]

use std::fs;
use std::path::{Path, PathBuf};

use coral_api::v1::ImportSourceRequest;
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
backend: jsonl
tables:
  - name: events
    description: Fixture events
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
    filters:
      - name: sessionId
        required: true
"#,
        data_dir.display(),
        data_dir.display(),
        data_dir.display()
    );
    let manifest_path = source_dir.join("source.yaml");
    fs::write(&manifest_path, manifest).expect("write manifest");
    manifest_path
}

fn json_object(value: &Value) -> Map<String, Value> {
    value.as_object().cloned().expect("json object")
}

async fn add_demo_source(source_client: &mut SourceClient, manifest_yaml: String) {
    source_client
        .import_source(Request::new(ImportSourceRequest {
            workspace: Some(default_workspace()),
            manifest_yaml,
            variables: Vec::new(),
            secrets: Vec::new(),
        }))
        .await
        .expect("add source");
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
            "list_tables",
            "search_tables",
            "describe_table",
            "list_columns"
        ]
    );
    assert!(
        initial_tools[0]
            .description
            .as_deref()
            .expect("sql description")
            .contains("0 configured source")
    );
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
            .contains("0 configured source")
    );

    let initial_guide = client
        .read_resource(ReadResourceRequestParams::new("coral://guide"))
        .await
        .expect("initial guide");
    let initial_guide_text = text_content(&initial_guide);
    assert!(initial_guide_text.contains("## Available Schemas"));
    assert!(initial_guide_text.contains("- coral: System metadata schema."));
    assert!(initial_guide_text.contains("No source schemas are currently configured."));
    assert!(initial_guide_text.contains("schema_name = '<schema>'"));

    add_demo_source(&mut session.source_client, manifest_yaml).await;

    let updated_tools = client.list_all_tools().await.expect("updated tools");
    let list_tables_tool = tool_by_name(&updated_tools, "list_tables");
    let search_tables_tool = tool_by_name(&updated_tools, "search_tables");
    let list_columns_tool = tool_by_name(&updated_tools, "list_columns");
    assert!(
        updated_tools[0]
            .description
            .as_deref()
            .expect("sql description")
            .contains("3 table(s) are currently visible")
    );
    assert!(
        updated_tools[1]
            .description
            .as_deref()
            .expect("tables description")
            .contains("3 table(s) are currently visible")
    );
    assert!(
        updated_tools[2]
            .description
            .as_deref()
            .expect("table search description")
            .contains("3 table(s) are currently visible")
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
            .contains("1 configured source")
    );

    let tables_resource = client
        .read_resource(ReadResourceRequestParams::new("coral://tables"))
        .await
        .expect("read tables resource");
    let tables_text = text_content(&tables_resource);
    let tables_json =
        serde_json::from_str::<serde_json::Value>(tables_text).expect("parse tables resource");
    assert_eq!(tables_json["tables"][0]["name"], "local_messages.events");
    assert_eq!(
        tables_json["tables"][0]["sql_reference"],
        "local_messages.events"
    );

    let updated_guide = client
        .read_resource(ReadResourceRequestParams::new("coral://guide"))
        .await
        .expect("updated guide");
    let updated_guide_text = text_content(&updated_guide);
    assert!(updated_guide_text.contains("## Available Schemas"));
    assert!(updated_guide_text.contains("- coral: System metadata schema."));
    assert!(updated_guide_text.contains("- local_messages"));
    assert!(!updated_guide_text.contains("## Visible SQL Schemas"));
    assert!(updated_guide_text.contains(
        "FROM coral.columns WHERE schema_name = 'local_messages' AND table_name = 'events'"
    ));

    let tables = client
        .call_tool(CallToolRequestParams::new("list_tables"))
        .await
        .expect("list tables");
    let structured_tables = tables.structured_content.expect("structured content");
    assert_eq!(structured_tables["total"], 3);
    assert_eq!(structured_tables["limit"], 50);
    assert_eq!(structured_tables["offset"], 0);
    assert_eq!(structured_tables["has_more"], false);
    assert_eq!(
        structured_tables["tables"][0]["name"],
        "local_messages.events"
    );
    assert_eq!(
        structured_tables["tables"][0]["sql_reference"],
        "local_messages.events"
    );
    assert!(structured_tables["tables"][0]["columns"].is_null());
    assert_eq!(tables.is_error, Some(false));
    assert_matches_output_schema(list_tables_tool, &structured_tables);

    let page = client
        .call_tool(
            CallToolRequestParams::new("list_tables").with_arguments(json_object(&json!({
                "schema": "local_messages",
                "limit": 2,
                "offset": 0
            }))),
        )
        .await
        .expect("list paginated tables");
    let page = page.structured_content.expect("structured content");
    assert_eq!(page["total"], 3);
    assert_eq!(page["limit"], 2);
    assert_eq!(page["has_more"], true);
    assert_eq!(page["next_offset"], 2);
    assert_eq!(page["tables"].as_array().expect("tables").len(), 2);
    assert_matches_output_schema(list_tables_tool, &page);

    let unknown_schema = client
        .call_tool(
            CallToolRequestParams::new("list_tables").with_arguments(json_object(&json!({
                "schema": "missing",
                "limit": 2,
                "offset": 0
            }))),
        )
        .await
        .expect("list unknown schema");
    let unknown_schema = unknown_schema
        .structured_content
        .expect("structured content");
    assert_eq!(unknown_schema["total"], 0);
    assert!(
        unknown_schema["tables"]
            .as_array()
            .expect("tables")
            .is_empty()
    );
    assert_matches_output_schema(list_tables_tool, &unknown_schema);

    client
        .call_tool(
            CallToolRequestParams::new("list_tables").with_arguments(json_object(&json!({
                "limit": 0
            }))),
        )
        .await
        .expect_err("limit zero should be invalid");

    let search = client
        .call_tool(
            CallToolRequestParams::new("search_tables").with_arguments(json_object(&json!({
                "pattern": "^MESSAGES$",
                "schema": "local_messages",
                "ignore_case": true
            }))),
        )
        .await
        .expect("search tables");
    let search = search.structured_content.expect("structured content");
    assert_eq!(search["total"], 1);
    assert_eq!(search["tables"][0]["name"], "local_messages.messages");
    assert_eq!(
        search["tables"][0]["sql_reference"],
        "local_messages.messages"
    );
    assert!(
        search["tables"][0]["guide"].is_string(),
        "search results should always expose guide text, even when empty"
    );
    assert!(
        search["tables"][0]["matched_fields"]
            .as_array()
            .expect("matched fields")
            .iter()
            .any(|field| field == "table_name")
    );
    assert_matches_output_schema(search_tables_tool, &search);

    let search_page = client
        .call_tool(
            CallToolRequestParams::new("search_tables").with_arguments(json_object(&json!({
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
    assert_matches_output_schema(search_tables_tool, &search_page);

    client
        .call_tool(
            CallToolRequestParams::new("search_tables").with_arguments(json_object(&json!({
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
    assert_eq!(missing_table["suggested_calls"][0]["tool"], "search_tables");
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
    assert_eq!(required_columns["total"], 1);
    assert_eq!(required_columns["columns"][0]["column_name"], "sessionId");
    assert_eq!(required_columns["columns"][0]["is_required_filter"], true);
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
        missing_columns["suggested_calls"][0]["arguments"]["schema"],
        "local_messages"
    );
    assert_matches_output_schema(list_columns_tool, &missing_columns);

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
            "list_tables",
            "search_tables",
            "describe_table",
            "list_columns",
            "feedback"
        ]
    );
    let feedback_annotations = tools[5].annotations.as_ref().expect("feedback annotations");
    assert_eq!(feedback_annotations.read_only_hint, Some(false));
    assert_eq!(feedback_annotations.destructive_hint, Some(false));
    assert_eq!(feedback_annotations.idempotent_hint, Some(false));
    assert_eq!(feedback_annotations.open_world_hint, Some(false));

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

    let tables_after_error = client
        .call_tool(CallToolRequestParams::new("list_tables"))
        .await
        .expect("list tables after error");
    let structured_tables_after_error = tables_after_error
        .structured_content
        .expect("structured content");
    assert_eq!(
        structured_tables_after_error["tables"][0]["name"],
        "local_messages.events"
    );
    assert_eq!(
        structured_tables_after_error["tables"][0]["sql_reference"],
        "local_messages.events"
    );
    assert_eq!(tables_after_error.is_error, Some(false));

    session.shutdown().await;
}
