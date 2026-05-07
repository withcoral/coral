use std::fs;
use std::path::{Path, PathBuf};

use coral_api::v1::ImportSourceRequest;
use coral_client::{
    AppClient, SourceClient, default_workspace,
    local::{RunningServer, ServerBuilder},
};
use rmcp::{
    RoleClient, ServiceExt,
    model::{CallToolRequestParams, ReadResourceRequestParams},
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
        let server = CoralMcpServer::new(&app, options)
            .serve(server_transport)
            .await?;
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

#[tokio::test]
#[allow(
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
        vec!["sql", "list_tables"]
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
    assert!(structured_tables["tables"][0]["columns"].is_null());
    assert_eq!(tables.is_error, Some(false));

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

    client
        .call_tool(
            CallToolRequestParams::new("list_tables").with_arguments(json_object(&json!({
                "limit": 0
            }))),
        )
        .await
        .expect_err("limit zero should be invalid");

    session.shutdown().await;
}

#[tokio::test]
async fn mcp_feedback_tool_persists_blocked_agent_report() {
    let temp = TempDir::new().expect("temp dir");
    let session = start_session_with_options(
        &temp,
        McpOptions {
            feedback_enabled: true,
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
        vec!["sql", "list_tables", "feedback"]
    );
    let feedback_annotations = tools[2].annotations.as_ref().expect("feedback annotations");
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
    assert_eq!(
        tables_after_error
            .structured_content
            .expect("structured content")["tables"][0]["name"],
        "local_messages.events"
    );
    assert_eq!(tables_after_error.is_error, Some(false));

    session.shutdown().await;
}
