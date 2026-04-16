use std::fs;
use std::path::{Path, PathBuf};

use coral_api::v1::ImportSourceRequest;
use coral_client::{AppClient, SourceClient, default_workspace, local::ServerBuilder};
use rmcp::{ServiceExt, model::CallToolRequestParams};
use serde_json::{Map, Value, json};
use tempfile::TempDir;
use tonic::Request;

use crate::CoralMcpServer;

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
"#,
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

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "This end-to-end MCP test intentionally verifies discovery refresh, guide rendering, success, and failure recovery in one session."
)]
async fn mcp_surface_refreshes_and_renders_dynamic_guide() {
    let temp = TempDir::new().expect("temp dir");
    let manifest_path = write_fixture_manifest(temp.path());
    let manifest_yaml = fs::read_to_string(&manifest_path).expect("read manifest");
    let server = ServerBuilder::new()
        .with_config_dir(temp.path().join("coral-config"))
        .start()
        .await
        .expect("start server");
    let app = AppClient::connect(server.endpoint_uri())
        .await
        .expect("connect client");
    let mut source_client = app.source_client();

    let (server_transport, client_transport) = tokio::io::duplex(4096);
    let server_handle = tokio::spawn(async move {
        let server = CoralMcpServer::new(&app).serve(server_transport).await?;
        server.waiting().await?;
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    });
    let client = ().serve(client_transport).await.expect("start rmcp client");

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
        .read_resource(rmcp::model::ReadResourceRequestParams::new("coral://guide"))
        .await
        .expect("initial guide");
    let initial_guide_text = match &initial_guide.contents[0] {
        rmcp::model::ResourceContents::TextResourceContents { text, .. } => text,
        other @ rmcp::model::ResourceContents::BlobResourceContents { .. } => {
            panic!("unexpected guide contents: {other:?}")
        }
    };
    assert!(initial_guide_text.contains("## Available Schemas"));
    assert!(initial_guide_text.contains("- coral: System metadata schema."));
    assert!(initial_guide_text.contains("No source schemas are currently configured."));
    assert!(initial_guide_text.contains("schema_name = '<schema>'"));

    add_demo_source(&mut source_client, manifest_yaml).await;

    let updated_tools = client.list_all_tools().await.expect("updated tools");
    assert!(
        updated_tools[0]
            .description
            .as_deref()
            .expect("sql description")
            .contains("1 visible SQL schema(s) are currently available")
    );
    assert!(
        updated_tools[1]
            .description
            .as_deref()
            .expect("tables description")
            .contains("1 table(s) are currently visible")
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
        .read_resource(rmcp::model::ReadResourceRequestParams::new(
            "coral://tables",
        ))
        .await
        .expect("read tables resource");
    let tables_text = match &tables_resource.contents[0] {
        rmcp::model::ResourceContents::TextResourceContents { text, .. } => text,
        other @ rmcp::model::ResourceContents::BlobResourceContents { .. } => {
            panic!("unexpected resource contents: {other:?}")
        }
    };
    let tables_json =
        serde_json::from_str::<serde_json::Value>(tables_text).expect("parse tables resource");
    assert_eq!(tables_json["tables"][0]["name"], "local_messages.messages");

    let updated_guide = client
        .read_resource(rmcp::model::ReadResourceRequestParams::new("coral://guide"))
        .await
        .expect("updated guide");
    let updated_guide_text = match &updated_guide.contents[0] {
        rmcp::model::ResourceContents::TextResourceContents { text, .. } => text,
        other @ rmcp::model::ResourceContents::BlobResourceContents { .. } => {
            panic!("unexpected guide contents: {other:?}")
        }
    };
    assert!(updated_guide_text.contains("## Available Schemas"));
    assert!(updated_guide_text.contains("- coral: System metadata schema."));
    assert!(updated_guide_text.contains("- local_messages"));
    assert!(!updated_guide_text.contains("## Visible SQL Schemas"));
    assert!(updated_guide_text.contains(
        "FROM coral.columns WHERE schema_name = 'local_messages' AND table_name = 'messages'"
    ));

    let tables = client
        .call_tool(CallToolRequestParams::new("list_tables"))
        .await
        .expect("list tables");
    assert_eq!(
        tables.structured_content.expect("structured content")["tables"][0]["name"],
        "local_messages.messages"
    );
    assert_eq!(tables.is_error, Some(false));

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
        "local_messages.messages"
    );
    assert_eq!(tables_after_error.is_error, Some(false));

    client.cancel().await.expect("cancel client");
    server_handle
        .await
        .expect("join server")
        .expect("server result");
}
