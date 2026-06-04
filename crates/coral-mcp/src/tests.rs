#![allow(
    clippy::indexing_slicing,
    clippy::string_slice,
    reason = "test code: assertion-style indexing is idiomatic in tests"
)]

use std::fs;
use std::path::{Path, PathBuf};

use coral_api::v1::{ImportSourceRequest, import_source_response};
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

#[cfg(feature = "code-mode")]
fn assert_code_mode_annotations(tools: &[Tool], open_world_hint: bool) {
    for name in ["exec", "wait"] {
        let annotations = tool_by_name(tools, name)
            .annotations
            .as_ref()
            .unwrap_or_else(|| panic!("{name} annotations"));
        assert_eq!(annotations.read_only_hint, Some(false));
        assert_eq!(annotations.open_world_hint, Some(open_world_hint));
    }
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

fn assert_matches_input_schema(tool: &Tool, value: &Value) {
    let schema = Value::Object(tool.input_schema.as_ref().clone());
    let compiled = JSONSchema::compile(&schema).expect("tool input schema should compile");
    if let Err(errors) = compiled.validate(value) {
        let details = errors
            .map(|error| error.to_string())
            .collect::<Vec<_>>()
            .join("; ");
        panic!(
            "tool '{}' input did not match input schema: {details}",
            tool.name
        );
    }
}

fn assert_rejected_by_input_schema(tool: &Tool, value: &Value) {
    let schema = Value::Object(tool.input_schema.as_ref().clone());
    let compiled = JSONSchema::compile(&schema).expect("tool input schema should compile");
    assert!(
        compiled.validate(value).is_err(),
        "tool '{}' input should not match input schema: {value}",
        tool.name
    );
}

fn assert_property_schemas_are_objects(tool: &Tool) {
    let Some(output_schema) = &tool.output_schema else {
        return;
    };
    let schema = Value::Object(output_schema.as_ref().clone());
    assert_property_schemas_are_objects_at(tool.name.as_ref(), "$", &schema);
}

fn assert_property_schemas_are_objects_at(tool_name: &str, path: &str, schema: &Value) {
    let Value::Object(object) = schema else {
        return;
    };

    if let Some(Value::Object(properties)) = object.get("properties") {
        for (property_name, property_schema) in properties {
            assert!(
                property_schema.is_object(),
                "tool '{tool_name}' output schema property {path}.properties.{property_name} must be an object schema for strict MCP clients"
            );
            assert_property_schemas_are_objects_at(
                tool_name,
                &format!("{path}.properties.{property_name}"),
                property_schema,
            );
        }
    }

    for keyword in [
        "items",
        "additionalItems",
        "contains",
        "not",
        "if",
        "then",
        "else",
    ] {
        if let Some(child) = object.get(keyword) {
            assert_property_schemas_are_objects_at(tool_name, &format!("{path}.{keyword}"), child);
        }
    }
    for keyword in ["allOf", "anyOf", "oneOf"] {
        if let Some(Value::Array(children)) = object.get(keyword) {
            for (index, child) in children.iter().enumerate() {
                assert_property_schemas_are_objects_at(
                    tool_name,
                    &format!("{path}.{keyword}[{index}]"),
                    child,
                );
            }
        }
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
            .contains("no visible SQL tables are currently available")
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
        assert_property_schemas_are_objects(tool);
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
            .contains("0 visible table")
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
            .contains("3 table(s) are currently visible")
    );
    assert!(
        updated_tools[1]
            .description
            .as_deref()
            .expect("catalog description")
            .contains("3 table(s) and 0 table function(s) are currently visible")
    );
    assert!(
        updated_tools[2]
            .description
            .as_deref()
            .expect("catalog search description")
            .contains("3 table(s) and 0 table function(s) are currently visible")
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
    assert_eq!(catalog["total"], 3);
    assert_eq!(catalog["items"][0]["kind"], "table");
    assert_eq!(catalog["items"][0]["name"], "local_messages.events");
    assert_eq!(
        catalog["items"][0]["sql_reference"],
        "local_messages.events"
    );
    assert_eq!(catalog["items"][0]["table"]["table_name"], "events");
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

    let system_catalog = client
        .call_tool(
            CallToolRequestParams::new("list_catalog").with_arguments(json_object(&json!({
                "schema": "coral",
                "kind": "table",
                "limit": 10,
                "offset": 0
            }))),
        )
        .await
        .expect("list system catalog schema")
        .structured_content
        .expect("structured system catalog");
    assert_eq!(system_catalog["total"], 5);
    assert_eq!(system_catalog["items"][0]["name"], "coral.columns");
    assert_eq!(system_catalog["items"][4]["name"], "coral.tables");
    assert_matches_output_schema(list_catalog_tool, &system_catalog);

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

    let system_search = client
        .call_tool(
            CallToolRequestParams::new("search_catalog").with_arguments(json_object(&json!({
                "pattern": "^coral\\.",
                "kind": "table",
                "limit": 10
            }))),
        )
        .await
        .expect("search system catalog")
        .structured_content
        .expect("structured system search");
    assert_eq!(system_search["total"], 5);
    assert_eq!(system_search["items"][0]["name"], "coral.columns");
    assert!(
        system_search["items"][0]["matched_fields"]
            .as_array()
            .expect("matched fields")
            .iter()
            .any(|field| field == "name")
    );
    assert_matches_output_schema(search_catalog_tool, &system_search);

    let uppercase_system_search = client
        .call_tool(
            CallToolRequestParams::new("search_catalog").with_arguments(json_object(&json!({
                "pattern": "^CORAL\\.",
                "kind": "table",
                "limit": 10
            }))),
        )
        .await
        .expect("case-insensitive system catalog search")
        .structured_content
        .expect("structured case-insensitive system search");
    assert_eq!(uppercase_system_search["total"], 5);
    assert_eq!(uppercase_system_search["items"][0]["name"], "coral.columns");

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
            .contains("1 table(s) and 2 table function(s) are currently visible")
    );
    assert!(tools.iter().all(|tool| tool.name != "list_tables"));
    assert!(tools.iter().all(|tool| tool.name != "search_tables"));

    let catalog_tool = tool_by_name(&tools, "list_catalog");
    let search_tool = tool_by_name(&tools, "search_catalog");
    let catalog = client
        .call_tool(CallToolRequestParams::new("list_catalog"))
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

#[cfg(feature = "code-mode")]
#[tokio::test]
async fn mcp_code_mode_exec_sql_matches_direct_sql() {
    let temp = TempDir::new().expect("temp dir");
    let manifest_path = write_fixture_manifest(temp.path());
    let manifest_yaml = fs::read_to_string(&manifest_path).expect("read manifest");
    let mut session = start_session_with_options(
        &temp,
        McpOptions {
            code_mode_enabled: true,
            feedback_enabled: true,
            ..McpOptions::default()
        },
    )
    .await;
    let client = &session.client;

    add_demo_source(&mut session.source_client, manifest_yaml).await;

    assert_code_mode_tool_surface(client).await;
    assert_direct_sql_parameter_validation(client).await;
    assert_exec_sql_matches_direct_sql(client).await;
    assert_exec_sql_parameters(client).await;
    assert_exec_limit_zero_and_arrow_function(client).await;
    assert_exec_function_declarations(client).await;
    assert_direct_and_nested_sql_reject_writes(client).await;
    assert_exec_catalog_matches_direct(client).await;
    assert_exec_system_catalog_matches_direct(client).await;
    assert_exec_feedback(client).await;

    session.shutdown().await;
}

#[cfg(feature = "code-mode")]
async fn assert_code_mode_tool_surface(client: &RunningService<RoleClient, ()>) {
    let tools = client.list_all_tools().await.expect("tools");
    let tool_names = tools
        .iter()
        .map(|tool| tool.name.as_ref())
        .collect::<Vec<_>>();
    assert!(tool_names.contains(&"sql"));
    assert!(tool_names.contains(&"exec"));
    assert!(tool_names.contains(&"wait"));
    assert_code_mode_annotations(&tools, true);
    for tool in &tools {
        assert_property_schemas_are_objects(tool);
    }
    let exec_tool = tool_by_name(&tools, "exec");
    let exec_description = exec_tool.description.as_deref().expect("exec description");
    assert!(exec_description.starts_with("Coral SQL-first guidance"));
    assert!(exec_description.contains("not a replacement for SQL"));
    assert!(exec_description.contains("default to one `tools.sql` query"));
    assert!(exec_description.contains("joins across tables or sources"));
    assert!(exec_description.contains("Do not fetch multiple tables into JavaScript"));
    assert!(exec_description.contains("DataFusion"));
    assert!(exec_description.contains("tools.sql"));
    assert!(
        exec_description.contains("Return the JSON-serializable value you want `exec` to return")
    );
    assert!(exec_description.contains("information_schema"));
    assert!(exec_description.contains("LIMIT 0"));
    assert!(exec_description.contains("source-aware filters/limits"));
    assert!(exec_description.contains("json_get_str"));
    assert!(exec_description.contains("tools.sql(\"SELECT 1 AS n\")"));
    assert!(exec_description.contains("tools.sql({ sql: \"SELECT 1 AS n\" })"));
    assert!(exec_description.contains("type SqlFunction"));
    assert!(exec_description.contains("<TRow extends SqlRow = SqlRow>"));
    assert!(exec_description.contains("list_catalog"));
    assert!(exec_description.contains("feedback"));
    assert!(exec_description.contains("declare namespace CoralSchema"));
    assert!(exec_description.contains("\"local_messages\""));
    assert!(exec_description.contains("\"text\": string"));
    assert!(!exec_description.contains("InferSql"));
    assert!(!exec_description.contains("rows("));
    assert!(!exec_description.contains("scalar("));
    assert!(!exec_description.contains("text(value"));
    assert!(!exec_description.contains("notify(value"));

    let all_tools = client
        .call_tool(
            CallToolRequestParams::new("exec").with_arguments(json_object(&json!({
                "source": "return ALL_TOOLS.map((tool) => tool.name).sort();"
            }))),
        )
        .await
        .expect("all tools exec")
        .structured_content
        .expect("all tools structured");
    assert_eq!(all_tools["status"], "completed");
    assert_eq!(
        all_tools["result"],
        json!([
            "describe_table",
            "feedback",
            "list_catalog",
            "list_columns",
            "search_catalog",
            "sql"
        ])
    );

    let removed_output_helpers = client
        .call_tool(
            CallToolRequestParams::new("exec").with_arguments(json_object(&json!({
                "source": "return { text: typeof text, notify: typeof notify };"
            }))),
        )
        .await
        .expect("output helper surface")
        .structured_content
        .expect("output helper structured");
    assert_eq!(removed_output_helpers["status"], "completed");
    assert_eq!(
        removed_output_helpers["result"],
        json!({
            "text": "undefined",
            "notify": "undefined",
        })
    );
}

#[cfg(feature = "code-mode")]
#[tokio::test]
async fn mcp_code_mode_only_advertises_exec_wait_but_keeps_nested_tools() {
    let temp = TempDir::new().expect("temp dir");
    let session = start_session_with_options(
        &temp,
        McpOptions {
            code_mode_only_enabled: true,
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
        vec!["exec", "wait"]
    );
    assert_code_mode_annotations(&tools, true);
    for tool in &tools {
        assert_property_schemas_are_objects(tool);
    }

    let direct_sql = client
        .call_tool(
            CallToolRequestParams::new("sql").with_arguments(json_object(&json!({
                "sql": "SELECT 1 AS n"
            }))),
        )
        .await
        .expect_err("direct sql should be hidden in code-mode-only mode");
    assert!(direct_sql.to_string().contains("tool 'sql' not found"));

    let nested_sql = client
        .call_tool(
            CallToolRequestParams::new("exec").with_arguments(json_object(&json!({
                "source": "return await tools.sql(\"SELECT 1 AS n\");"
            }))),
        )
        .await
        .expect("exec sql")
        .structured_content
        .expect("exec structured content");
    assert_eq!(nested_sql["status"], "completed");
    assert_eq!(nested_sql["result"]["rows"][0]["n"], "1");

    let all_tools = client
        .call_tool(
            CallToolRequestParams::new("exec").with_arguments(json_object(&json!({
                "source": "return ALL_TOOLS.map((tool) => tool.name).sort();"
            }))),
        )
        .await
        .expect("all tools exec")
        .structured_content
        .expect("all tools structured");
    assert_eq!(
        all_tools["result"],
        json!([
            "describe_table",
            "feedback",
            "list_catalog",
            "list_columns",
            "search_catalog",
            "sql"
        ])
    );

    session.shutdown().await;
}

#[cfg(feature = "code-mode")]
async fn assert_direct_sql_parameter_validation(client: &RunningService<RoleClient, ()>) {
    let tools = client.list_all_tools().await.expect("tools");
    let sql_tool = tool_by_name(&tools, "sql");
    assert_matches_input_schema(
        sql_tool,
        &json!({
            "sql": "SELECT $value",
            "params": { "value": 1 }
        }),
    );
    assert_rejected_by_input_schema(
        sql_tool,
        &json!({
            "sql": "SELECT $value",
            "params": { "": 1 }
        }),
    );
    assert_rejected_by_input_schema(
        sql_tool,
        &json!({
            "sql": "SELECT $value",
            "params": { "$value": 1 }
        }),
    );

    for params in [json!([[1]]), json!({ "value": { "nested": true } })] {
        let error = client
            .call_tool(
                CallToolRequestParams::new("sql").with_arguments(json_object(&json!({
                    "sql": "SELECT $1 AS value",
                    "params": params
                }))),
            )
            .await
            .expect_err("complex SQL parameter values should fail");
        assert!(
            error
                .to_string()
                .contains("SQL parameter values must be null, boolean, number, or string")
        );
    }
}

#[cfg(feature = "code-mode")]
async fn assert_exec_sql_matches_direct_sql(client: &RunningService<RoleClient, ()>) {
    let direct_sql = client
        .call_tool(
            CallToolRequestParams::new("sql").with_arguments(json_object(&json!({
                "sql": "SELECT text FROM local_messages.messages ORDER BY text"
            }))),
        )
        .await
        .expect("direct sql")
        .structured_content
        .expect("direct structured content");

    let exec_sql = client
        .call_tool(
            CallToolRequestParams::new("exec").with_arguments(json_object(&json!({
                "source": "return await tools.sql(\"SELECT text FROM local_messages.messages ORDER BY text\");"
            }))),
        )
        .await
        .expect("exec sql")
        .structured_content
        .expect("exec structured content");
    assert_eq!(exec_sql["status"], "completed");
    assert_eq!(exec_sql["result"], direct_sql);
}

#[cfg(feature = "code-mode")]
async fn assert_exec_sql_parameters(client: &RunningService<RoleClient, ()>) {
    let direct_positional = client
        .call_tool(
            CallToolRequestParams::new("sql").with_arguments(json_object(&json!({
                "sql": "SELECT $1 AS value, $2 AS label",
                "params": [7, "seven"]
            }))),
        )
        .await
        .expect("direct positional params")
        .structured_content
        .expect("direct positional structured");
    let object_params = client
        .call_tool(
            CallToolRequestParams::new("exec").with_arguments(json_object(&json!({
                "source": "return await tools.sql({ sql: \"SELECT $1 AS value, $2 AS label\", params: [7, \"seven\"] });"
            }))),
        )
        .await
        .expect("object params")
        .structured_content
        .expect("object params structured");
    assert_eq!(object_params["status"], "completed");
    assert_eq!(object_params["result"], direct_positional);
    assert_eq!(object_params["result"]["rows"][0]["value"], "7");
    assert_eq!(object_params["result"]["rows"][0]["label"], "seven");

    let direct_named = client
        .call_tool(
            CallToolRequestParams::new("sql").with_arguments(json_object(&json!({
                "sql": "SELECT $value AS value, $label AS label",
                "params": { "value": 7, "label": "seven" }
            }))),
        )
        .await
        .expect("direct named params")
        .structured_content
        .expect("direct named structured");
    let exec_named = client
        .call_tool(
            CallToolRequestParams::new("exec").with_arguments(json_object(&json!({
                "source": "return await tools.sql({ sql: \"SELECT $value AS value, $label AS label\", params: { value: 7, label: \"seven\" } });"
            }))),
        )
        .await
        .expect("exec named params")
        .structured_content
        .expect("exec named params structured");
    assert_eq!(exec_named["status"], "completed");
    assert_eq!(exec_named["result"], direct_named);

    let tagged_params = client
        .call_tool(
            CallToolRequestParams::new("exec").with_arguments(json_object(&json!({
                "source": "return await tools.sql`SELECT ${7} AS value, ${\"seven\"} AS label`;"
            }))),
        )
        .await
        .expect("tagged params")
        .structured_content
        .expect("tagged params structured");
    assert_eq!(tagged_params, object_params);
}

#[cfg(feature = "code-mode")]
async fn assert_exec_limit_zero_and_arrow_function(client: &RunningService<RoleClient, ()>) {
    let schema_sql = concat!(
        "SELECT m.text AS label, CAST(1 AS DOUBLE) AS score, ",
        "'literal' AS literal_value, json_get_str('{\"a\":\"b\"}', 'a') AS json_value ",
        "FROM local_messages.messages m ",
        "JOIN local_messages.events e ON m.text = e.text ",
        "LIMIT 0"
    );
    let direct_schema = client
        .call_tool(
            CallToolRequestParams::new("sql").with_arguments(json_object(&json!({
                "sql": schema_sql
            }))),
        )
        .await
        .expect("direct limit zero")
        .structured_content
        .expect("direct limit zero structured");
    assert_eq!(direct_schema["row_count"], 0);
    assert_eq!(direct_schema["rows"], json!([]));
    assert_eq!(direct_schema["columns"][0]["name"], "label");
    assert_eq!(direct_schema["columns"][1]["data_type"]["kind"], "float");
    assert_eq!(direct_schema["columns"][3]["name"], "json_value");

    let exec_schema = client
        .call_tool(
            CallToolRequestParams::new("exec").with_arguments(json_object(&json!({
                "source": format!("return await tools.sql({});", serde_json::to_string(schema_sql).expect("sql string"))
            }))),
        )
        .await
        .expect("exec limit zero")
        .structured_content
        .expect("exec limit zero structured");
    assert_eq!(exec_schema["status"], "completed");
    assert_eq!(exec_schema["result"], direct_schema);

    let transformed = client
        .call_tool(
            CallToolRequestParams::new("exec").with_arguments(json_object(&json!({
                "source": r#"async () => {
  const { rows } = await tools.sql("SELECT '{\"paths\":{\"/zones/{zone_id}/rulesets\":{\"get\":{\"summary\":\"List zone rulesets\"}}}}' AS spec");
  const spec = JSON.parse(rows[0].spec);
  const results = [];
  for (const [path, methods] of Object.entries(spec.paths)) {
    for (const [method, op] of Object.entries(methods)) {
      results.push({ method: method.toUpperCase(), path, summary: op.summary });
    }
  }
  return results;
}"#
            }))),
        )
        .await
        .expect("exec arrow transform")
        .structured_content
        .expect("exec arrow transform structured");
    assert_eq!(transformed["status"], "completed");
    assert_eq!(
        transformed["result"],
        json!([{
            "method": "GET",
            "path": "/zones/{zone_id}/rulesets",
            "summary": "List zone rulesets"
        }])
    );
}

#[cfg(feature = "code-mode")]
async fn assert_exec_function_declarations(client: &RunningService<RoleClient, ()>) {
    let sync_declaration = client
        .call_tool(
            CallToolRequestParams::new("exec").with_arguments(json_object(&json!({
                "source": r#"
function f() {
  return { kind: "sync-declaration", n: 1 };
}
return f();
"#
            }))),
        )
        .await
        .expect("exec sync function declaration")
        .structured_content
        .expect("sync declaration structured");
    assert_eq!(sync_declaration["status"], "completed");
    assert_eq!(
        sync_declaration["result"],
        json!({"kind": "sync-declaration", "n": 1})
    );

    let async_declaration = client
        .call_tool(
            CallToolRequestParams::new("exec").with_arguments(json_object(&json!({
                "source": r#"
async function f() {
  const result = await tools.sql("SELECT 2 AS n");
  return { kind: "async-declaration", n: result.rows[0].n };
}
return await f();
"#
            }))),
        )
        .await
        .expect("exec async function declaration")
        .structured_content
        .expect("async declaration structured");
    assert_eq!(async_declaration["status"], "completed");
    assert_eq!(
        async_declaration["result"],
        json!({"kind": "async-declaration", "n": "2"})
    );
}

#[cfg(feature = "code-mode")]
async fn assert_direct_and_nested_sql_reject_writes(client: &RunningService<RoleClient, ()>) {
    for sql in [
        "DELETE FROM local_messages.messages",
        "CREATE TABLE writes AS SELECT 1",
        "SET datafusion.execution.target_partitions = 1",
    ] {
        let direct = client
            .call_tool(
                CallToolRequestParams::new("sql").with_arguments(json_object(&json!({
                    "sql": sql
                }))),
            )
            .await
            .expect("invalid SQL should return tool result");
        assert_eq!(direct.is_error, Some(true), "{sql}");
    }

    let nested = client
        .call_tool(
            CallToolRequestParams::new("exec").with_arguments(json_object(&json!({
                "source": r#"
const statements = [
  "DELETE FROM local_messages.messages",
  "CREATE TABLE writes AS SELECT 1",
  "SET datafusion.execution.target_partitions = 1",
];
const rejected = [];
for (const sql of statements) {
  try {
    await tools.sql(sql);
    rejected.push(false);
  } catch (error) {
    rejected.push(String(error).includes("Query request is invalid") || String(error).includes("not supported"));
  }
}
return rejected;
"#
            }))),
        )
        .await
        .expect("nested invalid SQL")
        .structured_content
        .expect("nested invalid SQL structured");
    assert_eq!(nested["status"], "completed");
    assert_eq!(nested["result"], json!([true, true, true]));
}

#[cfg(feature = "code-mode")]
async fn assert_exec_catalog_matches_direct(client: &RunningService<RoleClient, ()>) {
    let direct_catalog = client
        .call_tool(
            CallToolRequestParams::new("list_catalog").with_arguments(json_object(&json!({
                "limit": 1,
                "kind": "table"
            }))),
        )
        .await
        .expect("direct catalog")
        .structured_content
        .expect("direct catalog structured");
    let exec_catalog = client
        .call_tool(
            CallToolRequestParams::new("exec").with_arguments(json_object(&json!({
                "source": "return await tools.list_catalog({ limit: 1, kind: \"table\" });"
            }))),
        )
        .await
        .expect("exec catalog")
        .structured_content
        .expect("exec catalog structured");
    assert_eq!(exec_catalog["status"], "completed");
    assert_eq!(exec_catalog["result"], direct_catalog);

    let direct_search = client
        .call_tool(
            CallToolRequestParams::new("search_catalog").with_arguments(json_object(&json!({
                "pattern": "messages",
                "limit": 1,
                "kind": "table"
            }))),
        )
        .await
        .expect("direct search")
        .structured_content
        .expect("direct search structured");
    let exec_search = client
        .call_tool(
            CallToolRequestParams::new("exec").with_arguments(json_object(&json!({
                "source": "return await tools.search_catalog({ pattern: \"messages\", limit: 1, kind: \"table\" });"
            }))),
        )
        .await
        .expect("exec search")
        .structured_content
        .expect("exec search structured");
    assert_eq!(exec_search["status"], "completed");
    assert_eq!(exec_search["result"], direct_search);

    let direct_describe = client
        .call_tool(
            CallToolRequestParams::new("describe_table").with_arguments(json_object(&json!({
                "schema": "local_messages",
                "table": "messages"
            }))),
        )
        .await
        .expect("direct describe")
        .structured_content
        .expect("direct describe structured");
    let exec_describe = client
        .call_tool(
            CallToolRequestParams::new("exec").with_arguments(json_object(&json!({
                "source": "return await tools.describe_table({ schema: \"local_messages\", table: \"messages\" });"
            }))),
        )
        .await
        .expect("exec describe")
        .structured_content
        .expect("exec describe structured");
    assert_eq!(exec_describe["status"], "completed");
    assert_eq!(exec_describe["result"], direct_describe);

    let direct_columns = client
        .call_tool(
            CallToolRequestParams::new("list_columns").with_arguments(json_object(&json!({
                "schema": "local_messages",
                "table": "messages",
                "limit": 2
            }))),
        )
        .await
        .expect("direct columns")
        .structured_content
        .expect("direct columns structured");
    let exec_columns = client
        .call_tool(
            CallToolRequestParams::new("exec").with_arguments(json_object(&json!({
                "source": "return await tools.list_columns({ schema: \"local_messages\", table: \"messages\", limit: 2 });"
            }))),
        )
        .await
        .expect("exec columns")
        .structured_content
        .expect("exec columns structured");
    assert_eq!(exec_columns["status"], "completed");
    assert_eq!(exec_columns["result"], direct_columns);
}

#[cfg(feature = "code-mode")]
async fn assert_exec_system_catalog_matches_direct(client: &RunningService<RoleClient, ()>) {
    let direct_system_catalog = client
        .call_tool(
            CallToolRequestParams::new("list_catalog").with_arguments(json_object(&json!({
                "schema": "coral",
                "limit": 10,
                "kind": "table"
            }))),
        )
        .await
        .expect("direct system catalog")
        .structured_content
        .expect("direct system catalog structured");
    let exec_system_catalog = client
        .call_tool(
            CallToolRequestParams::new("exec").with_arguments(json_object(&json!({
                "source": "return await tools.list_catalog({ schema: \"coral\", limit: 10, kind: \"table\" });"
            }))),
        )
        .await
        .expect("exec system catalog")
        .structured_content
        .expect("exec system catalog structured");
    assert_eq!(exec_system_catalog["status"], "completed");
    assert_eq!(exec_system_catalog["result"], direct_system_catalog);
    assert_eq!(exec_system_catalog["result"]["total"], 5);
}

#[cfg(feature = "code-mode")]
async fn assert_exec_feedback(client: &RunningService<RoleClient, ()>) {
    let feedback = client
        .call_tool(
            CallToolRequestParams::new("exec").with_arguments(json_object(&json!({
                "source": "return await tools.feedback({ trying_to_do: \"test code mode feedback\", tried: \"called feedback from exec\", stuck: \"not stuck\" });"
            }))),
        )
        .await
        .expect("exec feedback")
        .structured_content
        .expect("exec feedback structured");
    assert_eq!(feedback["status"], "completed");
    assert!(
        feedback["result"]["feedback_id"]
            .as_str()
            .expect("feedback id")
            .len()
            > 10
    );
}

#[cfg(feature = "code-mode")]
async fn assert_code_mode_finite_tool_surface(client: &RunningService<RoleClient, ()>) {
    let tools = client.list_all_tools().await.expect("tools");
    let tool_names = tools
        .iter()
        .map(|tool| tool.name.as_ref())
        .collect::<Vec<_>>();
    assert_eq!(
        tool_names,
        [
            "sql",
            "list_catalog",
            "search_catalog",
            "describe_table",
            "list_columns",
            "exec",
            "wait"
        ]
    );
    assert_code_mode_annotations(&tools, true);
    let all_tools = client
        .call_tool(
            CallToolRequestParams::new("exec").with_arguments(json_object(&json!({
                "source": "return ALL_TOOLS.map((tool) => tool.name).sort();"
            }))),
        )
        .await
        .expect("all tools exec")
        .structured_content
        .expect("all tools structured");
    assert_eq!(
        all_tools["result"],
        json!([
            "describe_table",
            "list_catalog",
            "list_columns",
            "search_catalog",
            "sql"
        ])
    );
}

#[cfg(feature = "code-mode")]
#[tokio::test]
async fn mcp_code_mode_wait_terminate_lifecycle_works() {
    let temp = TempDir::new().expect("temp dir");
    let session = start_session_with_options(
        &temp,
        McpOptions {
            code_mode_enabled: true,
            ..McpOptions::default()
        },
    )
    .await;
    let client = &session.client;

    assert_code_mode_finite_tool_surface(client).await;
    let direct_sql = client
        .call_tool(
            CallToolRequestParams::new("sql").with_arguments(json_object(&json!({
                "sql": "SELECT 1"
            }))),
        )
        .await
        .expect("direct sql should remain visible")
        .structured_content
        .expect("direct sql structured");
    assert_eq!(direct_sql["row_count"], 1);

    let oversized_pragma = client
        .call_tool(
            CallToolRequestParams::new("exec").with_arguments(json_object(&json!({
                "source": "// @exec: {\"yield_time_ms\": 60001}\nawait new Promise(() => {});"
            }))),
        )
        .await
        .expect("oversized pragma should return a structured failure")
        .structured_content
        .expect("oversized pragma structured");
    assert_eq!(oversized_pragma["status"], "failed");
    assert!(
        oversized_pragma["error"]["message"]
            .as_str()
            .expect("oversized pragma error")
            .contains("yield_time_ms must be at most 60000")
    );

    let exec = client
        .call_tool(
            CallToolRequestParams::new("exec").with_arguments(json_object(&json!({
                "source": "await new Promise(() => {});",
                "yield_time_ms": 1
            }))),
        )
        .await
        .expect("exec pending")
        .structured_content
        .expect("pending structured");
    assert_eq!(exec["status"], "running");
    let cell_id = exec["cell_id"].as_str().expect("cell id").to_string();

    let terminated = client
        .call_tool(
            CallToolRequestParams::new("wait").with_arguments(json_object(&json!({
                "cell_id": cell_id,
                "terminate": true
            }))),
        )
        .await
        .expect("terminate")
        .structured_content
        .expect("terminate structured");
    assert_eq!(terminated["status"], "terminated");

    let missing = client
        .call_tool(
            CallToolRequestParams::new("wait").with_arguments(json_object(&json!({
                "cell_id": terminated["cell_id"],
                "yield_time_ms": 1
            }))),
        )
        .await
        .expect("missing wait")
        .structured_content
        .expect("missing wait structured");
    assert_eq!(missing["status"], "failed");
    assert!(
        missing["error"]["message"]
            .as_str()
            .expect("missing error")
            .contains("not found")
    );

    session.shutdown().await;
}

#[cfg(feature = "code-mode")]
#[tokio::test]
async fn mcp_code_mode_wait_resumes_pending_sql_result() {
    let temp = TempDir::new().expect("temp dir");
    let session = start_session_with_options(
        &temp,
        McpOptions {
            code_mode_enabled: true,
            ..McpOptions::default()
        },
    )
    .await;
    let client = &session.client;

    let exec = client
        .call_tool(
            CallToolRequestParams::new("exec").with_arguments(json_object(&json!({
                "source": "await new Promise((resolve) => setTimeout(resolve, 1)); return await tools.sql(\"SELECT 1 AS n\");",
                "yield_time_ms": 1
            }))),
        )
        .await
        .expect("exec pending")
        .structured_content
        .expect("pending structured");
    let mut status = exec["status"].as_str().expect("status").to_string();
    let mut result = exec;
    for _ in 0..5 {
        if status == "completed" {
            break;
        }
        assert_eq!(status, "running");
        let cell_id = result["cell_id"].as_str().expect("cell id").to_string();
        result = client
            .call_tool(
                CallToolRequestParams::new("wait").with_arguments(json_object(&json!({
                    "cell_id": cell_id,
                    "yield_time_ms": 1000
                }))),
            )
            .await
            .expect("wait")
            .structured_content
            .expect("wait structured");
        status = result["status"].as_str().expect("status").to_string();
    }
    assert_eq!(status, "completed");
    assert_eq!(result["result"]["rows"][0]["n"], "1");

    session.shutdown().await;
}

#[cfg(feature = "code-mode")]
#[tokio::test]
async fn mcp_code_mode_enforces_max_output_tokens() {
    let temp = TempDir::new().expect("temp dir");
    let session = start_session_with_options(
        &temp,
        McpOptions {
            code_mode_enabled: true,
            ..McpOptions::default()
        },
    )
    .await;
    let client = &session.client;

    let result = client
        .call_tool(
            CallToolRequestParams::new("exec").with_arguments(json_object(&json!({
                "source": "return 'x'.repeat(100);",
                "max_output_tokens": 1
            }))),
        )
        .await
        .expect("exec")
        .structured_content
        .expect("structured");

    assert_eq!(result["status"], "failed");
    assert!(
        result["error"]["message"]
            .as_str()
            .expect("error message")
            .contains("exceeded max_output_tokens")
    );

    session.shutdown().await;
}

#[cfg(feature = "code-mode")]
#[tokio::test]
async fn mcp_code_mode_nested_call_budget_rejects_runaway_tool_loops() {
    let temp = TempDir::new().expect("temp dir");
    let session = start_session_with_options(
        &temp,
        McpOptions {
            code_mode_enabled: true,
            ..McpOptions::default()
        },
    )
    .await;
    let client = &session.client;

    let result = client
        .call_tool(
            CallToolRequestParams::new("exec").with_arguments(json_object(&json!({
                "source": r#"
for (let i = 0; i < 105; i += 1) {
  try {
    await tools.list_catalog({ limit: 1 });
  } catch (error) {
    return { i, error: String(error) };
  }
}
return { i: -1, error: "budget did not fire" };
"#
            }))),
        )
        .await
        .expect("budget exec")
        .structured_content
        .expect("budget structured");

    assert_eq!(result["status"], "completed");
    assert_eq!(result["result"]["i"], 100);
    assert!(
        result["result"]["error"]
            .as_str()
            .expect("budget error")
            .contains("exceeded the nested call limit")
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
    let tools = client.list_all_tools().await.expect("tools");
    let sql_tool = tool_by_name(&tools, "sql");

    let sql = client
        .call_tool(
            CallToolRequestParams::new("sql").with_arguments(json_object(&json!({
                "sql": "SELECT text FROM local_messages.messages ORDER BY text"
            }))),
        )
        .await
        .expect("sql");
    let sql_structured = sql.structured_content.expect("structured content");
    assert_matches_output_schema(sql_tool, &sql_structured);
    assert_eq!(sql_structured["columns"][0]["name"], "text");
    assert_eq!(sql_structured["columns"][0]["data_type"]["kind"], "string");
    assert_eq!(sql_structured["row_count"], 2);
    assert_eq!(sql_structured["rows"][0]["text"], "hello");
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
    let invalid_sql_structured = invalid_sql
        .structured_content
        .as_ref()
        .expect("structured content");
    assert_matches_output_schema(sql_tool, invalid_sql_structured);
    assert_eq!(
        invalid_sql_structured["error"]["summary"],
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
        .call_tool(CallToolRequestParams::new("list_catalog"))
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
