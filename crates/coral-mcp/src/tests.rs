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
    model::{
        CallToolRequestParams, CallToolResult, ReadResourceRequestParams, Resource,
        ResourceContents, Tool,
    },
    service::RunningService,
};
use serde_json::{Map, Value, json};
use tempfile::TempDir;
use tonic::Request;

use crate::{CoralMcpServer, McpOptions};

type BoxError = Box<dyn std::error::Error + Send + Sync>;
type McpClient = RunningService<RoleClient, ()>;
type McpServerResult = Result<(), BoxError>;

const FEEDBACK_TRIED: &str = "Ran cargo test and inspected the failing assertion";
const FEEDBACK_STUCK: &str = "The fixture shape does not match the documented contract";
const LOCAL_SCHEMA: &str = "local_messages";
const LOCAL_EVENTS: &str = "local_messages.events";

fn fixture_manifest_yaml(root: &Path) -> String {
    let data_dir = root.join("fixture-data");
    fs::create_dir_all(&data_dir).expect("create data dir");
    fs::write(
        data_dir.join("messages.jsonl"),
        r#"{"type":"user","sessionId":"s1","text":"hello"}
{"type":"assistant","sessionId":"s1","text":"world"}
"#,
    )
    .expect("write jsonl");
    let tables = [
        ("events", "Fixture events"),
        ("messages", "Fixture messages"),
        ("sessions", "Fixture sessions"),
    ]
    .into_iter()
    .map(|(name, description)| fixture_table_yaml(name, description, &data_dir))
    .collect::<String>();
    format!(
        r"
name: local_messages
version: 0.1.0
dsl_version: 3
backend: file
tables:
{tables}
",
    )
}

fn fixture_table_yaml(name: &str, description: &str, data_dir: &Path) -> String {
    format!(
        r#"  - name: {name}
    description: {description}
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
        data_dir.display()
    )
}

fn function_fixture_manifest_yaml() -> String {
    r"
name: searchy
version: 0.1.0
dsl_version: 3
backend: http
base_url: https://example.com
tables:
  - name: placeholder
    description: Placeholder table
    request:
      path: /placeholder
    columns:
      - name: id
        type: Utf8
functions:
  - name: lookup_issue
    args:
      - name: number
        required: true
        bind:
          arg: number
    request:
      path: /issues/{{arg.number}}
    columns:
      - name: title
        type: Utf8
        description: Issue title
  - name: search_issues
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
      path: /search/issues
      query:
        - name: q
          from: arg
          key: q
        - name: search_type
          from: arg
          key: search_type
    columns:
      - name: title
        type: Utf8
        description: Issue title
"
    .to_string()
}

fn json_object(value: &Value) -> Map<String, Value> {
    value.as_object().cloned().expect("json object")
}

fn merge_json_object(base: Value, fields: Value) -> Value {
    let Value::Object(mut base) = base else {
        unreachable!("base fixture is an object");
    };
    let Value::Object(fields) = fields else {
        unreachable!("fixture fields must be an object");
    };
    base.extend(fields);
    Value::Object(base)
}

fn local_table_args(table: &str) -> Value {
    json!({ "schema": LOCAL_SCHEMA, "table": table })
}

fn local_table_args_with(table: &str, fields: Value) -> Value {
    merge_json_object(local_table_args(table), fields)
}

fn feedback_args(tried: &str, stuck: &str) -> Value {
    json!({
        "trying_to_do": "Fix failing tests",
        "tried": tried,
        "stuck": stuck
    })
}

fn feedback_reports_path(temp: &TempDir) -> PathBuf {
    temp.path()
        .join("coral-config/workspaces/default/feedback/reports.jsonl")
}

fn read_feedback_records(temp: &TempDir) -> Vec<Value> {
    fs::read_to_string(feedback_reports_path(temp))
        .expect("feedback file should exist")
        .lines()
        .map(|line| serde_json::from_str(line).expect("feedback JSONL should parse"))
        .collect()
}

fn sql_args(sql: &str) -> Value {
    json!({ "sql": sql })
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
    client: McpClient,
    app_server: RunningServer,
    mcp_server_task: tokio::task::JoinHandle<McpServerResult>,
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
        Ok::<(), BoxError>(())
    });
    let client = ().serve(client_transport).await.expect("start rmcp client");
    TestSession {
        source_client,
        client,
        app_server: server,
        mcp_server_task,
    }
}

async fn read_resource_text(client: &McpClient, uri: &'static str) -> String {
    let resource = client
        .read_resource(ReadResourceRequestParams::new(uri))
        .await
        .unwrap_or_else(|error| panic!("resource '{uri}' should read: {error}"));
    match &resource.contents[0] {
        ResourceContents::TextResourceContents { text, .. } => text.clone(),
        other @ ResourceContents::BlobResourceContents { .. } => {
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

fn assert_tool_names(tools: &[Tool], expected: &[&str]) {
    assert_eq!(
        tools
            .iter()
            .map(|tool| tool.name.as_ref())
            .collect::<Vec<_>>(),
        expected
    );
}

fn assert_resource_uris(resources: &[Resource], expected: &[&str]) {
    assert_eq!(
        resources
            .iter()
            .map(|resource| resource.uri.as_str())
            .collect::<Vec<_>>(),
        expected
    );
}

fn assert_resource_description_contains(resource: &Resource, expected: &str) {
    let description = resource
        .description
        .as_deref()
        .expect("resource should advertise a description");
    assert_contains(description, expected);
}

fn assert_output_schema_roots_are_objects(tools: &[Tool]) {
    for tool in tools {
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
}

fn assert_contains(haystack: &str, needle: &str) {
    assert!(
        haystack.contains(needle),
        "expected text to contain {needle:?}"
    );
}

fn assert_contains_all(haystack: &str, needles: &[&str]) {
    for needle in needles {
        assert_contains(haystack, needle);
    }
}

fn assert_tool_description_contains(tool: &Tool, expected: &str) {
    let description = tool
        .description
        .as_deref()
        .unwrap_or_else(|| panic!("tool '{}' should advertise a description", tool.name));
    assert_contains(description, expected);
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

fn assert_page(value: &Value, total: u64, limit: u64, has_more: bool, next_offset: Option<u64>) {
    assert_eq!(value["total"], total);
    assert_eq!(value["limit"], limit);
    assert_eq!(value["has_more"], has_more);
    assert_eq!(
        value["next_offset"],
        next_offset.map_or(Value::Null, Value::from)
    );
}

fn catalog_item(value: &Value, index: usize) -> &Value {
    &value["items"][index]
}

fn assert_catalog_item(value: &Value, index: usize, kind: &str, name: &str) {
    let item = catalog_item(value, index);
    assert_eq!(item["kind"], kind);
    assert_eq!(item["name"], name);
    assert_eq!(item["sql_reference"], name);
}

fn assert_item_matched_field(item: &Value, expected: &str) {
    let fields = item["matched_fields"].as_array().expect("matched fields");
    assert!(
        fields.iter().any(|field| field == expected),
        "expected matched_fields to include {expected:?}: {item}"
    );
}

fn assert_missing_table_suggestions(value: &Value, table: &str) {
    assert_eq!(value["found"], false);
    assert_eq!(value["requested"]["schema"], LOCAL_SCHEMA);
    assert_eq!(value["requested"]["table"], table);
    assert_eq!(value["same_schema_tables"][0]["name"], LOCAL_EVENTS);
    assert_eq!(value["suggestions"][0]["name"], LOCAL_EVENTS);
}

fn assert_non_empty_string(value: &Value) {
    assert!(value.as_str().is_some_and(|text| !text.is_empty()));
}

fn structured_content(result: &CallToolResult) -> &Value {
    result
        .structured_content
        .as_ref()
        .expect("structured content")
}

fn tool_request(name: &'static str, args: Option<Value>) -> CallToolRequestParams {
    let mut request = CallToolRequestParams::new(name);
    if let Some(args) = args {
        request = request.with_arguments(json_object(&args));
    }
    request
}

async fn call_tool_result(
    client: &McpClient,
    name: &'static str,
    args: Option<Value>,
) -> CallToolResult {
    client
        .call_tool(tool_request(name, args))
        .await
        .unwrap_or_else(|error| panic!("tool '{name}' should succeed: {error}"))
}

async fn call_tool_structured(
    client: &McpClient,
    name: &'static str,
    args: Option<Value>,
) -> Value {
    call_tool_result(client, name, args)
        .await
        .structured_content
        .expect("structured content")
}

async fn call_tool_structured_args(client: &McpClient, name: &'static str, args: Value) -> Value {
    call_tool_structured(client, name, Some(args)).await
}

async fn call_tool_error(client: &McpClient, name: &'static str, args: Option<Value>) -> String {
    match client.call_tool(tool_request(name, args)).await {
        Ok(result) => panic!("tool '{name}' should fail, got {result:?}"),
        Err(error) => error.to_string(),
    }
}

async fn call_tool_error_args(client: &McpClient, name: &'static str, args: Value) -> String {
    call_tool_error(client, name, Some(args)).await
}

async fn assert_tool_error_contains(
    client: &McpClient,
    name: &'static str,
    args: Value,
    expected: &str,
) {
    assert_contains(&call_tool_error_args(client, name, args).await, expected);
}

async fn call_tool_result_args(
    client: &McpClient,
    name: &'static str,
    args: Value,
) -> CallToolResult {
    call_tool_result(client, name, Some(args)).await
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
    let manifest_yaml = fixture_manifest_yaml(temp.path());
    let mut session = start_session(&temp).await;
    let client = &session.client;

    let initial_tools = client.list_all_tools().await.expect("initial tools");
    assert_tool_names(
        &initial_tools,
        &[
            "sql",
            "list_catalog",
            "search_catalog",
            "describe_table",
            "list_columns",
        ],
    );
    assert_tool_description_contains(&initial_tools[0], "5 table(s) are currently visible");
    assert_tool_description_contains(
        &initial_tools[0],
        "No connected user sources are currently configured",
    );
    assert_output_schema_roots_are_objects(&initial_tools);
    let initial_resources = client
        .list_all_resources()
        .await
        .expect("initial resources");
    assert_resource_uris(&initial_resources, &["coral://guide", "coral://tables"]);
    assert_resource_description_contains(&initial_resources[0], "5 visible table");

    let initial_guide_text = read_resource_text(client, "coral://guide").await;
    assert_contains_all(
        &initial_guide_text,
        &[
            "## Available Schemas",
            "- coral: System catalog schema.",
            "No user schemas are currently configured.",
            "read-only SQL database",
            "CROSS JOIN",
            "schema_name = '<schema>'",
        ],
    );

    add_demo_source(&mut session.source_client, manifest_yaml).await;

    let updated_tools = client.list_all_tools().await.expect("updated tools");
    let list_catalog_tool = tool_by_name(&updated_tools, "list_catalog");
    let search_catalog_tool = tool_by_name(&updated_tools, "search_catalog");
    let list_columns_tool = tool_by_name(&updated_tools, "list_columns");
    assert_tool_description_contains(&updated_tools[0], "8 table(s) are currently visible");
    assert_tool_description_contains(
        &updated_tools[0],
        "Connected sources/schemas include: local_messages",
    );
    assert_tool_description_contains(&updated_tools[1], "8 table(s) and 0 table function(s)");
    assert_tool_description_contains(
        &updated_tools[1],
        "Connected sources/schemas include: local_messages",
    );
    assert_tool_description_contains(&updated_tools[2], "8 table(s) and 0 table function(s)");
    assert_tool_description_contains(
        &updated_tools[2],
        "Connected sources/schemas include: local_messages",
    );

    let updated_resources = client
        .list_all_resources()
        .await
        .expect("updated resources");
    assert_resource_description_contains(&updated_resources[0], "1 configured connection");

    let tables_text = read_resource_text(client, "coral://tables").await;
    let tables_json =
        serde_json::from_str::<serde_json::Value>(&tables_text).expect("parse tables resource");
    assert_eq!(tables_json["tables"][0]["name"], "coral.columns");
    assert_eq!(tables_json["tables"][0]["sql_reference"], "coral.columns");
    assert!(
        tables_json["tables"]
            .as_array()
            .expect("tables")
            .iter()
            .any(|table| table["name"] == "local_messages.events")
    );

    let updated_guide_text = read_resource_text(client, "coral://guide").await;
    assert_contains_all(
        &updated_guide_text,
        &[
            "## Available Schemas",
            "- coral: System catalog schema.",
            "- local_messages",
            "Prefer one SQL statement with `JOIN`, `CROSS JOIN`",
        ],
    );
    assert!(!updated_guide_text.contains("## Visible SQL Schemas"));
    assert_contains(
        &updated_guide_text,
        "FROM coral.columns WHERE schema_name = 'local_messages' AND table_name = 'events'",
    );

    let catalog = call_tool_structured(client, "list_catalog", None).await;
    assert_eq!(catalog["total"], 8);
    assert_catalog_item(&catalog, 0, "table", "coral.columns");
    assert_eq!(catalog["items"][0]["table"]["table_name"], "columns");
    assert_matches_output_schema(list_catalog_tool, &catalog);

    let catalog_page = call_tool_structured_args(
        client,
        "list_catalog",
        json!({
                "schema": "local_messages",
                "kind": "table",
                "limit": 2,
                "offset": 0
        }),
    )
    .await;
    assert_page(&catalog_page, 3, 2, true, Some(2));
    assert_eq!(catalog_page["items"].as_array().expect("items").len(), 2);
    assert_matches_output_schema(list_catalog_tool, &catalog_page);

    for (tool, args) in [
        ("list_catalog", json!({ "limit": 0 })),
        ("list_catalog", json!({ "kind": "invalid" })),
        ("search_catalog", json!({ "pattern": "[" })),
        ("describe_table", local_table_args(" ")),
        (
            "list_columns",
            local_table_args_with("messages", json!({ "pattern": "" })),
        ),
    ] {
        call_tool_error_args(client, tool, args).await;
    }

    let search = call_tool_structured_args(
        client,
        "search_catalog",
        json!({
                "pattern": "^MESSAGES$",
                "schema": "local_messages",
                "kind": "table",
                "ignore_case": true
        }),
    )
    .await;
    assert_eq!(search["total"], 1);
    assert_eq!(catalog_item(&search, 0)["name"], "local_messages.messages");
    assert_eq!(
        catalog_item(&search, 0)["sql_reference"],
        "local_messages.messages"
    );
    assert!(
        search["items"][0]["table"]["guide"].is_string(),
        "search results should always expose guide text, even when empty"
    );
    assert_item_matched_field(catalog_item(&search, 0), "table_name");
    assert_matches_output_schema(search_catalog_tool, &search);

    let described =
        call_tool_structured_args(client, "describe_table", local_table_args("messages")).await;
    assert_eq!(described["found"], true);
    assert_eq!(described["name"], "local_messages.messages");
    assert_eq!(described["column_count"], 3);
    assert!(described["columns_hint"].as_str().is_some());
    assert!(described["columns"].is_null());

    let missing_table =
        call_tool_result_args(client, "describe_table", local_table_args("missing")).await;
    assert_eq!(missing_table.is_error, Some(false));
    let missing_table = structured_content(&missing_table);
    assert_missing_table_suggestions(missing_table, "missing");
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
        LOCAL_SCHEMA
    );

    let missing_schema = call_tool_result_args(
        client,
        "describe_table",
        json!({
                "schema": "local_mesages",
                "table": "missing["
        }),
    )
    .await;
    assert_eq!(missing_schema.is_error, Some(false));
    let missing_schema = structured_content(&missing_schema);
    assert_eq!(missing_schema["found"], false);
    assert_eq!(
        missing_schema["suggested_calls"][0]["arguments"]["pattern"],
        r"missing\["
    );
    assert!(
        missing_schema["suggested_calls"][0]["arguments"]["schema"].is_null(),
        "search suggestion should not constrain a missing schema"
    );

    let columns = call_tool_structured_args(
        client,
        "list_columns",
        local_table_args_with("messages", json!({ "limit": 2 })),
    )
    .await;
    assert_eq!(columns["schema_name"], "local_messages");
    assert_eq!(columns["table_name"], "messages");
    assert_page(&columns, 3, 2, true, Some(2));
    assert_eq!(columns["columns"][0]["column_name"], "type");
    assert_eq!(columns["columns"][0]["data_type"], "Utf8");
    assert_matches_output_schema(list_columns_tool, &columns);

    let missing_columns =
        call_tool_structured_args(client, "list_columns", local_table_args("missing")).await;
    assert_missing_table_suggestions(&missing_columns, "missing");
    assert_eq!(
        missing_columns["suggested_calls"][0]["arguments"]["schema"],
        LOCAL_SCHEMA
    );
    assert_matches_output_schema(list_columns_tool, &missing_columns);

    session.shutdown().await;
}

#[tokio::test]
async fn list_catalog_surfaces_table_functions() {
    let temp = TempDir::new().expect("temp dir");
    let manifest_yaml = function_fixture_manifest_yaml();
    let mut session = start_session(&temp).await;
    let client = &session.client;

    add_demo_source(&mut session.source_client, manifest_yaml).await;

    let tools = client.list_all_tools().await.expect("tools");
    assert_tool_description_contains(
        tool_by_name(&tools, "list_catalog"),
        "6 table(s) and 2 table function(s) are currently visible",
    );
    assert_tool_description_contains(
        tool_by_name(&tools, "search_catalog"),
        "Connected sources/schemas include: searchy",
    );
    assert!(tools.iter().all(|tool| tool.name != "list_tables"));
    assert!(tools.iter().all(|tool| tool.name != "search_tables"));

    let catalog_tool = tool_by_name(&tools, "list_catalog");
    let search_tool = tool_by_name(&tools, "search_catalog");
    let catalog =
        call_tool_structured_args(client, "list_catalog", json!({"schema": "searchy"})).await;
    assert_eq!(catalog["total"], 3);
    assert_catalog_item(&catalog, 0, "table_function", "searchy.lookup_issue");
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
    assert_catalog_item(&catalog, 1, "table", "searchy.placeholder");
    assert_matches_output_schema(catalog_tool, &catalog);

    let functions = call_tool_structured_args(
        client,
        "list_catalog",
        json!({
                "kind": "table_function",
                "limit": 1,
                "offset": 1
        }),
    )
    .await;
    assert_page(&functions, 2, 1, false, None);
    assert_eq!(functions["offset"], 1);
    assert_eq!(functions["items"][0]["name"], "searchy.search_issues");
    assert_eq!(
        functions["items"][0]["sql_call_example"],
        "searchy.search_issues(q => '<value>')"
    );
    assert_matches_output_schema(catalog_tool, &functions);

    let search = call_tool_structured_args(
        client,
        "search_catalog",
        json!({
                "pattern": "hybrid",
                "kind": "table_function"
        }),
    )
    .await;
    assert_eq!(search["total"], 1);
    assert_catalog_item(&search, 0, "table_function", "searchy.search_issues");
    assert_item_matched_field(catalog_item(&search, 0), "arguments");
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
    let feedback_annotations = tool_by_name(&tools, "feedback")
        .annotations
        .as_ref()
        .expect("feedback annotations");
    assert_eq!(feedback_annotations.read_only_hint, Some(false));
    assert_eq!(feedback_annotations.destructive_hint, Some(false));
    assert_eq!(feedback_annotations.idempotent_hint, Some(false));
    assert_eq!(feedback_annotations.open_world_hint, Some(true));

    let feedback = call_tool_result_args(
        client,
        "feedback",
        feedback_args(FEEDBACK_TRIED, FEEDBACK_STUCK),
    )
    .await;
    assert_eq!(feedback.is_error, Some(false));
    let structured = structured_content(&feedback);
    assert_non_empty_string(&structured["feedback_id"]);
    assert_non_empty_string(&structured["created_at"]);
    assert_eq!(structured["message"], "Feedback report stored.");
    assert!(structured.get("upload").is_none());

    let records = read_feedback_records(&temp);
    assert_eq!(records.len(), 1);
    let record = &records[0];
    assert_eq!(record["id"], structured["feedback_id"]);
    assert_eq!(record["workspace"], "default");
    assert_eq!(record["trying_to_do"], "Fix failing tests");
    assert_eq!(record["tried"], FEEDBACK_TRIED);
    assert_eq!(record["stuck"], FEEDBACK_STUCK);

    assert_tool_error_contains(
        client,
        "feedback",
        feedback_args(" ", FEEDBACK_STUCK),
        "missing string argument 'tried'",
    )
    .await;

    assert_eq!(read_feedback_records(&temp).len(), 1);

    session.shutdown().await;
}

#[tokio::test]
async fn mcp_feedback_tool_is_disabled_by_default() {
    let temp = TempDir::new().expect("temp dir");
    let session = start_session(&temp).await;
    let client = &session.client;

    assert_tool_error_contains(
        client,
        "feedback",
        feedback_args("Ran cargo test", "Need more context"),
        "tool 'feedback' not found",
    )
    .await;
    assert!(!feedback_reports_path(&temp).exists());

    session.shutdown().await;
}

#[tokio::test]
async fn mcp_tool_error_does_not_end_session() {
    let temp = TempDir::new().expect("temp dir");
    let manifest_yaml = fixture_manifest_yaml(temp.path());
    let mut session = start_session(&temp).await;
    let client = &session.client;

    add_demo_source(&mut session.source_client, manifest_yaml).await;

    let sql = call_tool_result_args(
        client,
        "sql",
        sql_args("SELECT text FROM local_messages.messages ORDER BY text"),
    )
    .await;
    assert_eq!(structured_content(&sql)["rows"][0]["text"], "hello");
    assert_eq!(sql.is_error, Some(false));

    let invalid_sql = call_tool_result_args(
        client,
        "sql",
        sql_args("DELETE FROM local_messages.messages"),
    )
    .await;
    assert_eq!(invalid_sql.is_error, Some(true));
    assert_eq!(
        structured_content(&invalid_sql)["error"]["summary"],
        "Query request is invalid"
    );
    assert!(
        invalid_sql.content[0]
            .as_text()
            .expect("text content")
            .text
            .contains("Detail:")
    );

    let catalog_after_error =
        call_tool_result_args(client, "list_catalog", json!({"schema": "local_messages"})).await;
    let structured_catalog_after_error = structured_content(&catalog_after_error);
    assert_eq!(
        structured_catalog_after_error["items"][0]["name"],
        LOCAL_EVENTS
    );
    assert_eq!(
        structured_catalog_after_error["items"][0]["sql_reference"],
        LOCAL_EVENTS
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

    let sql = call_tool_result_args(
        client,
        "sql",
        sql_args("SELECT CAST(-8504475857937456387 AS BIGINT) AS user_id"),
    )
    .await;
    assert_eq!(sql.is_error, Some(false));

    let rows = &structured_content(&sql)["rows"];
    assert_eq!(
        rows[0]["user_id"],
        Value::String("-8504475857937456387".to_string()),
    );

    session.shutdown().await;
}
