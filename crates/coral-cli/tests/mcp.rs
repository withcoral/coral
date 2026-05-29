#![allow(
    clippy::indexing_slicing,
    clippy::panic_in_result_fn,
    clippy::string_slice,
    missing_docs,
    unused_crate_dependencies,
    reason = "Integration test: assertion-style indexing is idiomatic; only a subset of dependencies are used."
)]
#![cfg(feature = "cli-test-server")]

mod harness;

use std::process::{Command as StdCommand, Stdio};
use std::time::Duration;
use std::{fs, io};

use harness::MockServer;
use jsonschema::JSONSchema;
use rmcp::{
    RoleClient, ServiceExt,
    model::{CallToolRequestParams, ReadResourceRequestParams},
    service::RunningService,
    transport::{ConfigureCommandExt, TokioChildProcess},
};
use serde_json::{Map, Value, json};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::{ChildStdin, ChildStdout, Command},
    time::timeout,
};

fn json_object(value: &Value) -> Map<String, Value> {
    value.as_object().cloned().expect("json object")
}

fn write_config(server: &MockServer, raw: &str) -> Result<(), io::Error> {
    fs::create_dir_all(server.config_dir())?;
    fs::write(server.config_dir().join("config.toml"), raw)
}

fn run_features_command(
    server: &MockServer,
    args: &[&str],
) -> Result<(), Box<dyn std::error::Error>> {
    let output = StdCommand::new(env!("CARGO_BIN_EXE_coral"))
        .arg("features")
        .args(args)
        .env("CORAL_CONFIG_DIR", server.config_dir())
        .output()?;
    assert!(
        output.status.success(),
        "features command failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}

async fn start_mcp_client(
    server: &MockServer,
) -> Result<RunningService<RoleClient, ()>, Box<dyn std::error::Error>> {
    start_mcp_client_with_args(server, &[]).await
}

async fn start_mcp_client_with_args(
    server: &MockServer,
    args: &[&str],
) -> Result<RunningService<RoleClient, ()>, Box<dyn std::error::Error>> {
    let transport = TokioChildProcess::new(
        tokio::process::Command::new(env!("CARGO_BIN_EXE_coral")).configure(|cmd| {
            cmd.arg("mcp-stdio")
                .args(args)
                .env("CORAL_ENDPOINT", server.endpoint_uri())
                .env("CORAL_CONFIG_DIR", server.config_dir());
        }),
    )?;
    let client = ().serve(transport).await?;
    Ok(client)
}

fn text_content(result: &rmcp::model::ReadResourceResult) -> &str {
    match &result.contents[0] {
        rmcp::model::ResourceContents::TextResourceContents { text, .. } => text,
        other @ rmcp::model::ResourceContents::BlobResourceContents { .. } => {
            panic!("unexpected resource contents: {other:?}")
        }
    }
}

async fn structured_tool_content(
    client: &RunningService<RoleClient, ()>,
    request: CallToolRequestParams,
) -> Result<Value, Box<dyn std::error::Error>> {
    let result = client.call_tool(request).await?;
    assert_eq!(result.is_error, Some(false));
    Ok(result.structured_content.expect("structured content"))
}

async fn write_jsonrpc_message(
    stdin: &mut ChildStdin,
    message: &Value,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut payload = serde_json::to_vec(message)?;
    payload.push(b'\n');
    stdin.write_all(&payload).await?;
    stdin.flush().await?;
    Ok(())
}

async fn read_jsonrpc_response(
    stdout: &mut BufReader<ChildStdout>,
    id: i64,
) -> Result<Value, Box<dyn std::error::Error>> {
    let mut line = String::new();
    loop {
        line.clear();
        let bytes_read = timeout(Duration::from_secs(5), stdout.read_line(&mut line)).await??;
        if bytes_read == 0 {
            return Err(format!("mcp stdio closed before response id {id}").into());
        }
        let response: Value = serde_json::from_str(line.trim_end())?;
        if response.get("id").and_then(Value::as_i64) != Some(id) {
            continue;
        }
        assert_eq!(
            response.get("jsonrpc").and_then(Value::as_str),
            Some("2.0"),
            "response id {id} must declare JSON-RPC 2.0: {response}"
        );
        assert!(
            response.get("error").is_none(),
            "response id {id} must not be an error: {response}"
        );
        return Ok(response);
    }
}

fn assert_raw_tools_list_contract(response: &Value) {
    let tools = response
        .pointer("/result/tools")
        .and_then(Value::as_array)
        .expect("tools/list response should contain result.tools array");
    assert!(!tools.is_empty(), "tools/list should advertise tools");
    for tool in tools {
        let name = tool
            .get("name")
            .and_then(Value::as_str)
            .expect("advertised tool should include a string name");
        let input_schema = tool
            .get("inputSchema")
            .unwrap_or_else(|| panic!("tool '{name}' should advertise inputSchema"));
        assert!(
            input_schema.is_object(),
            "tool '{name}' inputSchema must be an object: {input_schema}"
        );
        JSONSchema::compile(input_schema).unwrap_or_else(|error| {
            panic!(
                "tool '{name}' inputSchema must compile as JSON Schema: {error}; schema: {input_schema}"
            )
        });
        let Some(output_schema) = tool.get("outputSchema") else {
            continue;
        };
        assert!(
            output_schema.is_object(),
            "tool '{name}' outputSchema must be an object when advertised: {output_schema}"
        );
        assert_eq!(
            output_schema.get("type").and_then(Value::as_str),
            Some("object"),
            "tool '{name}' outputSchema must declare root type object: {output_schema}"
        );
        JSONSchema::compile(output_schema).unwrap_or_else(|error| {
            panic!(
                "tool '{name}' outputSchema must compile as JSON Schema: {error}; schema: {output_schema}"
            )
        });
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn mcp_stdio_raw_tools_list_advertises_client_compatible_schemas()
-> Result<(), Box<dyn std::error::Error>> {
    let server = MockServer::start().await;
    let mut child = Command::new(env!("CARGO_BIN_EXE_coral"))
        .arg("mcp-stdio")
        .env("CORAL_ENDPOINT", server.endpoint_uri())
        .env("CORAL_CONFIG_DIR", server.config_dir())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;
    let mut stdin = child.stdin.take().expect("mcp stdio stdin");
    let stdout = child.stdout.take().expect("mcp stdio stdout");
    let mut stdout = BufReader::new(stdout);

    write_jsonrpc_message(
        &mut stdin,
        &json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": {
                    "name": "coral-cli-raw-stdio-test",
                    "version": "0.0.0"
                }
            }
        }),
    )
    .await?;
    let initialize = read_jsonrpc_response(&mut stdout, 1).await?;
    assert!(
        initialize.pointer("/result/protocolVersion").is_some(),
        "initialize response should include protocolVersion: {initialize}"
    );

    write_jsonrpc_message(
        &mut stdin,
        &json!({
            "jsonrpc": "2.0",
            "method": "notifications/initialized",
            "params": {}
        }),
    )
    .await?;
    write_jsonrpc_message(
        &mut stdin,
        &json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/list",
            "params": {}
        }),
    )
    .await?;
    let tools_list = read_jsonrpc_response(&mut stdout, 2).await?;
    assert_raw_tools_list_contract(&tools_list);

    drop(stdin);
    if timeout(Duration::from_secs(5), child.wait()).await.is_err() {
        child.start_kill()?;
        child.wait().await?;
    }
    server.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn mcp_stdio_lists_tools_and_resources() -> Result<(), Box<dyn std::error::Error>> {
    let server = MockServer::start().await;
    let client = start_mcp_client(&server).await?;

    let tools = client.list_all_tools().await?;
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
            "list_columns"
        ]
    );
    assert!(
        tools[0]
            .description
            .as_deref()
            .expect("sql description")
            .contains("3 table(s) are currently visible")
    );
    assert!(
        tools[1]
            .description
            .as_deref()
            .expect("list_catalog description")
            .contains("3 table(s) and 0 table function(s) are currently visible")
    );
    assert!(
        tools[2]
            .description
            .as_deref()
            .expect("search_catalog description")
            .contains("3 table(s) and 0 table function(s) are currently visible")
    );

    let resources = client.list_all_resources().await?;
    assert_eq!(
        resources
            .iter()
            .map(|resource| resource.uri.as_str())
            .collect::<Vec<_>>(),
        vec!["coral://guide", "coral://tables"]
    );

    let guide = client
        .read_resource(ReadResourceRequestParams::new("coral://guide"))
        .await?;
    let guide_text = text_content(&guide);
    assert!(guide_text.contains("## Available Schemas"));
    assert!(guide_text.contains("- local_messages"));
    assert!(guide_text.contains(
        "FROM coral.columns WHERE schema_name = 'local_messages' AND table_name = 'events'"
    ));

    let tables = client
        .read_resource(ReadResourceRequestParams::new("coral://tables"))
        .await?;
    let tables_json: Value = serde_json::from_str(text_content(&tables))?;
    assert_eq!(tables_json["tables"][0]["name"], "local_messages.events");

    client.cancel().await?;
    server.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn mcp_stdio_enable_feedback_flag_lists_feedback_tool()
-> Result<(), Box<dyn std::error::Error>> {
    let server = MockServer::start().await;
    let client = start_mcp_client_with_args(&server, &["--enable-feedback"]).await?;

    let tools = client.list_all_tools().await?;
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

    client.cancel().await?;
    server.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn mcp_stdio_feature_config_enables_feedback_tool() -> Result<(), Box<dyn std::error::Error>>
{
    let server = MockServer::start().await;
    write_config(
        &server,
        r"
[features]
feedback = true
",
    )?;
    let client = start_mcp_client(&server).await?;

    let tools = client.list_all_tools().await?;
    assert!(
        tools.iter().any(|tool| tool.name.as_ref() == "feedback"),
        "feedback tool should be listed when [features].feedback is true"
    );

    client.cancel().await?;
    server.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn mcp_stdio_features_enable_command_enables_feedback_tool()
-> Result<(), Box<dyn std::error::Error>> {
    let server = MockServer::start().await;
    run_features_command(&server, &["enable", "feedback"])?;
    let client = start_mcp_client(&server).await?;

    let tools = client.list_all_tools().await?;
    assert!(
        tools.iter().any(|tool| tool.name.as_ref() == "feedback"),
        "feedback tool should be listed after `coral features enable feedback`"
    );

    client.cancel().await?;
    server.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn mcp_stdio_features_disable_command_removes_feedback_tool()
-> Result<(), Box<dyn std::error::Error>> {
    let server = MockServer::start().await;
    run_features_command(&server, &["enable", "feedback"])?;
    run_features_command(&server, &["disable", "feedback"])?;
    let client = start_mcp_client(&server).await?;

    let tools = client.list_all_tools().await?;
    assert!(
        tools.iter().all(|tool| tool.name.as_ref() != "feedback"),
        "feedback tool should not be listed after `coral features disable feedback`"
    );

    client.cancel().await?;
    server.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn mcp_stdio_feature_config_can_leave_feedback_disabled()
-> Result<(), Box<dyn std::error::Error>> {
    let server = MockServer::start().await;
    write_config(
        &server,
        r"
[features]
feedback = false
",
    )?;
    let client = start_mcp_client(&server).await?;

    let tools = client.list_all_tools().await?;
    assert!(
        tools.iter().all(|tool| tool.name.as_ref() != "feedback"),
        "feedback tool should not be listed when [features].feedback is false"
    );

    client.cancel().await?;
    server.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn mcp_stdio_enable_feedback_override_overrides_config_disabled()
-> Result<(), Box<dyn std::error::Error>> {
    let server = MockServer::start().await;
    write_config(
        &server,
        r"
[features]
feedback = false
",
    )?;
    let client = start_mcp_client_with_args(&server, &["--enable-feedback"]).await?;

    let tools = client.list_all_tools().await?;
    assert!(
        tools.iter().any(|tool| tool.name.as_ref() == "feedback"),
        "feedback tool should be listed when --enable-feedback is set"
    );

    client.cancel().await?;
    server.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn mcp_stdio_disable_feedback_override_overrides_config_enabled()
-> Result<(), Box<dyn std::error::Error>> {
    let server = MockServer::start().await;
    write_config(
        &server,
        r"
[features]
feedback = true
",
    )?;
    let client = start_mcp_client_with_args(&server, &["--disable-feedback"]).await?;

    let tools = client.list_all_tools().await?;
    assert!(
        tools.iter().all(|tool| tool.name.as_ref() != "feedback"),
        "feedback tool should not be listed when --disable-feedback is set"
    );

    client.cancel().await?;
    server.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn mcp_stdio_invalid_feature_entries_do_not_corrupt_stdout()
-> Result<(), Box<dyn std::error::Error>> {
    let server = MockServer::start().await;
    write_config(
        &server,
        r#"
[features]
feedback = "yes"
future_flag = true
"#,
    )?;
    let mut child = Command::new(env!("CARGO_BIN_EXE_coral"))
        .arg("mcp-stdio")
        .env("CORAL_ENDPOINT", server.endpoint_uri())
        .env("CORAL_CONFIG_DIR", server.config_dir())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;
    let mut stdin = child.stdin.take().expect("mcp stdio stdin");
    let stdout = child.stdout.take().expect("mcp stdio stdout");
    let mut stdout = BufReader::new(stdout);

    write_jsonrpc_message(
        &mut stdin,
        &json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": {
                    "name": "coral-cli-invalid-feature-test",
                    "version": "0.0.0"
                }
            }
        }),
    )
    .await?;
    let initialize = read_jsonrpc_response(&mut stdout, 1).await?;
    assert!(
        initialize.pointer("/result/protocolVersion").is_some(),
        "initialize response should include protocolVersion: {initialize}"
    );

    write_jsonrpc_message(
        &mut stdin,
        &json!({
            "jsonrpc": "2.0",
            "method": "notifications/initialized",
            "params": {}
        }),
    )
    .await?;
    write_jsonrpc_message(
        &mut stdin,
        &json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/list",
            "params": {}
        }),
    )
    .await?;
    let tools_list = read_jsonrpc_response(&mut stdout, 2).await?;
    assert_raw_tools_list_contract(&tools_list);
    let tools = tools_list
        .pointer("/result/tools")
        .and_then(Value::as_array)
        .expect("tools/list result");
    assert!(
        tools
            .iter()
            .all(|tool| tool.get("name").and_then(Value::as_str) != Some("feedback")),
        "invalid feature config must not enable feedback: {tools_list}"
    );

    drop(stdin);
    if timeout(Duration::from_secs(5), child.wait()).await.is_err() {
        child.start_kill()?;
        child.wait().await?;
    }
    server.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn mcp_stdio_sql_and_catalog_tools_return_structured_content()
-> Result<(), Box<dyn std::error::Error>> {
    let server = MockServer::start().await;
    let client = start_mcp_client(&server).await?;

    assert_list_catalog_tool(&client, &server).await?;
    assert_search_catalog_tool(&client, &server).await?;
    assert_describe_table_tool(&client, &server).await?;
    assert_list_columns_tool(&client).await?;
    assert_sql_tool(&client).await?;

    client.cancel().await?;
    server.shutdown().await;
    Ok(())
}

async fn assert_list_catalog_tool(
    client: &RunningService<RoleClient, ()>,
    server: &MockServer,
) -> Result<(), Box<dyn std::error::Error>> {
    let structured_catalog =
        structured_tool_content(client, CallToolRequestParams::new("list_catalog")).await?;
    assert_eq!(structured_catalog["total"], 3);
    assert_eq!(structured_catalog["limit"], 50);
    assert_eq!(structured_catalog["offset"], 0);
    assert_eq!(structured_catalog["has_more"], false);
    assert_eq!(
        structured_catalog["items"][0]["name"],
        "local_messages.events"
    );
    assert_eq!(structured_catalog["items"][0]["kind"], "table");
    let requests = server.list_catalog_requests();
    let request = requests.last().expect("list catalog request");
    assert_eq!(request.schema_name, "");
    assert_eq!(request.kind, 0);
    let request_pagination = request.pagination.as_ref().expect("request pagination");
    assert_eq!(request_pagination.limit, 50);
    assert_eq!(request_pagination.offset, 0);

    let all_kinds = structured_tool_content(
        client,
        CallToolRequestParams::new("list_catalog").with_arguments(json_object(&json!({
            "schema": "local_messages",
            "kind": null
        }))),
    )
    .await?;
    assert_eq!(all_kinds["total"], 3);
    assert_eq!(all_kinds["items"][0]["kind"], "table");

    let paginated = structured_tool_content(
        client,
        CallToolRequestParams::new("list_catalog").with_arguments(json_object(&json!({
            "schema": "local_messages",
            "kind": "table",
            "limit": 2,
            "offset": 0
        }))),
    )
    .await?;
    assert_eq!(paginated["total"], 3);
    assert_eq!(paginated["has_more"], true);
    assert_eq!(paginated["next_offset"], 2);
    assert_eq!(paginated["items"].as_array().expect("items").len(), 2);

    let functions = structured_tool_content(
        client,
        CallToolRequestParams::new("list_catalog").with_arguments(json_object(&json!({
            "kind": "table_function"
        }))),
    )
    .await?;
    assert_eq!(functions["total"], 0);
    assert!(functions["items"].as_array().expect("items").is_empty());

    client
        .call_tool(
            CallToolRequestParams::new("list_catalog").with_arguments(json_object(&json!({
                "kind": "invalid"
            }))),
        )
        .await
        .expect_err("invalid catalog kind should fail");
    Ok(())
}

async fn assert_search_catalog_tool(
    client: &RunningService<RoleClient, ()>,
    server: &MockServer,
) -> Result<(), Box<dyn std::error::Error>> {
    let search = structured_tool_content(
        client,
        CallToolRequestParams::new("search_catalog").with_arguments(json_object(&json!({
            "pattern": "fixture.*messages",
            "schema": "local_messages",
            "kind": "table",
            "ignore_case": true
        }))),
    )
    .await?;
    assert_eq!(search["total"], 1);
    assert_eq!(search["items"][0]["name"], "local_messages.messages");
    assert_eq!(
        search["items"][0]["sql_reference"],
        "local_messages.messages"
    );
    assert!(
        search["items"][0]["matched_fields"]
            .as_array()
            .expect("matched fields")
            .iter()
            .any(|field| field == "description")
    );
    let search_requests = server.search_catalog_requests();
    let search_request = search_requests.last().expect("search catalog request");
    assert_eq!(search_request.pattern, "fixture.*messages");
    assert_eq!(search_request.schema_name, "local_messages");
    assert_eq!(search_request.kind, 1);
    let search_pagination = search_request
        .pagination
        .as_ref()
        .expect("search pagination");
    assert_eq!(search_pagination.limit, 20);
    assert_eq!(search_pagination.offset, 0);
    assert!(search_request.ignore_case);

    let guide_search = structured_tool_content(
        client,
        CallToolRequestParams::new("search_catalog").with_arguments(json_object(&json!({
            "pattern": "Query fixture messages",
            "schema": "local_messages"
        }))),
    )
    .await?;
    assert_eq!(guide_search["total"], 1);
    assert!(
        guide_search["items"][0]["matched_fields"]
            .as_array()
            .expect("matched fields")
            .iter()
            .any(|field| field == "guide")
    );
    Ok(())
}

async fn assert_describe_table_tool(
    client: &RunningService<RoleClient, ()>,
    server: &MockServer,
) -> Result<(), Box<dyn std::error::Error>> {
    let describe_before = server.describe_table_requests().len();
    let execute_sql_before = server.execute_sql_requests().len();
    let described = structured_tool_content(
        client,
        CallToolRequestParams::new("describe_table").with_arguments(json_object(&json!({
            "schema": "local_messages",
            "table": "messages"
        }))),
    )
    .await?;
    assert_eq!(described["found"], true);
    assert_eq!(described["name"], "local_messages.messages");
    assert_eq!(described["column_count"], 3);

    let describe_requests = server.describe_table_requests();
    assert_eq!(describe_requests.len(), describe_before + 1);
    let describe_request = &describe_requests[describe_before];
    assert_eq!(describe_request.schema_name, "local_messages");
    assert_eq!(describe_request.table_name, "messages");
    assert_eq!(server.execute_sql_requests().len(), execute_sql_before);
    Ok(())
}

async fn assert_list_columns_tool(
    client: &RunningService<RoleClient, ()>,
) -> Result<(), Box<dyn std::error::Error>> {
    let columns = structured_tool_content(
        client,
        CallToolRequestParams::new("list_columns").with_arguments(json_object(&json!({
            "schema": "local_messages",
            "table": "messages",
            "required_only": true
        }))),
    )
    .await?;
    assert_eq!(columns["total"], 2);
    assert_eq!(columns["columns"][0]["column_name"], "owner");
    assert_eq!(columns["columns"][1]["column_name"], "repo");

    let filtered_columns = structured_tool_content(
        client,
        CallToolRequestParams::new("list_columns").with_arguments(json_object(&json!({
            "schema": "local_messages",
            "table": "messages",
            "pattern": "text"
        }))),
    )
    .await?;
    assert_eq!(filtered_columns["total"], 1);
    assert_eq!(filtered_columns["columns"][0]["column_name"], "text");
    Ok(())
}

async fn assert_sql_tool(
    client: &RunningService<RoleClient, ()>,
) -> Result<(), Box<dyn std::error::Error>> {
    let sql = structured_tool_content(
        client,
        CallToolRequestParams::new("sql").with_arguments(json_object(&json!({
            "sql": "SELECT text FROM local_messages.messages ORDER BY text"
        }))),
    )
    .await?;
    assert_eq!(sql["rows"][0]["text"], "hello");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn mcp_stdio_tool_errors_do_not_end_the_session() -> Result<(), Box<dyn std::error::Error>> {
    let server = MockServer::start().await;
    let client = start_mcp_client(&server).await?;

    let invalid_sql = client
        .call_tool(
            CallToolRequestParams::new("sql").with_arguments(json_object(&json!({
                "sql": "DELETE FROM local_messages.messages"
            }))),
        )
        .await?;
    assert_eq!(invalid_sql.is_error, Some(true));
    assert_eq!(
        invalid_sql.structured_content.expect("structured content")["error"]["summary"],
        "Query request is invalid"
    );

    let catalog = client
        .call_tool(CallToolRequestParams::new("list_catalog"))
        .await?;
    assert_eq!(catalog.is_error, Some(false));
    assert_eq!(
        catalog.structured_content.expect("structured content")["items"][0]["name"],
        "local_messages.events"
    );

    client.cancel().await?;
    server.shutdown().await;
    Ok(())
}
