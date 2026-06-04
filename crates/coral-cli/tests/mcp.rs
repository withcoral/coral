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

use std::process::Stdio;
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

type TestResult<T> = Result<T, Box<dyn std::error::Error>>;
type McpClient = RunningService<RoleClient, ()>;

fn json_object(value: Value) -> Map<String, Value> {
    match value {
        Value::Object(object) => object,
        _ => panic!("json object"),
    }
}

fn tool_request(name: &'static str, arguments: Value) -> CallToolRequestParams {
    CallToolRequestParams::new(name).with_arguments(json_object(arguments))
}

fn write_config(server: &MockServer, raw: &str) -> Result<(), io::Error> {
    fs::create_dir_all(server.config_dir())?;
    fs::write(server.config_dir().join("config.toml"), raw)
}

async fn start_mcp_client(server: &MockServer) -> TestResult<McpClient> {
    start_mcp_client_with_args(server, &[]).await
}

async fn start_mcp_client_with_args(server: &MockServer, args: &[&str]) -> TestResult<McpClient> {
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

async fn tool_names(client: &McpClient) -> TestResult<Vec<String>> {
    Ok(client
        .list_all_tools()
        .await?
        .iter()
        .map(|tool| tool.name.as_ref().to_string())
        .collect())
}

async fn assert_feedback_tool(client: &McpClient, expected: bool, reason: &str) -> TestResult<()> {
    assert_eq!(
        tool_names(client)
            .await?
            .iter()
            .any(|name| name == "feedback"),
        expected,
        "{reason}"
    );
    Ok(())
}

async fn assert_feedback_tool_with_config(
    raw_config: &str,
    args: &[&str],
    expected: bool,
    reason: &str,
) -> TestResult<()> {
    let server = MockServer::start().await;
    write_config(&server, raw_config)?;
    let client = start_mcp_client_with_args(&server, args).await?;

    assert_feedback_tool(&client, expected, reason).await?;

    client.cancel().await?;
    server.shutdown().await;
    Ok(())
}

async fn write_jsonrpc_message(stdin: &mut ChildStdin, message: &Value) -> TestResult<()> {
    let mut payload = serde_json::to_vec(message)?;
    payload.push(b'\n');
    stdin.write_all(&payload).await?;
    stdin.flush().await?;
    Ok(())
}

async fn read_jsonrpc_response(stdout: &mut BufReader<ChildStdout>, id: i64) -> TestResult<Value> {
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

fn assert_schema_compiles(tool_name: &str, label: &str, schema: &Value) {
    assert!(
        schema.is_object(),
        "tool '{tool_name}' {label} must be an object: {schema}"
    );
    JSONSchema::compile(schema).unwrap_or_else(|error| {
        panic!("tool '{tool_name}' {label} must compile as JSON Schema: {error}; schema: {schema}")
    });
}

struct RawMcpStdio {
    child: tokio::process::Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
}

impl RawMcpStdio {
    fn start(server: &MockServer) -> TestResult<Self> {
        let mut child = Command::new(env!("CARGO_BIN_EXE_coral"))
            .arg("mcp-stdio")
            .env("CORAL_ENDPOINT", server.endpoint_uri())
            .env("CORAL_CONFIG_DIR", server.config_dir())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()?;
        let stdin = child.stdin.take().expect("mcp stdio stdin");
        let stdout = child.stdout.take().expect("mcp stdio stdout");
        Ok(Self {
            child,
            stdin,
            stdout: BufReader::new(stdout),
        })
    }

    async fn request(&mut self, id: i64, method: &str, params: Value) -> TestResult<Value> {
        write_jsonrpc_message(
            &mut self.stdin,
            &json!({
                "jsonrpc": "2.0",
                "id": id,
                "method": method,
                "params": params
            }),
        )
        .await?;
        read_jsonrpc_response(&mut self.stdout, id).await
    }

    async fn initialize(&mut self, client_name: &str) -> TestResult<()> {
        let initialize = self
            .request(
                1,
                "initialize",
                json!({
                    "protocolVersion": "2025-06-18",
                    "capabilities": {},
                    "clientInfo": {
                        "name": client_name,
                        "version": "0.0.0"
                    }
                }),
            )
            .await?;
        assert!(
            initialize.pointer("/result/protocolVersion").is_some(),
            "initialize response should include protocolVersion: {initialize}"
        );
        write_jsonrpc_message(
            &mut self.stdin,
            &json!({
                "jsonrpc": "2.0",
                "method": "notifications/initialized",
                "params": {}
            }),
        )
        .await
    }

    async fn tools_list(&mut self) -> TestResult<Value> {
        self.request(2, "tools/list", json!({})).await
    }

    async fn shutdown(self) -> TestResult<()> {
        let Self {
            mut child, stdin, ..
        } = self;
        drop(stdin);
        if timeout(Duration::from_secs(5), child.wait()).await.is_err() {
            child.start_kill()?;
            child.wait().await?;
        }
        Ok(())
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
        assert_schema_compiles(name, "inputSchema", input_schema);
        let Some(output_schema) = tool.get("outputSchema") else {
            continue;
        };
        assert_eq!(
            output_schema.get("type").and_then(Value::as_str),
            Some("object"),
            "tool '{name}' outputSchema must declare root type object: {output_schema}"
        );
        assert_schema_compiles(name, "outputSchema", output_schema);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn mcp_stdio_raw_tools_list_advertises_client_compatible_schemas() -> TestResult<()> {
    let server = MockServer::start().await;
    let mut stdio = RawMcpStdio::start(&server)?;
    stdio.initialize("coral-cli-raw-stdio-test").await?;
    let tools_list = stdio.tools_list().await?;
    assert_raw_tools_list_contract(&tools_list);

    stdio.shutdown().await?;
    server.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn mcp_stdio_lists_tools_and_resources() -> TestResult<()> {
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
    let catalog_requests = server.list_catalog_requests();
    let count_request = catalog_requests
        .last()
        .expect("tools/list should request catalog counts");
    assert_eq!(count_request.kind, 0);
    let count_pagination = count_request
        .pagination
        .as_ref()
        .expect("count request pagination");
    assert_eq!(count_pagination.limit, 1);
    assert_eq!(count_pagination.offset, 0);

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
async fn mcp_stdio_feedback_tool_respects_config_and_cli_overrides() -> TestResult<()> {
    for (raw_config, args, expected, reason) in [
        (
            "",
            ["--enable-feedback"].as_slice(),
            true,
            "feedback tool should be listed when --enable-feedback is set without a config override",
        ),
        (
            r"
[features]
feedback = true
",
            [].as_slice(),
            true,
            "feedback tool should be listed when [features].feedback is true",
        ),
        (
            r"
[features]
feedback = false
",
            [].as_slice(),
            false,
            "feedback tool should not be listed when [features].feedback is false",
        ),
        (
            r"
[features]
feedback = false
",
            ["--enable-feedback"].as_slice(),
            true,
            "feedback tool should be listed when --enable-feedback is set",
        ),
        (
            r"
[features]
feedback = true
",
            ["--disable-feedback"].as_slice(),
            false,
            "feedback tool should not be listed when --disable-feedback is set",
        ),
    ] {
        assert_feedback_tool_with_config(raw_config, args, expected, reason).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn mcp_stdio_invalid_feature_entries_do_not_corrupt_stdout() -> TestResult<()> {
    let server = MockServer::start().await;
    write_config(
        &server,
        r#"
[features]
feedback = "yes"
future_flag = true
"#,
    )?;
    let mut stdio = RawMcpStdio::start(&server)?;
    stdio.initialize("coral-cli-invalid-feature-test").await?;
    let tools_list = stdio.tools_list().await?;
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

    stdio.shutdown().await?;
    server.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn mcp_stdio_forwards_sql_tool_and_returns_structured_content() -> TestResult<()> {
    let server = MockServer::start().await;
    let client = start_mcp_client(&server).await?;

    let sql = client
        .call_tool(tool_request(
            "sql",
            json!({
            "sql": "SELECT text FROM local_messages.messages ORDER BY text"
            }),
        ))
        .await?;
    assert_eq!(sql.is_error, Some(false));
    assert_eq!(
        sql.structured_content.expect("structured content")["rows"][0]["text"],
        "hello"
    );
    assert_eq!(
        server
            .execute_sql_requests()
            .pop()
            .expect("execute_sql request")
            .sql,
        "SELECT text FROM local_messages.messages ORDER BY text"
    );

    client.cancel().await?;
    server.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn mcp_stdio_forwards_catalog_tools_and_returns_structured_content() -> TestResult<()> {
    let server = MockServer::start().await;
    let client = start_mcp_client(&server).await?;

    assert_list_catalog_tool(&client, &server).await?;
    assert_search_catalog_tool(&client, &server).await?;
    assert_describe_table_tool(&client, &server).await?;
    assert_list_columns_tool(&client, &server).await?;

    client.cancel().await?;
    server.shutdown().await;
    Ok(())
}

async fn assert_list_catalog_tool(client: &McpClient, server: &MockServer) -> TestResult<()> {
    let catalog = client
        .call_tool(tool_request(
            "list_catalog",
            json!({
                "schema": "local_messages",
                "kind": "table",
                "limit": 2,
                "offset": 1
            }),
        ))
        .await?;
    assert_eq!(catalog.is_error, Some(false));
    let catalog = catalog.structured_content.expect("structured content");
    assert_eq!(catalog["total"], 3);
    assert_eq!(catalog["items"][0]["name"], "local_messages.messages");
    let list_request = server
        .list_catalog_requests()
        .pop()
        .expect("list_catalog request");
    assert_eq!(list_request.schema_name, "local_messages");
    assert_eq!(list_request.kind, 1);
    let list_pagination = list_request.pagination.expect("list_catalog pagination");
    assert_eq!(list_pagination.limit, 2);
    assert_eq!(list_pagination.offset, 1);
    Ok(())
}

async fn assert_search_catalog_tool(client: &McpClient, server: &MockServer) -> TestResult<()> {
    let search = client
        .call_tool(tool_request(
            "search_catalog",
            json!({
                "pattern": "^MESSAGES$",
                "schema": "local_messages",
                "kind": "table",
                "ignore_case": true,
                "limit": 5
            }),
        ))
        .await?;
    assert_eq!(search.is_error, Some(false));
    let search = search.structured_content.expect("structured content");
    assert_eq!(search["total"], 1);
    assert_eq!(search["items"][0]["name"], "local_messages.messages");
    assert!(
        search["items"][0]["matched_fields"]
            .as_array()
            .expect("matched fields")
            .iter()
            .any(|field| field.as_str() == Some("table_name"))
    );
    let search_request = server
        .search_catalog_requests()
        .pop()
        .expect("search_catalog request");
    assert_eq!(search_request.pattern, "^MESSAGES$");
    assert!(search_request.ignore_case);
    assert_eq!(search_request.schema_name, "local_messages");
    assert_eq!(search_request.kind, 1);
    assert_eq!(
        search_request
            .pagination
            .expect("search_catalog pagination")
            .limit,
        5
    );
    Ok(())
}

async fn assert_describe_table_tool(client: &McpClient, server: &MockServer) -> TestResult<()> {
    let described = client
        .call_tool(tool_request(
            "describe_table",
            json!({
                "schema": "local_messages",
                "table": "messages"
            }),
        ))
        .await?;
    assert_eq!(described.is_error, Some(false));
    let described = described.structured_content.expect("structured content");
    assert_eq!(described["found"], true);
    assert_eq!(described["name"], "local_messages.messages");
    assert_eq!(described["column_count"], 3);
    let describe_request = server
        .describe_table_requests()
        .pop()
        .expect("describe_table request");
    assert_eq!(describe_request.schema_name, "local_messages");
    assert_eq!(describe_request.table_name, "messages");
    Ok(())
}

async fn assert_list_columns_tool(client: &McpClient, server: &MockServer) -> TestResult<()> {
    let columns = client
        .call_tool(tool_request(
            "list_columns",
            json!({
                "schema": "local_messages",
                "table": "messages",
                "pattern": "^repo$",
                "required_only": true,
                "limit": 2
            }),
        ))
        .await?;
    assert_eq!(columns.is_error, Some(false));
    let columns = columns.structured_content.expect("structured content");
    assert_eq!(columns["schema_name"], "local_messages");
    assert_eq!(columns["table_name"], "messages");
    assert_eq!(columns["total"], 1);
    assert_eq!(columns["columns"][0]["column_name"], "repo");
    assert_eq!(columns["columns"][0]["matched_fields"][0], "column_name");
    let columns_request = server
        .list_columns_requests()
        .pop()
        .expect("list_columns request");
    assert_eq!(columns_request.schema_name, "local_messages");
    assert_eq!(columns_request.table_name, "messages");
    assert_eq!(columns_request.pattern.as_deref(), Some("^repo$"));
    assert!(columns_request.ignore_case);
    assert!(columns_request.required_only);
    assert_eq!(
        columns_request
            .pagination
            .expect("list_columns pagination")
            .limit,
        2
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn mcp_stdio_tool_errors_do_not_end_the_session() -> TestResult<()> {
    let server = MockServer::start().await;
    let client = start_mcp_client(&server).await?;

    let invalid_sql = client
        .call_tool(tool_request(
            "sql",
            json!({ "sql": "DELETE FROM local_messages.messages" }),
        ))
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
