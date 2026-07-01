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

use std::path::Path;
use std::process::{Command as StdCommand, Stdio};
use std::time::Duration;
use std::{fs, io};

use coral_api::v1::{
    ExecuteSqlRequest, ImportSourceRequest, ListSourcesResponse, Source, SourceCredentialStorage,
    SourceOrigin, Workspace, import_source_response,
};
use coral_app::{ServerBuilder, shutdown_tracing};
use coral_client::{AppClient, default_workspace};
use harness::{MockServer, MockServerConfig, assert_default_workspace, assert_workspace_name};
use jsonschema::JSONSchema;
use rmcp::{
    RoleClient, ServiceExt,
    model::{CallToolRequestParams, CallToolResult, ReadResourceRequestParams},
    service::RunningService,
    transport::{ConfigureCommandExt, TokioChildProcess},
};
use serde_json::{Map, Value, json};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::{ChildStdin, ChildStdout, Command},
    time::timeout,
};
use tonic::Request;

const RAW_JSONRPC_RESPONSE_TIMEOUT: Duration = Duration::from_secs(15);

fn json_object(value: &Value) -> Map<String, Value> {
    value.as_object().cloned().expect("json object")
}

fn write_config(server: &MockServer, raw: &str) -> Result<(), io::Error> {
    fs::create_dir_all(server.config_dir())?;
    fs::write(server.config_dir().join("config.toml"), raw)
}

fn write_workspace_scoped_source_config(server: &MockServer) -> Result<(), io::Error> {
    write_config(
        server,
        r#"
[workspaces.default.sources.github]
origin = "bundled"

[workspaces.work.sources.jira]
origin = "bundled"
"#,
    )
}

fn write_query_history_trace_records(
    config_dir: &Path,
    records: &[Value],
) -> Result<(), Box<dyn std::error::Error>> {
    let trace_dir = config_dir.join("telemetry").join("traces");
    fs::create_dir_all(&trace_dir)?;
    let mut lines = String::new();
    for record in records {
        lines.push_str(&serde_json::to_string(record)?);
        lines.push('\n');
    }
    fs::write(
        trace_dir.join("spans-00000000000000000001-test-0000000000000000.jsonl"),
        lines,
    )?;
    Ok(())
}

fn query_history_trace_record(
    workspace: &str,
    trace_id: &str,
    sql: &str,
    sources: &[&str],
    row_count: u64,
    end_time_unix_nanos: i64,
) -> Value {
    let attributes = json!({
        "workspace": workspace,
        "sql": sql,
        "status": "ok",
        "row_count": row_count,
        "coral.query.sources": sources,
        "coral.query.tables": [],
        "coral.query.table_functions": [],
    });

    json!({
        "trace_id": trace_id,
        "span_id": format!("{trace_id}-span"),
        "name": "coral.query",
        "status": "ok",
        "end_time_unix_nanos": end_time_unix_nanos,
        "attributes_json": attributes.to_string(),
    })
}

fn source_fixture(workspace_name: &str, source_name: &str) -> Source {
    Source {
        workspace: Some(Workspace {
            name: workspace_name.to_string(),
        }),
        name: source_name.to_string(),
        version: "1.0.0".to_string(),
        secrets: Vec::new(),
        variables: Vec::new(),
        origin: SourceOrigin::Imported as i32,
        credential_storage: SourceCredentialStorage::File as i32,
    }
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

fn write_real_fixture_manifest(root: &Path) -> Result<String, Box<dyn std::error::Error>> {
    let source_dir = root.join("fixture-source");
    let data_dir = root.join("fixture-data");
    fs::create_dir_all(&source_dir)?;
    fs::create_dir_all(&data_dir)?;
    fs::write(
        data_dir.join("messages.jsonl"),
        r#"{"type":"user","sessionId":"s1","text":"hello"}
{"type":"assistant","sessionId":"s1","text":"world"}
"#,
    )?;
    Ok(format!(
        r#"
name: local_messages
version: 0.1.0
dsl_version: 3
backend: file
tables:
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
"#,
        data_dir.display()
    ))
}

async fn import_real_fixture_source(
    app: &AppClient,
    manifest_yaml: String,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut source_client = app.source_client();
    let mut stream = source_client
        .import_source(Request::new(ImportSourceRequest {
            workspace: Some(default_workspace()),
            manifest_yaml,
            variables: Vec::new(),
            secrets: Vec::new(),
            oauth_credential_retrievals: Vec::new(),
        }))
        .await?
        .into_inner();
    stream
        .message()
        .await?
        .and_then(|response| match response.event {
            Some(import_source_response::Event::Source(source)) => Some(source),
            _ => None,
        })
        .expect("import source response");
    Ok(())
}

fn text_content(result: &rmcp::model::ReadResourceResult) -> &str {
    match &result.contents[0] {
        rmcp::model::ResourceContents::TextResourceContents { text, .. } => text,
        other @ rmcp::model::ResourceContents::BlobResourceContents { .. } => {
            panic!("unexpected resource contents: {other:?}")
        }
    }
}

fn tool_input_properties(tool: &rmcp::model::Tool) -> &Map<String, Value> {
    tool.input_schema
        .get("properties")
        .and_then(Value::as_object)
        .unwrap_or_else(|| panic!("tool '{}' should advertise input properties", tool.name))
}

async fn structured_tool_content(
    client: &RunningService<RoleClient, ()>,
    request: CallToolRequestParams,
) -> Result<Value, Box<dyn std::error::Error>> {
    let result = client.call_tool(request).await?;
    assert_eq!(result.is_error, Some(false));
    assert!(
        result.content.is_empty(),
        "tool results should not duplicate structured payloads as text content"
    );
    Ok(result.structured_content.expect("structured content"))
}

fn tool_error_text(result: &CallToolResult) -> &str {
    result
        .content
        .first()
        .and_then(|content| content.as_text())
        .map(|text| text.text.as_str())
        .expect("tool error text content")
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
        let bytes_read = timeout(RAW_JSONRPC_RESPONSE_TIMEOUT, stdout.read_line(&mut line))
            .await
            .map_err(|error| {
                format!("timed out waiting for JSON-RPC response id {id}: {error}")
            })??;
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
        jsonschema::validator_for(input_schema).unwrap_or_else(|error| {
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
        jsonschema::validator_for(output_schema).unwrap_or_else(|error| {
            panic!(
                "tool '{name}' outputSchema must compile as JSON Schema: {error}; schema: {output_schema}"
            )
        });
    }
    let sql_tool = tools
        .iter()
        .find(|tool| tool.get("name").and_then(Value::as_str) == Some("sql"))
        .expect("sql tool should be advertised");
    let sql_properties = sql_tool
        .pointer("/inputSchema/properties")
        .and_then(Value::as_object)
        .expect("sql input properties");
    assert!(!sql_properties.contains_key("sql"));
    assert_eq!(
        sql_tool.pointer("/inputSchema/required/0"),
        Some(&json!("queries"))
    );
    assert_eq!(
        sql_tool.pointer("/inputSchema/properties/queries/minItems"),
        Some(&json!(1))
    );
    assert_eq!(
        sql_tool.pointer("/inputSchema/properties/queries/maxItems"),
        Some(&json!(10))
    );
    assert_eq!(
        sql_tool.pointer("/outputSchema/type"),
        Some(&json!("object"))
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn mcp_stdio_raw_tools_list_advertises_client_compatible_schemas()
-> Result<(), Box<dyn std::error::Error>> {
    let server = MockServer::start().await;
    write_config(
        &server,
        r#"
[workspaces.default.sources.jira]
origin = "bundled"

[workspaces.default.sources.github]
origin = "bundled"
"#,
    )?;
    let mut child = Command::new(env!("CARGO_BIN_EXE_coral"))
        .arg("mcp-stdio")
        .env("CORAL_ENDPOINT", server.endpoint_uri())
        .env("CORAL_CONFIG_DIR", server.config_dir())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
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
    let instructions = initialize
        .pointer("/result/instructions")
        .and_then(Value::as_str)
        .expect("initialize response should include instructions");
    assert!(
        instructions.contains("Connected Coral sources: github, jira."),
        "initialize instructions should include connected source names: {instructions}"
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
async fn mcp_stdio_initialize_includes_trace_backed_query_examples()
-> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::TempDir::new()?;
    let config_dir = temp.path().join("coral-config");
    fs::create_dir_all(&config_dir)?;
    fs::write(
        config_dir.join("config.toml"),
        r#"
[credentials]
storage = "file"
"#,
    )?;
    let server = ServerBuilder::new()
        .with_config_dir(&config_dir)
        .with_noop_feedback_uploads()
        .start()
        .await?;
    let app = AppClient::connect(server.endpoint_uri()).await?;
    import_real_fixture_source(&app, write_real_fixture_manifest(temp.path())?).await?;
    let sql = "SELECT text FROM local_messages.messages ORDER BY text";
    app.query_client()
        .execute_sql(Request::new(ExecuteSqlRequest {
            workspace: Some(default_workspace()),
            sql: sql.to_string(),
        }))
        .await?;
    shutdown_tracing();

    let mut child = Command::new(env!("CARGO_BIN_EXE_coral"))
        .arg("mcp-stdio")
        .env("CORAL_ENDPOINT", server.endpoint_uri())
        .env("CORAL_CONFIG_DIR", &config_dir)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
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
                    "name": "coral-cli-query-history-init-test",
                    "version": "0.0.0"
                }
            }
        }),
    )
    .await?;
    let initialize = read_jsonrpc_response(&mut stdout, 1).await?;
    let instructions = initialize
        .pointer("/result/instructions")
        .and_then(Value::as_str)
        .expect("initialize response should include instructions");
    assert!(
        instructions.contains("Connected Coral sources: local_messages."),
        "initialize instructions should include connected source names: {instructions}"
    );
    assert!(
        instructions.contains("Recent successful Coral SQL examples"),
        "initialize instructions should include query examples heading: {instructions}"
    );
    assert!(
        instructions.contains(&format!(
            "1. sources: local_messages; row_count: 2\n```sql\n{sql}\n```"
        )),
        "initialize instructions should include the traced query metadata and SQL: {instructions}"
    );

    drop(stdin);
    if timeout(Duration::from_secs(5), child.wait()).await.is_err() {
        child.start_kill()?;
        child.wait().await?;
    }
    server.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn mcp_stdio_workspace_flag_scopes_server_instance() -> Result<(), Box<dyn std::error::Error>>
{
    let server = MockServer::start_with_config(MockServerConfig::default().with_list_sources(
        ListSourcesResponse {
            sources: vec![source_fixture("work", "linear")],
        },
    ))
    .await;
    write_workspace_scoped_source_config(&server)?;
    write_query_history_trace_records(
        server.config_dir(),
        &[
            query_history_trace_record(
                "default",
                "default-history",
                "SELECT title FROM github.issues",
                &["github"],
                7,
                10,
            ),
            query_history_trace_record(
                "work",
                "work-history",
                "SELECT title FROM linear.issues",
                &["linear"],
                3,
                20,
            ),
        ],
    )?;
    let mut child = Command::new(env!("CARGO_BIN_EXE_coral"))
        .arg("mcp-stdio")
        .args(["--workspace", "work"])
        .env("CORAL_ENDPOINT", server.endpoint_uri())
        .env("CORAL_CONFIG_DIR", server.config_dir())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()?;
    let mut stdin = child.stdin.take().expect("mcp stdio stdin");
    let stdout = child.stdout.take().expect("mcp stdio stdout");
    let mut stdout = BufReader::new(stdout);

    assert_workspace_initialize_instructions(&mut stdin, &mut stdout).await?;

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

    let catalog_requests = server.list_catalog_requests();
    let count_request = catalog_requests
        .last()
        .expect("tools/list should request catalog counts");
    assert_workspace_name(count_request.workspace.as_ref(), "work");
    let list_sources_requests = server.list_sources_requests();
    assert!(
        !list_sources_requests.is_empty(),
        "expected at least one list_sources call"
    );
    for request in &list_sources_requests {
        assert_workspace_name(request.workspace.as_ref(), "work");
    }

    drop(stdin);
    if timeout(Duration::from_secs(5), child.wait()).await.is_err() {
        child.start_kill()?;
        child.wait().await?;
    }
    server.shutdown().await;
    Ok(())
}

async fn assert_workspace_initialize_instructions(
    stdin: &mut ChildStdin,
    stdout: &mut BufReader<ChildStdout>,
) -> Result<(), Box<dyn std::error::Error>> {
    write_jsonrpc_message(
        stdin,
        &json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": {
                    "name": "coral-cli-workspace-stdio-test",
                    "version": "0.0.0"
                }
            }
        }),
    )
    .await?;
    let initialize = read_jsonrpc_response(stdout, 1).await?;
    let instructions = initialize
        .pointer("/result/instructions")
        .and_then(Value::as_str)
        .expect("initialize response should include instructions");
    assert!(
        instructions.contains("Current Coral workspace: work."),
        "initialize instructions should include selected workspace: {instructions}"
    );
    assert!(
        instructions.contains("Connected Coral sources: linear."),
        "initialize instructions should include app-provided source names: {instructions}"
    );
    assert!(
        !instructions.contains("Connected Coral sources: jira."),
        "initialize instructions should not use config-store source names: {instructions}"
    );
    assert!(
        instructions.contains("Recent successful Coral SQL examples"),
        "non-default workspace initialize instructions should include workspace query history: {instructions}"
    );
    assert!(
        instructions.contains(
            "1. sources: linear; row_count: 3\n```sql\nSELECT title FROM linear.issues\n```"
        ),
        "initialize instructions should include selected workspace query history: {instructions}"
    );
    assert!(
        !instructions.contains("SELECT title FROM github.issues"),
        "initialize instructions should not include default workspace query history: {instructions}"
    );
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
            "search",
            "list_catalog",
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
            .expect("search description")
            .contains("Returns typed results plus provider statuses")
    );
    assert!(
        tools[2]
            .description
            .as_deref()
            .expect("list_catalog description")
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
            "search",
            "list_catalog",
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
async fn mcp_stdio_enable_episodes_flag_lists_open_episode_tool()
-> Result<(), Box<dyn std::error::Error>> {
    let server = MockServer::start().await;
    let client = start_mcp_client_with_args(&server, &["--enable-episodes"]).await?;

    let tools = client.list_all_tools().await?;
    assert_eq!(
        tools
            .iter()
            .map(|tool| tool.name.as_ref())
            .collect::<Vec<_>>(),
        vec![
            "sql",
            "search",
            "list_catalog",
            "describe_table",
            "list_columns",
            "open_episode"
        ]
    );
    for tool in tools
        .iter()
        .filter(|tool| tool.name.as_ref() != "open_episode")
    {
        assert!(
            tool_input_properties(tool).contains_key("episode_id"),
            "tool '{}' should advertise optional episode_id",
            tool.name
        );
    }
    let open_episode = tools
        .iter()
        .find(|tool| tool.name.as_ref() == "open_episode")
        .expect("open_episode tool should be listed");
    assert!(
        tool_input_properties(open_episode).contains_key("parent_episode_id"),
        "open_episode should accept an optional parent_episode_id"
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
async fn mcp_stdio_feature_config_enables_open_episode_tool()
-> Result<(), Box<dyn std::error::Error>> {
    let server = MockServer::start().await;
    write_config(
        &server,
        r"
[features]
episodes = true
",
    )?;
    let client = start_mcp_client(&server).await?;

    let tools = client.list_all_tools().await?;
    assert!(
        tools
            .iter()
            .any(|tool| tool.name.as_ref() == "open_episode"),
        "open_episode tool should be listed when [features].episodes is true"
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
        .stderr(Stdio::null())
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
    client
        .call_tool(CallToolRequestParams::new("search_catalog"))
        .await
        .expect_err("removed search_catalog tool should not be callable");
    assert_search_tool(&client, &server).await?;
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

async fn assert_search_tool(
    client: &RunningService<RoleClient, ()>,
    server: &MockServer,
) -> Result<(), Box<dyn std::error::Error>> {
    let search = structured_tool_content(
        client,
        CallToolRequestParams::new("search").with_arguments(json_object(&json!({
            "query": "messages",
            "limit": 5
        }))),
    )
    .await?;
    assert_eq!(search["results"][0]["kind"], "catalog_metadata");
    assert_eq!(
        search["results"][0]["catalog_metadata"]["item"]["sql_reference"],
        "local_messages.messages"
    );
    assert_eq!(
        search["provider_statuses"][0]["provider"],
        "catalog_metadata"
    );
    assert_eq!(search["provider_statuses"][0]["state"], "results_found");
    assert!(search["provider_statuses"][1]["coverage"].is_null());

    let search_requests = server.search_requests();
    let request = search_requests.last().expect("search request");
    assert_eq!(request.query, "messages");
    assert_eq!(request.limit, 5);
    assert_default_workspace(request.workspace.as_ref());
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
            "queries": ["SELECT text FROM local_messages.messages ORDER BY text"]
        }))),
    )
    .await?;
    assert_eq!(sql["total_count"], 1);
    assert_eq!(sql["success_count"], 1);
    assert_eq!(sql["error_count"], 0);
    assert_eq!(sql["results"][0]["status"], "success");
    assert_eq!(sql["results"][0]["rows"][0]["text"], "hello");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn mcp_stdio_tool_errors_do_not_end_the_session() -> Result<(), Box<dyn std::error::Error>> {
    let server = MockServer::start().await;
    let client = start_mcp_client(&server).await?;

    let mixed_sql = client
        .call_tool(
            CallToolRequestParams::new("sql").with_arguments(json_object(&json!({
                "queries": [
                    "SELECT text FROM local_messages.messages ORDER BY text",
                    "DELETE FROM local_messages.messages"
                ]
            }))),
        )
        .await?;
    assert_eq!(mixed_sql.is_error, Some(true));
    let mixed_sql_detail = mixed_sql
        .structured_content
        .as_ref()
        .expect("structured content")["data"]["results"][1]["error"]["detail"]
        .as_str()
        .expect("structured query error detail")
        .to_string();
    {
        let mixed_sql_text = tool_error_text(&mixed_sql);
        assert!(mixed_sql_text.contains("Query [1]: Query request is invalid"));
        assert!(mixed_sql_text.contains(&mixed_sql_detail));
    }
    let mixed_sql = mixed_sql.structured_content.expect("structured content");
    assert_eq!(mixed_sql["error"]["reason"], "SQL_BATCH_PARTIAL_FAILURE");
    let mixed_sql_batch = &mixed_sql["data"];
    assert_eq!(mixed_sql_batch["total_count"], 2);
    assert_eq!(mixed_sql_batch["success_count"], 1);
    assert_eq!(mixed_sql_batch["error_count"], 1);
    assert_eq!(mixed_sql_batch["results"][0]["status"], "success");
    assert_eq!(mixed_sql_batch["results"][0]["rows"][0]["text"], "hello");
    assert_eq!(mixed_sql_batch["results"][1]["status"], "error");
    assert_eq!(
        mixed_sql_batch["results"][1]["error"]["summary"],
        "Query request is invalid"
    );

    assert_eq!(server.execute_sql_requests().len(), 2);

    let catalog = client
        .call_tool(CallToolRequestParams::new("list_catalog"))
        .await?;
    assert_eq!(catalog.is_error, Some(false));
    assert!(catalog.content.is_empty());
    assert_eq!(
        catalog.structured_content.expect("structured content")["items"][0]["name"],
        "local_messages.events"
    );

    client.cancel().await?;
    server.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn mcp_stdio_sql_batch_records_each_execute_sql_request()
-> Result<(), Box<dyn std::error::Error>> {
    let server = MockServer::start().await;
    let client = start_mcp_client(&server).await?;

    let sql = structured_tool_content(
        &client,
        CallToolRequestParams::new("sql").with_arguments(json_object(&json!({
            "queries": [
                "SELECT 'first' AS label",
                "SELECT 'second' AS label"
            ]
        }))),
    )
    .await?;
    assert_eq!(sql["total_count"], 2);
    assert_eq!(sql["success_count"], 2);
    assert_eq!(sql["error_count"], 0);
    assert_eq!(sql["results"][0]["index"], 0);
    assert_eq!(sql["results"][0]["rows"][0]["label"], "first");
    assert_eq!(sql["results"][1]["index"], 1);
    assert_eq!(sql["results"][1]["rows"][0]["label"], "second");

    let requests = server.execute_sql_requests();
    assert_eq!(requests.len(), 2);
    assert!(
        requests
            .iter()
            .any(|request| request.sql == "SELECT 'first' AS label")
    );
    assert!(
        requests
            .iter()
            .any(|request| request.sql == "SELECT 'second' AS label")
    );

    client.cancel().await?;
    server.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn mcp_stdio_sql_batch_propagates_episode_id_to_each_query()
-> Result<(), Box<dyn std::error::Error>> {
    let server = MockServer::start().await;
    let client = start_mcp_client_with_args(&server, &["--enable-episodes"]).await?;

    let sql = structured_tool_content(
        &client,
        CallToolRequestParams::new("sql").with_arguments(json_object(&json!({
            "queries": [
                "SELECT 'first' AS label",
                "SELECT 'second' AS label"
            ],
            "episode_id": "ep_batch"
        }))),
    )
    .await?;
    assert_eq!(sql["total_count"], 2);
    assert_eq!(sql["success_count"], 2);

    let episode_ids = server.execute_sql_episode_ids();
    assert_eq!(episode_ids.len(), 2);
    assert!(
        episode_ids
            .iter()
            .all(|episode_id| episode_id.as_deref() == Some("ep_batch")),
        "expected every batch query to carry coral-episode-id, got {episode_ids:?}"
    );

    client.cancel().await?;
    server.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn mcp_stdio_sql_rejects_malformed_queries_before_backend_dispatch()
-> Result<(), Box<dyn std::error::Error>> {
    let server = MockServer::start().await;
    let client = start_mcp_client(&server).await?;

    client
        .call_tool(
            CallToolRequestParams::new("sql").with_arguments(json_object(&json!({
                "queries": []
            }))),
        )
        .await
        .expect_err("empty queries should fail as invalid params");
    assert!(
        server.execute_sql_requests().is_empty(),
        "malformed queries must not reach backend"
    );

    client.cancel().await?;
    server.shutdown().await;
    Ok(())
}
