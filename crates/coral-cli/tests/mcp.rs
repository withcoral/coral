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
    model::CallToolRequestParams,
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
    start_mcp_client_with_global_args_and_args(server, &[], args).await
}

async fn start_mcp_client_with_global_args(
    server: &MockServer,
    global_args: &[&str],
) -> Result<RunningService<RoleClient, ()>, Box<dyn std::error::Error>> {
    start_mcp_client_with_global_args_and_args(server, global_args, &[]).await
}

async fn start_mcp_client_with_global_args_and_args(
    server: &MockServer,
    global_args: &[&str],
    args: &[&str],
) -> Result<RunningService<RoleClient, ()>, Box<dyn std::error::Error>> {
    let transport = TokioChildProcess::new(
        tokio::process::Command::new(env!("CARGO_BIN_EXE_coral")).configure(|cmd| {
            cmd.args(global_args)
                .arg("mcp-stdio")
                .args(args)
                .env("CORAL_ENDPOINT", server.endpoint_uri())
                .env("CORAL_CONFIG_DIR", server.config_dir());
        }),
    )?;
    let client = ().serve(transport).await?;
    Ok(client)
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
async fn mcp_stdio_lists_tools() -> Result<(), Box<dyn std::error::Error>> {
    let server = MockServer::start().await;
    let client = start_mcp_client(&server).await?;

    let tools = client.list_all_tools().await?;
    assert_eq!(
        tools
            .iter()
            .map(|tool| tool.name.as_ref())
            .collect::<Vec<_>>(),
        vec!["search", "describe", "exec", "wait", "feedback"]
    );
    assert!(
        tools[0]
            .description
            .as_deref()
            .expect("search description")
            .contains("Search generated Coral exports")
    );
    assert!(
        tools[1]
            .description
            .as_deref()
            .expect("describe description")
            .contains("Describe a generated Coral export")
    );
    assert!(
        tools[2]
            .description
            .as_deref()
            .expect("exec description")
            .contains("Run Code Mode source")
    );

    client.cancel().await?;
    server.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn mcp_stdio_runtime_exposure_sql_hides_typescript_schema()
-> Result<(), Box<dyn std::error::Error>> {
    let server = MockServer::start().await;
    let client = start_mcp_client_with_global_args(&server, &["--runtime-exposure", "sql"]).await?;

    let tools = client.list_all_tools().await?;
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

    client.cancel().await?;
    server.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn mcp_stdio_lists_feedback_tool() -> Result<(), Box<dyn std::error::Error>> {
    let server = MockServer::start().await;
    let client = start_mcp_client(&server).await?;

    let tools = client.list_all_tools().await?;
    assert_eq!(
        tools
            .iter()
            .map(|tool| tool.name.as_ref())
            .collect::<Vec<_>>(),
        vec!["search", "describe", "exec", "wait", "feedback"]
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
async fn mcp_stdio_features_disable_command_keeps_feedback_tool()
-> Result<(), Box<dyn std::error::Error>> {
    let server = MockServer::start().await;
    run_features_command(&server, &["enable", "feedback"])?;
    run_features_command(&server, &["disable", "feedback"])?;
    let client = start_mcp_client(&server).await?;

    let tools = client.list_all_tools().await?;
    assert!(
        tools.iter().any(|tool| tool.name.as_ref() == "feedback"),
        "feedback tool should remain listed after `coral features disable feedback`"
    );

    client.cancel().await?;
    server.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn mcp_stdio_feature_config_cannot_hide_feedback_tool()
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
        tools.iter().any(|tool| tool.name.as_ref() == "feedback"),
        "feedback tool should remain listed when [features].feedback is false"
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
async fn mcp_stdio_disable_feedback_override_keeps_feedback_tool()
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
        tools.iter().any(|tool| tool.name.as_ref() == "feedback"),
        "feedback tool should remain listed when --disable-feedback is set"
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
            .any(|tool| tool.get("name").and_then(Value::as_str) == Some("feedback")),
        "invalid feature config must not hide feedback: {tools_list}"
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
async fn mcp_stdio_capability_tools_return_structured_content()
-> Result<(), Box<dyn std::error::Error>> {
    let server = MockServer::start().await;
    let client = start_mcp_client(&server).await?;

    assert_search_tool(&client, &server).await?;
    assert_describe_tool(&client, &server).await?;
    assert_exec_and_wait_tools(&client, &server).await?;

    client.cancel().await?;
    server.shutdown().await;
    Ok(())
}

async fn assert_search_tool(
    client: &RunningService<RoleClient, ()>,
    server: &MockServer,
) -> Result<(), Box<dyn std::error::Error>> {
    let search = structured_tool_content(
        client,
        CallToolRequestParams::new("search").with_arguments(json_object(&json!({
            "query": "issues",
            "kind": "typescript",
            "limit": 5
        }))),
    )
    .await?;
    assert_eq!(
        search["items"][0]["ref"],
        "typescript:github.rest.issues.listIssues"
    );
    assert_eq!(
        search["items"][0]["call"],
        "tools.github.rest.issues.listIssues"
    );
    assert_eq!(
        search["items"][0]["signature"],
        "tools.github.rest.issues.listIssues({ owner: string, repo: string }) -> value: array"
    );
    assert_eq!(search["items"][0]["source_key"], "github");
    assert_eq!(search["items"][0]["capability_kind"], "query");
    assert_eq!(search["items"][0]["effect"], "read");
    assert_eq!(search["items"][0]["matched_terms"], json!(["issues"]));
    assert_eq!(search["items"][0]["matched_fields"], json!(["title"]));
    assert_eq!(search["items"][0]["input_schema_available"], true);
    assert!(search["items"][0].get("deprecated").is_none());
    assert!(search["items"][0].get("support").is_none());
    assert_eq!(search["pagination"]["total"], 1);
    assert!(search["pagination"]["next_offset"].is_null());
    assert_eq!(
        search["diagnostics"][0]["code"],
        "SOURCE_ARTIFACTS_UNAVAILABLE"
    );
    assert_eq!(search["diagnostics"][0]["details"]["source_name"], "codex");
    assert!(search.get("rows").is_none());
    let search_requests = server.search_exports_requests();
    let search_request = search_requests.last().expect("search exports request");
    assert_eq!(search_request.query, "issues");
    assert_eq!(search_request.kind, 1);
    let pagination = search_request
        .pagination
        .as_ref()
        .expect("search pagination");
    assert_eq!(pagination.limit, 5);
    assert_eq!(pagination.offset, 0);
    Ok(())
}

async fn assert_describe_tool(
    client: &RunningService<RoleClient, ()>,
    server: &MockServer,
) -> Result<(), Box<dyn std::error::Error>> {
    let described = structured_tool_content(
        client,
        CallToolRequestParams::new("describe").with_arguments(json_object(&json!({
            "reference": "typescript:github.rest.issues.listIssues",
            "view": "detailed"
        }))),
    )
    .await?;
    assert_eq!(described["found"], true);
    assert_eq!(
        described["entry"]["capability_id"],
        "src_github.rest.list_issues"
    );
    assert_eq!(
        described["entry"]["capability"]["operation_id"],
        "list_issues"
    );
    assert_eq!(
        described["entry"]["full_path"],
        "tools.github.rest.issues.listIssues"
    );
    assert_eq!(described["entry"]["deprecated"], false);
    assert_eq!(described["entry"]["support_status"], "generated");
    assert!(described.get("description").is_none());
    assert_eq!(
        described["entry"]["diagnostics"][0]["details"]["field"],
        "query.q"
    );
    let describe_requests = server.describe_export_requests();
    let describe_request = describe_requests.last().expect("describe export request");
    assert_eq!(
        describe_request.reference,
        "typescript:github.rest.issues.listIssues"
    );
    Ok(())
}

async fn assert_exec_and_wait_tools(
    client: &RunningService<RoleClient, ()>,
    server: &MockServer,
) -> Result<(), Box<dyn std::error::Error>> {
    let exec = structured_tool_content(
        client,
        CallToolRequestParams::new("exec").with_arguments(json_object(&json!({
            "source": "return 1;"
        }))),
    )
    .await?;
    assert_eq!(exec["run"]["status"], "completed");
    assert_eq!(exec["run"]["id"], "run_1");
    assert!(exec["run"].get("cell_id").is_none());
    // The mock emits only lifecycle events, so the slim envelope omits every
    // empty field: no result, output, events, error, or cursor.
    assert_eq!(
        exec,
        json!({ "run": { "id": "run_1", "status": "completed" } })
    );
    assert_eq!(server.initialize_code_mode_requests().len(), 1);
    let exec_requests = server.exec_code_mode_requests();
    let exec_request = exec_requests.last().expect("exec code mode request");
    assert_eq!(exec_request.source, "return 1;");

    let wait = structured_tool_content(
        client,
        CallToolRequestParams::new("wait").with_arguments(json_object(&json!({
            "run_id": "run_1",
            "cursor": 1
        }))),
    )
    .await?;
    assert_eq!(wait["run"]["id"], "run_1");
    assert_eq!(wait["run"]["status"], "completed");
    assert!(wait.get("cursor").is_none());
    let wait_requests = server.wait_code_mode_requests();
    let wait_request = wait_requests.last().expect("wait code mode request");
    assert_eq!(wait_request.run_id, "run_1");
    assert_eq!(wait_request.after_event_id, 1);

    let terminated = structured_tool_content(
        client,
        CallToolRequestParams::new("wait").with_arguments(json_object(&json!({
            "run_id": "run_1",
            "terminate": true
        }))),
    )
    .await?;
    assert_eq!(terminated["run"]["id"], "run_1");
    assert_eq!(terminated["run"]["status"], "terminated");
    assert!(terminated.get("cursor").is_none());
    let terminate_requests = server.terminate_code_mode_requests();
    let terminate_request = terminate_requests
        .last()
        .expect("terminate code mode request");
    assert_eq!(terminate_request.run_id, "run_1");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn mcp_stdio_tool_errors_do_not_end_the_session() -> Result<(), Box<dyn std::error::Error>> {
    let server = MockServer::start().await;
    let client = start_mcp_client(&server).await?;

    client
        .call_tool(CallToolRequestParams::new("sql"))
        .await
        .expect_err("removed SQL tool should fail without ending the session");

    let search = client
        .call_tool(CallToolRequestParams::new("search"))
        .await?;
    assert_eq!(search.is_error, Some(false));
    assert_eq!(
        search.structured_content.expect("structured content")["items"][0]["source_key"],
        "github"
    );

    client.cancel().await?;
    server.shutdown().await;
    Ok(())
}
