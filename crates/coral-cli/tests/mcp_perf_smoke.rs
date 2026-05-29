#![allow(
    clippy::print_stderr,
    missing_docs,
    unused_crate_dependencies,
    reason = "Perf smoke integration test: stderr output is useful in CI logs and the target uses only a subset of test dependencies."
)]

use std::path::Path;
use std::process::Stdio;
use std::time::{Duration, Instant};

use coral_api::v1::{ImportSourceRequest, SourceSecret, SourceVariable, import_source_response};
use coral_client::{AppClient, default_workspace, local::ServerBuilder};
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::process::{ChildStdin, ChildStdout, Command};
use tokio::time::timeout;
use tonic::Request;

const SYNTHETIC_HTTP_SOURCE_COUNT: usize = 20;
const MCP_RESPONSE_TIMEOUT: Duration = Duration::from_mins(1);
const PROCESS_EXIT_TIMEOUT: Duration = Duration::from_secs(10);
const TOOLS_LIST_LIMIT: Duration = Duration::from_millis(1_500);
const LIST_CATALOG_LIMIT: Duration = Duration::from_secs(1);
const SEARCH_CATALOG_LIMIT: Duration = Duration::from_secs(1);

#[derive(Debug)]
struct Timings {
    initialize: Duration,
    tools_list: Duration,
    list_catalog: Duration,
    search_catalog: Duration,
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "performance smoke; run with `make rust-perf-smoke`"]
async fn mcp_catalog_perf_smoke_stays_below_regression_thresholds()
-> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let config_dir = temp_dir.path().join("coral-config");
    write_file_credentials_config(&config_dir)?;
    seed_synthetic_http_sources(&config_dir).await?;

    let timings = run_mcp_perf_smoke(&config_dir).await?;
    assert_under("tools/list", timings.tools_list, TOOLS_LIST_LIMIT, &timings);
    assert_under(
        "list_catalog",
        timings.list_catalog,
        LIST_CATALOG_LIMIT,
        &timings,
    );
    assert_under(
        "search_catalog",
        timings.search_catalog,
        SEARCH_CATALOG_LIMIT,
        &timings,
    );
    Ok(())
}

async fn run_mcp_perf_smoke(config_dir: &Path) -> Result<Timings, Box<dyn std::error::Error>> {
    let mut child = Command::new(env!("CARGO_BIN_EXE_coral"))
        .arg("mcp-stdio")
        .env("CORAL_CONFIG_DIR", config_dir)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;
    let mut stdin = child.stdin.take().expect("mcp stdio stdin");
    let stdout = child.stdout.take().expect("mcp stdio stdout");
    let mut stdout = BufReader::new(stdout);

    let initialize_elapsed = initialize_mcp(&mut stdin, &mut stdout).await?;
    let tools_list_elapsed = time_tools_list(&mut stdin, &mut stdout).await?;
    let list_catalog_elapsed = time_tool_call(
        &mut stdin,
        &mut stdout,
        3,
        "list_catalog",
        json!({ "limit": 5, "offset": 0 }),
    )
    .await?;
    let search_catalog_elapsed = time_tool_call(
        &mut stdin,
        &mut stdout,
        4,
        "search_catalog",
        json!({ "pattern": "events|lookup", "limit": 5, "offset": 0 }),
    )
    .await?;

    drop(stdin);
    if timeout(PROCESS_EXIT_TIMEOUT, child.wait()).await.is_err() {
        child.start_kill()?;
        child.wait().await?;
    }

    let timings = Timings {
        initialize: initialize_elapsed,
        tools_list: tools_list_elapsed,
        list_catalog: list_catalog_elapsed,
        search_catalog: search_catalog_elapsed,
    };
    eprintln!(
        "mcp perf smoke: initialize={} tools/list={} list_catalog={} search_catalog={}",
        millis(timings.initialize),
        millis(timings.tools_list),
        millis(timings.list_catalog),
        millis(timings.search_catalog)
    );
    Ok(timings)
}

async fn initialize_mcp(
    stdin: &mut ChildStdin,
    stdout: &mut BufReader<ChildStdout>,
) -> Result<Duration, Box<dyn std::error::Error>> {
    let (initialize, elapsed) = timed_request(
        stdin,
        stdout,
        1,
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": {
                    "name": "coral-cli-perf-smoke",
                    "version": "0.0.0"
                }
            }
        }),
    )
    .await?;
    assert_success_response("initialize", &initialize);
    write_jsonrpc_message(
        stdin,
        &json!({
            "jsonrpc": "2.0",
            "method": "notifications/initialized",
            "params": {}
        }),
    )
    .await?;
    Ok(elapsed)
}

async fn time_tools_list(
    stdin: &mut ChildStdin,
    stdout: &mut BufReader<ChildStdout>,
) -> Result<Duration, Box<dyn std::error::Error>> {
    let (tools_list, elapsed) = timed_request(
        stdin,
        stdout,
        2,
        json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/list",
            "params": {}
        }),
    )
    .await?;
    assert_success_response("tools/list", &tools_list);
    assert_tool_advertised(&tools_list, "list_catalog");
    assert_tool_advertised(&tools_list, "search_catalog");
    Ok(elapsed)
}

async fn time_tool_call(
    stdin: &mut ChildStdin,
    stdout: &mut BufReader<ChildStdout>,
    id: i64,
    name: &str,
    arguments: Value,
) -> Result<Duration, Box<dyn std::error::Error>> {
    let (response, elapsed) = timed_request(
        stdin,
        stdout,
        id,
        json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": "tools/call",
            "params": {
                "name": name,
                "arguments": arguments
            }
        }),
    )
    .await?;
    assert_success_tool_response(name, &response);
    Ok(elapsed)
}

fn write_file_credentials_config(config_dir: &Path) -> Result<(), std::io::Error> {
    std::fs::create_dir_all(config_dir)?;
    std::fs::write(
        config_dir.join("config.toml"),
        "[credentials]\nstorage = \"file\"\n",
    )
}

async fn seed_synthetic_http_sources(config_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let server = ServerBuilder::new()
        .with_config_dir(config_dir)
        .start()
        .await?;
    let app = AppClient::connect(server.endpoint_uri()).await?;
    for source_index in 0..SYNTHETIC_HTTP_SOURCE_COUNT {
        import_source(&app, synthetic_http_manifest(source_index)).await?;
    }
    server.shutdown().await?;
    Ok(())
}

async fn import_source(
    app: &AppClient,
    manifest_yaml: String,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut responses = app
        .source_client()
        .import_source(Request::new(ImportSourceRequest {
            workspace: Some(default_workspace()),
            manifest_yaml,
            variables: Vec::<SourceVariable>::new(),
            secrets: Vec::<SourceSecret>::new(),
            oauth_credential_retrievals: Vec::new(),
        }))
        .await?
        .into_inner();
    while let Some(response) = responses.message().await? {
        if matches!(
            response.event,
            Some(import_source_response::Event::Source(_))
        ) {
            return Ok(());
        }
    }
    Err("import source stream ended without source".into())
}

fn synthetic_http_manifest(source_index: usize) -> String {
    format!(
        r#"
name: perf_http_{source_index}
version: 0.1.0
dsl_version: 3
backend: http
base_url: https://example.com
tables:
  - name: events
    description: Synthetic events for catalog perf smoke
    request: {{ method: GET, path: /events }}
    response: {{}}
    columns:
      - {{ name: id, type: Utf8 }}
      - {{ name: title, type: Utf8 }}
functions:
  - name: lookup_event
    description: Synthetic event lookup for catalog perf smoke
    args:
      - {{ name: id, required: true, bind: {{ arg: id }} }}
    request: {{ method: GET, path: "/events/{{{{arg.id}}}}" }}
    response: {{}}
    columns:
      - {{ name: id, type: Utf8 }}
      - {{ name: title, type: Utf8 }}
"#
    )
}

async fn timed_request(
    stdin: &mut ChildStdin,
    stdout: &mut BufReader<ChildStdout>,
    id: i64,
    message: Value,
) -> Result<(Value, Duration), Box<dyn std::error::Error>> {
    let start = Instant::now();
    write_jsonrpc_message(stdin, &message).await?;
    let response = read_jsonrpc_response(stdout, id).await?;
    Ok((response, start.elapsed()))
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
        let bytes_read = timeout(MCP_RESPONSE_TIMEOUT, stdout.read_line(&mut line)).await??;
        if bytes_read == 0 {
            return Err(format!("mcp stdio closed before response id {id}").into());
        }
        let response: Value = serde_json::from_str(line.trim_end())?;
        if response.get("id").and_then(Value::as_i64) == Some(id) {
            return Ok(response);
        }
    }
}

fn assert_success_response(operation: &str, response: &Value) {
    assert!(
        response.get("error").is_none(),
        "{operation} returned JSON-RPC error: {response}"
    );
    assert!(
        response.get("result").is_some(),
        "{operation} response should contain result: {response}"
    );
}

fn assert_success_tool_response(operation: &str, response: &Value) {
    assert_success_response(operation, response);
    assert_eq!(
        response.pointer("/result/isError").and_then(Value::as_bool),
        Some(false),
        "{operation} tool response should not be an error: {response}"
    );
    assert!(
        response.pointer("/result/structuredContent").is_some(),
        "{operation} should include structured content: {response}"
    );
}

fn assert_tool_advertised(response: &Value, expected_name: &str) {
    let tools = response
        .pointer("/result/tools")
        .and_then(Value::as_array)
        .expect("tools/list response should contain tools");
    assert!(
        tools
            .iter()
            .any(|tool| tool.get("name").and_then(Value::as_str) == Some(expected_name)),
        "tools/list should advertise {expected_name}: {response}"
    );
}

fn assert_under(operation: &str, elapsed: Duration, limit: Duration, timings: &Timings) {
    assert!(
        elapsed <= limit,
        "{operation} took {}, expected <= {}; full timings: {timings:?}",
        millis(elapsed),
        millis(limit)
    );
}

fn millis(duration: Duration) -> String {
    format!("{:.3} ms", duration.as_secs_f64() * 1000.0)
}
