//! Isolated developer benchmarks.

#![allow(
    clippy::print_stderr,
    clippy::print_stdout,
    reason = "benchmark binary intentionally writes results and errors"
)]

use std::fs;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::Path;
use std::process::ExitCode;

use anyhow::{Context, Result, ensure};
use clap::{Parser, Subcommand};
use coral_api::v1::{
    CreateWorkspaceRequest, ImportSourceRequest, Workspace, import_source_response,
};
use coral_client::{AppClient, local::ServerBuilder, workspace};
use coral_mcp::{
    McpOptions,
    http::{McpHttpConfig, start_auth_disabled},
};
use rmcp::{ServiceExt, model::CallToolRequestParams, transport::StreamableHttpClientTransport};
use serde_json::{Map, Value};
use tempfile::TempDir;
use tiktoken_rs::o200k_base_singleton;
use tonic::Request;
use url::Url;

mod universal_search;

const SCHEMA: &str = "benchmark_columns";
const TABLE: &str = "wide_table";
const COLUMN_COUNT: usize = 50;

/// The workspace this benchmark creates and imports its fixture into.
///
/// A fresh state directory owns no workspace, so the harness has to create the
/// one it measures against and name it on every request that follows.
const WORKSPACE: &str = "benchmarks";
const TASK_INTENT: &str = "Measure the token cost of list_columns";

#[derive(Debug, Parser)]
#[command(
    name = "coral-benchmarks",
    about = "Isolated developer benchmarks for Coral"
)]
struct Cli {
    #[command(subcommand)]
    benchmark: Benchmark,
}

#[derive(Debug, Subcommand)]
enum Benchmark {
    /// Measure the token cost of the current `list_columns` response.
    ListColumns,
    /// Build, collect, replay, and report Universal Search relevance corpora.
    UniversalSearch(universal_search::Args),
}

fn main() -> ExitCode {
    match run() {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::from(1),
        Err(error) => {
            eprintln!("coral-benchmarks: {error:#}");
            ExitCode::from(2)
        }
    }
}

fn run() -> Result<bool> {
    match Cli::parse().benchmark {
        Benchmark::ListColumns => {
            tokio::runtime::Runtime::new()
                .context("creating benchmark runtime")?
                .block_on(run_list_columns_benchmark())?;
            Ok(true)
        }
        Benchmark::UniversalSearch(args) => universal_search::run(&args),
    }
}

async fn run_list_columns_benchmark() -> Result<()> {
    let fixture_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("fixtures/list-columns/data")
        .canonicalize()
        .context("resolving list_columns fixture directory")?;
    let fixture_url = Url::from_directory_path(&fixture_dir)
        .map_err(|()| anyhow::anyhow!("fixture path is not absolute: {}", fixture_dir.display()))?;
    let manifest_yaml = include_str!("../fixtures/list-columns/manifest.yaml")
        .replace("__FIXTURE_DATA_URL__", fixture_url.as_str());

    let temp = TempDir::new().context("creating benchmark directory")?;
    let config_dir = temp.path().join("coral-config");
    fs::create_dir_all(&config_dir).context("creating benchmark config directory")?;

    let app_server = ServerBuilder::new()
        .with_config_dir(config_dir)
        .with_noop_feedback_uploads()
        .start()
        .await
        .context("starting benchmark Coral server")?;
    let app = AppClient::connect(app_server.endpoint_uri())
        .await
        .context("connecting benchmark Coral client")?;
    create_benchmark_workspace(&app).await?;
    import_fixture(&app, manifest_yaml).await?;

    let mcp_server = start_auth_disabled(
        McpHttpConfig::new(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
            .context("configuring benchmark MCP HTTP server")?,
        app,
        McpOptions {
            source_names: vec![SCHEMA.to_string()],
            ..McpOptions::default()
        },
    )
    .await
    .context("starting benchmark MCP HTTP server")?;
    let transport = StreamableHttpClientTransport::from_uri(format!(
        "http://{}/mcp/workspace/{WORKSPACE}",
        mcp_server.local_addr()
    ));
    let client = ().serve(transport).await.context("starting benchmark MCP client")?;
    // `list_columns` is task-attributed, so the measurement runs inside a real
    // task exactly as an agent's would: the attribution travels with the call
    // rather than being measured as a special case.
    let task_id = start_benchmark_task(&client).await?;
    let response = client
        .call_tool(
            CallToolRequestParams::new("list_columns").with_arguments(Map::from_iter([
                ("schema".to_string(), Value::String(SCHEMA.to_string())),
                ("table".to_string(), Value::String(TABLE.to_string())),
                ("limit".to_string(), Value::from(COLUMN_COUNT)),
                ("task_id".to_string(), Value::String(task_id.clone())),
                ("intent".to_string(), Value::String(TASK_INTENT.to_string())),
            ])),
        )
        .await
        .context("calling the real MCP list_columns tool")?;
    let content = response
        .structured_content
        .as_ref()
        .context("list_columns response has no structured content")?;
    let total = content
        .get("total")
        .and_then(Value::as_u64)
        .context("list_columns response has no total")?;
    ensure!(
        total == COLUMN_COUNT as u64,
        "fixture exposed {total} columns instead of {COLUMN_COUNT}"
    );
    let returned_columns = ["columns", "rows"]
        .iter()
        .find_map(|field| content.get(field).and_then(Value::as_array))
        .context("list_columns response has no columns or rows")?;
    ensure!(
        returned_columns.len() == COLUMN_COUNT,
        "fixture returned {} columns instead of {COLUMN_COUNT}",
        returned_columns.len()
    );
    ensure!(
        content.get("has_more").and_then(Value::as_bool) == Some(false),
        "fixture response is unexpectedly paginated"
    );
    let json = serde_json::to_string(&response).context("serializing MCP list_columns response")?;
    let tokens = o200k_base_singleton().encode_ordinary(&json).len();
    println!(
        "list-columns/wide-table: {total} columns, {} bytes, {tokens} tokens (o200k_base)",
        json.len()
    );

    client.cancel().await.context("stopping MCP client")?;
    mcp_server
        .shutdown()
        .await
        .context("stopping benchmark MCP HTTP server")?;
    app_server
        .shutdown()
        .await
        .context("stopping benchmark Coral server")?;
    Ok(())
}

fn benchmark_workspace() -> Workspace {
    workspace(WORKSPACE)
}

async fn start_benchmark_task(
    client: &rmcp::service::RunningService<rmcp::RoleClient, ()>,
) -> Result<String> {
    let started = client
        .call_tool(
            CallToolRequestParams::new("start_task").with_arguments(Map::from_iter([(
                "intent".to_string(),
                Value::String(TASK_INTENT.to_string()),
            )])),
        )
        .await
        .context("starting the benchmark MCP task")?;
    let task_id = started
        .structured_content
        .as_ref()
        .and_then(|content| content.get("task_id"))
        .and_then(Value::as_str)
        .context("start_task response has no task ID")?;
    Ok(task_id.to_string())
}

async fn create_benchmark_workspace(app: &AppClient) -> Result<()> {
    app.workspace_client()
        .create_workspace(Request::new(CreateWorkspaceRequest {
            workspace: Some(benchmark_workspace()),
        }))
        .await
        .context("creating the benchmark workspace")?;
    Ok(())
}

async fn import_fixture(app: &AppClient, manifest_yaml: String) -> Result<()> {
    let mut stream = app
        .source_client()
        .import_source(Request::new(ImportSourceRequest {
            workspace: Some(benchmark_workspace()),
            manifest_yaml,
            variables: Vec::new(),
            secrets: Vec::new(),
            oauth_credential_retrievals: Vec::new(),
        }))
        .await
        .context("importing benchmark source")?
        .into_inner();
    while let Some(response) = stream
        .message()
        .await
        .context("reading benchmark source import response")?
    {
        if matches!(
            response.event,
            Some(import_source_response::Event::Source(_))
        ) {
            return Ok(());
        }
    }
    anyhow::bail!("benchmark source import ended without installing the source")
}
