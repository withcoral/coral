//! Token-efficiency benchmark for the MCP `list_columns` result.

#![allow(
    clippy::print_stderr,
    clippy::print_stdout,
    reason = "benchmark binary intentionally writes results and errors"
)]

use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use anyhow::{Context, Result, ensure};
use coral_api::v1::{ImportSourceRequest, SourceSecret, import_source_response};
use coral_client::{AppClient, default_workspace, local::ServerBuilder};
use coral_mcp::{CoralMcpServerFactory, McpOptions};
use rmcp::{
    RoleClient, ServerHandler, ServiceExt, model::CallToolRequestParams, service::RunningService,
};
use serde::Deserialize;
use serde_json::{Map, Value, json};
use tempfile::TempDir;
use tiktoken_rs::o200k_base_singleton;
use tonic::Request;

const COLUMN_FIELDS: [&str; 7] = [
    "column_name",
    "data_type",
    "is_nullable",
    "is_virtual",
    "is_required_filter",
    "description",
    "ordinal_position",
];

#[derive(Debug, Deserialize)]
struct Fixture {
    manifest: PathBuf,
    schema: String,
    table: String,
    limit: u32,
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("coral-benchmarks: {error:#}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<()> {
    ensure!(
        env::args().nth(1).as_deref() == Some("list-columns"),
        "usage: coral-benchmarks list-columns"
    );
    tokio::runtime::Runtime::new()
        .context("creating benchmark runtime")?
        .block_on(run_benchmark())
}

async fn run_benchmark() -> Result<()> {
    let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("benchmark package is inside xtask");
    let fixture: Fixture =
        serde_json::from_str(include_str!("../fixtures/list-columns/github-issues.json"))
            .context("parsing list_columns benchmark fixture")?;
    let manifest_path = workspace_root.join(&fixture.manifest);
    let manifest_yaml = benchmark_manifest(&manifest_path)?;

    let temp = TempDir::new().context("creating benchmark directory")?;
    let config_dir = temp.path().join("coral-config");
    fs::create_dir_all(&config_dir).context("creating benchmark config directory")?;
    fs::write(
        config_dir.join("config.toml"),
        "[credentials]\nstorage = \"file\"\n",
    )
    .context("configuring filesystem credential storage")?;

    let app_server = ServerBuilder::new()
        .with_config_dir(config_dir)
        .with_noop_feedback_uploads()
        .start()
        .await
        .context("starting benchmark Coral server")?;
    let app = AppClient::connect(app_server.endpoint_uri())
        .await
        .context("connecting benchmark Coral client")?;
    import_github(&app, manifest_yaml).await?;

    let handler = CoralMcpServerFactory::new(
        app,
        McpOptions {
            source_names: vec![fixture.schema.clone()],
            ..McpOptions::default()
        },
    )
    .create();
    let (client, mcp_task) = start_mcp_session(handler).await?;
    let current = client
        .call_tool(
            CallToolRequestParams::new("list_columns").with_arguments(Map::from_iter([
                ("schema".to_string(), Value::String(fixture.schema.clone())),
                ("table".to_string(), Value::String(fixture.table.clone())),
                ("limit".to_string(), Value::from(fixture.limit)),
            ])),
        )
        .await
        .context("calling the real MCP list_columns tool")?
        .structured_content
        .context("list_columns returned no structured content")?;
    let field_once = field_once_candidate(&current)?;

    let current = measure_json(&current)?;
    let field_once = measure_json(&field_once)?;
    println!(
        "list_columns {}.{} ({} of {} columns, o200k_base)",
        fixture.schema, fixture.table, fixture.limit, current.total
    );
    println!(
        "current:    {:>6} bytes  {:>6} tokens",
        current.bytes, current.tokens
    );
    println!(
        "field-once: {:>6} bytes  {:>6} tokens",
        field_once.bytes, field_once.tokens
    );
    println!(
        "saved:      {:>6} bytes  {:>6} tokens",
        percent_saved(current.bytes, field_once.bytes),
        percent_saved(current.tokens, field_once.tokens)
    );
    ensure!(field_once.tokens < current.tokens);

    client.cancel().await.context("stopping MCP client")?;
    mcp_task.await.context("joining MCP server task")??;
    app_server
        .shutdown()
        .await
        .context("stopping benchmark Coral server")?;
    Ok(())
}

fn benchmark_manifest(path: &Path) -> Result<String> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("reading benchmark manifest {}", path.display()))?;
    let mut manifest: serde_yaml::Value =
        serde_yaml::from_str(&raw).context("parsing benchmark manifest")?;
    manifest
        .as_mapping_mut()
        .context("benchmark manifest root is not a mapping")?
        .remove(serde_yaml::Value::String("test_queries".to_string()));
    serde_yaml::to_string(&manifest).context("serializing benchmark manifest")
}

async fn import_github(app: &AppClient, manifest_yaml: String) -> Result<()> {
    let mut stream = app
        .source_client()
        .import_source(Request::new(ImportSourceRequest {
            workspace: Some(default_workspace()),
            manifest_yaml,
            variables: Vec::new(),
            secrets: vec![SourceSecret {
                key: "GITHUB_TOKEN".to_string(),
                value: "coral-benchmark-fake-token".to_string(),
            }],
            oauth_credential_retrievals: Vec::new(),
        }))
        .await
        .context("importing benchmark GitHub source")?
        .into_inner();
    while let Some(response) = stream
        .message()
        .await
        .context("reading GitHub source import response")?
    {
        if matches!(
            response.event,
            Some(import_source_response::Event::Source(_))
        ) {
            return Ok(());
        }
    }
    anyhow::bail!("GitHub source import ended without installing the source")
}

async fn start_mcp_session(
    server: impl ServerHandler + Clone,
) -> Result<(
    RunningService<RoleClient, ()>,
    tokio::task::JoinHandle<Result<()>>,
)> {
    let (server_transport, client_transport) = tokio::io::duplex(64 * 1024);
    let task = tokio::spawn(async move {
        let server = Box::pin(server.serve(server_transport))
            .await
            .context("starting MCP server")?;
        server.waiting().await.context("running MCP server")?;
        Ok(())
    });
    let client = ().serve(client_transport).await.context("starting MCP client")?;
    Ok((client, task))
}

fn field_once_candidate(current: &Value) -> Result<Value> {
    let mut page = current
        .as_object()
        .context("list_columns output is not an object")?
        .clone();
    let columns = page
        .remove("columns")
        .and_then(|value| value.as_array().cloned())
        .context("list_columns output has no columns array")?;
    let rows = columns
        .iter()
        .map(|column| {
            Value::Array(
                COLUMN_FIELDS
                    .iter()
                    .map(|field| column.get(field).cloned().unwrap_or(Value::Null))
                    .collect(),
            )
        })
        .collect();
    page.insert("fields".to_string(), json!(COLUMN_FIELDS));
    page.insert("rows".to_string(), Value::Array(rows));
    Ok(Value::Object(page))
}

struct JsonMeasurement {
    bytes: usize,
    tokens: usize,
    total: u64,
}

fn measure_json(value: &Value) -> Result<JsonMeasurement> {
    let json = serde_json::to_string(value).context("serializing benchmark output")?;
    Ok(JsonMeasurement {
        bytes: json.len(),
        tokens: o200k_base_singleton().encode_ordinary(&json).len(),
        total: value
            .get("total")
            .and_then(Value::as_u64)
            .context("list_columns output has no total")?,
    })
}

fn percent_saved(before: usize, after: usize) -> String {
    let tenths = before
        .saturating_sub(after)
        .saturating_mul(1_000)
        .saturating_add(before / 2)
        .checked_div(before)
        .unwrap_or_default();
    format!("{}.{:01}%", tenths / 10, tenths % 10)
}
