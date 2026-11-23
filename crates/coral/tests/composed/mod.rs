//! A composed Coral in the test process, whose local Run Executors are real
//! `coral` children, with a Workspace and an MCP client connected to it.
#![allow(
    dead_code,
    clippy::indexing_slicing,
    reason = "each test binary uses some of the helpers, and a response that lacks a field fails \
              the test"
)]

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use coral_grpc::coral::management::v1::CreateWorkspaceRequest;
use coral_grpc::coral::management::v1::workspace_service_client::WorkspaceServiceClient;
use rmcp::ServiceExt as _;
use rmcp::transport::StreamableHttpClientTransport;
use tempfile::TempDir;

pub(crate) const WEB: &str = "\n[listeners.web]\npublic_url = 'http://127.0.0.1:5173'\n";

pub(crate) type McpClient = rmcp::service::RunningService<rmcp::RoleClient, ()>;

/// A composed runtime on its own state root, with a Workspace and an MCP
/// client connected to it.
pub(crate) struct Composed {
    pub(crate) state_root: TempDir,
    pub(crate) runtime: coral_app::RunningRuntime,
    pub(crate) workspace: String,
    pub(crate) client: McpClient,
}

/// Starts a composed runtime over `config` on a new state root.
pub(crate) async fn compose(config: &str) -> Composed {
    compose_with(config, None).await
}

/// [`compose`], with `factory` registered as the Sub-agent's.
pub(crate) async fn compose_with(
    config: &str,
    factory: Option<Arc<dyn coral_app::SubAgentToolsFactory>>,
) -> Composed {
    let state_root = TempDir::new().expect("temporary state root");
    coral_test_support::write_server_config(state_root.path(), config);
    let runtime = coral_app::start_runtime(coral_app::Launch {
        config_dir: Some(state_root.path().to_owned()),
        run_executor_program: Some(PathBuf::from(env!("CARGO_BIN_EXE_coral"))),
        sub_agent_factory: factory,
        ..coral_app::Launch::default()
    })
    .await
    .expect("start the composed runtime");
    let mut workspaces = WorkspaceServiceClient::connect(runtime.endpoint_uri().to_owned())
        .await
        .expect("connect gRPC");
    let mut request = tonic::Request::new(CreateWorkspaceRequest {
        display_name: "Run Executors".into(),
        request_id: uuid::Uuid::new_v4().to_string(),
    });
    request
        .metadata_mut()
        .insert("coral-audience", "reef".parse().expect("audience"));
    let name = workspaces
        .create_workspace(request)
        .await
        .expect("create a Workspace")
        .into_inner()
        .name;
    let workspace = name
        .strip_prefix("workspaces/")
        .expect("a Workspace name")
        .to_owned();
    let client = ()
        .serve(StreamableHttpClientTransport::from_uri(format!(
            "http://{}/workspaces/{workspace}/mcp",
            runtime.mcp_http_addr()
        )))
        .await
        .expect("connect the MCP client");
    Composed {
        state_root,
        runtime,
        workspace,
        client,
    }
}

pub(crate) async fn mcp_tool(
    client: &McpClient,
    name: &'static str,
    arguments: serde_json::Value,
) -> serde_json::Value {
    let serde_json::Value::Object(arguments) = arguments else {
        panic!("tool arguments must be an object");
    };
    client
        .call_tool(rmcp::model::CallToolRequestParams::new(name).with_arguments(arguments))
        .await
        .unwrap_or_else(|error| panic!("call {name}: {error}"))
        .structured_content
        .unwrap_or_else(|| panic!("{name} returns structured content"))
}

/// Runs `program` over MCP until it ends, and returns its last update.
pub(crate) async fn run_to_end(client: &McpClient, program: &str) -> serde_json::Value {
    let mut update = mcp_tool(
        client,
        "exec",
        serde_json::json!({
            "goal": "Run on a local Run Executor",
            "intent": "Run on a local Run Executor",
            "program": program,
            "wait_ms": 5000,
        }),
    )
    .await;
    tokio::time::timeout(Duration::from_mins(1), async {
        while update["state"] == "running" {
            update = mcp_tool(
                client,
                "wait",
                serde_json::json!({
                    "run_id": update["run_id"],
                    "cursor": update["next_cursor"],
                    "wait_ms": 5000,
                }),
            )
            .await;
        }
    })
    .await
    .expect("the Run ends");
    update
}

/// Starts a Run over MCP whose Program spins and commits nothing after it
/// starts, so only a stop ends it; returns its id once a local child runs it.
pub(crate) async fn spin_a_run(composed: &Composed) -> String {
    let exec = mcp_tool(
        &composed.client,
        "exec",
        serde_json::json!({
            "goal": "Spin until stopped",
            "intent": "Spin until stopped",
            "program": "while (true) {}",
            "wait_ms": 200,
        }),
    )
    .await;
    assert_eq!(exec.get("state"), Some(&serde_json::json!("running")));
    let run_id = exec
        .get("run_id")
        .and_then(serde_json::Value::as_str)
        .expect("run id")
        .to_owned();
    started_on_a_run_executor(composed.state_root.path(), &run_id).await;
    run_id
}

/// Waits up to 30 s until the Run `run_id`, in the SQLite state database at
/// `dir`, has started on a Run Executor.
pub(crate) async fn started_on_a_run_executor(dir: &Path, run_id: &str) {
    let mut connection = state_database(dir).await;
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let (started,): (bool,) = sqlx::query_as(
                "SELECT execution_started_at_unix_nanos IS NOT NULL \
                 FROM program_run_records WHERE id = ?",
            )
            .bind(run_id)
            .fetch_one(&mut connection)
            .await
            .expect("read the Run");
            if started {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the Run starts on a Run Executor");
}

/// The state of the Run `run_id` as the SQLite state database at `dir`
/// records it.
pub(crate) async fn recorded_run_state(dir: &Path, run_id: &str) -> String {
    let mut connection = state_database(dir).await;
    let (state,): (String,) = sqlx::query_as("SELECT state FROM program_run_records WHERE id = ?")
        .bind(run_id)
        .fetch_one(&mut connection)
        .await
        .expect("read the Run");
    state
}

/// The SQLite state database at `dir`.
async fn state_database(dir: &Path) -> sqlx::SqliteConnection {
    use sqlx::Connection as _;
    sqlx::SqliteConnection::connect_with(
        &sqlx::sqlite::SqliteConnectOptions::new().filename(dir.join("coral.db")),
    )
    .await
    .expect("open the state database")
}
