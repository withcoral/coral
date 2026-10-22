use coral_outbound::DestinationPolicy;
use coral_workspace::{
    ChangeContext, TraceId, WorkspaceDisplayName, WorkspaceModule, local_organization_owner,
};
use rmcp::ServiceExt as _;
use rmcp::transport::StreamableHttpClientTransport;
use tempfile::TempDir;

use super::{
    LOCAL_OUTBOUND_DEFAULTS, Launch, RunningRuntime, ServeError, ServeErrorKind, ServerBuilder,
    start_prepared, with_local_launch_defaults,
};

const WEB: &str = "\n[listeners.web]\npublic_url = 'http://127.0.0.1:5173'\n";

async fn start(builder: ServerBuilder) -> Result<RunningRuntime, ServeError> {
    let builder = builder
        .prepare()
        .await
        .map_err(|error| ServeError(Box::new(ServeErrorKind::Config(error))))?;
    start_prepared(builder).await
}

/// Writes an `[auth]` deployment, with its session signing key, into `dir`.
fn write_authenticated_config(dir: &std::path::Path) {
    std::fs::write(dir.join("session.key"), coral_test_support::p256_pkcs8()).unwrap();
    std::fs::write(
        dir.join("config.toml"),
        format!(
            "[listeners.mcp]\npublic_url = 'https://mcp.example.test'\n{WEB}\
             [auth]\n[listeners.auth]\npublic_url = 'https://auth.example.test'\n\
             [auth.session]\nsigning_key_file = 'session.key'\n\
             [auth.provider]\nissuer = 'https://accounts.example.test'\n\
             client_id = 'upstream-client'\nclient_secret = 'test-secret'\n"
        ),
    )
    .unwrap();
}

/// The launcher gives the implicit-user local experience and Desktop local
/// outbound defaults; an authenticated deployment it did not launch from
/// Desktop keeps deny/deny. Desktop's local auth support takes its fixed port.
#[tokio::test]
async fn launch_selects_local_outbound_defaults_for_local_experiences() {
    for (desktop, authenticated, expected, auth_port) in [
        (false, false, LOCAL_OUTBOUND_DEFAULTS, 0),
        (false, true, DestinationPolicy::default(), 0),
        (true, false, LOCAL_OUTBOUND_DEFAULTS, 8779),
        (true, true, LOCAL_OUTBOUND_DEFAULTS, 0),
    ] {
        let temp = TempDir::new().unwrap();
        if authenticated {
            write_authenticated_config(temp.path());
        } else {
            std::fs::write(temp.path().join("config.toml"), WEB).unwrap();
        }
        let launch = Launch {
            desktop,
            ..Launch::default()
        };
        let prepared = launch
            .configure(ServerBuilder::new().with_config_dir(temp.path()))
            .prepare()
            .await
            .unwrap();
        let settings = with_local_launch_defaults(prepared)
            .unwrap()
            .serve_settings()
            .unwrap();
        assert_eq!(
            settings.outbound_policy(),
            expected,
            "desktop {desktop}, authenticated {authenticated}"
        );
        assert_eq!(settings.auth_bind_addr().port(), auth_port);
    }
    assert_ne!(LOCAL_OUTBOUND_DEFAULTS, DestinationPolicy::default());
}

#[tokio::test]
async fn listeners_report_readiness_separately_from_process_liveness() {
    let temp = TempDir::new().unwrap();
    std::fs::write(temp.path().join("config.toml"), WEB).unwrap();
    let server = start(ServerBuilder::new().with_config_dir(temp.path()))
        .await
        .unwrap();
    let client = reqwest::Client::new();
    let mcp = format!("http://{}", server.mcp_http_addr());
    assert_eq!(
        client
            .get(format!("{mcp}/healthz"))
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    assert_eq!(
        client
            .get(format!("{mcp}/readyz"))
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    let auth = server
        .auth_support
        .as_ref()
        .expect("local deployments also have auth-support");
    let auth_origin = auth.endpoint_uri().to_owned();
    assert_ne!(auth.local_addr(), server.mcp_http_addr());
    assert_eq!(
        client
            .get(format!("{auth_origin}/readyz"))
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    server.grpc.database().close().await;
    tokio::time::sleep(std::time::Duration::from_millis(2100)).await;
    for origin in [&mcp, &auth_origin] {
        assert_eq!(
            client
                .get(format!("{origin}/healthz"))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        assert_eq!(
            client
                .get(format!("{origin}/readyz"))
                .send()
                .await
                .unwrap()
                .status(),
            503
        );
    }
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn configured_local_auth_bind_is_honored_without_enabling_oauth() {
    let temp = TempDir::new().unwrap();
    let occupied = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = occupied.local_addr().unwrap();
    std::fs::write(
        temp.path().join("config.toml"),
        format!("[listeners.auth]\nbind = '{address}'\n{WEB}"),
    )
    .unwrap();
    let error = match start(ServerBuilder::new().with_config_dir(temp.path())).await {
        Err(error) => error,
        Ok(server) => {
            server.shutdown().await.unwrap();
            panic!("configured auth bind was ignored");
        }
    };
    assert!(
        matches!(*error.0, ServeErrorKind::AuthSupportStart(_)),
        "{error}"
    );
    drop(occupied);
    let server = start(ServerBuilder::new().with_config_dir(temp.path()))
        .await
        .unwrap();
    assert_eq!(server.auth_support.as_ref().unwrap().local_addr(), address);
    let client = reqwest::Client::new();
    assert_eq!(
        client
            .get(format!("http://{address}/readyz"))
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    assert_eq!(
        client
            .get(format!("http://{address}/oauth/authorize"))
            .send()
            .await
            .unwrap()
            .status(),
        404
    );
    server.shutdown().await.unwrap();
    drop(std::net::TcpListener::bind(address).expect("auth bind released"));
}

#[tokio::test]
async fn an_auth_support_outage_does_not_change_mcp_readiness() {
    let temp = TempDir::new().unwrap();
    std::fs::write(temp.path().join("config.toml"), WEB).unwrap();
    let mut server = start(ServerBuilder::new().with_config_dir(temp.path()))
        .await
        .unwrap();
    let auth = server.auth_support.take().unwrap();
    let auth_address = auth.local_addr();
    auth.shutdown().await.unwrap();
    let client = reqwest::Client::new();
    client
        .get(format!("http://{auth_address}/readyz"))
        .send()
        .await
        .expect_err("stopped auth-support listener cannot answer");
    for path in ["healthz", "readyz"] {
        assert_eq!(
            client
                .get(format!("http://{}/{path}", server.mcp_http_addr()))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
    }
    server.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn mcp_bind_failure_aborts_startup_and_releases_the_private_listener() {
    let temp = TempDir::new().expect("temporary state root");
    let occupied_mcp = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve MCP port");
    let mcp_addr = occupied_mcp.local_addr().expect("MCP address");
    let grpc_reservation = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve gRPC port");
    let grpc_addr = grpc_reservation.local_addr().expect("gRPC address");
    std::fs::write(
        temp.path().join("config.toml"),
        format!(
            "[listeners.grpc]\nbind = '{grpc_addr}'\n[listeners.mcp]\nbind = '{mcp_addr}'\n{WEB}"
        ),
    )
    .expect("write listener config");
    drop(grpc_reservation);

    let result =
        start(ServerBuilder::configured_standalone_grpc().with_config_dir(temp.path())).await;
    let error = match result {
        Err(error) => error,
        Ok(server) => {
            server.shutdown().await.expect("stop unexpected server");
            panic!("MCP bind failure must not leave a successful gRPC-only runtime");
        }
    };
    assert!(matches!(*error.0, ServeErrorKind::McpStart(_)), "{error}");
    let rebound = std::net::TcpListener::bind(grpc_addr).expect("failed startup releases gRPC");
    drop(rebound);
    drop(occupied_mcp);
}

#[tokio::test(flavor = "multi_thread")]
async fn default_startup_exposes_the_mcp_contract_with_explicit_human_destination() {
    let temp = TempDir::new().expect("temporary state root");
    std::fs::write(temp.path().join("config.toml"), WEB).unwrap();
    let server = start(ServerBuilder::new().with_config_dir(temp.path()))
        .await
        .expect("start composed server");
    let workspaces = WorkspaceModule::new(server.grpc.database());
    let workspace = workspaces
        .create(
            &ChangeContext::new(
                local_organization_owner(),
                1,
                TraceId::parse("11111111111111111111111111111111").expect("trace id"),
            ),
            WorkspaceDisplayName::parse("MCP contract").expect("display name"),
        )
        .await
        .expect("create Workspace");
    let mcp_addr = server.mcp_http_addr();
    assert!(mcp_addr.ip().is_loopback());
    assert_ne!(mcp_addr.port(), 0);
    let urls = crate::WorkspaceMcpUrls::new(
        crate::CanonicalOauthUrl::parse(&format!("http://{mcp_addr}")).expect("MCP origin"),
    )
    .expect("Workspace URLs");
    let endpoint = urls.resource(workspace.id());
    let client =
        ().serve(StreamableHttpClientTransport::from_uri(endpoint))
            .await
            .expect("connect MCP client");

    let tools = client.list_all_tools().await.expect("list MCP tools");
    // coral-mcp owns the tool contract; composition only has to serve it.
    assert!(!tools.is_empty());
    client.cancel().await.expect("close MCP client");
    server.shutdown().await.expect("stop composed server");

    let rebound = std::net::TcpListener::bind(mcp_addr).expect("MCP port released after shutdown");
    drop(rebound);
}

/// Starts a composed runtime over `config` and a Workspace, and connects an MCP
/// client to that Workspace.
async fn runtime_with_mcp_client(
    config: &str,
) -> (
    TempDir,
    RunningRuntime,
    coral_workspace::WorkspaceId,
    rmcp::service::RunningService<rmcp::RoleClient, ()>,
) {
    let temp = TempDir::new().expect("temporary state root");
    std::fs::write(temp.path().join("config.toml"), config).unwrap();
    let (server, workspace, client) = serve_with_mcp_client(temp.path()).await;
    (temp, server, workspace, client)
}

/// Starts a composed runtime on the state root `dir`, whose configuration is
/// written already, with a Workspace and an MCP client connected to it.
async fn serve_with_mcp_client(
    dir: &std::path::Path,
) -> (
    RunningRuntime,
    coral_workspace::WorkspaceId,
    rmcp::service::RunningService<rmcp::RoleClient, ()>,
) {
    let server = start(ServerBuilder::new().with_config_dir(dir))
        .await
        .expect("start composed server");
    let workspace = WorkspaceModule::new(server.grpc.database())
        .create(
            &ChangeContext::new(
                local_organization_owner(),
                1,
                TraceId::parse("22222222222222222222222222222222").expect("trace id"),
            ),
            WorkspaceDisplayName::parse("Shared Runs").expect("display name"),
        )
        .await
        .expect("create Workspace");
    let urls = crate::WorkspaceMcpUrls::new(
        crate::CanonicalOauthUrl::parse(&format!("http://{}", server.mcp_http_addr()))
            .expect("MCP origin"),
    )
    .expect("Workspace URLs");
    let client = ()
        .serve(StreamableHttpClientTransport::from_uri(
            urls.resource(workspace.id()),
        ))
        .await
        .expect("connect MCP client");
    (server, workspace.id().clone(), client)
}

/// Starts a Run over MCP whose Program spins and commits nothing after it
/// starts, so only a stop ends it; returns its id once it is running.
async fn spin_a_run(client: &rmcp::service::RunningService<rmcp::RoleClient, ()>) -> String {
    let task = mcp_tool(
        client,
        "start_task",
        serde_json::json!({"intent": "Spin until stopped"}),
    )
    .await;
    let task_id = task
        .get("task_id")
        .and_then(serde_json::Value::as_str)
        .expect("task id");
    let exec = mcp_tool(
        client,
        "exec",
        serde_json::json!({
            "task_id": task_id,
            "intent": "Spin until stopped",
            "program": "while (true) {}",
            "wait_ms": 200,
        }),
    )
    .await;
    assert_eq!(exec.get("state"), Some(&serde_json::json!("running")));
    exec.get("run_id")
        .and_then(serde_json::Value::as_str)
        .expect("run id")
        .to_owned()
}

/// A connection of its own to the SQLite state database at `dir`.
async fn state_database(dir: &std::path::Path) -> sqlx::SqliteConnection {
    use sqlx::Connection as _;
    sqlx::SqliteConnection::connect_with(
        &sqlx::sqlite::SqliteConnectOptions::new().filename(dir.join("coral.db")),
    )
    .await
    .expect("open the state database")
}

/// The state of the Run `run_id` as the SQLite state database at `dir`
/// records it.
async fn recorded_run_state(dir: &std::path::Path, run_id: &str) -> String {
    let (state,): (String,) = sqlx::query_as("SELECT state FROM program_run_records WHERE id = ?")
        .bind(run_id)
        .fetch_one(&mut state_database(dir).await)
        .await
        .expect("read the Run");
    state
}

async fn mcp_tool(
    client: &rmcp::service::RunningService<rmcp::RoleClient, ()>,
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

/// The `[runtime]` section reaches the process's one Run module, which the
/// gRPC server and the MCP companion share.
#[tokio::test(flavor = "multi_thread")]
async fn the_configured_owner_limits_reach_the_shared_run_module() {
    const CAP: usize = 16 << 20;
    const FAN_OUT: usize = 7;
    assert_ne!(CAP, coral_run::AppliedOutputCap::DEFAULT_BYTES);
    assert_ne!(FAN_OUT, coral_run::FanOutLimit::DEFAULT_INVOCATIONS);
    let (_temp, server, _workspace, client) = runtime_with_mcp_client(&format!(
        "{WEB}[runtime]\napplied_output_cap_bytes = {CAP}\nfan_out_limit = {FAN_OUT}\n"
    ))
    .await;
    let runs = server.grpc.runs();
    assert_eq!(runs.applied_output_cap().bytes(), CAP);
    assert_eq!(runs.fan_out_limit().invocations(), FAN_OUT);
    client.cancel().await.expect("close MCP client");
    server.shutdown().await.expect("stop composed server");
}

/// A Run started over MCP and cancelled from Reef over gRPC stops at once:
/// both surfaces share one set of Run Owners, so the cancel reaches the owner
/// instead of waiting out the shutdown grace and forcing the Run to end.
#[tokio::test(flavor = "multi_thread")]
async fn a_reef_cancel_stops_a_run_started_over_mcp_promptly() {
    use coral_grpc::coral::management::v1::ActivityRunState;
    let (_temp, server, workspace, client) = runtime_with_mcp_client(WEB).await;
    let run_id = spin_a_run(&client).await;

    let started = std::time::Instant::now();
    let cancelled = cancel_over_grpc(server.endpoint_uri(), &workspace, &run_id)
        .await
        .expect("cancel the Run");
    let elapsed = started.elapsed();
    assert_eq!(cancelled.state(), ActivityRunState::Canceled);
    // A cancel that cannot find the owner waits out the 5 s shutdown grace.
    assert!(
        elapsed < std::time::Duration::from_secs(3),
        "the cancel waited out the shutdown grace instead of reaching the owner: {elapsed:?}"
    );

    client.cancel().await.expect("close MCP client");
    server.shutdown().await.expect("stop composed server");
}

/// Cancels the Run `run_id` of `workspace` over the private gRPC listener
/// at `endpoint`, as Reef does.
async fn cancel_over_grpc(
    endpoint: &str,
    workspace: &coral_workspace::WorkspaceId,
    run_id: &str,
) -> Result<coral_grpc::coral::management::v1::CancelActivityRunResponse, tonic::Status> {
    use coral_grpc::coral::management::v1::{
        CancelActivityRunRequest, activity_service_client::ActivityServiceClient,
    };
    let mut activity = ActivityServiceClient::connect(endpoint.to_owned())
        .await
        .expect("connect gRPC client");
    let mut request = tonic::Request::new(CancelActivityRunRequest {
        name: format!("workspaces/{workspace}/runs/{run_id}"),
    });
    request
        .metadata_mut()
        .insert("coral-audience", "reef".parse().expect("audience"));
    activity
        .cancel_activity_run(request)
        .await
        .map(tonic::Response::into_inner)
}

/// The expired-lease sweep's first pass runs before any listener binds. A
/// Run left running by a process that died (its Run Owner gone, its lease
/// lapsed) has its owner recreated by the next process before that process
/// serves, so a cancel sent the moment the listeners bind waits behind the
/// recovered owner's chunk and is answered from the record: interrupted,
/// never `unavailable` for a Run with no live owner.
#[test]
fn the_first_expired_lease_sweep_runs_before_the_listeners_bind() {
    use coral_grpc::coral::management::v1::ActivityRunState;
    let temp = TempDir::new().expect("temporary state root");
    std::fs::write(temp.path().join("config.toml"), WEB).unwrap();
    // The process that dies: its runtime goes away with the Run executing,
    // and its owner writes nothing more.
    let dying = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("the dying process's runtime");
    let (workspace, run_id) = dying.block_on(async {
        let (server, workspace, client) = serve_with_mcp_client(temp.path()).await;
        let run_id = spin_a_run(&client).await;
        drop(client);
        drop(server);
        (workspace, run_id)
    });
    dying.shutdown_timeout(std::time::Duration::from_secs(10));

    let next = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("the next process's runtime");
    next.block_on(async {
        assert_eq!(recorded_run_state(temp.path(), &run_id).await, "running");
        // The lease the dead owner last renewed lapses, without waiting it
        // out.
        sqlx::query(
            "UPDATE program_run_records \
             SET worker_claim_expires_at_unix_nanos = accepted_at_unix_nanos WHERE id = ?",
        )
        .bind(&run_id)
        .execute(&mut state_database(temp.path()).await)
        .await
        .expect("lapse the dead owner's lease");
        let server = start(ServerBuilder::new().with_config_dir(temp.path()))
            .await
            .expect("start the next process");

        let cancelled = cancel_over_grpc(server.endpoint_uri(), &workspace, &run_id)
            .await
            .expect("the recovered owner answers from the record");
        assert_eq!(cancelled.state(), ActivityRunState::Interrupted);
        server.shutdown().await.expect("stop the next process");
    });
}

/// Once shutdown begins, every listener reports not ready while it keeps
/// serving the live Runs for the Shutdown Drain (gRPC liveness and `/healthz`
/// stay up); the Run still live when the drain is out is interrupted before
/// the listeners stop.
#[tokio::test(flavor = "multi_thread")]
async fn readiness_reports_not_serving_once_shutdown_begins() {
    use tonic_health::pb::{
        HealthCheckRequest, health_check_response::ServingStatus, health_client::HealthClient,
    };
    let (temp, server, _workspace, client) =
        runtime_with_mcp_client(&format!("{WEB}[runtime]\nshutdown_drain_seconds = 1\n")).await;
    let run_id = spin_a_run(&client).await;
    client.cancel().await.expect("close MCP client");
    let mcp = format!("http://{}", server.mcp_http_addr());
    let http = reqwest::Client::new();
    let status = async |path: &str| {
        http.get(format!("{mcp}/{path}"))
            .send()
            .await
            .expect("the MCP listener serves")
            .status()
    };
    let mut health = HealthClient::new(
        tonic::transport::Channel::from_shared(server.endpoint_uri().to_owned())
            .unwrap()
            .connect()
            .await
            .expect("connect gRPC health"),
    );
    let mut check = async |service: &str| {
        health
            .check(HealthCheckRequest {
                service: service.into(),
            })
            .await
            .expect("the gRPC listener serves")
            .into_inner()
            .status
    };
    assert_eq!(
        check("coral.readiness").await,
        ServingStatus::Serving as i32
    );
    assert_eq!(status("readyz").await, 200);

    let shutdown = tokio::spawn(server.shutdown());
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while check("coral.readiness").await != ServingStatus::NotServing as i32 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("readiness reports not serving once shutdown begins");
    assert_eq!(status("readyz").await, 503);
    assert_eq!(status("healthz").await, 200, "the process is alive");
    assert_eq!(check("").await, ServingStatus::Serving as i32);
    assert!(!shutdown.is_finished(), "the drain lets the live Run go on");
    assert_eq!(recorded_run_state(temp.path(), &run_id).await, "running");

    shutdown
        .await
        .expect("shutdown task")
        .expect("stop composed server");
    assert_eq!(
        recorded_run_state(temp.path(), &run_id).await,
        "interrupted",
        "the Run still live when the drain was out"
    );
}
