use coral_outbound::DestinationPolicy;
use coral_workspace::{
    ChangeContext, RequestId, TraceId, WorkspaceDisplayName, WorkspaceModule,
    local_organization_owner,
};
use rmcp::ServiceExt as _;
use rmcp::transport::StreamableHttpClientTransport;
use tempfile::TempDir;

use super::{
    AuthServerError, LOCAL_OUTBOUND_DEFAULTS, Launch, McpStartError, RunningRuntime, RuntimeError,
    ServerBuilder, start_prepared, with_local_launch_defaults,
};

mod mcp_interface;

const WEB: &str = "\n[listeners.web]\npublic_url = 'http://127.0.0.1:5173'\n";

async fn start(builder: ServerBuilder) -> Result<RunningRuntime, RuntimeError> {
    start_prepared(builder.prepare().await?).await
}

/// Whether `error` is the auth-support listener that could not bind
/// `address`, after which the private listener stopped cleanly.
fn auth_bind_refused(error: &RuntimeError, address: std::net::SocketAddr) -> bool {
    matches!(
        error,
        RuntimeError::StartAuthSupport {
            error: AuthServerError::Bind { address: refused, .. },
            cleanup: None,
        } if *refused == address
    )
}

/// Writes an `[auth]` deployment, with its session signing key, into `dir`.
fn write_authenticated_config(dir: &std::path::Path) {
    std::fs::write(dir.join("session.key"), coral_test_support::p256_pkcs8()).unwrap();
    coral_test_support::write_server_config(
        dir,
        &format!(
            "[listeners.mcp]\npublic_url = 'https://mcp.example.test'\n{WEB}\
             [auth]\n[listeners.auth]\npublic_url = 'https://auth.example.test'\n\
             [auth.session]\nsigning_key_file = 'session.key'\n\
             [auth.provider]\nissuer = 'https://accounts.example.test'\n\
             client_id = 'upstream-client'\nclient_secret = 'test-secret'\n"
        ),
    );
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
            coral_test_support::write_server_config(temp.path(), WEB);
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
    coral_test_support::write_server_config(temp.path(), WEB);
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
    // Between the release of the address and the server's bind of it, any
    // socket on the host can take the port. The server then fails as it does
    // for an occupied address, and the test tries another address. A server
    // that ignored the bind never fails that way: it starts on an address of
    // its own and fails the comparison below.
    const ADDRESSES: usize = 5;
    let temp = TempDir::new().unwrap();
    let mut lost = 0;
    let (server, address) = loop {
        let occupied = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = occupied.local_addr().unwrap();
        coral_test_support::write_server_config(
            temp.path(),
            &format!("[listeners.auth]\nbind = '{address}'\n{WEB}"),
        );
        let error = match start(ServerBuilder::new().with_config_dir(temp.path())).await {
            Err(error) => error,
            Ok(server) => {
                server.shutdown().await.unwrap();
                panic!("configured auth bind was ignored");
            }
        };
        assert!(auth_bind_refused(&error, address), "{error}");
        drop(occupied);
        match start(ServerBuilder::new().with_config_dir(temp.path())).await {
            Ok(server) => break (server, address),
            Err(error) => {
                lost += 1;
                assert!(
                    auth_bind_refused(&error, address) && lost < ADDRESSES,
                    "{lost} of {ADDRESSES} released addresses: {error}"
                );
            }
        }
    };
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
    coral_test_support::write_server_config(temp.path(), WEB);
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
    coral_test_support::write_server_config(
        temp.path(),
        &format!(
            "[listeners.grpc]\nbind = '{grpc_addr}'\n[listeners.mcp]\nbind = '{mcp_addr}'\n{WEB}"
        ),
    );
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
    assert!(
        matches!(
            error,
            RuntimeError::StartMcp {
                error: McpStartError::Http(_),
                cleanup: None
            }
        ),
        "{error}"
    );
    let rebound = std::net::TcpListener::bind(grpc_addr).expect("failed startup releases gRPC");
    drop(rebound);
    drop(occupied_mcp);
}

#[tokio::test(flavor = "multi_thread")]
async fn default_startup_exposes_the_mcp_contract_with_explicit_human_destination() {
    let temp = TempDir::new().expect("temporary state root");
    coral_test_support::write_server_config(temp.path(), WEB);
    let server = start(ServerBuilder::new().with_config_dir(temp.path()))
        .await
        .expect("start composed server");
    let workspaces = WorkspaceModule::new(server.grpc.database());
    let workspace = workspaces
        .create(
            &ChangeContext::new(
                local_organization_owner(),
                coral_db::UnixNanos::new(1),
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
    assert_ne!(tools, [] as [rmcp::model::Tool; 0]);
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
    coral_test_support::write_server_config(temp.path(), config);
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
    serve_extended_with_mcp_client(dir, ServerBuilder::new()).await
}

/// As [`serve_with_mcp_client`], in the launch that `builder` describes.
async fn serve_extended_with_mcp_client(
    dir: &std::path::Path,
    builder: ServerBuilder,
) -> (
    RunningRuntime,
    coral_workspace::WorkspaceId,
    rmcp::service::RunningService<rmcp::RoleClient, ()>,
) {
    let server = start(builder.with_config_dir(dir))
        .await
        .expect("start composed server");
    let workspace = WorkspaceModule::new(server.grpc.database())
        .create(
            &ChangeContext::new(
                local_organization_owner(),
                coral_db::UnixNanos::new(1),
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

/// A connection of its own to the SQLite state database at `dir`.
async fn state_database(dir: &std::path::Path) -> sqlx::SqliteConnection {
    use sqlx::Connection as _;
    sqlx::SqliteConnection::connect_with(
        &sqlx::sqlite::SqliteConnectOptions::new().filename(dir.join("coral.db")),
    )
    .await
    .expect("open the state database")
}

/// The distinct stamps of the search rows in the SQLite state database at
/// `dir`, once the index has written any.
async fn search_row_stamps(dir: &std::path::Path) -> Vec<(i64,)> {
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        loop {
            let stamps: Vec<(i64,)> =
                sqlx::query_as("SELECT DISTINCT indexed_at_unix_nanos FROM search_entities")
                    .fetch_all(&mut state_database(dir).await)
                    .await
                    .expect("read the search rows' stamps");
            if !stamps.is_empty() {
                break stamps;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the index writes the Built-in Provider's rows")
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

/// The launch's one clock reaches the modules that date a request after the
/// listener admitted it, and the search index. An Agent whose Registration
/// ends an hour after that clock's start, in 2001, ends its Task over MCP,
/// and the private API's modules check the Agent's authority for its Tools
/// and Providers. The index stamps a new Workspace's rows by that clock.
///
/// Catches: bootstrap building the Task, authored Tool or search modules, or
/// the search index, without the launch's clock. Each then reads the host's
/// clock, a quarter of a century later: a module refuses the Agent that the
/// listener admitted, and the index stamps its rows by the host.
#[tokio::test(flavor = "multi_thread")]
#[expect(
    clippy::too_many_lines,
    reason = "one launch shows each module that bootstrap hands its clock to"
)]
async fn the_launchs_clock_dates_an_agents_requests_past_the_listener() {
    use rmcp::transport::streamable_http_client::StreamableHttpClientTransportConfig;

    // 2001-01-01.
    let launched_at = coral_db::UnixNanos::new(978_307_200 * 1_000_000_000);
    let a_minute_in = launched_at
        .checked_add(std::time::Duration::from_mins(1))
        .unwrap();
    let clock = coral_test_support::ManualClock::starting_at(launched_at);
    let temp = TempDir::new().expect("temporary state root");
    coral_test_support::write_server_config(temp.path(), WEB);
    let launch = Launch {
        clock: Some(std::sync::Arc::new(clock.clone())),
        ..Launch::default()
    };
    let server = start(launch.configure(ServerBuilder::new().with_config_dir(temp.path())))
        .await
        .expect("start composed server");
    let owner = local_organization_owner();
    let context = ChangeContext::new(
        owner.clone(),
        launched_at,
        TraceId::parse("33333333333333333333333333333333").expect("trace id"),
    );
    // The modules bootstrap built for the private API.
    let modules = server.grpc.engine().grpc_dependencies();
    let workspace = modules
        .workspace_workflow
        .create_workspace(
            &context,
            &RequestId::generate(),
            WorkspaceDisplayName::parse("Launched in 2001").expect("display name"),
        )
        .await
        .expect("create Workspace");
    // The workflow told the index of the Built-in Provider it installed.
    assert_eq!(search_row_stamps(temp.path()).await, [(launched_at.get(),)]);
    let workspaces = WorkspaceModule::new(server.grpc.database());
    let issued = workspaces
        .create_agent(
            &context,
            &RequestId::generate(),
            workspace.id(),
            coral_workspace::AgentDisplayName::parse("Expiring Agent").expect("Agent name"),
            Some(
                launched_at
                    .checked_add(std::time::Duration::from_hours(1))
                    .unwrap(),
            ),
        )
        .await
        .expect("register the expiring Agent");
    let agent = workspaces
        .authenticate_agent(workspace.id(), issued.credential.expose(), launched_at)
        .await
        .expect("authenticate the Agent");
    let task = modules
        .tasks
        .start(
            &agent,
            workspace.id(),
            coral_task::TaskIntent::parse("Dated by the launch's clock").expect("Task intent"),
            launched_at,
        )
        .await
        .expect("the Task Module reads the launch's clock");

    clock.set(a_minute_in);
    let urls = crate::WorkspaceMcpUrls::new(
        crate::CanonicalOauthUrl::parse(&format!("http://{}", server.mcp_http_addr()))
            .expect("MCP origin"),
    )
    .expect("Workspace URLs");
    let client = ()
        .serve(StreamableHttpClientTransport::from_config(
            StreamableHttpClientTransportConfig::with_uri(urls.resource(workspace.id()))
                .auth_header(issued.credential.expose()),
        ))
        .await
        .expect("connect MCP client as the Agent");
    let ended = mcp_tool(
        &client,
        "end_task",
        serde_json::json!({"task_id": task.id().to_string(), "outcome": "success"}),
    )
    .await;
    assert_eq!(
        ended.get("outcome"),
        Some(&serde_json::json!("success")),
        "{ended}"
    );
    let recorded = modules
        .tasks
        .get(
            &coral_workspace::AuthenticatedPrincipal::user(owner),
            workspace.id(),
            task.id(),
        )
        .await
        .expect("the Owner reads the Agent's Task");
    assert_eq!(recorded.ended_at(), Some(a_minute_in));

    modules
        .private_tools
        .list(
            &agent,
            workspace.id(),
            &coral_test_support::page_request(None, 10),
        )
        .await
        .expect("Private Tool inspection reads the launch's clock");
    modules
        .shared_tools
        .list(
            &agent,
            workspace.id(),
            &coral_test_support::page_request(None, 10),
        )
        .await
        .expect("the Shared Tool Module reads the launch's clock");
    modules
        .provider_readiness
        .searchable_providers(&agent, workspace.id())
        .await
        .expect("the search module reads the launch's clock");
    // Deletion admits the Agent on the launch's clock, and then finds no
    // Private Tool at the path.
    let absent = modules
        .private_tool_workflow
        .delete(
            &coral_tools::PrivateToolChangeContext::new(
                agent.clone(),
                a_minute_in,
                TraceId::current_or_new(),
            ),
            workspace.id(),
            None,
            &coral_tools::PrivateToolPath::parse("absent").expect("path"),
        )
        .await;
    assert!(
        matches!(
            absent,
            Err(coral_tools::Error::NotFound(
                coral_catalog::AuthoredToolKind::Private
            ))
        ),
        "{absent:?}"
    );
    client.cancel().await.expect("close MCP client");
    server.shutdown().await.expect("stop composed server");
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
    let runs = server.grpc.engine().grpc_dependencies().runs;
    assert_eq!(runs.applied_output_cap().bytes(), CAP);
    assert_eq!(runs.fan_out_limit().invocations(), FAN_OUT);
    client.cancel().await.expect("close MCP client");
    server.shutdown().await.expect("stop composed server");
}
