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
