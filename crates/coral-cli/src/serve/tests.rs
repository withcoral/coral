use std::net::TcpListener;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use coral_api::v1::{
    AddWorkspaceMemberRequest, CatalogItemKind, CreateWorkspaceRequest, ListCatalogRequest,
    ListWorkspacesRequest, PaginationRequest, RemoveWorkspaceMemberRequest, Workspace,
    WorkspaceMember, WorkspaceRole,
};
use coral_client::default_workspace;
use jsonwebtoken::jwk::{Jwk, ThumbprintHash};
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use reqwest::header::WWW_AUTHENTICATE;
use ring::rand::SystemRandom;
use ring::signature::{ECDSA_P256_SHA256_FIXED_SIGNING, EcdsaKeyPair};
use rmcp::model::CallToolRequestParams;
use rmcp::service::RunningService;
use rmcp::transport::StreamableHttpClientTransport;
use rmcp::transport::streamable_http_client::StreamableHttpClientTransportConfig;
use rmcp::{RoleClient, ServiceExt as _};
use tempfile::TempDir;
use tonic::Request;

use super::*;

const INITIALIZE: &str = r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"test","version":"1"}}}"#;
const OAUTH_ISSUER: &str = "http://localhost:9080";
const OAUTH_RESOURCE: &str = "http://localhost:1457";
const SESSION_ISSUER: &str = "https://auth.example";
const SESSION_RESOURCE: &str = "https://coral.example/mcp";
const REEF_RESOURCE: &str = "https://reef.example";
const SHARED_WORKSPACE: &str = "shared";

fn shared_workspace() -> Workspace {
    Workspace {
        name: SHARED_WORKSPACE.to_string(),
    }
}

fn shared_mcp_options() -> McpOptions {
    McpOptions {
        workspace: Some(shared_workspace()),
        ..McpOptions::default()
    }
}

fn write_config(temp: &TempDir, config: &str) {
    std::fs::write(temp.path().join("config.toml"), config).expect("write config");
}

fn write_oauth_config(temp: &TempDir, oauth_bind: SocketAddr, mcp_bind: Option<SocketAddr>) {
    let signing_key =
        EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &SystemRandom::new())
            .expect("P-256 signing key");
    std::fs::write(temp.path().join("session.key"), signing_key.as_ref()).expect("session key");
    let mcp_bind = mcp_bind.unwrap_or_else(|| SocketAddr::from((Ipv4Addr::LOCALHOST, 0)));
    write_config(
        temp,
        &format!(
            "[trace_history]\nenabled = false\n\n[server.mcp_http]\nenabled = true\nbind = '{mcp_bind}'\npublic_url = '{OAUTH_RESOURCE}'\n\n[auth]\nhttp_bind_addr = '{oauth_bind}'\n\n[auth.session]\nsigning_key_file = 'session.key'\n\n[auth.authorization_server]\nissuer = '{OAUTH_ISSUER}'\n\n[auth.provider]\nissuer = 'https://accounts.example.test'\nclient_id = 'upstream-client'\nclient_secret = 'test-secret'\nredirect_uri = '{OAUTH_ISSUER}/auth/oidc/callback'\n"
        ),
    );
}

fn grpc_addr(server: &RunningServer) -> SocketAddr {
    server
        .endpoint_uri()
        .strip_prefix("http://")
        .expect("HTTP endpoint")
        .parse()
        .expect("socket address")
}

async fn assert_catalog_tool(endpoint: String, bearer: Option<&str>) {
    const INTENT: &str = "Exercise the composite server";

    let mut config = StreamableHttpClientTransportConfig::with_uri(endpoint);
    if let Some(bearer) = bearer {
        config = config.auth_header(bearer);
    }
    let client =
        ().serve(StreamableHttpClientTransport::from_config(config))
            .await
            .expect("initialize MCP client");
    let task = client
        .call_tool(CallToolRequestParams::new("start_task").with_arguments(
            serde_json::Map::from_iter([("intent".to_string(), serde_json::json!(INTENT))]),
        ))
        .await
        .expect("start task");
    let task = task.structured_content.expect("structured task");
    let task_id = task
        .get("task_id")
        .and_then(serde_json::Value::as_str)
        .expect("task ID");
    let result = client
        .call_tool(CallToolRequestParams::new("list_catalog").with_arguments(
            serde_json::Map::from_iter([
                ("task_id".to_string(), serde_json::json!(task_id)),
                ("intent".to_string(), serde_json::json!(INTENT)),
            ]),
        ))
        .await
        .expect("call list_catalog");
    assert_eq!(result.is_error, Some(false));
    client.cancel().await.expect("stop MCP client");
}

async fn assert_feedback_tool(endpoint: String) {
    let config = StreamableHttpClientTransportConfig::with_uri(endpoint);
    let client =
        ().serve(StreamableHttpClientTransport::from_config(config))
            .await
            .expect("initialize MCP client");
    let tools = client.list_all_tools().await.expect("list MCP tools");
    assert!(tools.iter().any(|tool| tool.name == "feedback"));
    client.cancel().await.expect("stop MCP client");
}

async fn assert_unauthorized(base: &str, authorization: &str) {
    let rejected = reqwest::Client::new()
        .post(format!("{base}/mcp"))
        .header("accept", "application/json, text/event-stream")
        .header("content-type", "application/json")
        .header("authorization", authorization)
        .body(INITIALIZE)
        .send()
        .await
        .expect("MCP response");
    assert_eq!(rejected.status(), reqwest::StatusCode::UNAUTHORIZED);
    assert_eq!(
        rejected.headers().get_all(WWW_AUTHENTICATE).iter().count(),
        1
    );
    assert!(
        rejected
            .headers()
            .get(WWW_AUTHENTICATE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.starts_with("Bearer ")),
        "MCP rejection must include one bearer challenge"
    );
}

/// Asserts the private gRPC data plane refuses a call carrying no credentials.
///
/// This probes the listener rather than `grpc_authentication_enabled`, which is
/// derived from configuration and so reports true even if nobody installed the
/// session provider. It is the assertion that fails if composition stops gating
/// gRPC and the listener quietly falls back to the local principal.
async fn assert_grpc_rejects_unauthenticated(endpoint: &str) {
    let unauthenticated = AppClient::connect(endpoint)
        .await
        .expect("unauthenticated gRPC client");
    let denied = unauthenticated
        .catalog_client()
        .list_catalog(Request::new(catalog_request()))
        .await
        .expect_err("the gRPC data plane must refuse an unauthenticated call");
    assert_eq!(denied.code(), Code::Unauthenticated);
}

fn catalog_request() -> ListCatalogRequest {
    ListCatalogRequest {
        workspace: Some(default_workspace()),
        catalog_name: String::new(),
        schema_name: String::new(),
        kind: CatalogItemKind::Unspecified as i32,
        pagination: Some(PaginationRequest {
            limit: 1,
            offset: 0,
        }),
    }
}

/// Mints a session token through the real issuer.
///
/// Anything the issuer can legitimately emit goes through it, so these tests
/// track the issuer's wire format instead of re-implementing the JWT the
/// verifier expects. Fixtures the issuer can never emit — an already-expired
/// token, or one whose `kid` disclaims the key that signed it — still have to be
/// assembled by hand wherever they are needed.
fn session_token(signing_key: &[u8], audience: &str) -> String {
    session_token_for_subject(signing_key, audience, "alice")
}

fn session_token_for_subject(signing_key: &[u8], audience: &str, subject: &str) -> String {
    coral_app::test_session_tokens::issue_access_token(
        SESSION_ISSUER,
        signing_key,
        Duration::from_mins(5),
        subject,
        "https://client.example/client.json",
        audience,
    )
    .expect("session token")
}

/// Assembles a session token by hand, for fixtures the issuer cannot emit.
///
/// Reserved for tokens the real issuer will never produce: one that already
/// expired (its TTL is a `Duration`, and a zero TTL is rejected outright) or one
/// whose `kid` disclaims the key that signed it. Claims mirror
/// `SessionTokenClaims` exactly so these fixtures fail on the property under
/// test rather than on a shape the verifier never sees. Anything legitimate goes
/// through [`session_token`].
fn signed_session_token(
    signing_key: &EncodingKey,
    key_id: &str,
    audience: &str,
    issued_at: u64,
    expires_at: u64,
    token_id: &str,
) -> String {
    let mut header = Header::new(Algorithm::ES256);
    header.kid = Some(key_id.to_string());
    header.typ = Some("at+jwt".to_string());
    encode(
        &header,
        &serde_json::json!({
            "iss": SESSION_ISSUER,
            "aud": audience,
            "sub": "alice",
            "jti": token_id,
            "client_id": "https://client.example/client.json",
            "iat": issued_at,
            "nbf": issued_at,
            "exp": expires_at,
        }),
        signing_key,
    )
    .expect("session token")
}

fn write_session_config(temp: &TempDir, signing_key: &[u8]) {
    write_session_config_with_mcp(
        temp,
        signing_key,
        r"
[server.mcp_http]
enabled = true
bind = '127.0.0.1:0'
public_url = 'https://CORAL.example/mcp'
",
    );
}

fn write_reef_only_session_config(temp: &TempDir, signing_key: &[u8]) {
    write_session_config_with_mcp(temp, signing_key, "");
}

fn write_session_config_with_mcp(temp: &TempDir, signing_key: &[u8], mcp_http: &str) {
    std::fs::write(temp.path().join("session.key"), signing_key).expect("session key");
    // The uppercase hosts are deliberate: the assertions only prove
    // canonicalization if the configured URLs actually need it.
    write_config(
        temp,
        &format!(
            r"
[trace_history]
enabled = false

{mcp_http}

[auth]
allowed_audiences = ['https://REEF.example/']

[auth.session]
signing_key_file = 'session.key'

[auth.authorization_server]
issuer = 'https://auth.example'

[auth.provider]
issuer = 'https://accounts.example'
client_id = 'upstream-client'
client_secret = 'test-secret'
redirect_uri = 'https://auth.example/auth/oidc/callback'
",
        ),
    );
}

async fn assert_grpc_authenticated(server: &RunningServer, token: &str) {
    let authenticated = connect_with_loopback_bearer(
        server.endpoint_uri(),
        BearerToken::new(token).expect("bearer token"),
    )
    .await
    .expect("authenticated gRPC client");
    let _response = authenticated
        .workspace_client()
        .list_workspaces(Request::new(ListWorkspacesRequest {}))
        .await
        .expect("authenticated workspace listing");
}

async fn prepare_authenticated_workspace(
    temp: &TempDir,
    server: &RunningServer,
    signing_key: &[u8],
    workspace: &str,
) -> (String, String) {
    let identity = coral_app::test_session_tokens::persist_test_login_identity(
        temp.path(),
        "https://accounts.example",
        "alice",
        None,
        "unused-test-attribution",
    )
    .await
    .expect("persist test login identity");
    let token = session_token_for_subject(signing_key, SESSION_RESOURCE, &identity.user_id);
    let client = connect_with_loopback_bearer(
        server.endpoint_uri(),
        BearerToken::new(&token).expect("bearer token"),
    )
    .await
    .expect("authenticated gRPC client");
    client
        .workspace_client()
        .create_workspace(Request::new(CreateWorkspaceRequest {
            workspace: Some(Workspace {
                name: workspace.to_string(),
            }),
        }))
        .await
        .expect("create authenticated workspace");
    (token, identity.user_id)
}

async fn authenticated_mcp_client(endpoint: &str, token: &str) -> RunningService<RoleClient, ()> {
    let config =
        StreamableHttpClientTransportConfig::with_uri(endpoint.to_string()).auth_header(token);
    ().serve(StreamableHttpClientTransport::from_config(config))
        .await
        .expect("initialize authenticated MCP client")
}

async fn assert_authenticated_surfaces(server: &RunningServer, mcp_endpoint: &str, token: &str) {
    assert_grpc_authenticated(server, token).await;
    let client = authenticated_mcp_client(mcp_endpoint, token).await;
    client
        .cancel()
        .await
        .expect("stop authenticated MCP client");
}

#[derive(Debug, Eq, PartialEq)]
struct BearerRejection {
    grpc_code: Code,
    grpc_message: String,
    grpc_details: Vec<u8>,
    status: reqwest::StatusCode,
    challenge: String,
    body: String,
}

async fn assert_bearer_rejected(
    server: &RunningServer,
    mcp_endpoint: &str,
    token: &str,
) -> BearerRejection {
    let client = connect_with_loopback_bearer(
        server.endpoint_uri(),
        BearerToken::new(token).expect("bearer token"),
    )
    .await
    .expect("gRPC client");
    let denied = client
        .catalog_client()
        .list_catalog(Request::new(catalog_request()))
        .await
        .expect_err("invalid session token must fail gRPC");
    let grpc_code = denied.code();
    let grpc_message = denied.message().to_string();
    let grpc_details = denied.details().to_vec();
    assert_eq!(grpc_code, Code::Unauthenticated);
    assert_eq!(grpc_message, "unauthenticated: authentication required");
    assert!(grpc_details.is_empty());
    assert!(!format!("{denied:?}").contains(token));

    let denied = reqwest::Client::new()
        .post(mcp_endpoint)
        .bearer_auth(token)
        .header("accept", "application/json, text/event-stream")
        .header("content-type", "application/json")
        .body(INITIALIZE)
        .send()
        .await
        .expect("invalid session MCP response");
    let status = denied.status();
    let challenge = denied
        .headers()
        .get(WWW_AUTHENTICATE)
        .and_then(|value| value.to_str().ok())
        .expect("ASCII MCP authentication challenge")
        .to_string();
    assert_eq!(denied.headers().get_all(WWW_AUTHENTICATE).iter().count(), 1);
    let body = denied.text().await.expect("invalid session MCP body");
    assert_eq!(status, reqwest::StatusCode::UNAUTHORIZED);
    assert!(body.is_empty());
    BearerRejection {
        grpc_code,
        grpc_message,
        grpc_details,
        status,
        challenge,
        body,
    }
}

#[test]
fn loopback_grpc_endpoint_maps_wildcards_and_rejects_public_addresses() {
    assert_eq!(
        loopback_grpc_endpoint_uri(SocketAddr::from((Ipv4Addr::UNSPECIFIED, 14555)))
            .expect("IPv4 wildcard"),
        "http://127.0.0.1:14555"
    );
    assert_eq!(
        loopback_grpc_endpoint_uri(SocketAddr::from((Ipv6Addr::UNSPECIFIED, 14555)))
            .expect("IPv6 wildcard"),
        "http://[::1]:14555"
    );
    loopback_grpc_endpoint_uri(SocketAddr::from(([192, 0, 2, 1], 14555)))
        .expect_err("public address must be rejected");
    loopback_grpc_endpoint_uri(SocketAddr::new(
        Ipv4Addr::LOCALHOST.to_ipv6_mapped().into(),
        14555,
    ))
    .expect_err("IPv4-mapped IPv6 address must be rejected");
}

#[test]
fn shutdown_failures_retain_every_component_in_order() {
    let failures = ShutdownFailures::from_results(
        Err(McpHttpError::ShutdownTimedOut),
        Err(AuthServerError::Config("OAuth test failure".to_string())),
        Err(LocalServerError::Unavailable(
            "gRPC test failure".to_string(),
        )),
    )
    .expect_err("all shutdown failures");

    assert!(failures.mcp.is_some());
    assert!(failures.oauth.is_some());
    assert!(failures.grpc.is_some());
    assert_eq!(
        failures.to_string(),
        "MCP HTTP: MCP HTTP server shutdown timed out; OAuth: OAuth test failure; gRPC: unavailable: gRPC test failure"
    );
}

#[tokio::test]
async fn auth_disabled_companion_serves_and_shuts_down() {
    let temp = TempDir::new().expect("temp dir");
    write_config(
        &temp,
        "[trace_history]\nenabled = false\n\n[server.mcp_http]\nenabled = true\nbind = '127.0.0.1:0'\n\n[workspaces.default]\n",
    );
    let server = start(
        ServerBuilder::configured_standalone_grpc()
            .with_config_dir(temp.path())
            .with_noop_feedback_uploads(),
        McpOptions::default(),
    )
    .await
    .expect("start composite server");
    let grpc_addr = grpc_addr(&server);
    assert!(!server.grpc_authentication_enabled());
    let mcp_addr = server.mcp_http_addr().expect("MCP HTTP endpoint");
    assert!(server.oauth_addr().is_none());
    assert_catalog_tool(format!("http://{mcp_addr}/mcp"), None).await;
    server.shutdown().await.expect("shutdown composite server");
    let grpc_rebound = TcpListener::bind(grpc_addr).expect("gRPC port must be released");
    let mcp_rebound = TcpListener::bind(mcp_addr).expect("MCP port must be released");
    drop((grpc_rebound, mcp_rebound));
}

#[tokio::test]
async fn companion_uses_supplied_mcp_options() {
    let temp = TempDir::new().expect("temp dir");
    write_config(
        &temp,
        "[trace_history]\nenabled = false\n\n[server.mcp_http]\nenabled = true\nbind = '127.0.0.1:0'\n\n[workspaces.default]\n[workspaces.other]\n",
    );
    let server = start(
        ServerBuilder::configured_standalone_grpc()
            .with_config_dir(temp.path())
            .with_noop_feedback_uploads(),
        McpOptions {
            feedback_enabled: true,
            workspace: Some(default_workspace()),
            ..McpOptions::default()
        },
    )
    .await
    .expect("start composite server");
    let mcp_addr = server.mcp_http_addr().expect("MCP HTTP endpoint");
    assert_feedback_tool(format!("http://{mcp_addr}/mcp")).await;
    server.shutdown().await.expect("shutdown composite server");
}

#[tokio::test]
async fn auth_disabled_companion_guides_when_no_workspace_exists() {
    let temp = TempDir::new().expect("temp dir");
    write_config(
        &temp,
        "[trace_history]\nenabled = false\n\n[server.mcp_http]\nenabled = true\nbind = '127.0.0.1:0'\n",
    );

    let Err(error) = start(
        ServerBuilder::configured_standalone_grpc()
            .with_config_dir(temp.path())
            .with_noop_feedback_uploads(),
        McpOptions::default(),
    )
    .await
    else {
        panic!("empty local state must not choose a workspace");
    };
    assert!(
        error
            .to_string()
            .contains("no workspace is available; create one with `coral workspace create <name>`")
    );
}

#[tokio::test]
async fn auth_disabled_companion_guides_when_workspace_is_ambiguous() {
    let temp = TempDir::new().expect("temp dir");
    write_config(
        &temp,
        "[trace_history]\nenabled = false\n\n[server.mcp_http]\nenabled = true\nbind = '127.0.0.1:0'\n\n[workspaces.alpha]\n[workspaces.beta]\n",
    );

    let Err(error) = start(
        ServerBuilder::configured_standalone_grpc()
            .with_config_dir(temp.path())
            .with_noop_feedback_uploads(),
        McpOptions::default(),
    )
    .await
    else {
        panic!("ambiguous local state must not choose a workspace");
    };
    assert!(
        error
            .to_string()
            .contains("multiple workspaces are available; specify one with `--workspace <name>`")
    );
}

/// The advertised protected-resource identifier and the minted token audience
/// must be the same string, so this configures a `public_url` that
/// canonicalization changes (an uppercase host) and mints against whatever the
/// server advertises.
#[tokio::test]
async fn oauth_and_mcp_companions_serve_and_release_all_listeners() {
    let temp = TempDir::new().expect("temp dir");
    write_oauth_config(
        &temp,
        SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
        Some(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))),
    );
    let server = start(
        ServerBuilder::configured_standalone_grpc()
            .with_config_dir(temp.path())
            .with_noop_feedback_uploads(),
        McpOptions::default(),
    )
    .await
    .expect("start composite server");
    let grpc_addr = grpc_addr(&server);
    let oauth_addr = server.oauth_addr().expect("OAuth endpoint");
    let mcp_addr = server.mcp_http_addr().expect("MCP HTTP endpoint");

    let response = reqwest::get(format!(
        "http://{oauth_addr}/.well-known/oauth-authorization-server"
    ))
    .await
    .expect("OAuth metadata response");
    assert!(response.status().is_success());
    let metadata = response.text().await.expect("OAuth metadata body");
    assert!(metadata.contains(&format!(r#""issuer":"{OAUTH_ISSUER}""#)));

    server.shutdown().await.expect("shutdown composite server");
    let grpc_rebound = TcpListener::bind(grpc_addr).expect("gRPC port must be released");
    let oauth_rebound = TcpListener::bind(oauth_addr).expect("OAuth port must be released");
    let mcp_rebound = TcpListener::bind(mcp_addr).expect("MCP port must be released");
    drop((grpc_rebound, oauth_rebound, mcp_rebound));
}

#[tokio::test]
async fn oauth_start_failure_releases_the_started_grpc_listener() {
    let grpc_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("reserve gRPC port");
    let grpc_addr = grpc_listener.local_addr().expect("gRPC address");
    let occupied_oauth = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("occupy OAuth port");
    let oauth_addr = occupied_oauth.local_addr().expect("OAuth address");
    let temp = TempDir::new().expect("temp dir");
    write_oauth_config(&temp, oauth_addr, None);

    // Hand the live gRPC listener to the server so the reserved port never
    // lapses between selection and bind: a parallel process cannot claim it, so
    // startup fails on the occupied OAuth port and must release the gRPC
    // listener afterward.
    let result = start(
        ServerBuilder::standalone_grpc(grpc_addr)
            .with_config_dir(temp.path())
            .with_noop_feedback_uploads()
            .with_prebound_grpc_listener(grpc_listener),
        McpOptions::default(),
    )
    .await;
    let Err(error) = result else {
        panic!("occupied OAuth address must fail startup");
    };
    assert!(
        error
            .to_string()
            .contains("failed to bind authorization server")
    );
    let rebound = TcpListener::bind(grpc_addr).expect("gRPC listener must be released");
    drop((rebound, occupied_oauth));
}

#[tokio::test]
async fn session_authenticated_companion_gates_grpc_and_mcp() {
    let temp = TempDir::new().expect("temp dir");
    let signing_key =
        EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &SystemRandom::new())
            .expect("P-256 signing key");
    write_session_config(&temp, signing_key.as_ref());
    let server = start(
        ServerBuilder::configured_standalone_grpc()
            .with_config_dir(temp.path())
            .with_noop_feedback_uploads(),
        shared_mcp_options(),
    )
    .await
    .expect("start authenticated composite server");
    assert!(server.grpc_authentication_enabled());
    let mcp_addr = server.mcp_http_addr().expect("MCP HTTP endpoint");
    let base = format!("http://{mcp_addr}");

    let ready = reqwest::get(format!("{base}/readyz"))
        .await
        .expect("readiness response");
    assert_eq!(ready.status(), reqwest::StatusCode::NO_CONTENT);
    let advertised = reqwest::get(format!("{base}/.well-known/oauth-protected-resource/mcp"))
        .await
        .expect("metadata response")
        .json::<serde_json::Value>()
        .await
        .expect("metadata document");
    let resource = advertised
        .get("resource")
        .and_then(serde_json::Value::as_str)
        .expect("advertised resource")
        .to_string();
    assert_eq!(resource, "https://coral.example/mcp");

    assert_unauthorized(&base, "Bearer wrong-token").await;

    let wrong_audience = session_token(signing_key.as_ref(), "https://other.example/mcp");
    assert_unauthorized(&base, &format!("Bearer {wrong_audience}")).await;

    assert_grpc_rejects_unauthenticated(server.endpoint_uri()).await;

    let (token, _) =
        prepare_authenticated_workspace(&temp, &server, signing_key.as_ref(), SHARED_WORKSPACE)
            .await;
    assert_authenticated_surfaces(&server, &format!("{base}/mcp"), &token).await;

    let reef_token = session_token(signing_key.as_ref(), REEF_RESOURCE);
    assert_grpc_authenticated(&server, &reef_token).await;
    assert_unauthorized(&base, &format!("Bearer {reef_token}")).await;

    // Readiness observes the backend, not just the port: stopping gRPC while MCP
    // HTTP keeps serving must turn the authenticated probe unhealthy.
    let RunningServer {
        grpc,
        oauth,
        mcp_http,
        grpc_authentication_enabled: _,
    } = server;
    grpc.shutdown().await.expect("shutdown gRPC server");
    let unready = reqwest::get(format!("{base}/readyz"))
        .await
        .expect("readiness response");
    assert_eq!(
        unready.status(),
        reqwest::StatusCode::SERVICE_UNAVAILABLE,
        "an unreachable engine must not report ready"
    );
    mcp_http
        .expect("MCP HTTP server")
        .shutdown()
        .await
        .expect("shutdown MCP HTTP server");
    oauth
        .expect("OAuth server")
        .shutdown()
        .await
        .expect("shutdown OAuth server");
}

#[tokio::test]
async fn established_authenticated_mcp_session_observes_membership_revocation() {
    let temp = TempDir::new().expect("temp dir");
    let signing_key =
        EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &SystemRandom::new())
            .expect("P-256 signing key");
    write_session_config(&temp, signing_key.as_ref());
    let server = start(
        ServerBuilder::configured_standalone_grpc()
            .with_config_dir(temp.path())
            .with_noop_feedback_uploads(),
        shared_mcp_options(),
    )
    .await
    .expect("start authenticated composite server");
    let (token, user_id) =
        prepare_authenticated_workspace(&temp, &server, signing_key.as_ref(), SHARED_WORKSPACE)
            .await;
    let successor = coral_app::test_session_tokens::persist_test_login_identity(
        temp.path(),
        "https://accounts.example",
        "successor",
        None,
        "unused-test-attribution",
    )
    .await
    .expect("persist successor identity");
    let app = connect_with_loopback_bearer(
        server.endpoint_uri(),
        BearerToken::new(&token).expect("bearer token"),
    )
    .await
    .expect("authenticated gRPC client");
    app.workspace_client()
        .add_workspace_member(Request::new(AddWorkspaceMemberRequest {
            workspace: Some(shared_workspace()),
            member: Some(WorkspaceMember {
                user_id: successor.user_id,
                role: WorkspaceRole::Owner as i32,
                display_name: String::new(),
            }),
        }))
        .await
        .expect("add successor owner");

    let endpoint = format!(
        "http://{}/mcp",
        server.mcp_http_addr().expect("MCP HTTP endpoint")
    );
    let client = authenticated_mcp_client(&endpoint, &token).await;
    let arguments = serde_json::Map::from_iter([(
        "intent".to_string(),
        serde_json::json!("verify membership revocation"),
    )]);
    let before = client
        .call_tool(CallToolRequestParams::new("start_task").with_arguments(arguments.clone()))
        .await
        .expect("call tool before revocation");
    assert_eq!(before.is_error, Some(false));

    app.workspace_client()
        .remove_workspace_member(Request::new(RemoveWorkspaceMemberRequest {
            workspace: Some(shared_workspace()),
            user_id,
        }))
        .await
        .expect("remove original owner");
    let after = client
        .call_tool(CallToolRequestParams::new("start_task").with_arguments(arguments))
        .await
        .expect("call tool after revocation");
    assert_eq!(after.is_error, Some(true));
    let after = after.structured_content.expect("structured tool error");
    let error = after.get("error").expect("structured error details");
    assert_eq!(
        error.get("reason").and_then(serde_json::Value::as_str),
        Some("WORKSPACE_NOT_FOUND")
    );
    assert_eq!(
        error.get("retryable").and_then(serde_json::Value::as_bool),
        Some(false)
    );

    client.cancel().await.expect("stop MCP client");
    server.shutdown().await.expect("shutdown composite server");
}

#[tokio::test]
async fn reef_only_audience_authenticates_private_grpc_without_mcp_http() {
    let temp = TempDir::new().expect("temp dir");
    let signing_key =
        EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &SystemRandom::new())
            .expect("P-256 signing key");
    write_reef_only_session_config(&temp, signing_key.as_ref());

    let server = start(
        ServerBuilder::configured_standalone_grpc()
            .with_config_dir(temp.path())
            .with_noop_feedback_uploads(),
        McpOptions::default(),
    )
    .await
    .expect("start Reef-only authenticated server");

    assert!(server.grpc_authentication_enabled());
    assert!(server.mcp_http_addr().is_none());
    assert!(server.oauth_addr().is_some());
    assert_grpc_rejects_unauthenticated(server.endpoint_uri()).await;

    let reef_token = session_token(signing_key.as_ref(), REEF_RESOURCE);
    assert_grpc_authenticated(&server, &reef_token).await;

    server.shutdown().await.expect("shutdown Reef-only server");
}

#[tokio::test]
async fn session_failures_and_restart_are_fail_closed() {
    let temp = TempDir::new().expect("temp dir");
    let signing_key =
        EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &SystemRandom::new())
            .expect("P-256 signing key");
    write_session_config(&temp, signing_key.as_ref());

    let encoding_key = EncodingKey::from_ec_der(signing_key.as_ref());
    let key_id = Jwk::from_encoding_key(&encoding_key, Algorithm::ES256)
        .expect("signing JWK")
        .thumbprint(ThumbprintHash::SHA256);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("current time")
        .as_secs();
    let expired = signed_session_token(
        &encoding_key,
        &key_id,
        SESSION_RESOURCE,
        now.saturating_sub(300),
        now.saturating_sub(120),
        "expired-token",
    );
    let wrong_audience = session_token(signing_key.as_ref(), "https://coral.example/not-mcp");
    let forged_key =
        EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &SystemRandom::new())
            .expect("forged P-256 signing key");
    let forged = signed_session_token(
        &EncodingKey::from_ec_der(forged_key.as_ref()),
        &key_id,
        SESSION_RESOURCE,
        now.saturating_sub(1),
        now + 300,
        "forged-token",
    );

    let server = start(
        ServerBuilder::configured_standalone_grpc()
            .with_config_dir(temp.path())
            .with_noop_feedback_uploads(),
        shared_mcp_options(),
    )
    .await
    .expect("start authenticated composite server");
    let mcp_endpoint = format!(
        "http://{}/mcp",
        server.mcp_http_addr().expect("MCP HTTP endpoint")
    );
    let (valid, _) =
        prepare_authenticated_workspace(&temp, &server, signing_key.as_ref(), SHARED_WORKSPACE)
            .await;

    let expired_rejection = assert_bearer_rejected(&server, &mcp_endpoint, &expired).await;
    let wrong_audience_rejection =
        assert_bearer_rejected(&server, &mcp_endpoint, &wrong_audience).await;
    let malformed_rejection =
        assert_bearer_rejected(&server, &mcp_endpoint, "not-a-session-token").await;
    let forged_rejection = assert_bearer_rejected(&server, &mcp_endpoint, &forged).await;
    assert_eq!(expired_rejection, wrong_audience_rejection);
    assert_eq!(expired_rejection, malformed_rejection);
    assert_eq!(expired_rejection, forged_rejection);
    assert_authenticated_surfaces(&server, &mcp_endpoint, &valid).await;
    server.shutdown().await.expect("first shutdown");

    let restarted = start(
        ServerBuilder::configured_standalone_grpc()
            .with_config_dir(temp.path())
            .with_noop_feedback_uploads(),
        shared_mcp_options(),
    )
    .await
    .expect("restart authenticated composite server");
    let restarted_mcp = format!(
        "http://{}/mcp",
        restarted.mcp_http_addr().expect("restarted MCP endpoint")
    );
    assert_authenticated_surfaces(&restarted, &restarted_mcp, &valid).await;
    restarted.shutdown().await.expect("restarted shutdown");
}

#[tokio::test]
async fn mcp_start_failure_releases_started_oauth_and_grpc_listeners() {
    let grpc_probe = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("reserve gRPC port");
    let oauth_probe = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("reserve OAuth port");
    let grpc_addr = grpc_probe.local_addr().expect("gRPC address");
    let oauth_addr = oauth_probe.local_addr().expect("OAuth address");
    drop((grpc_probe, oauth_probe));
    let occupied_mcp = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("occupy MCP port");
    let mcp_addr = occupied_mcp.local_addr().expect("MCP address");
    let temp = TempDir::new().expect("temp dir");
    write_oauth_config(&temp, oauth_addr, Some(mcp_addr));

    let result = start(
        ServerBuilder::standalone_grpc(grpc_addr)
            .with_config_dir(temp.path())
            .with_noop_feedback_uploads(),
        McpOptions::default(),
    )
    .await;
    let Err(error) = result else {
        panic!("occupied MCP address must fail startup");
    };
    assert!(error.to_string().contains("failed to bind MCP HTTP server"));
    let grpc_rebound = TcpListener::bind(grpc_addr).expect("gRPC listener must be released");
    let oauth_rebound = TcpListener::bind(oauth_addr).expect("OAuth listener must be released");
    drop((grpc_rebound, oauth_rebound, occupied_mcp));
}
