//! A refresh-capable client keeps working past its access token's expiry,
//! through the production runtime: Coral's authorization server, private
//! gRPC for Reef, and public MCP.
#![allow(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    reason = "fixture setup fails the journey if it cannot complete or a response lacks a field"
)]

use std::time::{Duration, Instant};

use coral_grpc::coral::management::v1::CreateWorkspaceRequest;
use coral_grpc::coral::management::v1::workspace_service_client::WorkspaceServiceClient;
use serde_json::{Value, json};
use tempfile::TempDir;
use wiremock::MockServer;

use super::oidc_login_tests::{
    AUTH_ISSUER, CALLBACK, claims, location, mount_provider, param, redeem, sign_in,
};

const WEB: &str = "https://reef.example.test";
const MCP_HOST: &str = "mcp.example.test";

/// Access tokens live one second, plus the verifier's clock-skew leeway.
const ACCESS_TOKEN_TTL_SECONDS: u64 = 1;

struct Journey {
    _temp: TempDir,
    runtime: coral_app::RunningRuntime,
    provider: MockServer,
    endpoint: String,
    http: reqwest::Client,
}

impl Journey {
    async fn start() -> Self {
        let provider = MockServer::start().await;
        mount_provider(&provider).await;
        let temp = TempDir::new().unwrap();
        std::fs::write(
            temp.path().join("session.key"),
            coral_test_support::p256_pkcs8(),
        )
        .unwrap();
        std::fs::write(
            temp.path().join("config.toml"),
            format!(
                "[outbound]\nloopback = 'http-and-https'\n\
                 [listeners.grpc]\nbind = '127.0.0.1:0'\n\
                 [listeners.mcp]\nbind = '127.0.0.1:0'\npublic_url = 'https://{MCP_HOST}'\n\
                 [listeners.auth]\npublic_url = '{AUTH_ISSUER}'\n\
                 [listeners.web]\npublic_url = '{WEB}'\n\
                 [auth.session]\nsigning_key_file = 'session.key'\n\
                 access_token_ttl_seconds = {ACCESS_TOKEN_TTL_SECONDS}\n\
                 [auth.provider]\nissuer = '{}'\nclient_id = 'upstream-client'\n\
                 client_secret = 'test-secret'\n",
                provider.uri()
            ),
        )
        .unwrap();
        let runtime = coral_app::start_runtime(coral_app::Launch {
            config_dir: Some(temp.path().to_owned()),
            ..coral_app::Launch::default()
        })
        .await
        .expect("start the runtime");
        let endpoint = format!(
            "http://{}",
            runtime.auth_http_addr().expect("auth listener")
        );
        Self {
            _temp: temp,
            runtime,
            provider,
            endpoint,
            http: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .unwrap(),
        }
    }

    /// A client declaring the refresh grant signs in for `resource` and
    /// redeems its code, narrowed to `audience` when given.
    async fn sign_in(&self, resource: &str, audience: Option<&str>) -> (String, Value) {
        let registration = json!({
            "redirect_uris": [CALLBACK],
            "grant_types": ["authorization_code", "refresh_token"],
        });
        let (client_id, returned) = sign_in(
            &self.endpoint,
            &self.http,
            &self.provider,
            resource,
            &registration,
        )
        .await;
        let code = param(&location(&returned), "code");
        let tokens = redeem(
            &self.endpoint,
            &self.http,
            &client_id,
            &code,
            resource,
            audience,
        )
        .await;
        assert!(tokens["refresh_token"].is_string(), "{tokens}");
        (client_id, tokens)
    }

    /// Refreshes with only the token and client, as an MCP client does.
    async fn refresh(&self, client_id: &str, tokens: &Value) -> Value {
        let response = self
            .http
            .post(format!("{}/oauth/token", self.endpoint))
            .form(&[
                ("grant_type", "refresh_token"),
                ("refresh_token", tokens["refresh_token"].as_str().unwrap()),
                ("client_id", client_id),
            ])
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        let refreshed: Value = response.json().await.unwrap();
        assert_ne!(refreshed["refresh_token"], tokens["refresh_token"]);
        refreshed
    }

    async fn shutdown(self) {
        self.runtime.shutdown().await.unwrap();
    }
}

/// A raw MCP session over the legacy handshake, which needs no per-request
/// protocol headers.
struct Mcp {
    http: reqwest::Client,
    endpoint: String,
    session: Option<String>,
}

impl Mcp {
    fn new(runtime: &coral_app::RunningRuntime, workspace: &str) -> Self {
        Self {
            http: reqwest::Client::new(),
            endpoint: format!(
                "http://{}/workspaces/{workspace}/mcp",
                runtime.mcp_http_addr()
            ),
            session: None,
        }
    }

    async fn post(&self, bearer: &str, message: &Value) -> reqwest::Response {
        let mut request = self
            .http
            .post(&self.endpoint)
            .header("host", MCP_HOST)
            .header("accept", "application/json, text/event-stream")
            .header("mcp-protocol-version", "2025-03-26")
            .bearer_auth(bearer)
            .json(message);
        if let Some(session) = &self.session {
            request = request.header("mcp-session-id", session);
        }
        request.send().await.unwrap()
    }

    async fn initialize(&mut self, bearer: &str) {
        let response = self
            .post(
                bearer,
                &json!({"jsonrpc":"2.0", "id":1, "method":"initialize", "params":{
                    "protocolVersion":"2025-03-26", "capabilities":{},
                    "clientInfo":{"name":"refresh-journey", "version":"1"}}}),
            )
            .await;
        assert_eq!(response.status(), 200);
        self.session = response
            .headers()
            .get("mcp-session-id")
            .map(|value| value.to_str().unwrap().to_owned());
        let _initialized = message(response).await;
        let response = self
            .post(
                bearer,
                &json!({"jsonrpc":"2.0", "method":"notifications/initialized"}),
            )
            .await;
        assert!(response.status().is_success());
    }

    /// Sends one request and returns its JSON-RPC `result`.
    async fn result(&self, bearer: &str, method: &str, params: Value) -> Value {
        let response = self
            .post(
                bearer,
                &json!({"jsonrpc":"2.0", "id":2, "method":method, "params":params}),
            )
            .await;
        assert_eq!(response.status(), 200, "{method}");
        let message = message(response).await;
        message
            .get("result")
            .cloned()
            .unwrap_or_else(|| panic!("{method}: {message}"))
    }

    async fn tool(&self, bearer: &str, name: &str, arguments: Value) -> Value {
        let result = self
            .result(
                bearer,
                "tools/call",
                json!({"name": name, "arguments": arguments}),
            )
            .await;
        assert_ne!(result["isError"], true, "{name}: {result}");
        result["structuredContent"].clone()
    }

    /// Discovers tools, runs one Program that starts a Task, and observes it.
    async fn work(&self, bearer: &str, intent: &str) -> Value {
        let tools = self.result(bearer, "tools/list", json!({})).await;
        assert!(
            tools["tools"]
                .as_array()
                .unwrap()
                .iter()
                .any(|tool| tool["name"] == "exec"),
            "{tools}"
        );
        let mut update = self
            .tool(
                bearer,
                "exec",
                json!({"goal": intent, "intent": intent,
                       "wait_ms": 5000, "program": "return 2 + 3;"}),
            )
            .await;
        // A Run that never ends fails here, not later on an expired token.
        let deadline = Instant::now() + Duration::from_secs(30);
        while update["state"] == "running" {
            assert!(Instant::now() < deadline, "the Run never ended: {update}");
            update = self
                .tool(
                    bearer,
                    "wait",
                    json!({"run_id": update["run_id"], "cursor": update["next_cursor"],
                           "wait_ms": 5000}),
                )
                .await;
        }
        update
    }
}

/// A JSON-RPC message from a JSON body or the first SSE data line.
async fn message(response: reqwest::Response) -> Value {
    let text = response.text().await.unwrap();
    serde_json::from_str(&text).unwrap_or_else(|_error| {
        let data = text
            .lines()
            .filter_map(|line| line.strip_prefix("data: "))
            .find(|data| !data.is_empty())
            .unwrap_or_else(|| panic!("no JSON-RPC message in {text:?}"));
        serde_json::from_str(data).unwrap()
    })
}

/// One person signs in once per client. Reef's grant refreshes and creates a
/// Workspace; an MCP client's access token then expires for real, and
/// refresh carries it on in the same MCP session as the same User in the
/// same Workspace.
#[tokio::test(flavor = "multi_thread")]
async fn refresh_carries_reef_and_mcp_clients_past_access_token_expiry() {
    let journey = Journey::start().await;

    let (reef_client, reef) = journey.sign_in(WEB, Some("reef")).await;
    let reef = journey.refresh(&reef_client, &reef).await;
    let reef_claims = claims(reef["access_token"].as_str().unwrap());
    assert_eq!(reef_claims["aud"], "reef");
    let user = reef_claims["sub"].clone();
    let mut workspaces = WorkspaceServiceClient::connect(journey.runtime.endpoint_uri().to_owned())
        .await
        .unwrap();
    let mut request = tonic::Request::new(CreateWorkspaceRequest {
        display_name: "Refresh".into(),
        request_id: uuid::Uuid::new_v4().to_string(),
    });
    let metadata = request.metadata_mut();
    metadata.insert("coral-audience", "reef".parse().unwrap());
    metadata.insert(
        "authorization",
        format!("Bearer {}", reef["access_token"].as_str().unwrap())
            .parse()
            .unwrap(),
    );
    let workspace = workspaces
        .create_workspace(request)
        .await
        .expect("a refreshed Reef token manages Workspaces")
        .into_inner()
        .name;
    let workspace = workspace.strip_prefix("workspaces/").unwrap().to_owned();

    let resource = format!("https://{MCP_HOST}/workspaces/{workspace}/mcp");
    let (client, tokens) = journey.sign_in(&resource, None).await;
    let first = tokens["access_token"].as_str().unwrap().to_owned();
    let mut mcp = Mcp::new(&journey.runtime, &workspace);
    mcp.initialize(&first).await;
    let done = mcp.work(&first, "Before expiry").await;
    assert_eq!(done["state"], "succeeded", "{done}");

    // Real expiry: the verifier allows a minute of clock skew.
    let deadline = Instant::now() + Duration::from_secs(ACCESS_TOKEN_TTL_SECONDS + 90);
    loop {
        let response = mcp
            .post(
                &first,
                &json!({"jsonrpc":"2.0", "id":3, "method":"tools/list", "params":{}}),
            )
            .await;
        if response.status() == 401 {
            break;
        }
        assert!(Instant::now() < deadline, "the access token never expired");
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    let refreshed = journey.refresh(&client, &tokens).await;
    let second = refreshed["access_token"].as_str().unwrap();
    let claims = claims(second);
    assert_eq!(claims["sub"], user, "the same User");
    assert_eq!(claims["aud"], resource.as_str());
    let after = mcp.work(second, "After refresh").await;
    assert_eq!(after["state"], "succeeded", "{after}");
    let observed = mcp
        .tool(
            second,
            "wait",
            json!({"run_id": done["run_id"], "wait_ms": 0}),
        )
        .await;
    assert_eq!(
        observed["state"], "succeeded",
        "the earlier Run stays observable"
    );
    journey.shutdown().await;
}
