#![allow(
    clippy::indexing_slicing,
    reason = "test code: assertion-style indexing and fixture buffer slicing are intentional"
)]

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as BASE64_URL_SAFE_NO_PAD;
use coral_api::v1::{SourceSecret, SourceVariable};
use serde_json::json;
use tempfile::TempDir;
use tokio::sync::Notify;
use tonic::{Code, Status};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request as WiremockRequest, Respond, ResponseTemplate};

use crate::harness::{
    GrpcHarness, execute_sql_rows_with_client, import_source_with_client, source_dir,
    source_secret, source_variable,
};

const REFRESHED_MESSAGES_QUERY: &str = "SELECT id FROM refreshed_messages.messages";
const REFRESHED_MESSAGES_SOURCE: &str = "refreshed_messages";

fn assert_single_ok_row(rows: &[serde_json::Value]) {
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["id"], "ok");
}

async fn assert_message_authorizations(fixture: &RefreshingHttpFixture, expected: &[&str]) {
    assert_eq!(fixture.message_authorizations().await, expected);
}

async fn assert_no_token_forms(fixture: &RefreshingHttpFixture, reason: &str) {
    assert!(fixture.token_forms().await.is_empty(), "{reason}");
}

async fn assert_single_refresh_form(fixture: &RefreshingHttpFixture, refresh_token: &str) {
    let forms = fixture.token_forms().await;
    assert_eq!(forms.len(), 1);
    let form = forms.into_iter().next().expect("single token form");
    for (key, expected) in [
        ("grant_type", "refresh_token"),
        ("refresh_token", refresh_token),
        ("client_id", "stored-client"),
    ] {
        assert_eq!(form.get(key).map(String::as_str), Some(expected), "{key}");
    }
}

fn secret_material(path: impl AsRef<Path>, expectation: &str) -> String {
    fs::read_to_string(path).expect(expectation)
}

fn assert_material_contains(material: &str, expected: &str) {
    assert!(material.contains(expected), "{material}");
}

fn assert_failed_precondition_contains(status: &Status, expected: &str) {
    assert_eq!(status.code(), Code::FailedPrecondition);
    assert!(status.message().contains(expected), "{status:?}");
}

struct RefreshScenario {
    fixture: RefreshingHttpFixture,
    harness: GrpcHarness,
    secret_path: PathBuf,
}

impl RefreshScenario {
    async fn with_default_source() -> Self {
        Self::expired(
            |fixture| fixture.token_url.clone(),
            Some("stored-refresh-token"),
        )
        .await
    }

    async fn expired(
        token_url: impl FnOnce(&RefreshingHttpFixture) -> String,
        refresh_token: Option<&str>,
    ) -> Self {
        let fixture = RefreshingHttpFixture::new().await;
        let harness = GrpcHarness::new().await;
        let token_url = token_url(&fixture);
        let secret_path =
            import_expired_oauth_refresh_source(&harness, &fixture, &token_url, refresh_token)
                .await;
        Self {
            fixture,
            harness,
            secret_path,
        }
    }

    async fn execute_messages(&self) -> Vec<serde_json::Value> {
        self.harness
            .execute_sql_rows(REFRESHED_MESSAGES_QUERY)
            .await
    }

    async fn execute_messages_error(&self) -> Status {
        self.harness
            .execute_sql_error(REFRESHED_MESSAGES_QUERY)
            .await
    }

    fn secret_material(&self, expectation: &str) -> String {
        secret_material(&self.secret_path, expectation)
    }
}

#[tokio::test]
async fn query_refreshes_expired_oauth_access_token_at_request_time() {
    let scenario = RefreshScenario::with_default_source().await;

    let rows = scenario.execute_messages().await;

    assert_single_ok_row(&rows);
    assert_message_authorizations(&scenario.fixture, &["Bearer refreshed-token"]).await;
    assert_single_refresh_form(&scenario.fixture, "stored-refresh-token").await;

    let material = scenario.secret_material("read refreshed material");
    assert_material_contains(&material, "API_TOKEN=refreshed-token");
    assert_material_contains(
        &material,
        "__coral_oauth.QVBJX1RPS0VO.refresh_token=rotated-refresh-token",
    );
}

#[tokio::test]
async fn query_against_other_source_does_not_refresh_expired_oauth_source() {
    let scenario = RefreshScenario::with_default_source().await;
    scenario.harness.import_local_messages_source().await;

    let rows = scenario
        .harness
        .execute_sql_rows("SELECT text FROM local_messages.messages ORDER BY text")
        .await;

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["text"], "hello");
    assert_eq!(rows[1]["text"], "world");
    assert_no_token_forms(
        &scenario.fixture,
        "unrelated OAuth source should not refresh during another source's query",
    )
    .await;
    assert_message_authorizations(&scenario.fixture, &[]).await;
}

#[tokio::test]
async fn list_catalog_does_not_refresh_expired_oauth_access_token() {
    let scenario = RefreshScenario::with_default_source().await;

    let tables = scenario.harness.list_tables().await;
    let refreshed_tables = tables
        .iter()
        .filter(|table| table.schema_name == REFRESHED_MESSAGES_SOURCE)
        .collect::<Vec<_>>();

    assert_eq!(refreshed_tables.len(), 1);
    assert_eq!(refreshed_tables[0].name, "messages");
    assert_no_token_forms(
        &scenario.fixture,
        "passive catalog discovery should not call the token endpoint",
    )
    .await;
    let material = scenario.secret_material("read material");
    assert_material_contains(&material, "API_TOKEN=expired-token");
}

#[tokio::test]
async fn query_surfaces_oauth_refresh_failure_instead_of_skipping_source() {
    let scenario = RefreshScenario::expired(
        |fixture| format!("{}/token-fail", fixture.base_url),
        Some("stored-refresh-token"),
    )
    .await;

    let status = scenario.execute_messages_error().await;

    assert_failed_precondition_contains(&status, "OAuth token refresh failed with HTTP 500");
}

#[tokio::test]
async fn expired_oauth_access_token_without_refresh_token_tells_user_to_reconnect() {
    let scenario = RefreshScenario::expired(|fixture| fixture.token_url.clone(), None).await;

    let status = scenario.execute_messages_error().await;

    assert_failed_precondition_contains(&status, "reconnect the source");
    assert_no_token_forms(
        &scenario.fixture,
        "missing refresh token should fail before contacting token endpoint",
    )
    .await;
}

#[tokio::test]
async fn concurrent_queries_share_one_expired_oauth_refresh() {
    let scenario = RefreshScenario::with_default_source().await;

    let (first, second) = tokio::join!(scenario.execute_messages(), scenario.execute_messages(),);

    assert_single_ok_row(&first);
    assert_single_ok_row(&second);
    assert_single_refresh_form(&scenario.fixture, "stored-refresh-token").await;
    assert_message_authorizations(
        &scenario.fixture,
        &["Bearer refreshed-token", "Bearer refreshed-token"],
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_servers_share_one_expired_oauth_refresh() {
    let fixture = RefreshingHttpFixture::new().await;
    let config_root = TempDir::new().expect("config root");
    let config_dir = config_root.path().join("coral-config");
    let first_harness = GrpcHarness::start_with_config_dir(config_dir.clone()).await;
    import_default_refreshable_source(&first_harness, &fixture).await;

    let second_harness = GrpcHarness::start_with_config_dir(config_dir).await;
    let (first, second) = tokio::join!(
        first_harness.execute_sql_rows(REFRESHED_MESSAGES_QUERY),
        second_harness.execute_sql_rows(REFRESHED_MESSAGES_QUERY),
    );

    assert_single_ok_row(&first);
    assert_single_ok_row(&second);
    assert_single_refresh_form(&fixture, "stored-refresh-token").await;
    assert_message_authorizations(
        &fixture,
        &["Bearer refreshed-token", "Bearer refreshed-token"],
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn manual_credential_replacement_waits_for_in_flight_refresh() {
    let fixture = RefreshingHttpFixture::new_blocked_token_response().await;
    let harness = GrpcHarness::new().await;
    let secret_path = import_default_refreshable_source(&harness, &fixture).await;

    let query = tokio::spawn(execute_sql_rows_with_client(
        harness.query_client(),
        REFRESHED_MESSAGES_QUERY.to_string(),
    ));

    fixture.wait_for_token_request().await;
    let (import_manifest_yaml, import_variables, import_secrets) =
        oauth_refresh_import_args(&fixture, "manual-token");
    let import = tokio::spawn(import_source_with_client(
        harness.source_client(),
        import_manifest_yaml,
        import_variables,
        import_secrets,
    ));
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !import.is_finished(),
        "manual credential replacement should wait for the in-flight refresh"
    );
    fixture.allow_token_response();

    let rows = query.await.expect("query task");
    import.await.expect("import task");

    assert_single_ok_row(&rows);
    assert_message_authorizations(&fixture, &["Bearer refreshed-token"]).await;
    let material = secret_material(secret_path, "read material");
    assert_material_contains(&material, "API_TOKEN=manual-token");
    assert!(
        !material.contains("API_TOKEN=refreshed-token"),
        "stale refresh should not overwrite manual replacement: {material}"
    );
}

#[tokio::test]
async fn successful_refresh_is_persisted_before_later_oauth_input_failure() {
    let fixture = RefreshingHttpFixture::new().await;
    let harness = GrpcHarness::new().await;
    harness
        .import_source(
            two_oauth_inputs_manifest_yaml(&fixture.base_url, &fixture.token_url),
            vec![source_variable("API_BASE", &fixture.base_url)],
            vec![
                source_secret("API_TOKEN", "expired-primary-token"),
                source_secret("SECOND_TOKEN", "expired-secondary-token"),
            ],
        )
        .await;

    let secret_path = source_dir(harness.config_dir(), "multi_oauth_messages").join("secrets.env");
    let primary_prefix = oauth_metadata_prefix("API_TOKEN");
    fs::write(
        &secret_path,
        format!(
            "{}{}",
            expired_oauth_material(
                "API_TOKEN",
                "expired-primary-token",
                &fixture.token_url,
                Some("stored-primary-refresh-token"),
            ),
            expired_oauth_material(
                "SECOND_TOKEN",
                "expired-secondary-token",
                &format!("{}/token-fail", fixture.base_url),
                Some("stored-secondary-refresh-token"),
            ),
        ),
    )
    .expect("seed expired oauth material");

    let status = harness
        .execute_sql_error("SELECT id FROM multi_oauth_messages.messages")
        .await;

    assert_failed_precondition_contains(&status, "OAuth token refresh failed");

    let forms = fixture.token_forms().await;
    assert!(
        forms.iter().any(|form| {
            form.get("refresh_token").map(String::as_str) == Some("stored-secondary-refresh-token")
        }),
        "second failing refresh should be attempted: {forms:?}"
    );

    let material = secret_material(secret_path, "read partially refreshed material");
    assert_material_contains(&material, "API_TOKEN=refreshed-token");
    assert_material_contains(
        &material,
        &format!("{primary_prefix}refresh_token=rotated-refresh-token"),
    );
    assert_material_contains(&material, "SECOND_TOKEN=expired-secondary-token");
}

async fn import_expired_oauth_refresh_source(
    harness: &GrpcHarness,
    fixture: &RefreshingHttpFixture,
    token_url: &str,
    refresh_token: Option<&str>,
) -> PathBuf {
    import_oauth_refresh_source(harness, fixture, "expired-token").await;
    let secret_path = refreshed_messages_secret_path(harness.config_dir());
    seed_expired_api_token_material(&secret_path, token_url, refresh_token);
    secret_path
}

async fn import_default_refreshable_source(
    harness: &GrpcHarness,
    fixture: &RefreshingHttpFixture,
) -> PathBuf {
    import_expired_oauth_refresh_source(
        harness,
        fixture,
        &fixture.token_url,
        Some("stored-refresh-token"),
    )
    .await
}

async fn import_oauth_refresh_source(
    harness: &GrpcHarness,
    fixture: &RefreshingHttpFixture,
    token: &str,
) {
    let (manifest_yaml, variables, secrets) = oauth_refresh_import_args(fixture, token);
    harness
        .import_source(manifest_yaml, variables, secrets)
        .await;
}

fn oauth_refresh_import_args(
    fixture: &RefreshingHttpFixture,
    token: &str,
) -> (String, Vec<SourceVariable>, Vec<SourceSecret>) {
    (
        oauth_refresh_manifest_yaml(&fixture.base_url, &fixture.token_url),
        vec![source_variable("API_BASE", &fixture.base_url)],
        vec![source_secret("API_TOKEN", token)],
    )
}

fn refreshed_messages_secret_path(config_dir: &Path) -> PathBuf {
    source_dir(config_dir, REFRESHED_MESSAGES_SOURCE).join("secrets.env")
}

fn seed_expired_api_token_material(
    secret_path: &Path,
    token_url: &str,
    refresh_token: Option<&str>,
) {
    fs::write(
        secret_path,
        expired_oauth_material("API_TOKEN", "expired-token", token_url, refresh_token),
    )
    .expect("seed expired oauth material");
}

fn expired_oauth_material(
    input_key: &str,
    access_token: &str,
    token_url: &str,
    refresh_token: Option<&str>,
) -> String {
    let prefix = oauth_metadata_prefix(input_key);
    let refresh_token_line = refresh_token
        .map(|refresh_token| format!("{prefix}refresh_token={refresh_token}\n"))
        .unwrap_or_default();
    format!(
        "\
{input_key}={access_token}
{prefix}method=oauth
{prefix}access_token_expires_at={}
{refresh_token_line}{prefix}client_id=stored-client
{prefix}token_url={token_url}
",
        (chrono::Utc::now() - chrono::Duration::minutes(5)).to_rfc3339(),
    )
}

fn oauth_refresh_manifest_yaml(base_url: &str, token_url: &str) -> String {
    oauth_messages_manifest_yaml(
        REFRESHED_MESSAGES_SOURCE,
        base_url,
        &json!({
            "API_BASE": { "kind": "variable" },
            "API_TOKEN": oauth_input(token_url),
        }),
        "Messages behind an OAuth access token",
        None,
    )
}

fn two_oauth_inputs_manifest_yaml(base_url: &str, token_url: &str) -> String {
    oauth_messages_manifest_yaml(
        "multi_oauth_messages",
        base_url,
        &json!({
            "API_BASE": { "kind": "variable" },
            "API_TOKEN": oauth_input(token_url),
            "SECOND_TOKEN": oauth_input(&format!("{base_url}/token-fail")),
        }),
        "Messages behind multiple OAuth inputs",
        Some(&json!(["SELECT id FROM multi_oauth_messages.messages"])),
    )
}

fn oauth_messages_manifest_yaml(
    name: &str,
    base_url: &str,
    inputs: &serde_json::Value,
    description: &str,
    test_queries: Option<&serde_json::Value>,
) -> String {
    let mut manifest = json!({
        "name": name,
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "http",
        "inputs": inputs,
        "base_url": base_url,
        "auth": {
            "type": "HeaderAuth",
            "headers": [{
                "name": "Authorization",
                "from": "template",
                "template": "Bearer {{input.API_TOKEN}}",
            }],
        },
        "tables": [{
            "name": "messages",
            "description": description,
            "request": {
                "method": "GET",
                "path": "/messages",
            },
            "response": {
                "rows_path": ["data"],
            },
            "columns": [
                {"name": "id", "type": "Utf8"},
            ],
        }],
    });
    if let Some(test_queries) = test_queries {
        manifest["test_queries"] = test_queries.clone();
    }
    serde_yaml::to_string(&manifest).expect("serialize manifest")
}

fn oauth_input(token_url: &str) -> serde_json::Value {
    json!({
        "kind": "secret",
        "credential": {
            "methods": [{
                "type": "oauth",
                "oauth": {
                    "flow": {
                        "type": "authorization_code",
                        "pkce": "disabled",
                    },
                    "redirect_uri": "http://127.0.0.1:53682/oauth/callback",
                    "redirect_uri_port_mode": "fixed",
                    "endpoints": {
                        "authorization_url": "https://provider.example.test/oauth/authorize",
                        "token_url": token_url,
                    },
                    "client": {
                        "id": { "default": "manifest-client" },
                    },
                },
            }],
        },
    })
}

fn oauth_metadata_prefix(input_key: &str) -> String {
    format!(
        "__coral_oauth.{}.",
        BASE64_URL_SAFE_NO_PAD.encode(input_key.as_bytes())
    )
}

struct RefreshingHttpFixture {
    base_url: String,
    token_url: String,
    server: MockServer,
    token_request_seen: Arc<Notify>,
    token_response_gate: Option<Arc<TokenResponseGate>>,
}

impl RefreshingHttpFixture {
    async fn new() -> Self {
        Self::new_with_token_response_gate(None).await
    }

    async fn new_blocked_token_response() -> Self {
        Self::new_with_token_response_gate(Some(Arc::new(TokenResponseGate::default()))).await
    }

    async fn new_with_token_response_gate(
        token_response_gate: Option<Arc<TokenResponseGate>>,
    ) -> Self {
        let server = MockServer::start().await;
        let base_url = server.uri();
        let token_url = format!("{base_url}/token");
        let token_request_seen = Arc::new(Notify::new());

        Mock::given(method("POST"))
            .and(path("/token"))
            .respond_with(TokenResponder {
                token_request_seen: Arc::clone(&token_request_seen),
                token_response_gate: token_response_gate.clone(),
            })
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/token-fail"))
            .respond_with(
                ResponseTemplate::new(500).set_body_json(json!({ "error": "refresh failed" })),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/messages"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": [{ "id": "ok" }]
            })))
            .mount(&server)
            .await;

        Self {
            base_url,
            token_url,
            server,
            token_request_seen,
            token_response_gate,
        }
    }

    async fn wait_for_token_request(&self) {
        self.token_request_seen.notified().await;
    }

    fn allow_token_response(&self) {
        if let Some(gate) = &self.token_response_gate {
            gate.release();
        }
    }

    async fn token_forms(&self) -> Vec<BTreeMap<String, String>> {
        self.requests()
            .await
            .into_iter()
            .filter(|request| matches!(request.url.path(), "/token" | "/token-fail"))
            .map(|request| {
                url::form_urlencoded::parse(&request.body)
                    .into_owned()
                    .collect()
            })
            .collect()
    }

    async fn message_authorizations(&self) -> Vec<String> {
        self.requests()
            .await
            .into_iter()
            .filter(|request| request.url.path() == "/messages")
            .filter_map(|request| {
                request
                    .headers
                    .get("authorization")
                    .and_then(|value| value.to_str().ok())
                    .map(ToString::to_string)
            })
            .collect()
    }

    async fn requests(&self) -> Vec<WiremockRequest> {
        self.server
            .received_requests()
            .await
            .expect("request recording should be enabled")
    }
}

#[derive(Debug)]
struct TokenResponder {
    token_request_seen: Arc<Notify>,
    token_response_gate: Option<Arc<TokenResponseGate>>,
}

impl Respond for TokenResponder {
    fn respond(&self, _request: &WiremockRequest) -> ResponseTemplate {
        self.token_request_seen.notify_one();
        if let Some(gate) = &self.token_response_gate {
            gate.wait();
        }
        ResponseTemplate::new(200).set_body_json(json!({
            "access_token": "refreshed-token",
            "refresh_token": "rotated-refresh-token",
            "token_type": "Bearer",
            "expires_in": 3600
        }))
    }
}

#[derive(Debug, Default)]
struct TokenResponseGate {
    released: Mutex<bool>,
    cvar: Condvar,
}

impl TokenResponseGate {
    fn wait(&self) {
        let mut released = self.released.lock().expect("token response gate");
        while !*released {
            released = self.cvar.wait(released).expect("token response gate wait");
        }
    }

    fn release(&self) {
        *self.released.lock().expect("token response gate") = true;
        self.cvar.notify_one();
    }
}
