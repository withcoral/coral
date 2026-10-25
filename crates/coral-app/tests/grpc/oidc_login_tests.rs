//! The composed server provisions a User through the upstream OIDC callback.
#![allow(
    clippy::unwrap_used,
    reason = "fixture setup fails the sign-in test if it cannot complete"
)]

use base64::Engine as _;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use coral_app::ServerBuilder;
use coral_workspace::{OrganizationModule, OrganizationRole, UserId};
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode, get_current_timestamp};
use reqwest::header::{CONTENT_TYPE, COOKIE, LOCATION, ORIGIN, SET_COOKIE};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use tempfile::TempDir;
use url::Url;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const AUTH_ISSUER: &str = "https://auth.example.test";
const CALLBACK: &str = "https://client.example.test/callback";
const RESOURCE: &str =
    "https://mcp.example.test/workspaces/01234567-89ab-4cde-8fab-0123456789ab/mcp";
const ORGANIZATION_RESOURCE: &str = "https://mcp.example.test/mcp";
const VERIFIER: &str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQ";

fn location(response: &reqwest::Response) -> Url {
    let location = response
        .headers()
        .get(LOCATION)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_else(|| panic!("redirect expected, got {}", response.status()));
    Url::parse(location).expect("absolute redirect")
}

fn param(url: &Url, name: &str) -> String {
    url.query_pairs()
        .find_map(|(key, value)| (key == name).then(|| value.into_owned()))
        .unwrap_or_else(|| panic!("{url} has no {name}"))
}

/// Mounts discovery and keys for an upstream provider served by `provider`.
async fn mount_provider(provider: &MockServer) {
    let issuer = provider.uri();
    Mock::given(method("GET"))
        .and(path("/.well-known/openid-configuration"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "issuer": issuer,
            "authorization_endpoint": format!("{issuer}/authorize"),
            "token_endpoint": format!("{issuer}/token"),
            "jwks_uri": format!("{issuer}/jwks"),
            "id_token_signing_alg_values_supported": ["RS256"],
        })))
        .mount(provider)
        .await;
    Mock::given(method("GET"))
        .and(path("/jwks"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"keys": [{
            "kty": "RSA", "n": coral_test_support::RSA_TEST_PUBLIC_MODULUS, "e": "AQAB",
            "kid": "key-1", "alg": "RS256", "use": "sig",
        }]})))
        .mount(provider)
        .await;
}

/// Answers the code exchange with an ID token for `subject` bound to `nonce`.
async fn mount_token(provider: &MockServer, subject: &str, nonce: &str) {
    let now = get_current_timestamp();
    let mut header = Header::new(Algorithm::RS256);
    header.kid = Some("key-1".into());
    let key = STANDARD
        .decode(coral_test_support::RSA_TEST_PRIVATE_KEY_DER)
        .expect("test key");
    let id_token = encode(
        &header,
        &json!({
            "iss": provider.uri(), "aud": "upstream-client", "sub": subject,
            "iat": now - 1, "exp": now + 300, "nonce": nonce,
        }),
        &EncodingKey::from_rsa_der(&key),
    )
    .expect("ID token");
    Mock::given(method("POST"))
        .and(path("/token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"id_token": id_token})))
        .mount(provider)
        .await;
}

/// Unverified claims of a token this test just received from the server.
fn claims(token: &str) -> Value {
    let payload = token.split('.').nth(1).expect("JWT payload");
    serde_json::from_slice(&URL_SAFE_NO_PAD.decode(payload).expect("base64url")).expect("JSON")
}

/// A composed server with its authorization server running, and a client
/// that never follows redirects.
struct Composed {
    _temp: TempDir,
    server: coral_app::RunningServer,
    auth: coral_app::RunningCoralAuthorizationServer,
    endpoint: String,
    http: reqwest::Client,
    provider: MockServer,
}

impl Composed {
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
                 [listeners.mcp]\nbind = '127.0.0.1:0'\npublic_url = 'https://mcp.example.test'\n\
                 [listeners.auth]\npublic_url = '{AUTH_ISSUER}'\n\
                 [listeners.web]\npublic_url = 'http://127.0.0.1:5173'\n\
                 [auth.session]\nsigning_key_file = 'session.key'\n\
                 [auth.provider]\nissuer = '{}'\nclient_id = 'upstream-client'\n\
                 client_secret = 'test-secret'\n",
                provider.uri()
            ),
        )
        .unwrap();
        let mut server = ServerBuilder::new()
            .with_config_dir(temp.path())
            .start()
            .await
            .expect("start authenticated server");
        let auth = server
            .take_authorization_server()
            .expect("composed authorization server")
            .start()
            .await
            .expect("start authorization server");
        let endpoint = auth.endpoint_uri().to_owned();
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap();
        Self {
            _temp: temp,
            server,
            auth,
            endpoint,
            http,
            provider,
        }
    }

    /// Registers a client and walks one person through consent, where it
    /// comes before sign-in, and the upstream sign-in for `resource`. Returns the client ID and the
    /// response to the provider's callback.
    async fn sign_in(&self, resource: &str) -> (String, reqwest::Response) {
        let (endpoint, http) = (&self.endpoint, &self.http);
        let registered: Value = http
            .post(format!("{endpoint}/oauth/register"))
            .json(&json!({"redirect_uris": [CALLBACK]}))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let client_id = registered
            .get("client_id")
            .and_then(Value::as_str)
            .expect("client ID")
            .to_owned();
        let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(VERIFIER.as_bytes()));
        let page = http
            .get(format!("{endpoint}/oauth/authorize"))
            .query(&[
                ("client_id", client_id.as_str()),
                ("redirect_uri", CALLBACK),
                ("response_type", "code"),
                ("code_challenge_method", "S256"),
                ("code_challenge", &challenge),
                ("resource", resource),
                ("state", "client-state"),
            ])
            .send()
            .await
            .unwrap();
        // At `/mcp` the person approves after sign-in, where they pick the
        // Workspace, so the request goes straight to the provider.
        let approved = if resource == ORGANIZATION_RESOURCE {
            assert_eq!(page.status(), 302, "no approval page before sign-in");
            page
        } else {
            assert_eq!(page.status(), 200);
            let cookie = cookie_pair(&page, "");
            let ticket = ticket(&page.text().await.unwrap());
            http.post(format!("{endpoint}/oauth/authorize"))
                .header(ORIGIN, AUTH_ISSUER)
                .header(COOKIE, cookie)
                .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body(format!("ticket={ticket}&decision=continue"))
                .send()
                .await
                .unwrap()
        };
        let upstream = location(&approved);
        let sign_in_cookie = cookie_pair(&approved, "");
        mount_token(
            &self.provider,
            "upstream-subject",
            &param(&upstream, "nonce"),
        )
        .await;

        let returned = http
            .get(format!("{endpoint}/auth/oidc/callback"))
            .header(COOKIE, sign_in_cookie)
            .query(&[
                ("state", param(&upstream, "state").as_str()),
                ("code", "provider-code"),
            ])
            .send()
            .await
            .unwrap();
        (client_id, returned)
    }

    /// Redeems `code` for `resource` and returns the access token's claims.
    async fn token_claims(&self, client_id: &str, code: &str, resource: &str) -> Value {
        let token: Value = self
            .http
            .post(format!("{}/oauth/token", self.endpoint))
            .form(&[
                ("grant_type", "authorization_code"),
                ("code", code),
                ("client_id", client_id),
                ("redirect_uri", CALLBACK),
                ("code_verifier", VERIFIER),
                ("resource", resource),
            ])
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        claims(
            token
                .get("access_token")
                .and_then(Value::as_str)
                .expect("access token"),
        )
    }

    async fn shutdown(self) {
        self.auth.shutdown().await.unwrap();
        self.server.shutdown().await.unwrap();
    }
}

/// The first `name=value` pair a response sets whose name starts with `prefix`.
fn cookie_pair(response: &reqwest::Response, prefix: &str) -> String {
    response
        .headers()
        .get_all(SET_COOKIE)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .filter_map(|value| value.split(';').next())
        .find(|pair| pair.starts_with(prefix))
        .expect("cookie")
        .to_owned()
}

fn ticket(html: &str) -> String {
    html.split("name=\"ticket\" value=\"")
        .nth(1)
        .and_then(|tail| tail.split('"').next())
        .expect("ticket")
        .to_owned()
}

#[tokio::test]
async fn composed_server_provisions_a_user_through_the_oidc_callback() {
    let composed = Composed::start().await;
    let (client_id, returned) = composed.sign_in(RESOURCE).await;
    let redirect = location(&returned);
    assert!(redirect.as_str().starts_with(CALLBACK), "{redirect}");
    assert_eq!(param(&redirect, "state"), "client-state");
    let claims = composed
        .token_claims(&client_id, &param(&redirect, "code"), RESOURCE)
        .await;
    let subject = claims
        .get("sub")
        .and_then(Value::as_str)
        .expect("subject")
        .to_owned();
    assert_ne!(
        subject, "upstream-subject",
        "the upstream subject never leaks"
    );
    let user = OrganizationModule::new(composed.server.database())
        .resolve_user_authority(&UserId::parse(&subject).unwrap())
        .await
        .expect("the login provisioned a User");
    assert_eq!(user.organization_role(), Some(OrganizationRole::Member));
    composed.shutdown().await;
}

/// A sign-in at `/mcp` ends at the approval page where the person picks a
/// Workspace, and its token carries the one they pick.
#[tokio::test]
async fn composed_server_issues_an_organization_token_for_the_picked_workspace() {
    let composed = Composed::start().await;
    let database = composed.server.database();
    let Ok(coral_auth::LoginProvisioning::Provisioned(user_id)) =
        coral_auth::UserDirectory::new(database.clone())
            .provision_login(coral_auth::VerifiedOidcLogin::new(
                &composed.provider.uri(),
                "upstream-subject",
                None,
                1,
            ))
            .await
    else {
        panic!("provision the User who signs in");
    };
    let user = OrganizationModule::new(database.clone())
        .resolve_user_authority(&user_id)
        .await
        .unwrap();
    let workspace = coral_workspace::WorkspaceModule::new(database)
        .create(
            &coral_workspace::ChangeContext::new(
                user,
                2,
                coral_workspace::TraceId::current_or_new(),
            ),
            coral_workspace::WorkspaceDisplayName::parse("Research").unwrap(),
        )
        .await
        .unwrap()
        .id()
        .clone();

    let (client_id, picker) = composed.sign_in(ORGANIZATION_RESOURCE).await;
    assert_eq!(picker.status(), 200, "the approval comes before any code");
    let cookie = cookie_pair(&picker, "__Host-coral_oauth_approval=");
    let ticket = ticket(&picker.text().await.unwrap());
    let chosen = composed
        .http
        .post(format!("{}/oauth/workspace", composed.endpoint))
        .header(ORIGIN, AUTH_ISSUER)
        .header(COOKIE, cookie)
        .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
        .body(format!(
            "ticket={ticket}&workspace={workspace}&decision=continue"
        ))
        .send()
        .await
        .unwrap();
    let redirect = location(&chosen);
    let claims = composed
        .token_claims(&client_id, &param(&redirect, "code"), ORGANIZATION_RESOURCE)
        .await;
    assert_eq!(
        claims.get("aud").and_then(Value::as_str),
        Some(ORGANIZATION_RESOURCE)
    );
    assert_eq!(
        claims.get("workspace_id").and_then(Value::as_str),
        Some(workspace.to_string().as_str())
    );
    assert_eq!(
        claims.get("sub").and_then(Value::as_str),
        Some(user_id.as_str())
    );
    composed.shutdown().await;
}
