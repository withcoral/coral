//! The composed server provisions a User through the upstream OIDC callback.

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

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one login is followed from client registration to a provisioned User"
)]
async fn composed_server_provisions_a_user_through_the_oidc_callback() {
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
            ("resource", RESOURCE),
            ("state", "client-state"),
        ])
        .send()
        .await
        .unwrap();
    assert_eq!(page.status(), 200);
    let cookie = page
        .headers()
        .get(SET_COOKIE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .expect("approval cookie")
        .to_owned();
    let html = page.text().await.unwrap();
    let ticket = html
        .split("name=\"ticket\" value=\"")
        .nth(1)
        .and_then(|tail| tail.split('"').next())
        .expect("approval ticket");

    let approved = http
        .post(format!("{endpoint}/oauth/authorize"))
        .header(ORIGIN, AUTH_ISSUER)
        .header(COOKIE, cookie)
        .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
        .body(format!("ticket={ticket}&decision=continue"))
        .send()
        .await
        .unwrap();
    let upstream = location(&approved);
    let sign_in_cookie = approved
        .headers()
        .get(SET_COOKIE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .expect("sign-in cookie")
        .to_owned();
    mount_token(&provider, "upstream-subject", &param(&upstream, "nonce")).await;

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
    let redirect = location(&returned);
    assert!(redirect.as_str().starts_with(CALLBACK), "{redirect}");
    assert_eq!(param(&redirect, "state"), "client-state");
    let code = param(&redirect, "code");

    let token: Value = http
        .post(format!("{endpoint}/oauth/token"))
        .form(&[
            ("grant_type", "authorization_code"),
            ("code", code.as_str()),
            ("client_id", client_id.as_str()),
            ("redirect_uri", CALLBACK),
            ("code_verifier", VERIFIER),
            ("resource", RESOURCE),
        ])
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let access_token = token
        .get("access_token")
        .and_then(Value::as_str)
        .expect("access token");
    let subject = claims(access_token)
        .get("sub")
        .and_then(Value::as_str)
        .expect("subject")
        .to_owned();
    assert_ne!(
        subject, "upstream-subject",
        "the upstream subject never leaks"
    );
    let user = OrganizationModule::new(server.database())
        .resolve_user_authority(&UserId::parse(&subject).unwrap())
        .await
        .expect("the login provisioned a User");
    assert_eq!(user.organization_role(), Some(OrganizationRole::Member));

    auth.shutdown().await.unwrap();
    server.shutdown().await.unwrap();
}
