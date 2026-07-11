//! Native-app OAuth login protocol for the Coral CLI.

use std::fmt;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use reqwest::StatusCode;
use reqwest::header::{ACCEPT, CONTENT_TYPE};
use ring::rand::{SecureRandom as _, SystemRandom};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio::net::TcpListener;
use tokio::time::Instant;
use url::Url;
use zeroize::Zeroizing;

use super::{CLI_CLIENT, CLI_REDIRECT_URI, DEFAULT_OAUTH_SCOPE};
use crate::oauth_loopback::{OAuthCallbackOutcome, OAuthLoopbackError, OAuthLoopbackReceiver};
use crate::outbound_url_policy::{ConfiguredEndpointUrl, read_bounded_body};

const METADATA_PATH: &str = "/.well-known/oauth-authorization-server";
const CALLBACK_ADDR: SocketAddr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 14554));
const HTTP_CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
const HTTP_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const LOGIN_TIMEOUT: Duration = Duration::from_mins(5);
const RESPONSE_MAX_BYTES: usize = 16 * 1024;
const RESERVED_ENDPOINT_QUERY_KEYS: &[&str] = &[
    "response_type",
    "client_id",
    "redirect_uri",
    "scope",
    "state",
    "code_challenge",
    "code_challenge_method",
    "resource",
    "provider",
    "grant_type",
    "code",
    "code_verifier",
    "client_secret",
];

/// A successful OAuth login result.
pub struct OAuthLoginResult {
    access_token: Zeroizing<String>,
    issuer: String,
    resource: String,
}

impl OAuthLoginResult {
    /// Returns the bearer access token issued by the authorization server.
    #[must_use]
    pub fn access_token(&self) -> &str {
        self.access_token.as_str()
    }

    /// Returns the canonical authorization-server issuer.
    #[must_use]
    pub fn issuer(&self) -> &str {
        &self.issuer
    }

    /// Returns the protected resource advertised by the authorization server.
    #[must_use]
    pub fn resource(&self) -> &str {
        &self.resource
    }
}

impl fmt::Debug for OAuthLoginResult {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OAuthLoginResult")
            .field("access_token", &"<redacted>")
            .field("issuer", &self.issuer)
            .field("resource", &"<redacted>")
            .finish()
    }
}

/// Failure while running the native-app OAuth login protocol.
#[derive(Debug, Error)]
#[error(transparent)]
pub struct OAuthLoginError(#[from] LoginErrorKind);

#[derive(Debug, Error)]
enum LoginErrorKind {
    #[error("OAuth authorization server must be a root HTTPS URL or explicit loopback HTTP URL")]
    InvalidAuthorizationServer,
    #[error("OAuth authorization-server metadata request or response was invalid")]
    Metadata,
    #[error("failed to bind the OAuth loopback callback listener")]
    CallbackBind(#[source] std::io::Error),
    #[error("failed to generate OAuth login secrets")]
    Randomness,
    #[error("OAuth loopback callback failed")]
    Callback(#[source] OAuthLoopbackError),
    #[error("OAuth provider rejected authorization with `{0}`")]
    ProviderRejected(String),
    #[error("OAuth login timed out")]
    TimedOut,
    #[error("OAuth token request or response was invalid")]
    Token,
}

/// Runs Coral's browser-based public-client OAuth login protocol.
/// The callback receives the authorization URL after the listener is bound.
///
/// # Errors
/// Returns an error when issuer or metadata validation, callback handling, or
/// authorization-code redemption fails.
pub async fn run_oauth_login(
    authorization_server: &str,
    provider: Option<&str>,
    present_authorization_url: impl FnOnce(&str),
) -> Result<OAuthLoginResult, OAuthLoginError> {
    let server = AuthorizationServer::parse(authorization_server)?;
    let callback = bind_callback_at(CALLBACK_ADDR).await?;
    run_with_callback(server, callback, provider, present_authorization_url).await
}

struct AuthorizationServer {
    url: Url,
    canonical: String,
}

impl AuthorizationServer {
    fn parse(value: &str) -> Result<Self, OAuthLoginError> {
        if value.trim() != value {
            return Err(LoginErrorKind::InvalidAuthorizationServer.into());
        }
        let url = ConfiguredEndpointUrl::parse(value)
            .map_err(|_error| LoginErrorKind::InvalidAuthorizationServer)?
            .into_url();
        if url.query().is_some() || url.path() != "/" {
            return Err(LoginErrorKind::InvalidAuthorizationServer.into());
        }
        let canonical = url.as_str().trim_end_matches('/').to_string();
        Ok(Self { url, canonical })
    }

    fn metadata_url(&self) -> Url {
        let mut url = self.url.clone();
        url.set_path(METADATA_PATH);
        url
    }
}

struct CallbackListener {
    listener: TcpListener,
    redirect_uri: Url,
}

async fn bind_callback_at(address: SocketAddr) -> Result<CallbackListener, OAuthLoginError> {
    let listener = TcpListener::bind(address)
        .await
        .map_err(LoginErrorKind::CallbackBind)?;
    let port = listener
        .local_addr()
        .map_err(LoginErrorKind::CallbackBind)?
        .port();
    let mut redirect_uri = Url::parse(CLI_REDIRECT_URI)
        .map_err(|_error| LoginErrorKind::InvalidAuthorizationServer)?;
    redirect_uri
        .set_port(Some(port))
        .map_err(|()| LoginErrorKind::InvalidAuthorizationServer)?;
    Ok(CallbackListener {
        listener,
        redirect_uri,
    })
}

async fn run_with_callback(
    server: AuthorizationServer,
    callback: CallbackListener,
    provider: Option<&str>,
    present_authorization_url: impl FnOnce(&str),
) -> Result<OAuthLoginResult, OAuthLoginError> {
    let http = http_client()?;
    let metadata = discover(&http, &server).await?;
    let state = random_url_token()?;
    let verifier = random_url_token()?;
    let client_id = metadata.client_id.clone();
    let authorization_url = authorization_url(
        &metadata,
        &client_id,
        callback.redirect_uri.as_str(),
        &state,
        &pkce_challenge(&verifier),
        provider,
    );
    let deadline = Instant::now() + LOGIN_TIMEOUT;
    let receiver = OAuthLoopbackReceiver::new(
        callback.listener,
        callback.redirect_uri.clone(),
        state.to_string(),
        deadline,
    )
    .map_err(LoginErrorKind::Callback)?;
    present_authorization_url(authorization_url.as_str());
    let outcome = receiver.receive().await.map_err(|error| match error {
        OAuthLoopbackError::TimedOut => LoginErrorKind::TimedOut,
        error => LoginErrorKind::Callback(error),
    })?;
    let code = match outcome {
        OAuthCallbackOutcome::AuthorizationCode(code) => Zeroizing::new(code),
        OAuthCallbackOutcome::ProviderError { error, .. } => {
            return Err(LoginErrorKind::ProviderRejected(safe_error_code(&error)).into());
        }
    };
    tokio::time::timeout_at(
        deadline,
        redeem_code(
            &http,
            metadata,
            &client_id,
            callback.redirect_uri.as_str(),
            &code,
            &verifier,
        ),
    )
    .await
    .map_err(|_elapsed| LoginErrorKind::TimedOut)?
}

fn http_client() -> Result<reqwest::Client, OAuthLoginError> {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .no_proxy()
        .connect_timeout(HTTP_CONNECT_TIMEOUT)
        .timeout(HTTP_REQUEST_TIMEOUT)
        .build()
        .map_err(|_error| LoginErrorKind::Metadata.into())
}

#[derive(Clone, Deserialize, Serialize)]
struct MetadataDocument {
    issuer: String,
    authorization_endpoint: String,
    token_endpoint: String,
    response_types_supported: Vec<String>,
    grant_types_supported: Vec<String>,
    code_challenge_methods_supported: Vec<String>,
    token_endpoint_auth_methods_supported: Vec<String>,
    scopes_supported: Vec<String>,
    resource: String,
    client_id_metadata_document_supported: bool,
}

struct ValidatedMetadata {
    authorization_endpoint: Url,
    token_endpoint: Url,
    resource: String,
    issuer: String,
    client_id: String,
}

async fn discover(
    http: &reqwest::Client,
    server: &AuthorizationServer,
) -> Result<ValidatedMetadata, OAuthLoginError> {
    let response = http
        .get(server.metadata_url())
        .header(ACCEPT, "application/json")
        .send()
        .await
        .map_err(|_error| LoginErrorKind::Metadata)?;
    if response.status() != StatusCode::OK {
        return Err(LoginErrorKind::Metadata.into());
    }
    if !is_json_response(&response) {
        return Err(LoginErrorKind::Metadata.into());
    }
    let body = Zeroizing::new(
        read_bounded_body(response, RESPONSE_MAX_BYTES)
            .await
            .map_err(|_error| LoginErrorKind::Metadata)?,
    );
    let document = serde_json::from_slice(&body).map_err(|_error| LoginErrorKind::Metadata)?;
    validate_metadata(server, document)
}

fn validate_metadata(
    server: &AuthorizationServer,
    document: MetadataDocument,
) -> Result<ValidatedMetadata, OAuthLoginError> {
    let metadata_issuer =
        AuthorizationServer::parse(&document.issuer).map_err(|_error| LoginErrorKind::Metadata)?;
    if metadata_issuer.canonical != server.canonical
        || !supports(&document.response_types_supported, "code")
        || !supports(&document.grant_types_supported, "authorization_code")
        || !supports(&document.code_challenge_methods_supported, "S256")
        || !supports(&document.token_endpoint_auth_methods_supported, "none")
        || !supports(&document.scopes_supported, DEFAULT_OAUTH_SCOPE)
        || !document.client_id_metadata_document_supported
    {
        return Err(LoginErrorKind::Metadata.into());
    }
    let authorization_endpoint =
        discovered_endpoint(&document.authorization_endpoint, &server.url)?;
    let token_endpoint = discovered_endpoint(&document.token_endpoint, &server.url)?;
    if document.resource.trim() != document.resource || document.resource.is_empty() {
        return Err(LoginErrorKind::Metadata.into());
    }
    ConfiguredEndpointUrl::parse(&document.resource).map_err(|_error| LoginErrorKind::Metadata)?;
    Ok(ValidatedMetadata {
        authorization_endpoint,
        token_endpoint,
        resource: document.resource,
        issuer: server.canonical.clone(),
        client_id: format!(
            "{}/oauth/clients/{CLI_CLIENT}",
            document.issuer.trim_end_matches('/')
        ),
    })
}

fn supports(values: &[String], expected: &str) -> bool {
    values.iter().any(|value| value == expected)
}

fn discovered_endpoint(value: &str, issuer: &Url) -> Result<Url, OAuthLoginError> {
    if value.trim() != value {
        return Err(LoginErrorKind::Metadata.into());
    }
    let endpoint = ConfiguredEndpointUrl::parse(value)
        .map_err(|_error| LoginErrorKind::Metadata)?
        .into_url();
    if endpoint.origin() != issuer.origin()
        || endpoint.query_pairs().any(|(key, _value)| {
            RESERVED_ENDPOINT_QUERY_KEYS
                .iter()
                .any(|reserved| key.eq_ignore_ascii_case(reserved))
        })
    {
        return Err(LoginErrorKind::Metadata.into());
    }
    Ok(endpoint)
}

fn authorization_url(
    metadata: &ValidatedMetadata,
    client_id: &str,
    redirect_uri: &str,
    state: &str,
    code_challenge: &str,
    provider: Option<&str>,
) -> Url {
    let mut url = metadata.authorization_endpoint.clone();
    url.query_pairs_mut()
        .append_pair("response_type", "code")
        .append_pair("client_id", client_id)
        .append_pair("redirect_uri", redirect_uri)
        .append_pair("scope", DEFAULT_OAUTH_SCOPE)
        .append_pair("state", state)
        .append_pair("code_challenge", code_challenge)
        .append_pair("code_challenge_method", "S256")
        .append_pair("resource", &metadata.resource);
    if let Some(provider) = provider {
        url.query_pairs_mut().append_pair("provider", provider);
    }
    url
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: Zeroizing<String>,
    token_type: String,
    scope: Option<String>,
}

async fn redeem_code(
    http: &reqwest::Client,
    metadata: ValidatedMetadata,
    client_id: &str,
    redirect_uri: &str,
    code: &str,
    verifier: &str,
) -> Result<OAuthLoginResult, OAuthLoginError> {
    let response = http
        .post(metadata.token_endpoint)
        .header(ACCEPT, "application/json")
        .form(&[
            ("grant_type", "authorization_code"),
            ("code", code),
            ("redirect_uri", redirect_uri),
            ("client_id", client_id),
            ("code_verifier", verifier),
        ])
        .send()
        .await
        .map_err(|_error| LoginErrorKind::Token)?;
    if response.status() != StatusCode::OK {
        return Err(LoginErrorKind::Token.into());
    }
    if !is_json_response(&response) {
        return Err(LoginErrorKind::Token.into());
    }
    let body = Zeroizing::new(
        read_bounded_body(response, RESPONSE_MAX_BYTES)
            .await
            .map_err(|_error| LoginErrorKind::Token)?,
    );
    let token: TokenResponse =
        serde_json::from_slice(&body).map_err(|_error| LoginErrorKind::Token)?;
    if !token.token_type.eq_ignore_ascii_case("bearer")
        || !valid_access_token(&token.access_token)
        || token
            .scope
            .as_deref()
            .is_some_and(|scope| scope != DEFAULT_OAUTH_SCOPE)
    {
        return Err(LoginErrorKind::Token.into());
    }
    Ok(OAuthLoginResult {
        access_token: token.access_token,
        issuer: metadata.issuer,
        resource: metadata.resource,
    })
}

fn is_json_response(response: &reqwest::Response) -> bool {
    response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .is_some_and(|value| value.trim().eq_ignore_ascii_case("application/json"))
}

fn valid_access_token(token: &str) -> bool {
    !token.is_empty()
        && token.trim() == token
        && token.is_ascii()
        && !token
            .bytes()
            .any(|byte| byte.is_ascii_whitespace() || byte.is_ascii_control())
}

fn random_url_token() -> Result<Zeroizing<String>, OAuthLoginError> {
    let mut bytes = Zeroizing::new([0_u8; 32]);
    SystemRandom::new()
        .fill(&mut *bytes)
        .map_err(|_error| LoginErrorKind::Randomness)?;
    Ok(Zeroizing::new(URL_SAFE_NO_PAD.encode(bytes.as_slice())))
}

fn pkce_challenge(verifier: &str) -> String {
    URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()))
}

fn safe_error_code(value: &str) -> String {
    let valid = (1..=64).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'));
    if valid {
        value.to_string()
    } else {
        "unknown_error".to_string()
    }
}

#[cfg(test)]
#[expect(clippy::indexing_slicing, reason = "validated test maps")]
mod tests {
    use std::collections::BTreeMap;

    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;

    fn document(issuer: &str) -> MetadataDocument {
        MetadataDocument {
            issuer: issuer.into(),
            authorization_endpoint: format!("{issuer}/oauth/authorize?tenant=one"),
            token_endpoint: format!("{issuer}/oauth/token?tenant=one"),
            response_types_supported: vec!["code".into()],
            grant_types_supported: vec!["authorization_code".into()],
            code_challenge_methods_supported: vec!["S256".into()],
            token_endpoint_auth_methods_supported: vec!["none".into()],
            scopes_supported: vec![DEFAULT_OAUTH_SCOPE.into()],
            resource: "https://mcp.example.test/mcp?resource-secret".into(),
            client_id_metadata_document_supported: true,
        }
    }

    #[test]
    fn rejects_unsafe_issuers_and_incompatible_metadata() {
        for invalid in [
            "http://example.test",
            "https://user@example.test",
            "https://example.test/tenant",
            "https://example.test?query=one",
            "https://example.test#fragment",
        ] {
            assert!(AuthorizationServer::parse(invalid).is_err(), "{invalid}");
        }
        let server = AuthorizationServer::parse("https://login.example.test").expect("issuer");
        validate_metadata(&server, document(&server.canonical)).expect("metadata");
        let mut invalid = document(&server.canonical);
        invalid.issuer = "https://other-login.example.test".into();
        assert!(validate_metadata(&server, invalid).is_err());
        let mut invalid = document(&server.canonical);
        invalid.authorization_endpoint = "https://evil.example/oauth/authorize".into();
        assert!(validate_metadata(&server, invalid).is_err());
        let mut invalid = document(&server.canonical);
        invalid.token_endpoint = format!("{}/oauth/token?client_secret=fixed", server.canonical);
        assert!(validate_metadata(&server, invalid).is_err());
        let mut invalid = document(&server.canonical);
        invalid.scopes_supported.clear();
        assert!(validate_metadata(&server, invalid).is_err());
        let mut invalid = document(&server.canonical);
        invalid.client_id_metadata_document_supported = false;
        assert!(validate_metadata(&server, invalid).is_err());
    }

    #[tokio::test]
    async fn discovery_and_token_exchange_preserve_the_public_client_contract() {
        let server = MockServer::start().await;
        let issuer = server.uri();
        Mock::given(method("GET"))
            .and(path(METADATA_PATH))
            .respond_with(ResponseTemplate::new(200).set_body_json(document(&issuer)))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/oauth/token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "token-secret", "token_type": "bearer"
            })))
            .mount(&server)
            .await;
        let configured = AuthorizationServer::parse(&issuer).expect("issuer");
        let callback = bind_callback_at(SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)))
            .await
            .expect("callback");
        let client_id = format!("{issuer}/oauth/clients/{CLI_CLIENT}");
        let flow = run_with_callback(configured, callback, Some("zeta"), |authorization| {
            let url = Url::parse(authorization).expect("authorization URL");
            let query = url.query_pairs().into_owned().collect::<BTreeMap<_, _>>();
            assert_eq!(query["client_id"], client_id);
            assert_eq!(query["scope"], DEFAULT_OAUTH_SCOPE);
            assert_eq!(query["provider"], "zeta");
            assert_eq!(query["code_challenge"].len(), 43);
            let mut redirect = Url::parse(&query["redirect_uri"]).expect("redirect URI");
            redirect
                .query_pairs_mut()
                .append_pair("state", &query["state"])
                .append_pair("code", "code-secret");
            tokio::spawn(async move {
                http_client()
                    .expect("client")
                    .get(redirect)
                    .send()
                    .await
                    .expect("callback");
            });
        });
        let result = tokio::time::timeout(Duration::from_secs(2), flow)
            .await
            .expect("flow timeout")
            .expect("token");
        assert_eq!(result.access_token(), "token-secret");
        assert_eq!(result.issuer(), issuer);
        assert_eq!(result.resource(), document(&issuer).resource);
        assert!(!format!("{result:?}").contains("secret"));
        let requests = server.received_requests().await.expect("requests");
        let token = requests
            .iter()
            .find(|r| r.url.path() == "/oauth/token")
            .expect("token request");
        assert!(token.headers.get("authorization").is_none());
        let form = url::form_urlencoded::parse(&token.body)
            .into_owned()
            .collect::<BTreeMap<_, _>>();
        assert_eq!(form.len(), 5);
        assert_eq!(form["client_id"], client_id);
        assert_eq!(form["code_verifier"].len(), 43);
        assert!(!form.contains_key("client_secret"));
        assert_eq!(token.url.query(), Some("tenant=one"));
    }
}
