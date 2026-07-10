#![cfg_attr(not(test), expect(dead_code, reason = "wired by OAuth descendants"))]
use super::id_token::{ValidatedOidcIdentity, validate_id_token};
use super::provider::OidcProviderConfig;
use crate::outbound_url_policy::{ConfiguredEndpointUrl, read_bounded_body};
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use reqwest::header::ACCEPT;
use ring::rand::{SecureRandom as _, SystemRandom};
use serde::Deserialize;
use sha2::{Digest as _, Sha256};
use std::time::Duration;
use thiserror::Error;
use url::Url;
use zeroize::Zeroizing;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const RESPONSE_MAX_BYTES: usize = 64 * 1024;
#[derive(Clone)]
pub(super) struct OidcProviderClient {
    http: reqwest::Client,
}
pub(super) struct OidcAuthorizationRequest {
    pub(super) url: Url,
    pub(super) state: String,
    pub(super) nonce: String,
    pub(super) code_verifier: String,
}
pub(super) struct OidcCodeExchange {
    id_token: Zeroizing<String>,
    jwks_uri: ConfiguredEndpointUrl,
    signing_algorithms: Vec<String>,
}
impl OidcCodeExchange {
    pub(super) fn id_token(&self) -> &str {
        self.id_token.as_str()
    }
    pub(super) fn jwks_uri(&self) -> &ConfiguredEndpointUrl {
        &self.jwks_uri
    }
    pub(super) fn signing_algorithms(&self) -> &[String] {
        &self.signing_algorithms
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub(super) enum OidcProviderClientError {
    #[error("failed to initialize OIDC provider HTTP client")]
    ClientInitialization,
    #[error("OIDC provider discovery failed")]
    Discovery,
    #[error("OIDC provider discovery issuer did not match configured issuer")]
    IssuerMismatch,
    #[error("OIDC provider discovery contained an invalid {0}")]
    InvalidEndpoint(&'static str),
    #[error("failed to generate OIDC authorization parameters")]
    Randomness,
    #[error("OIDC provider token exchange failed")]
    TokenExchange,
    #[error("OIDC ID token validation failed")]
    IdTokenValidation,
}
impl OidcProviderClient {
    pub(super) fn new() -> Result<Self, OidcProviderClientError> {
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(REQUEST_TIMEOUT)
            .build()
            .map_err(|_error| OidcProviderClientError::ClientInitialization)?;
        Ok(Self { http })
    }

    pub(super) async fn authorization_request(
        &self,
        provider: &OidcProviderConfig,
    ) -> Result<OidcAuthorizationRequest, OidcProviderClientError> {
        let discovery = self.discover(provider).await?;
        let state = random_url_token()?;
        let nonce = random_url_token()?;
        let code_verifier = random_url_token()?;
        let code_challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(code_verifier.as_bytes()));
        let mut url = discovery.authorization_endpoint.into_url();
        {
            let mut query = url.query_pairs_mut();
            query
                .append_pair("response_type", "code")
                .append_pair("client_id", &provider.client_id)
                .append_pair("redirect_uri", &provider.redirect_uri)
                .append_pair("scope", &provider.scopes.join(" "))
                .append_pair("state", &state)
                .append_pair("nonce", &nonce)
                .append_pair("code_challenge", &code_challenge)
                .append_pair("code_challenge_method", "S256");
            for (key, value) in &provider.auth_params {
                query.append_pair(key, value);
            }
        }
        Ok(OidcAuthorizationRequest {
            url,
            state,
            nonce,
            code_verifier,
        })
    }
    pub(super) async fn exchange_code(
        &self,
        provider: &OidcProviderConfig,
        code: &str,
        code_verifier: &str,
    ) -> Result<OidcCodeExchange, OidcProviderClientError> {
        let discovery = self.discover(provider).await?;
        let response = self
            .http
            .post(discovery.token_endpoint.as_url().clone())
            .header(ACCEPT, "application/json")
            .form(&[
                ("grant_type", "authorization_code"),
                ("code", code),
                ("redirect_uri", provider.redirect_uri.as_str()),
                ("client_id", provider.client_id.as_str()),
                ("client_secret", provider.client_secret()),
                ("code_verifier", code_verifier),
            ])
            .send()
            .await
            .map_err(|_error| OidcProviderClientError::TokenExchange)?;
        if !response.status().is_success() {
            return Err(OidcProviderClientError::TokenExchange);
        }
        let body = Zeroizing::new(
            read_bounded_body(response, RESPONSE_MAX_BYTES)
                .await
                .map_err(|_error| OidcProviderClientError::TokenExchange)?,
        );
        let document: TokenResponse = serde_json::from_slice(&body)
            .map_err(|_error| OidcProviderClientError::TokenExchange)?;
        if document.id_token.is_empty() {
            return Err(OidcProviderClientError::TokenExchange);
        }
        Ok(OidcCodeExchange {
            id_token: document.id_token,
            jwks_uri: discovery.jwks_uri,
            signing_algorithms: discovery.signing_algorithms,
        })
    }
    pub(super) async fn validate_code_exchange(
        &self,
        provider: &OidcProviderConfig,
        exchange: OidcCodeExchange,
        expected_nonce: &str,
    ) -> Result<ValidatedOidcIdentity, OidcProviderClientError> {
        let OidcCodeExchange {
            id_token,
            jwks_uri,
            signing_algorithms,
        } = exchange;
        let response = self
            .http
            .get(jwks_uri.into_url())
            .header(ACCEPT, "application/json")
            .send()
            .await
            .map_err(|_error| OidcProviderClientError::IdTokenValidation)?;
        if !response.status().is_success() {
            return Err(OidcProviderClientError::IdTokenValidation);
        }
        let body = read_bounded_body(response, RESPONSE_MAX_BYTES)
            .await
            .map_err(|_error| OidcProviderClientError::IdTokenValidation)?;
        let jwks = serde_json::from_slice(&body)
            .map_err(|_error| OidcProviderClientError::IdTokenValidation)?;
        let identity = validate_id_token(
            provider,
            &id_token,
            expected_nonce,
            &signing_algorithms,
            &jwks,
        );
        drop(id_token);
        identity.map_err(|_error| OidcProviderClientError::IdTokenValidation)
    }
    async fn discover(
        &self,
        provider: &OidcProviderConfig,
    ) -> Result<ValidatedDiscovery, OidcProviderClientError> {
        let url = discovery_url(&provider.issuer)?;
        let response = self
            .http
            .get(url.into_url())
            .header(ACCEPT, "application/json")
            .send()
            .await
            .map_err(|_error| OidcProviderClientError::Discovery)?;
        if !response.status().is_success() {
            return Err(OidcProviderClientError::Discovery);
        }
        let body = read_bounded_body(response, RESPONSE_MAX_BYTES)
            .await
            .map_err(|_error| OidcProviderClientError::Discovery)?;
        let document: DiscoveryDocument =
            serde_json::from_slice(&body).map_err(|_error| OidcProviderClientError::Discovery)?;
        if document.issuer != provider.issuer {
            return Err(OidcProviderClientError::IssuerMismatch);
        }
        if !document
            .id_token_signing_alg_values_supported
            .iter()
            .any(|algorithm| algorithm == "RS256")
        {
            return Err(OidcProviderClientError::Discovery);
        }
        Ok(ValidatedDiscovery {
            authorization_endpoint: discovered_endpoint(
                "authorization_endpoint",
                &document.authorization_endpoint,
            )?,
            token_endpoint: discovered_endpoint("token_endpoint", &document.token_endpoint)?,
            jwks_uri: discovered_endpoint("jwks_uri", &document.jwks_uri)?,
            signing_algorithms: document.id_token_signing_alg_values_supported,
        })
    }
}
struct ValidatedDiscovery {
    authorization_endpoint: ConfiguredEndpointUrl,
    token_endpoint: ConfiguredEndpointUrl,
    jwks_uri: ConfiguredEndpointUrl,
    signing_algorithms: Vec<String>,
}
#[derive(Deserialize)]
struct DiscoveryDocument {
    issuer: String,
    authorization_endpoint: String,
    token_endpoint: String,
    jwks_uri: String,
    id_token_signing_alg_values_supported: Vec<String>,
}
#[derive(Deserialize)]
struct TokenResponse {
    id_token: Zeroizing<String>,
}
fn discovery_url(issuer: &str) -> Result<ConfiguredEndpointUrl, OidcProviderClientError> {
    ConfiguredEndpointUrl::parse(&format!(
        "{}/.well-known/openid-configuration",
        issuer.trim_end_matches('/')
    ))
    .map_err(|_error| OidcProviderClientError::Discovery)
}
fn discovered_endpoint(
    label: &'static str,
    value: &str,
) -> Result<ConfiguredEndpointUrl, OidcProviderClientError> {
    if value.trim() != value {
        return Err(OidcProviderClientError::InvalidEndpoint(label));
    }
    let url = ConfiguredEndpointUrl::parse(value)
        .map_err(|_error| OidcProviderClientError::InvalidEndpoint(label))?;
    let reserved = url.as_url().query_pairs().any(|(key, _value)| {
        let key = key.to_ascii_lowercase();
        match label {
            "authorization_endpoint" => matches!(
                key.as_str(),
                "response_type"
                    | "client_id"
                    | "redirect_uri"
                    | "scope"
                    | "state"
                    | "nonce"
                    | "code_challenge"
                    | "code_challenge_method"
            ),
            "token_endpoint" => matches!(
                key.as_str(),
                "grant_type"
                    | "code"
                    | "redirect_uri"
                    | "client_id"
                    | "client_secret"
                    | "code_verifier"
            ),
            _ => false,
        }
    });
    (!reserved)
        .then_some(url)
        .ok_or(OidcProviderClientError::InvalidEndpoint(label))
}

fn random_url_token() -> Result<String, OidcProviderClientError> {
    let mut bytes = Zeroizing::new([0_u8; 32]);
    SystemRandom::new()
        .fill(&mut *bytes)
        .map_err(|_error| OidcProviderClientError::Randomness)?;
    Ok(URL_SAFE_NO_PAD.encode(bytes.as_slice()))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde_json::{Value, json};
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;
    use crate::auth::id_token::tests::{claims as id_token_claims, rsa_key, token as id_token};
    use crate::auth::provider::ProviderConfigFile;

    const CLIENT_SECRET: &str = "client-secret-must-not-leak";

    fn provider(issuer: &str) -> OidcProviderConfig {
        toml::from_str::<ProviderConfigFile>(&format!(
            "issuer = '{issuer}'
             client_id = 'provider-client'
             client_secret = '{CLIENT_SECRET}'
             redirect_uri = 'http://localhost/callback'
             scopes = ['openid', 'email']
             [auth_params]
             prompt = 'login'"
        ))
        .expect("provider file")
        .build("test", &|_| Ok(None))
        .expect("provider")
    }

    fn discovery(issuer: &str, authorization: &str, token: &str, jwks: &str) -> Value {
        json!({
            "issuer": issuer,
            "authorization_endpoint": authorization,
            "token_endpoint": token,
            "jwks_uri": jwks,
            "id_token_signing_alg_values_supported": ["RS256"],
        })
    }

    async fn mount_discovery(server: &MockServer, request_path: &str, body: impl Into<Vec<u8>>) {
        Mock::given(method("GET"))
            .and(path(request_path))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(body))
            .mount(server)
            .await;
    }

    fn valid_document(server: &MockServer, issuer: &str) -> Value {
        discovery(
            issuer,
            &format!("{}/authorize?tenant=one", server.uri()),
            &format!("{}/token?tenant=one", server.uri()),
            &format!("{}/jwks", server.uri()),
        )
    }

    fn validation_exchange(server: &MockServer) -> OidcCodeExchange {
        OidcCodeExchange {
            id_token: Zeroizing::new(id_token(&id_token_claims())),
            jwks_uri: ConfiguredEndpointUrl::parse(&format!(
                "{}/jwks?source=url-secret",
                server.uri()
            ))
            .expect("JWKS URL"),
            signing_algorithms: vec!["RS256".into()],
        }
    }

    #[tokio::test]
    async fn authorization_request_uses_exact_discovery_and_fresh_pkce_secrets() {
        let server = MockServer::start().await;
        let issuer = format!("{}/tenant/", server.uri());
        mount_discovery(
            &server,
            "/tenant/.well-known/openid-configuration",
            valid_document(&server, &issuer).to_string(),
        )
        .await;
        let request = OidcProviderClient::new()
            .expect("client")
            .authorization_request(&provider(&issuer))
            .await
            .expect("authorization request");
        for value in [&request.state, &request.nonce, &request.code_verifier] {
            assert_eq!(value.len(), 43);
            assert!(
                value
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
            );
        }
        assert_ne!(request.state, request.nonce);
        assert_ne!(request.nonce, request.code_verifier);
        let query = request
            .url
            .query_pairs()
            .into_owned()
            .collect::<BTreeMap<_, _>>();
        for (key, expected) in [
            ("tenant", "one"),
            ("response_type", "code"),
            ("client_id", "provider-client"),
            ("redirect_uri", "http://localhost/callback"),
            ("scope", "openid email"),
            ("code_challenge_method", "S256"),
            ("prompt", "login"),
        ] {
            assert_eq!(query.get(key).map(String::as_str), Some(expected));
        }
        assert_eq!(query.get("state"), Some(&request.state));
        assert_eq!(query.get("nonce"), Some(&request.nonce));
        let expected = URL_SAFE_NO_PAD.encode(Sha256::digest(request.code_verifier.as_bytes()));
        assert_eq!(query.get("code_challenge"), Some(&expected));
    }

    #[tokio::test]
    async fn discovery_requires_exact_issuer_and_validates_every_endpoint() {
        for (field, invalid) in [
            ("issuer", "http://127.0.0.1:1/"),
            ("authorization_endpoint", "http://remote.test/authorize"),
            ("token_endpoint", "https://user:pass@remote.test/token"),
            ("jwks_uri", "https://remote.test/jwks#secret"),
            (
                "authorization_endpoint",
                "http://localhost:1/authorize?StAtE=evil",
            ),
            (
                "token_endpoint",
                "http://localhost:1/token?CLIENT_SECRET=evil",
            ),
        ] {
            let server = MockServer::start().await;
            let issuer = server.uri();
            let mut document = valid_document(&server, &issuer);
            *document.get_mut(field).expect("discovery field") = Value::String(invalid.into());
            mount_discovery(
                &server,
                "/.well-known/openid-configuration",
                document.to_string(),
            )
            .await;
            let error = OidcProviderClient::new()
                .expect("client")
                .discover(&provider(&issuer))
                .await
                .err()
                .expect("invalid discovery");
            let expected = if field == "issuer" {
                OidcProviderClientError::IssuerMismatch
            } else {
                OidcProviderClientError::InvalidEndpoint(field)
            };
            assert_eq!(error, expected);
            assert!(!format!("{error:?} {error}").contains(invalid));
        }

        let server = MockServer::start().await;
        let issuer = server.uri();
        mount_discovery(
            &server,
            "/.well-known/openid-configuration",
            discovery(
                &issuer,
                "https://10.0.0.8/authorize",
                "https://10.0.0.8/token",
                "https://10.0.0.8/jwks",
            )
            .to_string(),
        )
        .await;
        OidcProviderClient::new()
            .expect("client")
            .discover(&provider(&issuer))
            .await
            .expect("private HTTPS endpoints");

        for algorithms in [json!([]), json!(["ES256", "rs256"])] {
            let server = MockServer::start().await;
            let issuer = server.uri();
            let mut document = valid_document(&server, &issuer);
            *document
                .get_mut("id_token_signing_alg_values_supported")
                .expect("algorithms") = algorithms;
            mount_discovery(
                &server,
                "/.well-known/openid-configuration",
                document.to_string(),
            )
            .await;
            assert_eq!(
                OidcProviderClient::new()
                    .expect("client")
                    .discover(&provider(&issuer))
                    .await
                    .err()
                    .expect("missing RS256"),
                OidcProviderClientError::Discovery
            );
        }
    }

    #[tokio::test]
    async fn code_exchange_posts_exact_form_and_returns_redacted_result() {
        let server = MockServer::start().await;
        let issuer = server.uri();
        mount_discovery(
            &server,
            "/.well-known/openid-configuration",
            valid_document(&server, &issuer).to_string(),
        )
        .await;
        Mock::given(method("POST"))
            .and(path("/token"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({"id_token": "header.payload.signature"})),
            )
            .mount(&server)
            .await;
        let exchange = OidcProviderClient::new()
            .expect("client")
            .exchange_code(&provider(&issuer), "provider-code", "pkce-verifier")
            .await
            .expect("exchange");
        assert_eq!(exchange.id_token(), "header.payload.signature");
        assert_eq!(exchange.jwks_uri().as_url().path(), "/jwks");
        assert_eq!(exchange.signing_algorithms(), ["RS256"]);
        let requests = server.received_requests().await.expect("requests");
        let request = requests
            .iter()
            .find(|request| request.url.path() == "/token")
            .expect("token request");
        assert_eq!(request.url.query(), Some("tenant=one"));
        let form = url::form_urlencoded::parse(&request.body)
            .into_owned()
            .collect::<Vec<_>>();
        assert_eq!(
            form,
            vec![
                ("grant_type".into(), "authorization_code".into()),
                ("code".into(), "provider-code".into()),
                ("redirect_uri".into(), "http://localhost/callback".into()),
                ("client_id".into(), "provider-client".into()),
                ("client_secret".into(), CLIENT_SECRET.into()),
                ("code_verifier".into(), "pkce-verifier".into()),
            ]
        );
    }

    #[tokio::test]
    async fn token_redirect_is_not_followed_and_errors_never_echo_secrets() {
        let server = MockServer::start().await;
        let issuer = server.uri();
        mount_discovery(
            &server,
            "/.well-known/openid-configuration",
            valid_document(&server, &issuer).to_string(),
        )
        .await;
        Mock::given(method("POST"))
            .and(path("/token"))
            .respond_with(
                ResponseTemplate::new(307)
                    .insert_header("Location", format!("{}/leak", server.uri()))
                    .set_body_string("upstream-body-secret"),
            )
            .mount(&server)
            .await;
        Mock::given(path("/leak"))
            .respond_with(ResponseTemplate::new(200))
            .expect(0)
            .mount(&server)
            .await;
        let error = OidcProviderClient::new()
            .expect("client")
            .exchange_code(
                &provider(&issuer),
                "secret-provider-code",
                "secret-verifier",
            )
            .await
            .err()
            .expect("redirect refused");
        let rendered = format!("{error:?} {error}");
        for secret in [
            CLIENT_SECRET,
            "secret-provider-code",
            "secret-verifier",
            "upstream-body-secret",
            "/leak",
        ] {
            assert!(!rendered.contains(secret));
        }
        server.verify().await;
    }

    #[tokio::test]
    async fn discovery_and_token_responses_are_bounded() {
        let server = MockServer::start().await;
        let issuer = server.uri();
        mount_discovery(
            &server,
            "/.well-known/openid-configuration",
            vec![b'x'; RESPONSE_MAX_BYTES + 1],
        )
        .await;
        assert_eq!(
            OidcProviderClient::new()
                .expect("client")
                .discover(&provider(&issuer))
                .await
                .err()
                .expect("oversized discovery"),
            OidcProviderClientError::Discovery
        );
        server.reset().await;
        mount_discovery(
            &server,
            "/.well-known/openid-configuration",
            valid_document(&server, &issuer).to_string(),
        )
        .await;
        Mock::given(method("POST"))
            .and(path("/token"))
            .respond_with(
                ResponseTemplate::new(200).set_body_bytes(vec![b'x'; RESPONSE_MAX_BYTES + 1]),
            )
            .mount(&server)
            .await;
        let error = OidcProviderClient::new()
            .expect("client")
            .exchange_code(&provider(&issuer), "code", "verifier")
            .await
            .err()
            .expect("oversized token response");
        assert_eq!(error, OidcProviderClientError::TokenExchange);
    }

    #[tokio::test]
    async fn fetches_bounded_jwks_and_returns_only_verified_identity() {
        let server = MockServer::start().await;
        let provider = provider("http://localhost/issuer");
        let exchange = validation_exchange(&server);
        Mock::given(method("GET"))
            .and(path("/jwks"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"keys":[rsa_key()]})))
            .mount(&server)
            .await;
        let identity = OidcProviderClient::new()
            .expect("client")
            .validate_code_exchange(&provider, exchange, "expected-nonce")
            .await
            .expect("identity");
        assert_eq!(identity.principal, "subject");
        assert_eq!(identity.display_name.as_deref(), Some("User"));
    }

    #[tokio::test]
    async fn jwks_failures_are_bounded_redirect_safe_and_redacted() {
        for response in [
            ResponseTemplate::new(503).set_body_string("status-body-secret"),
            ResponseTemplate::new(200).set_body_string("json-body-secret"),
            ResponseTemplate::new(200).set_body_bytes(vec![b'x'; RESPONSE_MAX_BYTES + 1]),
        ] {
            let server = MockServer::start().await;
            Mock::given(path("/jwks"))
                .respond_with(response)
                .mount(&server)
                .await;
            let error = OidcProviderClient::new()
                .expect("client")
                .validate_code_exchange(
                    &provider("http://localhost/issuer"),
                    validation_exchange(&server),
                    "nonce-secret",
                )
                .await
                .map(|_| ())
                .expect_err("invalid JWKS");
            assert_eq!(
                format!("{error:?} {error}"),
                "IdTokenValidation OIDC ID token validation failed"
            );
        }
        let server = MockServer::start().await;
        Mock::given(path("/jwks"))
            .respond_with(
                ResponseTemplate::new(302)
                    .insert_header("Location", format!("{}/leak", server.uri())),
            )
            .mount(&server)
            .await;
        Mock::given(path("/leak"))
            .respond_with(ResponseTemplate::new(200))
            .expect(0)
            .mount(&server)
            .await;
        OidcProviderClient::new()
            .expect("client")
            .validate_code_exchange(
                &provider("http://localhost/issuer"),
                validation_exchange(&server),
                "nonce-secret",
            )
            .await
            .map(|_| ())
            .expect_err("redirect refused");
        server.verify().await;
    }
}
