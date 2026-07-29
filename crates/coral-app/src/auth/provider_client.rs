//! Client for the external `OpenID` Connect provider that authenticates users.
//!
//! # Trust
//!
//! The discovery document is fetched from the operator-configured issuer, but
//! it is not treated as operator-authored: a provider that is compromised, or
//! merely misconfigured, can put anything in it. Two checks bound what it can
//! do. Every endpoint is re-parsed under the [`Discovered`] policy, which
//! refuses a plain-HTTP downgrade the configured issuer did not already permit;
//! and no endpoint may pre-set a query parameter this module reserves, so the
//! document cannot pin `state`, swap `client_id`, or choose a `response_mode`
//! that strands the authorization code short of the callback route.
//!
//! # Errors
//!
//! Failures name the stage that failed and nothing else. No status code, body,
//! or URL from the provider travels in an error, so nothing a provider sends
//! can reach a log or an HTTP response by that route.

#![cfg_attr(not(test), expect(dead_code, reason = "wired by OAuth descendants"))]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use reqwest::header::ACCEPT;
use ring::rand::{SecureRandom as _, SystemRandom};
use serde::Deserialize;
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio::sync::{Mutex, RwLock};
use tokio::time::Instant;
use url::Url;
use zeroize::Zeroizing;

use super::config::{RESERVED_PROVIDER_AUTH_PARAMS, ResolvedOidcProvider};
use super::id_token::{ValidatedOidcIdentity, validate_id_token};
use crate::outbound_url_policy::{Configured, Discovered, EndpointUrl, read_bounded_body};

/// Timeout applied to establishing a connection to the provider.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);

/// Timeout applied to a complete discovery or token request.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// Ceiling on a provider response body, applied while it is read.
const RESPONSE_MAX_BYTES: usize = 64 * 1024;

/// The token endpoint authentication method this client uses.
const TOKEN_ENDPOINT_AUTH_METHOD: &str = "client_secret_post";

/// Form parameters the token request supplies for itself.
///
/// The authorization endpoint's counterpart is
/// [`RESERVED_PROVIDER_AUTH_PARAMS`], which is shared with the config
/// validation that applies the same rule to operator-supplied `auth_params`.
const RESERVED_TOKEN_PARAMS: &[&str] = &[
    "grant_type",
    "code",
    "redirect_uri",
    "client_id",
    "client_secret",
    "code_verifier",
];

/// Performs discovery, authorization requests, and code exchange for one
/// configured provider.
#[derive(Clone)]
pub(super) struct OidcProviderClient {
    http: reqwest::Client,
    random: SystemRandom,
    discovery: Arc<DiscoveryCache>,
}

/// An authorization URL and the per-login values that must outlive it.
///
/// The three values are held until the callback arrives: `state` and `nonce`
/// are compared against what the provider echoes back, and `code_verifier` is
/// sent to the token endpoint to prove this server started the login.
pub(super) struct OidcAuthorizationRequest {
    pub(super) url: Url,
    pub(super) state: String,
    pub(super) nonce: String,
    pub(super) code_verifier: Zeroizing<String>,
}

/// The result of redeeming an authorization code.
///
/// The ID token is unverified at this point; the fields beside it are what a
/// caller needs to verify it, carried from the same discovery document that
/// named the token endpoint.
pub(super) struct OidcCodeExchange {
    id_token: Zeroizing<String>,
    jwks_uri: EndpointUrl<Discovered>,
    signing_algorithms: Vec<String>,
}

impl OidcCodeExchange {
    /// Returns the unverified ID token as received from the provider.
    pub(super) fn id_token(&self) -> &str {
        self.id_token.as_str()
    }

    /// Returns the JWKS endpoint that publishes the ID token's verification
    /// keys.
    pub(super) fn jwks_uri(&self) -> &EndpointUrl<Discovered> {
        &self.jwks_uri
    }

    /// Returns the signing algorithms the provider advertises.
    pub(super) fn signing_algorithms(&self) -> &[String] {
        &self.signing_algorithms
    }
}

/// A failure talking to the provider, carrying no provider-supplied data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub(super) enum OidcProviderClientError {
    /// The HTTP client could not be built from this module's settings.
    #[error("failed to initialize OIDC provider HTTP client")]
    ClientInitialization,
    /// Discovery could not be fetched, parsed, or satisfied.
    #[error("OIDC provider discovery failed")]
    Discovery,
    /// The document's `issuer` differs from the configured one.
    #[error("OIDC provider discovery issuer did not match configured issuer")]
    IssuerMismatch,
    /// A discovered endpoint failed the trust policy or pre-set a parameter
    /// this module reserves. The field names the endpoint, never its value.
    #[error("OIDC provider discovery contained an invalid {0}")]
    InvalidEndpoint(&'static str),
    /// The provider does not offer the token endpoint authentication method
    /// this client uses.
    #[error(
        "OIDC provider does not offer the client_secret_post token endpoint authentication method"
    )]
    UnsupportedTokenEndpointAuth,
    /// The system random number generator failed.
    #[error("failed to generate OIDC authorization parameters")]
    Randomness,
    /// The token request failed, or its response was unusable.
    #[error("OIDC provider token exchange failed")]
    TokenExchange,
    #[error("OIDC ID token validation failed")]
    IdTokenValidation,
}

impl OidcProviderClient {
    /// Builds a client that refuses redirects and ignores proxy settings.
    ///
    /// Refusing redirects keeps a credential-bearing token request from being
    /// replayed to a destination the discovery document did not name and this
    /// module never validated.
    pub(super) fn new() -> Result<Self, OidcProviderClientError> {
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(REQUEST_TIMEOUT)
            .build()
            .map_err(|_error| OidcProviderClientError::ClientInitialization)?;
        Ok(Self {
            http,
            random: SystemRandom::new(),
            discovery: Arc::new(DiscoveryCache::default()),
        })
    }

    /// Builds the URL that starts a login, with the secrets it commits to.
    pub(super) async fn authorization_request(
        &self,
        provider: &ResolvedOidcProvider,
    ) -> Result<OidcAuthorizationRequest, OidcProviderClientError> {
        let discovery = self.discover(provider).await?;
        let state = self.random_url_token()?;
        let nonce = self.random_url_token()?;
        let code_verifier = Zeroizing::new(self.random_url_token()?);
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

    /// Redeems an authorization code for an ID token.
    ///
    /// The body is read into a [`Zeroizing`] buffer because it carries the ID
    /// token; the discovery body read below is not, because a discovery
    /// document is public.
    pub(super) async fn exchange_code(
        &self,
        provider: &ResolvedOidcProvider,
        code: &str,
        code_verifier: &str,
    ) -> Result<OidcCodeExchange, OidcProviderClientError> {
        let discovery = self.discover(provider).await?;
        let response = discovery
            .token_endpoint
            .post(&self.http)
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
        provider: &ResolvedOidcProvider,
        exchange: OidcCodeExchange,
        expected_nonce: &str,
    ) -> Result<ValidatedOidcIdentity, OidcProviderClientError> {
        let OidcCodeExchange {
            id_token,
            jwks_uri,
            signing_algorithms,
        } = exchange;
        let response = jwks_uri
            .get(&self.http)
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

    /// Returns the provider's validated discovery document, reusing a cached
    /// copy while one is live.
    ///
    /// Only successes are cached, so a provider outage never outlives itself:
    /// a failed discovery leaves the previous entry expired and the next call
    /// retries. Concurrent misses are collapsed behind a single refresh, so a
    /// burst of authorization requests makes one outbound call rather than one
    /// per request.
    async fn discover(
        &self,
        provider: &ResolvedOidcProvider,
    ) -> Result<ValidatedDiscovery, OidcProviderClientError> {
        let ttl = provider.discovery_cache_ttl();
        if let Some(cached) = self.discovery.get(&provider.issuer).await {
            return Ok(cached);
        }
        let _refresh = self.discovery.refresh.lock().await;
        // Another task may have populated the entry while this one waited for
        // the refresh lock.
        if let Some(cached) = self.discovery.get(&provider.issuer).await {
            return Ok(cached);
        }
        let discovery = self.fetch_discovery(provider).await?;
        self.discovery
            .insert(&provider.issuer, &discovery, ttl)
            .await;
        Ok(discovery)
    }

    /// Fetches and validates the provider's discovery document.
    async fn fetch_discovery(
        &self,
        provider: &ResolvedOidcProvider,
    ) -> Result<ValidatedDiscovery, OidcProviderClientError> {
        let issuer = EndpointUrl::<Configured>::parse(&provider.issuer)
            .map_err(|_error| OidcProviderClientError::Discovery)?;
        let url = discovery_url(&provider.issuer)?;
        let response = url
            .get(&self.http)
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
        if !document.offers_token_endpoint_auth_method(TOKEN_ENDPOINT_AUTH_METHOD) {
            return Err(OidcProviderClientError::UnsupportedTokenEndpointAuth);
        }

        Ok(ValidatedDiscovery {
            authorization_endpoint: discovered_endpoint(
                DiscoveredEndpoint::Authorization,
                &document.authorization_endpoint,
                &issuer,
            )?,
            token_endpoint: discovered_endpoint(
                DiscoveredEndpoint::Token,
                &document.token_endpoint,
                &issuer,
            )?,
            jwks_uri: discovered_endpoint(DiscoveredEndpoint::Jwks, &document.jwks_uri, &issuer)?,
            signing_algorithms: document.id_token_signing_alg_values_supported,
        })
    }

    /// Returns a fresh URL-safe 256-bit token.
    fn random_url_token(&self) -> Result<String, OidcProviderClientError> {
        let mut bytes = Zeroizing::new([0_u8; 32]);
        self.random
            .fill(&mut *bytes)
            .map_err(|_error| OidcProviderClientError::Randomness)?;
        Ok(URL_SAFE_NO_PAD.encode(bytes.as_slice()))
    }
}

/// A discovery document that passed every check in
/// [`OidcProviderClient::fetch_discovery`].
///
/// Only a validated document is cached, so a copy handed back by
/// [`OidcProviderClient::discover`] carries the same guarantees as a freshly
/// fetched one.
#[derive(Clone)]
struct ValidatedDiscovery {
    authorization_endpoint: EndpointUrl<Discovered>,
    token_endpoint: EndpointUrl<Discovered>,
    jwks_uri: EndpointUrl<Discovered>,
    signing_algorithms: Vec<String>,
}

/// Live discovery documents, keyed by the configured issuer they were fetched
/// for.
///
/// The key space is the set of configured providers, so the map is bounded by
/// configuration rather than by request traffic. `refresh` serializes cache
/// misses: it is held across the outbound call so that concurrent misses
/// produce one request, not one per caller.
#[derive(Default)]
struct DiscoveryCache {
    entries: RwLock<HashMap<String, CachedDiscovery>>,
    refresh: Mutex<()>,
}

struct CachedDiscovery {
    discovery: ValidatedDiscovery,
    expires_at: Instant,
}

impl DiscoveryCache {
    async fn get(&self, issuer: &str) -> Option<ValidatedDiscovery> {
        let now = Instant::now();
        self.entries
            .read()
            .await
            .get(issuer)
            .filter(|entry| entry.expires_at > now)
            .map(|entry| entry.discovery.clone())
    }

    /// Stores `discovery` for `ttl`, dropping it when the deadline cannot be
    /// represented or has already passed.
    ///
    /// A zero `ttl` therefore disables caching without a separate code path.
    async fn insert(&self, issuer: &str, discovery: &ValidatedDiscovery, ttl: Duration) {
        let now = Instant::now();
        let Some(expires_at) = now.checked_add(ttl).filter(|deadline| *deadline > now) else {
            return;
        };
        self.entries.write().await.insert(
            issuer.to_string(),
            CachedDiscovery {
                discovery: discovery.clone(),
                expires_at,
            },
        );
    }
}

/// The fields this module reads from a provider's discovery document.
#[derive(Deserialize)]
struct DiscoveryDocument {
    issuer: String,
    authorization_endpoint: String,
    token_endpoint: String,
    jwks_uri: String,
    id_token_signing_alg_values_supported: Vec<String>,
    token_endpoint_auth_methods_supported: Option<Vec<String>>,
}

impl DiscoveryDocument {
    /// Reports whether the document leaves `method` available at the token
    /// endpoint.
    ///
    /// `OpenID` Connect Discovery 1.0 §3 makes
    /// `token_endpoint_auth_methods_supported` optional and defaults an omitted
    /// value to `client_secret_basic`. Applying that default would reject the
    /// providers that accept `client_secret_post` without advertising it, so an
    /// omitted list is treated as unknown and left for the token request to
    /// settle. A list that is present is believed, which surfaces a provider
    /// this client cannot authenticate to as a named failure before the
    /// operator's user is sent anywhere, rather than an opaque one after they
    /// return.
    fn offers_token_endpoint_auth_method(&self, method: &str) -> bool {
        self.token_endpoint_auth_methods_supported
            .as_ref()
            .is_none_or(|methods| methods.iter().any(|supported| supported == method))
    }
}

/// The one field this module reads from a token response.
#[derive(Deserialize)]
struct TokenResponse {
    id_token: Zeroizing<String>,
}

/// An endpoint this module reads out of a discovery document.
///
/// Naming the endpoints in a type rather than by string keeps
/// [`DiscoveredEndpoint::reserved_params`] exhaustive: an endpoint added later
/// does not compile until it declares which parameters it reserves, instead of
/// silently reserving none.
#[derive(Clone, Copy)]
enum DiscoveredEndpoint {
    Authorization,
    Token,
    Jwks,
}

impl DiscoveredEndpoint {
    /// Returns the discovery document field this endpoint is read from.
    fn label(self) -> &'static str {
        match self {
            Self::Authorization => "authorization_endpoint",
            Self::Token => "token_endpoint",
            Self::Jwks => "jwks_uri",
        }
    }

    /// Returns the parameters this module reserves for its request to the
    /// endpoint, which a discovery document therefore may not pre-set.
    fn reserved_params(self) -> &'static [&'static str] {
        match self {
            Self::Authorization => RESERVED_PROVIDER_AUTH_PARAMS,
            Self::Token => RESERVED_TOKEN_PARAMS,
            // The JWKS endpoint is fetched with no query of this module's own.
            Self::Jwks => &[],
        }
    }
}

/// Builds the well-known discovery URL for a configured issuer.
fn discovery_url(issuer: &str) -> Result<EndpointUrl<Configured>, OidcProviderClientError> {
    EndpointUrl::<Configured>::parse(&format!(
        "{}/.well-known/openid-configuration",
        issuer.trim_end_matches('/')
    ))
    .map_err(|_error| OidcProviderClientError::Discovery)
}

/// Validates one endpoint out of a discovery document.
///
/// Beyond the transport policy [`Discovered`] applies, the endpoint may not
/// arrive with surrounding whitespace, nor carry a query parameter this module
/// reserves for the request to it.
fn discovered_endpoint(
    endpoint: DiscoveredEndpoint,
    value: &str,
    issuer: &EndpointUrl<Configured>,
) -> Result<EndpointUrl<Discovered>, OidcProviderClientError> {
    let label = endpoint.label();
    if value.trim() != value {
        return Err(OidcProviderClientError::InvalidEndpoint(label));
    }
    let url = EndpointUrl::<Discovered>::parse(value, issuer)
        .map_err(|_error| OidcProviderClientError::InvalidEndpoint(label))?;
    let reserved = url.as_url().query_pairs().any(|(key, _value)| {
        endpoint
            .reserved_params()
            .iter()
            .any(|reserved| key.eq_ignore_ascii_case(reserved))
    });
    (!reserved)
        .then_some(url)
        .ok_or(OidcProviderClientError::InvalidEndpoint(label))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde_json::{Value, json};
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
    use ring::signature::{ECDSA_P256_SHA256_FIXED_SIGNING, EcdsaKeyPair};
    use std::path::Path;

    use super::*;
    use crate::auth::config::AuthSettings;
    use crate::auth::id_token::tests::{claims as id_token_claims, rsa_key, token as id_token};

    const CLIENT_SECRET: &str = "client-secret-must-not-leak";

    fn provider(issuer: &str) -> ResolvedOidcProvider {
        provider_with(issuer, "")
    }

    fn provider_with(issuer: &str, extra: &str) -> ResolvedOidcProvider {
        let settings = AuthSettings::from_toml(&format!(
            "[auth]
             [auth.session]
             [auth.authorization_server]
             issuer = 'http://localhost'
             [auth.provider]
             issuer = '{issuer}'
             client_id = 'provider-client'
             client_secret = '{CLIENT_SECRET}'
             redirect_uri = 'http://localhost/auth/oidc/callback'
             scopes = ['openid', 'email']
             {extra}
             [auth.provider.auth_params]
             prompt = 'login'"
        ))
        .expect("valid auth config")
        .expect("auth settings");
        // A resolved provider is only reachable through runtime resolution, so
        // the helper supplies the session signing key the same way a running
        // server would.
        let signing_key = BASE64_STANDARD.encode(
            EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &SystemRandom::new())
                .expect("P-256 signing key"),
        );
        let (settings, _issuer) = settings
            .resolve_runtime_dependencies(Path::new("config.toml"), &|name| {
                Ok((name == "CORAL_SESSION_SIGNING_KEY").then(|| signing_key.clone()))
            })
            .expect("resolved runtime dependencies");
        settings.provider().clone()
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

    #[test]
    fn remote_issuer_cannot_discover_loopback_http_endpoints() {
        let issuer = EndpointUrl::<Configured>::parse("https://accounts.example.test/tenant")
            .expect("issuer");
        for (endpoint, value) in [
            (
                DiscoveredEndpoint::Authorization,
                "http://127.0.0.1:9000/authorize",
            ),
            (DiscoveredEndpoint::Token, "http://127.0.0.1:9000/token"),
            (DiscoveredEndpoint::Jwks, "http://127.0.0.1:9000/jwks"),
        ] {
            assert_eq!(
                discovered_endpoint(endpoint, value, &issuer).expect_err("loopback downgrade"),
                OidcProviderClientError::InvalidEndpoint(endpoint.label())
            );
        }
    }

    fn validation_exchange(server: &MockServer) -> OidcCodeExchange {
        let issuer = EndpointUrl::<Configured>::parse(&server.uri()).expect("issuer URL");
        OidcCodeExchange {
            id_token: Zeroizing::new(id_token(&id_token_claims())),
            jwks_uri: EndpointUrl::<Discovered>::parse(
                &format!("{}/jwks?source=url-secret", server.uri()),
                &issuer,
            )
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
        let state = request.state.as_str();
        let nonce = request.nonce.as_str();
        let code_verifier = request.code_verifier.as_str();
        for value in [state, nonce, code_verifier] {
            assert_eq!(value.len(), 43);
            assert!(
                value
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
            );
        }
        assert_ne!(state, nonce);
        assert_ne!(nonce, code_verifier);
        assert_ne!(state, code_verifier);
        let query = request
            .url
            .query_pairs()
            .into_owned()
            .collect::<BTreeMap<_, _>>();
        for (key, expected) in [
            ("tenant", "one"),
            ("response_type", "code"),
            ("client_id", "provider-client"),
            ("redirect_uri", "http://localhost/auth/oidc/callback"),
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

    async fn discovery_requests(server: &MockServer) -> usize {
        server
            .received_requests()
            .await
            .expect("requests")
            .iter()
            .filter(|request| {
                request
                    .url
                    .path()
                    .ends_with("/.well-known/openid-configuration")
            })
            .count()
    }

    async fn mount_valid_discovery(server: &MockServer, issuer: &str) {
        mount_discovery(
            server,
            "/tenant/.well-known/openid-configuration",
            valid_document(server, issuer).to_string(),
        )
        .await;
    }

    fn validated_discovery() -> ValidatedDiscovery {
        let issuer =
            EndpointUrl::<Configured>::parse("https://accounts.example.test").expect("issuer");
        let endpoint = |label, path: &str| {
            discovered_endpoint(
                label,
                &format!("https://accounts.example.test{path}"),
                &issuer,
            )
            .expect(label)
        };
        ValidatedDiscovery {
            authorization_endpoint: endpoint("authorization_endpoint", "/authorize"),
            token_endpoint: endpoint("token_endpoint", "/token"),
            jwks_uri: endpoint("jwks_uri", "/jwks"),
            signing_algorithms: vec!["RS256".to_string()],
        }
    }

    #[tokio::test]
    async fn discovery_is_reused_across_authorization_requests() {
        let server = MockServer::start().await;
        let issuer = format!("{}/tenant/", server.uri());
        mount_valid_discovery(&server, &issuer).await;
        let client = OidcProviderClient::new().expect("client");
        let provider = provider(&issuer);

        for _ in 0..3 {
            client
                .authorization_request(&provider)
                .await
                .expect("authorization request");
        }
        assert_eq!(discovery_requests(&server).await, 1);
    }

    /// Expiry is checked against the cache directly: `start_paused` auto-advances
    /// while a real socket read is pending, which would trip the client's own
    /// request timeout in an end-to-end test.
    #[tokio::test(start_paused = true)]
    async fn cached_discovery_is_dropped_once_its_ttl_expires() {
        let cache = DiscoveryCache::default();
        let issuer = "https://accounts.example.test";
        let ttl = Duration::from_mins(5);
        cache.insert(issuer, &validated_discovery(), ttl).await;
        assert!(cache.get("https://other.example.test").await.is_none());

        tokio::time::advance(Duration::from_mins(4)).await;
        assert!(cache.get(issuer).await.is_some(), "expired before its TTL");

        tokio::time::advance(Duration::from_mins(2)).await;
        assert!(cache.get(issuer).await.is_none(), "outlived its TTL");
    }

    /// A cached failure would turn a provider blip into an outage lasting a
    /// whole TTL, so only successes are stored.
    #[tokio::test]
    async fn discovery_failures_are_never_cached() {
        let server = MockServer::start().await;
        let issuer = format!("{}/tenant/", server.uri());
        Mock::given(method("GET"))
            .and(path("/tenant/.well-known/openid-configuration"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;
        let client = OidcProviderClient::new().expect("client");
        let provider = provider(&issuer);
        let Err(error) = client.authorization_request(&provider).await else {
            panic!("expected the provider outage to fail discovery");
        };
        assert_eq!(error, OidcProviderClientError::Discovery);

        server.reset().await;
        mount_valid_discovery(&server, &issuer).await;
        client
            .authorization_request(&provider)
            .await
            .expect("recovers without waiting out a TTL");
    }

    /// The cache exists so that a burst of authorization requests cannot be
    /// amplified into a burst of provider requests.
    #[tokio::test]
    async fn concurrent_discovery_misses_collapse_into_one_request() {
        let server = MockServer::start().await;
        let issuer = format!("{}/tenant/", server.uri());
        mount_valid_discovery(&server, &issuer).await;
        let client = OidcProviderClient::new().expect("client");
        let provider = provider(&issuer);

        let requests = (0..16)
            .map(|_index| {
                let client = client.clone();
                let provider = provider.clone();
                tokio::spawn(async move { client.authorization_request(&provider).await })
            })
            .collect::<Vec<_>>();
        for request in requests {
            request
                .await
                .expect("join")
                .expect("concurrent authorization request");
        }
        assert_eq!(discovery_requests(&server).await, 1);
    }

    #[tokio::test]
    async fn zero_discovery_cache_ttl_disables_reuse() {
        let server = MockServer::start().await;
        let issuer = format!("{}/tenant/", server.uri());
        mount_valid_discovery(&server, &issuer).await;
        let client = OidcProviderClient::new().expect("client");
        let provider = provider_with(&issuer, "discovery_cache_ttl_seconds = 0");

        for _ in 0..2 {
            client
                .authorization_request(&provider)
                .await
                .expect("authorization request");
        }
        assert_eq!(discovery_requests(&server).await, 2);
    }

    #[tokio::test]
    async fn discovery_rejects_mismatched_issuer_and_untrusted_endpoints() {
        for (field, invalid) in [
            ("issuer", "http://127.0.0.1:1/"),
            ("authorization_endpoint", "http://remote.test/authorize"),
            ("token_endpoint", "https://user:pass@remote.test/token"),
            ("jwks_uri", "https://remote.test/jwks#secret"),
            (
                "authorization_endpoint",
                "http://localhost:1/authorize?StAtE=evil",
            ),
            // `form_post` and `fragment` both keep the authorization code away
            // from the GET callback route, so a document that pre-sets either
            // strands every login it starts.
            (
                "authorization_endpoint",
                "http://localhost:1/authorize?response_mode=form_post",
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
    }

    /// HTTPS to a private host is accepted: an issuer reachable over HTTPS is
    /// entitled to name endpoints anywhere, and it is the transport, not the
    /// destination, that is policed.
    #[tokio::test]
    async fn discovery_accepts_private_https_endpoints() {
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
    }

    #[tokio::test]
    async fn discovery_requires_rs256_signing_support() {
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

    /// An advertised list that excludes `client_secret_post` is believed, and
    /// an omitted list is not read as the spec's `client_secret_basic` default.
    #[tokio::test]
    async fn discovery_believes_an_advertised_token_endpoint_auth_method() {
        for (methods, expected) in [
            (Some(json!(["client_secret_basic"])), false),
            (Some(json!([])), false),
            (
                Some(json!(["client_secret_basic", "client_secret_post"])),
                true,
            ),
            (None, true),
        ] {
            let server = MockServer::start().await;
            let issuer = server.uri();
            let mut document = valid_document(&server, &issuer);
            if let Some(methods) = methods {
                document
                    .as_object_mut()
                    .expect("discovery object")
                    .insert("token_endpoint_auth_methods_supported".into(), methods);
            }
            mount_discovery(
                &server,
                "/.well-known/openid-configuration",
                document.to_string(),
            )
            .await;
            let result = OidcProviderClient::new()
                .expect("client")
                .discover(&provider(&issuer))
                .await;
            if expected {
                result.expect("client_secret_post available");
            } else {
                assert_eq!(
                    result.err().expect("client_secret_post unavailable"),
                    OidcProviderClientError::UnsupportedTokenEndpointAuth
                );
            }
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
                (
                    "redirect_uri".into(),
                    "http://localhost/auth/oidc/callback".into(),
                ),
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
