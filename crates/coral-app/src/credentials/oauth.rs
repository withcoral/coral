//! OAuth credential authorization and token refresh helpers.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use base64::Engine as _;
use base64::engine::general_purpose::{
    STANDARD as BASE64_STANDARD, URL_SAFE_NO_PAD as BASE64_URL_SAFE_NO_PAD,
};
use chrono::{DateTime, Utc};
use coral_spec::{
    ManifestOAuthClientSecretTransport, ManifestOAuthCredentialSpec,
    ManifestOAuthDynamicClientRegistrationAuthMethod, ManifestOAuthEndpointUrls,
    ManifestOAuthFlowKind, ManifestOAuthPkceMode, ManifestOAuthRedirectBindPort,
    ManifestOAuthScopeDelimiter, ParsedTemplate,
};
use reqwest::header::{ACCEPT, AUTHORIZATION};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use url::{Host, Url, form_urlencoded};
use uuid::Uuid;

use crate::bootstrap::AppError;
use crate::credentials::OAUTH_INTERNAL_KEY_PREFIX;

const SESSION_TTL: Duration = Duration::from_mins(10);
const DEVICE_CODE_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_CALLBACK_BYTES: usize = 8 * 1024;
// Refresh just before provider expiry so a token does not age out while Coral
// is preparing or executing the query that needs it.
const REFRESH_EXPIRY_SKEW_SECONDS: i64 = 60;
const TOKEN_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone)]
pub(crate) struct OAuthCredentialService {
    http: OAuthHttpClients,
}

#[derive(Clone)]
struct OAuthHttpClients {
    proxied: reqwest::Client,
    direct: reqwest::Client,
}

impl OAuthHttpClients {
    fn post(&self, endpoint: &ValidatedOAuthEndpoint) -> reqwest::RequestBuilder {
        let client = if endpoint.is_loopback_http() {
            &self.direct
        } else {
            &self.proxied
        };
        client.post(endpoint.request_url())
    }
}

pub(crate) struct StartOAuthCredentialRequest<'a> {
    pub(crate) input_key: &'a str,
    pub(crate) oauth: &'a ManifestOAuthCredentialSpec,
    pub(crate) source_inputs: &'a BTreeMap<String, String>,
    pub(crate) credential_inputs: Vec<(String, String)>,
}

pub(super) struct RefreshOAuthCredentialRequest<'a> {
    access_token_material_key: &'a str,
    metadata_prefix: String,
    oauth: &'a ManifestOAuthCredentialSpec,
}

impl<'a> RefreshOAuthCredentialRequest<'a> {
    pub(super) fn for_source_input(
        input_key: &'a str,
        oauth: &'a ManifestOAuthCredentialSpec,
    ) -> Self {
        Self {
            access_token_material_key: input_key,
            metadata_prefix: oauth_metadata_prefix(input_key),
            oauth,
        }
    }
}

pub(crate) struct OAuthAuthorization {
    pub(crate) authorization_url: String,
    pub(crate) expires_in_seconds: u64,
    pub(crate) user_code: Option<String>,
    pub(crate) verification_uri: Option<String>,
    pub(crate) verification_uri_complete: Option<String>,
}

#[derive(Clone)]
pub(crate) struct OAuthCredentialMaterial {
    pub(crate) input_key: String,
    pub(crate) access_token: String,
    pub(crate) internal_metadata: BTreeMap<String, String>,
    pub(crate) safe_metadata: BTreeMap<String, String>,
}

struct OAuthSessionCommon {
    input_key: String,
    endpoints: ValidatedOAuthEndpoints,
    client: ResolvedOAuthClient,
    resource: Option<String>,
}

struct AuthorizationCodeSessionConfig {
    common: OAuthSessionCommon,
    state: String,
    code_verifier: Option<String>,
    // Request path accepted by the local callback listener.
    callback_path: String,
    // Exact redirect_uri value sent to the provider for authorization and token exchange.
    provider_redirect_uri: String,
    listener: TcpListener,
    expires_at: Instant,
}

struct DeviceCodeSessionConfig {
    common: OAuthSessionCommon,
    device_code: String,
    interval: Duration,
    expires_in: Duration,
}

struct Callback {
    code: String,
}

enum CallbackConnectionResult {
    Callback(Callback),
    Ignored,
}

enum CallbackRequestResult {
    Callback(Callback),
    Ignored {
        status: &'static str,
        message: &'static str,
    },
}

struct DeviceAuthorizationResponse {
    device_code: String,
    user_code: String,
    verification_uri: String,
    verification_uri_complete: Option<String>,
    expires_in: Duration,
    interval: Duration,
}

struct TokenResponse {
    access_token: String,
    refresh_token: Option<String>,
    token_type: Option<String>,
    scope: Option<String>,
    expires_at: Option<DateTime<Utc>>,
}

struct ResolvedOAuthClient {
    client_id: String,
    client_secret: Option<String>,
    client_secret_transport: Option<ManifestOAuthClientSecretTransport>,
    dynamic_client_registration: bool,
}

struct DynamicClientRegistrationResponse {
    id: String,
    secret: Option<String>,
    secret_transport: Option<ManifestOAuthClientSecretTransport>,
}

struct OAuthRefreshConfig {
    token_url: ValidatedOAuthEndpoint,
    client_id: String,
    client_secret: Option<String>,
    client_secret_transport: Option<ManifestOAuthClientSecretTransport>,
    refresh_token: String,
    resource: Option<String>,
}

struct OAuthAuthorizationRequest {
    input_key: String,
    oauth: ManifestOAuthCredentialSpec,
    endpoints: ValidatedOAuthEndpoints,
    credential_inputs: BTreeMap<String, String>,
    resource: Option<String>,
}

#[derive(Clone)]
struct ValidatedOAuthEndpoint(Url);

#[derive(Clone)]
struct ValidatedOAuthEndpoints {
    authorization: Option<ValidatedOAuthEndpoint>,
    device_authorization: Option<ValidatedOAuthEndpoint>,
    token: ValidatedOAuthEndpoint,
    registration: Option<ValidatedOAuthEndpoint>,
}

#[derive(Clone, Copy)]
enum OAuthEndpointErrorKind {
    InvalidInput,
    FailedPrecondition,
}

impl ValidatedOAuthEndpoint {
    fn authored(value: &str, label: &'static str) -> Result<Self, AppError> {
        Self::parse(value, label, OAuthEndpointErrorKind::InvalidInput)
    }

    fn untrusted(value: &str, label: &'static str) -> Result<Self, AppError> {
        Self::parse(value, label, OAuthEndpointErrorKind::FailedPrecondition)
    }

    fn parse(
        value: &str,
        label: &'static str,
        error_kind: OAuthEndpointErrorKind,
    ) -> Result<Self, AppError> {
        let error = |reason: &str| {
            let message = format!("OAuth {label} {reason}");
            match error_kind {
                OAuthEndpointErrorKind::InvalidInput => AppError::InvalidInput(message),
                OAuthEndpointErrorKind::FailedPrecondition => AppError::FailedPrecondition(message),
            }
        };
        let url = Url::parse(value).map_err(|_error| error("is invalid"))?;
        if !url.username().is_empty() || url.password().is_some() || oauth_url_has_userinfo(value) {
            return Err(error("must not include user information"));
        }
        if url.fragment().is_some() {
            return Err(error("must not include a fragment"));
        }
        let host = url.host().ok_or_else(|| error("must include a host"))?;
        let transport_is_allowed = match (url.scheme(), host) {
            ("https", _) => true,
            ("http", Host::Domain(host)) => host.eq_ignore_ascii_case("localhost"),
            ("http", Host::Ipv4(address)) => address.is_loopback(),
            ("http", Host::Ipv6(address)) => address.is_loopback(),
            _ => false,
        };
        if !transport_is_allowed {
            return Err(error(
                "must use HTTPS, except that HTTP is allowed for exact localhost or loopback IP hosts",
            ));
        }
        Ok(Self(url))
    }

    fn request_url(&self) -> Url {
        self.0.clone()
    }

    fn is_loopback_http(&self) -> bool {
        self.0.scheme() == "http"
    }

    fn as_str(&self) -> &str {
        self.0.as_str()
    }

    fn into_string(self) -> String {
        self.0.into()
    }
}

fn oauth_url_has_userinfo(value: &str) -> bool {
    value
        .split_once(':')
        .map(|(_scheme, remainder)| {
            remainder.trim_start_matches(|character| {
                matches!(character, '/' | '\\' | '\t' | '\n' | '\r')
            })
        })
        .and_then(|remainder| remainder.split(['/', '\\', '?', '#']).next())
        .is_some_and(|authority| authority.contains('@'))
}

const OAUTH_METADATA_METHOD_VALUE: &str = "oauth";

#[derive(Clone, Copy)]
enum OAuthMetadataKey {
    Method,
    AccessTokenExpiresAt,
    RefreshToken,
    TokenType,
    Scope,
    ClientId,
    TokenUrl,
    Resource,
    ClientSecretTransport,
    ClientSecret,
    DynamicClientRegistration,
}

impl OAuthMetadataKey {
    fn suffix(self) -> &'static str {
        match self {
            Self::Method => "method",
            Self::AccessTokenExpiresAt => "access_token_expires_at",
            Self::RefreshToken => "refresh_token",
            Self::TokenType => "token_type",
            Self::Scope => "scope",
            Self::ClientId => "client_id",
            Self::TokenUrl => "token_url",
            Self::Resource => "resource",
            Self::ClientSecretTransport => "client_secret_transport",
            Self::ClientSecret => "client_secret",
            Self::DynamicClientRegistration => "dynamic_client_registration",
        }
    }

    fn key(self, prefix: &str) -> String {
        format!("{prefix}{}", self.suffix())
    }

    fn get<'a>(self, prefix: &str, metadata: &'a BTreeMap<String, String>) -> Option<&'a str> {
        metadata.get(&self.key(prefix)).map(String::as_str)
    }

    fn insert(
        self,
        prefix: &str,
        metadata: &mut BTreeMap<String, String>,
        value: impl Into<String>,
    ) {
        metadata.insert(self.key(prefix), value.into());
    }

    fn insert_optional(
        self,
        prefix: &str,
        metadata: &mut BTreeMap<String, String>,
        value: Option<&str>,
    ) {
        if let Some(value) = value {
            self.insert(prefix, metadata, value);
        }
    }

    fn remove(self, prefix: &str, metadata: &mut BTreeMap<String, String>) {
        metadata.remove(&self.key(prefix));
    }
}

impl OAuthCredentialService {
    pub(crate) fn new() -> Self {
        Self {
            http: token_http_clients(TOKEN_REQUEST_TIMEOUT, None),
        }
    }

    #[cfg(test)]
    fn with_token_request_timeout(timeout: Duration) -> Self {
        Self {
            http: token_http_clients(timeout, None),
        }
    }

    #[cfg(test)]
    fn with_token_request_proxy(timeout: Duration, proxy: reqwest::Proxy) -> Self {
        Self {
            http: token_http_clients(timeout, Some(proxy)),
        }
    }

    #[cfg(test)]
    async fn authorize<F, Fut>(
        &self,
        request: StartOAuthCredentialRequest<'_>,
        on_authorization: F,
    ) -> Result<OAuthCredentialMaterial, AppError>
    where
        F: FnOnce(OAuthAuthorization) -> Fut,
        Fut: Future<Output = Result<(), AppError>>,
    {
        self.authorize_with_callback(request, on_authorization, || async { Ok(()) })
            .await
    }

    pub(crate) async fn authorize_with_callback<F, Fut, C, CallbackFut>(
        &self,
        request: StartOAuthCredentialRequest<'_>,
        on_authorization: F,
        on_callback_received: C,
    ) -> Result<OAuthCredentialMaterial, AppError>
    where
        F: FnOnce(OAuthAuthorization) -> Fut,
        Fut: Future<Output = Result<(), AppError>>,
        C: FnOnce() -> CallbackFut,
        CallbackFut: Future<Output = Result<(), AppError>>,
    {
        let oauth = request.oauth.clone();
        let endpoints = preflight_oauth_endpoints(
            &oauth,
            &oauth_endpoint_urls(&oauth, request.source_inputs)?,
            request.source_inputs,
        )?;
        let resource = oauth_resource(&oauth, request.source_inputs)?;
        let credential_inputs = normalize_credential_inputs(request.credential_inputs)?;
        reject_unknown_credential_inputs(&oauth, &credential_inputs)?;
        match oauth.flow.kind {
            ManifestOAuthFlowKind::AuthorizationCode => {
                self.authorize_authorization_code(
                    OAuthAuthorizationRequest {
                        input_key: request.input_key.to_string(),
                        oauth,
                        endpoints,
                        credential_inputs,
                        resource,
                    },
                    on_authorization,
                    on_callback_received,
                )
                .await
            }
            ManifestOAuthFlowKind::DeviceCode => {
                self.authorize_device_code(
                    OAuthAuthorizationRequest {
                        input_key: request.input_key.to_string(),
                        oauth,
                        endpoints,
                        credential_inputs,
                        resource,
                    },
                    on_authorization,
                )
                .await
            }
        }
    }

    async fn authorize_authorization_code<F, Fut, C, CallbackFut>(
        &self,
        request: OAuthAuthorizationRequest,
        on_authorization: F,
        on_callback_received: C,
    ) -> Result<OAuthCredentialMaterial, AppError>
    where
        F: FnOnce(OAuthAuthorization) -> Fut,
        Fut: Future<Output = Result<(), AppError>>,
        C: FnOnce() -> CallbackFut,
        CallbackFut: Future<Output = Result<(), AppError>>,
    {
        let OAuthAuthorizationRequest {
            input_key,
            oauth,
            endpoints,
            credential_inputs,
            resource,
        } = request;
        let (listener, callback_path, provider_redirect_uri) =
            bind_redirect_listener(&oauth).await?;
        let client = resolve_oauth_client(
            &self.http,
            &oauth,
            endpoints.registration.as_ref(),
            &credential_inputs,
            Some(&provider_redirect_uri),
        )
        .await?;
        let state = random_token();
        let code_verifier = pkce_code_verifier(&oauth);
        let authorization_url = build_authorization_url(
            &oauth,
            &endpoints,
            &provider_redirect_uri,
            &client.client_id,
            &state,
            code_verifier.as_deref(),
            resource.as_deref(),
        )?;
        let expires_at = Instant::now() + SESSION_TTL;
        let common = OAuthSessionCommon {
            input_key,
            endpoints,
            client,
            resource,
        };
        let session = AuthorizationCodeSessionConfig {
            common,
            state,
            code_verifier,
            callback_path,
            provider_redirect_uri,
            listener,
            expires_at,
        };
        on_authorization(OAuthAuthorization {
            authorization_url,
            expires_in_seconds: SESSION_TTL.as_secs(),
            user_code: None,
            verification_uri: None,
            verification_uri_complete: None,
        })
        .await?;
        self.run_authorization_code_session(session, on_callback_received)
            .await
    }

    pub(crate) fn validate_credential_inputs(
        oauth: &ManifestOAuthCredentialSpec,
        source_inputs: &BTreeMap<String, String>,
        credential_inputs: Vec<(String, String)>,
    ) -> Result<(), AppError> {
        let endpoints = preflight_oauth_endpoints(
            oauth,
            &oauth_endpoint_urls(oauth, source_inputs)?,
            source_inputs,
        )?;
        let _resource = oauth_resource(oauth, source_inputs)?;
        let credential_inputs = normalize_credential_inputs(credential_inputs)?;
        reject_unknown_credential_inputs(oauth, &credential_inputs)?;
        validate_oauth_client_inputs(oauth, &credential_inputs)?;
        match oauth.flow.kind {
            ManifestOAuthFlowKind::AuthorizationCode => {
                oauth
                    .redirect_bind_port()
                    .map_err(|error| AppError::InvalidInput(error.to_string()))?;
                endpoints.authorization.as_ref().ok_or_else(|| {
                    AppError::InvalidInput(
                        "authorization_code OAuth method is missing authorization_url".to_string(),
                    )
                })?;
            }
            ManifestOAuthFlowKind::DeviceCode => {
                if oauth.client.secret.is_some() {
                    return Err(AppError::InvalidInput(
                        "device_code OAuth methods must not declare a client secret".to_string(),
                    ));
                }
                endpoints.device_authorization.as_ref().ok_or_else(|| {
                    AppError::InvalidInput(
                        "device_code OAuth method is missing device_authorization_url".to_string(),
                    )
                })?;
            }
        }
        Ok(())
    }

    /// Uses persisted credential material as both the refresh input
    /// (expiry, refresh token, client metadata) and output (new token values).
    pub(super) async fn refresh_if_needed(
        &self,
        request: RefreshOAuthCredentialRequest<'_>,
        credential_material: &mut BTreeMap<String, String>,
    ) -> Result<bool, AppError> {
        let Some(refresh) = oauth_refresh_config(
            request.access_token_material_key,
            request.metadata_prefix.as_str(),
            request.oauth,
            credential_material,
        )?
        else {
            return Ok(false);
        };
        let token = refresh_access_token(&self.http, &refresh).await?;
        apply_refreshed_token(
            request.access_token_material_key,
            request.metadata_prefix.as_str(),
            credential_material,
            &token,
        );
        Ok(true)
    }

    async fn authorize_device_code<F, Fut>(
        &self,
        request: OAuthAuthorizationRequest,
        on_authorization: F,
    ) -> Result<OAuthCredentialMaterial, AppError>
    where
        F: FnOnce(OAuthAuthorization) -> Fut,
        Fut: Future<Output = Result<(), AppError>>,
    {
        let OAuthAuthorizationRequest {
            input_key,
            oauth,
            endpoints,
            credential_inputs,
            resource,
        } = request;
        let client = resolve_oauth_client(
            &self.http,
            &oauth,
            endpoints.registration.as_ref(),
            &credential_inputs,
            None,
        )
        .await?;
        let device = request_device_code(
            &self.http,
            &oauth,
            &endpoints,
            &client,
            resource.as_deref(),
            DEVICE_CODE_REQUEST_TIMEOUT,
        )
        .await?;
        let authorization_url = device
            .verification_uri_complete
            .clone()
            .unwrap_or_else(|| device.verification_uri.clone());
        let user_code = device.user_code.clone();
        let verification_uri = device.verification_uri.clone();
        let verification_uri_complete = device.verification_uri_complete.clone();
        let expires_in = device.expires_in;
        let common = OAuthSessionCommon {
            input_key,
            endpoints,
            client,
            resource,
        };
        let session = DeviceCodeSessionConfig {
            common,
            device_code: device.device_code,
            interval: device.interval,
            expires_in,
        };
        on_authorization(OAuthAuthorization {
            authorization_url,
            expires_in_seconds: expires_in.as_secs(),
            user_code: Some(user_code),
            verification_uri: Some(verification_uri),
            verification_uri_complete,
        })
        .await?;
        self.run_device_code_session(session).await
    }

    async fn run_authorization_code_session<C, CallbackFut>(
        &self,
        session: AuthorizationCodeSessionConfig,
        on_callback_received: C,
    ) -> Result<OAuthCredentialMaterial, AppError>
    where
        C: FnOnce() -> CallbackFut,
        CallbackFut: Future<Output = Result<(), AppError>>,
    {
        let deadline = tokio::time::Instant::from_std(session.expires_at);
        let callback = tokio::time::timeout_at(deadline, receive_callback(&session))
            .await
            .map_err(|_elapsed| expired_session_error(&session.common.input_key))??;
        on_callback_received().await?;
        let token = tokio::time::timeout_at(
            deadline,
            exchange_authorization_code(&self.http, &session, &callback.code),
        )
        .await
        .map_err(|_elapsed| expired_session_error(&session.common.input_key))??;
        Ok(oauth_credential_material(&session.common, &token))
    }

    async fn run_device_code_session(
        &self,
        session: DeviceCodeSessionConfig,
    ) -> Result<OAuthCredentialMaterial, AppError> {
        let token =
            tokio::time::timeout(session.expires_in, poll_device_token(&self.http, &session))
                .await
                .map_err(|_elapsed| expired_session_error(&session.common.input_key))??;
        Ok(oauth_credential_material(&session.common, &token))
    }
}

fn token_http_client_builder(timeout: Duration) -> reqwest::ClientBuilder {
    reqwest::Client::builder()
        .timeout(timeout)
        .redirect(reqwest::redirect::Policy::none())
}

fn token_http_clients(timeout: Duration, proxy: Option<reqwest::Proxy>) -> OAuthHttpClients {
    let proxied = if let Some(proxy) = proxy {
        token_http_client_builder(timeout).proxy(proxy)
    } else {
        token_http_client_builder(timeout)
    }
    .build()
    .expect("OAuth proxied HTTP client configuration should be valid");
    let direct = token_http_client_builder(timeout)
        .no_proxy()
        .build()
        .expect("OAuth direct HTTP client configuration should be valid");
    OAuthHttpClients { proxied, direct }
}

fn oauth_endpoint_urls(
    oauth: &ManifestOAuthCredentialSpec,
    source_inputs: &BTreeMap<String, String>,
) -> Result<ManifestOAuthEndpointUrls, AppError> {
    oauth
        .endpoint_urls(source_inputs)
        .map_err(|error| AppError::InvalidInput(error.to_string()))
}

fn preflight_oauth_endpoints(
    oauth: &ManifestOAuthCredentialSpec,
    endpoints: &ManifestOAuthEndpointUrls,
    source_inputs: &BTreeMap<String, String>,
) -> Result<ValidatedOAuthEndpoints, AppError> {
    let authorization_url = endpoints
        .authorization_url
        .as_deref()
        .map(|url| ValidatedOAuthEndpoint::authored(url, "authorization URL"))
        .transpose()?;
    let device_authorization_url = endpoints
        .device_authorization_url
        .as_deref()
        .map(|url| ValidatedOAuthEndpoint::authored(url, "device authorization URL"))
        .transpose()?;
    let token_url = ValidatedOAuthEndpoint::authored(&endpoints.token_url, "token URL")?;
    let registration_url = oauth
        .client
        .dynamic_registration
        .as_ref()
        .map(|registration| {
            registration
                .registration_url(source_inputs)
                .map_err(|error| AppError::InvalidInput(error.to_string()))
                .and_then(|url| ValidatedOAuthEndpoint::authored(&url, "dynamic registration URL"))
        })
        .transpose()?;
    Ok(ValidatedOAuthEndpoints {
        authorization: authorization_url,
        device_authorization: device_authorization_url,
        token: token_url,
        registration: registration_url,
    })
}

fn oauth_resource(
    oauth: &ManifestOAuthCredentialSpec,
    source_inputs: &BTreeMap<String, String>,
) -> Result<Option<String>, AppError> {
    oauth
        .resource(source_inputs)
        .map_err(|error| AppError::InvalidInput(error.to_string()))
}

fn normalize_credential_inputs(
    inputs: Vec<(String, String)>,
) -> Result<BTreeMap<String, String>, AppError> {
    let mut normalized = BTreeMap::new();
    for (key, value) in inputs {
        let key = normalize_credential_input_key(&key)?;
        if normalized
            .insert(key.clone(), value.trim().to_string())
            .is_some()
        {
            return Err(AppError::InvalidInput(format!(
                "credential input '{key}' is repeated"
            )));
        }
    }
    Ok(normalized)
}

fn expired_session_error(input_key: &str) -> AppError {
    AppError::FailedPrecondition(format!(
        "OAuth session for '{input_key}' expired; start a new credential retrieval"
    ))
}

fn normalize_credential_input_key(value: &str) -> Result<String, AppError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(AppError::InvalidInput(
            "missing credential input key".to_string(),
        ));
    }
    if trimmed.contains('/') || trimmed.contains('\\') {
        return Err(AppError::InvalidInput(
            "credential input key must not contain '/' or '\\'".to_string(),
        ));
    }
    if trimmed.contains('=') || trimmed.contains('\n') || trimmed.contains('\r') {
        return Err(AppError::InvalidInput(
            "credential input key must not contain '=', '\\n', or '\\r'".to_string(),
        ));
    }
    if trimmed.starts_with('#') {
        return Err(AppError::InvalidInput(
            "credential input key must not start with '#'".to_string(),
        ));
    }
    Ok(trimmed.to_string())
}

fn reject_unknown_credential_inputs(
    oauth: &ManifestOAuthCredentialSpec,
    inputs: &BTreeMap<String, String>,
) -> Result<(), AppError> {
    let mut expected = BTreeSet::new();
    if let Some(input) = oauth.client.id.input.as_deref() {
        expected.insert(input);
    }
    if let Some(secret) = oauth.client.secret.as_ref() {
        expected.insert(secret.input.as_str());
    }
    for key in inputs.keys() {
        if !expected.contains(key.as_str()) {
            return Err(AppError::InvalidInput(format!(
                "unknown OAuth credential input '{key}'"
            )));
        }
    }
    Ok(())
}

fn validate_oauth_client_inputs(
    oauth: &ManifestOAuthCredentialSpec,
    inputs: &BTreeMap<String, String>,
) -> Result<(), AppError> {
    if maybe_resolve_client_id(oauth, inputs).is_some() {
        let _client_secret = resolve_client_secret(oauth, inputs)?;
        return Ok(());
    }
    if oauth.client.dynamic_registration.is_none() {
        return Err(missing_client_id_error(oauth));
    }
    Ok(())
}

fn maybe_resolve_client_id(
    oauth: &ManifestOAuthCredentialSpec,
    inputs: &BTreeMap<String, String>,
) -> Option<String> {
    if let Some(input_key) = oauth.client.id.input.as_deref()
        && let Some(value) = inputs
            .get(input_key)
            .and_then(|value| trimmed_non_empty(value))
    {
        return Some(value.to_string());
    }
    oauth
        .client
        .id
        .default
        .as_deref()
        .and_then(trimmed_non_empty)
        .map(ToString::to_string)
}

fn missing_client_id_error(oauth: &ManifestOAuthCredentialSpec) -> AppError {
    let detail = oauth
        .client
        .id
        .input
        .as_deref()
        .map_or("client ID".to_string(), |input| {
            format!("client ID input '{input}'")
        });
    AppError::FailedPrecondition(format!("missing OAuth {detail}"))
}

fn resolve_client_secret(
    oauth: &ManifestOAuthCredentialSpec,
    inputs: &BTreeMap<String, String>,
) -> Result<Option<String>, AppError> {
    let Some(secret) = oauth.client.secret.as_ref() else {
        return Ok(None);
    };
    let Some(value) = inputs
        .get(&secret.input)
        .and_then(|value| trimmed_non_empty(value))
    else {
        return Err(AppError::FailedPrecondition(format!(
            "missing OAuth client secret input '{}'",
            secret.input
        )));
    };
    Ok(Some(value.to_string()))
}

async fn resolve_oauth_client(
    http: &OAuthHttpClients,
    oauth: &ManifestOAuthCredentialSpec,
    registration_url: Option<&ValidatedOAuthEndpoint>,
    credential_inputs: &BTreeMap<String, String>,
    redirect_uri: Option<&str>,
) -> Result<ResolvedOAuthClient, AppError> {
    if let Some(client_id) = maybe_resolve_client_id(oauth, credential_inputs) {
        return Ok(ResolvedOAuthClient {
            client_id,
            client_secret: resolve_client_secret(oauth, credential_inputs)?,
            client_secret_transport: oauth.client.secret.as_ref().map(|secret| secret.transport),
            dynamic_client_registration: false,
        });
    }
    let Some(registration) = oauth.client.dynamic_registration.as_ref() else {
        return Err(missing_client_id_error(oauth));
    };
    let registration_url = registration_url.ok_or_else(|| {
        AppError::InvalidInput("OAuth dynamic registration URL was not preflighted".to_string())
    })?;
    let registered =
        register_dynamic_client(http, oauth, registration, registration_url, redirect_uri).await?;
    Ok(ResolvedOAuthClient {
        client_id: registered.id,
        client_secret: registered.secret,
        client_secret_transport: registered.secret_transport,
        dynamic_client_registration: true,
    })
}

async fn bind_redirect_listener(
    oauth: &ManifestOAuthCredentialSpec,
) -> Result<(TcpListener, String, String), AppError> {
    let bind_port = oauth
        .redirect_bind_port()
        .map_err(|error| AppError::InvalidInput(error.to_string()))?;
    let redirect_uri_value = oauth.redirect_uri.as_deref().ok_or_else(|| {
        AppError::InvalidInput(
            "authorization_code OAuth method is missing redirect_uri".to_string(),
        )
    })?;
    let redirect_uri = Url::parse(redirect_uri_value)
        .map_err(|error| AppError::InvalidInput(format!("invalid OAuth redirect URI: {error}")))?;
    let host = redirect_uri
        .host()
        .ok_or_else(|| AppError::InvalidInput("OAuth redirect URI is missing host".to_string()))?;
    let host_label = host.to_string();
    let port = match bind_port {
        ManifestOAuthRedirectBindPort::Fixed(port) => port,
        ManifestOAuthRedirectBindPort::Random => {
            // Binding port 0 asks the OS to assign a free loopback port.
            0
        }
    };
    let listener = match host {
        Host::Domain(domain) => TcpListener::bind((domain, port)).await,
        Host::Ipv4(address) => TcpListener::bind((address, port)).await,
        Host::Ipv6(address) => TcpListener::bind((address, port)).await,
    }
    .map_err(|error| {
        let port_label = if port == 0 {
            "a random port".to_string()
        } else {
            port.to_string()
        };
        AppError::FailedPrecondition(format!(
            "OAuth callback listener could not bind {host_label}:{port_label}: {error}"
        ))
    })?;
    let mut effective_redirect_uri = redirect_uri;
    if bind_port == ManifestOAuthRedirectBindPort::Random {
        let assigned_port = listener.local_addr()?.port();
        effective_redirect_uri
            .set_port(Some(assigned_port))
            .map_err(|()| {
                AppError::InvalidInput("OAuth redirect URI port is invalid".to_string())
            })?;
    }
    let provider_redirect_uri = match bind_port {
        ManifestOAuthRedirectBindPort::Fixed(_) => redirect_uri_value.to_string(),
        ManifestOAuthRedirectBindPort::Random => effective_redirect_uri.to_string(),
    };
    let callback_path = effective_redirect_uri.path().to_string();
    Ok((listener, callback_path, provider_redirect_uri))
}

fn build_authorization_url(
    oauth: &ManifestOAuthCredentialSpec,
    endpoints: &ValidatedOAuthEndpoints,
    provider_redirect_uri: &str,
    client_id: &str,
    state: &str,
    code_verifier: Option<&str>,
    resource: Option<&str>,
) -> Result<String, AppError> {
    let authorization_url = endpoints.authorization.as_ref().ok_or_else(|| {
        AppError::InvalidInput(
            "authorization_code OAuth method is missing authorization_url".to_string(),
        )
    })?;
    let mut url = authorization_url.request_url();
    {
        let mut query = url.query_pairs_mut();
        query
            .append_pair("response_type", "code")
            .append_pair("client_id", client_id)
            .append_pair("redirect_uri", provider_redirect_uri)
            .append_pair("state", state);
        if let Some(scopes) = oauth.scopes.as_ref() {
            query.append_pair(
                "scope",
                &join_scope_values(scopes.scope.delimiter, &scopes.scope.values),
            );
        }
        if let Some(resource) = resource {
            query.append_pair("resource", resource);
        }
        if let Some(verifier) = code_verifier {
            query
                .append_pair("code_challenge", &pkce_challenge(verifier))
                .append_pair("code_challenge_method", "S256");
        }
    }
    Ok(url.to_string())
}

async fn register_dynamic_client(
    http: &OAuthHttpClients,
    oauth: &ManifestOAuthCredentialSpec,
    registration: &coral_spec::ManifestOAuthDynamicClientRegistrationSpec,
    registration_url: &ValidatedOAuthEndpoint,
    redirect_uri: Option<&str>,
) -> Result<DynamicClientRegistrationResponse, AppError> {
    let mut payload = serde_json::Map::new();
    payload.insert(
        "client_name".to_string(),
        json!(registration.client_name.as_deref().unwrap_or("Coral")),
    );
    // Coral always registers as a native client: it drives OAuth through loopback
    // redirects (authorization_code) and device-code, both native-app flows. The
    // RFC 7591 / OIDC default is "web", so send "native" explicitly.
    payload.insert("application_type".to_string(), json!("native"));
    payload.insert(
        "token_endpoint_auth_method".to_string(),
        json!(registration.token_endpoint_auth_method.label()),
    );
    match oauth.flow.kind {
        ManifestOAuthFlowKind::AuthorizationCode => {
            let redirect_uri = redirect_uri.ok_or_else(|| {
                AppError::InvalidInput(
                    "authorization_code OAuth dynamic client registration requires redirect_uri"
                        .to_string(),
                )
            })?;
            payload.insert("redirect_uris".to_string(), json!([redirect_uri]));
            payload.insert(
                "grant_types".to_string(),
                json!(dynamic_client_registration_grant_types(
                    "authorization_code",
                    registration.request_refresh_token_grant
                )),
            );
            payload.insert("response_types".to_string(), json!(["code"]));
        }
        ManifestOAuthFlowKind::DeviceCode => {
            payload.insert(
                "grant_types".to_string(),
                json!(dynamic_client_registration_grant_types(
                    "urn:ietf:params:oauth:grant-type:device_code",
                    registration.request_refresh_token_grant
                )),
            );
        }
    }
    if let Some(scopes) = oauth.scopes.as_ref() {
        payload.insert(
            "scope".to_string(),
            json!(join_dynamic_client_registration_scope_values(
                &scopes.scope.values
            )),
        );
    }

    let request = http
        .post(registration_url)
        .header(ACCEPT, "application/json")
        .json(&payload);
    let response = request.send().await.map_err(|error| {
        AppError::FailedPrecondition(format!(
            "OAuth dynamic client registration request failed: {error}"
        ))
    })?;
    let status = response.status();
    let body = response.text().await.map_err(|error| {
        AppError::FailedPrecondition(format!(
            "OAuth dynamic client registration response failed: {error}"
        ))
    })?;
    if !status.is_success() {
        return Err(AppError::FailedPrecondition(format!(
            "OAuth dynamic client registration failed with HTTP {status}: {}",
            truncate_detail(&body)
        )));
    }
    parse_dynamic_client_registration_response(&body, registration.token_endpoint_auth_method)
}

fn dynamic_client_registration_grant_types(
    primary_grant: &'static str,
    request_refresh_token_grant: bool,
) -> Vec<&'static str> {
    let mut grant_types = vec![primary_grant];
    if request_refresh_token_grant {
        grant_types.push("refresh_token");
    }
    grant_types
}

fn parse_dynamic_client_registration_response(
    body: &str,
    requested_auth_method: ManifestOAuthDynamicClientRegistrationAuthMethod,
) -> Result<DynamicClientRegistrationResponse, AppError> {
    let body: Value = serde_json::from_str(body).map_err(|error| {
        AppError::FailedPrecondition(format!(
            "OAuth dynamic client registration response was not JSON: {error}"
        ))
    })?;
    if let Some(message) = oauth_error_message(&body) {
        return Err(AppError::FailedPrecondition(format!(
            "OAuth dynamic client registration returned error: {message}"
        )));
    }
    let client_id = json_string_field(&body, "client_id")?.to_string();
    let client_secret = body
        .get("client_secret")
        .and_then(Value::as_str)
        .and_then(trimmed_non_empty)
        .map(ToString::to_string);
    let response_auth_method =
        parse_dynamic_client_registration_auth_method(&body, requested_auth_method)?;
    let client_secret = match response_auth_method {
        ManifestOAuthDynamicClientRegistrationAuthMethod::None => None,
        ManifestOAuthDynamicClientRegistrationAuthMethod::ClientSecretBasic
        | ManifestOAuthDynamicClientRegistrationAuthMethod::ClientSecretPost => client_secret,
    };
    let client_secret_transport =
        dynamic_client_auth_method_transport(response_auth_method, client_secret.as_deref())?;
    Ok(DynamicClientRegistrationResponse {
        id: client_id,
        secret: client_secret,
        secret_transport: client_secret_transport,
    })
}

fn parse_dynamic_client_registration_auth_method(
    body: &Value,
    requested_auth_method: ManifestOAuthDynamicClientRegistrationAuthMethod,
) -> Result<ManifestOAuthDynamicClientRegistrationAuthMethod, AppError> {
    let Some(value) = body.get("token_endpoint_auth_method") else {
        return Ok(requested_auth_method);
    };
    let value = value.as_str().and_then(trimmed_non_empty).ok_or_else(|| {
        AppError::FailedPrecondition(
            "OAuth dynamic client registration response token_endpoint_auth_method was not a string"
                .to_string(),
        )
    })?;
    ManifestOAuthDynamicClientRegistrationAuthMethod::from_label(value).ok_or_else(|| {
        AppError::FailedPrecondition(format!(
            "OAuth dynamic client registration response token_endpoint_auth_method is unsupported: {value}"
        ))
    })
}

fn dynamic_client_auth_method_transport(
    method: ManifestOAuthDynamicClientRegistrationAuthMethod,
    client_secret: Option<&str>,
) -> Result<Option<ManifestOAuthClientSecretTransport>, AppError> {
    use ManifestOAuthDynamicClientRegistrationAuthMethod as Method;
    match (method, client_secret.is_some()) {
        (Method::None, _) => Ok(None),
        (Method::ClientSecretBasic, true) => {
            Ok(Some(ManifestOAuthClientSecretTransport::BasicAuth))
        }
        (Method::ClientSecretPost, true) => {
            Ok(Some(ManifestOAuthClientSecretTransport::RequestBody))
        }
        (Method::ClientSecretBasic | Method::ClientSecretPost, false) => {
            Err(AppError::FailedPrecondition(
                "OAuth dynamic client registration selected client-secret authentication but did not return client_secret".to_string(),
            ))
        }
    }
}

fn join_scope_values(delimiter: ManifestOAuthScopeDelimiter, values: &[String]) -> String {
    let separator = match delimiter {
        ManifestOAuthScopeDelimiter::Space => " ",
        ManifestOAuthScopeDelimiter::Comma => ",",
    };
    values.join(separator)
}

fn join_dynamic_client_registration_scope_values(values: &[String]) -> String {
    values.join(" ")
}

fn random_token() -> String {
    format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple())
}

fn pkce_code_verifier(oauth: &ManifestOAuthCredentialSpec) -> Option<String> {
    (oauth.flow.pkce == ManifestOAuthPkceMode::Required).then(random_code_verifier)
}

fn random_code_verifier() -> String {
    format!(
        "{}{}{}",
        Uuid::new_v4().simple(),
        Uuid::new_v4().simple(),
        Uuid::new_v4().simple()
    )
}

fn pkce_challenge(verifier: &str) -> String {
    let digest = Sha256::digest(verifier.as_bytes());
    BASE64_URL_SAFE_NO_PAD.encode(digest)
}

async fn receive_callback(session: &AuthorizationCodeSessionConfig) -> Result<Callback, AppError> {
    let (result_tx, mut result_rx) = mpsc::channel(8);
    let deadline = tokio::time::Instant::from_std(session.expires_at);
    loop {
        tokio::select! {
            accepted = session.listener.accept() => {
                let (mut stream, _peer): (_, SocketAddr) = accepted?;
                let result_tx = result_tx.clone();
                let expected_path = session.callback_path.clone();
                let expected_state = session.state.clone();
                tokio::spawn(async move {
                    let result = handle_callback_connection(
                        &mut stream,
                        &expected_path,
                        &expected_state,
                        deadline,
                    )
                    .await;
                    if result_tx.send(result).await.is_err() {
                        tracing::debug!(
                            "OAuth callback receiver closed before connection result was delivered"
                        );
                    }
                });
            }
            Some(result) = result_rx.recv() => {
                match result? {
                    CallbackConnectionResult::Callback(callback) => return Ok(callback),
                    CallbackConnectionResult::Ignored => {}
                }
            }
        }
    }
}

async fn handle_callback_connection(
    stream: &mut tokio::net::TcpStream,
    expected_path: &str,
    expected_state: &str,
    deadline: tokio::time::Instant,
) -> Result<CallbackConnectionResult, AppError> {
    let request = match tokio::time::timeout_at(deadline, read_callback_http_request(stream)).await
    {
        Ok(Ok(request)) => request,
        Ok(Err(error)) => {
            tracing::debug!(%error, "ignoring unreadable OAuth callback connection");
            return Ok(CallbackConnectionResult::Ignored);
        }
        Err(_elapsed) => return Ok(CallbackConnectionResult::Ignored),
    };
    match parse_callback_request(&request, expected_path, expected_state) {
        Ok(CallbackRequestResult::Callback(callback)) => {
            let page = callback_page(
                "Authorization received. Coral is finishing sign-in in your terminal.",
            );
            write_callback_response(stream, "200 OK", &page).await?;
            Ok(CallbackConnectionResult::Callback(callback))
        }
        Ok(CallbackRequestResult::Ignored { status, message }) => {
            let page = callback_page(message);
            if let Err(error) = write_callback_response(stream, status, &page).await {
                tracing::debug!(%error, "failed to write ignored OAuth callback response");
            }
            Ok(CallbackConnectionResult::Ignored)
        }
        Err(error) => {
            let page = callback_page(&format!("OAuth failed: {error}"));
            write_callback_response(stream, "400 Bad Request", &page).await?;
            Err(error)
        }
    }
}

async fn read_callback_http_request(
    stream: &mut tokio::net::TcpStream,
) -> Result<String, AppError> {
    let mut buffer = Vec::new();
    let mut chunk = [0_u8; 1024];
    loop {
        let read = stream.read(&mut chunk).await?;
        if read == 0 {
            if buffer.is_empty() {
                return Err(AppError::FailedPrecondition(
                    "OAuth callback request was empty".to_string(),
                ));
            }
            break;
        }
        let next_len = buffer.len().checked_add(read).ok_or_else(|| {
            AppError::FailedPrecondition("OAuth callback request exceeded read buffer".to_string())
        })?;
        if next_len > MAX_CALLBACK_BYTES {
            return Err(AppError::FailedPrecondition(
                "OAuth callback request exceeded read buffer".to_string(),
            ));
        }
        let bytes = chunk.get(..read).ok_or_else(|| {
            AppError::FailedPrecondition("OAuth callback request exceeded read buffer".to_string())
        })?;
        buffer.extend_from_slice(bytes);
        if buffer.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
    }
    Ok(String::from_utf8_lossy(&buffer).into_owned())
}

fn parse_callback_request(
    raw: &str,
    expected_path: &str,
    expected_state: &str,
) -> Result<CallbackRequestResult, AppError> {
    let first_line = raw.lines().next().ok_or_else(|| {
        AppError::FailedPrecondition("OAuth callback request was empty".to_string())
    })?;
    let mut parts = first_line.split_whitespace();
    let method = parts.next().unwrap_or_default();
    let target = parts.next().unwrap_or_default();
    if method != "GET" || target.is_empty() {
        return Ok(CallbackRequestResult::Ignored {
            status: "400 Bad Request",
            message: "OAuth callback request ignored.",
        });
    }
    let Ok(callback) = Url::parse(&format!("http://callback.local{target}")) else {
        return Ok(CallbackRequestResult::Ignored {
            status: "400 Bad Request",
            message: "OAuth callback request ignored.",
        });
    };
    if callback.path() != expected_path {
        return Ok(CallbackRequestResult::Ignored {
            status: "404 Not Found",
            message: "OAuth callback request ignored.",
        });
    }
    let params = callback.query_pairs().into_owned().fold(
        BTreeMap::<String, Vec<String>>::new(),
        |mut values, (key, value)| {
            values.entry(key).or_default().push(value);
            values
        },
    );
    if let Some(error) = single_query_param(&params, "error")? {
        let description = single_query_param(&params, "error_description")?.unwrap_or_default();
        let message = if description.is_empty() {
            format!("OAuth provider returned error '{error}'")
        } else {
            format!("OAuth provider returned error '{error}': {description}")
        };
        return Err(AppError::FailedPrecondition(message));
    }
    let state = single_query_param(&params, "state")?.ok_or_else(|| {
        AppError::FailedPrecondition("OAuth callback was missing state".to_string())
    })?;
    if state != expected_state {
        return Err(AppError::FailedPrecondition(
            "OAuth callback state did not match the active session".to_string(),
        ));
    }
    let code = single_query_param(&params, "code")?.ok_or_else(|| {
        AppError::FailedPrecondition("OAuth callback was missing authorization code".to_string())
    })?;
    Ok(CallbackRequestResult::Callback(Callback { code }))
}

fn single_query_param(
    params: &BTreeMap<String, Vec<String>>,
    key: &str,
) -> Result<Option<String>, AppError> {
    let Some(values) = params.get(key) else {
        return Ok(None);
    };
    if values.len() != 1 {
        return Err(AppError::FailedPrecondition(format!(
            "OAuth callback repeated '{key}'"
        )));
    }
    Ok(values.first().cloned())
}

fn callback_page(message: &str) -> String {
    format!(
        "<!doctype html><html><head><meta charset=\"utf-8\"><title>Coral OAuth</title></head><body><p>{}</p></body></html>",
        html_escape(message)
    )
}

fn html_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

async fn write_callback_response(
    stream: &mut tokio::net::TcpStream,
    status: &str,
    body: &str,
) -> Result<(), AppError> {
    let response = format!(
        "HTTP/1.1 {status}\r\ncontent-type: text/html; charset=utf-8\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(response.as_bytes()).await?;
    stream.shutdown().await?;
    Ok(())
}

async fn request_device_code(
    http: &OAuthHttpClients,
    oauth: &ManifestOAuthCredentialSpec,
    endpoints: &ValidatedOAuthEndpoints,
    client: &ResolvedOAuthClient,
    resource: Option<&str>,
    timeout: Duration,
) -> Result<DeviceAuthorizationResponse, AppError> {
    let device_authorization_url = endpoints.device_authorization.as_ref().ok_or_else(|| {
        AppError::InvalidInput(
            "device_code OAuth method is missing device_authorization_url".to_string(),
        )
    })?;
    let mut form = Vec::new();
    if let Some(scopes) = oauth.scopes.as_ref() {
        form.push((
            "scope",
            join_scope_values(scopes.scope.delimiter, &scopes.scope.values),
        ));
    }
    if let Some(resource) = resource {
        form.push(("resource", resource.to_string()));
    }
    let request = async {
        let request = http
            .post(device_authorization_url)
            .header(ACCEPT, "application/json");
        // RFC 8628 applies token-endpoint client-auth rules to device authorization requests.
        let request = apply_oauth_client_auth(
            request,
            &mut form,
            &client.client_id,
            client.client_secret.as_deref(),
            client.client_secret_transport,
        )?;
        let response = request.form(&form).send().await.map_err(|error| {
            AppError::FailedPrecondition(format!("OAuth device code request failed: {error}"))
        })?;
        let status = response.status();
        let body = response.text().await.map_err(|error| {
            AppError::FailedPrecondition(format!("OAuth device code response failed: {error}"))
        })?;
        Ok::<_, AppError>((status, body))
    };
    let (status, body) = tokio::time::timeout(timeout, request)
        .await
        .map_err(|_elapsed| {
            AppError::FailedPrecondition(format!(
                "OAuth device code request timed out after {} seconds",
                timeout.as_secs()
            ))
        })??;
    if !status.is_success() {
        return Err(AppError::FailedPrecondition(format!(
            "OAuth device code request failed with HTTP {status}: {}",
            truncate_detail(&body)
        )));
    }
    parse_device_authorization_response(&body)
}

fn parse_device_authorization_response(
    body: &str,
) -> Result<DeviceAuthorizationResponse, AppError> {
    let body: Value = serde_json::from_str(body).map_err(|error| {
        AppError::FailedPrecondition(format!(
            "OAuth device authorization response was not JSON: {error}"
        ))
    })?;
    if let Some(message) = oauth_error_message(&body) {
        return Err(AppError::FailedPrecondition(format!(
            "OAuth device authorization failed: {message}"
        )));
    }
    let device_code = json_string_field(&body, "device_code")?.to_string();
    let user_code = json_string_field(&body, "user_code")?.to_string();
    let verification_uri = ValidatedOAuthEndpoint::untrusted(
        json_string_field(&body, "verification_uri")
            .or_else(|_| json_string_field(&body, "verification_url"))?,
        "provider verification URL",
    )?
    .into_string();
    let verification_uri_complete = body
        .get("verification_uri_complete")
        .and_then(Value::as_str)
        .and_then(trimmed_non_empty)
        .map(|url| {
            ValidatedOAuthEndpoint::untrusted(url, "provider complete verification URL")
                .map(ValidatedOAuthEndpoint::into_string)
        })
        .transpose()?;
    let expires_in = Duration::from_secs(json_u64_field(&body, "expires_in")?.max(1));
    let interval = Duration::from_secs(
        optional_json_u64_field(&body, "interval")
            .unwrap_or(5)
            .max(1),
    );
    Ok(DeviceAuthorizationResponse {
        device_code,
        user_code,
        verification_uri,
        verification_uri_complete,
        expires_in,
        interval,
    })
}

async fn poll_device_token(
    http: &OAuthHttpClients,
    session: &DeviceCodeSessionConfig,
) -> Result<TokenResponse, AppError> {
    let deadline = Instant::now() + session.expires_in;
    let mut interval = session.interval;
    loop {
        let mut form = vec![
            ("device_code", session.device_code.clone()),
            (
                "grant_type",
                "urn:ietf:params:oauth:grant-type:device_code".to_string(),
            ),
        ];
        if let Some(resource) = session.common.resource.as_deref() {
            form.push(("resource", resource.to_string()));
        }
        let request = http
            .post(&session.common.endpoints.token)
            .header(ACCEPT, "application/json");
        let request = apply_oauth_client_auth(
            request,
            &mut form,
            &session.common.client.client_id,
            session.common.client.client_secret.as_deref(),
            session.common.client.client_secret_transport,
        )?;
        let response = request.form(&form).send().await.map_err(|error| {
            AppError::FailedPrecondition(format!("OAuth device token request failed: {error}"))
        })?;
        let status = response.status();
        let body = response.text().await.map_err(|error| {
            AppError::FailedPrecondition(format!("OAuth device token response failed: {error}"))
        })?;
        let value: Value = serde_json::from_str(&body).map_err(|error| {
            AppError::FailedPrecondition(format!(
                "OAuth device token response was not JSON: {error}"
            ))
        })?;
        if let Some(error) = value.get("error").and_then(Value::as_str) {
            match error {
                "authorization_pending" => {}
                "slow_down" => {
                    interval += Duration::from_secs(5);
                }
                "expired_token" => {
                    return Err(AppError::FailedPrecondition(
                        "OAuth device code expired; rerun `coral source add`".to_string(),
                    ));
                }
                "access_denied" => {
                    return Err(AppError::FailedPrecondition(
                        "OAuth device authorization was denied".to_string(),
                    ));
                }
                _ => {
                    let message = oauth_error_message(&value)
                        .unwrap_or_else(|| format!("OAuth provider returned error '{error}'"));
                    return Err(AppError::FailedPrecondition(format!(
                        "OAuth device token request failed: {message}"
                    )));
                }
            }
        } else {
            if !status.is_success() {
                return Err(AppError::FailedPrecondition(format!(
                    "OAuth device token request failed with HTTP {status}: {}",
                    truncate_detail(&body)
                )));
            }
            return parse_token_response_value(&value);
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(AppError::FailedPrecondition(
                "OAuth device code expired; rerun `coral source add`".to_string(),
            ));
        }
        tokio::time::sleep(interval.min(remaining)).await;
    }
}

async fn exchange_authorization_code(
    http: &OAuthHttpClients,
    session: &AuthorizationCodeSessionConfig,
    code: &str,
) -> Result<TokenResponse, AppError> {
    let mut form = vec![
        ("grant_type", "authorization_code".to_string()),
        ("code", code.to_string()),
        ("redirect_uri", session.provider_redirect_uri.clone()),
    ];
    if let Some(resource) = session.common.resource.as_deref() {
        form.push(("resource", resource.to_string()));
    }
    let mut request = http
        .post(&session.common.endpoints.token)
        .header(ACCEPT, "application/json");
    request = apply_oauth_client_auth(
        request,
        &mut form,
        &session.common.client.client_id,
        session.common.client.client_secret.as_deref(),
        session.common.client.client_secret_transport,
    )?;
    if let Some(verifier) = session.code_verifier.as_deref() {
        form.push(("code_verifier", verifier.to_string()));
    }
    send_token_request(request, form, "OAuth token exchange").await
}

async fn refresh_access_token(
    http: &OAuthHttpClients,
    refresh: &OAuthRefreshConfig,
) -> Result<TokenResponse, AppError> {
    let mut form = vec![
        ("grant_type", "refresh_token".to_string()),
        ("refresh_token", refresh.refresh_token.clone()),
    ];
    if let Some(resource) = refresh.resource.as_deref() {
        form.push(("resource", resource.to_string()));
    }
    let request = http
        .post(&refresh.token_url)
        .header(ACCEPT, "application/json");
    let request = apply_oauth_client_auth(
        request,
        &mut form,
        &refresh.client_id,
        refresh.client_secret.as_deref(),
        refresh.client_secret_transport,
    )?;
    send_token_request(request, form, "OAuth token refresh").await
}

fn apply_oauth_client_auth(
    request: reqwest::RequestBuilder,
    form: &mut Vec<(&'static str, String)>,
    client_id: &str,
    client_secret: Option<&str>,
    secret_transport: Option<ManifestOAuthClientSecretTransport>,
) -> Result<reqwest::RequestBuilder, AppError> {
    match (client_secret, secret_transport) {
        (Some(secret), Some(ManifestOAuthClientSecretTransport::BasicAuth)) => {
            Ok(request.header(AUTHORIZATION, basic_client_authorization(client_id, secret)))
        }
        (Some(secret), Some(ManifestOAuthClientSecretTransport::RequestBody)) => {
            form.push(("client_id", client_id.to_string()));
            form.push(("client_secret", secret.to_string()));
            Ok(request)
        }
        (None, None) => {
            form.push(("client_id", client_id.to_string()));
            Ok(request)
        }
        _ => Err(AppError::FailedPrecondition(
            "OAuth client secret configuration was incomplete".to_string(),
        )),
    }
}

async fn send_token_request(
    request: reqwest::RequestBuilder,
    form: Vec<(&'static str, String)>,
    label: &str,
) -> Result<TokenResponse, AppError> {
    let response = request.form(&form).send().await.map_err(|error| {
        AppError::FailedPrecondition(format!("{label} request failed: {error}"))
    })?;
    let status = response.status();
    let body = response.text().await.map_err(|error| {
        AppError::FailedPrecondition(format!("{label} response failed: {error}"))
    })?;
    if !status.is_success() {
        return Err(AppError::FailedPrecondition(format!(
            "{label} failed with HTTP {status}: {}",
            truncate_detail(&body)
        )));
    }
    parse_token_response(&body)
}

fn basic_client_authorization(client_id: &str, client_secret: &str) -> String {
    let client_id = form_urlencoded::byte_serialize(client_id.as_bytes()).collect::<String>();
    let client_secret =
        form_urlencoded::byte_serialize(client_secret.as_bytes()).collect::<String>();
    let encoded = BASE64_STANDARD.encode(format!("{client_id}:{client_secret}"));
    format!("Basic {encoded}")
}

fn parse_token_response(body: &str) -> Result<TokenResponse, AppError> {
    let body: Value = serde_json::from_str(body).map_err(|error| {
        AppError::FailedPrecondition(format!("OAuth token response was not JSON: {error}"))
    })?;
    parse_token_response_value(&body)
}

fn parse_token_response_value(body: &Value) -> Result<TokenResponse, AppError> {
    if let Some(message) = oauth_error_message(body) {
        return Err(AppError::FailedPrecondition(format!(
            "OAuth token response returned error: {message}"
        )));
    }
    let access_token = body
        .get("access_token")
        .and_then(Value::as_str)
        .and_then(trimmed_non_empty)
        .ok_or_else(|| {
            AppError::FailedPrecondition(
                "OAuth token response did not include access_token".to_string(),
            )
        })?
        .to_string();
    let refresh_token = body
        .get("refresh_token")
        .and_then(Value::as_str)
        .and_then(trimmed_non_empty)
        .map(ToString::to_string);
    let token_type = body
        .get("token_type")
        .and_then(Value::as_str)
        .and_then(trimmed_non_empty)
        .map(ToString::to_string);
    let scope = body
        .get("scope")
        .and_then(Value::as_str)
        .and_then(trimmed_non_empty)
        .map(ToString::to_string);
    let expires_at = body
        .get("expires_in")
        .and_then(|value| value.as_i64().or_else(|| value.as_str()?.parse().ok()))
        .and_then(chrono::Duration::try_seconds)
        .and_then(|duration| Utc::now().checked_add_signed(duration));
    Ok(TokenResponse {
        access_token,
        refresh_token,
        token_type,
        scope,
        expires_at,
    })
}

fn json_string_field<'a>(body: &'a Value, field: &str) -> Result<&'a str, AppError> {
    body.get(field)
        .and_then(Value::as_str)
        .and_then(trimmed_non_empty)
        .ok_or_else(|| {
            AppError::FailedPrecondition(format!("OAuth response did not include {field}"))
        })
}

fn trimmed_non_empty(value: &str) -> Option<&str> {
    let value = value.trim();
    (!value.is_empty()).then_some(value)
}

fn json_u64_field(body: &Value, field: &str) -> Result<u64, AppError> {
    optional_json_u64_field(body, field).ok_or_else(|| {
        AppError::FailedPrecondition(format!("OAuth response did not include {field}"))
    })
}

fn optional_json_u64_field(body: &Value, field: &str) -> Option<u64> {
    body.get(field)
        .and_then(|value| value.as_u64().or_else(|| value.as_str()?.parse().ok()))
}

fn oauth_error_message(body: &Value) -> Option<String> {
    let error = body
        .get("error")
        .and_then(Value::as_str)
        .and_then(trimmed_non_empty)?;
    let description = body
        .get("error_description")
        .or_else(|| body.get("error_description_uri"))
        .and_then(Value::as_str)
        .and_then(trimmed_non_empty);
    Some(match description {
        Some(description) => format!("{error}: {description}"),
        None => error.to_string(),
    })
}

fn oauth_credential_material(
    session: &OAuthSessionCommon,
    token: &TokenResponse,
) -> OAuthCredentialMaterial {
    let prefix = oauth_metadata_prefix(&session.input_key);
    let mut internal_metadata = BTreeMap::new();
    OAuthMetadataKey::Method.insert(&prefix, &mut internal_metadata, OAUTH_METADATA_METHOD_VALUE);
    if let Some(expires_at) = token.expires_at {
        OAuthMetadataKey::AccessTokenExpiresAt.insert(
            &prefix,
            &mut internal_metadata,
            expires_at.to_rfc3339(),
        );
    }
    if let Some(refresh_token) = token.refresh_token.as_deref() {
        OAuthMetadataKey::RefreshToken.insert(&prefix, &mut internal_metadata, refresh_token);
    }
    if let Some(token_type) = token.token_type.as_deref() {
        OAuthMetadataKey::TokenType.insert(&prefix, &mut internal_metadata, token_type);
    }
    if let Some(scope) = token.scope.as_deref() {
        OAuthMetadataKey::Scope.insert(&prefix, &mut internal_metadata, scope);
    }
    OAuthMetadataKey::ClientId.insert(
        &prefix,
        &mut internal_metadata,
        session.client.client_id.clone(),
    );
    OAuthMetadataKey::TokenUrl.insert(
        &prefix,
        &mut internal_metadata,
        session.endpoints.token.as_str(),
    );
    OAuthMetadataKey::Resource.insert_optional(
        &prefix,
        &mut internal_metadata,
        session.resource.as_deref(),
    );
    OAuthMetadataKey::ClientSecretTransport.insert_optional(
        &prefix,
        &mut internal_metadata,
        session
            .client
            .client_secret_transport
            .map(ManifestOAuthClientSecretTransport::label),
    );
    OAuthMetadataKey::ClientSecret.insert_optional(
        &prefix,
        &mut internal_metadata,
        session.client.client_secret.as_deref(),
    );
    if session.client.dynamic_client_registration {
        OAuthMetadataKey::DynamicClientRegistration.insert(&prefix, &mut internal_metadata, "true");
    }
    OAuthCredentialMaterial {
        input_key: session.input_key.clone(),
        access_token: token.access_token.clone(),
        internal_metadata,
        safe_metadata: safe_metadata(token),
    }
}

pub(crate) fn material_key_belongs_to_input(key: &str, input_key: &str) -> bool {
    key.starts_with(&oauth_metadata_prefix(input_key))
}

fn oauth_metadata_prefix(input_key: &str) -> String {
    format!(
        "{OAUTH_INTERNAL_KEY_PREFIX}{}.",
        BASE64_URL_SAFE_NO_PAD.encode(input_key.as_bytes())
    )
}

fn oauth_refresh_config(
    access_token_material_key: &str,
    metadata_prefix: &str,
    oauth: &ManifestOAuthCredentialSpec,
    material: &BTreeMap<String, String>,
) -> Result<Option<OAuthRefreshConfig>, AppError> {
    if OAuthMetadataKey::Method.get(metadata_prefix, material) != Some(OAUTH_METADATA_METHOD_VALUE)
    {
        return Ok(None);
    }
    let Some(expires_at) = OAuthMetadataKey::AccessTokenExpiresAt.get(metadata_prefix, material)
    else {
        return Ok(None);
    };
    let expires_at = DateTime::parse_from_rfc3339(expires_at)
        .map_err(|error| {
            AppError::FailedPrecondition(format!(
                "stored OAuth access token expiry for source secret '{access_token_material_key}' is invalid: {error}"
            ))
        })?
        .with_timezone(&Utc);
    let now = Utc::now();
    if expires_at > now + chrono::Duration::seconds(REFRESH_EXPIRY_SKEW_SECONDS) {
        return Ok(None);
    }
    let Some(refresh_token) = OAuthMetadataKey::RefreshToken
        .get(metadata_prefix, material)
        .and_then(trimmed_non_empty)
        .map(ToString::to_string)
    else {
        if expires_at > now {
            return Ok(None);
        }
        return Err(AppError::FailedPrecondition(format!(
            "OAuth access token for source secret '{access_token_material_key}' expired and cannot be refreshed because no refresh token is stored; reconnect the source"
        )));
    };
    let client_id = OAuthMetadataKey::ClientId
        .get(metadata_prefix, material)
        .and_then(trimmed_non_empty)
        .map(ToString::to_string)
        .or_else(|| {
            oauth
                .client
                .id
                .default
                .as_deref()
                .and_then(trimmed_non_empty)
                .map(ToString::to_string)
        })
        .ok_or_else(|| {
            AppError::FailedPrecondition(format!(
                "OAuth access token for source secret '{access_token_material_key}' expired and cannot be refreshed because client ID metadata is missing"
            ))
        })?;
    let token_url = OAuthMetadataKey::TokenUrl
        .get(metadata_prefix, material)
        .and_then(trimmed_non_empty)
        .unwrap_or(&oauth.token_url);
    let token_url = ValidatedOAuthEndpoint::untrusted(token_url, "stored token URL")?;
    let resource =
        oauth_refresh_resource(access_token_material_key, metadata_prefix, oauth, material)?;
    let client_secret_transport = OAuthMetadataKey::ClientSecretTransport
        .get(metadata_prefix, material)
        .and_then(trimmed_non_empty)
        .map(|value| {
            ManifestOAuthClientSecretTransport::from_label(value).ok_or_else(|| {
                AppError::FailedPrecondition(format!(
                    "stored OAuth client secret transport for source secret '{access_token_material_key}' is invalid: {value}"
                ))
            })
        })
        .transpose()?
        .or_else(|| oauth.client.secret.as_ref().map(|secret| secret.transport));
    let client_secret = OAuthMetadataKey::ClientSecret
        .get(metadata_prefix, material)
        .and_then(trimmed_non_empty)
        .map(ToString::to_string);
    if client_secret_transport.is_some() && client_secret.is_none() {
        return Err(AppError::FailedPrecondition(format!(
            "OAuth access token for source secret '{access_token_material_key}' expired and cannot be refreshed because client secret metadata is missing"
        )));
    }
    Ok(Some(OAuthRefreshConfig {
        token_url,
        client_id,
        client_secret,
        client_secret_transport,
        refresh_token,
        resource,
    }))
}

fn oauth_refresh_resource(
    access_token_material_key: &str,
    metadata_prefix: &str,
    oauth: &ManifestOAuthCredentialSpec,
    material: &BTreeMap<String, String>,
) -> Result<Option<String>, AppError> {
    if let Some(resource) = OAuthMetadataKey::Resource
        .get(metadata_prefix, material)
        .and_then(trimmed_non_empty)
    {
        return Ok(Some(resource.to_string()));
    }

    oauth_refresh_manifest_resource(access_token_material_key, oauth)
}

fn oauth_refresh_manifest_resource(
    access_token_material_key: &str,
    oauth: &ManifestOAuthCredentialSpec,
) -> Result<Option<String>, AppError> {
    let Some(resource_template) = oauth.resource.as_deref().filter(|value| !value.is_empty())
    else {
        return Ok(None);
    };
    let template = ParsedTemplate::parse(resource_template).map_err(|error| {
        AppError::FailedPrecondition(format!(
            "OAuth access token for source secret '{access_token_material_key}' expired and cannot be refreshed because OAuth resource metadata is missing and the manifest resource template is invalid: {error}"
        ))
    })?;
    if template.tokens().next().is_some() {
        return Err(AppError::FailedPrecondition(format!(
            "OAuth access token for source secret '{access_token_material_key}' expired and cannot be refreshed because rendered OAuth resource metadata is missing; reconnect the source"
        )));
    }

    let url = Url::parse(resource_template).map_err(|error| {
        AppError::FailedPrecondition(format!(
            "OAuth access token for source secret '{access_token_material_key}' expired and cannot be refreshed because OAuth resource metadata is missing and the literal manifest resource is invalid: {error}"
        ))
    })?;
    if url.fragment().is_some() {
        return Err(AppError::FailedPrecondition(format!(
            "OAuth access token for source secret '{access_token_material_key}' expired and cannot be refreshed because OAuth resource metadata is missing and the literal manifest resource must not include a fragment"
        )));
    }
    Ok(Some(resource_template.to_string()))
}

fn apply_refreshed_token(
    access_token_material_key: &str,
    metadata_prefix: &str,
    material: &mut BTreeMap<String, String>,
    token: &TokenResponse,
) {
    material.insert(
        access_token_material_key.to_string(),
        token.access_token.clone(),
    );
    OAuthMetadataKey::Method.insert(metadata_prefix, material, OAUTH_METADATA_METHOD_VALUE);
    match token.expires_at {
        Some(expires_at) => {
            OAuthMetadataKey::AccessTokenExpiresAt.insert(
                metadata_prefix,
                material,
                expires_at.to_rfc3339(),
            );
        }
        None => {
            OAuthMetadataKey::AccessTokenExpiresAt.remove(metadata_prefix, material);
        }
    }
    if let Some(refresh_token) = token.refresh_token.as_deref() {
        OAuthMetadataKey::RefreshToken.insert(metadata_prefix, material, refresh_token);
    }
    if let Some(token_type) = token.token_type.as_deref() {
        OAuthMetadataKey::TokenType.insert(metadata_prefix, material, token_type);
    }
    if let Some(scope) = token.scope.as_deref() {
        OAuthMetadataKey::Scope.insert(metadata_prefix, material, scope);
    }
}

fn safe_metadata(token: &TokenResponse) -> BTreeMap<String, String> {
    let mut metadata = BTreeMap::new();
    if let Some(token_type) = token.token_type.as_deref() {
        metadata.insert("token_type".to_string(), token_type.to_string());
    }
    if let Some(scope) = token.scope.as_deref() {
        metadata.insert("scope".to_string(), scope.to_string());
    }
    if let Some(expires_at) = token.expires_at {
        metadata.insert(
            "access_token_expires_at".to_string(),
            expires_at.to_rfc3339(),
        );
    }
    metadata
}

fn truncate_detail(value: &str) -> String {
    const MAX: usize = 512;
    if value.len() <= MAX {
        return value.to_string();
    }
    let mut cut = MAX;
    while cut > 0 && !value.is_char_boundary(cut) {
        cut -= 1;
    }
    let prefix = value.get(..cut).unwrap_or(value);
    format!("{prefix}...")
}

#[cfg(test)]
mod tests {
    #![expect(
        clippy::indexing_slicing,
        reason = "OAuth test server buffer assertions intentionally fail loudly in tests"
    )]

    use std::collections::BTreeMap;
    use std::io::{Read as _, Write as _};
    use std::net::TcpListener as StdTcpListener;
    use std::sync::LazyLock;
    use std::time::Duration;

    use super::{
        AuthorizationCodeSessionConfig, OAuthCredentialService, OAuthMetadataKey,
        OAuthRefreshConfig, OAuthSessionCommon, RefreshOAuthCredentialRequest, ResolvedOAuthClient,
        StartOAuthCredentialRequest, ValidatedOAuthEndpoint, basic_client_authorization,
        bind_redirect_listener, dynamic_client_registration_grant_types,
        join_dynamic_client_registration_scope_values, join_scope_values,
        material_key_belongs_to_input, oauth_metadata_prefix, parse_device_authorization_response,
        parse_dynamic_client_registration_response, parse_token_response, pkce_challenge,
        preflight_oauth_endpoints, receive_callback, refresh_access_token, request_device_code,
        token_http_clients,
    };
    use coral_spec::{
        ManifestOAuthClientIdSpec, ManifestOAuthClientSecretSpec,
        ManifestOAuthClientSecretTransport, ManifestOAuthClientSpec, ManifestOAuthCredentialSpec,
        ManifestOAuthDynamicClientRegistrationAuthMethod,
        ManifestOAuthDynamicClientRegistrationSpec, ManifestOAuthFlowKind, ManifestOAuthFlowSpec,
        ManifestOAuthPkceMode, ManifestOAuthRedirectUriPortMode, ManifestOAuthScopeDelimiter,
        ManifestOAuthScopeSpec, ManifestOAuthScopesSpec,
    };
    use serde_json::Value;
    use tokio::sync::oneshot;
    use tokio::task::JoinHandle;
    use tokio::{io::AsyncReadExt as _, io::AsyncWriteExt as _};
    use url::Url;

    static EMPTY_SOURCE_INPUTS: LazyLock<BTreeMap<String, String>> = LazyLock::new(BTreeMap::new);

    fn metadata_key(input_key: &str, key: OAuthMetadataKey) -> String {
        key.key(&oauth_metadata_prefix(input_key))
    }

    fn assert_public_dcr_metadata(metadata: &BTreeMap<String, String>, input_key: &str) {
        assert_eq!(
            metadata
                .get(&metadata_key(input_key, OAuthMetadataKey::ClientId))
                .map(String::as_str),
            Some("registered-client")
        );
        assert_eq!(
            metadata
                .get(&metadata_key(input_key, OAuthMetadataKey::Resource))
                .map(String::as_str),
            Some("https://mcp.example.com/mcp")
        );
        assert_eq!(
            metadata
                .get(&metadata_key(
                    input_key,
                    OAuthMetadataKey::DynamicClientRegistration
                ))
                .map(String::as_str),
            Some("true")
        );
        assert!(!metadata.contains_key(&metadata_key(input_key, OAuthMetadataKey::ClientSecret)));
    }

    fn assert_no_dcr_client_management_metadata(
        metadata: &BTreeMap<String, String>,
        input_key: &str,
    ) {
        let prefix = oauth_metadata_prefix(input_key);
        for suffix in [
            "registration_client_uri",
            "registration_access_token",
            "client_id_issued_at",
            "client_secret_expires_at",
        ] {
            assert!(
                !metadata.contains_key(&format!("{prefix}{suffix}")),
                "DCR client-management metadata should not store {suffix}"
            );
        }
    }

    #[test]
    fn joins_scope_values_with_configured_delimiter() {
        let values = vec!["repo".to_string(), "read:org".to_string()];
        assert_eq!(
            join_scope_values(ManifestOAuthScopeDelimiter::Space, &values),
            "repo read:org"
        );
        assert_eq!(
            join_scope_values(ManifestOAuthScopeDelimiter::Comma, &values),
            "repo,read:org"
        );
    }

    #[test]
    fn dynamic_client_registration_scope_metadata_uses_space_delimiter() {
        let values = vec!["repo".to_string(), "read:org".to_string()];
        assert_eq!(
            join_dynamic_client_registration_scope_values(&values),
            "repo read:org"
        );
    }

    #[test]
    fn dynamic_client_registration_grants_request_refresh_only_when_configured() {
        assert_eq!(
            dynamic_client_registration_grant_types("authorization_code", false),
            vec!["authorization_code"]
        );
        assert_eq!(
            dynamic_client_registration_grant_types("authorization_code", true),
            vec!["authorization_code", "refresh_token"]
        );
    }

    #[test]
    fn pkce_challenge_uses_s256_base64url_without_padding() {
        assert_eq!(
            pkce_challenge("dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"),
            "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"
        );
    }

    #[test]
    fn basic_client_authorization_form_encodes_credentials_before_base64() {
        assert_eq!(
            basic_client_authorization("client id", "sec+ret:1"),
            "Basic Y2xpZW50K2lkOnNlYyUyQnJldCUzQTE="
        );
    }

    #[test]
    fn oauth_endpoint_policy_allows_secure_and_loopback_urls_only() {
        for url in [
            "https://provider.example/oauth/token",
            "https://provider.example/oauth/@callback",
            "http://localhost:8080/token",
            "http://LOCALHOST/token",
            "http://127.23.45.67/token",
            "http://[::1]/token",
        ] {
            ValidatedOAuthEndpoint::authored(url, "test URL").expect(url);
        }
        for url in [
            "http://provider.example/token",
            "http://localhost.example/token",
            "http://[::2]/token",
            "ftp://localhost/token",
            "file:///tmp/token",
            "https://provider.example/token#fragment",
            "https:/@provider.example/token",
            "https:////@provider.example/token",
            r"https:\@provider.example/token",
            "https://:@provider.example/token",
            "https:/alice-credential:hunter-credential@provider.example/token",
            "https:////alice-credential:hunter-credential@provider.example/token",
        ] {
            assert!(
                ValidatedOAuthEndpoint::authored(url, "test URL").is_err(),
                "{url}"
            );
        }

        let Err(error) = ValidatedOAuthEndpoint::authored(
            "https://alice-credential:hunter-credential@provider.example/private-route?token=query-credential",
            "test URL",
        ) else {
            panic!("userinfo should fail");
        };
        let detail = error.to_string();
        assert!(matches!(error, crate::bootstrap::AppError::InvalidInput(_)));
        for secret in [
            "alice-credential",
            "hunter-credential",
            "private-route",
            "query-credential",
        ] {
            assert!(
                !detail.contains(secret),
                "diagnostic leaked {secret}: {detail}"
            );
        }
    }

    #[tokio::test]
    async fn ipv6_redirect_uri_binds_fixed_and_random_listeners() {
        let fixed_probe = StdTcpListener::bind("[::1]:0").expect("IPv6 callback probe");
        let fixed_port = fixed_probe.local_addr().expect("IPv6 probe addr").port();
        drop(fixed_probe);

        for (redirect_uri, port_mode) in [
            (
                format!("http://[::1]:{fixed_port}/oauth/callback"),
                ManifestOAuthRedirectUriPortMode::Fixed,
            ),
            (
                "http://[::1]:0/oauth/callback".to_string(),
                ManifestOAuthRedirectUriPortMode::Random,
            ),
        ] {
            let oauth = oauth_spec_with_redirect_uri(
                "https://provider.example/oauth/token",
                &redirect_uri,
                port_mode,
                ManifestOAuthPkceMode::Required,
                ManifestOAuthClientSpec {
                    id: ManifestOAuthClientIdSpec {
                        default: Some("default-client".to_string()),
                        input: None,
                    },
                    secret: None,
                    dynamic_registration: None,
                },
            );

            let (listener, callback_path, provider_redirect_uri) = bind_redirect_listener(&oauth)
                .await
                .expect("bind IPv6 callback");
            let local_addr = listener.local_addr().expect("IPv6 listener addr");
            let provider_redirect_uri =
                Url::parse(&provider_redirect_uri).expect("provider redirect URI");

            assert_eq!(
                local_addr.ip(),
                std::net::IpAddr::V6(std::net::Ipv6Addr::LOCALHOST)
            );
            assert_eq!(callback_path, "/oauth/callback");
            assert!(matches!(
                provider_redirect_uri.host(),
                Some(url::Host::Ipv6(address)) if address.is_loopback()
            ));
            assert_eq!(provider_redirect_uri.port(), Some(local_addr.port()));
            if port_mode == ManifestOAuthRedirectUriPortMode::Fixed {
                assert_eq!(provider_redirect_uri.as_str(), redirect_uri);
            }
        }
    }

    #[tokio::test]
    async fn oauth_preflights_rendered_dcr_url_before_listener_or_event() {
        let occupied_listener = StdTcpListener::bind("127.0.0.1:0").expect("callback listener");
        let redirect_port = occupied_listener
            .local_addr()
            .expect("callback addr")
            .port();
        let oauth = dynamic_registration_oauth_spec(
            redirect_port,
            "https://provider.example/oauth/token",
            "{{input.OAUTH_ENDPOINT}}",
        );
        let source_inputs = BTreeMap::from([(
            "OAUTH_ENDPOINT".to_string(),
            "http://provider.example/register?tenant=private-tenant".to_string(),
        )]);

        let result = OAuthCredentialService::new()
            .authorize(
                StartOAuthCredentialRequest {
                    input_key: "API_TOKEN",
                    oauth: &oauth,
                    source_inputs: &source_inputs,
                    credential_inputs: Vec::new(),
                },
                |_authorization| async {
                    Err(crate::bootstrap::AppError::FailedPrecondition(
                        "authorization event was emitted".to_string(),
                    ))
                },
            )
            .await;
        let error = match result {
            Ok(_material) => panic!("unsafe rendered DCR URL should fail preflight"),
            Err(error) => error,
        };

        let detail = error.to_string();
        assert!(detail.contains("dynamic registration URL"), "{detail}");
        assert!(detail.contains("must use HTTPS"), "{detail}");
        assert!(!detail.contains("private-tenant"), "{detail}");
    }

    #[tokio::test]
    async fn oauth_preflights_every_rendered_endpoint_before_session_side_effects() {
        for (field, expected_label) in [
            ("authorization", "authorization URL"),
            ("device_authorization", "device authorization URL"),
            ("token", "token URL"),
        ] {
            let callback_listener =
                StdTcpListener::bind("127.0.0.1:0").expect("occupied callback listener");
            let redirect_port = callback_listener
                .local_addr()
                .expect("callback address")
                .port();
            let network_sentinel = StdTcpListener::bind("0.0.0.0:0").expect("network sentinel");
            network_sentinel
                .set_nonblocking(true)
                .expect("nonblocking sentinel");
            let rendered_endpoint = format!(
                "http://{}/private-route?token=private-token",
                network_sentinel.local_addr().expect("sentinel address")
            );
            let source_inputs = BTreeMap::from([("OAUTH_ENDPOINT".to_string(), rendered_endpoint)]);
            let oauth = oauth_spec_with_rendered_endpoint(field, redirect_port);

            let result =
                OAuthCredentialService::with_token_request_timeout(Duration::from_millis(100))
                    .authorize(
                        StartOAuthCredentialRequest {
                            input_key: "API_TOKEN",
                            oauth: &oauth,
                            source_inputs: &source_inputs,
                            credential_inputs: Vec::new(),
                        },
                        |_authorization| async {
                            Err(crate::bootstrap::AppError::FailedPrecondition(
                                "authorization event was emitted".to_string(),
                            ))
                        },
                    )
                    .await;
            let error = match result {
                Ok(_material) => panic!("unsafe rendered endpoint should fail preflight"),
                Err(error) => error,
            };

            let detail = error.to_string();
            assert!(matches!(error, crate::bootstrap::AppError::InvalidInput(_)));
            assert!(detail.contains(expected_label), "{detail}");
            for redacted in ["0.0.0.0", "private-route", "private-token"] {
                assert!(!detail.contains(redacted), "{detail}");
            }
            assert!(
                !detail.contains("authorization event was emitted"),
                "{detail}"
            );
            assert!(
                matches!(
                    network_sentinel.accept(),
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock
                ),
                "{field} touched the network before preflight"
            );
        }
    }

    #[test]
    fn oauth_metadata_key_matching_is_exact_for_dotted_inputs() {
        let dotted_key = format!("{}refresh_token", oauth_metadata_prefix("A.B"));

        assert!(material_key_belongs_to_input(&dotted_key, "A.B"));
        assert!(!material_key_belongs_to_input(&dotted_key, "A"));
    }

    #[test]
    fn token_response_ignores_unrepresentable_expires_in() {
        let token = parse_token_response(
            r#"{"access_token":" access-token ","refresh_token":" refresh-token ","token_type":" Bearer ","scope":" repo ","expires_in":9223372036854775807}"#,
        )
        .expect("parse token response");

        assert_eq!(token.access_token, "access-token");
        assert_eq!(token.refresh_token.as_deref(), Some("refresh-token"));
        assert_eq!(token.token_type.as_deref(), Some("Bearer"));
        assert_eq!(token.scope.as_deref(), Some("repo"));
        assert!(token.expires_at.is_none());
    }

    #[test]
    fn device_response_rejects_unsafe_verification_urls_without_leaking_them() {
        for (body, expected_label) in [
            (
                r#"{"device_code":"code","user_code":"user","verification_uri":"http://provider.example/verify?code=primary-secret","expires_in":60}"#,
                "provider verification URL",
            ),
            (
                r#"{"device_code":"code","user_code":"user","verification_url":"http://provider.example/verify?code=legacy-secret","expires_in":60}"#,
                "provider verification URL",
            ),
            (
                r#"{"device_code":"code","user_code":"user","verification_uri":"https://provider.example/verify","verification_uri_complete":"http://provider.example/verify?code=complete-secret","expires_in":60}"#,
                "provider complete verification URL",
            ),
        ] {
            let error = match parse_device_authorization_response(body) {
                Ok(_response) => panic!("unsafe provider verification URL should fail"),
                Err(error) => error,
            };
            let detail = error.to_string();
            assert!(matches!(
                error,
                crate::bootstrap::AppError::FailedPrecondition(_)
            ));
            assert!(detail.contains(expected_label), "{detail}");
            assert!(!detail.contains("provider.example"), "{detail}");
            assert!(!detail.contains("secret"), "{detail}");
        }

        let response = parse_device_authorization_response(
            r#"{"device_code":"code","user_code":"user","verification_url":"https://provider.example/verify","expires_in":60}"#,
        )
        .expect("safe legacy verification URL");
        assert_eq!(response.verification_uri, "https://provider.example/verify");
    }

    #[test]
    fn dynamic_client_registration_response_rejects_unsupported_auth_method() {
        let Err(error) = parse_dynamic_client_registration_response(
            r#"{"client_id":"registered-client","token_endpoint_auth_method":"private_key_jwt"}"#,
            ManifestOAuthDynamicClientRegistrationAuthMethod::None,
        ) else {
            panic!("unsupported DCR auth method should fail");
        };

        assert!(
            error
                .to_string()
                .contains("token_endpoint_auth_method is unsupported: private_key_jwt"),
            "unexpected error: {error}"
        );
    }

    async fn assert_unsafe_stored_refresh_url_is_rejected(
        service: &OAuthCredentialService,
        oauth: &ManifestOAuthCredentialSpec,
        material: &mut BTreeMap<String, String>,
        token_url_key: &str,
    ) {
        let safe_token_url = material
            .insert(
                token_url_key.to_string(),
                "https://alice-credential:hunter-credential@provider.example/token?secret=query-credential"
                    .to_string(),
            )
            .expect("safe token URL");
        let original = material.clone();
        let error = service
            .refresh_if_needed(
                RefreshOAuthCredentialRequest::for_source_input("API_TOKEN", oauth),
                material,
            )
            .await
            .expect_err("unsafe stored URL should fail before refresh");
        let detail = error.to_string();
        assert!(matches!(
            error,
            crate::bootstrap::AppError::FailedPrecondition(_)
        ));
        assert_eq!(*material, original);
        for secret in [
            "alice-credential",
            "hunter-credential",
            "provider.example",
            "query-credential",
        ] {
            assert!(!detail.contains(secret), "diagnostic leaked {secret}");
        }
        material.insert(token_url_key.to_string(), safe_token_url);
    }

    #[tokio::test]
    async fn expired_oauth_material_refreshes_access_token() {
        let fixture = OAuthFixture::new(Some(
            r#"{"access_token":"refreshed-token","refresh_token":"rotated-refresh-token","token_type":"Bearer","scope":"repo read:org","expires_in":3600}"#,
        ));
        let proxy = StdTcpListener::bind("127.0.0.1:0").expect("hostile proxy");
        proxy.set_nonblocking(true).expect("nonblocking proxy");
        let proxy_url = format!("http://{}", proxy.local_addr().expect("proxy addr"));
        let oauth = oauth_spec(
            &fixture.token_url,
            free_loopback_port(),
            ManifestOAuthPkceMode::Disabled,
            ManifestOAuthClientSpec {
                id: ManifestOAuthClientIdSpec {
                    default: Some("default-client".to_string()),
                    input: None,
                },
                secret: None,
                dynamic_registration: None,
            },
        );
        let prefix = oauth_metadata_prefix("API_TOKEN");
        let mut material = BTreeMap::from([
            ("API_TOKEN".to_string(), "expired-token".to_string()),
            (format!("{prefix}method"), "oauth".to_string()),
            (
                format!("{prefix}access_token_expires_at"),
                (chrono::Utc::now() - chrono::Duration::minutes(5)).to_rfc3339(),
            ),
            (
                format!("{prefix}refresh_token"),
                " stored-refresh-token ".to_string(),
            ),
            (format!("{prefix}client_id"), " stored-client ".to_string()),
            (
                format!("{prefix}token_url"),
                format!(" {} ", fixture.token_url),
            ),
        ]);
        let service = OAuthCredentialService::with_token_request_proxy(
            Duration::from_secs(1),
            reqwest::Proxy::all(&proxy_url).expect("proxy URL"),
        );

        let token_url_key = format!("{prefix}token_url");
        assert_unsafe_stored_refresh_url_is_rejected(
            &service,
            &oauth,
            &mut material,
            &token_url_key,
        )
        .await;

        let refreshed = service
            .refresh_if_needed(
                RefreshOAuthCredentialRequest::for_source_input("API_TOKEN", &oauth),
                &mut material,
            )
            .await
            .expect("refresh oauth material");
        let captured = fixture.token_server.await.expect("token server");

        assert!(refreshed);
        assert_eq!(
            captured.form.get("grant_type").map(String::as_str),
            Some("refresh_token")
        );
        assert_eq!(
            captured.form.get("refresh_token").map(String::as_str),
            Some("stored-refresh-token")
        );
        assert_eq!(
            captured.form.get("client_id").map(String::as_str),
            Some("stored-client")
        );
        assert_eq!(
            material.get("API_TOKEN").map(String::as_str),
            Some("refreshed-token")
        );
        assert_eq!(
            material
                .get(&format!("{prefix}refresh_token"))
                .map(String::as_str),
            Some("rotated-refresh-token")
        );
        assert!(captured.authorization.is_none());
        assert!(
            matches!(proxy.accept(), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock),
            "loopback refresh must bypass the hostile proxy"
        );
    }

    #[tokio::test]
    async fn oauth_client_does_not_replay_refresh_secret_across_307_or_308() {
        for status in [307, 308] {
            let target = StdTcpListener::bind("127.0.0.1:0").expect("redirect target");
            target.set_nonblocking(true).expect("nonblocking target");
            let target_url = format!(
                "http://{}/replay",
                target.local_addr().expect("target addr")
            );
            let source = StdTcpListener::bind("127.0.0.1:0").expect("redirect source");
            let source_url = format!("http://{}/token", source.local_addr().expect("source addr"));
            let server = tokio::task::spawn_blocking(move || {
                let (mut stream, _) = source.accept().expect("accept refresh");
                let request = read_http_request(&mut stream);
                let response = format!(
                    "HTTP/1.1 {status} Redirect\r\nlocation: {target_url}\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"
                );
                stream
                    .write_all(response.as_bytes())
                    .expect("write redirect");
                request
            });
            let refresh = OAuthRefreshConfig {
                token_url: ValidatedOAuthEndpoint::untrusted(&source_url, "stored token URL")
                    .expect("loopback URL"),
                client_id: "client".to_string(),
                client_secret: None,
                client_secret_transport: None,
                refresh_token: "refresh-secret".to_string(),
                resource: None,
            };

            let http = token_http_clients(Duration::from_millis(250), None);
            let Err(error) = refresh_access_token(&http, &refresh).await else {
                panic!("redirect response should not be followed");
            };
            let captured = server.await.expect("redirect server");

            assert!(error.to_string().contains(&format!("HTTP {status}")));
            assert_eq!(
                captured.form.get("refresh_token").map(String::as_str),
                Some("refresh-secret")
            );
            assert!(
                matches!(
                    target.accept(),
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock
                ),
                "redirect target received the refresh secret"
            );
        }
    }

    #[tokio::test]
    async fn oauth_refresh_uses_stored_rendered_resource_metadata() {
        let fixture = OAuthFixture::new(Some(
            r#"{"access_token":"refreshed-token","token_type":"Bearer","expires_in":3600}"#,
        ));
        let mut oauth = oauth_spec(
            &fixture.token_url,
            free_loopback_port(),
            ManifestOAuthPkceMode::Disabled,
            ManifestOAuthClientSpec {
                id: ManifestOAuthClientIdSpec {
                    default: Some("default-client".to_string()),
                    input: None,
                },
                secret: None,
                dynamic_registration: None,
            },
        );
        oauth.resource = Some("https://{{input.MCP_HOST}}/mcp".to_string());
        let prefix = oauth_metadata_prefix("API_TOKEN");
        let mut material = BTreeMap::from([
            ("API_TOKEN".to_string(), "expired-token".to_string()),
            (format!("{prefix}method"), "oauth".to_string()),
            (
                format!("{prefix}access_token_expires_at"),
                (chrono::Utc::now() - chrono::Duration::minutes(5)).to_rfc3339(),
            ),
            (
                format!("{prefix}refresh_token"),
                "stored-refresh-token".to_string(),
            ),
            (format!("{prefix}client_id"), "stored-client".to_string()),
            (format!("{prefix}token_url"), fixture.token_url.clone()),
            (
                format!("{prefix}resource"),
                " https://mcp.example.com/mcp ".to_string(),
            ),
        ]);
        let service = OAuthCredentialService::new();

        let refreshed = service
            .refresh_if_needed(
                RefreshOAuthCredentialRequest::for_source_input("API_TOKEN", &oauth),
                &mut material,
            )
            .await
            .expect("refresh oauth material");
        let captured = fixture.token_server.await.expect("token server");

        assert!(refreshed);
        assert_eq!(
            captured.form.get("resource").map(String::as_str),
            Some("https://mcp.example.com/mcp")
        );
    }

    #[tokio::test]
    async fn oauth_refresh_uses_literal_manifest_resource_when_metadata_is_missing() {
        let fixture = OAuthFixture::new(Some(
            r#"{"access_token":"refreshed-token","token_type":"Bearer","expires_in":3600}"#,
        ));
        let mut oauth = oauth_spec(
            &fixture.token_url,
            free_loopback_port(),
            ManifestOAuthPkceMode::Disabled,
            ManifestOAuthClientSpec {
                id: ManifestOAuthClientIdSpec {
                    default: Some("default-client".to_string()),
                    input: None,
                },
                secret: None,
                dynamic_registration: None,
            },
        );
        oauth.resource = Some("https://mcp.example.com/mcp".to_string());
        let prefix = oauth_metadata_prefix("API_TOKEN");
        let mut material = BTreeMap::from([
            ("API_TOKEN".to_string(), "expired-token".to_string()),
            (format!("{prefix}method"), "oauth".to_string()),
            (
                format!("{prefix}access_token_expires_at"),
                (chrono::Utc::now() - chrono::Duration::minutes(5)).to_rfc3339(),
            ),
            (
                format!("{prefix}refresh_token"),
                "stored-refresh-token".to_string(),
            ),
            (format!("{prefix}client_id"), "stored-client".to_string()),
            (format!("{prefix}token_url"), fixture.token_url.clone()),
        ]);
        let service = OAuthCredentialService::new();

        let refreshed = service
            .refresh_if_needed(
                RefreshOAuthCredentialRequest::for_source_input("API_TOKEN", &oauth),
                &mut material,
            )
            .await
            .expect("refresh oauth material");
        let captured = fixture.token_server.await.expect("token server");

        assert!(refreshed);
        assert_eq!(
            captured.form.get("resource").map(String::as_str),
            Some("https://mcp.example.com/mcp")
        );
    }

    #[tokio::test]
    async fn oauth_refresh_rejects_template_resource_without_stored_metadata() {
        let mut oauth = oauth_spec(
            "http://127.0.0.1:9/token",
            53682,
            ManifestOAuthPkceMode::Disabled,
            ManifestOAuthClientSpec {
                id: ManifestOAuthClientIdSpec {
                    default: Some("default-client".to_string()),
                    input: None,
                },
                secret: None,
                dynamic_registration: None,
            },
        );
        oauth.resource = Some("https://{{input.MCP_HOST}}/mcp".to_string());
        let prefix = oauth_metadata_prefix("API_TOKEN");
        let mut material = BTreeMap::from([
            ("API_TOKEN".to_string(), "expired-token".to_string()),
            (format!("{prefix}method"), "oauth".to_string()),
            (
                format!("{prefix}access_token_expires_at"),
                (chrono::Utc::now() - chrono::Duration::minutes(5)).to_rfc3339(),
            ),
            (
                format!("{prefix}refresh_token"),
                "stored-refresh-token".to_string(),
            ),
            (format!("{prefix}client_id"), "stored-client".to_string()),
            (
                format!("{prefix}token_url"),
                "http://127.0.0.1:9/token".to_string(),
            ),
        ]);
        let service = OAuthCredentialService::new();

        let error = service
            .refresh_if_needed(
                RefreshOAuthCredentialRequest::for_source_input("API_TOKEN", &oauth),
                &mut material,
            )
            .await
            .expect_err("templated resource without stored metadata should fail");

        assert!(
            error
                .to_string()
                .contains("rendered OAuth resource metadata is missing"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn oauth_refresh_rejects_literal_manifest_resource_fragment() {
        let mut oauth = oauth_spec(
            "http://127.0.0.1:9/token",
            53682,
            ManifestOAuthPkceMode::Disabled,
            ManifestOAuthClientSpec {
                id: ManifestOAuthClientIdSpec {
                    default: Some("default-client".to_string()),
                    input: None,
                },
                secret: None,
                dynamic_registration: None,
            },
        );
        oauth.resource = Some("https://mcp.example.com/mcp#fragment".to_string());
        let prefix = oauth_metadata_prefix("API_TOKEN");
        let mut material = BTreeMap::from([
            ("API_TOKEN".to_string(), "expired-token".to_string()),
            (format!("{prefix}method"), "oauth".to_string()),
            (
                format!("{prefix}access_token_expires_at"),
                (chrono::Utc::now() - chrono::Duration::minutes(5)).to_rfc3339(),
            ),
            (
                format!("{prefix}refresh_token"),
                "stored-refresh-token".to_string(),
            ),
            (format!("{prefix}client_id"), "stored-client".to_string()),
            (
                format!("{prefix}token_url"),
                "http://127.0.0.1:9/token".to_string(),
            ),
        ]);
        let service = OAuthCredentialService::new();

        let error = service
            .refresh_if_needed(
                RefreshOAuthCredentialRequest::for_source_input("API_TOKEN", &oauth),
                &mut material,
            )
            .await
            .expect_err("literal resource fragments should fail before refresh");

        assert!(
            error
                .to_string()
                .contains("literal manifest resource must not include a fragment"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn unexpired_oauth_material_does_not_refresh_access_token() {
        let oauth = oauth_spec(
            "http://127.0.0.1:9/token",
            53682,
            ManifestOAuthPkceMode::Disabled,
            ManifestOAuthClientSpec {
                id: ManifestOAuthClientIdSpec {
                    default: Some("default-client".to_string()),
                    input: None,
                },
                secret: None,
                dynamic_registration: None,
            },
        );
        let prefix = oauth_metadata_prefix("API_TOKEN");
        let mut material = BTreeMap::from([
            ("API_TOKEN".to_string(), "fresh-token".to_string()),
            (format!("{prefix}method"), "oauth".to_string()),
            (
                format!("{prefix}access_token_expires_at"),
                (chrono::Utc::now() + chrono::Duration::minutes(5)).to_rfc3339(),
            ),
            (
                format!("{prefix}refresh_token"),
                "stored-refresh-token".to_string(),
            ),
        ]);
        let service = OAuthCredentialService::new();

        let refreshed = service
            .refresh_if_needed(
                RefreshOAuthCredentialRequest::for_source_input("API_TOKEN", &oauth),
                &mut material,
            )
            .await
            .expect("refresh oauth material");

        assert!(!refreshed);
        assert_eq!(
            material.get("API_TOKEN").map(String::as_str),
            Some("fresh-token")
        );
    }

    #[tokio::test]
    async fn near_expiry_oauth_material_without_refresh_token_remains_usable() {
        let oauth = oauth_spec(
            "http://127.0.0.1:9/token",
            53682,
            ManifestOAuthPkceMode::Disabled,
            ManifestOAuthClientSpec {
                id: ManifestOAuthClientIdSpec {
                    default: Some("default-client".to_string()),
                    input: None,
                },
                secret: None,
                dynamic_registration: None,
            },
        );
        let prefix = oauth_metadata_prefix("API_TOKEN");
        let mut material = BTreeMap::from([
            ("API_TOKEN".to_string(), "near-expiry-token".to_string()),
            (format!("{prefix}method"), "oauth".to_string()),
            (
                format!("{prefix}access_token_expires_at"),
                (chrono::Utc::now() + chrono::Duration::seconds(30)).to_rfc3339(),
            ),
        ]);
        let service = OAuthCredentialService::new();

        let refreshed = service
            .refresh_if_needed(
                RefreshOAuthCredentialRequest::for_source_input("API_TOKEN", &oauth),
                &mut material,
            )
            .await
            .expect("near-expiry token without refresh token should still be usable");

        assert!(!refreshed);
        assert_eq!(
            material.get("API_TOKEN").map(String::as_str),
            Some("near-expiry-token")
        );
    }

    #[tokio::test]
    async fn oauth_refresh_request_times_out_when_token_endpoint_stalls() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind stalling token endpoint");
        let token_url = format!(
            "http://{}/token",
            listener.local_addr().expect("token endpoint addr")
        );
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept token request");
            let mut buffer = [0_u8; 1024];
            let _bytes_read = socket
                .read(&mut buffer)
                .await
                .expect("read stalled token request");
            tokio::time::sleep(Duration::from_mins(1)).await;
        });
        let oauth = oauth_spec(
            &token_url,
            53682,
            ManifestOAuthPkceMode::Disabled,
            ManifestOAuthClientSpec {
                id: ManifestOAuthClientIdSpec {
                    default: Some("default-client".to_string()),
                    input: None,
                },
                secret: None,
                dynamic_registration: None,
            },
        );
        let prefix = oauth_metadata_prefix("API_TOKEN");
        let mut material = BTreeMap::from([
            ("API_TOKEN".to_string(), "expired-token".to_string()),
            (format!("{prefix}method"), "oauth".to_string()),
            (
                format!("{prefix}access_token_expires_at"),
                (chrono::Utc::now() - chrono::Duration::minutes(5)).to_rfc3339(),
            ),
            (
                format!("{prefix}refresh_token"),
                "stored-refresh-token".to_string(),
            ),
            (format!("{prefix}client_id"), "stored-client".to_string()),
            (format!("{prefix}token_url"), token_url),
        ]);
        let service = OAuthCredentialService::with_token_request_timeout(Duration::from_millis(50));

        let error = service
            .refresh_if_needed(
                RefreshOAuthCredentialRequest::for_source_input("API_TOKEN", &oauth),
                &mut material,
            )
            .await
            .expect_err("stalled refresh should time out");

        assert!(
            error
                .to_string()
                .contains("OAuth token refresh request failed"),
            "unexpected error: {error:#}"
        );
        assert_eq!(
            material.get("API_TOKEN").map(String::as_str),
            Some("expired-token")
        );
        server.abort();
    }

    #[tokio::test]
    async fn confidential_oauth_refresh_uses_stored_basic_auth_client_secret() {
        let fixture = OAuthFixture::new(Some(
            r#"{"access_token":"refreshed-token","token_type":"Bearer","expires_in":3600}"#,
        ));
        let oauth = oauth_spec(
            &fixture.token_url,
            free_loopback_port(),
            ManifestOAuthPkceMode::Disabled,
            confidential_client(ManifestOAuthClientSecretTransport::BasicAuth),
        );
        let prefix = oauth_metadata_prefix("API_TOKEN");
        let mut material = BTreeMap::from([
            ("API_TOKEN".to_string(), "expired-token".to_string()),
            (format!("{prefix}method"), "oauth".to_string()),
            (
                format!("{prefix}access_token_expires_at"),
                (chrono::Utc::now() - chrono::Duration::minutes(5)).to_rfc3339(),
            ),
            (
                format!("{prefix}refresh_token"),
                "stored-refresh-token".to_string(),
            ),
            (format!("{prefix}client_id"), "stored-client".to_string()),
            (
                format!("{prefix}client_secret"),
                " stored-secret ".to_string(),
            ),
            (
                format!("{prefix}client_secret_transport"),
                " basic_auth ".to_string(),
            ),
            (format!("{prefix}token_url"), fixture.token_url.clone()),
        ]);
        let service = OAuthCredentialService::new();

        let refreshed = service
            .refresh_if_needed(
                RefreshOAuthCredentialRequest::for_source_input("API_TOKEN", &oauth),
                &mut material,
            )
            .await
            .expect("refresh oauth material");
        let captured = fixture.token_server.await.expect("token server");

        assert!(refreshed);
        let expected_authorization = basic_client_authorization("stored-client", "stored-secret");
        assert_eq!(
            captured.authorization.as_deref(),
            Some(expected_authorization.as_str())
        );
        assert!(!captured.form.contains_key("client_secret"));
        assert_eq!(
            material.get("API_TOKEN").map(String::as_str),
            Some("refreshed-token")
        );
    }

    #[tokio::test]
    async fn public_pkce_oauth_session_exchanges_and_returns_token_material() {
        let fixture = OAuthFixture::new(None);
        let redirect_port = free_loopback_port();
        let oauth = oauth_spec(
            &fixture.token_url,
            redirect_port,
            ManifestOAuthPkceMode::Required,
            ManifestOAuthClientSpec {
                id: ManifestOAuthClientIdSpec {
                    default: Some("default-client".to_string()),
                    input: Some("OAUTH_CLIENT_ID".to_string()),
                },
                secret: None,
                dynamic_registration: None,
            },
        );
        let service = OAuthCredentialService::new();

        let (authorization_tx, authorization_rx) = oneshot::channel();
        let authorize = service.authorize(
            StartOAuthCredentialRequest {
                input_key: "API_TOKEN",
                oauth: &oauth,
                source_inputs: &EMPTY_SOURCE_INPUTS,
                credential_inputs: vec![(
                    "OAUTH_CLIENT_ID".to_string(),
                    " override-client ".to_string(),
                )],
            },
            move |authorization| async move {
                authorization_tx
                    .send(authorization.authorization_url)
                    .map_err(|_authorization_url| {
                        crate::bootstrap::AppError::FailedPrecondition(
                            "authorization receiver closed".to_string(),
                        )
                    })
            },
        );
        let callback = async {
            let authorization_url = authorization_rx.await.expect("authorization url");
            let authorization_url = Url::parse(&authorization_url).expect("authorization url");
            let query = query_pairs(&authorization_url);
            assert_eq!(
                query.get("client_id").map(String::as_str),
                Some("override-client")
            );
            assert_eq!(
                query.get("scope").map(String::as_str),
                Some("repo read:org")
            );
            assert_eq!(
                query.get("code_challenge_method").map(String::as_str),
                Some("S256")
            );
            assert!(!query.contains_key("client_secret"));
            let callback_url = format!(
                "http://127.0.0.1:{redirect_port}/oauth/callback?state={}&code=test-code",
                query.get("state").expect("state")
            );
            reqwest::get(callback_url)
                .await
                .expect("callback response")
                .error_for_status()
                .expect("callback success");
        };

        let (completed, ()) = tokio::join!(authorize, callback);
        let completed = completed.expect("authorize oauth");
        let captured = fixture.token_server.await.expect("token server");

        assert_eq!(completed.input_key, "API_TOKEN");
        assert_eq!(completed.access_token, "access-token");
        assert_eq!(
            captured.form.get("client_id").map(String::as_str),
            Some("override-client")
        );
        assert_eq!(
            captured.form.get("code").map(String::as_str),
            Some("test-code")
        );
        assert!(captured.form.contains_key("code_verifier"));
        assert!(!captured.form.contains_key("client_secret"));
        assert!(captured.authorization.is_none());
        let prefix = oauth_metadata_prefix("API_TOKEN");
        assert_eq!(
            completed
                .internal_metadata
                .get(&format!("{prefix}refresh_token"))
                .map(String::as_str),
            Some("refresh-token")
        );
        assert_eq!(
            completed
                .internal_metadata
                .get(&format!("{prefix}client_id"))
                .map(String::as_str),
            Some("override-client")
        );
        assert_eq!(
            completed.safe_metadata.get("scope").map(String::as_str),
            Some("repo read:org")
        );
    }

    #[tokio::test]
    async fn dynamic_client_registration_runs_before_authorization_code_session() {
        let registration_fixture = DynamicRegistrationFixture::new(
            r#"{"client_id":"registered-client","client_secret":"unused-public-secret","registration_client_uri":"https://provider.example.com/register/registered-client","registration_access_token":"registration-access","token_endpoint_auth_method":"none","client_id_issued_at":1710000000}"#,
        );
        let token_fixture = OAuthFixture::new(None);
        let redirect_port = free_loopback_port();
        let mut oauth = dynamic_registration_oauth_spec(
            redirect_port,
            &token_fixture.token_url,
            &registration_fixture.registration_url,
        );
        oauth.scopes.as_mut().expect("scopes").scope.delimiter = ManifestOAuthScopeDelimiter::Comma;
        let service = OAuthCredentialService::new();

        let (authorization_tx, authorization_rx) = oneshot::channel();
        let authorize = service.authorize(
            StartOAuthCredentialRequest {
                input_key: "MCP_ACCESS_TOKEN",
                oauth: &oauth,
                source_inputs: &EMPTY_SOURCE_INPUTS,
                credential_inputs: Vec::new(),
            },
            move |authorization| async move {
                authorization_tx
                    .send(authorization.authorization_url)
                    .map_err(|_authorization_url| {
                        crate::bootstrap::AppError::FailedPrecondition(
                            "authorization receiver closed".to_string(),
                        )
                    })
            },
        );
        let callback = async {
            let authorization_url = authorization_rx.await.expect("authorization url");
            let authorization_url = Url::parse(&authorization_url).expect("authorization url");
            let query = query_pairs(&authorization_url);
            assert_eq!(
                query.get("client_id").map(String::as_str),
                Some("registered-client")
            );
            assert_eq!(
                query.get("resource").map(String::as_str),
                Some("https://mcp.example.com/mcp")
            );
            assert_eq!(
                query.get("scope").map(String::as_str),
                Some("repo,read:org")
            );
            callback(authorization_url.as_str()).await;
        };

        let (completed, ()) = tokio::join!(authorize, callback);
        let completed = completed.expect("authorize oauth");
        let registration = registration_fixture
            .server
            .await
            .expect("registration server");
        let token = token_fixture.token_server.await.expect("token server");

        assert_dynamic_registration_request(&registration, redirect_port);
        assert_eq!(
            token.form.get("client_id").map(String::as_str),
            Some("registered-client")
        );
        assert_eq!(
            token.form.get("resource").map(String::as_str),
            Some("https://mcp.example.com/mcp")
        );
        assert!(!token.form.contains_key("client_secret"));
        assert_eq!(completed.access_token, "access-token");
        assert_public_dcr_metadata(&completed.internal_metadata, "MCP_ACCESS_TOKEN");
        assert_no_dcr_client_management_metadata(&completed.internal_metadata, "MCP_ACCESS_TOKEN");
    }

    #[tokio::test]
    async fn device_code_oauth_session_polls_and_stores_token_material() {
        let fixture = DeviceOAuthFixture::new();
        let oauth = device_oauth_spec(&fixture.device_url, &fixture.token_url);
        let service = OAuthCredentialService::new();

        let (authorization_tx, authorization_rx) = oneshot::channel();
        let authorize = service.authorize(
            StartOAuthCredentialRequest {
                input_key: "API_TOKEN",
                oauth: &oauth,
                source_inputs: &EMPTY_SOURCE_INPUTS,
                credential_inputs: vec![(
                    "OAUTH_CLIENT_ID".to_string(),
                    "device-client".to_string(),
                )],
            },
            move |authorization| async move {
                authorization_tx.send(authorization).map_err(|_error| {
                    crate::bootstrap::AppError::FailedPrecondition(
                        "authorization receiver closed".to_string(),
                    )
                })
            },
        );
        let authorization = async {
            let authorization = authorization_rx.await.expect("authorization");
            assert_eq!(
                authorization.authorization_url,
                "https://github.com/login/device?user_code=ABCD-1234"
            );
            assert_eq!(authorization.user_code.as_deref(), Some("ABCD-1234"));
            assert_eq!(
                authorization.verification_uri.as_deref(),
                Some("https://github.com/login/device")
            );
        };

        let (completed, ()) = tokio::join!(authorize, authorization);
        let completed = completed.expect("authorize oauth");
        let captured = fixture.server.await.expect("device server");

        assert_eq!(completed.input_key, "API_TOKEN");
        assert_eq!(completed.access_token, "access-token");
        assert_eq!(
            captured.device.form.get("client_id").map(String::as_str),
            Some("device-client")
        );
        assert_eq!(
            captured.device.form.get("scope").map(String::as_str),
            Some("repo read:org")
        );
        assert_eq!(
            captured.token.form.get("grant_type").map(String::as_str),
            Some("urn:ietf:params:oauth:grant-type:device_code")
        );
        assert_eq!(
            captured.token.form.get("device_code").map(String::as_str),
            Some("device-code")
        );
        assert!(!captured.token.form.contains_key("client_secret"));
        assert_eq!(
            completed
                .internal_metadata
                .get(&format!("{}client_id", oauth_metadata_prefix("API_TOKEN")))
                .map(String::as_str),
            Some("device-client")
        );
    }

    #[tokio::test]
    async fn dynamic_registration_device_code_session_authenticates_confidential_client() {
        let registration_fixture = DynamicRegistrationFixture::new(
            r#"{"client_id":"registered-device-client","client_secret":"registered-secret","token_endpoint_auth_method":"client_secret_basic"}"#,
        );
        let fixture = DeviceOAuthFixture::new();
        let mut oauth = device_oauth_spec(&fixture.device_url, &fixture.token_url);
        oauth.client.dynamic_registration = Some(ManifestOAuthDynamicClientRegistrationSpec {
            registration_url: registration_fixture.registration_url.clone(),
            client_name: Some("Coral MCP".to_string()),
            token_endpoint_auth_method:
                ManifestOAuthDynamicClientRegistrationAuthMethod::ClientSecretBasic,
            request_refresh_token_grant: true,
        });
        let service = OAuthCredentialService::new();

        let (authorization_tx, authorization_rx) = oneshot::channel();
        let authorize = service.authorize(
            StartOAuthCredentialRequest {
                input_key: "API_TOKEN",
                oauth: &oauth,
                source_inputs: &EMPTY_SOURCE_INPUTS,
                credential_inputs: Vec::new(),
            },
            move |authorization| async move {
                authorization_tx.send(authorization).map_err(|_error| {
                    crate::bootstrap::AppError::FailedPrecondition(
                        "authorization receiver closed".to_string(),
                    )
                })
            },
        );
        let authorization = async {
            let authorization = authorization_rx.await.expect("authorization");
            assert_eq!(authorization.user_code.as_deref(), Some("ABCD-1234"));
            assert_eq!(
                authorization.verification_uri.as_deref(),
                Some("https://github.com/login/device")
            );
        };

        let (completed, ()) = tokio::join!(authorize, authorization);
        let completed = completed.expect("authorize oauth");
        let registration = registration_fixture
            .server
            .await
            .expect("registration server");
        let captured = fixture.server.await.expect("device server");
        let expected_authorization =
            basic_client_authorization("registered-device-client", "registered-secret");

        let registration_body: Value =
            serde_json::from_str(&registration.body).expect("registration request body");
        assert_eq!(
            registration_body["grant_types"][0],
            "urn:ietf:params:oauth:grant-type:device_code"
        );
        assert_eq!(registration_body["grant_types"][1], "refresh_token");
        assert_eq!(
            registration_body["token_endpoint_auth_method"],
            "client_secret_basic"
        );
        assert_eq!(
            captured.device.authorization.as_deref(),
            Some(expected_authorization.as_str())
        );
        assert!(!captured.device.form.contains_key("client_id"));
        assert_eq!(
            captured.token.authorization.as_deref(),
            Some(expected_authorization.as_str())
        );
        assert!(!captured.device.form.contains_key("client_secret"));
        assert!(!captured.token.form.contains_key("client_secret"));
        assert_eq!(
            completed
                .internal_metadata
                .get(&metadata_key("API_TOKEN", OAuthMetadataKey::ClientSecret))
                .map(String::as_str),
            Some("registered-secret")
        );
        assert_eq!(
            completed
                .internal_metadata
                .get(&metadata_key(
                    "API_TOKEN",
                    OAuthMetadataKey::ClientSecretTransport
                ))
                .map(String::as_str),
            Some("basic_auth")
        );
    }

    #[tokio::test]
    async fn device_code_oauth_session_renders_endpoint_templates_from_source_inputs() {
        let fixture = DeviceOAuthFixture::new();
        let device_url_template = fixture
            .device_url
            .replace("/device/code", "/{{input.OUTLOOK_TENANT_ID}}/device/code");
        let token_url_template = fixture
            .token_url
            .replace("/access_token", "/{{input.OUTLOOK_TENANT_ID}}/access_token");
        let oauth = device_oauth_spec(&device_url_template, &token_url_template);
        let source_inputs =
            BTreeMap::from([("OUTLOOK_TENANT_ID".to_string(), "organizations".to_string())]);
        let rendered_token_url = fixture
            .token_url
            .replace("/access_token", "/organizations/access_token");
        let service = OAuthCredentialService::new();

        let authorize = service.authorize(
            StartOAuthCredentialRequest {
                input_key: "API_TOKEN",
                oauth: &oauth,
                source_inputs: &source_inputs,
                credential_inputs: vec![(
                    "OAUTH_CLIENT_ID".to_string(),
                    "device-client".to_string(),
                )],
            },
            |_authorization| async { Ok(()) },
        );

        let completed = authorize.await.expect("authorize oauth");
        let _captured = fixture.server.await.expect("device server");
        assert_eq!(completed.access_token, "access-token");
        assert_eq!(
            completed
                .internal_metadata
                .get(&format!("{}token_url", oauth_metadata_prefix("API_TOKEN")))
                .map(String::as_str),
            Some(rendered_token_url.as_str())
        );
    }

    #[tokio::test]
    async fn device_code_request_times_out_before_session_start() {
        let listener = StdTcpListener::bind("127.0.0.1:0").expect("device listener");
        let device_url = format!(
            "http://{}/device/code",
            listener.local_addr().expect("addr")
        );
        let server = tokio::task::spawn_blocking(move || {
            let (mut stream, _) = listener.accept().expect("accept device request");
            let request = read_http_request(&mut stream);
            let mut closed = [0_u8; 1];
            match stream.read(&mut closed) {
                Ok(_) | Err(_) => {}
            }
            request
        });
        let oauth = device_oauth_spec(&device_url, "http://127.0.0.1/token");
        let endpoints = preflight_oauth_endpoints(
            &oauth,
            &oauth
                .endpoint_urls(&EMPTY_SOURCE_INPUTS)
                .expect("render endpoints"),
            &EMPTY_SOURCE_INPUTS,
        )
        .expect("preflight endpoints");
        let client = ResolvedOAuthClient {
            client_id: "device-client".to_string(),
            client_secret: None,
            client_secret_transport: None,
            dynamic_client_registration: false,
        };

        let http = token_http_clients(Duration::from_secs(1), None);
        let result = request_device_code(
            &http,
            &oauth,
            &endpoints,
            &client,
            None,
            std::time::Duration::from_millis(50),
        )
        .await;
        let error = match result {
            Ok(_device) => panic!("device request should time out"),
            Err(error) => error,
        };
        let captured = server.await.expect("device server");

        assert!(
            error
                .to_string()
                .contains("OAuth device code request timed out"),
            "unexpected error: {error}"
        );
        assert_eq!(
            captured.form.get("client_id").map(String::as_str),
            Some("device-client")
        );
    }

    #[tokio::test]
    async fn confidential_oauth_session_uses_basic_auth_secret_transport() {
        let fixture = OAuthFixture::new(None);
        let redirect_port = free_loopback_port();
        let oauth = oauth_spec(
            &fixture.token_url,
            redirect_port,
            ManifestOAuthPkceMode::Disabled,
            confidential_client(ManifestOAuthClientSecretTransport::BasicAuth),
        );
        let service = OAuthCredentialService::new();

        let (authorization_tx, authorization_rx) = oneshot::channel();
        let authorize = service.authorize(
            StartOAuthCredentialRequest {
                input_key: "API_TOKEN",
                oauth: &oauth,
                source_inputs: &EMPTY_SOURCE_INPUTS,
                credential_inputs: vec![
                    ("OAUTH_CLIENT_ID".to_string(), "client".to_string()),
                    ("OAUTH_CLIENT_SECRET".to_string(), "secret".to_string()),
                ],
            },
            move |authorization| async move {
                authorization_tx
                    .send(authorization.authorization_url)
                    .map_err(|_authorization_url| {
                        crate::bootstrap::AppError::FailedPrecondition(
                            "authorization receiver closed".to_string(),
                        )
                    })
            },
        );
        let callback = async {
            let authorization_url = authorization_rx.await.expect("authorization url");
            let parsed = Url::parse(&authorization_url).expect("authorization url");
            assert!(!query_pairs(&parsed).contains_key("client_secret"));
            callback(&authorization_url).await;
        };

        let (completed, ()) = tokio::join!(authorize, callback);
        let completed = completed.expect("authorize oauth");
        let captured = fixture.token_server.await.expect("token server");
        assert_eq!(
            captured.authorization.as_deref(),
            Some("Basic Y2xpZW50OnNlY3JldA==")
        );
        assert!(!captured.form.contains_key("client_secret"));
        assert_eq!(
            completed
                .internal_metadata
                .get(&format!(
                    "{}client_secret",
                    oauth_metadata_prefix("API_TOKEN")
                ))
                .map(String::as_str),
            Some("secret")
        );
        assert!(!completed.safe_metadata.contains_key("client_secret"));
    }

    #[tokio::test]
    async fn oauth_callback_accepts_request_split_across_reads() {
        let redirect_port = free_loopback_port();
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", redirect_port))
            .await
            .expect("bind callback listener");
        let oauth = oauth_spec(
            "https://provider.example.com/oauth/token",
            redirect_port,
            ManifestOAuthPkceMode::Disabled,
            ManifestOAuthClientSpec {
                id: ManifestOAuthClientIdSpec {
                    default: Some("client".to_string()),
                    input: None,
                },
                secret: None,
                dynamic_registration: None,
            },
        );
        let endpoints = preflight_oauth_endpoints(
            &oauth,
            &oauth
                .endpoint_urls(&EMPTY_SOURCE_INPUTS)
                .expect("render endpoints"),
            &EMPTY_SOURCE_INPUTS,
        )
        .expect("preflight endpoints");
        let session = AuthorizationCodeSessionConfig {
            common: OAuthSessionCommon {
                input_key: "API_TOKEN".to_string(),
                endpoints,
                client: ResolvedOAuthClient {
                    client_id: "client".to_string(),
                    client_secret: None,
                    client_secret_transport: None,
                    dynamic_client_registration: false,
                },
                resource: None,
            },
            state: "expected-state".to_string(),
            code_verifier: None,
            callback_path: "/oauth/callback".to_string(),
            provider_redirect_uri: format!("http://127.0.0.1:{redirect_port}/oauth/callback"),
            listener,
            expires_at: std::time::Instant::now() + std::time::Duration::from_mins(1),
        };

        let receive = receive_callback(&session);
        let send = async move {
            let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", redirect_port))
                .await
                .expect("connect callback");
            stream
                .write_all(b"GET /oauth/callback?sta")
                .await
                .expect("write partial callback");
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            stream
                .write_all(b"te=expected-state&code=test-code HTTP/1.1\r\nhost: 127.0.0.1\r\n\r\n")
                .await
                .expect("write rest of callback");
            let mut response = Vec::new();
            stream
                .read_to_end(&mut response)
                .await
                .expect("read callback response");
            assert!(
                String::from_utf8_lossy(&response).starts_with("HTTP/1.1 200 OK"),
                "unexpected callback response: {}",
                String::from_utf8_lossy(&response)
            );
        };

        let (callback, ()) = tokio::join!(receive, send);
        assert_eq!(callback.expect("callback").code, "test-code");
    }

    #[tokio::test]
    async fn oauth_callback_accepts_real_callback_after_idle_preconnection() {
        let redirect_port = free_loopback_port();
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", redirect_port))
            .await
            .expect("bind callback listener");
        let oauth = oauth_spec(
            "https://provider.example.com/oauth/token",
            redirect_port,
            ManifestOAuthPkceMode::Disabled,
            ManifestOAuthClientSpec {
                id: ManifestOAuthClientIdSpec {
                    default: Some("client".to_string()),
                    input: None,
                },
                secret: None,
                dynamic_registration: None,
            },
        );
        let endpoints = preflight_oauth_endpoints(
            &oauth,
            &oauth
                .endpoint_urls(&EMPTY_SOURCE_INPUTS)
                .expect("render endpoints"),
            &EMPTY_SOURCE_INPUTS,
        )
        .expect("preflight endpoints");
        let session = AuthorizationCodeSessionConfig {
            common: OAuthSessionCommon {
                input_key: "API_TOKEN".to_string(),
                endpoints,
                client: ResolvedOAuthClient {
                    client_id: "client".to_string(),
                    client_secret: None,
                    client_secret_transport: None,
                    dynamic_client_registration: false,
                },
                resource: None,
            },
            state: "expected-state".to_string(),
            code_verifier: None,
            callback_path: "/oauth/callback".to_string(),
            provider_redirect_uri: format!("http://127.0.0.1:{redirect_port}/oauth/callback"),
            listener,
            expires_at: std::time::Instant::now() + std::time::Duration::from_mins(1),
        };

        let receive = receive_callback(&session);
        let send = async move {
            let _idle = tokio::net::TcpStream::connect(("127.0.0.1", redirect_port))
                .await
                .expect("connect idle preconnection");
            let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", redirect_port))
                .await
                .expect("connect callback");
            stream
                .write_all(
                    b"GET /oauth/callback?state=expected-state&code=test-code HTTP/1.1\r\nhost: 127.0.0.1\r\n\r\n",
                )
                .await
                .expect("write callback");
            let mut response = Vec::new();
            stream
                .read_to_end(&mut response)
                .await
                .expect("read callback response");
            assert!(
                String::from_utf8_lossy(&response).starts_with("HTTP/1.1 200 OK"),
                "unexpected callback response: {}",
                String::from_utf8_lossy(&response)
            );
        };

        let (callback, ()) = tokio::join!(receive, send);
        assert_eq!(callback.expect("callback").code, "test-code");
    }

    #[tokio::test]
    async fn confidential_oauth_session_uses_request_body_secret_transport() {
        let fixture = OAuthFixture::new(None);
        let redirect_port = free_loopback_port();
        let oauth = oauth_spec(
            &fixture.token_url,
            redirect_port,
            ManifestOAuthPkceMode::Disabled,
            confidential_client(ManifestOAuthClientSecretTransport::RequestBody),
        );
        let service = OAuthCredentialService::new();

        let (authorization_tx, authorization_rx) = oneshot::channel();
        let authorize = service.authorize(
            StartOAuthCredentialRequest {
                input_key: "API_TOKEN",
                oauth: &oauth,
                source_inputs: &EMPTY_SOURCE_INPUTS,
                credential_inputs: vec![
                    ("OAUTH_CLIENT_ID".to_string(), "client".to_string()),
                    ("OAUTH_CLIENT_SECRET".to_string(), "secret".to_string()),
                ],
            },
            move |authorization| async move {
                authorization_tx
                    .send(authorization.authorization_url)
                    .map_err(|_authorization_url| {
                        crate::bootstrap::AppError::FailedPrecondition(
                            "authorization receiver closed".to_string(),
                        )
                    })
            },
        );
        let callback = async {
            let authorization_url = authorization_rx.await.expect("authorization url");
            callback(&authorization_url).await;
        };

        let (completed, ()) = tokio::join!(authorize, callback);
        completed.expect("authorize oauth");
        let captured = fixture.token_server.await.expect("token server");
        assert!(captured.authorization.is_none());
        assert_eq!(
            captured.form.get("client_secret").map(String::as_str),
            Some("secret")
        );
    }

    #[tokio::test]
    async fn random_redirect_port_is_used_for_authorization_callback_and_token_exchange() {
        let fixture = OAuthFixture::new(None);
        let oauth = oauth_spec_with_redirect_uri(
            &fixture.token_url,
            "http://127.0.0.1/oauth/callback",
            ManifestOAuthRedirectUriPortMode::Random,
            ManifestOAuthPkceMode::Required,
            ManifestOAuthClientSpec {
                id: ManifestOAuthClientIdSpec {
                    default: Some("default-client".to_string()),
                    input: None,
                },
                secret: None,
                dynamic_registration: None,
            },
        );
        let service = OAuthCredentialService::new();

        let (authorization_tx, authorization_rx) = oneshot::channel();
        let authorize = service.authorize(
            StartOAuthCredentialRequest {
                input_key: "API_TOKEN",
                oauth: &oauth,
                source_inputs: &EMPTY_SOURCE_INPUTS,
                credential_inputs: Vec::new(),
            },
            move |authorization| async move {
                authorization_tx
                    .send(authorization.authorization_url)
                    .map_err(|_authorization_url| {
                        crate::bootstrap::AppError::FailedPrecondition(
                            "authorization receiver closed".to_string(),
                        )
                    })
            },
        );
        let callback = async {
            let authorization_url = authorization_rx.await.expect("authorization url");
            let authorization_url = Url::parse(&authorization_url).expect("authorization url");
            let query = query_pairs(&authorization_url);
            let redirect_uri =
                Url::parse(query.get("redirect_uri").expect("redirect uri")).expect("redirect uri");
            let redirect_port = redirect_uri.port().expect("assigned redirect port");
            assert_ne!(redirect_port, 0);

            callback(authorization_url.as_str()).await;
            redirect_uri
        };
        let (completed, redirect_uri) = tokio::join!(authorize, callback);
        completed.expect("authorize oauth");

        let captured = fixture.token_server.await.expect("token server");
        assert_eq!(
            captured.form.get("redirect_uri").map(String::as_str),
            Some(redirect_uri.as_str())
        );
    }

    #[tokio::test]
    async fn fixed_redirect_uri_is_sent_exactly_as_authored() {
        let fixture = OAuthFixture::new(None);
        let redirect_port = free_loopback_port();
        let redirect_uri = format!("http://127.0.0.1:{redirect_port}");
        let oauth = oauth_spec_with_redirect_uri(
            &fixture.token_url,
            &redirect_uri,
            ManifestOAuthRedirectUriPortMode::Fixed,
            ManifestOAuthPkceMode::Required,
            ManifestOAuthClientSpec {
                id: ManifestOAuthClientIdSpec {
                    default: Some("default-client".to_string()),
                    input: None,
                },
                secret: None,
                dynamic_registration: None,
            },
        );
        let service = OAuthCredentialService::new();

        let (authorization_tx, authorization_rx) = oneshot::channel();
        let authorize = service.authorize(
            StartOAuthCredentialRequest {
                input_key: "API_TOKEN",
                oauth: &oauth,
                source_inputs: &EMPTY_SOURCE_INPUTS,
                credential_inputs: Vec::new(),
            },
            move |authorization| async move {
                authorization_tx
                    .send(authorization.authorization_url)
                    .map_err(|_authorization_url| {
                        crate::bootstrap::AppError::FailedPrecondition(
                            "authorization receiver closed".to_string(),
                        )
                    })
            },
        );
        let callback = async {
            let authorization_url = authorization_rx.await.expect("authorization url");
            let authorization_url = Url::parse(&authorization_url).expect("authorization url");
            let query = query_pairs(&authorization_url);
            assert_eq!(
                query.get("redirect_uri").map(String::as_str),
                Some(redirect_uri.as_str())
            );

            callback(authorization_url.as_str()).await;
        };
        let (completed, ()) = tokio::join!(authorize, callback);
        completed.expect("authorize oauth");

        let captured = fixture.token_server.await.expect("token server");
        assert_eq!(
            captured.form.get("redirect_uri").map(String::as_str),
            Some(redirect_uri.as_str())
        );
    }

    async fn callback(authorization_url: &str) {
        let authorization_url = Url::parse(authorization_url).expect("authorization url");
        let mut query = query_pairs(&authorization_url);
        let state = query.remove("state").expect("state");
        let mut callback_url =
            Url::parse(query.get("redirect_uri").expect("redirect uri")).expect("redirect uri");
        callback_url
            .query_pairs_mut()
            .append_pair("state", &state)
            .append_pair("code", "test-code");
        reqwest::get(callback_url)
            .await
            .expect("callback response")
            .error_for_status()
            .expect("callback success");
    }

    fn oauth_spec_with_rendered_endpoint(
        field: &str,
        redirect_port: u16,
    ) -> ManifestOAuthCredentialSpec {
        let mut oauth = if field == "device_authorization" {
            let mut oauth = device_oauth_spec(
                "https://provider.example/oauth/device",
                "https://provider.example/oauth/token",
            );
            oauth.client.id = ManifestOAuthClientIdSpec {
                default: Some("default-client".to_string()),
                input: None,
            };
            oauth
        } else {
            oauth_spec(
                "https://provider.example/oauth/token",
                redirect_port,
                ManifestOAuthPkceMode::Required,
                ManifestOAuthClientSpec {
                    id: ManifestOAuthClientIdSpec {
                        default: Some("default-client".to_string()),
                        input: None,
                    },
                    secret: None,
                    dynamic_registration: None,
                },
            )
        };
        match field {
            "authorization" => {
                oauth.authorization_url = Some("{{input.OAUTH_ENDPOINT}}".to_string());
            }
            "device_authorization" => {
                oauth.device_authorization_url = Some("{{input.OAUTH_ENDPOINT}}".to_string());
            }
            "token" => oauth.token_url = "{{input.OAUTH_ENDPOINT}}".to_string(),
            _ => panic!("unknown endpoint field: {field}"),
        }
        oauth
    }

    fn oauth_spec(
        token_url: &str,
        redirect_port: u16,
        pkce: ManifestOAuthPkceMode,
        client: ManifestOAuthClientSpec,
    ) -> ManifestOAuthCredentialSpec {
        oauth_spec_with_redirect_uri(
            token_url,
            &format!("http://127.0.0.1:{redirect_port}/oauth/callback"),
            ManifestOAuthRedirectUriPortMode::Fixed,
            pkce,
            client,
        )
    }

    fn dynamic_registration_oauth_spec(
        redirect_port: u16,
        token_url: &str,
        registration_url: &str,
    ) -> ManifestOAuthCredentialSpec {
        let mut oauth = oauth_spec(
            token_url,
            redirect_port,
            ManifestOAuthPkceMode::Required,
            ManifestOAuthClientSpec {
                id: ManifestOAuthClientIdSpec {
                    default: None,
                    input: Some("OAUTH_CLIENT_ID".to_string()),
                },
                secret: None,
                dynamic_registration: Some(ManifestOAuthDynamicClientRegistrationSpec {
                    registration_url: registration_url.to_string(),
                    client_name: Some("Coral MCP".to_string()),
                    token_endpoint_auth_method:
                        ManifestOAuthDynamicClientRegistrationAuthMethod::None,
                    request_refresh_token_grant: true,
                }),
            },
        );
        oauth.resource = Some("https://mcp.example.com/mcp".to_string());
        oauth
    }

    fn assert_dynamic_registration_request(
        registration: &CapturedTokenRequest,
        redirect_port: u16,
    ) {
        assert_eq!(registration.authorization, None);
        assert_eq!(
            registration.headers.get("content-type").map(String::as_str),
            Some("application/json")
        );
        let body: Value =
            serde_json::from_str(&registration.body).expect("registration request body");
        assert_eq!(body["client_name"], "Coral MCP");
        assert_eq!(body["application_type"], "native");
        assert_eq!(body["token_endpoint_auth_method"], "none");
        assert_eq!(body["grant_types"][0], "authorization_code");
        assert_eq!(body["grant_types"][1], "refresh_token");
        assert_eq!(body["scope"], "repo read:org");
        assert_eq!(
            body["redirect_uris"][0],
            format!("http://127.0.0.1:{redirect_port}/oauth/callback")
        );
    }

    fn oauth_spec_with_redirect_uri(
        token_url: &str,
        redirect_uri: &str,
        redirect_uri_port_mode: ManifestOAuthRedirectUriPortMode,
        pkce: ManifestOAuthPkceMode,
        client: ManifestOAuthClientSpec,
    ) -> ManifestOAuthCredentialSpec {
        ManifestOAuthCredentialSpec {
            flow: ManifestOAuthFlowSpec {
                kind: ManifestOAuthFlowKind::AuthorizationCode,
                pkce,
            },
            resource: None,
            redirect_uri: Some(redirect_uri.to_string()),
            redirect_uri_port_mode,
            authorization_url: Some("https://provider.example.com/oauth/authorize".to_string()),
            device_authorization_url: None,
            token_url: token_url.to_string(),
            client,
            scopes: Some(ManifestOAuthScopesSpec {
                scope: ManifestOAuthScopeSpec {
                    delimiter: ManifestOAuthScopeDelimiter::Space,
                    values: vec!["repo".to_string(), "read:org".to_string()],
                },
            }),
        }
    }

    fn device_oauth_spec(device_url: &str, token_url: &str) -> ManifestOAuthCredentialSpec {
        ManifestOAuthCredentialSpec {
            flow: ManifestOAuthFlowSpec {
                kind: ManifestOAuthFlowKind::DeviceCode,
                pkce: ManifestOAuthPkceMode::Disabled,
            },
            resource: None,
            redirect_uri: None,
            redirect_uri_port_mode: ManifestOAuthRedirectUriPortMode::Fixed,
            authorization_url: None,
            device_authorization_url: Some(device_url.to_string()),
            token_url: token_url.to_string(),
            client: ManifestOAuthClientSpec {
                id: ManifestOAuthClientIdSpec {
                    default: None,
                    input: Some("OAUTH_CLIENT_ID".to_string()),
                },
                secret: None,
                dynamic_registration: None,
            },
            scopes: Some(ManifestOAuthScopesSpec {
                scope: ManifestOAuthScopeSpec {
                    delimiter: ManifestOAuthScopeDelimiter::Space,
                    values: vec!["repo".to_string(), "read:org".to_string()],
                },
            }),
        }
    }

    fn confidential_client(
        transport: ManifestOAuthClientSecretTransport,
    ) -> ManifestOAuthClientSpec {
        ManifestOAuthClientSpec {
            id: ManifestOAuthClientIdSpec {
                default: None,
                input: Some("OAUTH_CLIENT_ID".to_string()),
            },
            secret: Some(ManifestOAuthClientSecretSpec {
                input: "OAUTH_CLIENT_SECRET".to_string(),
                transport,
            }),
            dynamic_registration: None,
        }
    }

    fn query_pairs(url: &Url) -> BTreeMap<String, String> {
        url.query_pairs().into_owned().collect()
    }

    fn free_loopback_port() -> u16 {
        StdTcpListener::bind("127.0.0.1:0")
            .expect("bind free port")
            .local_addr()
            .expect("addr")
            .port()
    }

    struct OAuthFixture {
        token_url: String,
        token_server: JoinHandle<CapturedTokenRequest>,
    }

    impl OAuthFixture {
        fn new(response_body: Option<&'static str>) -> Self {
            let token_listener = StdTcpListener::bind("127.0.0.1:0").expect("token listener");
            let token_url = format!(
                "http://{}/token",
                token_listener.local_addr().expect("addr")
            );
            let token_server = tokio::task::spawn_blocking(move || {
                let (mut stream, _) = token_listener.accept().expect("accept token request");
                let request = read_http_request(&mut stream);
                let response_body = response_body.unwrap_or(
                    r#"{"access_token":"access-token","refresh_token":"refresh-token","token_type":"Bearer","scope":"repo read:org","expires_in":3600}"#,
                );
                let response = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{response_body}",
                    response_body.len()
                );
                stream
                    .write_all(response.as_bytes())
                    .expect("write token response");
                request
            });
            Self {
                token_url,
                token_server,
            }
        }
    }

    struct DynamicRegistrationFixture {
        registration_url: String,
        server: JoinHandle<CapturedTokenRequest>,
    }

    impl DynamicRegistrationFixture {
        fn new(response_body: &'static str) -> Self {
            let listener = StdTcpListener::bind("127.0.0.1:0").expect("registration listener");
            let registration_url = format!(
                "http://{}/register",
                listener.local_addr().expect("registration addr")
            );
            let server = tokio::task::spawn_blocking(move || {
                let (mut stream, _) = listener.accept().expect("accept registration request");
                let request = read_http_request(&mut stream);
                write_json_response(&mut stream, response_body);
                request
            });
            Self {
                registration_url,
                server,
            }
        }
    }

    struct DeviceOAuthFixture {
        device_url: String,
        token_url: String,
        server: JoinHandle<CapturedDeviceFlowRequests>,
    }

    impl DeviceOAuthFixture {
        fn new() -> Self {
            let listener = StdTcpListener::bind("127.0.0.1:0").expect("device listener");
            let base_url = format!("http://{}", listener.local_addr().expect("addr"));
            let device_url = format!("{base_url}/device/code");
            let token_url = format!("{base_url}/access_token");
            let server = tokio::task::spawn_blocking(move || {
                let (mut device_stream, _) = listener.accept().expect("accept device request");
                let device = read_http_request(&mut device_stream);
                let device_body = r#"{"device_code":"device-code","user_code":"ABCD-1234","verification_uri":"https://github.com/login/device","verification_uri_complete":"https://github.com/login/device?user_code=ABCD-1234","expires_in":900,"interval":1}"#;
                write_json_response(&mut device_stream, device_body);

                let (mut token_stream, _) = listener.accept().expect("accept token request");
                let token = read_http_request(&mut token_stream);
                let token_body = r#"{"access_token":"access-token","token_type":"Bearer","scope":"repo read:org"}"#;
                write_json_response(&mut token_stream, token_body);

                CapturedDeviceFlowRequests { device, token }
            });
            Self {
                device_url,
                token_url,
                server,
            }
        }
    }

    struct CapturedDeviceFlowRequests {
        device: CapturedTokenRequest,
        token: CapturedTokenRequest,
    }

    struct CapturedTokenRequest {
        authorization: Option<String>,
        headers: BTreeMap<String, String>,
        body: String,
        form: BTreeMap<String, String>,
    }

    fn read_http_request(stream: &mut std::net::TcpStream) -> CapturedTokenRequest {
        let mut buffer = Vec::new();
        let mut temp = [0_u8; 1024];
        loop {
            let read = stream.read(&mut temp).expect("read token request");
            if read == 0 {
                break;
            }
            buffer.extend_from_slice(&temp[..read]);
            if buffer.windows(4).any(|window| window == b"\r\n\r\n") {
                let header_end = buffer
                    .windows(4)
                    .position(|window| window == b"\r\n\r\n")
                    .expect("header end")
                    + 4;
                let headers = String::from_utf8_lossy(&buffer[..header_end]);
                let content_length = headers
                    .lines()
                    .find_map(|line| line.strip_prefix("content-length: "))
                    .or_else(|| {
                        headers
                            .lines()
                            .find_map(|line| line.strip_prefix("Content-Length: "))
                    })
                    .and_then(|value| value.parse::<usize>().ok())
                    .unwrap_or(0);
                while buffer.len() < header_end + content_length {
                    let read = stream.read(&mut temp).expect("read token body");
                    if read == 0 {
                        break;
                    }
                    buffer.extend_from_slice(&temp[..read]);
                }
                break;
            }
        }
        let raw = String::from_utf8_lossy(&buffer);
        let (headers, body) = raw.split_once("\r\n\r\n").expect("split request");
        let parsed_headers = headers
            .lines()
            .filter_map(|line| {
                let (name, value) = line.split_once(": ")?;
                Some((name.to_ascii_lowercase(), value.to_string()))
            })
            .collect();
        let authorization = headers.lines().find_map(|line| {
            line.strip_prefix("authorization: ")
                .or_else(|| line.strip_prefix("Authorization: "))
                .map(ToString::to_string)
        });
        let form = url::form_urlencoded::parse(body.as_bytes())
            .into_owned()
            .collect();
        CapturedTokenRequest {
            authorization,
            headers: parsed_headers,
            body: body.to_string(),
            form,
        }
    }

    fn write_json_response(stream: &mut std::net::TcpStream, body: &str) {
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
            body.len()
        );
        stream
            .write_all(response.as_bytes())
            .expect("write json response");
    }
}
