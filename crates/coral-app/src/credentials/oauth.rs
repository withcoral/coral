//! App-owned OAuth credential retrieval runner.

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
    ManifestOAuthClientSecretTransport, ManifestOAuthCredentialSpec, ManifestOAuthPkceMode,
    ManifestOAuthScopeDelimiter,
};
use reqwest::header::{ACCEPT, AUTHORIZATION};
use serde_json::Value;
use sha2::{Digest as _, Sha256};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use url::{Url, form_urlencoded};
use uuid::Uuid;

use crate::bootstrap::AppError;
use crate::credentials::OAUTH_INTERNAL_KEY_PREFIX;

const SESSION_TTL: Duration = Duration::from_mins(10);
const MAX_CALLBACK_BYTES: usize = 8 * 1024;

#[derive(Clone)]
pub(crate) struct OAuthCredentialManager {
    http: reqwest::Client,
}

pub(crate) struct StartOAuthCredentialRequest<'a> {
    pub(crate) input_key: &'a str,
    pub(crate) oauth: &'a ManifestOAuthCredentialSpec,
    pub(crate) credential_inputs: Vec<(String, String)>,
}

pub(crate) struct OAuthAuthorization {
    pub(crate) authorization_url: String,
    pub(crate) expires_in_seconds: u64,
}

#[derive(Clone)]
pub(crate) struct OAuthCredentialMaterial {
    pub(crate) input_key: String,
    pub(crate) access_token: String,
    pub(crate) internal_metadata: BTreeMap<String, String>,
    pub(crate) safe_metadata: BTreeMap<String, String>,
}

struct OAuthSessionConfig {
    input_key: String,
    oauth: ManifestOAuthCredentialSpec,
    client_id: String,
    client_secret: Option<String>,
    state: String,
    code_verifier: Option<String>,
    redirect_uri: Url,
    listener: TcpListener,
    expires_at: Instant,
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

struct TokenResponse {
    access_token: String,
    refresh_token: Option<String>,
    token_type: Option<String>,
    scope: Option<String>,
    expires_at: Option<DateTime<Utc>>,
}

impl OAuthCredentialManager {
    pub(crate) fn new() -> Self {
        Self {
            http: reqwest::Client::new(),
        }
    }

    pub(crate) async fn authorize<F, Fut>(
        &self,
        request: StartOAuthCredentialRequest<'_>,
        on_authorization: F,
    ) -> Result<OAuthCredentialMaterial, AppError>
    where
        F: FnOnce(OAuthAuthorization) -> Fut,
        Fut: Future<Output = Result<(), AppError>>,
    {
        let oauth = request.oauth.clone();
        let credential_inputs = normalize_credential_inputs(request.credential_inputs)?;
        reject_unknown_credential_inputs(&oauth, &credential_inputs)?;
        let client_id = resolve_client_id(&oauth, &credential_inputs)?;
        let client_secret = resolve_client_secret(&oauth, &credential_inputs)?;
        let redirect_uri = Url::parse(&oauth.redirect_uri).map_err(|error| {
            AppError::InvalidInput(format!("invalid OAuth redirect URI: {error}"))
        })?;
        let listener = bind_redirect_listener(&redirect_uri).await?;
        let state = random_token();
        let code_verifier =
            (oauth.flow.pkce == ManifestOAuthPkceMode::Required).then(random_code_verifier);
        let authorization_url =
            build_authorization_url(&oauth, &client_id, &state, code_verifier.as_deref())?;
        let expires_at = Instant::now() + SESSION_TTL;
        let session = OAuthSessionConfig {
            input_key: request.input_key.to_string(),
            oauth,
            client_id,
            client_secret,
            state,
            code_verifier,
            redirect_uri,
            listener,
            expires_at,
        };
        on_authorization(OAuthAuthorization {
            authorization_url,
            expires_in_seconds: SESSION_TTL.as_secs(),
        })
        .await?;
        self.run_session(session).await
    }

    pub(crate) fn validate_credential_inputs(
        oauth: &ManifestOAuthCredentialSpec,
        credential_inputs: Vec<(String, String)>,
    ) -> Result<(), AppError> {
        let credential_inputs = normalize_credential_inputs(credential_inputs)?;
        reject_unknown_credential_inputs(oauth, &credential_inputs)?;
        let _client_id = resolve_client_id(oauth, &credential_inputs)?;
        let _client_secret = resolve_client_secret(oauth, &credential_inputs)?;
        Url::parse(&oauth.redirect_uri).map_err(|error| {
            AppError::InvalidInput(format!("invalid OAuth redirect URI: {error}"))
        })?;
        Ok(())
    }

    async fn run_session(
        &self,
        session: OAuthSessionConfig,
    ) -> Result<OAuthCredentialMaterial, AppError> {
        let deadline = tokio::time::Instant::from_std(session.expires_at);
        let callback = tokio::time::timeout_at(deadline, receive_callback(&session))
            .await
            .map_err(|_elapsed| expired_session_error(&session.input_key))??;
        let token = tokio::time::timeout_at(
            deadline,
            exchange_authorization_code(&self.http, &session, &callback.code),
        )
        .await
        .map_err(|_elapsed| expired_session_error(&session.input_key))??;
        Ok(oauth_credential_material(&session, &token))
    }
}

fn normalize_credential_inputs(
    inputs: Vec<(String, String)>,
) -> Result<BTreeMap<String, String>, AppError> {
    let mut normalized = BTreeMap::new();
    for (key, value) in inputs {
        let key = normalize_credential_input_key(&key)?;
        if normalized.insert(key.clone(), value).is_some() {
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

fn resolve_client_id(
    oauth: &ManifestOAuthCredentialSpec,
    inputs: &BTreeMap<String, String>,
) -> Result<String, AppError> {
    if let Some(input_key) = oauth.client.id.input.as_deref()
        && let Some(value) = inputs.get(input_key)
        && !value.is_empty()
    {
        return Ok(value.clone());
    }
    if let Some(default) = oauth.client.id.default.as_deref()
        && !default.is_empty()
    {
        return Ok(default.to_string());
    }
    let detail = oauth
        .client
        .id
        .input
        .as_deref()
        .map_or("client ID".to_string(), |input| {
            format!("client ID input '{input}'")
        });
    Err(AppError::FailedPrecondition(format!(
        "missing OAuth {detail}"
    )))
}

fn resolve_client_secret(
    oauth: &ManifestOAuthCredentialSpec,
    inputs: &BTreeMap<String, String>,
) -> Result<Option<String>, AppError> {
    let Some(secret) = oauth.client.secret.as_ref() else {
        return Ok(None);
    };
    let Some(value) = inputs.get(&secret.input).filter(|value| !value.is_empty()) else {
        return Err(AppError::FailedPrecondition(format!(
            "missing OAuth client secret input '{}'",
            secret.input
        )));
    };
    Ok(Some(value.clone()))
}

async fn bind_redirect_listener(redirect_uri: &Url) -> Result<TcpListener, AppError> {
    let host = redirect_uri
        .host_str()
        .ok_or_else(|| AppError::InvalidInput("OAuth redirect URI is missing host".to_string()))?;
    let port = redirect_uri.port().ok_or_else(|| {
        AppError::InvalidInput("OAuth redirect URI is missing explicit port".to_string())
    })?;
    if host != "127.0.0.1" && host != "localhost" {
        return Err(AppError::InvalidInput(
            "OAuth redirect URI must use a loopback host".to_string(),
        ));
    }
    TcpListener::bind((host, port)).await.map_err(|error| {
        AppError::FailedPrecondition(format!(
            "OAuth callback listener could not bind {host}:{port}: {error}"
        ))
    })
}

fn build_authorization_url(
    oauth: &ManifestOAuthCredentialSpec,
    client_id: &str,
    state: &str,
    code_verifier: Option<&str>,
) -> Result<String, AppError> {
    let mut url = Url::parse(&oauth.authorization_url).map_err(|error| {
        AppError::InvalidInput(format!("invalid OAuth authorization URL: {error}"))
    })?;
    {
        let mut query = url.query_pairs_mut();
        query
            .append_pair("response_type", "code")
            .append_pair("client_id", client_id)
            .append_pair("redirect_uri", &oauth.redirect_uri)
            .append_pair("state", state);
        if let Some(scopes) = oauth.scopes.as_ref() {
            query.append_pair(
                "scope",
                &join_scope_values(scopes.scope.delimiter, &scopes.scope.values),
            );
        }
        if let Some(verifier) = code_verifier {
            query
                .append_pair("code_challenge", &pkce_challenge(verifier))
                .append_pair("code_challenge_method", "S256");
        }
    }
    Ok(url.to_string())
}

fn join_scope_values(delimiter: ManifestOAuthScopeDelimiter, values: &[String]) -> String {
    let separator = match delimiter {
        ManifestOAuthScopeDelimiter::Space => " ",
        ManifestOAuthScopeDelimiter::Comma => ",",
    };
    values.join(separator)
}

fn random_token() -> String {
    format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple())
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

async fn receive_callback(session: &OAuthSessionConfig) -> Result<Callback, AppError> {
    let (result_tx, mut result_rx) = mpsc::channel(8);
    let deadline = tokio::time::Instant::from_std(session.expires_at);
    loop {
        tokio::select! {
            accepted = session.listener.accept() => {
                let (mut stream, _peer): (_, SocketAddr) = accepted?;
                let result_tx = result_tx.clone();
                let expected_path = session.redirect_uri.path().to_string();
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
            let page = callback_page("OAuth complete. You can return to Coral.");
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

async fn exchange_authorization_code(
    http: &reqwest::Client,
    session: &OAuthSessionConfig,
    code: &str,
) -> Result<TokenResponse, AppError> {
    let mut form = vec![
        ("grant_type", "authorization_code".to_string()),
        ("code", code.to_string()),
        ("redirect_uri", session.oauth.redirect_uri.clone()),
    ];
    let mut request = http
        .post(&session.oauth.token_url)
        .header(ACCEPT, "application/json");
    match (
        session.client_secret.as_deref(),
        session
            .oauth
            .client
            .secret
            .as_ref()
            .map(|secret| secret.transport),
    ) {
        (Some(secret), Some(ManifestOAuthClientSecretTransport::BasicAuth)) => {
            request = request.header(
                AUTHORIZATION,
                basic_client_authorization(&session.client_id, secret),
            );
        }
        (Some(secret), Some(ManifestOAuthClientSecretTransport::RequestBody)) => {
            form.push(("client_id", session.client_id.clone()));
            form.push(("client_secret", secret.to_string()));
        }
        (None, None) => {
            form.push(("client_id", session.client_id.clone()));
        }
        _ => {
            return Err(AppError::FailedPrecondition(
                "OAuth client secret configuration was incomplete".to_string(),
            ));
        }
    }
    if let Some(verifier) = session.code_verifier.as_deref() {
        form.push(("code_verifier", verifier.to_string()));
    }
    let response = request.form(&form).send().await.map_err(|error| {
        AppError::FailedPrecondition(format!("OAuth token exchange request failed: {error}"))
    })?;
    let status = response.status();
    let body = response.text().await.map_err(|error| {
        AppError::FailedPrecondition(format!("OAuth token exchange response failed: {error}"))
    })?;
    if !status.is_success() {
        return Err(AppError::FailedPrecondition(format!(
            "OAuth token exchange failed with HTTP {status}: {}",
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
    let access_token = body
        .get("access_token")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            AppError::FailedPrecondition(
                "OAuth token response did not include access_token".to_string(),
            )
        })?
        .to_string();
    let refresh_token = body
        .get("refresh_token")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string);
    let token_type = body
        .get("token_type")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string);
    let scope = body
        .get("scope")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
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

fn oauth_credential_material(
    session: &OAuthSessionConfig,
    token: &TokenResponse,
) -> OAuthCredentialMaterial {
    let prefix = oauth_metadata_prefix(&session.input_key);
    let mut internal_metadata = BTreeMap::new();
    internal_metadata.insert(format!("{prefix}method"), "oauth".to_string());
    if let Some(expires_at) = token.expires_at {
        internal_metadata.insert(
            format!("{prefix}access_token_expires_at"),
            expires_at.to_rfc3339(),
        );
    }
    if let Some(refresh_token) = token.refresh_token.as_deref() {
        internal_metadata.insert(format!("{prefix}refresh_token"), refresh_token.to_string());
    }
    if let Some(token_type) = token.token_type.as_deref() {
        internal_metadata.insert(format!("{prefix}token_type"), token_type.to_string());
    }
    if let Some(scope) = token.scope.as_deref() {
        internal_metadata.insert(format!("{prefix}scope"), scope.to_string());
    }
    internal_metadata.insert(format!("{prefix}client_id"), session.client_id.clone());
    internal_metadata.insert(
        format!("{prefix}token_url"),
        session.oauth.token_url.clone(),
    );
    if let Some(secret) = session.oauth.client.secret.as_ref() {
        internal_metadata.insert(
            format!("{prefix}client_secret_transport"),
            client_secret_transport_label(secret.transport).to_string(),
        );
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

fn client_secret_transport_label(transport: ManifestOAuthClientSecretTransport) -> &'static str {
    match transport {
        ManifestOAuthClientSecretTransport::BasicAuth => "basic_auth",
        ManifestOAuthClientSecretTransport::RequestBody => "request_body",
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

    use super::{
        OAuthCredentialManager, OAuthSessionConfig, StartOAuthCredentialRequest,
        basic_client_authorization, join_scope_values, material_key_belongs_to_input,
        oauth_metadata_prefix, parse_token_response, pkce_challenge, receive_callback,
    };
    use coral_spec::{
        ManifestOAuthClientIdSpec, ManifestOAuthClientSecretSpec,
        ManifestOAuthClientSecretTransport, ManifestOAuthClientSpec, ManifestOAuthCredentialSpec,
        ManifestOAuthFlowKind, ManifestOAuthFlowSpec, ManifestOAuthPkceMode,
        ManifestOAuthScopeDelimiter, ManifestOAuthScopeSpec, ManifestOAuthScopesSpec,
    };
    use tokio::sync::oneshot;
    use tokio::task::JoinHandle;
    use tokio::{io::AsyncReadExt as _, io::AsyncWriteExt as _};
    use url::Url;

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
    fn oauth_metadata_key_matching_is_exact_for_dotted_inputs() {
        let dotted_key = format!("{}refresh_token", oauth_metadata_prefix("A.B"));

        assert!(material_key_belongs_to_input(&dotted_key, "A.B"));
        assert!(!material_key_belongs_to_input(&dotted_key, "A"));
    }

    #[test]
    fn token_response_ignores_unrepresentable_expires_in() {
        let token = parse_token_response(
            r#"{"access_token":"access-token","expires_in":9223372036854775807}"#,
        )
        .expect("parse token response");

        assert_eq!(token.access_token, "access-token");
        assert!(token.expires_at.is_none());
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
            },
        );
        let manager = OAuthCredentialManager::new();

        let (authorization_tx, authorization_rx) = oneshot::channel();
        let authorize = manager.authorize(
            StartOAuthCredentialRequest {
                input_key: "API_TOKEN",
                oauth: &oauth,
                credential_inputs: vec![(
                    "OAUTH_CLIENT_ID".to_string(),
                    "override-client".to_string(),
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
        assert_eq!(
            completed
                .internal_metadata
                .get(&format!(
                    "{}refresh_token",
                    oauth_metadata_prefix("API_TOKEN")
                ))
                .map(String::as_str),
            Some("refresh-token")
        );
        assert_eq!(
            completed
                .internal_metadata
                .get(&format!("{}client_id", oauth_metadata_prefix("API_TOKEN")))
                .map(String::as_str),
            Some("override-client")
        );
        assert_eq!(
            completed.safe_metadata.get("scope").map(String::as_str),
            Some("repo read:org")
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
        let manager = OAuthCredentialManager::new();

        let (authorization_tx, authorization_rx) = oneshot::channel();
        let authorize = manager.authorize(
            StartOAuthCredentialRequest {
                input_key: "API_TOKEN",
                oauth: &oauth,
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
            callback(&authorization_url, redirect_port).await;
        };

        let (completed, ()) = tokio::join!(authorize, callback);
        completed.expect("authorize oauth");
        let captured = fixture.token_server.await.expect("token server");
        assert_eq!(
            captured.authorization.as_deref(),
            Some("Basic Y2xpZW50OnNlY3JldA==")
        );
        assert!(!captured.form.contains_key("client_secret"));
    }

    #[tokio::test]
    async fn oauth_callback_accepts_request_split_across_reads() {
        let redirect_port = free_loopback_port();
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", redirect_port))
            .await
            .expect("bind callback listener");
        let session = OAuthSessionConfig {
            input_key: "API_TOKEN".to_string(),
            oauth: oauth_spec(
                "https://provider.example.com/oauth/token",
                redirect_port,
                ManifestOAuthPkceMode::Disabled,
                ManifestOAuthClientSpec {
                    id: ManifestOAuthClientIdSpec {
                        default: Some("client".to_string()),
                        input: None,
                    },
                    secret: None,
                },
            ),
            client_id: "client".to_string(),
            client_secret: None,
            state: "expected-state".to_string(),
            code_verifier: None,
            redirect_uri: Url::parse(&format!("http://127.0.0.1:{redirect_port}/oauth/callback"))
                .expect("redirect uri"),
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
        let session = OAuthSessionConfig {
            input_key: "API_TOKEN".to_string(),
            oauth: oauth_spec(
                "https://provider.example.com/oauth/token",
                redirect_port,
                ManifestOAuthPkceMode::Disabled,
                ManifestOAuthClientSpec {
                    id: ManifestOAuthClientIdSpec {
                        default: Some("client".to_string()),
                        input: None,
                    },
                    secret: None,
                },
            ),
            client_id: "client".to_string(),
            client_secret: None,
            state: "expected-state".to_string(),
            code_verifier: None,
            redirect_uri: Url::parse(&format!("http://127.0.0.1:{redirect_port}/oauth/callback"))
                .expect("redirect uri"),
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
        let manager = OAuthCredentialManager::new();

        let (authorization_tx, authorization_rx) = oneshot::channel();
        let authorize = manager.authorize(
            StartOAuthCredentialRequest {
                input_key: "API_TOKEN",
                oauth: &oauth,
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
            callback(&authorization_url, redirect_port).await;
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

    async fn callback(authorization_url: &str, redirect_port: u16) {
        let authorization_url = Url::parse(authorization_url).expect("authorization url");
        let state = query_pairs(&authorization_url)
            .remove("state")
            .expect("state");
        let callback_url =
            format!("http://127.0.0.1:{redirect_port}/oauth/callback?state={state}&code=test-code");
        reqwest::get(callback_url)
            .await
            .expect("callback response")
            .error_for_status()
            .expect("callback success");
    }

    fn oauth_spec(
        token_url: &str,
        redirect_port: u16,
        pkce: ManifestOAuthPkceMode,
        client: ManifestOAuthClientSpec,
    ) -> ManifestOAuthCredentialSpec {
        ManifestOAuthCredentialSpec {
            flow: ManifestOAuthFlowSpec {
                kind: ManifestOAuthFlowKind::AuthorizationCode,
                pkce,
            },
            redirect_uri: format!("http://127.0.0.1:{redirect_port}/oauth/callback"),
            authorization_url: "https://provider.example.com/oauth/authorize".to_string(),
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

    struct CapturedTokenRequest {
        authorization: Option<String>,
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
            form,
        }
    }
}
