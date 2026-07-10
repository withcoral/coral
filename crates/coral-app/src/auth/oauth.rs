//! Configuration and lifecycle for Coral's OAuth authorization server.

use std::collections::BTreeMap;
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::extract::{Path as AxumPath, State};
use axum::http::{StatusCode, header};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use serde::Deserialize;
use serde::de::IgnoredAny;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use super::provider::{OidcProviderConfig, ProviderConfigFile};
use super::provider_client::OidcProviderClient;
use super::session::SessionTokenConfig;
use super::state_store::{InMemoryStateStore, StateStore};
use crate::outbound_url_policy::ConfiguredEndpointUrl;

mod authorize;
mod callback;
mod token;

const DEFAULT_BIND_ADDR: SocketAddr = SocketAddr::new(IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), 0);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_millis(if cfg!(test) { 25 } else { 5_000 });
const CLI_CLIENT: &str = "coral-cli";
const CLI_REDIRECT_URI: &str = "http://127.0.0.1:14554/oauth/callback";
const DEFAULT_OAUTH_SCOPE: &str = "coral:mcp";

/// Validated configuration for Coral's OAuth authorization server.
#[derive(Debug, Clone)]
pub struct OidcAuthConfig {
    bind_addr: SocketAddr,
    session: SessionTokenConfig,
    providers: BTreeMap<String, OidcProviderConfig>,
    oauth: OAuthServerConfig,
}

impl OidcAuthConfig {
    /// Loads the OAuth-owned config sections from Coral's `config.toml`.
    /// # Errors
    /// Returns an error when config I/O, parsing, or validation fails.
    pub fn load(config_dir_override: Option<PathBuf>) -> Result<Option<Self>, String> {
        Self::load_with(config_dir_override, &crate::bootstrap::env_var)
    }

    fn load_with(
        config_dir_override: Option<PathBuf>,
        get_var: &impl Fn(&str) -> Result<Option<String>, std::env::VarError>,
    ) -> Result<Option<Self>, String> {
        let layout = crate::bootstrap::discover_app_state_layout(config_dir_override)
            .map_err(|error| format!("failed to locate Coral config: {error}"))?;
        let config_path = layout.config_file();
        match std::fs::symlink_metadata(config_path) {
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(file_error("inspect", config_path, &error)),
        }
        let raw = std::fs::read_to_string(config_path)
            .map_err(|e| file_error("read", config_path, &e))?;
        let raw = zeroize::Zeroizing::new(raw);
        let file: ConfigFile = toml::from_str(&raw).map_err(|e| config_error(e.message()))?;
        let Some(auth) = file.auth else {
            return Ok(None);
        };
        let session = SessionTokenConfig::load(&layout)?
            .ok_or_else(|| config_error("auth.session is required when [auth] is configured"))?;
        auth.build(session, get_var)
    }

    /// Starts this HTTP listener on loopback or behind a TLS-terminating reverse proxy.
    /// # Errors
    /// Returns an error when the listener cannot start.
    pub async fn start(self) -> Result<RunningOidcAuthServer, String> {
        let bind_addr = self.bind_addr;
        let state = AuthState {
            config: Arc::new(self),
            store: Arc::new(InMemoryStateStore::new()),
            provider_client: OidcProviderClient::new().map_err(|error| error.to_string())?,
        };
        let router = Router::new()
            .route(
                "/.well-known/oauth-authorization-server",
                get(authorization_server_metadata),
            )
            .route("/oauth/clients/{client}", get(client_metadata))
            .route("/oauth/authorize", get(authorize::oauth_authorize))
            .route("/oauth/token", post(token::oauth_token))
            .route(
                "/auth/oidc/{provider}/callback",
                get(callback::oidc_callback),
            )
            .with_state(state);
        let listener = TcpListener::bind(bind_addr)
            .await
            .map_err(|error| format!("failed to bind OAuth server: {error}"))?;
        let endpoint_uri = format!(
            "http://{}",
            listener
                .local_addr()
                .map_err(|error| format!("failed to read OAuth server address: {error}"))?
        );
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async {
                    let _result = shutdown_rx.await;
                })
                .await
        });
        Ok(RunningOidcAuthServer {
            endpoint_uri,
            shutdown_tx: Some(shutdown_tx),
            task: Some(task),
        })
    }
}

/// An active OAuth HTTP listener with deterministic graceful shutdown.
pub struct RunningOidcAuthServer {
    endpoint_uri: String,
    shutdown_tx: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<std::io::Result<()>>>,
}

impl RunningOidcAuthServer {
    /// Returns the cleartext listener endpoint, including an assigned port.
    #[must_use]
    pub fn endpoint_uri(&self) -> &str {
        &self.endpoint_uri
    }

    /// Requests graceful shutdown and joins the HTTP server task.
    /// # Errors
    /// Returns an error when the server task fails.
    pub async fn shutdown(mut self) -> Result<(), String> {
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _result = shutdown_tx.send(());
        }
        if let Some(mut task) = self.task.take() {
            let Ok(result) = tokio::time::timeout(SHUTDOWN_TIMEOUT, &mut task).await else {
                task.abort();
                let _result = task.await;
                return Err("OAuth server graceful shutdown timed out; task aborted".into());
            };
            result
                .map_err(|error| format!("OAuth server task failed: {error}"))?
                .map_err(|error| format!("OAuth server failed: {error}"))?;
        }
        Ok(())
    }
}

impl Drop for RunningOidcAuthServer {
    fn drop(&mut self) {
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _result = shutdown_tx.send(());
        }
    }
}

#[derive(Clone)]
struct AuthState {
    config: Arc<OidcAuthConfig>,
    store: Arc<dyn StateStore>,
    provider_client: OidcProviderClient,
}

#[derive(Debug, Clone)]
struct OAuthServerConfig {
    issuer: String,
    resource: String,
    scope: String,
    clients: BTreeMap<String, Vec<String>>,
}

async fn authorization_server_metadata(State(state): State<AuthState>) -> impl IntoResponse {
    let oauth = &state.config.oauth;
    json_response(&serde_json::json!({
        "issuer": oauth.issuer,
        "authorization_endpoint": format!("{}/oauth/authorize", oauth.issuer),
        "token_endpoint": format!("{}/oauth/token", oauth.issuer),
        "response_types_supported": ["code"],
        "grant_types_supported": ["authorization_code"],
        "code_challenge_methods_supported": ["S256"],
        "token_endpoint_auth_methods_supported": ["none"],
        "scopes_supported": [oauth.scope],
        "resource": oauth.resource,
        "client_id_metadata_document_supported": false,
    }))
}

async fn client_metadata(
    State(state): State<AuthState>,
    AxumPath(client): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, &'static str)> {
    let oauth = &state.config.oauth;
    let redirect_uris = oauth
        .clients
        .get(&client)
        .ok_or((StatusCode::NOT_FOUND, "unknown OAuth client"))?;
    let name = match client.as_str() {
        CLI_CLIENT => "Coral CLI",
        _ => &client,
    };
    Ok(json_response(&serde_json::json!({
        "client_id": oauth_client_id(oauth, &client),
        "redirect_uris": redirect_uris,
        "client_name": name,
        "token_endpoint_auth_method": "none",
        "grant_types": ["authorization_code"],
        "response_types": ["code"],
        "scope": oauth.scope,
    })))
}

fn oauth_client_id(oauth: &OAuthServerConfig, client: &str) -> String {
    format!("{}/oauth/clients/{client}", oauth.issuer)
}

fn json_response(value: &serde_json::Value) -> impl IntoResponse + use<> {
    (
        [(header::CONTENT_TYPE, "application/json")],
        value.to_string(),
    )
}

#[derive(Deserialize)]
struct ConfigFile {
    auth: Option<AuthConfigFile>,
}

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct AuthConfigFile {
    http_bind_addr: Option<String>,
    allow_insecure_remote_http_bind: bool,
    #[serde(rename = "session")]
    _session: Option<IgnoredAny>,
    oauth: Option<OAuthConfigFile>,
    providers: BTreeMap<String, ProviderConfigFile>,
}

impl AuthConfigFile {
    fn build(
        self,
        session: SessionTokenConfig,
        get_var: &impl Fn(&str) -> Result<Option<String>, std::env::VarError>,
    ) -> Result<Option<OidcAuthConfig>, String> {
        let Some(oauth_file) = self.oauth else {
            return if self.providers.is_empty() {
                Ok(None)
            } else {
                Err(config_error(
                    "auth.oauth is required when auth.providers are configured",
                ))
            };
        };
        if self.providers.is_empty() {
            return Err(config_error(
                "auth.providers must configure at least one OIDC provider",
            ));
        }
        let mut providers = BTreeMap::new();
        for (name, provider) in self.providers {
            if !valid_path_segment(&name) {
                return Err(config_error(
                    "auth.providers keys must be non-empty path segments",
                ));
            }
            providers.insert(name.clone(), provider.build(&name, get_var)?);
        }
        let oauth = oauth_file.build()?;
        if session.issuer != oauth.issuer {
            return Err(config_error("session issuer must match OAuth issuer"));
        }
        if session.audience != oauth.resource {
            return Err(config_error("session audience must match OAuth resource"));
        }
        let bind_addr = self
            .http_bind_addr
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|value| {
                value.parse::<SocketAddr>().map_err(|error| {
                    config_error(format!("auth.http_bind_addr is invalid: {error}"))
                })
            })
            .transpose()?
            .unwrap_or(DEFAULT_BIND_ADDR);
        if !is_loopback_ip(bind_addr.ip()) && !self.allow_insecure_remote_http_bind {
            return Err(config_error(
                "non-loopback auth.http_bind_addr serves cleartext OAuth endpoints and requires auth.allow_insecure_remote_http_bind = true",
            ));
        }
        Ok(Some(OidcAuthConfig {
            bind_addr,
            session,
            providers,
            oauth,
        }))
    }
}

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct OAuthConfigFile {
    issuer: Option<String>,
    resource: Option<String>,
    scope: Option<String>,
    clients: BTreeMap<String, OAuthClientConfigFile>,
}

impl OAuthConfigFile {
    fn build(self) -> Result<OAuthServerConfig, String> {
        let issuer = required("auth.oauth.issuer", self.issuer.as_deref())?;
        let issuer = validate_issuer("auth.oauth.issuer", &issuer, true)?;
        let resource = required("auth.oauth.resource", self.resource.as_deref())?;
        validate_endpoint("auth.oauth.resource", &resource)?;
        let scope = self
            .scope
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or(DEFAULT_OAUTH_SCOPE)
            .to_string();
        if scope
            .bytes()
            .any(|byte| !matches!(byte, 0x21 | 0x23..=0x5b | 0x5d..=0x7e))
        {
            return Err(config_error(
                "auth.oauth.scope must be one valid OAuth scope token",
            ));
        }
        let mut clients = BTreeMap::new();
        for (name, client) in self.clients {
            if !valid_path_segment(&name) {
                return Err(config_error(
                    "auth.oauth.clients keys must be non-empty path segments",
                ));
            }
            if name == CLI_CLIENT {
                return Err(config_error(
                    "auth.oauth.clients.coral-cli is reserved for the built-in CLI client",
                ));
            }
            clients.insert(name.clone(), client.validate(&name)?);
        }
        clients.insert(CLI_CLIENT.into(), vec![CLI_REDIRECT_URI.into()]);
        Ok(OAuthServerConfig {
            issuer,
            resource,
            scope,
            clients,
        })
    }
}

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct OAuthClientConfigFile {
    redirect_uris: Vec<String>,
    client_secret: Option<IgnoredAny>,
    client_secret_env: Option<IgnoredAny>,
}

impl OAuthClientConfigFile {
    fn validate(self, name: &str) -> Result<Vec<String>, String> {
        if self.client_secret.is_some() || self.client_secret_env.is_some() {
            return Err(config_error(format!(
                "auth.oauth.clients.{name} must be public; client secrets are not supported"
            )));
        }
        if self.redirect_uris.is_empty() {
            return Err(config_error(format!(
                "auth.oauth.clients.{name}.redirect_uris must not be empty"
            )));
        }
        for uri in &self.redirect_uris {
            if uri.trim() != uri {
                return Err(config_error("redirect URI has surrounding whitespace"));
            }
            let url = validate_endpoint("OAuth client redirect URI", uri)?;
            if url.as_url().query_pairs().any(|(key, _value)| {
                matches!(
                    key.to_ascii_lowercase().as_str(),
                    "code" | "state" | "error" | "error_description"
                )
            }) {
                return Err(config_error(
                    "OAuth client redirect URI must not contain OAuth response parameters",
                ));
            }
        }
        Ok(self.redirect_uris)
    }
}

fn required(label: &str, value: Option<&str>) -> Result<String, String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .ok_or_else(|| config_error(format!("{label} is required")))
}

fn valid_path_segment(value: &str) -> bool {
    !value.is_empty()
        && !matches!(value, "." | "..")
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~'))
}

fn validate_issuer(label: &str, raw: &str, root_only: bool) -> Result<String, String> {
    let url = validate_endpoint(label, raw)?;
    if url.as_url().query().is_some() {
        return Err(config_error(format!("{label} must not include a query")));
    }
    if root_only && !matches!(url.as_url().path(), "" | "/") {
        return Err(config_error("auth.oauth.issuer must mount at the root"));
    }
    Ok(raw.trim_end_matches('/').to_string())
}

fn validate_endpoint(label: &str, raw: &str) -> Result<ConfiguredEndpointUrl, String> {
    ConfiguredEndpointUrl::parse(raw.trim())
        .map_err(|error| config_error(format!("{label} is invalid: {error}")))
}

fn is_loopback_ip(ip: IpAddr) -> bool {
    ip.is_loopback()
        || matches!(ip, IpAddr::V6(ip) if ip.to_ipv4_mapped().is_some_and(|ip| ip.is_loopback()))
}

fn config_error(message: impl AsRef<str>) -> String {
    format!("invalid auth configuration: {}", message.as_ref())
}

fn file_error(action: &str, path: &Path, error: &std::io::Error) -> String {
    let path = path.display();
    config_error(format!("failed to {action} config file {path}: {error}"))
}

#[cfg(test)]
mod tests {
    use std::fs;

    use serde_json::Value;

    use super::{CLI_REDIRECT_URI, OidcAuthConfig, RunningOidcAuthServer};

    const SESSION: &str = "[auth.session]\nissuer = 'http://localhost:9080'\naudience = 'http://localhost:1457'\nsigning_key_file = 'session.key'\n";
    const OAUTH: &str =
        "[auth.oauth]\nissuer = 'http://localhost:9080'\nresource = 'http://localhost:1457'\n";
    const PROVIDER: &str = "[auth.providers.test]\nissuer = 'https://accounts.example.test'\nclient_id = 'upstream-client'\nclient_secret = 'test-secret'\nredirect_uri = 'http://localhost:9080/auth/oidc/test/callback'\n";

    fn config(auth: &str) -> tempfile::TempDir {
        let dir = tempfile::tempdir().expect("tempdir");
        fs::write(dir.path().join("session.key"), [b'k'; 32]).expect("session key");
        let config = format!("unowned = 'preserved'\n[auth]\n{auth}{SESSION}");
        fs::write(dir.path().join("config.toml"), config).expect("config");
        dir
    }

    fn oauth(extra: &str) -> tempfile::TempDir {
        config(&format!("{OAUTH}{extra}\n{PROVIDER}"))
    }

    fn reject(dir: &tempfile::TempDir) -> String {
        OidcAuthConfig::load(Some(dir.path().to_path_buf())).expect_err("invalid config")
    }

    async fn json(client: &reqwest::Client, url: String) -> Value {
        let response = client.get(url).send().await.expect("request");
        assert!(response.status().is_success());
        response.json().await.expect("JSON")
    }

    #[expect(clippy::indexing_slicing, reason = "metadata fixture")]
    #[tokio::test]
    async fn serves_metadata_and_stops_cleanly() {
        let dir =
            oauth("[auth.oauth.clients.web]\nredirect_uris = ['http://localhost:14555/callback']");
        let config = OidcAuthConfig::load(Some(dir.path().to_path_buf()))
            .expect("valid config")
            .expect("OAuth config");
        let server = config.start().await.expect("start");
        let endpoint = server.endpoint_uri().to_string();
        let client = reqwest::Client::new();
        let metadata_url = format!("{endpoint}/.well-known/oauth-authorization-server");
        let metadata = json(&client, metadata_url).await;
        assert_eq!(metadata["scopes_supported"][0], "coral:mcp");
        assert_eq!(metadata["client_id_metadata_document_supported"], false);
        let cli = json(&client, format!("{endpoint}/oauth/clients/coral-cli")).await;
        assert_eq!(cli["redirect_uris"][0], CLI_REDIRECT_URI);
        assert_eq!(cli["scope"], "coral:mcp");
        let web = json(&client, format!("{endpoint}/oauth/clients/web")).await;
        assert_eq!(web["redirect_uris"][0], "http://localhost:14555/callback");
        server.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn aborts_task_when_shutdown_times_out() {
        let server = RunningOidcAuthServer {
            endpoint_uri: String::new(),
            shutdown_tx: None,
            task: Some(tokio::spawn(std::future::pending::<std::io::Result<()>>())),
        };
        let error = server.shutdown().await.expect_err("timeout");
        assert!(error.contains("timed out"));
    }

    #[test]
    #[expect(
        clippy::disallowed_methods,
        reason = "This test isolates CORAL_CONFIG_DIR access in a subprocess."
    )]
    fn load_honors_config_dir_env_unless_explicitly_overridden() {
        const RUN_FLAG: &str = "CORAL_RUN_OIDC_CONFIG_DIR_TEST";
        if std::env::var_os(RUN_FLAG).is_some() {
            assert!(OidcAuthConfig::load(None).expect("env config").is_some());
            let explicit = tempfile::tempdir().expect("explicit config dir");
            assert!(
                OidcAuthConfig::load(Some(explicit.path().to_path_buf()))
                    .expect("explicit config")
                    .is_none()
            );
            return;
        }

        let env_config = oauth("");
        let status = std::process::Command::new(std::env::current_exe().expect("current exe"))
            .env(RUN_FLAG, "1")
            .env("CORAL_CONFIG_DIR", env_config.path())
            .arg("--exact")
            .arg("auth::oauth::tests::load_honors_config_dir_env_unless_explicitly_overridden")
            .arg("--nocapture")
            .status()
            .expect("run subprocess");
        assert!(status.success(), "subprocess should pass");
    }

    #[test]
    fn validates_configuration_fail_closed_without_leaking_secrets() {
        let missing_session = tempfile::tempdir().expect("tempdir");
        fs::write(missing_session.path().join("config.toml"), "[auth]\n").expect("config");
        assert!(reject(&missing_session).contains("auth.session is required"));

        for extra in [
            "[auth.oauth.clients.coral-cli]\nredirect_uris = ['http://localhost/cb']",
            "[auth.oauth.clients.web]\nredirect_uris = ['http://localhost/cb']\nclient_secret = 'DO_NOT_LEAK'",
            "[auth.oauth.clients.\"bad/key\"]\nredirect_uris = ['http://localhost/cb']",
            "[auth.oauth.clients.\"web%2Fprod\"]\nredirect_uris = ['http://localhost/cb']",
            "[auth.oauth.clients.web]\nredirect_uris = [' http://localhost/cb']",
        ] {
            assert!(!reject(&oauth(extra)).contains("DO_NOT_LEAK"));
        }

        let dir = oauth("");
        let path = dir.path().join("config.toml");
        let raw = fs::read_to_string(&path).expect("config");
        let invalid = raw.replace("[auth]\n", "[auth]\nhttp_bind_addr = '0.0.0.0:0'\n");
        fs::write(&path, invalid).expect("config");
        assert!(reject(&dir).contains("allow_insecure_remote_http_bind"));

        let malformed = "[auth]\nclient_secret = 'SUPER_SECRET' trailing-garbage\n";
        fs::write(&path, malformed).expect("config");
        assert!(!reject(&dir).contains("SUPER_SECRET"));
    }
}
