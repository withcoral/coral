//! HTTP lifecycle for Coral's authorization server.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use super::OIDC_CALLBACK_PATH;
use super::config::{AuthSettings, ResolvedAuthSettings, signing_key_env_error};
use super::error::AuthServerError;
use super::provider_client::OidcProviderClient;
use super::session::SessionTokenIssuer;
use super::state_store::{InMemoryStateStore, StateStore};
use crate::oauth_resource::{CanonicalOauthUrl, OauthUrlError};
use axum::Router;
use axum::extract::State;
use axum::http::header;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

mod authorize;
mod callback;
mod query;
mod response;
mod token;

const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// Prepared Coral authorization server with validated settings and runtime dependencies.
pub struct CoralAuthorizationServer {
    settings: Arc<ResolvedAuthSettings>,
    session_tokens: SessionTokenIssuer,
    state_store: Arc<dyn StateStore>,
    registered_clients: Arc<BTreeMap<String, Vec<String>>>,
    authorization_resources: BTreeSet<String>,
}

impl CoralAuthorizationServer {
    /// Builds the Coral authorization server from validated auth settings.
    ///
    /// [`AuthSettings`] can only be produced by [`AuthSettings::from_toml`],
    /// so the settings arrive already validated and are not rechecked here.
    /// This resolves what parsing cannot: the provider secret and the session
    /// signing key.
    ///
    /// `config_path` is used only to resolve a relative session signing-key
    /// path. Config parsing and the `config.toml` filesystem read remain the
    /// caller's responsibility.
    ///
    /// # Errors
    ///
    /// Returns an error when the provider secret cannot be resolved from its
    /// environment variable, the session signing key cannot be resolved, or
    /// the session-token key material is invalid.
    pub fn from_settings(
        config_path: &Path,
        settings: AuthSettings,
    ) -> Result<Self, AuthServerError> {
        let (settings, session_tokens) = settings
            .resolve_runtime_dependencies(config_path, &|name| {
                crate::bootstrap::env_var(name).map_err(|error| signing_key_env_error(&error))
            })?;
        Self::from_resolved_settings(settings, session_tokens)
    }

    pub(crate) fn from_resolved_settings(
        settings: ResolvedAuthSettings,
        session_tokens: SessionTokenIssuer,
    ) -> Result<Self, AuthServerError> {
        if !settings.matches_session_token_issuer(&session_tokens) {
            return Err(AuthServerError::SessionIssuerMismatch);
        }
        Ok(Self::from_validated_parts(settings, session_tokens))
    }

    fn from_validated_parts(
        settings: ResolvedAuthSettings,
        session_tokens: SessionTokenIssuer,
    ) -> Self {
        Self {
            settings: Arc::new(settings),
            session_tokens,
            state_store: Arc::new(InMemoryStateStore::new()),
            registered_clients: Arc::new(BTreeMap::new()),
            authorization_resources: BTreeSet::new(),
        }
    }

    /// Registers a resource identifier that authorization requests may target.
    ///
    /// Call this once for each public resource server that shares this
    /// authorization server.
    ///
    /// # Errors
    ///
    /// Returns an error when `resource` is not an HTTPS URL or an explicit
    /// loopback HTTP URL, or when it contains credentials, a query, or a
    /// fragment.
    pub fn with_authorization_resource(
        mut self,
        resource: impl AsRef<str>,
    ) -> Result<Self, String> {
        self.authorization_resources.insert(
            canonical_authorization_resource(resource.as_ref())
                .map_err(|error| format!("authorization resource {error}"))?,
        );
        Ok(self)
    }

    /// Starts the HTTP listener.
    ///
    /// The server is intended to run on loopback or behind a TLS-terminating
    /// reverse proxy that forwards at the origin root. Every endpoint is served
    /// and advertised relative to `auth.authorization_server.issuer`, which must
    /// mount at the root, so a proxy that adds a path prefix strands discovery,
    /// the authorize route, and the OIDC callback.
    /// # Errors
    /// Returns an error when the listener cannot start.
    pub async fn start(self) -> Result<RunningCoralAuthorizationServer, AuthServerError> {
        let bind_addr = self.settings.bind_addr();
        let state = AuthorizationServerHttpState {
            settings: self.settings,
            session_tokens: self.session_tokens,
            state_store: self.state_store,
            provider_client: OidcProviderClient::new()
                .map_err(|error| AuthServerError::ProviderClient(error.to_string()))?,
            registered_clients: self.registered_clients,
            authorization_resources: Arc::new(self.authorization_resources),
        };
        let router = Router::new()
            .route(
                "/.well-known/oauth-authorization-server",
                get(authorization_server_metadata),
            )
            .route("/oauth/authorize", get(authorize::oauth_authorize))
            .route("/oauth/token", post(token::oauth_token))
            .route(OIDC_CALLBACK_PATH, get(callback::oidc_callback))
            .with_state(state);
        let listener =
            TcpListener::bind(bind_addr)
                .await
                .map_err(|source| AuthServerError::Bind {
                    address: bind_addr,
                    source,
                })?;
        let endpoint_uri = format!(
            "http://{}",
            listener.local_addr().map_err(AuthServerError::LocalAddr)?
        );
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async {
                    let _result = shutdown_rx.await;
                })
                .await
        });
        Ok(RunningCoralAuthorizationServer {
            endpoint_uri,
            shutdown_tx: Some(shutdown_tx),
            task: Some(task),
        })
    }
}

/// An active Coral authorization-server listener with deterministic graceful shutdown.
///
/// Only [`Self::shutdown`] guarantees the listening port has been released when
/// it returns: it joins the serve task. Dropping the handle signals shutdown but
/// leaves the task detached, so a caller that drops and immediately rebinds the
/// same fixed address can still lose the race and see `EADDRINUSE`.
pub struct RunningCoralAuthorizationServer {
    endpoint_uri: String,
    shutdown_tx: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<std::io::Result<()>>>,
}

impl RunningCoralAuthorizationServer {
    /// Returns the cleartext listener endpoint, including an assigned port.
    #[must_use]
    pub fn endpoint_uri(&self) -> &str {
        &self.endpoint_uri
    }

    /// Requests graceful shutdown and joins the HTTP server task.
    /// # Errors
    /// Returns an error when the server task fails.
    pub async fn shutdown(self) -> Result<(), AuthServerError> {
        self.shutdown_with_timeout(SHUTDOWN_TIMEOUT).await
    }

    async fn shutdown_with_timeout(mut self, timeout: Duration) -> Result<(), AuthServerError> {
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _result = shutdown_tx.send(());
        }
        if let Some(mut task) = self.task.take() {
            let Ok(result) = tokio::time::timeout(timeout, &mut task).await else {
                task.abort();
                let _result = task.await;
                return Err(AuthServerError::ShutdownTimedOut);
            };
            result
                .map_err(AuthServerError::Join)?
                .map_err(AuthServerError::Server)?;
        }
        Ok(())
    }
}

impl Drop for RunningCoralAuthorizationServer {
    fn drop(&mut self) {
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _result = shutdown_tx.send(());
        }
    }
}

#[derive(Clone)]
struct AuthorizationServerHttpState {
    settings: Arc<ResolvedAuthSettings>,
    session_tokens: SessionTokenIssuer,
    state_store: Arc<dyn StateStore>,
    provider_client: OidcProviderClient,
    registered_clients: Arc<BTreeMap<String, Vec<String>>>,
    authorization_resources: Arc<BTreeSet<String>>,
}

/// Canonicalizes an RFC 8707 `resource` for comparison and recording.
///
/// The identifier a client asks for must land on exactly the string the
/// operator configured, so both sides run the one canonicalizer in
/// [`crate::oauth_resource`].
fn canonical_authorization_resource(value: &str) -> Result<String, OauthUrlError> {
    CanonicalOauthUrl::parse(value).map(CanonicalOauthUrl::into_identifier)
}

async fn authorization_server_metadata(
    State(state): State<AuthorizationServerHttpState>,
) -> impl IntoResponse {
    let authorization_server = state.settings.authorization_server();
    json_response(&serde_json::json!({
        "issuer": authorization_server.issuer,
        "authorization_endpoint": format!("{}/oauth/authorize", authorization_server.issuer),
        "token_endpoint": format!("{}/oauth/token", authorization_server.issuer),
        "response_types_supported": ["code"],
        "grant_types_supported": ["authorization_code"],
        "code_challenge_methods_supported": ["S256"],
        "token_endpoint_auth_methods_supported": ["none"],
        "client_id_metadata_document_supported": false,
    }))
}

fn json_response(value: &serde_json::Value) -> impl IntoResponse + use<> {
    (
        [(header::CONTENT_TYPE, "application/json")],
        value.to_string(),
    )
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::net::SocketAddr;

    use ring::rand::SystemRandom;
    use ring::signature::{ECDSA_P256_SHA256_FIXED_SIGNING, EcdsaKeyPair};
    use serde_json::{Value, json};

    use super::{AuthSettings, CoralAuthorizationServer, RunningCoralAuthorizationServer};

    use crate::auth::test_config::{AUTHORIZATION_SERVER, PROVIDER, SESSION};

    fn config(auth: &str) -> tempfile::TempDir {
        let dir = tempfile::tempdir().expect("tempdir");
        let signing_key =
            EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &SystemRandom::new())
                .expect("P-256 signing key");
        fs::write(dir.path().join("session.key"), signing_key.as_ref()).expect("session key");
        let config = format!("unowned = 'preserved'\n[auth]\n{auth}{SESSION}");
        fs::write(dir.path().join("config.toml"), config).expect("config");
        dir
    }

    fn authorization_server(extra: &str) -> tempfile::TempDir {
        config(&format!("{AUTHORIZATION_SERVER}{extra}\n{PROVIDER}"))
    }

    fn server(dir: &tempfile::TempDir) -> CoralAuthorizationServer {
        let config_path = dir.path().join("config.toml");
        let raw = fs::read_to_string(&config_path).expect("config snapshot");
        let settings = AuthSettings::from_toml(&raw)
            .expect("valid config")
            .expect("auth settings");
        CoralAuthorizationServer::from_settings(&config_path, settings)
            .expect("valid server dependencies")
    }

    async fn json(client: &reqwest::Client, url: String) -> Value {
        let response = client.get(url).send().await.expect("request");
        assert!(response.status().is_success());
        response.json().await.expect("JSON")
    }

    #[test]
    fn uses_passed_settings_without_reopening_config() {
        let dir = authorization_server("");
        let config_path = dir.path().join("config.toml");
        let raw = fs::read_to_string(&config_path).expect("config snapshot");
        let settings = AuthSettings::from_toml(&raw)
            .expect("valid config")
            .expect("auth settings");
        fs::remove_file(&config_path).expect("remove config after parsing");

        CoralAuthorizationServer::from_settings(&config_path, settings)
            .expect("prepared from snapshot");
    }

    /// `AuthSettings` has no public constructor other than `from_toml`, so the
    /// unsafe-bind guard cannot be bypassed by building a value directly; the
    /// parsing boundary is the only place it needs to hold.
    #[test]
    fn rejects_unsafe_bind_at_the_only_construction_site() {
        let raw = format!(
            "[auth]\nhttp_bind_addr = '0.0.0.0:0'\n{AUTHORIZATION_SERVER}{PROVIDER}{SESSION}"
        );
        let Err(error) = AuthSettings::from_toml(&raw) else {
            panic!("expected unsafe bind to be rejected");
        };

        assert!(
            error
                .to_string()
                .contains("allow_insecure_remote_http_bind")
        );
    }

    #[test]
    fn rejects_session_tokens_resolved_from_different_settings() {
        let dir = authorization_server("");
        let config_path = dir.path().join("config.toml");
        let raw = fs::read_to_string(&config_path).expect("config snapshot");
        let (settings, _issuer) = AuthSettings::from_toml(&raw)
            .expect("valid config")
            .expect("auth settings")
            .resolve_runtime_dependencies(&config_path, &|_| Ok(None))
            .expect("resolved runtime dependencies");
        let signing_key = fs::read(dir.path().join("session.key")).expect("session key");
        let session_tokens = super::SessionTokenIssuer::new(
            Some("http://localhost:9999"),
            signing_key,
            std::time::Duration::from_hours(720),
        )
        .expect("session token issuer");

        let Err(error) = CoralAuthorizationServer::from_resolved_settings(settings, session_tokens)
        else {
            panic!("expected mismatched session settings");
        };
        assert!(
            error
                .to_string()
                .contains("does not match authorization-server settings")
        );
    }

    #[test]
    fn validates_and_canonicalizes_authorization_resources() {
        let dir = authorization_server("");
        let prepared = server(&dir)
            .with_authorization_resource("https://mcp.example.test/")
            .expect("resource");
        assert_eq!(
            prepared.authorization_resources,
            ["https://mcp.example.test".to_string()].into()
        );

        for resource in [
            "http://mcp.example.test",
            "https://user@mcp.example.test",
            "https://mcp.example.test?tenant=one",
            "https://mcp.example.test/#fragment",
        ] {
            let error = server(&dir)
                .with_authorization_resource(resource)
                .err()
                .expect("invalid resource");
            assert!(error.contains("authorization resource"));
        }
    }

    #[tokio::test]
    async fn serves_metadata_and_stops_cleanly() {
        let dir = authorization_server("");
        let server = server(&dir).start().await.expect("start");
        let endpoint = server.endpoint_uri().to_string();
        let client = reqwest::Client::new();
        let metadata_url = format!("{endpoint}/.well-known/oauth-authorization-server");
        let metadata = json(&client, metadata_url).await;
        assert_eq!(
            metadata,
            json!({
                "issuer": "http://localhost:9080",
                "authorization_endpoint": "http://localhost:9080/oauth/authorize",
                "token_endpoint": "http://localhost:9080/oauth/token",
                "response_types_supported": ["code"],
                "grant_types_supported": ["authorization_code"],
                "code_challenge_methods_supported": ["S256"],
                "token_endpoint_auth_methods_supported": ["none"],
                "client_id_metadata_document_supported": false,
            })
        );
        let response = client
            .get(format!("{endpoint}/oauth/clients/anything"))
            .send()
            .await
            .expect("request");
        assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);
        server.shutdown().await.expect("shutdown");
        let bind_addr = endpoint
            .strip_prefix("http://")
            .expect("HTTP endpoint")
            .parse::<SocketAddr>()
            .expect("socket address");
        tokio::net::TcpListener::bind(bind_addr)
            .await
            .expect("listener released after shutdown");
    }

    #[tokio::test]
    async fn aborts_task_when_shutdown_times_out() {
        let server = RunningCoralAuthorizationServer {
            endpoint_uri: String::new(),
            shutdown_tx: None,
            task: Some(tokio::spawn(std::future::pending::<std::io::Result<()>>())),
        };
        let error = server
            .shutdown_with_timeout(std::time::Duration::ZERO)
            .await
            .expect_err("timeout");
        assert!(error.to_string().contains("timed out"));
    }
}
