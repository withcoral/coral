//! HTTP lifecycle for Coral's authorization server.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::extract::State;
use axum::http::header;
use axum::response::IntoResponse;
use axum::routing::get;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use super::config::{AuthSettings, signing_key_env_error};
use super::session::SessionTokenIssuer;
use super::state_store::{InMemoryStateStore, StateStore};

const SHUTDOWN_TIMEOUT: Duration = Duration::from_millis(if cfg!(test) { 25 } else { 5_000 });

/// Prepared Coral authorization server with validated settings and runtime dependencies.
pub struct CoralAuthorizationServer {
    settings: Arc<AuthSettings>,
    session_tokens: SessionTokenIssuer,
    state_store: Arc<dyn StateStore>,
}

impl CoralAuthorizationServer {
    /// Builds the Coral authorization server from validated auth settings,
    /// revalidating them before use.
    ///
    /// `config_path` is used only to resolve a relative session signing-key
    /// path. Config parsing and the `config.toml` filesystem read remain the
    /// caller's responsibility.
    ///
    /// # Errors
    ///
    /// Returns an error when the settings fail revalidation, the session
    /// signing key cannot be resolved, or the session-token key material is
    /// invalid.
    pub fn from_settings(config_path: &Path, settings: AuthSettings) -> Result<Self, String> {
        let (settings, session_tokens) = settings
            .resolve_session_token_issuer(config_path, &|name| {
                crate::bootstrap::env_var(name).map_err(|error| signing_key_env_error(&error))
            })?;
        Self::from_resolved_settings(settings, session_tokens)
    }

    pub(crate) fn from_resolved_settings(
        mut settings: AuthSettings,
        session_tokens: SessionTokenIssuer,
    ) -> Result<Self, String> {
        settings.validate()?;
        if !settings.matches_session_token_issuer(&session_tokens) {
            return Err(
                "resolved session-token issuer does not match authorization-server settings"
                    .to_string(),
            );
        }
        Ok(Self::from_validated_parts(settings, session_tokens))
    }

    fn from_validated_parts(settings: AuthSettings, session_tokens: SessionTokenIssuer) -> Self {
        Self {
            settings: Arc::new(settings),
            session_tokens,
            state_store: Arc::new(InMemoryStateStore::new()),
        }
    }

    /// Starts the HTTP listener.
    ///
    /// The server is intended to run on loopback or behind a TLS-terminating
    /// reverse proxy.
    /// # Errors
    /// Returns an error when the listener cannot start.
    pub async fn start(self) -> Result<RunningCoralAuthorizationServer, String> {
        let bind_addr = self.settings.bind_addr();
        let state = AuthorizationServerHttpState {
            settings: self.settings,
            session_tokens: self.session_tokens,
            state_store: self.state_store,
        };
        let router = Router::new()
            .route(
                "/.well-known/oauth-authorization-server",
                get(authorization_server_metadata),
            )
            .with_state(state);
        let listener = TcpListener::bind(bind_addr)
            .await
            .map_err(|error| format!("failed to bind authorization server: {error}"))?;
        let endpoint_uri = format!(
            "http://{}",
            listener
                .local_addr()
                .map_err(|error| format!("failed to read authorization server address: {error}"))?
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
    pub async fn shutdown(mut self) -> Result<(), String> {
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _result = shutdown_tx.send(());
        }
        if let Some(mut task) = self.task.take() {
            let Ok(result) = tokio::time::timeout(SHUTDOWN_TIMEOUT, &mut task).await else {
                task.abort();
                let _result = task.await;
                return Err(
                    "authorization server graceful shutdown timed out; task aborted".into(),
                );
            };
            result
                .map_err(|error| format!("authorization server task failed: {error}"))?
                .map_err(|error| format!("authorization server failed: {error}"))?;
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
    settings: Arc<AuthSettings>,
    #[expect(
        dead_code,
        reason = "used by the OAuth token endpoint in a descendant PR"
    )]
    session_tokens: SessionTokenIssuer,
    #[expect(
        dead_code,
        reason = "used by the OAuth authorization endpoint in a descendant PR"
    )]
    state_store: Arc<dyn StateStore>,
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

    const SESSION: &str = "[auth.session]\nsigning_key_file = 'session.key'\n";
    const AUTHORIZATION_SERVER: &str =
        "[auth.authorization_server]\nissuer = 'http://localhost:9080'\n";
    const PROVIDER: &str = "[auth.providers.test]\nissuer = 'https://accounts.example.test'\nclient_id = 'upstream-client'\nclient_secret_env = 'UNREAD_ENV'\nredirect_uri = 'http://localhost:9080/auth/oidc/test/callback'\n";

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

    #[test]
    fn rejects_settings_that_bypassed_the_validating_parser() {
        let raw = format!(
            "http_bind_addr = '0.0.0.0:0'\n{}{}{}",
            SESSION.replace("[auth.", "["),
            AUTHORIZATION_SERVER.replace("[auth.", "["),
            PROVIDER.replace("[auth.", "["),
        );
        let settings: AuthSettings = toml::from_str(&raw).expect("deserialized settings");
        let Err(error) =
            CoralAuthorizationServer::from_settings(std::path::Path::new("config.toml"), settings)
        else {
            panic!("expected unsafe bind to be rejected");
        };

        assert!(error.contains("allow_insecure_remote_http_bind"));
    }

    #[test]
    fn rejects_session_tokens_resolved_from_different_settings() {
        let dir = authorization_server("");
        let config_path = dir.path().join("config.toml");
        let raw = fs::read_to_string(&config_path).expect("config snapshot");
        let settings = AuthSettings::from_toml(&raw)
            .expect("valid config")
            .expect("auth settings");
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
        assert!(error.contains("does not match authorization-server settings"));
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
        let error = server.shutdown().await.expect_err("timeout");
        assert!(error.contains("timed out"));
    }
}
