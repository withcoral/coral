use std::sync::Arc;

use coral_app::{
    AwsEngineExtensionsProvider, CanonicalRemoteEndpoint, OAuthLoginStoreError,
    RemoteEndpointError, features::FeatureOverrides,
};
use coral_client::{
    AppClient, BearerToken, ClientError,
    local::{LocalServerError, RunningServer as AppRunningServer, ServerBuilder},
};

use crate::env::{self, ConnectionEnvError};

pub(crate) struct Bootstrap {
    pub(crate) app: AppClient,
    pub(crate) mode: BootstrapMode,
    server: Option<AppRunningServer>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BootstrapMode {
    Local,
    Remote,
}

#[derive(Default)]
pub(crate) struct BootstrapOptions {
    pub(crate) enable_stderr_logs: bool,
    pub(crate) feature_overrides: FeatureOverrides,
    pub(crate) connection: ConnectionOverrides,
}

#[derive(Default)]
pub(crate) struct ConnectionOverrides {
    pub(crate) endpoint: Option<String>,
    pub(crate) token: Option<String>,
}

enum ResolvedConnection {
    Local,
    Remote {
        endpoint: CanonicalRemoteEndpoint,
        bearer: Option<BearerToken>,
    },
}

impl Bootstrap {
    pub(crate) async fn shutdown(self) {
        if let Some(server) = self.server {
            drop(server.shutdown().await);
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum BootstrapError {
    #[error(transparent)]
    ConnectionEnvironment(#[from] ConnectionEnvError),
    #[error(transparent)]
    RemoteEndpoint(#[from] RemoteEndpointError),
    #[error(transparent)]
    OAuthLoginStore(#[from] OAuthLoginStoreError),
    #[error(transparent)]
    Startup(#[from] LocalServerError),
    #[error(transparent)]
    Connect(#[from] ClientError),
    #[error(transparent)]
    Serve(#[from] crate::serve::ServeError),
}

pub(crate) async fn bootstrap(options: BootstrapOptions) -> Result<Bootstrap, BootstrapError> {
    let BootstrapOptions {
        enable_stderr_logs,
        feature_overrides,
        connection,
    } = options;
    if let ResolvedConnection::Remote { endpoint, bearer } = resolve_connection(connection)? {
        return Ok(Bootstrap {
            app: connect(endpoint.as_uri(), bearer).await?,
            mode: BootstrapMode::Remote,
            server: None,
        });
    }

    let server =
        configure_server_builder(ServerBuilder::new(), enable_stderr_logs, feature_overrides)
            .start()
            .await?;
    let app = AppClient::connect(server.endpoint_uri()).await?;
    Ok(Bootstrap {
        app,
        mode: BootstrapMode::Local,
        server: Some(server),
    })
}

fn resolve_connection(
    overrides: ConnectionOverrides,
) -> Result<ResolvedConnection, BootstrapError> {
    resolve_connection_with(
        overrides,
        env::endpoint,
        env::auth_token,
        load_stored_bearer,
    )
}

fn resolve_connection_with(
    overrides: ConnectionOverrides,
    read_endpoint_env: impl FnOnce() -> Result<Option<String>, ConnectionEnvError>,
    read_token_env: impl FnOnce() -> Result<Option<String>, ConnectionEnvError>,
    load_stored_bearer: impl FnOnce(
        &CanonicalRemoteEndpoint,
    ) -> Result<Option<BearerToken>, BootstrapError>,
) -> Result<ResolvedConnection, BootstrapError> {
    let endpoint = select_override(overrides.endpoint, read_endpoint_env)?;
    let Some(endpoint) = endpoint.filter(|endpoint| !endpoint.is_empty()) else {
        return Ok(ResolvedConnection::Local);
    };
    let endpoint = CanonicalRemoteEndpoint::parse(&endpoint)?;
    let token = select_override(overrides.token, read_token_env)?;
    let bearer = match token {
        Some(token) if token.is_empty() => None,
        Some(token) => Some(BearerToken::new(token)?),
        None => load_stored_bearer(&endpoint)?,
    };
    Ok(ResolvedConnection::Remote { endpoint, bearer })
}

fn select_override(
    value: Option<String>,
    read_env: impl FnOnce() -> Result<Option<String>, ConnectionEnvError>,
) -> Result<Option<String>, ConnectionEnvError> {
    match value {
        Some(value) => Ok(Some(value)),
        None => read_env(),
    }
}

fn load_stored_bearer(
    endpoint: &CanonicalRemoteEndpoint,
) -> Result<Option<BearerToken>, BootstrapError> {
    let login = coral_app::load_oauth_login(None, endpoint)?;
    Ok(login
        .map(|login| BearerToken::new(login.access_token()))
        .transpose()?)
}

async fn connect(endpoint: &str, bearer: Option<BearerToken>) -> Result<AppClient, ClientError> {
    match bearer {
        Some(bearer) => AppClient::connect_with_bearer(endpoint, bearer).await,
        None => AppClient::connect(endpoint).await,
    }
}

#[cfg(feature = "embedded-ui")]
pub(crate) async fn start_ui_server(
    port: u16,
    feature_overrides: FeatureOverrides,
) -> Result<AppRunningServer, BootstrapError> {
    let server = configure_server_builder(
        ServerBuilder::embedded_ui_loopback(port, crate::embedded_ui_assets()),
        false,
        feature_overrides,
    )
    .start()
    .await?;
    Ok(server)
}

pub(crate) async fn start_standalone_server(
    feature_overrides: FeatureOverrides,
) -> Result<crate::serve::RunningServer, BootstrapError> {
    let builder = configure_server_builder(
        ServerBuilder::configured_standalone_grpc(),
        false,
        feature_overrides,
    );
    crate::serve::start(builder).await.map_err(Into::into)
}

fn configure_server_builder(
    builder: ServerBuilder,
    enable_stderr_logs: bool,
    feature_overrides: FeatureOverrides,
) -> ServerBuilder {
    builder
        .with_stderr_logs(enable_stderr_logs)
        .with_feature_overrides(feature_overrides)
        .add_engine_extensions_provider(Arc::new(AwsEngineExtensionsProvider))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn overrides(endpoint: Option<&str>, token: Option<&str>) -> ConnectionOverrides {
        ConnectionOverrides {
            endpoint: endpoint.map(str::to_string),
            token: token.map(str::to_string),
        }
    }

    #[test]
    fn endpoint_and_token_flags_win_without_reading_lower_precedence_sources() {
        let resolved = resolve_connection_with(
            overrides(Some("https://EXAMPLE.test:443/"), Some("")),
            || panic!("endpoint flag read endpoint environment"),
            || panic!("token flag read token environment"),
            |_| panic!("empty token flag read login store"),
        )
        .expect("remote connection");

        let ResolvedConnection::Remote { endpoint, bearer } = resolved else {
            panic!("remote endpoint selected local mode");
        };
        assert_eq!(endpoint.as_uri(), "https://example.test");
        assert!(bearer.is_none());

        let explicit = resolve_connection_with(
            overrides(Some("https://example.test"), Some("flag-token")),
            || panic!("endpoint flag read endpoint environment"),
            || panic!("token flag read token environment"),
            |_| panic!("token flag read login store"),
        )
        .expect("explicit bearer");
        assert!(matches!(
            explicit,
            ResolvedConnection::Remote {
                bearer: Some(_),
                ..
            }
        ));
    }

    #[test]
    fn token_environment_is_presence_aware_and_store_receives_canonical_endpoint() {
        let present = resolve_connection_with(
            overrides(Some("https://example.test"), None),
            || panic!("endpoint flag read endpoint environment"),
            || Ok(Some("environment-token".to_string())),
            |_| panic!("token environment read login store"),
        )
        .expect("environment bearer");
        assert!(matches!(
            present,
            ResolvedConnection::Remote {
                bearer: Some(_),
                ..
            }
        ));

        let empty = resolve_connection_with(
            overrides(Some("https://example.test"), None),
            || panic!("endpoint flag read endpoint environment"),
            || Ok(Some(String::new())),
            |_| panic!("empty token environment read login store"),
        )
        .expect("remote connection");
        assert!(matches!(
            empty,
            ResolvedConnection::Remote { bearer: None, .. }
        ));

        let stored = resolve_connection_with(
            overrides(Some("https://EXAMPLE.test:443/"), None),
            || panic!("endpoint flag read endpoint environment"),
            || Ok(None),
            |endpoint| {
                assert_eq!(endpoint.as_uri(), "https://example.test");
                Ok(Some(BearerToken::new("stored-token")?))
            },
        )
        .expect("stored remote connection");
        assert!(matches!(
            stored,
            ResolvedConnection::Remote {
                bearer: Some(_),
                ..
            }
        ));
    }

    #[test]
    fn invalid_endpoint_fails_before_token_environment_or_login_store() {
        let Err(error) = resolve_connection_with(
            overrides(Some(" https://endpoint-secret.example"), None),
            || panic!("endpoint flag read endpoint environment"),
            || panic!("invalid endpoint read token environment"),
            |_| panic!("invalid endpoint read login store"),
        ) else {
            panic!("invalid endpoint was accepted");
        };

        assert!(matches!(error, BootstrapError::RemoteEndpoint(_)));
        let rendered = format!("{error:?} {error}");
        assert!(!rendered.contains("endpoint-secret"));
    }

    #[test]
    fn invalid_explicit_token_error_is_value_redacted() {
        let Err(error) = resolve_connection_with(
            overrides(
                Some("https://example.test"),
                Some("token-secret-sentinel with-space"),
            ),
            || panic!("endpoint flag read endpoint environment"),
            || panic!("token flag read token environment"),
            |_| Ok(None),
        ) else {
            panic!("invalid token was accepted");
        };

        let rendered = format!("{error:?} {error}");
        assert!(!rendered.contains("token-secret-sentinel"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn explicit_endpoint_uses_remote_bootstrap() -> Result<(), Box<dyn std::error::Error>> {
        let config_dir = tempfile::tempdir()?;
        let server = ServerBuilder::new()
            .with_config_dir(config_dir.path())
            .with_noop_feedback_uploads()
            .start()
            .await?;
        let bootstrap = bootstrap(BootstrapOptions {
            connection: overrides(Some(server.endpoint_uri()), Some("")),
            ..BootstrapOptions::default()
        })
        .await?;

        if bootstrap.mode != BootstrapMode::Remote {
            return Err(std::io::Error::other("explicit endpoint started a local server").into());
        }
        bootstrap.shutdown().await;
        server.shutdown().await?;
        Ok(())
    }
}
