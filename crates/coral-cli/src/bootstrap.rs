use std::sync::Arc;

use coral_app::{AwsEngineExtensionsProvider, features::FeatureOverrides};
use coral_client::{
    AppClient, BearerToken, ClientError,
    local::{LocalServerError, RunningServer as AppRunningServer, ServerBuilder},
};

use crate::env::ConnectionOptions;

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
    pub(crate) connection: ConnectionOptions,
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
    let bearer = connection
        .token
        .as_deref()
        .map(BearerToken::new)
        .transpose()?;
    if let Some(endpoint) = connection.endpoint {
        return Ok(Bootstrap {
            app: connect(&endpoint, bearer).await?,
            mode: BootstrapMode::Remote,
            server: None,
        });
    }

    let server =
        configure_server_builder(ServerBuilder::new(), enable_stderr_logs, feature_overrides)
            .start()
            .await?;
    let app = connect(server.endpoint_uri(), bearer).await?;
    Ok(Bootstrap {
        app,
        mode: BootstrapMode::Local,
        server: Some(server),
    })
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

    #[tokio::test(flavor = "multi_thread")]
    async fn explicit_endpoint_uses_authenticated_remote_bootstrap()
    -> Result<(), Box<dyn std::error::Error>> {
        let config_dir = tempfile::tempdir()?;
        let server = ServerBuilder::new()
            .with_config_dir(config_dir.path())
            .with_noop_feedback_uploads()
            .start()
            .await?;
        let bootstrap = bootstrap(BootstrapOptions {
            connection: ConnectionOptions {
                endpoint: Some(server.endpoint_uri().to_string()),
                token: Some("test-token".to_string()),
            },
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
