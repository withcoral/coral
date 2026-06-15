use std::sync::Arc;

use coral_app::{AwsEngineExtensionsProvider, features::FeatureOverrides};
use coral_client::{
    AppClient, ClientError,
    local::{LocalServerError, RunningServer, ServerBuilder},
};

pub(crate) struct Bootstrap {
    pub(crate) app: AppClient,
    server: Option<RunningServer>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct BootstrapOptions {
    pub(crate) enable_stderr_logs: bool,
    pub(crate) feature_overrides: FeatureOverrides,
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
}

pub(crate) async fn bootstrap(options: BootstrapOptions) -> Result<Bootstrap, BootstrapError> {
    if let Some(endpoint) = bootstrap_endpoint() {
        return Ok(Bootstrap {
            app: AppClient::connect(&endpoint).await?,
            server: None,
        });
    }

    let server = configure_server_builder(ServerBuilder::new(), options)
        .start()
        .await?;
    let app = AppClient::connect(server.endpoint_uri()).await?;
    Ok(Bootstrap {
        app,
        server: Some(server),
    })
}

#[cfg(feature = "embedded-ui")]
pub(crate) async fn start_ui_server(
    port: u16,
    feature_overrides: FeatureOverrides,
) -> Result<RunningServer, BootstrapError> {
    let server = configure_server_builder(
        ServerBuilder::embedded_ui_loopback(port, crate::embedded_ui_assets()),
        BootstrapOptions {
            feature_overrides,
            ..BootstrapOptions::default()
        },
    )
    .start()
    .await?;
    Ok(server)
}

fn configure_server_builder(builder: ServerBuilder, options: BootstrapOptions) -> ServerBuilder {
    builder
        .with_stderr_logs(options.enable_stderr_logs)
        .with_feature_overrides(options.feature_overrides)
        .add_engine_extensions_provider(Arc::new(AwsEngineExtensionsProvider))
}

#[cfg(feature = "cli-test-server")]
fn bootstrap_endpoint() -> Option<String> {
    crate::env::bootstrap_endpoint()
}

#[cfg(not(feature = "cli-test-server"))]
fn bootstrap_endpoint() -> Option<String> {
    None
}
