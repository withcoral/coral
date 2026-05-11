use std::sync::Arc;

use coral_app::AwsEngineExtensionsProvider;
use coral_client::{
    AppClient, ClientError,
    local::{LocalServerError, RunningServer, ServerBuilder},
};

pub(crate) struct Bootstrap {
    pub(crate) app: AppClient,
    server: Option<RunningServer>,
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct BootstrapOptions {
    pub(crate) enable_stderr_logs: bool,
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
    #[error("failed to connect to Coral endpoint '{endpoint}': {source}")]
    Connect {
        endpoint: String,
        #[source]
        source: ClientError,
    },
}

pub(crate) async fn bootstrap(options: BootstrapOptions) -> Result<Bootstrap, BootstrapError> {
    if let Some(endpoint) = bootstrap_endpoint() {
        let app =
            AppClient::connect(&endpoint)
                .await
                .map_err(|source| BootstrapError::Connect {
                    endpoint: endpoint.clone(),
                    source,
                })?;
        return Ok(Bootstrap { app, server: None });
    }

    let server = configure_server_builder(ServerBuilder::new(), options)
        .start()
        .await?;
    let endpoint = server.endpoint_uri().to_string();
    let app = AppClient::connect(&endpoint)
        .await
        .map_err(|source| BootstrapError::Connect { endpoint, source })?;
    Ok(Bootstrap {
        app,
        server: Some(server),
    })
}

#[cfg(feature = "embedded-ui")]
pub(crate) async fn start_ui_server(port: u16) -> Result<RunningServer, BootstrapError> {
    let server = configure_server_builder(
        ServerBuilder::embedded_ui_loopback(port, crate::embedded_ui_assets()),
        BootstrapOptions::default(),
    )
    .start()
    .await?;
    Ok(server)
}

fn configure_server_builder(builder: ServerBuilder, options: BootstrapOptions) -> ServerBuilder {
    builder
        .with_stderr_logs(options.enable_stderr_logs)
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
