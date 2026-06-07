use coral_app::{AppError as LocalServerError, RunningServer, ServerBuilder};
use coral_client::{AppClient, ClientError};

pub(crate) struct Bootstrap {
    pub(crate) app: AppClient,
    pub(crate) runtime_exposure: coral_app::RuntimeExposureMode,
    server: Option<RunningServer>,
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct BootstrapOptions {
    pub(crate) enable_stderr_logs: bool,
    pub(crate) runtime_exposure: Option<coral_app::RuntimeExposureMode>,
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
            runtime_exposure: options
                .runtime_exposure
                .unwrap_or(coral_app::RuntimeExposureMode::Both),
            server: None,
        });
    }

    let server = configure_server_builder(ServerBuilder::new(), options)
        .start()
        .await?;
    let runtime_exposure = server.runtime_exposure();
    let app = AppClient::connect(server.endpoint_uri()).await?;
    Ok(Bootstrap {
        app,
        runtime_exposure,
        server: Some(server),
    })
}

#[cfg(feature = "embedded-ui")]
pub(crate) async fn start_ui_server(
    port: u16,
    options: BootstrapOptions,
) -> Result<RunningServer, BootstrapError> {
    let server = configure_server_builder(
        ServerBuilder::embedded_ui_loopback(port, crate::embedded_ui_assets()),
        options,
    )
    .start()
    .await?;
    Ok(server)
}

fn configure_server_builder(builder: ServerBuilder, options: BootstrapOptions) -> ServerBuilder {
    let builder = builder.with_stderr_logs(options.enable_stderr_logs);
    if let Some(runtime_exposure) = options.runtime_exposure {
        builder.with_runtime_exposure(runtime_exposure)
    } else {
        builder
    }
}

#[cfg(feature = "cli-test-server")]
fn bootstrap_endpoint() -> Option<String> {
    crate::env::bootstrap_endpoint()
}

#[cfg(not(feature = "cli-test-server"))]
fn bootstrap_endpoint() -> Option<String> {
    None
}
