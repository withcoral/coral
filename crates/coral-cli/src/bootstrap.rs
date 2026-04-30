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

impl Bootstrap {
    pub(crate) async fn shutdown(self) {
        if let Some(server) = self.server {
            let _ = server.shutdown().await;
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

pub(crate) async fn bootstrap(enable_stderr_logs: bool) -> Result<Bootstrap, BootstrapError> {
    if let Some(endpoint) = bootstrap_endpoint() {
        return Ok(Bootstrap {
            app: AppClient::connect(&endpoint).await?,
            server: None,
        });
    }

    let server = configure_server_builder(ServerBuilder::new(), enable_stderr_logs)
        .start()
        .await?;
    let app = AppClient::connect(server.endpoint_uri()).await?;
    Ok(Bootstrap {
        app,
        server: Some(server),
    })
}

fn configure_server_builder(builder: ServerBuilder, enable_stderr_logs: bool) -> ServerBuilder {
    builder
        .with_stderr_logs(enable_stderr_logs)
        .add_engine_extensions_provider(Arc::new(AwsEngineExtensionsProvider))
}

#[cfg(feature = "cli-test-server")]
fn bootstrap_endpoint() -> Option<String> {
    coral_cli::env::bootstrap_endpoint()
}

#[cfg(not(feature = "cli-test-server"))]
fn bootstrap_endpoint() -> Option<String> {
    None
}
