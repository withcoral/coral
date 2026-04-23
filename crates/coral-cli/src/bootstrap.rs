use coral_client::{
    AppClient, ClientError,
    local::{LocalServerError, RunningServer, ServerBuilder},
};

pub(crate) struct Bootstrap {
    pub(crate) app: AppClient,
    pub(crate) _server: Option<RunningServer>,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum BootstrapError {
    #[error(transparent)]
    Startup(#[from] LocalServerError),
    #[error(transparent)]
    Connect(#[from] ClientError),
}

pub(crate) async fn bootstrap() -> Result<Bootstrap, BootstrapError> {
    if let Some(endpoint) = bootstrap_endpoint() {
        return Ok(Bootstrap {
            app: AppClient::connect(&endpoint).await?,
            _server: None,
        });
    }

    let server = ServerBuilder::new().start().await?;
    let app = AppClient::connect(server.endpoint_uri()).await?;
    Ok(Bootstrap {
        app,
        _server: Some(server),
    })
}

#[cfg(feature = "cli-test-server")]
fn bootstrap_endpoint() -> Option<String> {
    coral_cli::env::bootstrap_endpoint()
}

#[cfg(not(feature = "cli-test-server"))]
fn bootstrap_endpoint() -> Option<String> {
    None
}
