use coral_client::{
    AppClient, ClientError,
    local::{LocalServerError, RunningServer, ServerBuilder},
};

#[cfg(feature = "cli-test-server")]
use std::env;

#[cfg(feature = "cli-test-server")]
const CORAL_ENDPOINT_ENV: &str = "CORAL_ENDPOINT";

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
#[allow(
    clippy::disallowed_methods,
    reason = "This feature-gated test hook owns the CORAL_ENDPOINT bootstrap override."
)]
fn bootstrap_endpoint() -> Option<String> {
    env::var_os(CORAL_ENDPOINT_ENV)
        .and_then(|value| value.into_string().ok())
        .filter(|value| !value.is_empty())
}

#[cfg(not(feature = "cli-test-server"))]
fn bootstrap_endpoint() -> Option<String> {
    None
}
