use std::sync::Arc;

use coral_auth_aws::AwsSigV4Authenticator;
use coral_client::{
    AppClient, ClientError,
    local::{
        EngineExtensions, EngineExtensionsProvider, LocalServerError, QuerySource, RunningServer,
        ServerBuilder,
    },
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

    let server = ServerBuilder::new()
        .with_engine_extensions_provider(Arc::new(AwsEngineExtensionsProvider))
        .start()
        .await?;
    let app = AppClient::connect(server.endpoint_uri()).await?;
    Ok(Bootstrap {
        app,
        _server: Some(server),
    })
}

#[derive(Debug)]
struct AwsEngineExtensionsProvider;

impl EngineExtensionsProvider for AwsEngineExtensionsProvider {
    fn extensions_for(&self, _selected_sources: &[QuerySource]) -> EngineExtensions {
        let mut extensions = EngineExtensions::default();
        extensions
            .request_authenticators
            .insert("aws_sigv4".to_string(), Arc::new(AwsSigV4Authenticator));
        extensions
    }
}

#[cfg(feature = "cli-test-server")]
fn bootstrap_endpoint() -> Option<String> {
    coral_cli::env::bootstrap_endpoint()
}

#[cfg(not(feature = "cli-test-server"))]
fn bootstrap_endpoint() -> Option<String> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cli_provider_registers_aws_sigv4() {
        let extensions = AwsEngineExtensionsProvider.extensions_for(&[]);
        let authenticator = extensions
            .request_authenticators
            .get("aws_sigv4")
            .expect("cli should register aws authenticator");

        assert_eq!(authenticator.name(), "aws_sigv4");
    }
}
