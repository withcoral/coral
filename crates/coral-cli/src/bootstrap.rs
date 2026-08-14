use std::sync::Arc;

use coral_app::{
    AwsEngineExtensionsProvider,
    features::{Feature, FeatureOverrides, FeatureStore},
};
use coral_client::{
    AppClient, ClientError,
    local::{LocalServerError, RunningServer as AppRunningServer, ServerBuilder},
};
use coral_mcp::McpOptions;

pub(crate) struct Bootstrap {
    pub(crate) app: AppClient,
    server: Option<AppRunningServer>,
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
    #[error(transparent)]
    Serve(#[from] crate::serve::ServeError),
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

pub(crate) async fn start_desktop_server(
    port: u16,
    feature_overrides: FeatureOverrides,
) -> Result<AppRunningServer, BootstrapError> {
    configure_server_builder(
        ServerBuilder::loopback_grpc_web(port),
        BootstrapOptions {
            feature_overrides,
            ..BootstrapOptions::default()
        },
    )
    .start()
    .await
    .map_err(Into::into)
}

pub(crate) async fn start_standalone_server(
    feature_overrides: FeatureOverrides,
) -> Result<crate::serve::RunningServer, BootstrapError> {
    let features = FeatureStore::discover(None)?.load_with_overrides(&feature_overrides)?;
    let mcp_options = McpOptions {
        feedback_enabled: features.enabled(Feature::Feedback),
        observed_values_search_enabled: features.enabled(Feature::ObservedValuesSearch),
        ..McpOptions::default()
    };
    let builder = configure_server_builder(
        ServerBuilder::configured_standalone_grpc(),
        BootstrapOptions {
            feature_overrides,
            ..BootstrapOptions::default()
        },
    );
    crate::serve::start(builder, mcp_options)
        .await
        .map_err(Into::into)
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
