use std::sync::Arc;

use coral_app::{
    AwsEngineExtensionsProvider, EngineExtensionsProvider,
    features::{Feature, FeatureOverrides, FeatureStore},
};
use coral_client::{
    AppClient, ClientError,
    local::{LocalServerError, RunningServer as AppRunningServer, ServerBuilder},
};
use coral_mcp::{McpOptions, McpSurfaceProvider};

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

pub(crate) async fn bootstrap(
    options: BootstrapOptions,
    engine_extensions_providers: Vec<Arc<dyn EngineExtensionsProvider>>,
) -> Result<Bootstrap, BootstrapError> {
    if let Some(endpoint) = bootstrap_endpoint() {
        return Ok(Bootstrap {
            app: AppClient::connect(&endpoint).await?,
            server: None,
        });
    }

    let server =
        configure_server_builder(ServerBuilder::new(), options, engine_extensions_providers)
            .start()
            .await?;
    let app = AppClient::connect(server.endpoint_uri()).await?;
    Ok(Bootstrap {
        app,
        server: Some(server),
    })
}

pub(crate) async fn start_standalone_server(
    feature_overrides: FeatureOverrides,
    engine_extensions_providers: Vec<Arc<dyn EngineExtensionsProvider>>,
    mcp_surface_provider: Option<Arc<dyn McpSurfaceProvider>>,
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
        engine_extensions_providers,
    );
    crate::serve::start(builder, mcp_options, mcp_surface_provider)
        .await
        .map_err(Into::into)
}

fn configure_server_builder(
    builder: ServerBuilder,
    options: BootstrapOptions,
    engine_extensions_providers: Vec<Arc<dyn EngineExtensionsProvider>>,
) -> ServerBuilder {
    let builder = builder
        .with_stderr_logs(options.enable_stderr_logs)
        .with_feature_overrides(options.feature_overrides)
        .add_engine_extensions_provider(Arc::new(AwsEngineExtensionsProvider));
    engine_extensions_providers
        .into_iter()
        .fold(builder, |builder, provider| {
            builder.add_engine_extensions_provider(provider)
        })
}

#[cfg(feature = "cli-test-server")]
fn bootstrap_endpoint() -> Option<String> {
    crate::env::bootstrap_endpoint()
}

#[cfg(not(feature = "cli-test-server"))]
fn bootstrap_endpoint() -> Option<String> {
    None
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use coral_api::v1::CreateWorkspaceRequest;
    use coral_api::v1::ExecuteSqlRequest;
    use coral_app::{EngineExtensions, QuerySource};
    use coral_client::workspace;
    use tempfile::TempDir;
    use tonic::Request;

    use super::*;

    struct RecordingProvider {
        name: &'static str,
        calls: Arc<Mutex<Vec<&'static str>>>,
    }

    impl EngineExtensionsProvider for RecordingProvider {
        fn extensions_for(&self, _selected_sources: &[QuerySource]) -> EngineExtensions {
            self.calls.lock().expect("calls lock").push(self.name);
            EngineExtensions::default()
        }
    }

    #[tokio::test]
    async fn configured_builder_runs_host_engine_providers_in_order() {
        let temp = TempDir::new().expect("temp dir");
        let calls = Arc::new(Mutex::new(Vec::new()));
        let providers = ["first", "second"]
            .map(|name| {
                Arc::new(RecordingProvider {
                    name,
                    calls: Arc::clone(&calls),
                }) as Arc<dyn EngineExtensionsProvider>
            })
            .into();
        let server = configure_server_builder(
            ServerBuilder::new().with_config_dir(temp.path()),
            BootstrapOptions::default(),
            providers,
        )
        .start()
        .await
        .expect("start server");
        let app = AppClient::connect(server.endpoint_uri())
            .await
            .expect("connect client");

        // Nothing provisions a workspace any more, so the probe creates the
        // one it queries.
        app.workspace_client()
            .create_workspace(Request::new(CreateWorkspaceRequest {
                workspace: Some(workspace("engine-probe")),
            }))
            .await
            .expect("create workspace");
        app.query_client()
            .execute_sql(Request::new(ExecuteSqlRequest {
                workspace: Some(workspace("engine-probe")),
                sql: "SELECT 1".to_string(),
                guide_read_context: None,
                task_attribution: None,
            }))
            .await
            .expect("execute query");

        assert_eq!(*calls.lock().expect("calls lock"), ["first", "second"]);
        server.shutdown().await.expect("stop server");
    }
}
