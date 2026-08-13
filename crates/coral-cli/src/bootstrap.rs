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

    let builder = configure_server_builder(ServerBuilder::new(), options);
    let server = with_configured_session_auth(builder)?.start().await?;
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
) -> Result<AppRunningServer, BootstrapError> {
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

pub(crate) async fn start_standalone_server(
    feature_overrides: FeatureOverrides,
    requested_workspace: Option<String>,
) -> Result<crate::serve::RunningServer, BootstrapError> {
    let features = FeatureStore::discover(None)?.load_with_overrides(&feature_overrides)?;
    let mcp_options = McpOptions {
        feedback_enabled: features.enabled(Feature::Feedback),
        observed_values_search_enabled: features.enabled(Feature::ObservedValuesSearch),
        workspace: requested_workspace.map(coral_client::workspace),
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

fn with_configured_session_auth(builder: ServerBuilder) -> Result<ServerBuilder, LocalServerError> {
    match builder.serve_settings()?.take_session_auth() {
        Some(session_auth) => Ok(builder.with_session_auth(session_auth)),
        None => Ok(builder),
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

#[cfg(test)]
mod tests {
    use coral_api::v1::ListWorkspacesRequest;
    use ring::rand::SystemRandom;
    use ring::signature::{ECDSA_P256_SHA256_FIXED_SIGNING, EcdsaKeyPair};
    use tempfile::TempDir;
    use tonic::{Code, Request};

    use super::{ServerBuilder, with_configured_session_auth};
    use coral_client::AppClient;

    #[tokio::test]
    async fn configured_auth_uses_no_local_principal_policy_for_cli_client() {
        let config_dir = TempDir::new().expect("config dir");
        let signing_key =
            EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &SystemRandom::new())
                .expect("generate session signing key");
        std::fs::write(config_dir.path().join("session.key"), signing_key.as_ref())
            .expect("write session signing key");
        std::fs::write(
            config_dir.path().join("config.toml"),
            "[auth]\nallowed_audiences = ['https://surface.example.test/']\n\n[auth.session]\nsigning_key_file = 'session.key'\n\n[auth.authorization_server]\nissuer = 'https://auth.example.test'\n\n[auth.provider]\nissuer = 'https://accounts.example.test'\nclient_id = 'client'\nclient_secret = 'secret'\nredirect_uri = 'https://auth.example.test/auth/oidc/callback'\n",
        )
        .expect("write config");

        let server =
            with_configured_session_auth(ServerBuilder::new().with_config_dir(config_dir.path()))
                .expect("resolve configured session auth")
                .start()
                .await
                .expect("start CLI app server");
        let client = AppClient::connect(server.endpoint_uri())
            .await
            .expect("connect CLI app client");
        let denied = client
            .workspace_client()
            .list_workspaces(Request::new(ListWorkspacesRequest {}))
            .await
            .expect_err("NoLocalPrincipal CLI client must not become the local principal");
        assert_eq!(denied.code(), Code::Unauthenticated);
        server.shutdown().await.expect("shut down CLI app server");
    }
}
