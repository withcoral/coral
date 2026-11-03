//! The composed runtime: the private gRPC listener, auth support, and the MCP
//! companion, started and stopped as one.

use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;

use coral_db::Database;
use coral_functions::SavedFunctionModule;
use coral_mcp::{
    McpHttpConfig, McpHttpError, McpModules, RunningMcpHttpServer, start_authenticated, start_local,
};
use coral_run::RunModule;
use coral_search::{SearchIndex, UniversalSearchModule};
use coral_task::{TaskCommands, TaskModule};
use coral_workspace::WorkspaceModule;

use super::error::AppError;
use super::health::EngineReadiness as ListenerReadiness;
use super::server::{PreparedServerBuilder, RunningServer as GrpcServer, ServerBuilder};
use coral_run::RuntimeLimits;

use super::server_config::{LOCAL_OUTBOUND_DEFAULTS, McpHttpServeConfig};
use crate::auth::{AuthServerError, CoralAuthorizationServer, RunningCoralAuthorizationServer};

/// What the launching process says about this launch.
///
/// [`Launch::from_env`] reads it once at process bootstrap.
#[derive(Default)]
pub struct Launch {
    /// Desktop owns this launch and its local outbound defaults.
    pub desktop: bool,
    /// A local launcher's web origin for human handoff.
    pub local_web_url: Option<coral_mcp::HumanInterfaceUrl>,
    /// Treat EOF on stdin like a termination signal.
    pub stop_on_stdin_close: bool,
    /// The launching binary's version, for telemetry's `service.version`.
    /// Without one, Coral's package version names the service.
    pub service_version: Option<&'static str>,
}

impl Launch {
    /// Passes this launch to `builder`.
    fn configure(self, builder: ServerBuilder) -> ServerBuilder {
        let builder = match self.service_version {
            Some(version) => builder.with_service_version(version),
            None => builder,
        };
        let builder = match self.local_web_url {
            Some(url) => builder.with_local_web_url(url),
            None => builder,
        };
        if self.desktop {
            // Desktop owns this launch default even if its operator configures sign-in.
            builder
                .with_desktop_launch()
                .with_outbound_defaults(LOCAL_OUTBOUND_DEFAULTS)
        } else {
            builder
        }
    }
}

/// Why the runtime did not start or did not stop cleanly.
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct RuntimeError(RuntimeErrorKind);

#[derive(Debug, thiserror::Error)]
enum RuntimeErrorKind {
    #[error(transparent)]
    Startup(AppError),
    #[error(transparent)]
    Serve(ServeError),
}

impl RuntimeError {
    /// Whether another runtime holds the state root. The condition clears when
    /// that runtime exits, so a launcher may retry.
    #[must_use]
    pub fn state_locked(&self) -> bool {
        matches!(
            self.0,
            RuntimeErrorKind::Startup(AppError::StateLocked { .. })
        )
    }
}

impl From<AppError> for RuntimeError {
    fn from(error: AppError) -> Self {
        Self(RuntimeErrorKind::Startup(error))
    }
}

impl From<ServeError> for RuntimeError {
    fn from(error: ServeError) -> Self {
        Self(RuntimeErrorKind::Serve(error))
    }
}

/// Starts the runtime on the configured state root and listeners.
///
/// # Errors
///
/// Returns an error when state initialization or a listener fails to start.
pub async fn start_runtime(launch: Launch) -> Result<RunningRuntime, RuntimeError> {
    let builder = launch
        .configure(ServerBuilder::configured_standalone_grpc())
        .prepare()
        .await?;
    Ok(start_prepared(builder).await?)
}

#[derive(Debug, thiserror::Error)]
#[error(transparent)]
struct ServeError(Box<ServeErrorKind>);

#[derive(Debug, thiserror::Error)]
enum ServeErrorKind {
    #[error("failed to resolve server configuration: {0}")]
    Config(#[source] AppError),
    #[error("failed to start gRPC server: {0}")]
    GrpcStart(#[source] AppError),
    #[error("failed to start auth-support listener: {0}")]
    AuthSupportStart(#[source] AuthServerError),
    #[error(
        "failed to start auth-support listener: {auth_support}; cleanup also failed: {cleanup}"
    )]
    AuthSupportStartCleanup {
        auth_support: AuthServerError,
        cleanup: ShutdownFailures,
    },
    #[error("failed to start MCP HTTP server: {0}")]
    McpStart(#[source] McpStartError),
    #[error("failed to start MCP HTTP server: {mcp}; cleanup also failed: {cleanup}")]
    McpStartCleanup {
        mcp: McpStartError,
        cleanup: ShutdownFailures,
    },
    #[error("failed to stop server components: {0}")]
    Shutdown(ShutdownFailures),
}

#[derive(Debug)]
struct ShutdownFailures {
    mcp: Option<McpHttpError>,
    auth_support: Option<AuthServerError>,
    grpc: Option<Box<AppError>>,
}

impl ShutdownFailures {
    fn from_results(
        mcp: Result<(), McpHttpError>,
        auth_support: Result<(), AuthServerError>,
        grpc: Result<(), AppError>,
    ) -> Result<(), Self> {
        let failures = Self {
            mcp: mcp.err(),
            auth_support: auth_support.err(),
            grpc: grpc.err().map(Box::new),
        };
        if failures.mcp.is_none() && failures.auth_support.is_none() && failures.grpc.is_none() {
            Ok(())
        } else {
            Err(failures)
        }
    }
}

impl fmt::Display for ShutdownFailures {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut wrote_failure = false;
        if let Some(error) = &self.mcp {
            write_shutdown_failure(formatter, &mut wrote_failure, "MCP HTTP", error)?;
        }
        if let Some(error) = &self.auth_support {
            write_shutdown_failure(formatter, &mut wrote_failure, "auth-support", error)?;
        }
        if let Some(error) = &self.grpc {
            write_shutdown_failure(formatter, &mut wrote_failure, "gRPC", error)?;
        }
        Ok(())
    }
}

impl std::error::Error for ShutdownFailures {}

fn write_shutdown_failure(
    formatter: &mut fmt::Formatter<'_>,
    wrote_failure: &mut bool,
    component: &str,
    error: &dyn fmt::Display,
) -> fmt::Result {
    if *wrote_failure {
        formatter.write_str("; ")?;
    }
    *wrote_failure = true;
    write!(formatter, "{component}: {error}")
}

#[derive(Debug, thiserror::Error)]
enum McpStartError {
    #[error("MCP authentication configuration is missing or invalid")]
    MissingAuthentication,
    #[error("failed to initialize MCP domain Modules: {0}")]
    Runtime(#[source] coral_run::Error),
    #[error(transparent)]
    Http(#[from] McpHttpError),
}

/// The running runtime. Call [`Self::shutdown`] to stop every listener.
pub struct RunningRuntime {
    grpc: GrpcServer,
    auth_support: Option<RunningCoralAuthorizationServer>,
    mcp_http: RunningMcpHttpServer,
    grpc_authentication_enabled: bool,
}

impl RunningRuntime {
    /// The private gRPC endpoint.
    #[must_use]
    pub fn endpoint_uri(&self) -> &str {
        self.grpc.endpoint_uri()
    }

    /// The private gRPC listener address.
    #[must_use]
    pub fn grpc_addr(&self) -> SocketAddr {
        self.grpc.local_addr()
    }

    /// The MCP HTTP listener address.
    #[must_use]
    pub fn mcp_http_addr(&self) -> SocketAddr {
        self.mcp_http.local_addr()
    }

    /// The local Provider OAuth callback, when sign-in is off.
    #[must_use]
    pub fn provider_oauth_callback_url(&self) -> Option<String> {
        (!self.grpc_authentication_enabled)
            .then(|| {
                self.auth_support
                    .as_ref()
                    .map(|server| format!("http://{}/provider-oauth/callback", server.local_addr()))
            })
            .flatten()
    }

    /// Whether the private gRPC listener authenticates its clients.
    #[must_use]
    pub fn grpc_authentication_enabled(&self) -> bool {
        self.grpc_authentication_enabled
    }

    /// Completes when the runtime stops serving on its own.
    pub async fn wait_for_exit(&self) {
        self.grpc.wait_for_exit().await;
    }

    /// Shuts the runtime down gracefully, then stops every listener.
    ///
    /// Readiness reports not serving at once, while every listener keeps
    /// serving for the Shutdown Drain (`[runtime] shutdown_drain_seconds`,
    /// see [`coral_run::RunModule::shutdown`]). The drain and the commit grace
    /// run one after the other, so an orchestrator's termination grace must
    /// exceed their sum.
    ///
    /// # Errors
    ///
    /// Returns every listener that failed to stop.
    pub async fn shutdown(self) -> Result<(), RuntimeError> {
        let Self {
            grpc,
            auth_support,
            mcp_http,
            grpc_authentication_enabled: _,
        } = self;
        grpc.drain_runs().await;
        shutdown_components(grpc, auth_support, Some(mcp_http))
            .await
            .map_err(|failures| ServeError(Box::new(ServeErrorKind::Shutdown(failures))).into())
    }
}

/// Composes the served surfaces after the state owner has prepared persistent
/// state, so no configuration parser runs before the state-format fence.
async fn start_prepared(builder: PreparedServerBuilder) -> Result<RunningRuntime, ServeError> {
    Box::pin(start_inner(builder)).await
}

/// The runtime owns the implicit-user local experience, so an auth-disabled
/// deployment gets local outbound defaults. Ordinary server builders retain
/// deny/deny; authentication never changes explicit fields.
fn with_local_launch_defaults(
    builder: PreparedServerBuilder,
) -> Result<PreparedServerBuilder, AppError> {
    let settings = builder.serve_settings()?;
    Ok(
        if matches!(settings.mcp_http(), McpHttpServeConfig::AuthDisabled { .. }) {
            builder.with_outbound_defaults(LOCAL_OUTBOUND_DEFAULTS)
        } else {
            builder
        },
    )
}

async fn start_inner(builder: PreparedServerBuilder) -> Result<RunningRuntime, ServeError> {
    let builder = with_local_launch_defaults(builder)
        .map_err(|error| ServeError(Box::new(ServeErrorKind::Config(error))))?;
    let mut settings = builder
        .serve_settings()
        .map_err(|error| ServeError(Box::new(ServeErrorKind::Config(error))))?;
    let mcp_config = settings.mcp_http().clone();
    let public_mcp_origin = match &mcp_config {
        McpHttpServeConfig::Authenticated { public_url, .. } => Some(public_url.clone()),
        McpHttpServeConfig::AuthDisabled { .. } => None,
    };
    let auth_bind_addr = settings.auth_bind_addr();
    let session_auth = settings.take_session_auth();
    let grpc_authentication_enabled = session_auth.is_some();
    let builder = match session_auth {
        Some(session_auth) => builder.with_session_auth(session_auth),
        None => builder,
    };

    let mut grpc = builder
        .start()
        .await
        .map_err(|error| ServeError(Box::new(ServeErrorKind::GrpcStart(error))))?;
    let authentication = grpc.take_workspace_authentication();
    let authorization_server = grpc.take_authorization_server();
    let auth_support = match start_auth_support(
        authorization_server,
        auth_bind_addr,
        grpc.readiness(),
        grpc.provider_accounts(),
    )
    .await
    {
        Ok(server) => server,
        Err(auth_support) => {
            let error = match shutdown_components(grpc, None, None).await {
                Ok(()) => ServeErrorKind::AuthSupportStart(auth_support),
                Err(cleanup) => ServeErrorKind::AuthSupportStartCleanup {
                    auth_support,
                    cleanup,
                },
            };
            return Err(ServeError(Box::new(error)));
        }
    };
    let mcp_http = match start_mcp_http(
        mcp_config,
        &grpc,
        authentication,
        settings.runtime_limits(),
        settings.human_interface().clone(),
    )
    .await
    {
        Ok(server) => server,
        Err(mcp) => {
            let error = match shutdown_components(grpc, auth_support, None).await {
                Ok(()) => ServeErrorKind::McpStart(mcp),
                Err(cleanup) => ServeErrorKind::McpStartCleanup { mcp, cleanup },
            };
            return Err(ServeError(Box::new(error)));
        }
    };
    grpc.publish_mcp_origin(
        public_mcp_origin.unwrap_or_else(|| format!("http://{}", mcp_http.local_addr())),
    );
    Ok(RunningRuntime {
        grpc,
        auth_support,
        mcp_http,
        grpc_authentication_enabled,
    })
}

async fn start_auth_support(
    authorization_server: Option<CoralAuthorizationServer>,
    bind_addr: SocketAddr,
    readiness: ListenerReadiness,
    accounts: Option<coral_provider::account::AccountModule>,
) -> Result<Option<RunningCoralAuthorizationServer>, AuthServerError> {
    match authorization_server {
        Some(server) => server.start().await.map(Some),
        None if accounts.is_some() => CoralAuthorizationServer::start_local_with_provider_oauth(
            bind_addr,
            readiness,
            accounts.ok_or_else(|| {
                AuthServerError::Config("Provider authorization is unavailable".into())
            })?,
        )
        .await
        .map(Some),
        None => CoralAuthorizationServer::start_local(bind_addr, readiness)
            .await
            .map(Some),
    }
}

async fn shutdown_components(
    grpc: GrpcServer,
    auth_support: Option<RunningCoralAuthorizationServer>,
    mcp_http: Option<RunningMcpHttpServer>,
) -> Result<(), ShutdownFailures> {
    let mcp_result = match mcp_http {
        Some(server) => server.shutdown().await,
        None => Ok(()),
    };
    let auth_support_result = match auth_support {
        Some(server) => server.shutdown().await,
        None => Ok(()),
    };
    let grpc_result = grpc.shutdown().await;
    ShutdownFailures::from_results(mcp_result, auth_support_result, grpc_result)
}

/// Starts the MCP companion over the running server's database, Provider
/// accounts, and search index.
async fn start_mcp_http(
    settings: McpHttpServeConfig,
    grpc: &GrpcServer,
    authentication: Option<crate::WorkspaceAuthentication>,
    runtime_limits: RuntimeLimits,
    human_interface: coral_mcp::HumanInterfaceUrl,
) -> Result<RunningMcpHttpServer, McpStartError> {
    let readiness = grpc.readiness().probe();
    let modules = mcp_modules(
        grpc.database(),
        grpc.search_index(),
        grpc.runs(),
        runtime_limits,
        human_interface,
    )
    .map_err(McpStartError::Runtime)?;
    let server = match settings {
        McpHttpServeConfig::AuthDisabled { bind_addr } => {
            start_local(McpHttpConfig::new(bind_addr)?, modules, readiness).await?
        }
        McpHttpServeConfig::Authenticated {
            bind_addr,
            public_url,
            ..
        } => {
            let auth = authentication.ok_or(McpStartError::MissingAuthentication)?;
            let public = crate::CanonicalOauthUrl::parse(&public_url)
                .map_err(|_error| McpStartError::MissingAuthentication)?;
            let host = public
                .url()
                .host_str()
                .ok_or(McpStartError::MissingAuthentication)?
                .to_owned();
            start_authenticated(bind_addr, vec![host], modules, Arc::new(auth), readiness).await?
        }
    };
    Ok(server)
}

/// Composes MCP domain Modules without selecting authentication or Principal.
/// `search_index` and `runs` are the running server's, so the process has one
/// index and one set of Run Owners that every surface reaches.
fn mcp_modules(
    database: Arc<Database>,
    search_index: SearchIndex,
    runs: RunModule,
    runtime_limits: RuntimeLimits,
    human_interface: coral_mcp::HumanInterfaceUrl,
) -> Result<McpModules, coral_run::Error> {
    let workspaces = WorkspaceModule::new(Arc::clone(&database));
    let tasks = TaskModule::new(Arc::clone(&database), workspaces.clone());
    let search = UniversalSearchModule::new(Arc::clone(&database), workspaces.clone());
    let saved_functions =
        SavedFunctionModule::new(database, workspaces.clone(), runtime_limits.isolate())?;
    // MCP-started Runs link people to Reef; this clone shares the server's Run Owners.
    let runs = runs.with_human_interface(human_interface.clone());
    let commands = TaskCommands::new(
        tasks.clone(),
        saved_functions,
        search,
        search_index,
        runs.clone(),
    );
    Ok(McpModules::new(
        workspaces,
        tasks,
        commands,
        runs,
        human_interface,
    ))
}

#[cfg(test)]
mod tests;
