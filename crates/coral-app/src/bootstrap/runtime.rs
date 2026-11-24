//! The composed runtime: the private gRPC listener, auth support, and the MCP
//! companion, started and stopped as one.

use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;

use coral_mcp::{
    McpHttpConfig, McpHttpError, McpModules, RunningMcpHttpServer, start_authenticated, start_local,
};

use super::engine::Engine;
use super::error::AppError;
use super::server::{
    Composition, PreparedServerBuilder, RunningServer as GrpcServer, ServerBuilder,
};
use super::server_config::{LOCAL_OUTBOUND_DEFAULTS, McpHttpServeConfig, ServeSettings};
use super::sub_agent::{SubAgentDependencies, SubAgentToolsFactory};
use crate::auth::{AuthServerError, CoralAuthorizationServer, RunningCoralAuthorizationServer};

/// What a launching binary adds to Coral beyond what every build has.
///
/// The `coral` binary passes the default. The released binary passes what
/// `ee/` registers, and each Enterprise Feature checks [`Self::edition`] each
/// time it is used (ADR 0033).
#[derive(Clone, Default)]
pub struct Extensions {
    /// Decides the deployment's Edition. The default is always Community.
    pub edition: coral_edition::EditionCheck,
    /// The Secret Store kinds this binary adds to the `env` kind every build
    /// has. A store of any other kind shows as a kind the build lacks.
    pub secret_store_kinds: coral_secret_store::SecretStoreKinds,
    /// The Enterprise Features this binary contains, which Reef shows with
    /// their state. Core registers none.
    pub enterprise_features: coral_edition::EnterpriseFeatures,
}

impl Extensions {
    /// The Edition beside the features this binary registers.
    pub(crate) fn inspection(&self) -> coral_edition::EditionInspection {
        coral_edition::EditionInspection::new(
            self.edition.clone(),
            self.enterprise_features.clone(),
        )
    }
}

pub use coral_db::MigrationProgress;

/// Where startup reports each database migration step it applies.
pub type MigrationReport = std::sync::Arc<dyn Fn(MigrationProgress<'_>) + Send + Sync + 'static>;

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
    /// A config directory the launcher selects. Without one, the environment
    /// or the platform default selects it.
    pub config_dir: Option<std::path::PathBuf>,
    /// The variables that stop this process from sending usage data, as
    /// bootstrap read them. Without them, the process sends nothing.
    pub usage_environment: Option<coral_usage::SendEnvironment>,
    /// What the launching binary adds to Coral.
    pub extensions: Extensions,
    /// Builds the Sub-agent's Tools from its config section. Without one,
    /// no config file can turn the Sub-agent on.
    pub sub_agent_factory: Option<Arc<dyn SubAgentToolsFactory>>,
    /// The `coral` binary that local Run Executors run: for a test that
    /// starts Coral in its own process, whose binary is no `coral`. Without
    /// one, this process's own binary.
    pub run_executor_program: Option<std::path::PathBuf>,
    /// Where startup reports each database migration step it applies. They
    /// run before telemetry starts, so without this a long step is silent.
    pub migration_report: Option<MigrationReport>,
    /// The clock this Coral Instance reads: for a test that starts Coral in
    /// its own process and drives time itself. Without one, the instance
    /// reads the host's clock, as every launching binary does.
    pub clock: Option<Arc<dyn coral_db::Clock>>,
}

impl Launch {
    /// Passes this launch to `builder`.
    fn configure(self, builder: ServerBuilder) -> ServerBuilder {
        let builder = builder.with_extensions(self.extensions);
        let builder = match self.sub_agent_factory {
            Some(factory) => builder.with_sub_agent_factory(factory),
            None => builder,
        };
        let builder = match self.run_executor_program {
            Some(program) => builder.with_run_executor_program(program),
            None => builder,
        };
        let builder = match self.service_version {
            Some(version) => builder.with_service_version(version),
            None => builder,
        };
        let builder = match self.local_web_url {
            Some(url) => builder.with_local_web_url(url),
            None => builder,
        };
        let builder = match self.config_dir {
            Some(config_dir) => builder.with_config_dir(config_dir),
            None => builder,
        };
        let builder = match self.usage_environment {
            Some(environment) => builder.with_usage_environment(environment),
            None => builder,
        };
        let builder = match self.migration_report {
            Some(report) => builder.with_migration_report(report),
            None => builder,
        };
        let builder = match self.clock {
            Some(clock) => builder.with_clock(clock),
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
///
/// Each variant names the phase and the part that failed. A start that fails
/// after a listener is up stops the listeners already started; `cleanup`
/// holds what did not stop.
#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    /// Start: the state root, the database or the configuration was refused,
    /// before anything was composed.
    #[error(transparent)]
    Prepare(#[from] AppError),
    /// Start: a secret the serving settings name could not be read.
    #[error("failed to resolve server configuration: {0}")]
    StartSettings(#[source] AppError),
    /// Start: the `Engine` did not boot, or the private gRPC listener did
    /// not bind.
    #[error("failed to start gRPC server: {0}")]
    StartGrpc(#[source] AppError),
    /// Start: the auth-support listener did not start.
    #[error("failed to start auth-support listener: {error}{}", cleanup_clause(.cleanup.as_deref()))]
    StartAuthSupport {
        /// Why the listener did not start.
        #[source]
        error: AuthServerError,
        /// The listeners that then failed to stop.
        cleanup: Option<Box<ShutdownFailures>>,
    },
    /// Start: the MCP HTTP listener did not start.
    #[error("failed to start MCP HTTP server: {error}{}", cleanup_clause(.cleanup.as_deref()))]
    StartMcp {
        /// Why the listener did not start.
        #[source]
        error: McpStartError,
        /// The listeners that then failed to stop.
        cleanup: Option<Box<ShutdownFailures>>,
    },
    /// Stop: one or more listeners did not stop cleanly.
    #[error("failed to stop server components: {0}")]
    Stop(Box<ShutdownFailures>),
}

impl RuntimeError {
    /// Whether another runtime holds the state root. The condition clears when
    /// that runtime exits, so a launcher may retry.
    #[must_use]
    pub fn state_locked(&self) -> bool {
        matches!(self, Self::Prepare(AppError::StateLocked { .. }))
    }
}

/// What a start failure's message adds when its cleanup failed too.
fn cleanup_clause(cleanup: Option<&ShutdownFailures>) -> String {
    cleanup.map_or_else(String::new, |cleanup| {
        format!("; cleanup also failed: {cleanup}")
    })
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
    start_prepared(builder).await
}

/// The listeners that did not stop cleanly, each with its error.
#[derive(Debug)]
pub struct ShutdownFailures {
    mcp: Option<McpHttpError>,
    auth_support: Option<AuthServerError>,
    grpc: Option<AppError>,
}

impl ShutdownFailures {
    fn from_results(
        mcp: Result<(), McpHttpError>,
        auth_support: Result<(), AuthServerError>,
        grpc: Result<(), AppError>,
    ) -> Result<(), Box<Self>> {
        let failures = Self {
            mcp: mcp.err(),
            auth_support: auth_support.err(),
            grpc: grpc.err(),
        };
        if failures.mcp.is_none() && failures.auth_support.is_none() && failures.grpc.is_none() {
            Ok(())
        } else {
            Err(Box::new(failures))
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

/// Why the MCP HTTP listener did not start.
#[derive(Debug, thiserror::Error)]
pub enum McpStartError {
    /// An `[auth]` deployment reached the listener without its Workspace
    /// authentication or its public host.
    #[error("MCP authentication configuration is missing or invalid")]
    MissingAuthentication,
    /// The registered Sub-agent did not start.
    #[error("the Sub-agent of config section `{section_name}` did not start: {error}")]
    SubAgent {
        /// The config section the Sub-agent is built from.
        section_name: &'static str,
        /// Why its factory could not build it.
        error: Box<dyn std::error::Error + Send + Sync>,
    },
    /// The listener did not bind or serve.
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

    /// The Run Executor listener address.
    #[must_use]
    pub fn run_executor_addr(&self) -> SocketAddr {
        self.grpc.run_executor_addr()
    }

    /// This start's Coral Instance id, which the Runs it holds record.
    #[must_use]
    pub fn coral_instance_id(&self) -> String {
        self.grpc.coral_instance_id()
    }

    /// The MCP HTTP listener address.
    #[must_use]
    pub fn mcp_http_addr(&self) -> SocketAddr {
        self.mcp_http.local_addr()
    }

    /// The auth listener address: Coral's authorization server when sign-in
    /// is on, local auth support otherwise.
    #[must_use]
    pub fn auth_http_addr(&self) -> Option<SocketAddr> {
        self.auth_support
            .as_ref()
            .map(RunningCoralAuthorizationServer::local_addr)
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

    /// The state of the search model's files. `None` when `[search]
    /// semantic_search_enabled` is off: then nothing looks for the files
    /// and nothing downloads them.
    #[must_use]
    pub fn search_model(&self) -> Option<super::SearchModel> {
        self.grpc.search_model()
    }

    /// What startup made of the configured License, for the launcher to
    /// show the operator.
    #[must_use]
    pub fn license_notices(&self) -> &[coral_edition::LicenseNotice] {
        self.grpc.license_notices()
    }

    /// Completes when the runtime stops serving on its own, or when the
    /// License recheck finds that Coral must stop.
    pub async fn wait_for_exit(&self) {
        self.grpc.wait_for_exit().await;
    }

    /// Why the License recheck stopped Coral, if it has: shut down, then
    /// exit with it as the error.
    #[must_use]
    pub fn license_lapse(&self) -> Option<coral_edition::LicenseRefusal> {
        self.grpc.license_lapse()
    }

    /// Shuts the runtime down gracefully, then stops every listener.
    ///
    /// Readiness reports not serving at once, while every listener keeps
    /// serving for the Shutdown Drain (`[runtime] shutdown_drain_seconds`,
    /// see [`coral_run::RunModule::shutdown`]). The drain and the commit grace
    /// run one after the other, so an orchestrator's termination grace must
    /// exceed their sum.
    ///
    /// After the drain, the stop waits for the search index's writes, and the
    /// listeners keep serving meanwhile. A write that builds its rows holds
    /// the stop until they are built, with no bound: the row build cannot
    /// stop part way, and the Tokio runtime waits for it before the process
    /// exits in any case.
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
            .map_err(RuntimeError::Stop)
    }
}

/// Composes the served surfaces after the state owner has prepared persistent
/// state, so no configuration parser runs before the state-format fence.
async fn start_prepared(builder: PreparedServerBuilder) -> Result<RunningRuntime, RuntimeError> {
    Box::pin(start_inner(builder)).await
}

/// The runtime owns the implicit-user local experience, so an auth-disabled
/// deployment gets local outbound defaults. Ordinary server builders retain
/// deny/deny; authentication never changes explicit fields.
fn with_local_launch_defaults(builder: PreparedServerBuilder) -> PreparedServerBuilder {
    if builder.authenticated() {
        builder
    } else {
        builder.with_outbound_defaults(LOCAL_OUTBOUND_DEFAULTS)
    }
}

async fn start_inner(builder: PreparedServerBuilder) -> Result<RunningRuntime, RuntimeError> {
    let builder = with_local_launch_defaults(builder);
    // The one read of the session signing key and the provider secret.
    let mut settings = builder
        .serve_settings()
        .map_err(RuntimeError::StartSettings)?;
    let public_mcp_origin = match settings.mcp_http() {
        McpHttpServeConfig::Authenticated { public_url, .. } => Some(public_url.clone()),
        McpHttpServeConfig::AuthDisabled { .. } => None,
    };
    let auth_bind_addr = settings.auth_bind_addr();
    let session_auth = settings.take_session_auth();
    let grpc_authentication_enabled = session_auth.is_some();

    // The `Engine` first, then each listener over it.
    let Composition {
        engine,
        private_listeners,
        authorization_server,
        workspace_authentication,
        clock,
    } = builder
        .compose(session_auth)
        .await
        .map_err(RuntimeError::StartGrpc)?;
    let grpc = private_listeners
        .serve(engine)
        .await
        .map_err(RuntimeError::StartGrpc)?;
    let auth_support = match start_auth_support(
        authorization_server.map(|server| *server),
        auth_bind_addr,
        grpc.engine(),
        clock.clone(),
    )
    .await
    {
        Ok(server) => server,
        Err(error) => {
            let cleanup = shutdown_components(grpc, None, None).await.err();
            return Err(RuntimeError::StartAuthSupport { error, cleanup });
        }
    };
    let mcp_http = match start_mcp_http(&settings, &grpc, workspace_authentication, clock).await {
        Ok(server) => server,
        Err(error) => {
            let cleanup = shutdown_components(grpc, auth_support, None).await.err();
            return Err(RuntimeError::StartMcp { error, cleanup });
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

/// Starts Coral's authorization server when sign-in is on, and local auth
/// support, with the Provider OAuth callback, otherwise.
async fn start_auth_support(
    authorization_server: Option<CoralAuthorizationServer>,
    bind_addr: SocketAddr,
    engine: &Engine,
    clock: Arc<dyn coral_db::Clock>,
) -> Result<Option<RunningCoralAuthorizationServer>, AuthServerError> {
    match authorization_server {
        Some(server) => server.start().await.map(Some),
        None => CoralAuthorizationServer::start_local_with_provider_oauth(
            bind_addr,
            engine.readiness(),
            engine.accounts(),
            clock,
        )
        .await
        .map(Some),
    }
}

async fn shutdown_components(
    grpc: GrpcServer,
    auth_support: Option<RunningCoralAuthorizationServer>,
    mcp_http: Option<RunningMcpHttpServer>,
) -> Result<(), Box<ShutdownFailures>> {
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

/// Starts the MCP companion over `grpc`'s `Engine`. Without sign-in, its
/// authentication reads `clock`.
async fn start_mcp_http(
    settings: &ServeSettings,
    grpc: &GrpcServer,
    authentication: Option<crate::WorkspaceAuthentication>,
    clock: Arc<dyn coral_db::Clock>,
) -> Result<RunningMcpHttpServer, McpStartError> {
    let engine = grpc.engine();
    let readiness = engine.readiness().probe();
    let modules = engine
        .mcp_modules(settings.human_interface().clone())
        .with_elicitation(settings.elicitation());
    let modules = with_registered_sub_agent(modules, settings, grpc, &clock)?;
    let server = match settings.mcp_http().clone() {
        McpHttpServeConfig::AuthDisabled { bind_addr } => {
            start_local(McpHttpConfig::new(bind_addr)?, modules, readiness, clock).await?
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

/// Wires the registered factory's Sub-agent into the Runs of `modules`, when
/// the config file has the factory's section.
fn with_registered_sub_agent(
    modules: McpModules,
    settings: &ServeSettings,
    grpc: &GrpcServer,
    clock: &Arc<dyn coral_db::Clock>,
) -> Result<McpModules, McpStartError> {
    let Some(configured) = &settings.sub_agent else {
        return Ok(modules);
    };
    let tools = configured.build(SubAgentDependencies {
        database: grpc.engine().database(),
        modules: modules.clone(),
        outbound_policy: settings.outbound_policy(),
        env: Arc::new(super::env_var),
        clock: Arc::clone(clock),
        stopping: grpc.engine().readiness().stopping(),
    })?;
    modules.runs.set_sub_agent_tools(tools);
    Ok(modules)
}

#[cfg(test)]
mod tests;
