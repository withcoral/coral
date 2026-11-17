//! Composes one Coral Instance and runs its private gRPC server.

use std::net::{Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;

use tokio::net::TcpListener;
use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;
use tonic::service::Routes;
use tonic::transport::Server;
use tonic::transport::server::TcpIncoming;
use zeroize::Zeroizing;

use super::engine::{
    BootedEngine, CredentialParts, Engine, EngineParts, LocalChildren, RunExecutorLink,
    RunExecutorParts, SearchParts, UsageReporting,
};
use super::env::AppEnvironment;
use super::error::AppError;
use super::health::EngineReadiness;
use super::search_model::{Delivery, SearchModel};
use super::server_config::{
    DeploymentConfig, Launcher, LoadedServerConfig, ServeSettings, SessionAuthSettings,
};
use crate::auth::CoralAuthorizationServer;
use crate::state::DatabaseConfig;
use crate::state::{AppStateLayout, InitializedState, initialize_state};
use crate::telemetry::TelemetryConfig;
use coral_db::{Database, DatabaseOptions};
use coral_telemetry::safe;

/// HTTP/2 `SETTINGS_MAX_HEADER_LIST_SIZE` for the private listener.
const HTTP2_MAX_HEADER_LIST_SIZE: u32 = 128 * 1024;

/// How long shutdown waits for the private gRPC and Run Executor listeners to
/// close their connections. An open stream ends on the stop signal, so only
/// a request still in flight, or a client that does not read, lasts this long.
const SHUTDOWN_GRACE_PERIOD: std::time::Duration = std::time::Duration::from_secs(5);

#[derive(Clone)]
enum ServerModeSelection {
    Explicit(ServerMode),
    ConfiguredStandaloneGrpc,
}

/// Server-side bootstrap configuration for the Coral server.
#[derive(Clone)]
pub(crate) struct ServerConfig {
    config_dir: Option<PathBuf>,
    mode: ServerModeSelection,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl ServerConfig {
    pub(crate) fn new() -> Self {
        Self {
            config_dir: None,
            mode: ServerModeSelection::Explicit(ServerMode::EphemeralGrpc),
        }
    }

    pub(crate) fn with_config_dir(mut self, config_dir: impl Into<PathBuf>) -> Self {
        self.config_dir = Some(config_dir.into());
        self
    }

    pub(crate) fn with_mode(mut self, mode: ServerMode) -> Self {
        self.mode = ServerModeSelection::Explicit(mode);
        self
    }

    pub(crate) fn with_configured_standalone_grpc(mut self) -> Self {
        self.mode = ServerModeSelection::ConfiguredStandaloneGrpc;
        self
    }

    /// The private gRPC address this composition selects itself; none when
    /// `[listeners.grpc].bind` selects it.
    fn grpc_bind(&self) -> Option<SocketAddr> {
        match &self.mode {
            ServerModeSelection::Explicit(mode) => Some(mode.bind_addr()),
            ServerModeSelection::ConfiguredStandaloneGrpc => None,
        }
    }
}

/// Concrete local server mode.
///
/// These variants select the private listener for process composition and tests;
/// the runtime process always composes MCP alongside it.
#[derive(Clone)]
pub enum ServerMode {
    /// Ephemeral native gRPC for local process composition and lifecycle tests.
    EphemeralGrpc,
    /// Native gRPC bound to an explicit address for a standalone server.
    StandaloneGrpc {
        /// Address to bind.
        bind: SocketAddr,
    },
}

impl ServerMode {
    fn bind_addr(&self) -> SocketAddr {
        match self {
            Self::EphemeralGrpc => SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
            Self::StandaloneGrpc { bind } => *bind,
        }
    }
}

/// Builder for the Coral server runtime.
#[derive(Default)]
pub struct ServerBuilder {
    config: ServerConfig,
    outbound_defaults: coral_outbound::DestinationPolicy,
    launcher: Launcher,
}

/// A server builder whose complete persistent state has passed the startup
/// fence and completed fresh initialization, and whose configuration passed
/// every rule that needs no secret.
///
/// This temporal type lets process composition read companion settings
/// without re-running or reordering state-format checks. Call [`Self::start`]
/// to consume it and start gRPC.
pub struct PreparedServerBuilder {
    builder: ServerBuilder,
    state: InitializedState,
    deployment_config: DeploymentConfig,
}

impl ServerBuilder {
    #[must_use]
    /// Creates an ephemeral native gRPC server with deny/deny outbound defaults.
    /// Local launchers explicitly supply their own outbound default overrides.
    pub fn new() -> Self {
        Self {
            config: ServerConfig::new(),
            outbound_defaults: coral_outbound::DestinationPolicy::default(),
            launcher: Launcher::default(),
        }
    }

    #[must_use]
    /// Creates a builder for an ephemeral native gRPC local server.
    pub fn ephemeral_grpc() -> Self {
        Self::new().with_mode(ServerMode::EphemeralGrpc)
    }

    #[must_use]
    /// Creates a standalone native gRPC server bound to an explicit address.
    pub fn standalone_grpc(bind: SocketAddr) -> Self {
        Self::new().with_mode(ServerMode::StandaloneGrpc { bind })
    }

    #[must_use]
    /// Creates a standalone gRPC server using `[listeners.grpc].bind`.
    pub fn configured_standalone_grpc() -> Self {
        Self {
            config: ServerConfig::new().with_configured_standalone_grpc(),
            ..Self::new()
        }
    }

    /// Supplies composition defaults. Explicit operator fields always win.
    #[must_use]
    pub fn with_outbound_defaults(mut self, defaults: coral_outbound::DestinationPolicy) -> Self {
        self.outbound_defaults = defaults;
        self
    }

    /// Names the launching binary's version in exported telemetry, as
    /// `service.version`. Without one, Coral's package version names it.
    #[must_use]
    pub fn with_service_version(mut self, version: &'static str) -> Self {
        self.launcher.service_version = Some(version);
        self
    }

    /// Supplies the variables, read at process bootstrap, that stop this
    /// process from sending usage data. A process that never calls this
    /// sends nothing.
    #[must_use]
    pub fn with_usage_environment(mut self, environment: coral_usage::SendEnvironment) -> Self {
        self.launcher.usage_environment = Some(environment);
        self
    }

    /// Supplies what the launching binary adds to Coral. Without it, Coral
    /// runs as the Community Edition.
    #[must_use]
    pub fn with_extensions(mut self, extensions: super::runtime::Extensions) -> Self {
        self.launcher.extensions = extensions;
        self
    }

    /// Compiles in this process instead of on Run Executors, for a test
    /// that starts Coral in its own process: its binary is no `coral`, so
    /// it cannot start a local child.
    #[must_use]
    pub fn with_in_process_compiler(mut self) -> Self {
        self.launcher.compile_in_process = true;
        self
    }

    /// Supplies where startup reports each database migration step it
    /// applies. Migrations run before telemetry starts and before any
    /// listener, so this is the one sign of a long step. Without it, startup
    /// reports none.
    #[must_use]
    pub fn with_migration_report(mut self, report: super::runtime::MigrationReport) -> Self {
        self.launcher.migration_report = Some(report);
        self
    }

    /// Marks a Desktop launch: local auth support then defaults to Desktop's
    /// fixed Provider OAuth callback port. An explicit `listeners.auth.bind` wins.
    #[must_use]
    pub fn with_desktop_launch(mut self) -> Self {
        self.launcher.desktop = true;
        self
    }

    /// Supplies a local launcher's web origin for human handoff.
    ///
    /// It replaces `listeners.web.public_url` only without `[auth]`; serving
    /// fails if both are present or if `[auth]` is configured.
    #[must_use]
    pub fn with_local_web_url(mut self, url: coral_mcp::HumanInterfaceUrl) -> Self {
        self.launcher.local_web_url = Some(url);
        self
    }

    #[must_use]
    /// Selects the local server mode.
    pub fn with_mode(mut self, mode: ServerMode) -> Self {
        self.config = self.config.with_mode(mode);
        self
    }

    #[must_use]
    /// Overrides the Coral config directory used by the local server.
    pub fn with_config_dir(mut self, config_dir: impl Into<PathBuf>) -> Self {
        self.config = self.config.with_config_dir(config_dir);
        self
    }

    /// Passes the complete persistent state through its one initialization
    /// Module before any caller can read companion configuration or start a
    /// listener.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when the state root or configured database is
    /// retired or malformed, when the configuration is invalid, or when
    /// fresh-state initialization fails. Invalid configuration leaves a fresh
    /// state root unchanged.
    pub async fn prepare(self) -> Result<PreparedServerBuilder, AppError> {
        let environment = AppEnvironment::discover();
        let layout = environment.app_state_layout(self.config.config_dir.clone())?;
        let captured = LoadedServerConfig::load(&layout)?;
        let report_migration = self.launcher.migration_report.clone();
        let (state, deployment_config) = initialize_state(
            layout,
            captured.raw(),
            resolve_database_config,
            || {
                let config = captured.validate(&self.launcher, self.config.grpc_bind())?;
                let vector_storage = config.vector_storage();
                Ok((config, vector_storage))
            },
            report_migration.as_deref().unwrap_or(&|_| {}),
        )
        .await?;
        Ok(PreparedServerBuilder {
            builder: self,
            state,
            deployment_config,
        })
    }

    /// Starts the Coral gRPC server on TCP.
    ///
    /// This convenience Interface prepares state and then starts the server.
    /// With `[auth]` configured, startup prepares the authorization server
    /// over the migrated database and installs private authentication against
    /// the same authority; taking it from the returned [`RunningServer`] and
    /// running its transport stays the caller's job. Process composition that
    /// needs companion settings should call [`Self::prepare`] and use the
    /// returned [`PreparedServerBuilder`].
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] if state preparation, configuration, credential
    /// initialization, or gRPC startup fails.
    pub async fn start(self) -> Result<RunningServer, AppError> {
        self.prepare().await?.start().await
    }
}

impl PreparedServerBuilder {
    /// Resolves settings for transports served beside the prepared gRPC server.
    ///
    /// The configuration is already validated. With `[auth]` configured, this
    /// reads the session signing key and the provider secret from their
    /// sources.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when a secret source cannot be read or holds an
    /// unusable value.
    pub fn serve_settings(&self) -> Result<ServeSettings, AppError> {
        self.deployment_config
            .serve_settings(self.builder.outbound_defaults)
    }

    /// Whether `[auth]` is configured, so every listener authenticates.
    pub(crate) fn authenticated(&self) -> bool {
        self.deployment_config.authenticated()
    }

    /// Supplies composition defaults without changing the operator snapshot.
    #[must_use]
    pub fn with_outbound_defaults(mut self, defaults: coral_outbound::DestinationPolicy) -> Self {
        self.builder.outbound_defaults = defaults;
        self
    }

    /// Starts the prepared Coral gRPC server on TCP.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] if a secret cannot be read, or if credential
    /// initialization or gRPC startup fails.
    pub async fn start(self) -> Result<RunningServer, AppError> {
        let session_auth = self.serve_settings()?.take_session_auth();
        let Composition {
            engine,
            private_listeners,
            authorization_server,
            workspace_authentication: _,
        } = self.compose(session_auth).await?;
        private_listeners
            .holding(authorization_server)
            .serve(engine)
            .await
    }

    /// Composes this Coral Instance: its `Engine`, booted, and what each
    /// listener is built from beside it. `session_auth` is the session
    /// authentication that [`Self::serve_settings`] resolved, when `[auth]`
    /// is configured.
    ///
    /// Only the Run Executor listener is bound when this returns. Nothing
    /// serves yet, and no periodic pass runs.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] if credential initialization, a secret read or
    /// the `Engine`'s boot fails.
    pub(crate) async fn compose(
        self,
        session_auth: Option<SessionAuthSettings>,
    ) -> Result<Composition, AppError> {
        let Self {
            builder,
            state,
            deployment_config,
        } = self;
        let launcher = builder.launcher;
        let policy = deployment_config.outbound_policy(builder.outbound_defaults);
        let vault = open_credential_vault(
            deployment_config.credential_key_env(),
            state.credential_key_path(),
        )?;
        let search_model = search_model_delivery(&deployment_config, state.state_dir(), policy)?;
        let (database, state_lock) = state.into_parts();
        let run_executor_listener =
            deployment_config.run_executor_listener(&AppEnvironment::required_env_var)?;
        let local_children = match deployment_config.local_run_executors() {
            Some(settings) => Some(LocalChildren {
                settings,
                program: std::env::current_exe()?,
                environment: super::env::run_executor_environment(),
            }),
            None => None,
        };
        let workspace_authentication = session_auth
            .as_ref()
            .map(|auth| auth.workspace_authentication(database.clone()));
        let authentication = private_authentication(session_auth.as_ref(), &database).await?;
        // Coral publishes its Client ID Metadata Document at Reef's public HTTPS
        // origin, which is what upstream authorization servers accept as client_id.
        let client_id_metadata_document = session_auth
            .as_ref()
            .and_then(|auth| auth.web_public_url())
            .map(|origin| format!("{origin}/.well-known/oauth-client"));
        // One readiness for every listener, so shutdown takes them all out of
        // rotation at once.
        let readiness = EngineReadiness::from_database(Arc::clone(&database));
        let authorization_server = session_auth
            .map(|session_auth| {
                build_authorization_server(session_auth, &database, policy, readiness.clone())
            })
            .transpose()?;
        let releases = release_check(&deployment_config, policy)?;
        let usage =
            UsageReporting::new(&database, &launcher, authorization_server.is_some(), policy)?;
        let secret_stores = deployment_config.secret_stores(policy)?;
        // This start's Coral Instance, recorded on the Runs it claims. The
        // first start in a process also names it in exported telemetry.
        let instance = coral_run::CoralInstanceId::generate();
        init_server_telemetry(
            deployment_config.telemetry(),
            &instance.to_string(),
            launcher.service_version,
        )?;
        tracing::info!(
            coral.edition = safe::kind(launcher.extensions.edition.current().as_str()),
            "Coral Instance starting"
        );
        let (engine, run_executors) = Engine::boot(EngineParts {
            database,
            state_lock,
            instance,
            readiness,
            policy,
            runtime_limits: deployment_config.runtime_limits(),
            credentials: CredentialParts {
                vault,
                secret_stores,
                client_id_metadata_document,
                client_bootstrap_policy: deployment_config.mcp_oauth_client_bootstrap(),
            },
            search: SearchParts {
                reconcile_interval: deployment_config.search_reconcile_interval(),
                model: search_model,
            },
            run_executors: RunExecutorParts {
                listener: run_executor_listener,
                local_children,
                compile_in_process: launcher.compile_in_process,
            },
            releases,
            usage,
        })
        .await?;
        let authorization_server =
            authorization_server.map(|server| Box::new(server.with_providers(engine.providers())));
        Ok(Composition {
            engine,
            private_listeners: PrivateListeners {
                grpc_bind: deployment_config.grpc_bind(),
                authentication,
                run_executors,
                held_authorization_server: None,
            },
            authorization_server,
            workspace_authentication,
        })
    }
}

/// One composed Coral Instance before its listeners serve: the booted
/// `Engine`, and what each listener is built from beside it.
pub(crate) struct Composition {
    pub(crate) engine: BootedEngine,
    pub(crate) private_listeners: PrivateListeners,
    /// The authorization server this instance's logins are provisioned by,
    /// when `[auth]` is configured. Running its transport is the caller's
    /// decision.
    pub(crate) authorization_server: Option<Box<CoralAuthorizationServer>>,
    /// Public Workspace credential verification, when `[auth]` is configured.
    pub(crate) workspace_authentication: Option<crate::WorkspaceAuthentication>,
}

/// The private gRPC listener and the Run Executor listener served beside it.
pub(crate) struct PrivateListeners {
    grpc_bind: SocketAddr,
    authentication: coral_auth::PrincipalAuthentication,
    run_executors: RunExecutorLink,
    held_authorization_server: Option<Box<CoralAuthorizationServer>>,
}

impl PrivateListeners {
    /// Leaves `server` on the running server, for a caller that takes it
    /// from there and runs its transport itself.
    fn holding(mut self, server: Option<Box<CoralAuthorizationServer>>) -> Self {
        self.held_authorization_server = server;
        self
    }

    /// Binds the private gRPC listener, starts `engine`'s background work,
    /// and serves the listener, and the Run Executor listener, over it.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when the private gRPC address cannot be bound.
    /// No periodic pass has run then.
    pub(crate) async fn serve(self, engine: BootedEngine) -> Result<RunningServer, AppError> {
        let listener = TcpListener::bind(self.grpc_bind).await?;
        let local_addr = listener.local_addr()?;
        let engine = engine.run();
        let mcp_origin = Arc::new(std::sync::OnceLock::new());
        // One stop signal for both listeners and the streams they serve.
        let (stop, stopping) = watch::channel(false);
        let routes = coral_grpc::routes(
            engine.grpc_dependencies(),
            coral_grpc::TransportContext {
                authentication: self.authentication,
                mcp_origin: mcp_origin.clone(),
                readiness: engine.readiness().probe(),
                stopping: stopping.clone(),
            },
        );
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let (task_finished_tx, task_finished) = watch::channel(false);
        let task = start_grpc_server(
            listener,
            self.run_executors,
            shutdown_rx,
            (stop, stopping),
            routes,
            task_finished_tx,
        );
        Ok(RunningServer {
            mcp_origin,
            endpoint_uri: format!("http://{local_addr}"),
            local_addr,
            authorization_server: self.held_authorization_server,
            shutdown_tx: Mutex::new(Some(shutdown_tx)),
            task_finished,
            task: Mutex::new(Some(task)),
            engine,
        })
    }
}

/// How the search model's files arrive. With `[search]
/// semantic_search_enabled` off there is none: nothing looks for the files and
/// nothing downloads them.
fn search_model_delivery(
    config: &DeploymentConfig,
    state_dir: &Path,
    policy: coral_outbound::DestinationPolicy,
) -> Result<Option<Delivery>, AppError> {
    if !config.semantic_search_enabled() {
        return Ok(None);
    }
    coral_outbound::Client::new(policy, super::search_model::CONNECT_TIMEOUT)
        .map(|client| Some(Delivery::coral(state_dir, client)))
        .map_err(|source| AppError::HttpClient {
            purpose: "search model download",
            source,
        })
}

/// The release check, while `[updates] check` is on.
fn release_check(
    config: &DeploymentConfig,
    policy: coral_outbound::DestinationPolicy,
) -> Result<Option<coral_release::ReleaseCheck>, AppError> {
    if !config.update_check() {
        return Ok(None);
    }
    coral_outbound::Client::new(policy, coral_release::REQUEST_TIMEOUT)
        .map(|client| Some(coral_release::ReleaseCheck::coral(client)))
        .map_err(|source| AppError::HttpClient {
            purpose: "release check",
            source,
        })
}

/// The authentication of the private gRPC listener: the session's, or the
/// local Organization Owner's without `[auth]`, whose login this provisions.
async fn private_authentication(
    session_auth: Option<&SessionAuthSettings>,
    database: &Arc<Database>,
) -> Result<coral_auth::PrincipalAuthentication, AppError> {
    if let Some(auth) = session_auth {
        return Ok(auth.principal_authentication(database.clone()));
    }
    coral_workspace::OrganizationModule::new(database.clone())
        .provision_login(&coral_workspace::ChangeContext::new(
            coral_workspace::local_organization_owner(),
            0,
            coral_workspace::TraceId::current_or_new(),
        ))
        .await
        .map_err(AppError::LocalAuthority)?;
    Ok(coral_auth::PrincipalAuthentication::local(
        coral_workspace::WorkspaceModule::new(database.clone()),
    ))
}

/// Prepares the authorization server this instance's logins are provisioned by.
///
/// It never opens a database of its own: it records verified logins,
/// authorization codes, and registered clients in the single pool.
fn build_authorization_server(
    session_auth: SessionAuthSettings,
    coral_db: &Arc<Database>,
    policy: coral_outbound::DestinationPolicy,
    readiness: EngineReadiness,
) -> Result<Box<CoralAuthorizationServer>, AppError> {
    Ok(Box::new(
        session_auth
            .into_authorization_server(policy, Arc::clone(coral_db))?
            .with_readiness(readiness),
    ))
}

fn init_server_telemetry(
    config: &TelemetryConfig,
    instance_id: &str,
    service_version: Option<&'static str>,
) -> Result<(), AppError> {
    let service_version = service_version.unwrap_or(env!("CARGO_PKG_VERSION"));
    crate::telemetry::init_tracing(config, instance_id, service_version)
}

/// Opens the host credential vault from the source the deployment selects:
/// the named environment variable, else the key file at `key_path`.
///
/// A key file beside a configured variable is refused rather than ignored.
/// The two may differ, and sealing new credentials under one key while the
/// stored ones need the other would only surface later as unreadable
/// credentials. The operator moves the file's key into the secret, or drops
/// the setting, and says so by removing the file.
fn open_credential_vault(
    key_env: Option<&str>,
    key_path: &Path,
) -> Result<coral_credentials::CredentialVault, AppError> {
    const KEY_ENV: &str = "credentials.key_env";
    let Some(key_env) = key_env else {
        return coral_credentials::CredentialVault::open(key_path).map_err(AppError::CredentialKey);
    };
    // Only a confirmed absence lets the secret stand in: an entry of any kind
    // is the conflict, and a path that cannot be inspected is not known to be
    // empty.
    match std::fs::symlink_metadata(key_path) {
        Ok(_entry) => {
            return Err(AppError::config(
                KEY_ENV,
                format!(
                    "{KEY_ENV} is configured but a key file exists at '{}'; encode that file's key into `{key_env}` and remove the file, or remove {KEY_ENV}",
                    key_path.display()
                ),
            ));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(AppError::config(
                KEY_ENV,
                format!(
                    "{KEY_ENV} is configured but the key file path '{}' cannot be inspected: {error}",
                    key_path.display()
                ),
            ));
        }
    }
    let encoded = Zeroizing::new(AppEnvironment::required_env_var(KEY_ENV, key_env)?);
    coral_credentials::CredentialVault::from_encoded_key(&encoded).map_err(|_error| {
        AppError::invalid_secret(
            KEY_ENV,
            key_env,
            format!(
                "environment variable `{key_env}` must contain the standard base64 encoding of a {}-byte credential key",
                coral_credentials::CredentialVault::KEY_LEN
            ),
        )
    })
}

fn resolve_database_config(
    layout: &AppStateLayout,
    raw: &str,
) -> Result<DatabaseOptions, AppError> {
    match DatabaseConfig::from_toml(raw, layout)? {
        DatabaseConfig::Sqlite { path } => Ok(DatabaseOptions::Sqlite { path }),
        DatabaseConfig::Postgres { url_env } => Ok(DatabaseOptions::Postgres {
            url: AppEnvironment::required_env_var("database backend 'postgres'", &url_env)?,
        }),
    }
}

/// Running Coral server: the `Engine`, and the private gRPC listener and the
/// Run Executor listener that serve over it.
///
/// Call [`RunningServer::shutdown`] for deterministic teardown. Dropping this
/// handle sends shutdown to the background task as a best-effort fallback, but
/// does not wait for the task to finish.
pub struct RunningServer {
    mcp_origin: Arc<std::sync::OnceLock<String>>,
    endpoint_uri: String,
    local_addr: SocketAddr,
    authorization_server: Option<Box<CoralAuthorizationServer>>,
    shutdown_tx: Mutex<Option<oneshot::Sender<()>>>,
    task_finished: watch::Receiver<bool>,
    task: Mutex<Option<JoinHandle<Result<(), tonic::transport::Error>>>>,
    /// Declared last: it holds the state root's lock, so the lock is
    /// released after every other field.
    engine: Engine,
}

impl RunningServer {
    #[must_use]
    /// Returns the endpoint URI for this server.
    ///
    /// Process composition and integration tests use this to reach the
    /// listener whose lifecycle they control.
    pub fn endpoint_uri(&self) -> &str {
        &self.endpoint_uri
    }

    /// Returns the address bound by this server.
    #[must_use]
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Returns the initialized persistence foundation for application-owned
    /// companion composition.
    ///
    /// The caller may use this handle only to construct transport-neutral
    /// domain Modules. Persistence queries remain owned by those Modules.
    #[must_use]
    pub fn database(&self) -> Arc<Database> {
        self.engine.database()
    }

    /// The `Engine` this server serves over, which every other listener of
    /// the process is built over too.
    pub(crate) fn engine(&self) -> &Engine {
        &self.engine
    }

    /// Takes the authorization server prepared from this server's session
    /// authentication, when [`ServerBuilder::start`] composed one.
    ///
    /// Preparing it during app startup is what lets it share the one migrated
    /// state database. Whether and how its transport runs is the caller's
    /// decision, so it transfers out — and only the first caller receives it.
    #[must_use]
    pub fn take_authorization_server(&mut self) -> Option<CoralAuthorizationServer> {
        self.authorization_server.take().map(|server| *server)
    }

    /// Waits until the background server task exits.
    ///
    /// This method is cancellation-safe and does not initiate shutdown,
    /// consume the task result, or release server resources. Call
    /// [`RunningServer::shutdown`] afterward to join the task, surface errors,
    /// and complete cleanup.
    pub async fn wait_for_exit(&self) {
        let mut task_finished = self.task_finished.clone();
        let _finished = task_finished.wait_for(|finished| *finished).await;
    }

    /// Shuts the server down and waits for the background task to finish,
    /// for the shutdown grace period at most.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] if the server task fails while shutting down, or
    /// if a connection is still open when the grace period ends.
    pub async fn shutdown(self) -> Result<(), AppError> {
        self.shutdown_inner().await
    }

    /// Reports not ready, stops the expired-lease sweep and drains the Runs
    /// ([`coral_run::RunModule::shutdown`]) while every listener still
    /// serves; stop the listeners after it.
    pub async fn drain_runs(&self) {
        self.engine.drain_runs().await;
    }

    async fn shutdown_inner(&self) -> Result<(), AppError> {
        self.engine.drain_runs().await;
        self.engine.stop().await;
        self.stop_listeners();
        let task = self.task.lock().expect("task mutex poisoned").take();
        match task {
            Some(task) => join_listeners(task).await,
            None => Ok(()),
        }
    }

    /// Tells the listener task to stop. A second call does nothing.
    fn stop_listeners(&self) {
        if let Some(shutdown_tx) = self
            .shutdown_tx
            .lock()
            .expect("shutdown mutex poisoned")
            .take()
        {
            #[expect(
                clippy::let_underscore_must_use,
                reason = "send error means the receiver is already dropped, which is fine during shutdown"
            )]
            let _ = shutdown_tx.send(());
        }
    }

    /// The state of the search model's files. `None` when `[search]
    /// semantic_search_enabled` is off: then nothing looks for the files
    /// and nothing downloads them.
    #[must_use]
    pub fn search_model(&self) -> Option<SearchModel> {
        self.engine.search_model()
    }

    /// Publishes the canonical MCP origin after the companion listener binds.
    /// Bootstrap supplies the validated public origin or actual local listener address.
    /// # Panics
    /// Panics if bootstrap attempts to publish a second origin.
    pub fn publish_mcp_origin(&self, origin: String) {
        self.mcp_origin
            .set(origin)
            .expect("MCP origin published once at bootstrap");
    }
}

impl Drop for RunningServer {
    fn drop(&mut self) {
        self.engine.halt();
        self.stop_listeners();
    }
}

/// Joins the listener task, which ends once every connection has closed. A
/// connection still open after [`SHUTDOWN_GRACE_PERIOD`] holds shutdown no
/// longer: the task is aborted.
async fn join_listeners(
    mut task: JoinHandle<Result<(), tonic::transport::Error>>,
) -> Result<(), AppError> {
    match tokio::time::timeout(SHUTDOWN_GRACE_PERIOD, &mut task).await {
        Ok(Ok(Ok(()))) => Ok(()),
        Ok(Ok(Err(error))) => Err(AppError::from(error)),
        Ok(Err(error)) => Err(AppError::from(error)),
        Err(_elapsed) => {
            task.abort();
            drop(task.await);
            Err(AppError::ShutdownTimedOut)
        }
    }
}

/// Serves the private gRPC listener and the Run Executor listener until
/// shutdown, or until either fails. Shutdown sets `stop`, which `routes`
/// observes too; dropping the task drops `stop`, which reads the same.
fn start_grpc_server(
    listener: TcpListener,
    run_executors: RunExecutorLink,
    shutdown_rx: oneshot::Receiver<()>,
    (stop, stopping): (watch::Sender<bool>, watch::Receiver<bool>),
    routes: Routes,
    task_finished: watch::Sender<bool>,
) -> JoinHandle<Result<(), tonic::transport::Error>> {
    // `Server::tcp_nodelay` is ignored for a stream that the caller supplies.
    let incoming = TcpIncoming::from(listener).with_nodelay(Some(true));
    tokio::spawn(async move {
        let stopped = |mut stopping: watch::Receiver<bool>| async move {
            drop(stopping.wait_for(|stopping| *stopping).await);
        };
        let grpc = Server::builder()
            .http2_max_header_list_size(HTTP2_MAX_HEADER_LIST_SIZE)
            .add_routes(routes)
            .serve_with_incoming_shutdown(incoming, stopped(stopping.clone()));
        let run_executors = run_executors
            .server
            .serve(run_executors.listener, stopped(stopping));
        let shutdown = async move {
            drop(shutdown_rx.await);
            stop.send_replace(true);
            Ok(())
        };
        let result = tokio::try_join!(shutdown, grpc, run_executors).map(drop);
        task_finished.send_replace(true);
        result
    })
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::{
        AppError, SHUTDOWN_GRACE_PERIOD, ServerBuilder, join_listeners, open_credential_vault,
        private_authentication,
    };

    /// Catches: a start that says only that credential storage failed, so
    /// the operator cannot tell a missing folder from a damaged key file.
    #[test]
    fn a_key_file_that_cannot_be_opened_reports_why() {
        let temp = TempDir::new().unwrap();
        let damaged = temp.path().join("damaged.key");
        std::fs::write(&damaged, [7_u8; 8]).unwrap();
        let no_folder = temp.path().join("absent").join("credentials.key");
        let is_missing_folder: fn(&coral_credentials::Error) -> bool = |cause| {
            matches!(cause, coral_credentials::Error::Io(error)
                if error.kind() == std::io::ErrorKind::NotFound)
        };
        for (case, key_path, is_cause) in [
            ("no folder for the key file", &no_folder, is_missing_folder),
            ("a key file of the wrong length", &damaged, |cause| {
                matches!(cause, coral_credentials::Error::InvalidKey)
            }),
        ] {
            let Err(error) = open_credential_vault(None, key_path) else {
                panic!("{case}: the vault must not open");
            };
            assert!(
                matches!(&error, AppError::CredentialKey(cause) if is_cause(cause)),
                "{case}: {error:?}"
            );
            let source = std::error::Error::source(&error).expect("the cause is the source");
            assert!(
                source
                    .downcast_ref::<coral_credentials::Error>()
                    .is_some_and(is_cause),
                "{case}: {source}"
            );
        }
        assert_eq!(std::fs::read(&damaged).unwrap(), [7_u8; 8]);
    }

    /// Catches: a start that says only that the local authority failed, with
    /// the database's answer dropped.
    #[tokio::test]
    async fn a_refused_local_authority_write_reports_the_database_error() {
        let fixture = coral_test_support::TestDatabase::new(coral_db::migration_plan())
            .await
            .expect("apply current migrations");
        fixture
            .execute_fixture_sql(
                "ALTER TABLE organization_authority_guard RENAME TO organization_authority_moved",
            )
            .await
            .expect("move the table the login holds");

        let Err(error) = private_authentication(None, &fixture.database()).await else {
            panic!("the login cannot be provisioned without its table");
        };

        assert!(
            matches!(&error, AppError::LocalAuthority(cause) if cause.database_error().is_some()),
            "{error:?}"
        );
        let source = std::error::Error::source(&error).expect("the cause is the source");
        assert!(
            source
                .downcast_ref::<coral_workspace::Error>()
                .is_some_and(|cause| cause.database_error().is_some()),
            "{source}"
        );
    }

    /// A connection that stays open does not hold the process up: the join
    /// gives up at the grace period and aborts the listener task.
    #[tokio::test(start_paused = true)]
    async fn shutdown_aborts_a_listener_task_still_open_after_the_grace_period() {
        // Dropped when the task ends, however it ends.
        let (held, released) = tokio::sync::oneshot::channel::<()>();
        let task = tokio::spawn(async move {
            let _held = held;
            std::future::pending::<Result<(), tonic::transport::Error>>().await
        });
        let started = tokio::time::Instant::now();

        let joined = join_listeners(task).await;

        assert!(
            matches!(joined, Err(AppError::ShutdownTimedOut)),
            "{joined:?}"
        );
        assert_eq!(started.elapsed(), SHUTDOWN_GRACE_PERIOD);
        released
            .await
            .expect_err("the aborted task dropped what it held");
    }

    #[tokio::test]
    async fn the_search_model_is_not_looked_for_while_the_setting_is_off() {
        // With the setting on, the delivery would request the real model
        // from GitHub. `docker/image-test.sh` boots the image with the
        // setting on, and the delivery's own tests use a local server.
        for search in ["", "[search]\nsemantic_search_enabled = false\n"] {
            let temp = TempDir::new().unwrap();
            std::fs::write(
                temp.path().join("config.toml"),
                format!("[listeners.web]\npublic_url = 'http://127.0.0.1:5173'\n{search}"),
            )
            .unwrap();
            let server = ServerBuilder::new()
                .with_config_dir(temp.path())
                .start()
                .await
                .unwrap();

            assert!(server.search_model().is_none(), "{search:?}");

            server.shutdown_inner().await.unwrap();
        }
    }
}
