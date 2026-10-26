//! Builds and runs the Coral gRPC server.

use std::net::{Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;

use tokio::net::TcpListener;
use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::service::Routes;
use tonic::transport::Server;
use zeroize::Zeroizing;

use super::env::AppEnvironment;
use super::error::AppError;
use super::health::EngineReadiness;
use super::server_config::{Launcher, LoadedServerConfig, ServeSettings, SessionAuthSettings};
use crate::auth::CoralAuthorizationServer;
use crate::state::DatabaseConfig;
use crate::state::{AppStateLayout, InitializedState, StateLock, initialize_state};
use crate::telemetry::TelemetryConfig;
use coral_db::{Database, DatabaseOptions};

/// HTTP/2 `SETTINGS_MAX_HEADER_LIST_SIZE` for the private listener.
const HTTP2_MAX_HEADER_LIST_SIZE: u32 = 128 * 1024;

/// How often bootstrap sweeps `provider_device_jobs` for leases whose worker
/// has gone away. The provider sets how often a flow polls it, anywhere from
/// one second to an hour, so a lost flow resumes up to this long after its
/// lease ends on top of that poll interval; the person is already waiting on
/// a flow that lasts minutes. A one-second sweep cost a transaction per second
/// per replica against a table that is empty whenever no flow is mid-flight.
const DEVICE_RECOVERY_INTERVAL: std::time::Duration = std::time::Duration::from_secs(15);

/// A sweep schedule: a fixed cadence anchored at startup, so a slow pass does
/// not push every later sweep back, and ticks a slow pass ran through are
/// skipped rather than replayed back to back. The first tick is at once.
fn fixed_cadence(period: std::time::Duration) -> tokio::time::Interval {
    let mut sweeps = tokio::time::interval(period);
    sweeps.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    sweeps
}

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

    fn resolved_mode(&self, config: &LoadedServerConfig) -> Result<ServerMode, AppError> {
        match &self.mode {
            ServerModeSelection::Explicit(mode) => Ok(mode.clone()),
            ServerModeSelection::ConfiguredStandaloneGrpc => {
                let bind = config.grpc_settings()?.bind;
                Ok(ServerMode::StandaloneGrpc { bind })
            }
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
///
/// This is not [`Clone`]: session authentication hands the builder resolved
/// signing-key and provider material, and one instance's authorization server
/// is built from it exactly once.
#[derive(Default)]
pub struct ServerBuilder {
    config: ServerConfig,
    outbound_defaults: coral_outbound::DestinationPolicy,
    launcher: Launcher,
    session_auth: Option<Box<SessionAuthSettings>>,
}

/// A server builder whose complete persistent state has passed the startup
/// fence and completed fresh initialization.
///
/// This temporal type lets process composition read companion settings and
/// install their session policy without re-running or reordering state-format
/// checks. Call [`Self::start`] to consume it and start gRPC.
pub struct PreparedServerBuilder {
    builder: ServerBuilder,
    state: InitializedState,
    deployment_config: LoadedServerConfig,
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
            session_auth: None,
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

    #[must_use]
    /// Configures session authentication for this server.
    ///
    /// Startup prepares the authorization server once the app database has
    /// been opened and migrated, and installs private authentication against
    /// the same current authority. Taking the authorization server from the
    /// returned [`RunningServer`] and running its transport stays the caller's
    /// job. Only gRPC health is unauthenticated in an authenticated deployment.
    pub fn with_session_auth(mut self, session_auth: SessionAuthSettings) -> Self {
        self.session_auth = Some(Box::new(session_auth));
        self
    }

    /// Passes the complete persistent state through its one initialization
    /// Module before any caller can read companion configuration or start a
    /// listener.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when the state root or configured database is
    /// retired or malformed, or when fresh-state initialization fails.
    pub async fn prepare(self) -> Result<PreparedServerBuilder, AppError> {
        let environment = AppEnvironment::discover();
        let layout = environment.app_state_layout(self.config.config_dir.clone())?;
        let deployment_config = LoadedServerConfig::load(&layout)?;
        let state = initialize_state(
            layout,
            deployment_config.raw(),
            resolve_database_config,
            || deployment_config.validate(),
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
    /// Process composition that needs companion settings should call
    /// [`Self::prepare`] and use the returned [`PreparedServerBuilder`].
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
    /// State preparation has already completed, so this read cannot bypass or
    /// repeat any part of the state-format sequence.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when companion configuration is invalid.
    pub fn serve_settings(&self) -> Result<ServeSettings, AppError> {
        self.deployment_config
            .serve_settings(self.builder.outbound_defaults, &self.builder.launcher)
    }

    /// Supplies composition defaults without changing the operator snapshot.
    #[must_use]
    pub fn with_outbound_defaults(mut self, defaults: coral_outbound::DestinationPolicy) -> Self {
        self.builder.outbound_defaults = defaults;
        self
    }

    /// Installs the session policy resolved from this prepared state.
    #[must_use]
    pub fn with_session_auth(mut self, session_auth: SessionAuthSettings) -> Self {
        self.builder.session_auth = Some(Box::new(session_auth));
        self
    }

    /// Starts the prepared Coral gRPC server on TCP.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] if configuration, credential initialization, or
    /// gRPC startup fails.
    pub async fn start(self) -> Result<RunningServer, AppError> {
        let Self {
            builder,
            state,
            deployment_config,
        } = self;
        let session_auth = match builder.session_auth {
            Some(auth) => Some(auth),
            None => deployment_config
                .serve_settings(builder.outbound_defaults, &builder.launcher)?
                .take_session_auth()
                .map(Box::new),
        };
        let policy = deployment_config.outbound_policy(builder.outbound_defaults)?;
        let runtime_limits = deployment_config.runtime_limits()?;
        let config = builder.config;
        let vault = open_credential_vault(&deployment_config, state.credential_key_path())?;
        let (coral_db, state_lock) = state.into_parts();
        let mode = config.resolved_mode(&deployment_config)?;
        if session_auth.is_none() && !super::is_loopback_ip(mode.bind_addr().ip()) {
            return Err(AppError::FailedPrecondition(
                "auth-disabled private gRPC bind must be loopback".to_owned(),
            ));
        }
        let workspace_authentication = session_auth
            .as_ref()
            .map(|auth| auth.workspace_authentication(coral_db.clone()));
        let private_authentication = session_auth
            .as_ref()
            .map(|auth| auth.principal_authentication(coral_db.clone()));
        if session_auth.is_none() {
            coral_workspace::OrganizationModule::new(coral_db.clone())
                .provision_login(&coral_workspace::ChangeContext::new(
                    coral_workspace::local_organization_owner(),
                    0,
                    coral_workspace::TraceId::current_or_new(),
                ))
                .await
                .map_err(|_error| {
                    AppError::FailedPrecondition("local authority initialization failed".to_owned())
                })?;
        }
        // Coral publishes its Client ID Metadata Document at Reef's public HTTPS
        // origin, which is what upstream authorization servers accept as client_id.
        let client_id_metadata_document = session_auth
            .as_ref()
            .and_then(|auth| auth.web_public_url())
            .map(|origin| format!("{origin}/.well-known/oauth-client"));
        let client_bootstrap_policy = deployment_config.mcp_oauth_client_bootstrap()?;
        // One readiness for every listener, so shutdown takes them all out of
        // rotation at once.
        let readiness = EngineReadiness::from_database(Arc::clone(&coral_db));
        let authorization_server = session_auth
            .map(|session_auth| {
                build_authorization_server(*session_auth, &coral_db, policy, readiness.clone())
            })
            .transpose()?;
        let reconcile_interval = deployment_config.search_reconcile_interval()?;
        // This start's Coral Instance, recorded on the Runs it claims. The
        // first start in a process also names it in exported telemetry.
        let instance = coral_run::CoralInstanceId::generate();
        init_server_telemetry(deployment_config.raw(), &instance.to_string())?;
        tracing::info!(coral_instance_id = %instance, "Coral Instance starting");
        let mut server = start_server(
            Arc::clone(&coral_db),
            mode,
            private_authentication,
            Some(vault),
            client_id_metadata_document,
            client_bootstrap_policy,
            policy,
            runtime_limits,
            reconcile_interval,
            readiness,
            state_lock,
            instance,
        )
        .await?;
        server.authorization_server = authorization_server;
        server.workspace_authentication = workspace_authentication;
        Ok(server)
    }
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

fn init_server_telemetry(raw: &str, instance_id: &str) -> Result<(), AppError> {
    let config = TelemetryConfig::from_toml(raw)?;
    crate::telemetry::init_tracing(&config, instance_id)
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
    deployment_config: &LoadedServerConfig,
    key_path: &Path,
) -> Result<coral_credentials::CredentialVault, AppError> {
    let Some(key_env) = deployment_config.credential_key_env()? else {
        return coral_credentials::CredentialVault::open(key_path).map_err(|_error| {
            AppError::FailedPrecondition("credential storage initialization failed".into())
        });
    };
    // Only a confirmed absence lets the secret stand in: an entry of any kind
    // is the conflict, and a path that cannot be inspected is not known to be
    // empty.
    match std::fs::symlink_metadata(key_path) {
        Ok(_entry) => {
            return Err(AppError::FailedPrecondition(format!(
                "credentials.key_env is configured but a key file exists at '{}'; encode that file's key into `{key_env}` and remove the file, or remove credentials.key_env",
                key_path.display()
            )));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(AppError::FailedPrecondition(format!(
                "credentials.key_env is configured but the key file path '{}' cannot be inspected: {error}",
                key_path.display()
            )));
        }
    }
    let encoded = Zeroizing::new(AppEnvironment::required_env_var(
        "credentials.key_env",
        &key_env,
    )?);
    coral_credentials::CredentialVault::from_encoded_key(&encoded).map_err(|_error| {
        AppError::FailedPrecondition(format!(
            "environment variable `{key_env}` must contain the standard base64 encoding of a {}-byte credential key",
            coral_credentials::CredentialVault::KEY_LEN
        ))
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

/// Running Coral server.
///
/// Call [`RunningServer::shutdown`] for deterministic teardown. Dropping this
/// handle sends shutdown to the background task as a best-effort fallback, but
/// does not wait for the task to finish.
pub struct RunningServer {
    providers: coral_provider::ProviderModule,
    accounts: Option<coral_provider::account::AccountModule>,
    runs: coral_run::RunModule,
    readiness: EngineReadiness,
    run_recovery: JoinHandle<()>,
    device_recovery: Option<JoinHandle<()>>,
    search_index: coral_search::SearchIndex,
    search_reconcile: JoinHandle<()>,
    mcp_origin: Arc<std::sync::OnceLock<String>>,
    endpoint_uri: String,
    local_addr: SocketAddr,
    database: Arc<Database>,
    authorization_server: Option<Box<CoralAuthorizationServer>>,
    workspace_authentication: Option<crate::WorkspaceAuthentication>,
    shutdown_tx: Mutex<Option<oneshot::Sender<()>>>,
    task_finished: watch::Receiver<bool>,
    task: Mutex<Option<JoinHandle<Result<(), tonic::transport::Error>>>>,
    /// Keeps other runtimes off this server's state root. Declared last so
    /// that it is released after every other field.
    _state_lock: StateLock,
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
        Arc::clone(&self.database)
    }

    /// Takes the authorization server prepared from this server's session
    /// authentication, when it was configured with any.
    ///
    /// Preparing it during app startup is what lets it share the one migrated
    /// state database. Whether and how its transport runs is the caller's
    /// decision, so it transfers out — and only the first caller receives it.
    #[must_use]
    pub fn take_authorization_server(&mut self) -> Option<CoralAuthorizationServer> {
        self.authorization_server.take().map(|server| *server)
    }

    /// Takes public Workspace authentication composed over this server's database.
    pub fn take_workspace_authentication(&mut self) -> Option<crate::WorkspaceAuthentication> {
        self.workspace_authentication.take()
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

    /// Shuts the server down and waits for the background task to finish.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] if the server task fails while shutting down.
    pub async fn shutdown(self) -> Result<(), AppError> {
        self.shutdown_inner().await
    }

    /// Reports not ready, stops the expired-lease sweep and drains the Runs
    /// ([`coral_run::RunModule::shutdown`]) while every listener still
    /// serves; stop the listeners after it.
    pub async fn drain_runs(&self) {
        self.readiness.stop_serving();
        self.run_recovery.abort();
        self.runs.shutdown().await;
    }

    async fn shutdown_inner(&self) -> Result<(), AppError> {
        self.drain_runs().await;
        if let Some(worker) = &self.device_recovery {
            worker.abort();
        }
        self.search_reconcile.abort();
        if let Some(accounts) = &self.accounts
            && let Err(error) = accounts.stop_device_authorizations().await
        {
            // Workers are already stopped; unreleased leases expire on their own.
            tracing::warn!(%error, "device authorization lease release failed");
        }
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

        let task = self.task.lock().expect("task mutex poisoned").take();
        match task {
            Some(task) => match task.await {
                Ok(Ok(())) => Ok(()),
                Ok(Err(error)) => Err(AppError::from(error)),
                Err(error) => Err(AppError::from(error)),
            },
            None => Ok(()),
        }
    }
}

impl RunningServer {
    /// Shares the bootstrap-owned Provider Module with companion runtime composition.
    #[must_use]
    pub fn providers(&self) -> coral_provider::ProviderModule {
        self.providers.clone()
    }

    /// Shares the bootstrap-owned credential subsystem with companion runtime composition.
    #[must_use]
    pub fn provider_accounts(&self) -> Option<coral_provider::account::AccountModule> {
        self.accounts.clone()
    }

    /// Shares the process's one search index with companion runtime
    /// composition, so every notification and reconcile pass in the process
    /// goes through the same in-process mutex.
    #[must_use]
    pub fn search_index(&self) -> coral_search::SearchIndex {
        self.search_index.clone()
    }

    /// The readiness every listener of this process reports: database
    /// reachability, and not serving once shutdown begins.
    #[must_use]
    pub fn readiness(&self) -> EngineReadiness {
        self.readiness.clone()
    }

    /// Shares the process's one Run module with companion runtime composition,
    /// so a cancel or decision from any surface reaches the Run Owner that
    /// another surface's Run registered.
    #[must_use]
    pub fn runs(&self) -> coral_run::RunModule {
        self.runs.clone()
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
        self.readiness.stop_serving();
        self.run_recovery.abort();
        if let Some(worker) = &self.device_recovery {
            worker.abort();
        }
        self.search_reconcile.abort();
        if let Some(accounts) = &self.accounts {
            accounts.abort_device_authorizations();
        }
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
}

#[expect(
    clippy::too_many_arguments,
    reason = "process composition passes each prepared dependency once"
)]
#[expect(
    clippy::too_many_lines,
    reason = "process composition builds each Module once, in dependency order"
)]
pub(crate) async fn start_server(
    database: Arc<Database>,
    mode: ServerMode,
    authentication: Option<coral_auth::PrincipalAuthentication>,
    vault: Option<coral_credentials::CredentialVault>,
    client_id_metadata_document: Option<String>,
    client_bootstrap_policy: coral_provider::account::ClientBootstrapPolicy,
    policy: coral_outbound::DestinationPolicy,
    runtime_limits: coral_run::RuntimeLimits,
    reconcile_interval: std::time::Duration,
    readiness: EngineReadiness,
    state_lock: StateLock,
    instance: coral_run::CoralInstanceId,
) -> Result<RunningServer, AppError> {
    let workspaces = coral_workspace::WorkspaceModule::new(database.clone());
    let tasks = coral_task::TaskModule::new(database.clone(), workspaces.clone());
    let builtin_catalog = coral_catalog::BuiltinCatalog::running()
        .map_err(|error| AppError::FailedPrecondition(error.to_string()))?;
    let providers = coral_provider::ProviderModule::new(
        policy,
        database.clone(),
        workspaces.clone(),
        builtin_catalog,
    );
    let accounts = vault.map(|vault| {
        let accounts = coral_provider::account::AccountModule::new(providers.clone(), vault)
            .with_client_bootstrap_policy(client_bootstrap_policy);
        match client_id_metadata_document {
            Some(document) => accounts.with_client_id_metadata_document(document),
            None => accounts,
        }
    });
    // State initialization already ran the search checks and stream. The
    // reconcile task's first pass runs in the background, so neither boot nor
    // a request waits for it.
    let search_index = coral_search::SearchIndex::new(Arc::clone(&database), reconcile_interval);
    let provider_workflow = coral_administration::ProviderWorkflow::new(
        database.clone(),
        providers.clone(),
        search_index.clone(),
    );
    // Every Workspace has a Built-in Provider with this process's catalog
    // before the process takes requests. A rollback moves Workspaces back the
    // same way.
    let booted_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|elapsed| i64::try_from(elapsed.as_nanos()).ok())
        .ok_or_else(|| AppError::FailedPrecondition("system clock is out of range".into()))?;
    provider_workflow
        .install_missing_builtins(booted_at)
        .await
        .map_err(|error| {
            AppError::FailedPrecondition(format!("Built-in Provider install failed: {error}"))
        })?;
    provider_workflow
        .update_builtin_catalogs(booted_at)
        .await
        .map_err(|error| {
            AppError::FailedPrecondition(format!("Built-in Provider update failed: {error}"))
        })?;
    // addProvider publishes through the same workflow as Reef.
    let mut runs = coral_run::RunModule::new(
        policy,
        database.clone(),
        workspaces.clone(),
        providers.clone(),
        runtime_limits,
        instance,
        coral_run::RunInbox::table(database.clone()),
    )
    .map_err(|error| AppError::FailedPrecondition(error.to_string()))?
    .with_provider_publisher(Arc::new(provider_workflow.clone()));
    if let Some(accounts) = &accounts {
        runs = runs.with_accounts(accounts.clone());
    }
    let saved_functions =
        coral_functions::SavedFunctionInspection::new(database.clone(), workspaces.clone());
    let saved_function_deletion =
        coral_functions::SavedFunctionDeletion::new(database.clone(), workspaces.clone());
    let mcp_origin = Arc::new(std::sync::OnceLock::new());
    let routes = coral_grpc::routes(
        coral_grpc::Dependencies {
            organization: coral_workspace::OrganizationModule::new(database.clone()),
            provider_readiness: coral_search::UniversalSearchModule::new(
                database.clone(),
                workspaces.clone(),
            ),
            policies: coral_policy::OperationPolicyModule::new(
                database.clone(),
                workspaces.clone(),
            ),
            access_lifecycle: coral_administration::AccessLifecycleModule::new(
                database.clone(),
                workspaces.clone(),
            ),
            provider_workflow,
            workspace_workflow: coral_administration::WorkspaceWorkflow::new(
                database.clone(),
                workspaces.clone(),
                providers.clone(),
                search_index.clone(),
            ),
            workspaces,
            providers: providers.clone(),
            accounts: accounts.clone(),
            tasks,
            saved_functions,
            saved_function_deletion,
            runs: runs.clone(),
        },
        coral_grpc::TransportContext {
            authentication,
            mcp_origin: mcp_origin.clone(),
            readiness: readiness.probe(),
        },
    );

    // The expired-lease sweep's first pass runs before any listener binds:
    // when it succeeds, a Run whose lease lapsed before boot has its owner
    // back before the first request. A pass that fails is logged, the
    // listeners bind anyway, and the next pass, a half lease later, reads
    // again.
    sweep_expired_leases(&runs).await;
    let listener = TcpListener::bind(mode.bind_addr()).await?;
    let local_addr = listener.local_addr()?;
    let endpoint_uri = format!("http://{local_addr}");
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let (task_finished_tx, task_finished) = watch::channel(false);

    let task = match mode {
        ServerMode::EphemeralGrpc | ServerMode::StandaloneGrpc { .. } => {
            start_grpc_server(listener, shutdown_rx, routes, task_finished_tx)
        }
    };

    let run_recovery = {
        let runs = runs.clone();
        tokio::spawn(async move {
            let mut sweeps = fixed_cadence(runs.sweep_interval());
            // The first tick is at once, and the boot pass above ran it.
            sweeps.tick().await;
            loop {
                sweeps.tick().await;
                sweep_expired_leases(&runs).await;
            }
        })
    };
    let device_recovery = accounts.clone().map(|accounts| {
        tokio::spawn(async move {
            let mut sweeps = fixed_cadence(DEVICE_RECOVERY_INTERVAL);
            loop {
                sweeps.tick().await;
                if let Ok(elapsed) =
                    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)
                    && let Ok(now) = i64::try_from(elapsed.as_nanos())
                    && let Err(error) = accounts.recover_device_authorizations(now).await
                {
                    tracing::warn!(%error, "device authorization recovery failed");
                }
            }
        })
    });
    let search_reconcile = search_index.spawn_reconcile_task();
    Ok(RunningServer {
        providers,
        readiness,
        run_recovery,
        device_recovery,
        search_index,
        search_reconcile,
        accounts,
        runs,
        mcp_origin,
        endpoint_uri,
        local_addr,
        database,
        authorization_server: None,
        workspace_authentication: None,
        shutdown_tx: Mutex::new(Some(shutdown_tx)),
        task_finished,
        task: Mutex::new(Some(task)),
        _state_lock: state_lock,
    })
}

/// One pass of the expired-lease sweep; a failed pass is logged, and the next
/// one reads again.
async fn sweep_expired_leases(runs: &coral_run::RunModule) {
    match runs.sweep_expired_leases().await {
        Ok(0) => {}
        Ok(recovered) => tracing::info!(recovered, "expired-lease sweep recovered Runs"),
        Err(error) => tracing::warn!(%error, "expired-lease sweep failed; the next pass retries"),
    }
}

fn start_grpc_server(
    listener: TcpListener,
    shutdown_rx: oneshot::Receiver<()>,
    routes: Routes,
    task_finished: watch::Sender<bool>,
) -> JoinHandle<Result<(), tonic::transport::Error>> {
    tokio::spawn(async move {
        let result = Server::builder()
            .http2_max_header_list_size(HTTP2_MAX_HEADER_LIST_SIZE)
            .add_routes(routes)
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                drop(shutdown_rx.await);
            })
            .await;
        task_finished.send_replace(true);
        result
    })
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tempfile::TempDir;

    use super::{DEVICE_RECOVERY_INTERVAL, ServerBuilder, fixed_cadence};

    #[tokio::test(start_paused = true)]
    async fn recovery_sweeps_keep_a_fixed_cadence() {
        let start = tokio::time::Instant::now();
        let mut sweeps = fixed_cadence(DEVICE_RECOVERY_INTERVAL);
        sweeps.tick().await;
        assert_eq!(
            start.elapsed(),
            Duration::ZERO,
            "the first sweep runs at once"
        );
        sweeps.tick().await;
        assert_eq!(start.elapsed(), DEVICE_RECOVERY_INTERVAL);

        // A pass that runs through two boundaries gets one sweep as soon as it
        // ends, then the schedule resumes on the original grid rather than a
        // full interval after the late pass.
        tokio::time::sleep(DEVICE_RECOVERY_INTERVAL * 2 + Duration::from_secs(1)).await;
        sweeps.tick().await;
        assert_eq!(
            start.elapsed(),
            DEVICE_RECOVERY_INTERVAL * 3 + Duration::from_secs(1)
        );
        sweeps.tick().await;
        assert_eq!(start.elapsed(), DEVICE_RECOVERY_INTERVAL * 4);
    }

    #[tokio::test]
    async fn the_search_reconcile_task_runs_beside_the_server_until_shutdown() {
        let temp = TempDir::new().unwrap();
        std::fs::write(
            temp.path().join("config.toml"),
            "[listeners.web]\npublic_url = 'http://127.0.0.1:5173'\n",
        )
        .unwrap();
        let server = ServerBuilder::new()
            .with_config_dir(temp.path())
            .start()
            .await
            .unwrap();
        assert!(
            !server.search_reconcile.is_finished(),
            "the task runs a pass each interval until it is stopped"
        );

        server.shutdown_inner().await.unwrap();

        tokio::time::timeout(Duration::from_secs(5), async {
            while !server.search_reconcile.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("shutdown aborts the reconcile task");
    }
}
