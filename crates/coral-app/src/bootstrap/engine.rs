//! [`Engine`]: one Coral Instance's domain modules and the background work
//! beside them.
//!
//! An `Engine` serves nothing. Process composition builds it from what
//! bootstrap resolved, then builds each listener over it, so every surface
//! reaches the same Run module, search index and Run Executor link.

use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;

use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

use super::error::AppError;
use super::health::EngineReadiness;
use super::periodic::{Passes, Schedule};
use super::search_model::{Delivery, SearchModel};
use super::server_config::{Launcher, LocalRunExecutorSettings, RunExecutorListener};
use crate::state::StateLock;
use coral_db::Database;
use coral_telemetry::safe;

/// How often bootstrap sweeps `provider_device_jobs` for leases whose worker
/// has gone away. The provider sets how often a flow polls it, anywhere from
/// one second to an hour, so a lost flow resumes up to this long after its
/// lease ends on top of that poll interval; the person is already waiting on
/// a flow that lasts minutes. A one-second sweep cost a transaction per second
/// per Coral Instance against a table that is empty whenever no flow is
/// mid-flight.
const DEVICE_RECOVERY_INTERVAL: std::time::Duration = std::time::Duration::from_secs(15);

/// How often the License recheck asks whether Coral may keep running. An
/// Evaluation License that expires stops Coral within this span (ADR 0038).
const LICENSE_RECHECK_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);

/// What the License recheck asks, and where it says that Coral must stop.
pub(crate) struct LicenseRecheck {
    pub(crate) edition: coral_edition::EditionCheck,
    pub(crate) lapse: watch::Sender<Option<coral_edition::LicenseRefusal>>,
}

/// What bootstrap resolved for one [`Engine`]: every dependency it does not
/// choose itself.
pub(crate) struct EngineParts {
    pub(crate) database: Arc<Database>,
    /// Keeps other runtimes off the state root for as long as the `Engine`
    /// lives.
    pub(crate) state_lock: StateLock,
    /// This start's Coral Instance, recorded on the Runs it claims.
    pub(crate) instance: coral_run::CoralInstanceId,
    /// The one readiness of every listener, so shutdown takes them all out
    /// of rotation at once.
    pub(crate) readiness: EngineReadiness,
    /// The instance's one clock. The modules this Engine gives it to, the
    /// boot writes and the periodic passes read it. The Run module and the
    /// Run Executor link keep their own until ADR 0027.
    pub(crate) clock: Arc<dyn coral_db::Clock>,
    pub(crate) policy: coral_outbound::DestinationPolicy,
    pub(crate) runtime_limits: coral_run::RuntimeLimits,
    /// Whether the deployment configures a Sub-agent, whose Tools the
    /// built-in catalog then holds.
    pub(crate) sub_agent: bool,
    /// Reef's origin, where a Run's Built-in Tools link a person.
    pub(crate) human_interface: coral_mcp::HumanInterfaceUrl,
    pub(crate) credentials: CredentialParts,
    pub(crate) search: SearchParts,
    pub(crate) run_executors: RunExecutorParts,
    /// The release check; none while `[updates] check` is off.
    pub(crate) releases: Option<coral_release::ReleaseCheck>,
    /// The deployment's Edition and the Enterprise Features the launching
    /// binary registers, for the private API to report.
    pub(crate) edition: coral_edition::EditionInspection,
    /// Usage data; none when the launch supplied no environment.
    pub(crate) usage: Option<UsageReporting>,
}

/// What the Provider Account module is built from.
pub(crate) struct CredentialParts {
    pub(crate) vault: coral_credentials::CredentialVault,
    pub(crate) secret_stores: coral_secret_store::SecretStores,
    /// Where Coral publishes its Client ID Metadata Document, when it has a
    /// public HTTPS origin to publish it at.
    pub(crate) client_id_metadata_document: Option<String>,
    pub(crate) client_bootstrap_policy: coral_provider::account::ClientBootstrapPolicy,
}

impl CredentialParts {
    fn accounts(
        self,
        providers: coral_provider::ProviderModule,
        clock: Arc<dyn coral_db::Clock>,
    ) -> (
        coral_provider::account::AccountModule,
        coral_secret_store::SecretStores,
    ) {
        let accounts = coral_provider::account::AccountModule::new(providers, self.vault)
            .with_clock(clock)
            .with_secret_stores(self.secret_stores.clone())
            .with_client_bootstrap_policy(self.client_bootstrap_policy);
        let accounts = match self.client_id_metadata_document {
            Some(document) => accounts.with_client_id_metadata_document(document),
            None => accounts,
        };
        (accounts, self.secret_stores)
    }
}

/// Universal Search's settings.
pub(crate) struct SearchParts {
    pub(crate) reconcile_interval: std::time::Duration,
    /// How the search model's files arrive; none while the vector side is
    /// off, and then nothing looks for them.
    pub(crate) model: Option<Delivery>,
    /// How many threads the Embedder runs, with one model session for each.
    pub(crate) embedder_cores: std::num::NonZeroUsize,
}

impl SearchParts {
    /// The search index, the one search module of the process, and the slot
    /// the Embedder's model loads into while the vector side is on. Both
    /// read `clock`, the instance's one clock.
    fn modules(
        &self,
        database: &Arc<Database>,
        workspaces: &coral_workspace::WorkspaceModule,
        clock: &Arc<dyn coral_db::Clock>,
    ) -> (
        coral_search::SearchIndex,
        coral_search::UniversalSearchModule,
        Option<watch::Sender<coral_search::ModelLoad>>,
    ) {
        // State initialization already ran the search checks and stream. The
        // reconcile task's first pass runs in the background, so neither boot nor
        // a request waits for it.
        let index = coral_search::SearchIndex::new(Arc::clone(database), self.reconcile_interval)
            .with_clock(Arc::clone(clock));
        // Runs, gRPC and MCP share this module. It embeds queries with
        // the Embedder's model, so a query waits for at most the one document
        // the Embedder runs.
        let module = coral_search::UniversalSearchModule::new(database.clone(), workspaces.clone())
            .with_clock(Arc::clone(clock));
        if self.model.is_none() {
            return (index, module, None);
        }
        // Every clone of the index is made after this, so each write wakes the
        // Embedder; the model fills the slot once the delivery finds the files.
        let (slot, model) = watch::channel(coral_search::ModelLoad::Pending);
        (
            index.with_embedder(model.clone(), self.embedder_cores),
            module.with_model(model),
            Some(slot),
        )
    }
}

/// Where Run Executors dial, and the ones Coral starts itself.
pub(crate) struct RunExecutorParts {
    pub(crate) listener: RunExecutorListener,
    /// The local children; none with them turned off.
    pub(crate) local_children: Option<LocalChildren>,
}

/// What bootstrap resolved to start local Run Executors: the `[runtime]`
/// settings, this process's own binary and the children's environment.
pub(crate) struct LocalChildren {
    pub(crate) settings: LocalRunExecutorSettings,
    pub(crate) program: std::path::PathBuf,
    pub(crate) environment: Vec<(std::ffi::OsString, std::ffi::OsString)>,
}

/// The usage data module and the outbound client it sends with.
pub(crate) struct UsageReporting {
    data: coral_usage::UsageData,
    client: coral_outbound::Client,
}

impl UsageReporting {
    /// The usage data module, or none when the launch supplied no
    /// environment: then nothing could honor `DO_NOT_TRACK`, so nothing
    /// sends.
    pub(crate) fn new(
        database: &Arc<Database>,
        launcher: &Launcher,
        signs_in: bool,
        policy: coral_outbound::DestinationPolicy,
    ) -> Result<Option<Self>, AppError> {
        let Some(environment) = &launcher.usage_environment else {
            return Ok(None);
        };
        let host = coral_usage::Host {
            version: launcher
                .service_version
                .unwrap_or(env!("CARGO_PKG_VERSION"))
                .to_owned(),
            kind: if launcher.desktop {
                coral_usage::HostKind::Desktop
            } else {
                coral_usage::HostKind::Server
            },
            auth: if signs_in {
                coral_usage::HostAuth::Required
            } else {
                coral_usage::HostAuth::Disabled
            },
            database: database.system(),
        };
        Ok(Some(Self {
            data: coral_usage::UsageData::new(
                Arc::clone(database),
                host,
                coral_usage::Build::from_marker(option_env!("CORAL_RELEASE_BUILD")),
                environment,
            ),
            client: coral_outbound::Client::new(policy, coral_usage::REQUEST_TIMEOUT).map_err(
                |source| AppError::HttpClient {
                    purpose: "usage data",
                    source,
                },
            )?,
        }))
    }
}

/// The Run Executor listener, bound and not yet served, with the link it
/// serves. Whoever serves the private listeners serves it beside them.
pub(crate) struct RunExecutorLink {
    pub(crate) listener: TcpListener,
    pub(crate) server: coral_run_executor::LinkServer,
}

/// The Run Executors of one `Engine`: the link they dial, the local children,
/// and the compiler client over both.
struct RunExecutors {
    link: RunExecutorLink,
    local: Option<coral_run_executor::LocalRunExecutors>,
    /// The Run Executor id of each local child that exits.
    exited: mpsc::UnboundedReceiver<String>,
    compiler: Arc<dyn coral_run_executor::CompilerClient>,
}

impl RunExecutorParts {
    /// Binds the Run Executor listener and starts the local child supervisor.
    async fn start(
        self,
        database: &Arc<Database>,
        coral_instance_id: String,
        runtime_limits: coral_run::RuntimeLimits,
    ) -> Result<RunExecutors, AppError> {
        let credentials = coral_run_executor::Credentials::new(self.listener.tokens);
        let listener = TcpListener::bind(self.listener.bind).await?;
        let server = coral_run_executor::LinkServer::new(
            credentials.clone(),
            database.clone(),
            coral_instance_id,
        );
        let (exits, exited) = mpsc::unbounded_channel();
        let local = match self.local_children {
            Some(children) => {
                // A local child dials this host: loopback, unless the listener
                // is bound to one address.
                let mut address = listener.local_addr()?;
                if address.ip().is_unspecified() {
                    address.set_ip(match address {
                        SocketAddr::V4(_) => Ipv4Addr::LOCALHOST.into(),
                        SocketAddr::V6(_) => Ipv6Addr::LOCALHOST.into(),
                    });
                }
                let command = coral_run_executor::ChildCommand {
                    program: children.program,
                    coral_address: format!("http://{address}"),
                    max_runs_held: children.settings.max_runs_held,
                    compiler_heap_bytes: runtime_limits.isolate().compiler_heap_bytes(),
                    environment: children.environment,
                };
                Some(coral_run_executor::LocalRunExecutors::start(
                    command,
                    children.settings.limit,
                    credentials,
                    server.streams(),
                    Some(exits),
                ))
            }
            None => None,
        };
        // Program Admission and Private Tool save share one compiler client:
        // the Run Executors' pull streams, relayed through the database to any
        // Coral Instance with room when none has room.
        let compiler: Arc<dyn coral_run_executor::CompilerClient> =
            Arc::new(server.compiler(local.clone()));
        Ok(RunExecutors {
            link: RunExecutorLink { listener, server },
            local,
            exited,
            compiler,
        })
    }
}

/// The domain modules of one Coral Instance, which every surface shares.
struct Modules {
    database: Arc<Database>,
    workspaces: coral_workspace::WorkspaceModule,
    providers: coral_provider::ProviderModule,
    accounts: coral_provider::account::AccountModule,
    provider_workflow: coral_administration::ProviderWorkflow,
    runs: coral_run::RunModule,
    compiler: Arc<dyn coral_run_executor::CompilerClient>,
    runtime_limits: coral_run::RuntimeLimits,
    search: coral_search::UniversalSearchModule,
    search_index: coral_search::SearchIndex,
    secret_stores: coral_secret_store::SecretStores,
    releases: Option<coral_release::ReleaseCheck>,
    edition: coral_edition::EditionInspection,
    usage: Option<coral_usage::UsageData>,
    readiness: EngineReadiness,
    /// The instance's one clock, which the periodic passes and the modules
    /// each surface builds read.
    clock: Arc<dyn coral_db::Clock>,
    /// The local child supervisor; none with the children turned off.
    local_run_executors: Option<coral_run_executor::LocalRunExecutors>,
}

/// An `Engine` whose modules are built and whose boot writes and first Run
/// sweep are done, with nothing running beside them yet.
///
/// [`Self::run`] starts the background work. Whoever binds the private
/// listeners calls it after the bind, so a start that fails on their address
/// has run no pass and sent no request.
pub(crate) struct BootedEngine {
    modules: Modules,
    /// What the usage report pass sends with.
    usage_reporting: Option<UsageReporting>,
    search_model: Option<Delivery>,
    /// Where the search model loads for the Embedder.
    embedder_slot: Option<watch::Sender<coral_search::ModelLoad>>,
    /// How many sessions the search model loads into.
    embedder_cores: std::num::NonZeroUsize,
    /// The pull streams the take loop takes Runs for.
    pull_streams: coral_run_executor::PullStreams,
    /// The Run Executor id of each local child that exits.
    exited_children: mpsc::UnboundedReceiver<String>,
    state_lock: StateLock,
}

impl BootedEngine {
    /// The Provider module, which the authorization server reads sign-in
    /// Providers from.
    pub(crate) fn providers(&self) -> coral_provider::ProviderModule {
        self.modules.providers.clone()
    }

    /// Starts the periodic passes, the search reconcile task and the search
    /// model's delivery.
    pub(crate) fn run(self, license: LicenseRecheck) -> Engine {
        let Self {
            modules,
            usage_reporting,
            search_model,
            embedder_slot,
            embedder_cores,
            pull_streams,
            exited_children,
            state_lock,
        } = self;
        let take_loop = spawn_take_loop(&modules, pull_streams);
        let child_exits = spawn_child_exits(&modules.runs, exited_children);
        let mut passes = Passes::default();
        start_passes(
            &mut passes,
            &modules.runs,
            &modules.accounts,
            modules.releases.clone(),
            usage_reporting,
            &modules.clock,
        );
        start_license_recheck(&mut passes, license, &modules.clock);
        let search_reconcile = modules.search_index.spawn_reconcile_task();
        let (search_model, search_model_delivery) = search_model.map(SearchModel::start).unzip();
        let search_model_load = search_model
            .as_ref()
            .zip(embedder_slot)
            .map(|(files, slot)| files.load_into(slot, embedder_cores));
        Engine {
            modules,
            passes,
            take_loop,
            child_exits,
            search_reconcile,
            search_model,
            search_model_delivery,
            search_model_load,
            _state_lock: state_lock,
        }
    }
}

/// The `Engine` of one Coral Instance: its domain modules and the background
/// work beside them.
///
/// Call [`Self::drain_runs`] and then [`Self::stop`] for deterministic
/// teardown. Dropping it stops its background work as a best-effort
/// fallback, and ends no Run.
pub(crate) struct Engine {
    modules: Modules,
    /// Every periodic pass of this Coral Instance.
    passes: Passes,
    /// Takes waiting Runs for this Coral Instance's pull streams.
    take_loop: JoinHandle<()>,
    /// Releases the Runs of each local child that exits.
    child_exits: JoinHandle<()>,
    search_reconcile: JoinHandle<()>,
    search_model: Option<SearchModel>,
    search_model_delivery: Option<JoinHandle<()>>,
    /// Loads the model for the Embedder; none while the vector side is off.
    search_model_load: Option<JoinHandle<()>>,
    /// Keeps other runtimes off this state root. Declared last so that it is
    /// released after every other field.
    _state_lock: StateLock,
}

impl Engine {
    /// Builds the domain modules, brings every Workspace's Coral Providers
    /// to this process's catalog, binds the Run Executor listener, starts
    /// the local Run Executors and runs the first pass of the Run sweeper.
    ///
    /// The Run Executor listener comes back bound and unserved: an `Engine`
    /// serves nothing.
    #[expect(
        clippy::too_many_lines,
        reason = "boot builds each module once, in the order they depend on one another"
    )]
    pub(crate) async fn boot(
        parts: EngineParts,
    ) -> Result<(BootedEngine, RunExecutorLink), AppError> {
        let EngineParts {
            database,
            state_lock,
            instance,
            readiness,
            clock,
            policy,
            runtime_limits,
            sub_agent,
            human_interface,
            credentials,
            search,
            run_executors,
            releases,
            edition,
            usage,
        } = parts;
        let workspaces = coral_workspace::WorkspaceModule::new(database.clone());
        let builtin_catalog = if sub_agent {
            coral_catalog::BuiltinCatalog::with_sub_agent()
        } else {
            coral_catalog::BuiltinCatalog::running()
        }
        .map_err(|error| AppError::engine_boot(error.to_string()))?;
        let providers = coral_provider::ProviderModule::new(
            policy,
            database.clone(),
            workspaces.clone(),
            builtin_catalog,
            coral_catalog::SchemaGraphCache::new(
                coral_catalog::SchemaGraphCache::DEFAULT_BUDGET_BYTES,
            ),
        )
        .with_clock(clock.clone());
        let (accounts, secret_stores) = credentials.accounts(providers.clone(), clock.clone());
        let (search_index, search_module, embedder_slot) =
            search.modules(&database, &workspaces, &clock);
        let provider_workflow =
            coral_administration::ProviderWorkflow::new(providers.clone(), search_index.clone());
        install_coral_providers(&provider_workflow, clock.now()).await?;
        let RunExecutors {
            link,
            local: local_run_executors,
            exited: exited_children,
            compiler,
        } = run_executors
            .start(&database, instance.to_string(), runtime_limits)
            .await?;
        // updateProvider renames through the same workflow as Reef. Every
        // Run runs on this module, whichever surface started it: the take
        // loop and the Run stream server hold it, so it links people to Reef
        // and saves Private Tools for all of them.
        let runs = coral_run::RunModule::with_compiler_client(
            policy,
            database.clone(),
            workspaces.clone(),
            providers.clone(),
            runtime_limits,
            compiler.clone(),
            instance,
        )
        .map_err(|error| AppError::engine_boot(error.to_string()))?
        .with_provider_renamer(Arc::new(provider_workflow.clone()))
        .with_search(search_module.clone())
        .with_accounts(accounts.clone())
        .with_human_interface(human_interface)
        .with_private_tools(private_tools(
            &database,
            &workspaces,
            &compiler,
            runtime_limits,
            &clock,
            &search_index,
        ))
        .with_pull_streams(link.server.streams());
        let pull_streams = link.server.streams();
        let link = RunExecutorLink {
            server: link.server.with_run_streams(runs.run_stream_server()),
            listener: link.listener,
        };

        Box::pin(sweep_at_boot(&runs)).await;
        let booted = BootedEngine {
            modules: Modules {
                database,
                workspaces,
                providers,
                accounts,
                provider_workflow,
                runs,
                compiler,
                runtime_limits,
                search: search_module,
                search_index,
                secret_stores,
                releases,
                edition,
                usage: usage.as_ref().map(|usage| usage.data.clone()),
                readiness,
                clock,
                local_run_executors,
            },
            usage_reporting: usage,
            search_model: search.model,
            embedder_slot,
            embedder_cores: search.embedder_cores,
            pull_streams,
            exited_children,
            state_lock,
        };
        Ok((booted, link))
    }

    /// This start's Coral Instance.
    pub(crate) fn instance(&self) -> &coral_run::CoralInstanceId {
        self.modules.runs.instance()
    }

    /// The initialized persistence foundation.
    pub(crate) fn database(&self) -> Arc<Database> {
        Arc::clone(&self.modules.database)
    }

    /// The credential subsystem, which the Provider OAuth callback completes
    /// an Authorization in.
    pub(crate) fn accounts(&self) -> coral_provider::account::AccountModule {
        self.modules.accounts.clone()
    }

    /// The readiness every listener of this process reports: database
    /// reachability, and not serving once shutdown begins.
    pub(crate) fn readiness(&self) -> EngineReadiness {
        self.modules.readiness.clone()
    }

    /// The state of the search model's files. `None` when `[search]
    /// semantic_search_enabled` is off: then nothing looks for the files
    /// and nothing downloads them.
    pub(crate) fn search_model(&self) -> Option<SearchModel> {
        self.search_model.clone()
    }

    /// The domain modules the private gRPC API serves. Its Run module is the
    /// one MCP's Runs start in, so a cancel or decision from either surface
    /// reaches the Run Owner that the other's Run registered.
    pub(crate) fn grpc_dependencies(&self) -> coral_grpc::Dependencies {
        let database = &self.modules.database;
        let workspaces = &self.modules.workspaces;
        let clock = &self.modules.clock;
        // A Shared Tool Publish compiles through the same warm compiler.
        let shared_tools = coral_tools::SharedToolModule::with_compiler_client(
            database.clone(),
            workspaces.clone(),
            self.modules.compiler.clone(),
            self.modules.runtime_limits.compilation_timeout(),
        )
        .with_clock(Arc::clone(clock));
        coral_grpc::Dependencies {
            organization: coral_workspace::OrganizationModule::new(database.clone()),
            // A publish and a removal tell the search index after they commit.
            shared_tool_workflow: coral_administration::SharedToolWorkflow::new(
                database.clone(),
                shared_tools.clone(),
                self.modules.search.clone(),
                self.modules.search_index.clone(),
            ),
            provider_readiness: self.modules.search.clone(),
            policies: coral_policy::ToolPolicyModule::new(database.clone(), workspaces.clone()),
            access_lifecycle: coral_administration::AccessLifecycleModule::new(
                database.clone(),
                workspaces.clone(),
            ),
            provider_workflow: self.modules.provider_workflow.clone(),
            workspace_workflow: coral_administration::WorkspaceWorkflow::new(
                database.clone(),
                workspaces.clone(),
                self.modules.providers.clone(),
                self.modules.search_index.clone(),
            ),
            workspaces: workspaces.clone(),
            providers: self.modules.providers.clone(),
            accounts: Some(self.modules.accounts.clone()),
            tasks: coral_task::TaskModule::new(database.clone(), workspaces.clone())
                .with_clock(Arc::clone(clock)),
            private_tools: coral_tools::PrivateToolInspection::new(
                database.clone(),
                workspaces.clone(),
            )
            .with_clock(Arc::clone(clock)),
            // A delete removes the Private Tool's exact rule and tells the search index.
            private_tool_workflow: coral_administration::PrivateToolWorkflow::new(
                database.clone(),
                coral_tools::PrivateToolDeletion::new(workspaces.clone())
                    .with_clock(Arc::clone(clock)),
                self.modules.search_index.clone(),
            ),
            shared_tools,
            runs: self.modules.runs.clone(),
            releases: self.modules.releases.clone(),
            edition: self.modules.edition.clone(),
            usage: self.modules.usage.clone(),
            secret_stores: coral_administration::SecretStoreAdministration::new(
                coral_workspace::OrganizationModule::new(database.clone()),
                workspaces.clone(),
                self.modules.secret_stores.clone(),
            ),
        }
    }

    /// The domain modules MCP serves, without selecting authentication or
    /// Principal. They are this `Engine`'s, so the process has one search
    /// module, one index, one set of Run Owners and one warm compiler that
    /// every surface reaches.
    pub(crate) fn mcp_modules(
        &self,
        human_interface: coral_mcp::HumanInterfaceUrl,
    ) -> coral_mcp::McpModules {
        let workspaces = self.modules.workspaces.clone();
        let clock = &self.modules.clock;
        let tasks =
            coral_task::TaskModule::new(Arc::clone(&self.modules.database), workspaces.clone())
                .with_clock(Arc::clone(clock));
        let runs = self.modules.runs.clone();
        let commands =
            coral_task::TaskCommands::new(tasks.clone(), self.modules.search.clone(), runs.clone());
        coral_mcp::McpModules::new(
            workspaces,
            tasks,
            commands,
            runs,
            self.modules.search.clone(),
            human_interface,
        )
    }

    /// Reports not ready, stops the take loop, and drains the Runs
    /// ([`coral_run::RunModule::shutdown`]). Every listener still serves
    /// meanwhile, but no new Run Executor stream opens; stop them after it.
    pub(crate) async fn drain_runs(&self) {
        self.modules.readiness.stop_serving();
        // A draining Coral Instance takes no Run and starts no child.
        self.take_loop.abort();
        self.modules.runs.shutdown().await;
    }

    /// Stops the background work, waits for the search index's tasks to end
    /// and releases the device authorization leases. Call it after
    /// [`Self::drain_runs`].
    pub(crate) async fn stop(&self) {
        self.stop_background();
        // An index write still running when the runtime shuts down panics
        // at its next timer, so the runtime must outlive every one.
        self.modules.search_index.stopped().await;
        if let Err(error) = self.modules.accounts.stop_device_authorizations().await {
            // Workers are already stopped; unreleased leases expire on their own.
            tracing::warn!(
                error.type = safe::kind(error.code()),
                "device authorization lease release failed"
            );
        }
    }

    /// Stops what runs beside the modules, without waiting for any of it.
    /// Each child's stdin closes, and nothing waits for the children.
    fn stop_background(&self) {
        if let Some(supervisor) = &self.modules.local_run_executors {
            supervisor.stop();
        }
        self.passes.stop();
        self.take_loop.abort();
        self.child_exits.abort();
        self.modules.search_index.stop();
        self.search_reconcile.abort();
        if let Some(delivery) = &self.search_model_delivery {
            delivery.abort();
        }
        if let Some(load) = &self.search_model_load {
            load.abort();
        }
    }

    /// The best-effort teardown of a dropped `Engine`: not ready, and its
    /// background work and device authorizations stopped. Idempotent.
    pub(crate) fn halt(&self) {
        self.modules.readiness.stop_serving();
        self.stop_background();
        self.modules.accounts.abort_device_authorizations();
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        self.halt();
    }
}

/// Every Workspace has a Built-in Provider with this process's catalog, and
/// a Private Provider, before the process takes requests. A rollback moves
/// Workspaces back the same way. `booted_at` dates every write: one reading
/// of the instance's clock.
async fn install_coral_providers(
    provider_workflow: &coral_administration::ProviderWorkflow,
    booted_at: coral_db::UnixNanos,
) -> Result<(), AppError> {
    provider_workflow
        .install_missing_coral_providers(booted_at)
        .await
        .map_err(|error| {
            AppError::engine_boot(format!("Coral's Provider install failed: {error}"))
        })?;
    provider_workflow
        .update_builtin_catalogs(booted_at)
        .await
        .map_err(|error| AppError::engine_boot(format!("Built-in Provider update failed: {error}")))
}

/// Schedules the periodic passes. A pass that takes the time reads `clock`,
/// the instance's one clock, when it runs.
fn start_passes(
    passes: &mut Passes,
    runs: &coral_run::RunModule,
    accounts: &coral_provider::account::AccountModule,
    releases: Option<coral_release::ReleaseCheck>,
    usage: Option<UsageReporting>,
    clock: &Arc<dyn coral_db::Clock>,
) {
    {
        let runs = runs.clone();
        let interval = coral_run::RUN_SWEEP_INTERVAL;
        // The boot pass was the first, so this one waits a cadence.
        let schedule = Schedule::every(interval).first_after(interval);
        passes.start("run_sweep", schedule, move || {
            let runs = runs.clone();
            async move { sweep_runs(runs.sweep_runs()).await }
        });
    }
    let accounts = accounts.clone();
    let schedule = Schedule::every(DEVICE_RECOVERY_INTERVAL);
    let recovery_clock = Arc::clone(clock);
    passes.start("device_authorization_recovery", schedule, move || {
        let accounts = accounts.clone();
        let now = recovery_clock.now();
        async move {
            accounts
                .recover_device_authorizations(now)
                .await
                .map_err(|error| error.code())?;
            Ok(())
        }
    });
    if let Some(releases) = releases {
        let schedule = Schedule::every(coral_release::CHECK_INTERVAL);
        passes.start("release_check", schedule, move || {
            let releases = releases.clone();
            async move {
                releases.check().await;
                Ok(())
            }
        });
    }
    if let Some(usage) = usage {
        let schedule = Schedule::every(coral_usage::CHECK_INTERVAL);
        let clock = Arc::clone(clock);
        passes.start("usage_report_check", schedule, move || {
            let (data, client) = (usage.data.clone(), usage.client.clone());
            let clock = Arc::clone(&clock);
            async move {
                // The collector answering late or not at all is no failure:
                // the reports wait for the next check.
                data.send_due(&client, coral_usage::COLLECTOR_URL, || clock.now())
                    .await
                    .map_err(|error| error.code())?;
                Ok(())
            }
        });
    }
}

/// Schedules the License recheck. When it finds that Coral must stop, it
/// logs why and says so on the lapse channel, which the launcher waits on.
/// The Edition does not change before Coral stops.
fn start_license_recheck(
    passes: &mut Passes,
    license: LicenseRecheck,
    clock: &Arc<dyn coral_db::Clock>,
) {
    let clock = Arc::clone(clock);
    passes.start(
        "license_recheck",
        Schedule::every(LICENSE_RECHECK_INTERVAL),
        move || {
            let today = super::server::utc_date(clock.as_ref());
            if let Err(refusal) = license.edition.recheck_license(today) {
                tracing::error!(
                    coral.local.license = %refusal,
                    "License ended; Coral stops"
                );
                license.lapse.send_if_modified(|lapse| {
                    let first = lapse.is_none();
                    lapse.get_or_insert(refusal);
                    first
                });
            }
            std::future::ready(Ok(()))
        },
    );
}

/// The Private Tools a Run's `coral.addCustomTool` and its kin save through:
/// the one compiler client and deadline every save shares, with the search
/// index told of each change.
fn private_tools(
    database: &Arc<Database>,
    workspaces: &coral_workspace::WorkspaceModule,
    compiler: &Arc<dyn coral_run_executor::CompilerClient>,
    runtime_limits: coral_run::RuntimeLimits,
    clock: &Arc<dyn coral_db::Clock>,
    search_index: &coral_search::SearchIndex,
) -> coral_run::PrivateTools {
    let module = coral_tools::PrivateToolModule::with_compiler_client(
        Arc::clone(database),
        workspaces.clone(),
        Arc::clone(compiler),
        runtime_limits.compilation_timeout(),
    )
    .with_clock(Arc::clone(clock));
    coral_run::PrivateTools::new(module, search_index.clone())
}

/// The Run sweeper's first pass, before any listener serves and before any
/// but the Run Executor listener binds. On SQLite it ends the Runs that an
/// earlier process's local children held, up to its pass limit: later
/// passes end the rest once their Run Executors' leases lapse. A pass that
/// fails is logged, the listeners bind anyway, and its next pass reads
/// again.
async fn sweep_at_boot(runs: &coral_run::RunModule) {
    if let Err(code) = sweep_runs(runs.sweep_runs_at_boot()).await {
        tracing::warn!(
            error.type = safe::kind(code),
            "Run sweep failed at boot; the next pass retries"
        );
    }
}

/// Takes waiting Runs for `streams` until aborted, asking the local
/// children for room while Runs wait and no stream has it.
fn spawn_take_loop(modules: &Modules, streams: coral_run_executor::PullStreams) -> JoinHandle<()> {
    let runs = modules.runs.clone();
    let children = modules.local_run_executors.clone();
    tokio::spawn(async move {
        let _never = runs.take_runs(streams, children).await;
    })
}

/// Releases the Runs of each local child that exits, at once: its started
/// Runs end interrupted, its unstarted ones wait again (ADR 0027). Each exit
/// is an event, so this is no periodic pass.
fn spawn_child_exits(
    runs: &coral_run::RunModule,
    mut exited: mpsc::UnboundedReceiver<String>,
) -> JoinHandle<()> {
    let runs = runs.clone();
    tokio::spawn(async move {
        while let Some(run_executor_id) = exited.recv().await {
            if let Err(error) = runs.release_run_executor(&run_executor_id).await {
                tracing::warn!(
                    error.type = safe::kind(error.code()),
                    "the Runs of a local Run Executor that exited were not released; \
                     the Run sweep releases them once its lease lapses"
                );
            }
        }
    })
}

/// One pass of the Run sweeper, which logs each Run it writes. It fails
/// with the error's code, and the next pass reads again.
async fn sweep_runs(
    pass: impl std::future::Future<Output = Result<Option<coral_run::Swept>, coral_run::Error>>,
) -> Result<(), &'static str> {
    pass.await.map(drop).map_err(|error| error.code())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use coral_db::UnixNanos;
    use coral_test_support::ManualClock;
    use coral_workspace::{
        ChangeContext, TraceId, WorkspaceDisplayName, WorkspaceModule, local_organization_owner,
    };
    use sqlx::Connection as _;
    use tempfile::TempDir;

    use crate::ServerBuilder;

    /// A connection of its own to the SQLite state database at `dir`.
    async fn state_database(dir: &std::path::Path) -> sqlx::SqliteConnection {
        sqlx::SqliteConnection::connect_with(
            &sqlx::sqlite::SqliteConnectOptions::new().filename(dir.join("coral.db")),
        )
        .await
        .expect("open the state database")
    }

    /// The lease of the one device job in the state database at `dir`.
    async fn device_job_lease(dir: &std::path::Path) -> i64 {
        let (lease_until,): (i64,) = sqlx::query_as("SELECT lease_until FROM provider_device_jobs")
            .fetch_one(&mut state_database(dir).await)
            .await
            .expect("read the device job");
        lease_until
    }

    /// The `Engine` reads the clock its launch supplied, and no other: for
    /// the Provider install at boot and for the device recovery pass. Both
    /// would read the host's clock, a lifetime before this one, if bootstrap
    /// stopped handing its clock down.
    #[tokio::test]
    async fn the_boot_install_and_the_recovery_pass_read_the_clock_given_at_launch() {
        // 2100-01-01: no host's clock reads it.
        let launched_at = UnixNanos::new(4_102_444_800 * 1_000_000_000);
        let temp = TempDir::new().unwrap();
        coral_test_support::write_server_config(
            temp.path(),
            "[listeners.web]\npublic_url = 'http://127.0.0.1:5173'\n",
        );
        // A first start leaves what the second one finds: a Workspace with
        // no Provider that Coral defines, and a device job that cannot be
        // resumed, since `corrupt` is no trace id. Its lease ends just before
        // the second start's clock, in this start's far future.
        let owner = local_organization_owner();
        let server = ServerBuilder::new()
            .with_config_dir(temp.path())
            .start()
            .await
            .unwrap();
        let workspace = WorkspaceModule::new(server.database())
            .create(
                &ChangeContext::new(
                    owner.clone(),
                    coral_db::UnixNanos::new(coral_test_support::now_unix_nanos()),
                    TraceId::current_or_new(),
                ),
                WorkspaceDisplayName::parse("Launched").unwrap(),
            )
            .await
            .unwrap();
        let lease_ended = launched_at.get() - 1;
        let stored = sqlx::query(
            "INSERT INTO provider_device_jobs \
             (id, workspace_id, actor_user_id, trace_id, interval_seconds, next_poll_at, lease_until) \
             VALUES ('job', ?, 'user:device-poll', 'corrupt', 5, 0, ?)",
        )
        .bind(workspace.id().to_string())
        .bind(lease_ended)
        .execute(&mut state_database(temp.path()).await)
        .await
        .unwrap();
        assert_eq!(stored.rows_affected(), 1);
        server.shutdown().await.unwrap();

        let server = ServerBuilder::new()
            .with_config_dir(temp.path())
            .with_clock(Arc::new(ManualClock::starting_at(launched_at)))
            .start()
            .await
            .unwrap();

        let installs = server
            .engine()
            .modules
            .providers
            .list_lifecycle_events(&owner, workspace.id())
            .await
            .unwrap();
        assert!(!installs.is_empty(), "boot installs Coral's Providers");
        assert_eq!(
            installs
                .iter()
                .map(coral_provider::AdministrativeLifecycleEvent::occurred_at)
                .collect::<std::collections::BTreeSet<_>>(),
            [launched_at].into(),
            "every install is dated by the one reading of the launch's clock"
        );
        // The pass found the lease ended and moved it one lease on from its
        // own reading: past the launch's clock.
        tokio::time::timeout(Duration::from_secs(30), async {
            while device_job_lease(temp.path()).await == lease_ended {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the recovery pass reads the launch's clock and finds the lease ended");
        assert!(device_job_lease(temp.path()).await > launched_at.get());
        server.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn the_search_reconcile_task_runs_beside_the_engine_until_it_stops() {
        let temp = TempDir::new().unwrap();
        coral_test_support::write_server_config(
            temp.path(),
            "[listeners.web]\npublic_url = 'http://127.0.0.1:5173'\n",
        );
        let server = ServerBuilder::new()
            .with_config_dir(temp.path())
            .start()
            .await
            .unwrap();
        let engine = server.engine();
        assert!(
            !engine.search_reconcile.is_finished(),
            "the task runs a pass each interval until it is stopped"
        );

        engine.drain_runs().await;
        engine.stop().await;

        tokio::time::timeout(Duration::from_secs(5), async {
            while !engine.search_reconcile.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("stopping the engine aborts the reconcile task");
        server.shutdown().await.unwrap();
    }
}
