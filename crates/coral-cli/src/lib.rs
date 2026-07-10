//! Shared CLI command parsing and dispatch for Coral clients.

#![allow(
    unused_crate_dependencies,
    clippy::print_stdout,
    clippy::print_stderr,
    reason = "CLI intentionally renders user-facing output and the package includes test-only dependencies."
)]

mod bootstrap;
mod branding;
mod browser;
#[cfg(feature = "embedded-ui")]
mod embedded_ui;
pub mod env;
mod onboard;
mod query_error;
mod source_ops;

use std::borrow::Cow;
use std::fmt::Write as _;
use std::future::Future;
use std::net::SocketAddr;
use std::path::PathBuf;
#[cfg(feature = "embedded-ui")]
use std::sync::Arc;

use clap::{
    Arg, ArgAction, ArgGroup, ArgMatches, Args, CommandFactory, Error as ClapError, FromArgMatches,
    Parser, Subcommand, ValueEnum,
};
use clap_complete::{Shell, generate};
use coral_api::v1::{
    AddFunctionRequest, ClearSearchDataRequest, CreateWorkspaceRequest, DeleteFunctionRequest,
    DeleteWorkspaceRequest, DrainSearchQueueRequest, ExecuteSqlRequest, Function,
    FunctionRuntimeReady, ListFunctionsRequest, ListWorkspacesRequest, RebuildSearchIndexRequest,
    SearchClearTarget, SearchDataScope, SearchIndexProvider, SearchProvider, SearchRequest,
    Workspace, function, search_clear_target, search_maintenance_result,
};
#[cfg(feature = "embedded-ui")]
use coral_app::StaticAssetsProvider;
use coral_client::{
    AppClient, DEFAULT_WORKSPACE_ID, decode_execute_sql_response, format_batches_json,
    format_batches_table, format_search_response_json, format_search_response_text,
    manifest_input_from_proto, workspace as workspace_resource,
};
use dialoguer::console::measure_text_width;
use tonic::Request;

#[cfg(test)]
use tempfile as _;

/// Default loopback port used by `coral ui` to expose a browser-facing
/// gRPC-Web surface.
#[cfg(feature = "embedded-ui")]
const DEFAULT_SERVER_PORT: u16 = 1457;
const MCP_INITIAL_QUERY_EXAMPLE_LIMIT: usize = 5;
const DEFAULT_SEARCH_LIMIT: u32 = 10;
const MIN_SEARCH_LIMIT: u32 = 1;
const MAX_SEARCH_LIMIT: u32 = 50;

#[derive(Debug, Parser)]
#[command(
    name = "coral",
    version = concat!(env!("CARGO_PKG_VERSION"), "+", env!("CORAL_GIT_SHA")),
    arg_required_else_help = true
)]
/// A local-first SQL interface for APIs, files, and other data sources.
struct Cli {
    #[command(flatten)]
    feature_overrides: FeatureOverrideArgs,
    #[command(flatten)]
    workspace_selection: WorkspaceSelectionArgs,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Execute a SQL query
    Sql(SqlArgs),
    /// Search Coral's catalog and, when enabled, locally observed values
    Search(SearchArgs),
    /// Manage Coral's local search indexes
    SearchIndex(SearchIndexArgs),
    /// Manage data sources
    Source(SourceArgs),
    /// Manage workspaces
    Workspace(WorkspaceArgs),
    /// Manage functions
    #[command(name = "functions")]
    Function(FunctionArgs),
    /// Interactive wizard to set up Coral and explore use cases
    Onboard,
    /// Start the MCP server over stdio
    McpStdio(McpStdioArgs),
    /// Start the long-running gRPC server
    Server,
    /// Inspect and manage experimental runtime features
    Features(FeaturesArgs),
    #[cfg(feature = "embedded-ui")]
    /// Start the local gRPC-Web server with the embedded Coral UI
    Ui(UiArgs),
    /// Generate shell completion scripts
    Completion(CompletionArgs),
}

/// Runtime a command needs before it can execute.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RequiredRuntime {
    AppClient,
    None,
}

#[cfg(feature = "embedded-ui")]
#[derive(Debug, Clone, Copy, Args)]
/// Local browser-facing server options
struct UiArgs {
    /// Port to bind on 127.0.0.1 for the local gRPC-Web server
    #[arg(long = "port", value_name = "PORT", default_value_t = DEFAULT_SERVER_PORT)]
    port: u16,
    /// Start the server without opening a browser
    #[arg(long = "no-open")]
    no_open: bool,
}

#[derive(Debug, Args)]
/// Generate shell completion scripts
struct CompletionArgs {
    /// Shell to generate completions for
    shell: Shell,
}

#[derive(Debug, Args)]
/// Execute a SQL query
struct SqlArgs {
    /// Output format for query results
    #[arg(long, value_enum, default_value = "table")]
    format: OutputFormat,
    /// SQL query to execute
    sql: String,
}

#[derive(Debug, Args)]
/// Search Coral's catalog and, when enabled, locally observed values
struct SearchArgs {
    /// Render the shared machine-readable JSON response
    #[arg(long)]
    json: bool,
    /// Maximum search results to return, from 1 to 50. Defaults to 10.
    #[arg(
        long,
        default_value_t = DEFAULT_SEARCH_LIMIT,
        value_parser = clap::value_parser!(u32).range(MIN_SEARCH_LIMIT as i64..=MAX_SEARCH_LIMIT as i64)
    )]
    limit: u32,
    /// Natural language search text
    #[arg(value_name = "QUERY", num_args = 1.., required = true)]
    query: Vec<String>,
}

#[derive(Debug, Args)]
/// Manage Coral's local search indexes
struct SearchIndexArgs {
    #[command(subcommand)]
    command: SearchIndexCommand,
}

#[derive(Debug, Subcommand)]
enum SearchIndexCommand {
    /// Rebuild one or all local search indexes.
    ///
    /// Catalog rebuilds are skipped when already current unless you pass `--force`.
    /// The observed-value projection is always rebuilt when selected.
    Rebuild(SearchRebuildArgs),
    /// Drain app-owned local search queues into queryable projections.
    Drain(SearchDrainArgs),
    /// Clear Coral's local search data for one workspace.
    ///
    /// Clear deletes local search data, so Coral requires both `--yes` and an
    /// explicit `--workspace NAME`.
    Clear(SearchClearArgs),
}

#[derive(Debug, Args)]
struct SearchRebuildArgs {
    /// Search index provider to rebuild
    #[arg(long, value_enum, default_value = "all")]
    provider: SearchRebuildProvider,
    /// Rebuild the catalog projection even when its fingerprint is current
    #[arg(long)]
    force: bool,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum SearchRebuildProvider {
    Catalog,
    ObservedValues,
    All,
}

#[derive(Debug, Args)]
struct SearchDrainArgs {
    /// Per-request drain budget in milliseconds. Zero applies the server default.
    #[arg(long, default_value_t = 0)]
    budget_ms: u32,
}

#[derive(Debug, Args)]
struct SearchClearArgs {
    /// Search data scope to clear
    #[arg(long, value_enum)]
    scope: SearchClearScope,
    /// Installed source owner whose runtime schemas and surfaces should be cleared
    #[arg(long, value_name = "SOURCE")]
    source: Option<String>,
    /// Set when an explicit global `--workspace NAME` selector is present.
    #[arg(skip)]
    explicit_workspace: bool,
    /// Confirm destructive search-data deletion
    #[arg(long, required = true, action = ArgAction::SetTrue)]
    yes: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum SearchClearScope {
    ObservedValues,
    All,
}

#[derive(Debug, Args)]
struct WorkspaceSelectionArgs {
    /// Workspace to target. Overrides `CORAL_WORKSPACE`.
    #[arg(
        long = "workspace",
        value_name = "NAME",
        global = true,
        value_parser = clap::builder::NonEmptyStringValueParser::new()
    )]
    workspace: Option<String>,
}

#[derive(Debug, Default)]
struct FeatureOverrideArgs {
    overrides: coral_app::features::FeatureOverrides,
}

impl FeatureOverrideArgs {
    fn into_overrides(self) -> coral_app::features::FeatureOverrides {
        self.overrides
    }
}

impl FromArgMatches for FeatureOverrideArgs {
    fn from_arg_matches(matches: &ArgMatches) -> Result<Self, ClapError> {
        let mut matches = matches.clone();
        Self::from_arg_matches_mut(&mut matches)
    }

    fn from_arg_matches_mut(matches: &mut ArgMatches) -> Result<Self, ClapError> {
        let mut overrides = coral_app::features::FeatureOverrides::default();
        apply_feature_override_matches(matches, &mut overrides);
        Ok(Self { overrides })
    }

    fn update_from_arg_matches(&mut self, matches: &ArgMatches) -> Result<(), ClapError> {
        let mut matches = matches.clone();
        self.update_from_arg_matches_mut(&mut matches)
    }

    fn update_from_arg_matches_mut(&mut self, matches: &mut ArgMatches) -> Result<(), ClapError> {
        apply_feature_override_matches(matches, &mut self.overrides);
        Ok(())
    }
}

impl Args for FeatureOverrideArgs {
    fn augment_args(cmd: clap::Command) -> clap::Command {
        add_feature_override_args(cmd)
    }

    fn augment_args_for_update(cmd: clap::Command) -> clap::Command {
        add_feature_override_args(cmd)
    }
}

fn apply_feature_override_matches(
    matches: &ArgMatches,
    overrides: &mut coral_app::features::FeatureOverrides,
) {
    for feature in coral_app::features::Feature::all() {
        if matches.get_flag(feature.enable_flag()) {
            overrides.set(feature, true);
        }
        if matches.get_flag(feature.disable_flag()) {
            overrides.set(feature, false);
        }
    }
}

fn add_feature_override_args(mut cmd: clap::Command) -> clap::Command {
    for feature in coral_app::features::Feature::all() {
        let key = feature.key();
        cmd = cmd
            .arg(
                Arg::new(feature.enable_flag())
                    .long(feature.enable_flag())
                    .help(format!(
                        "Enable experimental `{key}` feature for this process"
                    ))
                    .action(ArgAction::SetTrue)
                    .global(true)
                    .hide(true),
            )
            .arg(
                Arg::new(feature.disable_flag())
                    .long(feature.disable_flag())
                    .help(format!(
                        "Disable experimental `{key}` feature for this process"
                    ))
                    .action(ArgAction::SetTrue)
                    .global(true)
                    .hide(true),
            )
            .group(
                ArgGroup::new(feature.key())
                    .arg(feature.enable_flag())
                    .arg(feature.disable_flag())
                    .multiple(false),
            );
    }
    cmd
}

#[derive(Debug, Args)]
/// Start the MCP server over stdio
struct McpStdioArgs {}

#[derive(Debug, Args)]
/// Manage workspaces
struct WorkspaceArgs {
    #[command(subcommand)]
    command: WorkspaceCommand,
}

#[derive(Debug, Subcommand)]
enum WorkspaceCommand {
    /// List configured workspaces
    List,
    /// Create a workspace
    Create {
        /// Name of the workspace to create
        name: String,
    },
    /// Remove a workspace and its sources/artifacts
    Remove {
        /// Name of the workspace to remove
        name: String,
    },
}

#[derive(Debug, Args)]
/// Inspect and manage experimental runtime features
struct FeaturesArgs {
    #[command(subcommand)]
    command: FeaturesCommand,
}

#[derive(Debug, Args)]
/// Manage functions
struct FunctionArgs {
    #[command(subcommand)]
    command: FunctionCommand,
}

#[derive(Debug, Subcommand)]
enum FunctionCommand {
    /// List installed functions
    List,
    /// Add or replace a user function
    Add {
        /// Path to a function SQL artifact
        #[arg(long)]
        file: PathBuf,
    },
    /// Remove a user function
    Remove {
        /// Name of the function to remove
        name: String,
    },
}

#[derive(Debug, Subcommand)]
enum FeaturesCommand {
    /// List experimental runtime features and their current status
    List,
    /// Enable an experimental runtime feature
    Enable {
        /// Feature key to enable
        feature: String,
    },
    /// Disable an experimental runtime feature
    Disable {
        /// Feature key to disable
        feature: String,
    },
}

#[derive(Debug, Args)]
/// Manage data sources
struct SourceArgs {
    #[command(subcommand)]
    command: SourceCommand,
}

#[derive(Debug, Args)]
#[command(group(
    ArgGroup::new("source_input")
        .args(["name", "file"])
        .required(true)
        .multiple(false)
))]
struct SourceAddArgs {
    /// Name for the new source
    name: Option<String>,

    /// Path to a file
    #[arg(long)]
    file: Option<PathBuf>,

    /// Prompt for input values interactively. When unset, values are read from
    /// environment variables matching each input key.
    #[arg(long)]
    interactive: bool,
}

#[derive(Debug, Subcommand)]
enum SourceCommand {
    /// Discover available sources
    Discover,
    /// List configured sources
    List,
    /// Show metadata for a source
    Info {
        /// Name of the source to show info for
        name: String,
        /// Show additional details such as input hints
        #[arg(short, long)]
        verbose: bool,
    },
    /// Add a new source
    Add(SourceAddArgs),
    /// Lint manifest file
    Lint { file: PathBuf },
    /// Test connectivity for a source
    Test {
        /// Name of the source to test
        name: String,
    },
    /// Remove a source
    Remove {
        /// Name of the source to remove
        name: String,
    },
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum OutputFormat {
    Table,
    Json,
}

/// Typed CLI error whose stderr rendering and exit code are owned by the binary.
#[derive(Debug, thiserror::Error)]
pub enum CliError {
    /// Query execution failed with a structured, user-facing diagnostic.
    #[error("query failed")]
    Query {
        /// Complete stderr diagnostic rendered from the query status.
        rendered_stderr: String,
        /// Low-cardinality query failure class for telemetry.
        error_type: String,
        /// Human-readable query failure summary for telemetry.
        error_message: String,
    },
    /// A source was available as a bundled source but has not been installed.
    #[error("source '{source_name}' is not installed")]
    SourceNotInstalled {
        /// Normalized source name requested by the user.
        source_name: String,
    },
    /// A requested source was not found in installed or bundled sources.
    #[error("source '{source_name}' was not found")]
    SourceNotFound {
        /// Normalized source name requested by the user.
        source_name: String,
    },
    /// A requested source was not found while removing an installed source.
    #[error("source '{source_name}' was not found")]
    SourceRemoveNotFound {
        /// Normalized source name requested by the user.
        source_name: String,
    },
    /// Any non-renderable internal command failure.
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

impl CliError {
    #[must_use]
    /// Returns stderr content for user-facing CLI failures.
    pub fn rendered_stderr(&self) -> Option<String> {
        match self {
            Self::Query {
                rendered_stderr, ..
            } => Some(rendered_stderr.clone()),
            Self::SourceNotInstalled { source_name } => Some(format!(
                "source '{source_name}' is not installed. Run `coral source add {source_name}` to install it, then retry `coral source test {source_name}`.\n"
            )),
            Self::SourceNotFound { source_name } => Some(format!(
                "source '{source_name}' was not found. Run `coral source list` to see installed sources or `coral source discover` to see bundled sources available to install.\n"
            )),
            Self::SourceRemoveNotFound { source_name } => Some(format!(
                "source '{source_name}' was not found. Run `coral source list` to see installed sources.\n"
            )),
            Self::Internal(_) => None,
        }
    }
}

impl Command {
    fn required_runtime(&self) -> RequiredRuntime {
        match self {
            Command::Sql(_)
            | Command::Search(_)
            | Command::SearchIndex(_)
            | Command::Source(_)
            | Command::Workspace(_)
            | Command::Function(_)
            | Command::Onboard
            | Command::McpStdio(_) => RequiredRuntime::AppClient,
            Command::Features(_) | Command::Completion(_) | Command::Server => {
                RequiredRuntime::None
            }
            #[cfg(feature = "embedded-ui")]
            Command::Ui(_) => RequiredRuntime::None,
        }
    }

    fn enables_stderr_logs(&self) -> bool {
        matches!(self, Command::McpStdio(_))
    }

    fn uses_selected_workspace(&self) -> bool {
        matches!(
            self,
            Command::Sql(_)
                | Command::Search(_)
                | Command::SearchIndex(_)
                | Command::Source(_)
                | Command::Function(_)
                | Command::Onboard
                | Command::McpStdio(_)
        )
    }

    fn apply_workspace_flag_presence(&mut self, present: bool) {
        if let Command::SearchIndex(args) = self {
            args.apply_workspace_flag_presence(present);
        }
    }
}

impl SearchIndexArgs {
    fn apply_workspace_flag_presence(&mut self, present: bool) {
        if let SearchIndexCommand::Clear(args) = &mut self.command {
            args.explicit_workspace = present;
        }
    }
}

impl coral_app::RunErrorTelemetry for CliError {
    fn telemetry_error_type(&self) -> Cow<'_, str> {
        match self {
            Self::Query { error_type, .. } => Cow::Borrowed(error_type.as_str()),
            Self::SourceNotInstalled { .. } => Cow::Borrowed("SOURCE_NOT_INSTALLED"),
            Self::SourceNotFound { .. } | Self::SourceRemoveNotFound { .. } => {
                Cow::Borrowed("SOURCE_NOT_FOUND")
            }
            Self::Internal(_) => Cow::Borrowed("INTERNAL"),
        }
    }

    fn telemetry_error_message(&self) -> Cow<'_, str> {
        match self {
            Self::Query { error_message, .. } => Cow::Borrowed(error_message.as_str()),
            Self::SourceNotInstalled { source_name } => {
                Cow::Owned(format!("source '{source_name}' is not installed"))
            }
            Self::SourceNotFound { source_name } | Self::SourceRemoveNotFound { source_name } => {
                Cow::Owned(format!("source '{source_name}' was not found"))
            }
            Self::Internal(error) => Cow::Owned(error.to_string()),
        }
    }
}

/// Classifies whether a parsed CLI invocation should render telemetry logs to
/// stderr. `MCP` stdio reserves stdout for protocol messages, so stderr is the
/// only local diagnostics stream that can be safely exposed while the server is
/// running. Exercised only in tests; the live decision uses
/// `command.enables_stderr_logs()` directly in `run_from_env`.
#[cfg(test)]
fn command_enables_stderr_logs<I, T>(args: I) -> bool
where
    I: IntoIterator<Item = T>,
    T: Into<std::ffi::OsString> + Clone,
{
    matches!(
        Cli::try_parse_from(args).map(|cli| cli.command),
        Ok(command) if command.enables_stderr_logs()
    )
}

/// Parses CLI arguments, starts the runtime required by the selected command,
/// and runs the command.
///
/// # Errors
///
/// Returns an error if runtime startup, command execution, or output
/// formatting fails.
pub async fn run_from_env() -> Result<(), CliError> {
    let Cli {
        feature_overrides,
        workspace_selection,
        mut command,
    } = Cli::parse();
    let workspace_flag_present = workspace_selection.workspace.is_some();
    command.apply_workspace_flag_presence(workspace_flag_present);
    let feature_overrides = feature_overrides.into_overrides();
    let ctx = coral_app::RunContext {
        trace_parent: env::trace_parent(),
    };

    match command.required_runtime() {
        RequiredRuntime::AppClient => {
            let workspace = if command.uses_selected_workspace() {
                selected_workspace(workspace_selection.workspace)
            } else {
                workspace_resource(DEFAULT_WORKSPACE_ID)
            };
            let is_mcp_stdio = matches!(&command, Command::McpStdio(_));
            let bootstrap = bootstrap::bootstrap(bootstrap::BootstrapOptions {
                enable_stderr_logs: command.enables_stderr_logs(),
                feature_overrides: feature_overrides.clone(),
            })
            .await
            .map_err(anyhow::Error::from)?;
            let app = bootstrap.app.clone();
            let result = if is_mcp_stdio {
                run_app_command(app, command, Some(&ctx), &feature_overrides, &workspace).await
            } else {
                coral_app::run_with_context(
                    &ctx,
                    Box::pin(run_app_command(
                        app,
                        command,
                        None,
                        &feature_overrides,
                        &workspace,
                    )),
                )
                .await
            };
            bootstrap.shutdown().await;
            result
        }
        RequiredRuntime::None => {
            coral_app::run_with_context(
                &ctx,
                Box::pin(run_no_runtime_command(command, &feature_overrides)),
            )
            .await
        }
    }
}

fn selected_workspace(cli_workspace: Option<String>) -> Workspace {
    workspace_resource(selected_workspace_name(cli_workspace, env::workspace()))
}

fn selected_workspace_name(cli_workspace: Option<String>, env_workspace: Option<String>) -> String {
    cli_workspace
        .or_else(|| env_workspace.filter(|value| !value.is_empty()))
        .unwrap_or_else(|| DEFAULT_WORKSPACE_ID.to_string())
}

/// Returns the embedded Coral UI assets for the local server to serve.
#[cfg(feature = "embedded-ui")]
#[must_use]
pub fn embedded_ui_assets() -> Arc<dyn StaticAssetsProvider> {
    Arc::new(embedded_ui::EmbeddedUi)
}

/// Opens the given URL in the user's default browser.
///
/// # Errors
///
/// Returns an error if the platform browser opener fails.
#[cfg(feature = "embedded-ui")]
pub fn open_url(url: &str) -> Result<(), std::io::Error> {
    browser::open_url(url)
}

#[cfg(feature = "embedded-ui")]
async fn run_ui(
    args: UiArgs,
    feature_overrides: coral_app::features::FeatureOverrides,
) -> Result<(), anyhow::Error> {
    let server = bootstrap::start_ui_server(args.port, feature_overrides).await?;
    let endpoint = server.endpoint_uri().to_string();

    println!("Coral UI listening on {endpoint}");
    if args.no_open {
        println!("Open {endpoint} manually.");
    } else {
        match open_url(&endpoint) {
            Ok(()) => println!("Opened {endpoint}"),
            Err(error) => {
                eprintln!("Could not open browser: {error}");
                eprintln!("Open {endpoint} manually.");
            }
        }
    }
    println!("Press Ctrl-C to stop the UI.");

    run_until_server_stops(server, tokio::signal::ctrl_c()).await
}

async fn run_server(
    feature_overrides: coral_app::features::FeatureOverrides,
) -> Result<(), anyhow::Error> {
    let server = bootstrap::start_standalone_server(feature_overrides).await?;
    let endpoint = server.endpoint_uri().to_string();

    if !server_endpoint_is_loopback(&endpoint) {
        eprintln!(
            "Warning: the native gRPC server at {endpoint} does not authenticate clients; \
             any client that can reach the server can access Coral and its configured sources. \
             Protect it with a trusted network boundary or authenticating proxy."
        );
    }
    println!("Coral gRPC server listening on {endpoint}");
    if let Some(address) = server.mcp_http_addr() {
        println!("Coral MCP HTTP server listening on http://{address}/mcp");
    }
    println!("Connect clients with CORAL_ENDPOINT={endpoint}");
    println!("Press Ctrl-C to stop the server.");

    run_until_server_stops(server, wait_for_server_shutdown_signal()).await
}

async fn run_until_server_stops(
    server: coral_app::RunningServer,
    shutdown_signal: impl Future<Output = Result<(), std::io::Error>>,
) -> Result<(), anyhow::Error> {
    let signal = tokio::select! {
        result = shutdown_signal => Some(result),
        () = server.wait_for_exit() => None,
    };
    let shutdown = server.shutdown().await;
    if let Some(signal) = signal {
        signal?;
    }
    shutdown?;
    Ok(())
}

fn server_endpoint_is_loopback(endpoint: &str) -> bool {
    endpoint
        .strip_prefix("http://")
        .and_then(|authority| authority.parse::<SocketAddr>().ok())
        .is_some_and(|address| address.ip().is_loopback())
}

#[cfg(unix)]
async fn wait_for_server_shutdown_signal() -> Result<(), std::io::Error> {
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    tokio::select! {
        signal = tokio::signal::ctrl_c() => signal,
        _ = sigterm.recv() => Ok(()),
    }
}

#[cfg(windows)]
async fn wait_for_server_shutdown_signal() -> Result<(), std::io::Error> {
    let mut ctrl_break = tokio::signal::windows::ctrl_break()?;
    let mut ctrl_close = tokio::signal::windows::ctrl_close()?;
    let mut ctrl_shutdown = tokio::signal::windows::ctrl_shutdown()?;
    tokio::select! {
        signal = tokio::signal::ctrl_c() => signal,
        _ = ctrl_break.recv() => Ok(()),
        _ = ctrl_close.recv() => Ok(()),
        _ = ctrl_shutdown.recv() => Ok(()),
    }
}

#[cfg(all(not(unix), not(windows)))]
async fn wait_for_server_shutdown_signal() -> Result<(), std::io::Error> {
    tokio::signal::ctrl_c().await
}

async fn run_no_runtime_command(
    command: Command,
    feature_overrides: &coral_app::features::FeatureOverrides,
) -> Result<(), CliError> {
    match command {
        Command::Completion(args) => {
            let mut cmd = Cli::command();
            let bin_name = cmd.get_name().to_string();
            generate(args.shell, &mut cmd, bin_name, &mut std::io::stdout());
            Ok(())
        }
        Command::Features(args) => run_features(args, feature_overrides).map_err(Into::into),
        Command::Server => run_server(feature_overrides.clone())
            .await
            .map_err(Into::into),
        #[cfg(feature = "embedded-ui")]
        Command::Ui(args) => run_ui(args, feature_overrides.clone())
            .await
            .map_err(Into::into),
        Command::Sql(_)
        | Command::Search(_)
        | Command::SearchIndex(_)
        | Command::Source(_)
        | Command::Workspace(_)
        | Command::Function(_)
        | Command::Onboard
        | Command::McpStdio(_) => {
            unreachable!("app client commands are routed through app runtime startup")
        }
    }
}

async fn run_app_command(
    app: AppClient,
    command: Command,
    ctx: Option<&coral_app::RunContext>,
    feature_overrides: &coral_app::features::FeatureOverrides,
    workspace: &Workspace,
) -> Result<(), CliError> {
    match command {
        Command::Sql(args) => {
            let response = match app
                .query_client()
                .execute_sql(Request::new(ExecuteSqlRequest {
                    workspace: Some(workspace.clone()),
                    sql: args.sql,
                }))
                .await
            {
                Ok(response) => response.into_inner(),
                Err(status) => {
                    return Err(CliError::Query {
                        error_message: query_error::telemetry_error_message(&status),
                        error_type: query_error::telemetry_error_type(&status),
                        rendered_stderr: query_error::render_query_error(&status),
                    });
                }
            };
            let result = decode_execute_sql_response(&response).map_err(anyhow::Error::from)?;
            print_batches(result.batches(), args.format)?;
        }
        Command::Search(args) => run_search(&app, workspace, args).await?,
        Command::SearchIndex(args) => run_search_index(&app, workspace, args).await?,
        Command::Source(args) => run_source(&app, workspace, args).await?,
        Command::Workspace(args) => run_workspace(&app, args).await?,
        Command::Function(args) => run_function(&app, workspace, args).await?,
        Command::Onboard => {
            onboard::run(&app, workspace).await?;
        }
        Command::McpStdio(_) => {
            let features = coral_app::features::FeatureStore::discover(None)
                .and_then(|store| store.load_with_overrides(feature_overrides))
                .map_err(anyhow::Error::from)?;
            let source_names = match source_ops::list_sources(&app, workspace).await {
                Ok(sources) => sources.into_iter().map(|source| source.name).collect(),
                Err(error) => {
                    eprintln!(
                        "warning: failed to load source names for MCP initialize instructions: {error}"
                    );
                    Vec::new()
                }
            };
            let (source_names, query_examples) =
                match coral_app::bootstrap::workspace_mcp_startup_context(
                    &workspace.name,
                    source_names.clone(),
                    MCP_INITIAL_QUERY_EXAMPLE_LIMIT,
                ) {
                    Ok(context) => (
                        context.source_names().to_vec(),
                        context
                            .query_history()
                            .iter()
                            .map(|entry| {
                                coral_mcp::McpQueryExample::new(entry.sql())
                                    .with_sources(entry.sources().iter().cloned())
                                    .with_row_count(entry.row_count())
                            })
                            .collect(),
                    ),
                    Err(error) => {
                        eprintln!(
                            "warning: failed to load MCP startup context for initialize instructions: {error}"
                        );
                        (source_names, Vec::new())
                    }
                };
            Box::pin(coral_mcp::run_stdio_with_client(
                app,
                coral_mcp::McpOptions {
                    feedback_enabled: features.enabled(coral_app::features::Feature::Feedback),
                    observed_values_search_enabled: features
                        .enabled(coral_app::features::Feature::ObservedValuesSearch),
                    trace_parent: ctx.and_then(|ctx| ctx.trace_parent.clone()),
                    source_names,
                    query_examples,
                    workspace: Some(workspace.clone()),
                },
            ))
            .await
            .map_err(anyhow::Error::from)?;
        }
        Command::Completion(_) | Command::Features(_) | Command::Server => {
            unreachable!("no-runtime commands are routed without an app client")
        }
        #[cfg(feature = "embedded-ui")]
        Command::Ui(_) => {
            unreachable!("no-runtime commands are routed without an app client")
        }
    }

    Ok(())
}

fn run_features(
    args: FeaturesArgs,
    feature_overrides: &coral_app::features::FeatureOverrides,
) -> Result<(), anyhow::Error> {
    let store = coral_app::features::FeatureStore::discover(None)?;
    match args.command {
        FeaturesCommand::List => {
            let rows = store
                .statuses_with_overrides(feature_overrides)?
                .into_iter()
                .map(|status| {
                    [
                        status.key.to_string(),
                        status.configured.as_str().to_string(),
                        status.enabled.to_string(),
                        status.description.to_string(),
                    ]
                });
            print_text_table(["Feature", "Configured", "Enabled", "Description"], rows);
        }
        FeaturesCommand::Enable { feature } => {
            store.enable(&feature)?;
            println!("Enabled feature `{feature}` in config.toml.");
        }
        FeaturesCommand::Disable { feature } => {
            store.disable(&feature)?;
            println!("Disabled feature `{feature}` in config.toml.");
        }
    }
    Ok(())
}

async fn run_workspace(app: &AppClient, args: WorkspaceArgs) -> Result<(), CliError> {
    match args.command {
        WorkspaceCommand::List => {
            let workspaces = app
                .workspace_client()
                .list_workspaces(Request::new(ListWorkspacesRequest {}))
                .await
                .map_err(anyhow::Error::from)?
                .into_inner()
                .workspaces;
            if workspaces.is_empty() {
                println!("No workspaces configured.");
            } else {
                let rows = workspaces.into_iter().map(|workspace| [workspace.name]);
                print_text_table(["Workspace"], rows);
            }
        }
        WorkspaceCommand::Create { name } => {
            let workspace = workspace_resource(name);
            let workspace = app
                .workspace_client()
                .create_workspace(Request::new(CreateWorkspaceRequest {
                    workspace: Some(workspace),
                }))
                .await
                .map_err(anyhow::Error::from)?
                .into_inner()
                .workspace
                .ok_or_else(|| anyhow::anyhow!("create workspace response missing workspace"))?;
            println!("Created workspace {}", workspace.name);
        }
        WorkspaceCommand::Remove { name } => {
            let workspace = workspace_resource(name);
            let workspace = app
                .workspace_client()
                .delete_workspace(Request::new(DeleteWorkspaceRequest {
                    workspace: Some(workspace),
                }))
                .await
                .map_err(anyhow::Error::from)?
                .into_inner()
                .workspace
                .ok_or_else(|| anyhow::anyhow!("delete workspace response missing workspace"))?;
            println!("Removed workspace {}", workspace.name);
        }
    }
    Ok(())
}

async fn run_source(
    app: &AppClient,
    workspace: &Workspace,
    args: SourceArgs,
) -> Result<(), CliError> {
    match args.command {
        SourceCommand::Discover => {
            let sources = source_ops::discover_sources(app, workspace).await?;
            if sources.is_empty() {
                println!("No bundled sources available.");
            } else {
                let rows = sources.into_iter().map(|source| {
                    let status = if source.installed {
                        "installed".to_string()
                    } else {
                        "available".to_string()
                    };
                    [
                        source.name,
                        source_ops::display_version(&source.version),
                        status,
                    ]
                });
                print_text_table(["Source", "Version", "Status"], rows);
            }
        }
        SourceCommand::List => {
            let sources = source_ops::list_sources(app, workspace).await?;
            if sources.is_empty() {
                println!("No sources configured.");
            } else {
                let rows = sources.into_iter().map(|source| {
                    [
                        source.name,
                        source_ops::display_version(&source.version),
                        source_ops::source_origin_label(source.origin).to_string(),
                        source_ops::source_credential_storage_label(source.credential_storage)
                            .to_string(),
                    ]
                });
                print_text_table(["Source", "Version", "Origin", "Secrets"], rows);
            }
        }
        SourceCommand::Info { name, verbose } => {
            source_ops::print_source_info(app, workspace, &name, verbose).await?;
        }
        SourceCommand::Add(args) => run_source_add(app, workspace, args).await?,
        SourceCommand::Lint { file } => {
            source_ops::load_validated_manifest_file(&file)?;
            println!("Manifest is valid");
        }
        SourceCommand::Test { name } => {
            source_ops::test_and_print(
                app,
                workspace,
                &name,
                source_ops::TableDisplayLimit::All,
                source_ops::ValidationSeverityMode::Strict,
            )
            .await?;
        }
        SourceCommand::Remove { name } => {
            source_ops::remove_and_print(app, workspace, &name).await?;
        }
    }
    Ok(())
}

async fn run_search(
    app: &AppClient,
    workspace: &Workspace,
    args: SearchArgs,
) -> Result<(), CliError> {
    let response = app
        .search_client()
        .search(Request::new(SearchRequest {
            workspace: Some(workspace.clone()),
            query: args.query.join(" "),
            limit: args.limit,
        }))
        .await
        .map_err(anyhow::Error::from)?
        .into_inner();
    if args.json {
        println!(
            "{}",
            format_search_response_json(&response).map_err(anyhow::Error::from)?
        );
    } else {
        print!("{}", format_search_response_text(&response));
    }
    Ok(())
}

async fn run_function(
    app: &AppClient,
    workspace: &Workspace,
    args: FunctionArgs,
) -> Result<(), CliError> {
    match args.command {
        FunctionCommand::List => {
            let mut client = app.function_client();
            let functions = client
                .list_functions(Request::new(ListFunctionsRequest {
                    workspace: Some(workspace.clone()),
                }))
                .await
                .map_err(anyhow::Error::from)?
                .into_inner()
                .functions;
            if functions.is_empty() {
                println!("No installed functions.");
            } else {
                let rows = functions.iter().map(|function| {
                    [
                        function.name.clone(),
                        function_status_summary(function),
                        function_arguments_summary(function),
                        function_publish_summary(function),
                        function_columns_summary(function),
                    ]
                });
                print_text_table(
                    ["Function", "Status", "Arguments", "Publish", "Columns"],
                    rows,
                );
                print_function_invalid_details(&functions);
            }
        }
        FunctionCommand::Add { file } => {
            let sql = std::fs::read_to_string(&file).map_err(anyhow::Error::from)?;
            let mut client = app.function_client();
            let function = client
                .add_function(Request::new(AddFunctionRequest {
                    workspace: Some(workspace.clone()),
                    sql,
                }))
                .await
                .map_err(anyhow::Error::from)?
                .into_inner();
            let function = function.function.ok_or_else(|| {
                anyhow::anyhow!("function service returned no function after add")
            })?;
            println!("Added function {}", function.name);
        }
        FunctionCommand::Remove { name } => {
            let name = function_name_arg(&name)?;
            let mut client = app.function_client();
            client
                .delete_function(Request::new(DeleteFunctionRequest {
                    workspace: Some(workspace.clone()),
                    name: name.clone(),
                }))
                .await
                .map_err(anyhow::Error::from)?;
            println!("Removed function {name}");
        }
    }
    Ok(())
}

fn function_status_summary(function: &Function) -> String {
    match function.runtime.as_ref() {
        Some(function::Runtime::Ready(_)) => "ready".to_string(),
        Some(function::Runtime::Invalid(_)) | None => "invalid".to_string(),
    }
}

fn print_function_invalid_details(functions: &[Function]) {
    let mut invalid_functions = functions.iter().filter_map(|function| {
        function_invalid_reason(function).map(|reason| (function.name.as_str(), reason))
    });
    let Some((first_name, first_reason)) = invalid_functions.next() else {
        return;
    };

    println!("\nInvalid functions:");
    for (name, reason) in std::iter::once((first_name, first_reason)).chain(invalid_functions) {
        println!("  {name}:");
        for line in reason.lines() {
            println!("    {line}");
        }
    }
}

fn function_invalid_reason(function: &Function) -> Option<&str> {
    match function.runtime.as_ref() {
        Some(function::Runtime::Ready(_)) => None,
        Some(function::Runtime::Invalid(invalid)) => Some(invalid.reason.as_str()),
        None => Some("runtime status unavailable"),
    }
}

fn function_publish_summary(function: &Function) -> String {
    let Some(ready) = function_runtime_ready(function) else {
        return "-".to_string();
    };
    ready.table_function.as_ref().map_or_else(
        || "-".to_string(),
        |target| format!("sql: {}.{}", target.schema_name, target.name),
    )
}

fn function_arguments_summary(function: &Function) -> String {
    let Some(ready) = function_runtime_ready(function) else {
        return "-".to_string();
    };
    if ready.arguments.is_empty() {
        return "-".to_string();
    }
    ready
        .arguments
        .iter()
        .map(|argument| format!("{}: {}", argument.name, argument.data_type))
        .collect::<Vec<_>>()
        .join(", ")
}

fn function_columns_summary(function: &Function) -> String {
    let Some(ready) = function_runtime_ready(function) else {
        return "-".to_string();
    };
    if ready.result_columns.is_empty() {
        return "-".to_string();
    }
    let visible_columns = ready
        .result_columns
        .iter()
        .map(|column| column.name.as_str())
        .take(4)
        .collect::<Vec<_>>();
    let hidden_count = ready
        .result_columns
        .len()
        .saturating_sub(visible_columns.len());
    let mut summary = visible_columns.join(", ");
    if hidden_count > 0 {
        write!(summary, ", +{hidden_count}").expect("writing to String should not fail");
    }
    summary
}

fn function_runtime_ready(function: &Function) -> Option<&FunctionRuntimeReady> {
    match function.runtime.as_ref() {
        Some(function::Runtime::Ready(ready)) => Some(ready),
        Some(function::Runtime::Invalid(_)) | None => None,
    }
}

fn function_name_arg(name: &str) -> Result<String, anyhow::Error> {
    let trimmed = name.trim();
    if trimmed.is_empty() {
        return Err(anyhow::anyhow!("missing function name"));
    }
    if trimmed.contains('/') || trimmed.contains('\\') {
        return Err(anyhow::anyhow!(
            "function name must not contain '/' or '\\\\'"
        ));
    }
    if trimmed == "." || trimmed == ".." {
        return Err(anyhow::anyhow!("function name must not be '.' or '..'"));
    }
    Ok(trimmed.to_string())
}

async fn run_search_index(
    app: &AppClient,
    workspace: &Workspace,
    args: SearchIndexArgs,
) -> Result<(), CliError> {
    match args.command {
        SearchIndexCommand::Rebuild(args) => {
            let response = app
                .search_client()
                .rebuild_search_index(Request::new(RebuildSearchIndexRequest {
                    workspace: Some(workspace.clone()),
                    provider: search_rebuild_provider_to_proto(args.provider) as i32,
                    force: args.force,
                }))
                .await
                .map_err(anyhow::Error::from)?
                .into_inner();
            print_search_rebuild_response(&response);
        }
        SearchIndexCommand::Drain(args) => {
            let response = app
                .search_client()
                .drain_search_queue(Request::new(DrainSearchQueueRequest {
                    workspace: Some(workspace.clone()),
                    budget_ms: args.budget_ms,
                }))
                .await
                .map_err(anyhow::Error::from)?
                .into_inner();
            print_search_drain_response(&response);
        }
        SearchIndexCommand::Clear(args) => {
            validate_search_clear_args(&args)?;
            let target = match args.source {
                Some(source) => search_clear_target::Target::SourceName(source),
                None => search_clear_target::Target::Workspace(true),
            };
            let response = app
                .search_client()
                .clear_search_data(Request::new(ClearSearchDataRequest {
                    workspace: Some(workspace.clone()),
                    scope: search_clear_scope_to_proto(args.scope) as i32,
                    target: Some(SearchClearTarget {
                        target: Some(target),
                    }),
                }))
                .await
                .map_err(anyhow::Error::from)?
                .into_inner();
            print_search_clear_response(&response);
        }
    }
    Ok(())
}

fn validate_search_clear_args(args: &SearchClearArgs) -> Result<(), CliError> {
    if !args.explicit_workspace || !args.yes {
        return Err(anyhow::anyhow!(
            "`coral search-index clear --scope {}` requires an explicit `--workspace NAME` and `--yes`",
            search_clear_scope_label(args.scope)
        )
        .into());
    }
    Ok(())
}

fn search_rebuild_provider_to_proto(provider: SearchRebuildProvider) -> SearchIndexProvider {
    match provider {
        SearchRebuildProvider::Catalog => SearchIndexProvider::Catalog,
        SearchRebuildProvider::ObservedValues => SearchIndexProvider::ObservedValues,
        SearchRebuildProvider::All => SearchIndexProvider::All,
    }
}

fn search_clear_scope_to_proto(scope: SearchClearScope) -> SearchDataScope {
    match scope {
        SearchClearScope::ObservedValues => SearchDataScope::ObservedValues,
        SearchClearScope::All => SearchDataScope::All,
    }
}

fn search_clear_scope_label(scope: SearchClearScope) -> &'static str {
    match scope {
        SearchClearScope::ObservedValues => "observed-values",
        SearchClearScope::All => "all",
    }
}

fn print_search_rebuild_response(response: &coral_api::v1::RebuildSearchIndexResponse) {
    for result in &response.results {
        match result.detail.as_ref() {
            Some(search_maintenance_result::Detail::CatalogRebuild(detail)) => {
                if detail.rebuild_performed {
                    println!(
                        "Rebuilt {} search index: old documents {}, new documents {}, projection changed {}.",
                        search_provider_label(result.provider),
                        detail.old_document_count,
                        detail.new_document_count,
                        yes_no(detail.projection_changed)
                    );
                } else {
                    println!(
                        "Skipped rebuilding {} search index: projection already current with {} documents.",
                        search_provider_label(result.provider),
                        detail.new_document_count
                    );
                }
            }
            Some(search_maintenance_result::Detail::ObservedDrain(detail)) => {
                println!(
                    "Drained {} search queue before rebuild: processed {}, stale {}, failed {}, dropped {}, remaining {}, budget exhausted {}, purged {}, evicted {}, storage limit reached {}.",
                    search_provider_label(result.provider),
                    detail.queue_jobs_processed,
                    detail.stale_jobs_skipped,
                    detail.failed_jobs,
                    detail.storage_jobs_dropped,
                    detail.remaining_queue_depth,
                    yes_no(detail.budget_exhausted),
                    detail.stale_rows_purged,
                    detail.evicted_rows,
                    yes_no(detail.storage_limit_reached)
                );
            }
            Some(search_maintenance_result::Detail::ObservedRebuild(detail)) => {
                println!(
                    "Rebuilt {} search index: scanned {} observed values, rebuilt {} FTS rows.",
                    search_provider_label(result.provider),
                    detail.canonical_rows_scanned,
                    detail.fts_rows_rebuilt
                );
                if let Some(drain) = detail.drain.as_ref() {
                    println!(
                        "Pre-rebuild queue: processed {}, upserted {}, wrote {} FTS rows, stale {}, failed {}, dropped {}, remaining {}, budget exhausted {}, purged {}, evicted {}, storage limit reached {}.",
                        drain.queue_jobs_processed,
                        drain.canonical_rows_upserted,
                        drain.fts_rows_written,
                        drain.stale_jobs_skipped,
                        drain.failed_jobs,
                        drain.storage_jobs_dropped,
                        drain.remaining_queue_depth,
                        yes_no(drain.budget_exhausted),
                        drain.stale_rows_purged,
                        drain.evicted_rows,
                        yes_no(drain.storage_limit_reached)
                    );
                }
                if !result.note.is_empty() {
                    println!("{}", result.note);
                }
            }
            _ => {
                println!(
                    "{} search index maintenance: {}",
                    search_provider_label(result.provider),
                    result.note
                );
            }
        }
    }
}

fn print_search_drain_response(response: &coral_api::v1::DrainSearchQueueResponse) {
    for result in &response.results {
        match result.detail.as_ref() {
            Some(search_maintenance_result::Detail::ObservedDrain(detail)) => {
                println!(
                    "Drained {} search queue: processed {}, upserted {}, wrote {} FTS rows, stale {}, failed {}, dropped {}, remaining {}, budget exhausted {}, purged {}, evicted {}, storage limit reached {}.",
                    search_provider_label(result.provider),
                    detail.queue_jobs_processed,
                    detail.canonical_rows_upserted,
                    detail.fts_rows_written,
                    detail.stale_jobs_skipped,
                    detail.failed_jobs,
                    detail.storage_jobs_dropped,
                    detail.remaining_queue_depth,
                    yes_no(detail.budget_exhausted),
                    detail.stale_rows_purged,
                    detail.evicted_rows,
                    yes_no(detail.storage_limit_reached)
                );
            }
            _ => {
                println!(
                    "{} search queue maintenance: {}",
                    search_provider_label(result.provider),
                    result.note
                );
            }
        }
    }
}

fn print_search_clear_response(response: &coral_api::v1::ClearSearchDataResponse) {
    for result in &response.results {
        match result.detail.as_ref() {
            Some(search_maintenance_result::Detail::CatalogClear(detail)) => {
                println!(
                    "Cleared {} search data: deleted {} documents.",
                    search_provider_label(result.provider),
                    detail.deleted_document_count
                );
            }
            Some(search_maintenance_result::Detail::ObservedClear(detail)) => {
                println!(
                    "Cleared {} search data: deleted {} observed values, {} FTS rows, {} queue jobs.",
                    search_provider_label(result.provider),
                    detail.deleted_value_count,
                    detail.deleted_fts_count,
                    detail.deleted_queue_job_count
                );
            }
            _ => {
                println!(
                    "{} search data maintenance: {}",
                    search_provider_label(result.provider),
                    result.note
                );
            }
        }
    }
    if let Some(storage_cleanup) = response.storage_cleanup.as_ref() {
        println!("Storage cleanup: {}.", storage_cleanup.note);
    }
}

fn search_provider_label(provider: i32) -> &'static str {
    match SearchProvider::try_from(provider).ok() {
        Some(SearchProvider::CatalogMetadata) => "catalog",
        Some(SearchProvider::ObservedValues) => "observed-values",
        Some(SearchProvider::NativeFanout) => "native-fanout",
        Some(SearchProvider::Unspecified) | None => "unknown",
    }
}

fn yes_no(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}

fn print_batches(
    batches: &[arrow::record_batch::RecordBatch],
    format: OutputFormat,
) -> Result<(), anyhow::Error> {
    let output = match format {
        OutputFormat::Table => format_batches_table(batches)?,
        OutputFormat::Json => format_batches_json(batches)?,
    };
    println!("{output}");
    Ok(())
}

fn print_text_table<const COLUMNS: usize>(
    headers: [&str; COLUMNS],
    rows: impl IntoIterator<Item = [String; COLUMNS]>,
) {
    let rows = rows.into_iter().collect::<Vec<_>>();
    let widths = compute_column_widths(headers, &rows);

    println!("{}", format_table_row(headers, &widths));
    println!("{}", format_separator_row(&widths));
    for row in rows {
        println!("{}", format_table_row(row.each_ref(), &widths));
    }
}

fn compute_column_widths<const COLUMNS: usize>(
    headers: [&str; COLUMNS],
    rows: &[[String; COLUMNS]],
) -> [usize; COLUMNS] {
    std::array::from_fn(|idx| {
        let header_width = measure_text_width(
            headers
                .get(idx)
                .expect("column index is bounded by array length"),
        );
        let row_width = rows
            .iter()
            .map(|row| {
                measure_text_width(
                    row.get(idx)
                        .expect("column index is bounded by array length"),
                )
            })
            .max()
            .unwrap_or(0);
        header_width.max(row_width)
    })
}

fn format_table_row<const COLUMNS: usize, T>(
    cells: [T; COLUMNS],
    widths: &[usize; COLUMNS],
) -> String
where
    T: AsRef<str>,
{
    cells
        .into_iter()
        .enumerate()
        .map(|(idx, cell)| {
            let width = widths
                .get(idx)
                .expect("column index is bounded by array length");
            pad_cell(cell.as_ref(), *width, idx + 1 < COLUMNS)
        })
        .collect::<Vec<_>>()
        .join("  ")
}

fn format_separator_row<const COLUMNS: usize>(widths: &[usize; COLUMNS]) -> String {
    widths
        .iter()
        .map(|width| "-".repeat(*width))
        .collect::<Vec<_>>()
        .join("  ")
}

fn pad_cell(value: &str, width: usize, pad: bool) -> String {
    if !pad {
        return value.to_string();
    }

    let padding = width.saturating_sub(measure_text_width(value));
    format!("{value}{}", " ".repeat(padding))
}

async fn run_source_add(
    app: &AppClient,
    workspace: &Workspace,
    args: SourceAddArgs,
) -> Result<(), CliError> {
    let SourceAddArgs {
        name,
        file,
        interactive,
    } = args;
    if interactive {
        source_ops::require_interactive()?;
    }
    let response = match (name, file) {
        (Some(name), None) => {
            let bundled_name = source_ops::source_name_arg(Some(&name))?;
            let discover = source_ops::discover_sources(app, workspace).await?;
            let available = discover
                .into_iter()
                .find(|source| source.name == bundled_name)
                .ok_or_else(|| anyhow::anyhow!("unknown bundled source '{bundled_name}'"))?;
            let inputs = available
                .inputs
                .iter()
                .map(manifest_input_from_proto)
                .collect::<Result<Vec<_>, _>>()
                .map_err(anyhow::Error::from)?;
            if interactive {
                let inputs = source_ops::prompt_for_inputs_with_credential_methods(&inputs)?;
                source_ops::add_bundled_source_with_credentials(
                    app,
                    workspace,
                    &available.name,
                    inputs,
                )
                .await?
            } else {
                let (variables, secrets) = source_ops::collect_inputs_from_env(
                    &inputs,
                    format!("coral source add --interactive {}", available.name),
                )?;
                source_ops::add_bundled_source(app, workspace, &available.name, variables, secrets)
                    .await?
            }
        }
        (None, Some(file)) => {
            let (manifest_yaml, manifest) = source_ops::load_validated_manifest_file(&file)?;
            if interactive {
                let inputs = source_ops::prompt_for_inputs_with_credential_methods(
                    manifest.declared_inputs(),
                )?;
                source_ops::import_source_with_credentials(app, workspace, manifest_yaml, inputs)
                    .await?
            } else {
                let (variables, secrets) = source_ops::collect_inputs_from_env(
                    manifest.declared_inputs(),
                    format!(
                        "coral source add --interactive --file {}",
                        source_ops::shell_quote_arg(&file.display().to_string())
                    ),
                )?;
                source_ops::import_source(app, workspace, manifest_yaml, variables, secrets).await?
            }
        }
        _ => unreachable!("clap enforces exactly one of name or file"),
    };
    println!(
        "Added source {} (secrets: {})",
        response.name,
        source_ops::source_credential_storage_label(response.credential_storage)
    );
    source_ops::validate_and_warn(
        app,
        workspace,
        &response.name,
        source_ops::TableDisplayLimit::DEFAULT,
    )
    .await
    .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use clap::{CommandFactory, Parser};
    use coral_api::v1::{
        Function, FunctionRuntimeInvalid, FunctionRuntimeReady, TableFunctionResultColumn, function,
    };

    use super::{
        Cli, RequiredRuntime, command_enables_stderr_logs, function_columns_summary,
        function_status_summary, server_endpoint_is_loopback,
    };

    #[test]
    fn server_command_requires_no_app_client_runtime() {
        let cli = Cli::try_parse_from(["coral", "server"]).expect("server should parse");

        assert_eq!(cli.command.required_runtime(), RequiredRuntime::None);
        assert!(matches!(cli.command, super::Command::Server));
    }

    #[test]
    fn server_command_rejects_command_line_bind_overrides() {
        for args in [
            ["coral", "server", "--bind", "127.0.0.1"],
            ["coral", "server", "--port", "14555"],
        ] {
            Cli::try_parse_from(args).expect_err("server bind overrides should be rejected");
        }
    }

    #[test]
    fn server_security_warning_targets_non_loopback_endpoints() {
        for endpoint in ["http://127.0.0.1:14555", "http://[::1]:14555"] {
            assert!(
                server_endpoint_is_loopback(endpoint),
                "endpoint: {endpoint}"
            );
        }
        for endpoint in [
            "http://0.0.0.0:14555",
            "http://[::]:14555",
            "http://192.168.1.10:14555",
        ] {
            assert!(
                !server_endpoint_is_loopback(endpoint),
                "endpoint: {endpoint}"
            );
        }
    }

    #[cfg(feature = "embedded-ui")]
    #[test]
    fn ui_command_uses_custom_port_without_required_runtime() {
        let cli = Cli::try_parse_from(["coral", "ui", "--port", "1459", "--no-open"])
            .expect("ui args should parse");

        assert_eq!(cli.command.required_runtime(), RequiredRuntime::None);
        let super::Command::Ui(args) = cli.command else {
            panic!("expected ui command");
        };
        assert_eq!(args.port, 1459);
        assert!(args.no_open);
    }

    #[test]
    fn completion_requires_no_runtime() {
        let cli = Cli::try_parse_from(["coral", "completion", "bash"])
            .expect("completion args should parse");

        assert_eq!(cli.command.required_runtime(), RequiredRuntime::None);
    }

    #[test]
    fn features_command_requires_no_runtime() {
        let cli =
            Cli::try_parse_from(["coral", "features", "list"]).expect("features args should parse");

        assert_eq!(cli.command.required_runtime(), RequiredRuntime::None);
    }

    #[test]
    fn regular_commands_use_normal_app_bootstrap() {
        let cli = Cli::try_parse_from(["coral", "source", "list"]).expect("source list parses");

        assert_eq!(cli.command.required_runtime(), RequiredRuntime::AppClient);

        let cli =
            Cli::try_parse_from(["coral", "functions", "list"]).expect("functions list parses");

        assert_eq!(cli.command.required_runtime(), RequiredRuntime::AppClient);
    }

    #[test]
    fn function_columns_summary_shows_hidden_column_count() {
        let mut function = Function {
            runtime: Some(function::Runtime::Ready(FunctionRuntimeReady {
                result_columns: [
                    "number",
                    "title",
                    "html_url",
                    "state",
                    "author",
                    "updated_at",
                ]
                .into_iter()
                .map(|name| TableFunctionResultColumn {
                    name: name.to_string(),
                    ..TableFunctionResultColumn::default()
                })
                .collect(),
                ..FunctionRuntimeReady::default()
            })),
            ..Function::default()
        };

        assert_eq!(
            function_columns_summary(&function),
            "number, title, html_url, state, +2"
        );

        match function.runtime.as_mut() {
            Some(function::Runtime::Ready(ready)) => ready.result_columns.truncate(4),
            Some(function::Runtime::Invalid(_)) | None => panic!("ready function"),
        }
        assert_eq!(
            function_columns_summary(&function),
            "number, title, html_url, state"
        );

        match function.runtime.as_mut() {
            Some(function::Runtime::Ready(ready)) => ready.result_columns.clear(),
            Some(function::Runtime::Invalid(_)) | None => panic!("ready function"),
        }
        assert_eq!(function_columns_summary(&function), "-");
    }

    #[test]
    fn search_command_uses_app_runtime() {
        let cli =
            Cli::try_parse_from(["coral", "search", "github", "issues"]).expect("search parses");

        assert_eq!(cli.command.required_runtime(), RequiredRuntime::AppClient);
    }

    #[test]
    fn search_rebuild_parses_as_free_text_query() {
        let cli = Cli::try_parse_from(["coral", "search", "rebuild"])
            .expect("search rebuild should parse as query text");

        let super::Command::Search(args) = cli.command else {
            panic!("expected search command");
        };
        assert_eq!(args.query, vec!["rebuild".to_string()]);
    }

    #[test]
    fn workspace_flag_requires_a_non_empty_name() {
        Cli::try_parse_from([
            "coral",
            "search-index",
            "clear",
            "--scope",
            "all",
            "--workspace",
            "--yes",
        ])
        .expect_err("bare workspace flag must be rejected");
        Cli::try_parse_from([
            "coral",
            "search-index",
            "clear",
            "--scope",
            "all",
            "--workspace",
            "",
            "--yes",
        ])
        .expect_err("empty workspace name must be rejected");
    }

    #[test]
    fn former_workspace_confirmation_marker_is_a_regular_name() {
        let cli = Cli::try_parse_from([
            "coral",
            "search-index",
            "clear",
            "--scope",
            "all",
            "--workspace",
            "__coral_current_workspace_confirmation__",
            "--yes",
        ])
        .expect("former confirmation marker should parse as a workspace name");

        assert_eq!(
            cli.workspace_selection.workspace.as_deref(),
            Some("__coral_current_workspace_confirmation__")
        );
    }

    #[test]
    fn observed_values_clear_scope_matches_rebuild_provider_name() {
        let cli = Cli::try_parse_from([
            "coral",
            "search-index",
            "clear",
            "--scope",
            "observed-values",
            "--workspace",
            "work",
            "--yes",
        ])
        .expect("observed-values clear scope should parse");

        let super::Command::SearchIndex(search_index) = cli.command else {
            panic!("expected search-index command");
        };
        let super::SearchIndexCommand::Clear(args) = search_index.command else {
            panic!("expected search-index clear command");
        };
        assert_eq!(args.scope, super::SearchClearScope::ObservedValues);
    }

    #[test]
    fn source_scoped_search_index_clear_requires_workspace_confirmation() {
        let args = super::SearchClearArgs {
            scope: super::SearchClearScope::ObservedValues,
            source: Some("github".to_string()),
            explicit_workspace: false,
            yes: true,
        };

        let error = super::validate_search_clear_args(&args)
            .expect_err("source clear without workspace must fail");

        assert!(
            error
                .to_string()
                .contains("requires an explicit `--workspace NAME` and `--yes`"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn search_limit_rejects_values_outside_cli_contract() {
        Cli::try_parse_from(["coral", "search", "--limit", "0", "github"])
            .expect_err("zero limit should fail before contacting the server");
        Cli::try_parse_from(["coral", "search", "--limit", "51", "github"])
            .expect_err("limit above the server cap should fail before contacting the server");
        Cli::try_parse_from(["coral", "search", "--limit", "50", "github"])
            .expect("server maximum should parse");
    }

    #[test]
    fn function_status_summary_stays_on_one_line() {
        let function = Function {
            runtime: Some(function::Runtime::Invalid(FunctionRuntimeInvalid {
                reason: "source 'github' is not installed".to_string(),
            })),
            ..Function::default()
        };

        assert_eq!(function_status_summary(&function), "invalid");
        let ready = Function {
            runtime: Some(function::Runtime::Ready(FunctionRuntimeReady::default())),
            ..Function::default()
        };
        assert_eq!(function_status_summary(&ready), "ready");
    }

    #[test]
    fn selected_workspace_preserves_raw_name_for_app_validation() {
        let workspace = super::selected_workspace(Some(" ../bad ".to_string()));

        assert_eq!(workspace.name, " ../bad ");
    }

    #[test]
    fn selected_workspace_preserves_former_confirmation_marker() {
        let workspace = super::selected_workspace_name(
            Some("__coral_current_workspace_confirmation__".to_string()),
            None,
        );

        assert_eq!(workspace, "__coral_current_workspace_confirmation__");
    }

    #[test]
    fn selected_workspace_treats_empty_env_value_as_unset() {
        let workspace = super::selected_workspace_name(None, Some(String::new()));

        assert_eq!(workspace, super::DEFAULT_WORKSPACE_ID);
    }

    #[test]
    fn only_workspace_scoped_commands_use_selected_workspace() {
        let sql = Cli::try_parse_from(["coral", "sql", "SELECT 1"]).expect("sql parses");
        let search =
            Cli::try_parse_from(["coral", "search", "github", "issues"]).expect("search parses");
        let source = Cli::try_parse_from(["coral", "source", "list"]).expect("source parses");
        let functions =
            Cli::try_parse_from(["coral", "functions", "list"]).expect("functions parses");
        let onboard = Cli::try_parse_from(["coral", "onboard"]).expect("onboard parses");
        let workspace =
            Cli::try_parse_from(["coral", "workspace", "list"]).expect("workspace parses");
        let mcp = Cli::try_parse_from(["coral", "mcp-stdio"]).expect("mcp parses");

        assert!(sql.command.uses_selected_workspace());
        assert!(search.command.uses_selected_workspace());
        assert!(source.command.uses_selected_workspace());
        assert!(functions.command.uses_selected_workspace());
        assert!(onboard.command.uses_selected_workspace());
        assert!(mcp.command.uses_selected_workspace());
        assert!(!workspace.command.uses_selected_workspace());
    }

    #[test]
    fn mcp_stdio_invocation_enables_stderr_logs() {
        assert!(command_enables_stderr_logs(["coral", "mcp-stdio"]));
    }

    #[test]
    fn mcp_stdio_with_feedback_invocation_enables_stderr_logs() {
        assert!(command_enables_stderr_logs([
            "coral",
            "mcp-stdio",
            "--enable-feedback"
        ]));
    }

    #[test]
    fn global_feature_overrides_parse_before_subcommand() {
        let cli = Cli::try_parse_from(["coral", "--enable-feedback", "mcp-stdio"])
            .expect("global feature override should parse before subcommand");

        assert!(matches!(cli.command, super::Command::McpStdio(_)));
    }

    #[test]
    fn global_feature_overrides_are_hidden_from_help() {
        let mut help = Vec::new();
        Cli::command()
            .write_long_help(&mut help)
            .expect("help should render");
        let help = String::from_utf8(help).expect("help should be utf8");

        assert!(
            !help.contains("--enable-feedback"),
            "feature override flags should not be visible in help: {help}"
        );
        assert!(
            !help.contains("--disable-feedback"),
            "feature override flags should not be visible in help: {help}"
        );
    }

    #[test]
    fn conflicting_global_feature_overrides_are_rejected() {
        let error = Cli::try_parse_from([
            "coral",
            "--enable-feedback",
            "--disable-feedback",
            "mcp-stdio",
        ])
        .expect_err("conflicting feature overrides should fail");

        assert_eq!(error.kind(), clap::error::ErrorKind::ArgumentConflict);
    }

    #[test]
    fn retired_task_feature_flags_are_rejected() {
        for flag in ["--enable-tasks", "--disable-tasks"] {
            let error = Cli::try_parse_from(["coral", flag, "mcp-stdio"])
                .expect_err("retired task feature flag should be rejected");

            assert_eq!(error.kind(), clap::error::ErrorKind::UnknownArgument);
        }
    }

    #[test]
    fn non_mcp_invocation_disables_stderr_logs() {
        assert!(!command_enables_stderr_logs(["coral", "sql", "SELECT 1"]));
    }
}
