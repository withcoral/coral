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
use std::future::Future;
use std::path::PathBuf;
#[cfg(feature = "embedded-ui")]
use std::sync::Arc;

use clap::{
    Arg, ArgAction, ArgGroup, ArgMatches, Args, CommandFactory, Error as ClapError, FromArgMatches,
    Parser, Subcommand, ValueEnum,
};
use clap_complete::{Shell, generate};
use coral_api::v1::{ExecuteSqlRequest, ExecuteSqlResponse, Workspace};
#[cfg(feature = "embedded-ui")]
use coral_app::StaticAssetsProvider;
use coral_client::{
    AppClient, decode_execute_sql_response, default_workspace, format_batches_json,
    format_batches_table, manifest_input_from_proto,
};
use dialoguer::console::measure_text_width;
use tonic::Request;

#[cfg(test)]
use tempfile as _;

/// Default loopback port used by `coral ui` to expose a browser-facing
/// gRPC-Web surface.
#[cfg(feature = "embedded-ui")]
const DEFAULT_SERVER_PORT: u16 = 1457;

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
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Execute a SQL query
    Sql(SqlArgs),
    /// Manage data sources
    Source(SourceArgs),
    /// Manage stored user-owned identities
    Identity(IdentityArgs),
    /// Manage global identity specs used by source identity requirements
    IdentitySpec(IdentitySpecArgs),
    /// Interactive wizard to set up Coral and explore use cases
    Onboard,
    /// Start the MCP server over stdio
    McpStdio(McpStdioArgs),
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

/// Runtime options parsed from the shared CLI surface before an app client is
/// needed.
#[derive(Debug, Clone)]
pub struct SuppliedRuntimeOptions {
    /// Whether the supplied server should render logs to stderr for this
    /// invocation.
    pub enable_stderr_logs: bool,
    /// Process-local runtime feature overrides parsed from global CLI flags.
    pub feature_overrides: coral_app::features::FeatureOverrides,
}

/// Product-specific defaults used by a sibling binary that supplies the app
/// runtime.
#[derive(Debug, Clone, Default)]
pub struct AppRuntimeConfig {
    /// Baseline process-local feature overrides. Explicit global CLI feature
    /// flags still take precedence.
    pub feature_overrides: coral_app::features::FeatureOverrides,
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
/// Inspect and manage experimental runtime features
struct FeaturesArgs {
    #[command(subcommand)]
    command: FeaturesCommand,
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

    /// User-owned source identity binding for imported DSL v4 sources, as `SURFACE=IDENTITY[:ACCEPTED_IDENTITY]`
    #[arg(
        long = "user-identity-binding",
        value_name = "SURFACE=IDENTITY[:ACCEPTED_IDENTITY]"
    )]
    user_identity_bindings: Vec<String>,

    /// Workspace-owned source identity binding for imported DSL v4 sources, as `SURFACE=IDENTITY[:ACCEPTED_IDENTITY]`
    #[arg(
        long = "workspace-identity-binding",
        value_name = "SURFACE=IDENTITY[:ACCEPTED_IDENTITY]"
    )]
    workspace_identity_bindings: Vec<String>,
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

#[derive(Debug, Args)]
/// Manage stored user-owned identities
struct IdentityArgs {
    #[command(subcommand)]
    command: IdentityCommand,
}

#[derive(Debug, Subcommand)]
enum IdentityCommand {
    /// List stored user-owned identities
    List,
    /// Show a stored user-owned identity
    Info {
        /// Identity name
        name: String,
    },
    /// Create or replace a user-owned identity from an identity spec
    Add {
        /// Identity name used by source identity bindings
        name: String,
        /// Installed identity spec name
        #[arg(long = "identity-spec")]
        identity_spec: String,
        /// Read the fixed-token credential from stdin instead of prompting
        #[arg(long = "token-stdin")]
        token_stdin: bool,
    },
    /// Remove a stored user-owned identity
    Remove {
        /// Identity name
        name: String,
    },
}

#[derive(Debug, Args)]
/// Manage global identity specs used by source identity requirements
struct IdentitySpecArgs {
    #[command(subcommand)]
    command: IdentitySpecCommand,
}

#[derive(Debug, Subcommand)]
enum IdentitySpecCommand {
    /// List installed identity specs
    List,
    /// Show an installed identity spec
    Info {
        /// Identity spec name
        name: String,
    },
    /// Install an identity spec or replace an unused existing spec from a manifest file
    Add {
        /// Path to an identity spec YAML file
        #[arg(long)]
        file: PathBuf,
    },
    /// Remove an installed identity spec
    Remove {
        /// Identity spec name
        name: String,
        /// Confirm removal when stored identity instances would become orphaned
        #[arg(long)]
        force: bool,
    },
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
/// Output format for rendered SQL query results.
pub enum OutputFormat {
    /// Render query results as an aligned terminal table.
    Table,
    /// Render query results as JSON.
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
            | Command::Source(_)
            | Command::Identity(_)
            | Command::IdentitySpec(_)
            | Command::Onboard
            | Command::McpStdio(_) => RequiredRuntime::AppClient,
            Command::Features(_) | Command::Completion(_) => RequiredRuntime::None,
            #[cfg(feature = "embedded-ui")]
            Command::Ui(_) => RequiredRuntime::None,
        }
    }

    fn enables_stderr_logs(&self) -> bool {
        matches!(self, Command::McpStdio(_))
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
        command,
    } = Cli::parse();
    let feature_overrides = feature_overrides.into_overrides();
    let ctx = coral_app::RunContext {
        trace_parent: env::trace_parent(),
    };

    match command.required_runtime() {
        RequiredRuntime::AppClient => {
            let is_mcp_stdio = matches!(&command, Command::McpStdio(_));
            let bootstrap = bootstrap::bootstrap(bootstrap::BootstrapOptions {
                enable_stderr_logs: command.enables_stderr_logs(),
                feature_overrides: feature_overrides.clone(),
            })
            .await
            .map_err(anyhow::Error::from)?;
            let app = bootstrap.app.clone();
            let result = if is_mcp_stdio {
                run_app_command(app, command, Some(&ctx), &feature_overrides).await
            } else {
                coral_app::run_with_context(
                    &ctx,
                    Box::pin(run_app_command(app, command, None, &feature_overrides)),
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

/// Parses CLI arguments and runs them against an app runtime supplied by a
/// sibling binary.
///
/// This keeps shared command parsing, feature override handling, output
/// rendering, and telemetry classification in `coral-cli` while allowing a
/// product-specific binary to own server startup and extension composition.
/// The runtime factory is called only for commands that need an app client, and
/// receives parsed runtime options before the app client is constructed.
///
/// # Errors
///
/// Returns an error if runtime startup, command execution, or output formatting
/// fails.
pub async fn run_with_app_runtime<Start, StartFuture, RuntimeGuard>(
    ctx: coral_app::RunContext,
    start_runtime: Start,
) -> Result<(), CliError>
where
    Start: FnOnce(SuppliedRuntimeOptions) -> StartFuture,
    StartFuture: Future<Output = Result<(AppClient, RuntimeGuard), anyhow::Error>>,
{
    run_with_app_runtime_config(ctx, AppRuntimeConfig::default(), start_runtime).await
}

/// Parses CLI arguments and runs them against an app runtime supplied by a
/// sibling binary, applying product-specific runtime defaults before command
/// execution.
///
/// Explicit global CLI feature flags override `config` defaults. This lets a
/// sibling binary keep no-runtime commands such as `features list` aligned with
/// the runtime it would supply for app-backed commands.
///
/// # Errors
///
/// Returns an error if runtime startup, command execution, or output formatting
/// fails.
pub async fn run_with_app_runtime_config<Start, StartFuture, RuntimeGuard>(
    ctx: coral_app::RunContext,
    config: AppRuntimeConfig,
    start_runtime: Start,
) -> Result<(), CliError>
where
    Start: FnOnce(SuppliedRuntimeOptions) -> StartFuture,
    StartFuture: Future<Output = Result<(AppClient, RuntimeGuard), anyhow::Error>>,
{
    let Cli {
        feature_overrides,
        command,
    } = Cli::parse();
    let feature_overrides =
        feature_overrides_with_defaults(feature_overrides.into_overrides(), &config);

    match command.required_runtime() {
        RequiredRuntime::AppClient => {
            let is_mcp_stdio = matches!(&command, Command::McpStdio(_));
            let (app, _runtime_guard) = start_runtime(SuppliedRuntimeOptions {
                enable_stderr_logs: command.enables_stderr_logs(),
                feature_overrides: feature_overrides.clone(),
            })
            .await?;
            if is_mcp_stdio {
                run_app_command(app, command, Some(&ctx), &feature_overrides).await
            } else {
                coral_app::run_with_context(
                    &ctx,
                    Box::pin(run_app_command(app, command, None, &feature_overrides)),
                )
                .await
            }
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

fn feature_overrides_with_defaults(
    mut feature_overrides: coral_app::features::FeatureOverrides,
    config: &AppRuntimeConfig,
) -> coral_app::features::FeatureOverrides {
    feature_overrides.apply_defaults_from(&config.feature_overrides);
    feature_overrides
}

/// Parses CLI arguments and runs them against an already-configured app client.
///
/// Use [`run_with_app_runtime`] instead when parsed feature overrides or stderr
/// log settings must affect the server that backs the client.
///
/// # Errors
///
/// Returns an error if command execution or output formatting fails.
pub async fn run(app: AppClient, ctx: coral_app::RunContext) -> Result<(), CliError> {
    run_with_app_runtime(ctx, |_| async move { Ok((app, ())) }).await
}

/// Returns the shared Coral CLI command definition.
#[must_use]
pub fn command() -> clap::Command {
    Cli::command()
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
    feature_overrides: &coral_app::features::FeatureOverrides,
) -> Result<(), anyhow::Error> {
    let server = bootstrap::start_ui_server(args.port, feature_overrides.clone()).await?;
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

    let signal = tokio::signal::ctrl_c().await;
    let shutdown = server.shutdown().await;
    signal?;
    shutdown?;
    Ok(())
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
        #[cfg(feature = "embedded-ui")]
        Command::Ui(args) => run_ui(args, feature_overrides).await.map_err(Into::into),
        Command::Sql(_)
        | Command::Source(_)
        | Command::Identity(_)
        | Command::IdentitySpec(_)
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
) -> Result<(), CliError> {
    match command {
        Command::Sql(args) => {
            run_sql(&app, default_workspace(), args.sql, args.format).await?;
        }
        Command::Source(args) => run_source(&app, args).await?,
        Command::Identity(args) => run_identity(&app, args).await?,
        Command::IdentitySpec(args) => run_identity_spec(&app, args).await?,
        Command::Onboard => {
            onboard::run(&app).await?;
        }
        Command::McpStdio(_) => {
            let features = coral_app::features::FeatureStore::discover(None)
                .and_then(|store| store.load_with_overrides(feature_overrides))
                .map_err(anyhow::Error::from)?;
            let source_names = match coral_app::bootstrap::default_workspace_source_names() {
                Ok(source_names) => source_names,
                Err(error) => {
                    eprintln!(
                        "warning: failed to load source names for MCP initialize instructions: {error}"
                    );
                    Vec::new()
                }
            };
            Box::pin(coral_mcp::run_stdio_with_client(
                app,
                coral_mcp::McpOptions {
                    feedback_enabled: features.enabled(coral_app::features::Feature::Feedback),
                    episodes_enabled: features.enabled(coral_app::features::Feature::Episodes),
                    trace_parent: ctx.and_then(|ctx| ctx.trace_parent.clone()),
                    source_names,
                },
            ))
            .await
            .map_err(anyhow::Error::from)?;
        }
        Command::Completion(_) => {
            unreachable!("no-runtime commands are routed without an app client")
        }
        Command::Features(_) => {
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

async fn run_source(app: &AppClient, args: SourceArgs) -> Result<(), CliError> {
    match args.command {
        SourceCommand::Discover => {
            let sources = source_ops::discover_sources(app).await?;
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
            let sources = source_ops::list_sources(app).await?;
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
            source_ops::print_source_info(app, &name, verbose).await?;
        }
        SourceCommand::Add(args) => run_source_add(app, args).await?,
        SourceCommand::Lint { file } => {
            source_ops::load_validated_manifest_file(&file)?;
            println!("Manifest is valid");
        }
        SourceCommand::Test { name } => {
            source_ops::test_and_print(
                app,
                &name,
                source_ops::TableDisplayLimit::All,
                source_ops::ValidationSeverityMode::Strict,
            )
            .await?;
        }
        SourceCommand::Remove { name } => {
            source_ops::remove_and_print(app, &name).await?;
        }
    }
    Ok(())
}

async fn run_identity(app: &AppClient, args: IdentityArgs) -> Result<(), CliError> {
    match args.command {
        IdentityCommand::List => {
            let identities = source_ops::list_user_owned_identities(app).await?;
            if identities.is_empty() {
                println!("No identities configured.");
            } else {
                let rows = identities.into_iter().map(|identity| {
                    [
                        identity.name,
                        identity.identity_spec,
                        identity.issuer,
                        identity.identity_type,
                        source_ops::identity_owner_label(identity.owner).to_string(),
                    ]
                });
                print_text_table(
                    ["Identity", "Identity Spec", "Issuer", "Type", "Owner"],
                    rows,
                );
            }
        }
        IdentityCommand::Info { name } => {
            let identity = source_ops::get_user_owned_identity(app, &name).await?;
            print_identity_info(&identity);
        }
        IdentityCommand::Add {
            name,
            identity_spec,
            token_stdin,
        } => {
            let identity_spec_record = source_ops::get_identity_spec(app, &identity_spec).await?;
            let manifest =
                coral_spec::parse_identity_manifest_yaml(&identity_spec_record.manifest_yaml)
                    .map_err(anyhow::Error::from)?;
            let identity = match manifest.identity_type {
                coral_spec::IdentitySpecType::OAuth => {
                    if token_stdin {
                        return Err(anyhow::anyhow!(
                            "--token-stdin is only supported for fixed-token identity specs"
                        )
                        .into());
                    }
                    source_ops::require_interactive_for("identity OAuth setup")?;
                    let selected = source_ops::identity_oauth_method(&manifest)?;
                    source_ops::print_oauth_hint(selected.hint);
                    source_ops::create_user_owned_identity_with_oauth(
                        app,
                        &name,
                        &identity_spec,
                        &selected.label,
                    )
                    .await?
                }
                coral_spec::IdentitySpecType::FixedToken => {
                    let token = if token_stdin {
                        source_ops::read_fixed_token_identity_token_from_stdin()?
                    } else {
                        source_ops::require_interactive_for("fixed-token identity setup")?;
                        source_ops::prompt_fixed_token_identity_token(&identity_spec)?
                    };
                    source_ops::create_user_owned_identity_with_fixed_token(
                        app,
                        &name,
                        &identity_spec,
                        token,
                    )
                    .await?
                }
            };
            println!(
                "Created identity {} ({})",
                identity.name, identity.identity_spec
            );
        }
        IdentityCommand::Remove { name } => {
            source_ops::delete_user_owned_identity(app, &name).await?;
            println!("Removed identity {name}");
        }
    }
    Ok(())
}

fn print_identity_info(identity: &coral_api::v1::Identity) {
    println!("{}", identity.name);
    println!("  Identity spec: {}", identity.identity_spec);
    println!("  Issuer:        {}", identity.issuer);
    println!("  Type:          {}", identity.identity_type);
    println!(
        "  Owner:         {}",
        source_ops::identity_owner_label(identity.owner)
    );
    if !identity.metadata.is_empty() {
        println!("  Metadata:");
        for item in &identity.metadata {
            println!("    {}: {}", item.key, item.value);
        }
    }
}

async fn run_identity_spec(app: &AppClient, args: IdentitySpecArgs) -> Result<(), CliError> {
    match args.command {
        IdentitySpecCommand::List => {
            let identity_specs = source_ops::list_identity_specs(app).await?;
            if identity_specs.is_empty() {
                println!("No identity specs installed.");
            } else {
                let rows = identity_specs.into_iter().map(|identity_spec| {
                    [
                        identity_spec.name,
                        source_ops::display_version(&identity_spec.version),
                        identity_spec.issuer,
                        identity_spec.identity_type,
                    ]
                });
                print_text_table(["Identity Spec", "Version", "Issuer", "Type"], rows);
            }
        }
        IdentitySpecCommand::Info { name } => {
            let identity_spec = source_ops::get_identity_spec(app, &name).await?;
            print!("{}", identity_spec.manifest_yaml);
        }
        IdentitySpecCommand::Add { file } => {
            let manifest_yaml = source_ops::load_validated_identity_spec_file(&file)?;
            let manifest = coral_spec::parse_identity_manifest_yaml(&manifest_yaml)
                .map_err(anyhow::Error::from)?;
            let inputs = source_ops::identity_spec_inputs_for_add(
                &manifest,
                format!(
                    "coral identity-spec add --file {}",
                    source_ops::shell_quote_arg(&file.display().to_string())
                ),
            )?;
            let (identity_spec, replaced) =
                source_ops::add_identity_spec(app, manifest_yaml, inputs).await?;
            let action = if replaced { "Replaced" } else { "Added" };
            println!(
                "{action} identity spec {} ({})",
                identity_spec.name,
                source_ops::display_version(&identity_spec.version)
            );
        }
        IdentitySpecCommand::Remove { name, force } => {
            let orphaned = source_ops::remove_identity_spec(app, &name, force).await?;
            if orphaned == 0 {
                println!("Removed identity spec {name}");
            } else {
                println!("Removed identity spec {name} (orphaned identities: {orphaned})");
            }
        }
    }
    Ok(())
}

/// Build a workspace selector for shared CLI query helpers.
#[must_use]
pub fn workspace_with_name(name: impl Into<String>) -> Workspace {
    Workspace { name: name.into() }
}

/// Execute a SQL query against the given workspace and return the raw response.
///
/// # Errors
///
/// Returns the gRPC status from query execution when the server rejects or fails
/// the request.
pub async fn execute_sql(
    app: &AppClient,
    workspace: Workspace,
    sql: String,
) -> Result<ExecuteSqlResponse, tonic::Status> {
    app.query_client()
        .execute_sql(Request::new(ExecuteSqlRequest {
            workspace: Some(workspace),
            sql,
        }))
        .await
        .map(tonic::Response::into_inner)
}

/// Render a SQL response in the same format used by the shared CLI.
///
/// # Errors
///
/// Returns an error when the response payload cannot be decoded or formatted.
pub fn print_sql_response(
    response: &ExecuteSqlResponse,
    format: OutputFormat,
) -> Result<(), anyhow::Error> {
    let result = decode_execute_sql_response(response).map_err(anyhow::Error::from)?;
    print_batches(result.batches(), format)
}

/// Execute and render a SQL query using the shared CLI's query diagnostics.
///
/// # Errors
///
/// Returns [`CliError::Query`] when the server rejects query execution, or
/// [`CliError::Internal`] when the successful response cannot be decoded or
/// rendered.
pub async fn run_sql(
    app: &AppClient,
    workspace: Workspace,
    sql: String,
    format: OutputFormat,
) -> Result<(), CliError> {
    let response = execute_sql(app, workspace, sql)
        .await
        .map_err(|status| sql_status_to_cli_error(&status))?;
    print_sql_response(&response, format)?;
    Ok(())
}

/// Convert a query status into the shared CLI's structured query diagnostic.
#[must_use]
pub fn sql_status_to_cli_error(status: &tonic::Status) -> CliError {
    CliError::Query {
        error_message: query_error::telemetry_error_message(status),
        error_type: query_error::telemetry_error_type(status),
        rendered_stderr: query_error::render_query_error(status),
    }
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

async fn run_source_add(app: &AppClient, args: SourceAddArgs) -> Result<(), CliError> {
    let SourceAddArgs {
        name,
        file,
        interactive,
        user_identity_bindings,
        workspace_identity_bindings,
    } = args;
    if interactive {
        source_ops::require_interactive()?;
    }
    let has_identity_binding_args =
        !user_identity_bindings.is_empty() || !workspace_identity_bindings.is_empty();
    let response = match (name, file) {
        (Some(name), None) => {
            if has_identity_binding_args {
                return Err(anyhow::anyhow!(
                    "source identity bindings are only supported with `coral source add --file`"
                )
                .into());
            }
            let bundled_name = source_ops::source_name_arg(Some(&name))?;
            let discover = source_ops::discover_sources(app).await?;
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
                source_ops::add_bundled_source_with_credentials(app, &available.name, inputs)
                    .await?
            } else {
                let (variables, secrets) = source_ops::collect_inputs_from_env(
                    &inputs,
                    format!("coral source add --interactive {}", available.name),
                )?;
                source_ops::add_bundled_source(app, &available.name, variables, secrets).await?
            }
        }
        (None, Some(file)) => {
            import_source_file(
                app,
                file,
                interactive,
                &user_identity_bindings,
                &workspace_identity_bindings,
            )
            .await?
        }
        _ => unreachable!("clap enforces exactly one of name or file"),
    };
    println!(
        "Added source {} (secrets: {})",
        response.name,
        source_ops::source_credential_storage_label(response.credential_storage)
    );
    source_ops::validate_and_warn(app, &response.name, source_ops::TableDisplayLimit::DEFAULT)
        .await
        .map_err(Into::into)
}

async fn import_source_file(
    app: &AppClient,
    file: PathBuf,
    interactive: bool,
    user_identity_bindings: &[String],
    workspace_identity_bindings: &[String],
) -> Result<coral_api::v1::Source, CliError> {
    let loaded = source_ops::load_validated_manifest_file(&file)?;
    let interactive_command = format!(
        "coral source add --interactive --file {}",
        source_ops::shell_quote_arg(&file.display().to_string())
    );
    let identity_spec_manifest_yamls = loaded.identity_spec_manifest_yamls();
    let identity_bindings = source_ops::import_source_identity_bindings_from_args(
        user_identity_bindings,
        workspace_identity_bindings,
    )?;

    if interactive {
        let inputs = source_ops::prompt_for_inputs_with_credential_methods(
            loaded.manifest.declared_inputs(),
        )?;
        let identity_spec_inputs =
            source_ops::prompt_identity_spec_inputs_for_import(&loaded.identity_manifests)?;
        return source_ops::import_source_with_credentials(
            app,
            loaded.manifest_yaml,
            inputs,
            identity_spec_manifest_yamls,
            identity_spec_inputs,
            identity_bindings,
        )
        .await
        .map_err(Into::into);
    }

    let (variables, secrets) = source_ops::collect_inputs_from_env(
        loaded.manifest.declared_inputs(),
        interactive_command.clone(),
    )?;
    let identity_spec_inputs = source_ops::identity_spec_inputs_for_import_from_env(
        &loaded.identity_manifests,
        &interactive_command,
    )?;
    source_ops::import_source(
        app,
        loaded.manifest_yaml,
        variables,
        secrets,
        identity_spec_manifest_yamls,
        identity_spec_inputs,
        identity_bindings,
    )
    .await
    .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use clap::{CommandFactory, Parser};
    use coral_app::features::{Feature, FeatureOverrides};

    use super::{
        AppRuntimeConfig, Cli, RequiredRuntime, command_enables_stderr_logs,
        feature_overrides_with_defaults,
    };

    #[test]
    fn server_command_is_not_available() {
        let error = Cli::try_parse_from(["coral", "server", "--help"])
            .expect_err("dev server command should not be exposed");

        assert!(
            error.to_string().contains("unrecognized subcommand"),
            "unexpected parse error: {error}"
        );
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

    fn dsl_v4_overrides(enabled: bool) -> FeatureOverrides {
        let mut overrides = FeatureOverrides::default();
        overrides.set(Feature::DslV4, enabled);
        overrides
    }

    #[test]
    fn runtime_config_feature_overrides_apply_as_defaults() {
        let merged = feature_overrides_with_defaults(
            FeatureOverrides::default(),
            &AppRuntimeConfig {
                feature_overrides: dsl_v4_overrides(true),
            },
        );

        assert_eq!(merged.get(Feature::DslV4), Some(true));
    }

    #[test]
    fn cli_feature_overrides_win_over_runtime_config_defaults() {
        let merged = feature_overrides_with_defaults(
            dsl_v4_overrides(false),
            &AppRuntimeConfig {
                feature_overrides: dsl_v4_overrides(true),
            },
        );

        assert_eq!(merged.get(Feature::DslV4), Some(false));
    }

    #[test]
    fn non_mcp_invocation_disables_stderr_logs() {
        assert!(!command_enables_stderr_logs(["coral", "sql", "SELECT 1"]));
    }
}
