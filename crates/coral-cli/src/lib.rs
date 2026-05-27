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
use std::path::PathBuf;
#[cfg(feature = "embedded-ui")]
use std::sync::Arc;

use clap::{
    Arg, ArgAction, ArgGroup, ArgMatches, Args, CommandFactory, Error as ClapError, FromArgMatches,
    Parser, Subcommand, ValueEnum,
};
use clap_complete::{Shell, generate};
use coral_api::v1::ExecuteSqlRequest;
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
            Command::Sql(_) | Command::Source(_) | Command::Onboard | Command::McpStdio(_) => {
                RequiredRuntime::AppClient
            }
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

/// Returns whether this CLI invocation should render telemetry logs to stderr.
///
/// `MCP` stdio reserves stdout for protocol messages, so stderr is the only
/// local diagnostics stream that can be safely exposed while the server is
/// running.
#[must_use]
pub fn enables_stderr_logs() -> bool {
    command_enables_stderr_logs(std::env::args_os())
}

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
async fn run_ui(args: UiArgs) -> Result<(), anyhow::Error> {
    let server = bootstrap::start_ui_server(args.port).await?;
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

/// Parses CLI arguments and runs the shared Coral CLI.
///
/// # Errors
///
/// Returns an error if argument parsing, command execution, or output
/// formatting fails.
pub async fn run(app: AppClient, ctx: coral_app::RunContext) -> Result<(), CliError> {
    let Cli {
        feature_overrides,
        command,
    } = Cli::parse();
    let feature_overrides = feature_overrides.into_overrides();
    let is_mcp_stdio = matches!(&command, Command::McpStdio(_));

    match command.required_runtime() {
        RequiredRuntime::AppClient if is_mcp_stdio => {
            run_app_command(app, command, Some(&ctx), &feature_overrides).await
        }
        RequiredRuntime::AppClient => {
            coral_app::run_with_context(
                &ctx,
                Box::pin(run_app_command(app, command, None, &feature_overrides)),
            )
            .await
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
        Command::Ui(args) => run_ui(args).await.map_err(Into::into),
        Command::Sql(_) | Command::Source(_) | Command::Onboard | Command::McpStdio(_) => {
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
            let response = match app
                .query_client()
                .execute_sql(Request::new(ExecuteSqlRequest {
                    workspace: Some(default_workspace()),
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
        Command::Source(args) => run_source(&app, args).await?,
        Command::Onboard => {
            onboard::run(&app).await?;
        }
        Command::McpStdio(_) => {
            let features = coral_app::features::FeatureStore::discover(None)
                .and_then(|store| store.load_with_overrides(feature_overrides))
                .map_err(anyhow::Error::from)?;
            Box::pin(coral_mcp::run_stdio_with_client(
                app,
                coral_mcp::McpOptions {
                    feedback_enabled: features.enabled(coral_app::features::Feature::Feedback),
                    trace_parent: ctx.and_then(|ctx| ctx.trace_parent.clone()),
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
                    [source.name, source.version, status]
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
                        source.version,
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
    } = args;
    if interactive {
        source_ops::require_interactive()?;
    }
    let response = match (name, file) {
        (Some(name), None) => {
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
            let (manifest_yaml, manifest) = source_ops::load_validated_manifest_file(&file)?;
            if interactive {
                let inputs = source_ops::prompt_for_inputs_with_credential_methods(
                    manifest.declared_inputs(),
                )?;
                source_ops::import_source_with_credentials(app, manifest_yaml, inputs).await?
            } else {
                let (variables, secrets) = source_ops::collect_inputs_from_env(
                    manifest.declared_inputs(),
                    format!(
                        "coral source add --interactive --file {}",
                        source_ops::shell_quote_arg(&file.display().to_string())
                    ),
                )?;
                source_ops::import_source(app, manifest_yaml, variables, secrets).await?
            }
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

#[cfg(test)]
mod tests {
    use clap::{CommandFactory, Parser};

    use super::{Cli, RequiredRuntime, command_enables_stderr_logs};

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

    #[test]
    fn non_mcp_invocation_disables_stderr_logs() {
        assert!(!command_enables_stderr_logs(["coral", "sql", "SELECT 1"]));
    }
}
