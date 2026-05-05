//! Shared CLI command parsing and dispatch for Coral clients.

#![allow(
    unused_crate_dependencies,
    clippy::print_stdout,
    clippy::print_stderr,
    reason = "CLI intentionally renders user-facing output and the package includes test-only dependencies."
)]

mod branding;
pub mod env;
mod onboard;
mod query_error;
mod source_ops;

use std::path::PathBuf;

use clap::{ArgGroup, Args, CommandFactory, Parser, Subcommand, ValueEnum};
use clap_complete::{Shell, generate};
use coral_api::v1::ExecuteSqlRequest;
use coral_client::{
    AppClient, decode_execute_sql_response, default_workspace, format_batches_json,
    format_batches_table,
};
use dialoguer::console::measure_text_width;
use tonic::Request;

#[cfg(test)]
use tempfile as _;

#[derive(Debug, Parser)]
#[command(name = "coral", version, arg_required_else_help = true)]
/// A local-first SQL interface for APIs, files, and other data sources.
struct Cli {
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
    /// Generate shell completion scripts
    Completion(CompletionArgs),
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
/// Start the MCP server over stdio
struct McpStdioArgs {
    /// Expose the feedback submission tool.
    #[arg(long)]
    enable_feedback: bool,
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
#[error("cli command failed")]
pub struct CliExitError {
    rendered_stderr: String,
}

impl CliExitError {
    #[must_use]
    /// Builds a CLI error with pre-rendered stderr output.
    pub fn new(rendered_stderr: String) -> Self {
        Self { rendered_stderr }
    }

    #[must_use]
    /// Returns the stderr block the binary should render before exiting.
    pub fn rendered_stderr(&self) -> &str {
        &self.rendered_stderr
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
        Ok(Command::McpStdio(_))
    )
}

/// Parses CLI arguments and runs the shared Coral CLI.
///
/// # Errors
///
/// Returns an error if argument parsing, command execution, or output
/// formatting fails.
pub async fn run(app: AppClient, ctx: coral_app::RunContext) -> Result<(), anyhow::Error> {
    coral_app::run_with_context(&ctx, Box::pin(run_parsed(app, Cli::parse()))).await
}

async fn run_parsed(app: AppClient, cli: Cli) -> Result<(), anyhow::Error> {
    match cli.command {
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
                    return Err(CliExitError::new(query_error::render_query_error(&status)).into());
                }
            };
            let result = decode_execute_sql_response(&response)?;
            print_batches(result.batches(), args.format)?;
        }
        Command::Source(args) => match args.command {
            SourceCommand::Discover => {
                let sources = source_ops::discover_sources(&app).await?;
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
                let sources = source_ops::list_sources(&app).await?;
                if sources.is_empty() {
                    println!("No sources configured.");
                } else {
                    let rows = sources.into_iter().map(|source| {
                        [
                            source.name,
                            source.version,
                            source_ops::source_origin_label(source.origin).to_string(),
                        ]
                    });
                    print_text_table(["Source", "Version", "Origin"], rows);
                }
            }
            SourceCommand::Info { name, verbose } => {
                source_ops::print_source_info(&app, &name, verbose).await?;
            }
            SourceCommand::Add(args) => run_source_add(&app, args).await?,
            SourceCommand::Lint { file } => {
                source_ops::load_validated_manifest_file(&file)?;
                println!("Manifest is valid");
            }
            SourceCommand::Test { name } => {
                source_ops::test_and_print(
                    &app,
                    &name,
                    source_ops::TableDisplayLimit::All,
                    source_ops::ValidationSeverityMode::Strict,
                )
                .await?;
            }
            SourceCommand::Remove { name } => {
                source_ops::delete_source(&app, &name).await?;
                println!("Removed source {name}");
            }
        },
        Command::Onboard => {
            onboard::run(&app).await?;
        }
        Command::McpStdio(args) => {
            coral_mcp::run_stdio_with_client(
                app,
                coral_mcp::McpOptions {
                    feedback_enabled: args.enable_feedback,
                },
            )
            .await?;
        }
        Command::Completion(args) => {
            let mut cmd = Cli::command();
            let bin_name = cmd.get_name().to_string();
            generate(args.shell, &mut cmd, bin_name, &mut std::io::stdout());
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
        let header_width = measure_text_width(headers[idx]);
        let row_width = rows
            .iter()
            .map(|row| measure_text_width(&row[idx]))
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
        .map(|(idx, cell)| pad_cell(cell.as_ref(), widths[idx], idx + 1 < COLUMNS))
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

async fn run_source_add(app: &AppClient, args: SourceAddArgs) -> Result<(), anyhow::Error> {
    let SourceAddArgs {
        name,
        file,
        interactive,
    } = args;
    if interactive {
        source_ops::require_interactive()?;
    }
    let collect = |inputs: &[coral_spec::ManifestInputSpec]| {
        if interactive {
            source_ops::prompt_for_inputs(inputs)
        } else {
            source_ops::collect_inputs_from_env(inputs)
        }
    };
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
                .map(source_ops::manifest_input_from_proto)
                .collect::<Result<Vec<_>, _>>()?;
            let (variables, secrets) = collect(&inputs)?;
            source_ops::add_bundled_source(app, &available.name, variables, secrets).await?
        }
        (None, Some(file)) => {
            let (manifest_yaml, manifest) = source_ops::load_validated_manifest_file(&file)?;
            let (variables, secrets) = collect(manifest.declared_inputs())?;
            source_ops::import_source(app, manifest_yaml, variables, secrets).await?
        }
        _ => unreachable!("clap enforces exactly one of name or file"),
    };
    println!("Added source {}", response.name);
    source_ops::validate_and_warn(app, &response.name, source_ops::TableDisplayLimit::DEFAULT).await
}

#[cfg(test)]
mod tests {
    use super::command_enables_stderr_logs;

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
    fn non_mcp_invocation_disables_stderr_logs() {
        assert!(!command_enables_stderr_logs(["coral", "sql", "SELECT 1"]));
    }
}
