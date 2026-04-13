//! `CLI` entrypoint for the local Coral app.

#![allow(
    clippy::print_stdout,
    clippy::print_stderr,
    reason = "CLI intentionally renders user-facing output"
)]
#![cfg_attr(
    test,
    allow(
        unused_crate_dependencies,
        reason = "Dev-dependencies (e.g. tempfile) are only consumed by integration tests under `tests/`."
    )
)]

mod branding;
mod onboard;
mod source_ops;

use std::path::PathBuf;

use clap::{ArgGroup, Args, Parser, Subcommand, ValueEnum};
use coral_api::v1::ExecuteSqlRequest;
use coral_client::{
    AppClient, ClientBuilder, decode_execute_sql_response, default_workspace, format_batches_json,
    format_batches_table,
};
use tonic::Request;

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
    McpStdio,
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
}

#[derive(Debug, Subcommand)]
enum SourceCommand {
    /// Discover available sources
    Discover,
    /// List configured sources
    List,
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

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let cli = Cli::parse();
    let app = ClientBuilder::new().build().await?;

    match cli.command {
        Command::Sql(args) => {
            let response = app
                .query_client()
                .execute_sql(Request::new(ExecuteSqlRequest {
                    workspace: Some(default_workspace()),
                    sql: args.sql,
                }))
                .await?
                .into_inner();
            let result = decode_execute_sql_response(&response)?;
            print_batches(result.batches(), args.format)?;
        }
        Command::Source(args) => match args.command {
            SourceCommand::Discover => {
                let sources = source_ops::discover_sources(&app).await?;
                if sources.is_empty() {
                    println!("No bundled sources available.");
                } else {
                    for source in sources {
                        let status = if source.installed {
                            "installed"
                        } else {
                            "available"
                        };
                        println!("{}\t{}\t{}", source.name, source.version, status);
                    }
                }
            }
            SourceCommand::List => {
                let sources = source_ops::list_sources(&app).await?;
                if sources.is_empty() {
                    println!("No sources configured.");
                } else {
                    for source in sources {
                        let origin = source_ops::source_origin_label(source.origin);
                        println!("{}\t{}\t{}", source.name, source.version, origin);
                    }
                }
            }
            SourceCommand::Add(args) => run_source_add(&app, args).await?,
            SourceCommand::Lint { file } => {
                source_ops::load_validated_manifest_file(&file)?;
                println!("Manifest is valid");
            }
            SourceCommand::Test { name } => {
                source_ops::validate_and_print(&app, &name, source_ops::TableDisplayLimit::All)
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
        Command::McpStdio => {
            coral_mcp::run_stdio_with_client(app).await?;
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

async fn run_source_add(app: &AppClient, args: SourceAddArgs) -> Result<(), anyhow::Error> {
    source_ops::require_interactive()?;
    let SourceAddArgs { name, file } = args;
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
            let (variables, secrets) = source_ops::prompt_for_inputs(&inputs)?;
            source_ops::add_bundled_source(app, &available.name, variables, secrets).await?
        }
        (None, Some(file)) => {
            let (manifest_yaml, _, inputs) = source_ops::load_validated_manifest_file(&file)?;
            let (variables, secrets) = source_ops::prompt_for_inputs(&inputs)?;
            source_ops::import_source(app, manifest_yaml, variables, secrets).await?
        }
        _ => unreachable!("clap enforces exactly one of name or file"),
    };
    println!("Added source {}", response.name);
    if let Err(err) =
        source_ops::validate_and_print(app, &response.name, source_ops::TableDisplayLimit::DEFAULT)
            .await
    {
        eprintln!("Warning: validation failed: {err}");
    }
    Ok(())
}
