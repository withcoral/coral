use std::path::PathBuf;

use clap::{ArgGroup, Args, Parser, Subcommand, ValueEnum};

#[derive(Debug, Parser)]
#[command(name = "coral", version, arg_required_else_help = true)]
/// Query and manage local data sources
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
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
pub struct SqlArgs {
    /// Output format for query results
    #[arg(long, value_enum, default_value = "table")]
    pub format: OutputFormat,
    /// SQL query to execute
    pub sql: String,
}

#[derive(Debug, Args)]
/// Manage data sources
pub struct SourceArgs {
    #[command(subcommand)]
    pub command: SourceCommand,
}

#[derive(Debug, Args)]
#[command(group(
    ArgGroup::new("source_input")
        .args(["name", "file"])
        .required(true)
        .multiple(false)
))]
pub struct SourceAddArgs {
    /// Name for the new source
    pub name: Option<String>,

    /// Path to a file
    #[arg(long)]
    pub file: Option<PathBuf>,
}

#[derive(Debug, Subcommand)]
pub enum SourceCommand {
    /// Discover available sources
    Discover,
    /// List configured sources
    List,
    /// Add a new source
    Add(SourceAddArgs),
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
pub enum OutputFormat {
    Table,
    Json,
}
