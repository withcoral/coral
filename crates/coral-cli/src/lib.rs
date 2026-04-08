//! Clap CLI definitions for the `coral` binary.
//!
//! These types are public so that tooling (such as the `xtask` doc generator)
//! can introspect the full command tree via [`clap::CommandFactory`].

#![allow(
    unused_crate_dependencies,
    reason = "lib target only re-exports clap types; other deps are consumed by the bin target"
)]

use std::path::PathBuf;

use clap::{Args, Parser, Subcommand, ValueEnum};

/// Query and manage local data sources
#[derive(Debug, Parser)]
#[command(name = "coral", version, arg_required_else_help = true)]
pub struct Cli {
    /// Subcommand to run
    #[command(subcommand)]
    pub command: Command,
}

/// Top-level CLI commands
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Execute a SQL query
    #[command(after_long_help = "\
Examples:
  coral sql \"SELECT * FROM coral.tables LIMIT 10\"
  coral sql --format json \"SELECT * FROM coral.tables LIMIT 10\"
  coral sql --format table \"SELECT * FROM slack.messages LIMIT 10\"")]
    Sql(SqlArgs),

    /// Manage bundled and imported sources in the local workspace
    Source(SourceArgs),

    /// Run the guided source setup flow
    #[command(after_long_help = "\
This interactive command helps you:

- add a bundled source
- import a custom source manifest
- validate an installed source
- see the next recommended commands after setup")]
    Onboard,

    /// Expose the local Coral runtime as an MCP stdio server
    #[command(after_long_help = "\
This command is intended to be launched by an MCP-capable client rather \
than used directly in an interactive shell.

The current MCP surface is intentionally small:

- tools: `sql`, `list_tables`
- resources: `coral://guide`, `coral://tables`")]
    McpStdio,
}

/// Arguments for the `sql` command
#[derive(Debug, Args)]
pub struct SqlArgs {
    /// Output format for query results
    #[arg(long, value_enum, default_value = "table")]
    pub format: OutputFormat,

    /// SQL query to execute
    pub sql: String,
}

/// Arguments for the `source` command
#[derive(Debug, Args)]
pub struct SourceArgs {
    /// Source management subcommand
    #[command(subcommand)]
    pub command: SourceCommand,
}

/// Source management subcommands
#[derive(Debug, Subcommand)]
pub enum SourceCommand {
    /// List bundled sources available in your build
    Discover,

    /// List sources currently installed in the workspace
    List,

    /// Install a bundled source by name
    #[command(after_long_help = "\
Coral prompts for required variables or secrets interactively.

Examples:
  coral source add github")]
    Add {
        /// Name of the bundled source to install
        name: String,
    },

    /// Import a custom source from a manifest file
    #[command(after_long_help = "\
Examples:
  coral source import ./local-messages.yaml")]
    Import {
        /// Path to the source manifest file
        path: PathBuf,
    },

    /// Validate that an installed source can initialize and expose tables
    #[command(after_long_help = "\
Examples:
  coral source test github
  coral source test local_messages")]
    Test {
        /// Name of the source to test
        name: String,
    },

    /// Remove an installed source from the local workspace
    #[command(after_long_help = "\
Examples:
  coral source remove github")]
    Remove {
        /// Name of the source to remove
        name: String,
    },
}

/// Output format for query results
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum OutputFormat {
    /// ASCII table
    Table,
    /// JSON
    Json,
}
