//! Developer tooling for Coral repository automation.
//!
//! This binary exposes two subcommands that share workspace conventions but
//! serve different workflows:
//!   - `generate-docs` regenerates the generator-owned Mintlify pages and
//!     nav from source manifests plus `CHANGELOG.md`.
//!   - `detect-truncations` scans manifests for likely-truncated descriptions
//!     (the regression gate for the SOURCE-465 manifest cleanup).
//!   - `export-skills` exports installable agent skills from the canonical
//!     plugin tree into a distribution checkout.
//!   - `perf-check` runs command-level performance regression checks.
//!   - `generate-schemas` refreshes checked-in generated JSON schemas.
//!   - `virtual-graph-tck-report` summarizes the openCypher compatibility gate.
//!   - `virtual-graph-upstream-tck-report` inventories the upstream openCypher
//!     TCK feature tree against Coral's read-only product scope.
//!   - `virtual-graph-baseline-report` summarizes virtual graph compatibility
//!     fixtures.
//!   - `virtual-graph-graphql-schema-coverage` reports GraphQL schema-driven
//!     read coverage against the engine capability surface.
//!   - `release-macos-sign-notarize` signs and notarizes macOS release
//!     artifacts.

#![allow(
    clippy::print_stderr,
    clippy::print_stdout,
    reason = "CLI intentionally writes human-readable diagnostics to stdout/stderr"
)]

use std::path::PathBuf;
use std::process::ExitCode;

use anyhow::Result;
use clap::{Parser, Subcommand};

mod detect;
mod docs;
mod env;
mod perf;
mod release;
mod schemas;
mod skills;
mod sources;
mod tck;

#[derive(Debug, Parser)]
#[command(
    name = "xtask",
    about = "Developer tooling for Coral repository automation"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Regenerate generator-owned docs pages and Mintlify nav entries.
    GenerateDocs(docs::Args),
    /// Scan manifests for likely-truncated descriptions.
    DetectTruncations(DetectArgs),
    /// Export installable skills from plugins/coral/skills.
    ExportSkills(ExportSkillsArgs),
    /// Run command-level performance regression checks.
    PerfCheck(perf::Args),
    /// Regenerate checked-in generated JSON schemas.
    GenerateSchemas(schemas::Args),
    /// Summarize a virtual graph compatibility baseline fixture.
    VirtualGraphTckReport(tck::Args),
    /// Inventory the upstream openCypher TCK feature tree.
    VirtualGraphUpstreamTckReport(tck::UpstreamArgs),
    /// Summarize a virtual graph compatibility baseline fixture.
    VirtualGraphBaselineReport(tck::Args),
    /// Report GraphQL read coverage against the engine capability surface.
    VirtualGraphGraphqlSchemaCoverage(tck::GraphqlSchemaCoverageArgs),
    /// Sign, package, and notarize one macOS release binary.
    ReleaseMacosSignNotarize(release::MacosSignNotarizeArgs),
}

#[derive(Debug, clap::Args)]
struct DetectArgs {
    /// Manifest files or directories to scan. Defaults to `sources/` when
    /// no paths are given.
    paths: Vec<PathBuf>,

    /// Print one line per manifest scanned, including those with no hits.
    #[arg(long)]
    verbose: bool,
}

#[derive(Debug, clap::Args)]
struct ExportSkillsArgs {
    /// Destination checkout or directory to receive the exported skills.
    #[arg(long)]
    dest: PathBuf,
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    match run(&cli.command) {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::from(1),
        Err(err) => {
            eprintln!("xtask: {err:#}");
            ExitCode::from(2)
        }
    }
}

/// Returns `Ok(true)` on success, `Ok(false)` on a detected regression
/// (stale generated file or suspected truncation).
fn run(command: &Command) -> Result<bool> {
    match command {
        Command::GenerateDocs(args) => docs::run(args),
        Command::DetectTruncations(args) => {
            let paths: Vec<PathBuf> = if args.paths.is_empty() {
                vec![PathBuf::from("sources")]
            } else {
                args.paths.clone()
            };
            detect::run(&paths, args.verbose)
        }
        Command::ExportSkills(args) => skills::export(&args.dest),
        Command::PerfCheck(args) => perf::run(args),
        Command::GenerateSchemas(args) => schemas::run(args),
        Command::VirtualGraphTckReport(args) | Command::VirtualGraphBaselineReport(args) => {
            tck::run(args)
        }
        Command::VirtualGraphUpstreamTckReport(args) => tck::run_upstream(args),
        Command::VirtualGraphGraphqlSchemaCoverage(args) => tck::run_graphql_schema_coverage(args),
        Command::ReleaseMacosSignNotarize(args) => release::macos_sign_notarize(args),
    }
}
