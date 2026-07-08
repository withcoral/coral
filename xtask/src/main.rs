//! Developer tooling for Coral repository automation.
//!
//! This binary exposes subcommands that share workspace conventions but
//! serve different workflows:
//!   - `generate-docs` regenerates the generator-owned Mintlify pages and
//!     nav from source manifests plus `CHANGELOG.md`.
//!   - `detect-truncations` scans manifests for likely-truncated descriptions
//!     (the regression gate for the SOURCE-465 manifest cleanup).
//!   - `export-skills` exports installable agent skills from the canonical
//!     plugin tree into a distribution checkout.
//!   - `export-openapi` converts v3 HTTP source manifests into OpenAPI
//!     documents for the DSL v4 migration.
//!   - `perf-check` runs command-level performance regression checks.
//!   - `generate-schemas` refreshes checked-in generated JSON schemas.
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
mod openapi;
mod perf;
mod release;
mod schemas;
mod skills;
mod sources;

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
    /// Convert v3 HTTP source manifests to OpenAPI 3.0 documents.
    ExportOpenapi(openapi::Args),
    /// Run command-level performance regression checks.
    PerfCheck(perf::Args),
    /// Regenerate checked-in generated JSON schemas.
    GenerateSchemas(schemas::Args),
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
        Command::ExportOpenapi(args) => openapi::run(args),
        Command::PerfCheck(args) => perf::run(args),
        Command::GenerateSchemas(args) => schemas::run(args),
        Command::ReleaseMacosSignNotarize(args) => release::macos_sign_notarize(args),
    }
}
