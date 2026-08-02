//! Developer tooling for Coral repository automation.
//!
//! This binary exposes subcommands that share workspace conventions but serve
//! different workflows:
//!   - `generate-docs` regenerates the generator-owned Mintlify pages and
//!     nav from source manifests plus `CHANGELOG.md`.
//!   - `detect-truncations` scans manifests for likely-truncated descriptions
//!     (the regression gate for the SOURCE-465 manifest cleanup).
//!   - `export-skills` exports installable agent skills from the canonical
//!     plugin tree into a distribution checkout.
//!   - `perf-check` runs command-level performance regression checks.
//!   - `benchmark` runs developer benchmarks.
//!   - `generate-schemas` refreshes checked-in generated JSON schemas.
//!   - `release-macos-sign-notarize` signs and notarizes macOS release
//!     artifacts.
//!   - `release-desktop-macos-package` packages, signs, notarizes, and verifies
//!     the prepared macOS desktop app.
//!   - `openapi-hydrate` produces a self-contained JSON `OpenAPI` descriptor.
//!   - `v4-metadata-report` reports inferred row paths and pagination contracts
//!     for the v4 source catalog, for diffing across inference changes.

#![allow(
    clippy::print_stderr,
    clippy::print_stdout,
    reason = "CLI intentionally writes human-readable diagnostics to stdout/stderr"
)]

use std::path::PathBuf;
use std::process::ExitCode;

use anyhow::Result;
use clap::{Parser, Subcommand};

#[cfg(test)]
use assert_cmd as _;

mod benchmarks;
mod detect;
mod docs;
mod env;
mod metadata_report;
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
    /// Run command-level performance regression checks.
    PerfCheck(perf::Args),
    /// Run developer benchmarks.
    Benchmark(benchmarks::Args),
    /// Regenerate checked-in generated JSON schemas.
    GenerateSchemas(schemas::Args),
    /// Sign, package, and notarize one macOS release binary.
    ReleaseMacosSignNotarize(release::MacosSignNotarizeArgs),
    /// Package, sign, notarize, and verify the prepared macOS desktop app.
    ReleaseDesktopMacosPackage(release::DesktopMacosPackageArgs),
    /// Hydrate reachable external `OpenAPI` references into JSON.
    OpenapiHydrate(openapi::HydrateArgs),
    /// Report inferred row paths and pagination contracts for v4 sources.
    V4MetadataReport(metadata_report::Args),
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
        Command::Benchmark(args) => benchmarks::run(args),
        Command::GenerateSchemas(args) => schemas::run(args),
        Command::ReleaseMacosSignNotarize(args) => release::macos_sign_notarize(args),
        Command::ReleaseDesktopMacosPackage(args) => release::desktop_macos_package(args),
        Command::OpenapiHydrate(args) => openapi::hydrate(args),
        Command::V4MetadataReport(args) => metadata_report::run(args),
    }
}
