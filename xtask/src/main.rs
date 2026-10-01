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
//!   - `openapi-hydrate` produces a self-contained JSON OpenAPI descriptor.
//!   - `v4-metadata-report` reports inferred row paths and pagination contracts
//!     for the v4 source catalog, for diffing across inference changes.
//!
//! One further subcommand, `workspace-admin`, exists only when the
//! off-by-default `admin` feature is enabled. The default build neither
//! compiles its module nor carries its command variant, so `xtask --help` on a
//! shipped checkout offers no recovery surface at all.

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

#[cfg(feature = "admin")]
mod admin;
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
    /// Hydrate reachable external OpenAPI references into JSON.
    OpenapiHydrate(openapi::HydrateArgs),
    /// Report inferred row paths and pagination contracts for v4 sources.
    V4MetadataReport(metadata_report::Args),
    /// Repair workspace ownership directly in a deployment's state database.
    ///
    /// Gated on the `admin` feature: without it this variant does not exist,
    /// so the parser rejects `workspace-admin` as an unknown subcommand and
    /// the help text never names it.
    #[cfg(feature = "admin")]
    WorkspaceAdmin(admin::Args),
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
        #[cfg(feature = "admin")]
        Command::WorkspaceAdmin(args) => admin::run(args),
    }
}

/// Command-surface coverage for the feature-gated recovery subcommand.
///
/// The tests run the built binary rather than inspecting `cfg` or the derived
/// clap model, because what has to hold is a property of the shipped artifact:
/// a default `xtask` must offer no recovery surface at all. Each direction is
/// compiled into exactly the build that can observe it, so the negative test
/// cannot pass vacuously in a build where the command exists.
#[cfg(test)]
mod workspace_admin_cli {
    /// The binary this test package builds, invoked as an operator would.
    fn xtask() -> assert_cmd::Command {
        assert_cmd::Command::cargo_bin("xtask").expect("the xtask binary is built for its tests")
    }

    /// Stdout of a successful invocation, as UTF-8.
    fn stdout(output: &std::process::Output) -> String {
        String::from_utf8(output.stdout.clone()).expect("help is UTF-8")
    }

    /// Stderr of an invocation, as UTF-8.
    fn stderr(output: &std::process::Output) -> String {
        String::from_utf8(output.stderr.clone()).expect("diagnostics are UTF-8")
    }

    /// A default build must not name, document, or accept the recovery
    /// subcommand: the feature's entire security posture is that possession of
    /// a shipped checkout grants no lock-out override.
    #[cfg(not(feature = "admin"))]
    #[test]
    fn default_build_offers_no_recovery_surface() {
        let help = xtask().arg("--help").output().expect("run xtask --help");
        assert!(help.status.success(), "xtask --help must succeed");
        let rendered = stdout(&help);
        for absent in [
            "workspace-admin",
            "list-workspaces",
            "list-users",
            "set-owner",
            "rebind-issuer",
        ] {
            assert!(
                !rendered.contains(absent),
                "default xtask help names `{absent}`:\n{rendered}"
            );
        }

        let rejected = xtask()
            .args(["workspace-admin", "list-workspaces"])
            .output()
            .expect("run the recovery subcommand");
        assert!(
            !rejected.status.success(),
            "a default build must reject `workspace-admin`, but it ran:\n{}",
            stdout(&rejected)
        );
        let refusal = stderr(&rejected);
        assert!(
            refusal.contains("unrecognized subcommand"),
            "the refusal must read as an unknown subcommand rather than a runtime failure:\n{refusal}"
        );
    }

    /// An admin build must document every argument the Program Design's
    /// recovery syntax requires, so an operator can drive it from `--help`
    /// alone.
    #[cfg(feature = "admin")]
    #[test]
    fn admin_build_documents_every_recovery_argument() {
        let top = xtask().arg("--help").output().expect("run xtask --help");
        assert!(
            stdout(&top).contains("workspace-admin"),
            "an admin build must offer `workspace-admin`:\n{}",
            stdout(&top)
        );

        let group = xtask()
            .args(["workspace-admin", "--help"])
            .output()
            .expect("run the recovery help");
        let rendered = stdout(&group);
        for subcommand in [
            "list-workspaces",
            "list-users",
            "set-owner",
            "rebind-issuer",
        ] {
            assert!(
                rendered.contains(subcommand),
                "`workspace-admin --help` omits `{subcommand}`:\n{rendered}"
            );
        }

        for (subcommand, arguments) in [
            ("list-users", vec!["--show-subjects"]),
            ("set-owner", vec!["--workspace", "--user"]),
            ("rebind-issuer", vec!["--from", "--to"]),
        ] {
            let help = xtask()
                .args(["workspace-admin", subcommand, "--help"])
                .output()
                .expect("run a recovery subcommand's help");
            let rendered = stdout(&help);
            for argument in arguments {
                assert!(
                    rendered.contains(argument),
                    "`workspace-admin {subcommand} --help` omits `{argument}`:\n{rendered}"
                );
            }
        }
    }

    /// A missing required argument must be refused by the parser, before any
    /// state database is opened, and must say which argument is missing.
    #[cfg(feature = "admin")]
    #[test]
    fn admin_build_names_the_argument_an_incomplete_repair_omits() {
        let incomplete = xtask()
            .args(["workspace-admin", "set-owner", "--workspace", "abandoned"])
            .output()
            .expect("run an incomplete repair");
        assert!(
            !incomplete.status.success(),
            "an incomplete repair must not run"
        );
        let refusal = stderr(&incomplete);
        assert!(
            refusal.contains("--user"),
            "the refusal must name the missing argument:\n{refusal}"
        );
    }
}
