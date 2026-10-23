//! Developer tooling for Coral repository automation.
//!
//! This binary exposes subcommands that share workspace conventions but serve
//! different workflows:
//!   - `release-desktop-*` check, package, sign, and publish Desktop releases
//!     to the v1 update feed.
//!   - `openapi-hydrate` produces a self-contained JSON OpenAPI descriptor.

#![allow(
    clippy::print_stderr,
    clippy::print_stdout,
    reason = "CLI intentionally writes human-readable diagnostics to stdout/stderr"
)]

use std::process::ExitCode;

use anyhow::Result;
use clap::{Parser, Subcommand};

#[cfg(test)]
use assert_cmd as _;

mod desktop_release;
mod env;
mod openapi;
mod proto;
mod release;

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
    /// Generate private Rust protobuf bindings (requires Buf).
    GenerateProto {
        /// Fail rather than write when checked-in bindings have drifted.
        #[arg(long)]
        check: bool,
    },
    /// Check that the release tag, the runtime and Desktop name one version.
    ReleaseDesktopCheckVersion(desktop_release::CheckVersionArgs),
    /// Package, sign, and verify the staged Desktop build for one platform.
    ReleaseDesktopPackage(desktop_release::PackageArgs),
    /// Upload verified Desktop assets to a draft release in the v1 feed.
    ReleaseDesktopPublish(desktop_release::PublishArgs),
    /// Publish the complete draft, so updaters see it.
    ReleaseDesktopPromote(desktop_release::FeedReleaseArgs),
    /// Hydrate reachable external OpenAPI references into JSON.
    OpenapiHydrate(openapi::HydrateArgs),
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

/// Returns `Ok(true)` on success and `Ok(false)` on a detected regression.
fn run(command: &Command) -> Result<bool> {
    match command {
        Command::GenerateProto { check } => proto::generate(*check),
        Command::ReleaseDesktopCheckVersion(args) => desktop_release::check_version(args),
        Command::ReleaseDesktopPackage(args) => desktop_release::package(args),
        Command::ReleaseDesktopPublish(args) => desktop_release::publish(args),
        Command::ReleaseDesktopPromote(args) => desktop_release::promote(args),
        Command::OpenapiHydrate(args) => openapi::hydrate(args),
    }
}
