//! `openapi` command-line entrypoint.

#![allow(
    unused_crate_dependencies,
    reason = "The thin binary delegates hydration logic to the openapi-tools library."
)]

use std::io::{self, Write as _};
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use openapi::hydrate_openapi_from_location;

#[derive(Debug, Parser)]
#[command(version, about = "OpenAPI document utilities")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Hydrate reachable external `OpenAPI` references.
    Hydrate {
        /// HTTPS URL or local file path to the `OpenAPI` descriptor.
        location: String,
    },
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            if writeln!(io::stderr().lock(), "error: {error}").is_err() {
                return ExitCode::FAILURE;
            }
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    match cli.command {
        Command::Hydrate { location } => {
            let value = hydrate_openapi_from_location(&location)?;
            serde_json::to_writer_pretty(io::stdout().lock(), &value)?;
            writeln!(io::stdout().lock())?;
        }
    }
    Ok(())
}
