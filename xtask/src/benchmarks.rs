//! Representation-efficiency benchmark dispatch.

use std::process::Command;

use anyhow::Context;
use anyhow::Result;
use clap::Subcommand;

#[derive(Debug, clap::Args)]
pub(crate) struct Args {
    #[command(subcommand)]
    benchmark: Benchmark,
}

#[derive(Debug, Subcommand)]
enum Benchmark {
    /// Measure the token cost of the current `list_columns` response.
    ListColumns,
}

pub(crate) fn run(args: &Args) -> Result<bool> {
    match &args.benchmark {
        Benchmark::ListColumns => {
            let status = Command::new("cargo")
                .args([
                    "run",
                    "--locked",
                    "-p",
                    "coral-benchmarks",
                    "--",
                    "list-columns",
                ])
                .status()
                .context("running the list_columns efficiency benchmark")?;
            Ok(status.success())
        }
    }
}
