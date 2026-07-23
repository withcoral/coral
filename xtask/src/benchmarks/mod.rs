//! Representation-efficiency benchmarks.

mod list_columns;

use anyhow::Result;
use clap::Subcommand;

#[derive(Debug, clap::Args)]
pub(crate) struct Args {
    #[command(subcommand)]
    benchmark: Benchmark,
}

#[derive(Debug, Subcommand)]
enum Benchmark {
    /// Compare verbose and positional `list_columns` JSON token counts.
    ListColumns(list_columns::Args),
}

pub(crate) fn run(args: &Args) -> Result<bool> {
    match &args.benchmark {
        Benchmark::ListColumns(args) => list_columns::run(args),
    }
}
