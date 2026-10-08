//! Benchmark dispatch into the isolated `coral-benchmarks` package.

use std::ffi::OsString;
use std::process::Command;

use anyhow::{Context, Result};
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
    /// Build, collect, replay, and report Universal Search relevance corpora.
    #[command(disable_help_flag = true)]
    UniversalSearch {
        /// Arguments forwarded to the Universal Search benchmark.
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        args: Vec<OsString>,
    },
}

pub(crate) fn run(args: &Args) -> Result<bool> {
    match &args.benchmark {
        Benchmark::ListColumns => run_benchmark("list-columns", &[]),
        Benchmark::UniversalSearch { args } => run_benchmark("universal-search", args),
    }
}

fn run_benchmark(name: &str, args: &[OsString]) -> Result<bool> {
    let status = Command::new("cargo")
        .args(["run", "--locked", "-p", "coral-benchmarks", "--", name])
        .args(args)
        .status()
        .with_context(|| format!("running the {name} benchmark"))?;
    Ok(status.success())
}
