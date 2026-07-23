//! Token-efficiency benchmark for the MCP `list_columns` result.

use std::path::Path;
use std::process::Command;

use anyhow::{Context, Result};

const BENCHMARK_TEST: &str = "surface::catalog::tests::list_columns_token_efficiency_benchmark";

pub(crate) fn run() -> Result<bool> {
    let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xtask is inside the workspace");
    let status = Command::new("cargo")
        .current_dir(workspace_root)
        .args([
            "test",
            "--quiet",
            "--locked",
            "-p",
            "coral-mcp",
            "--lib",
            BENCHMARK_TEST,
            "--",
            "--exact",
            "--nocapture",
        ])
        .status()
        .context("running the coral-mcp list_columns benchmark")?;
    Ok(status.success())
}
