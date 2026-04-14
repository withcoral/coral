//! Thin binary entrypoint for the shared Coral CLI implementation.

#![cfg_attr(
    test,
    allow(
        unused_crate_dependencies,
        reason = "Dev-dependencies (e.g. tempfile) are only consumed by integration tests under `tests/`."
    )
)]

use arrow as _;
use clap as _;
use coral_api as _;
use coral_client as _;
use coral_mcp as _;
use coral_spec as _;
use dialoguer as _;
#[cfg(test)]
use tempfile as _;
use tokio as _;
use tonic as _;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    coral_cli::run().await
}
