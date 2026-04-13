//! `CLI` entrypoint for the local Coral app.

#![allow(
    unused_crate_dependencies,
    reason = "The binary target delegates almost all implementation to the library target."
)]

use clap::Parser;
use coral_cli::{Cli, CliServices, DialoguerCliPrompter, RealCliHost, run};

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let cli = Cli::parse();
    let mut services = CliServices::connect_local().await?;
    let mut host = RealCliHost::new();
    let mut prompts = DialoguerCliPrompter::new();
    run(cli, &mut services, &mut host, &mut prompts).await
}
