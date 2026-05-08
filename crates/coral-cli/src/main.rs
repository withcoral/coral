//! `CLI` entrypoint for the local Coral app.

#![allow(
    clippy::print_stderr,
    unused_crate_dependencies,
    reason = "The thin binary delegates command logic to the shared coral-cli library and owns stderr rendering for exit paths."
)]

mod bootstrap;

use bootstrap::bootstrap;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let bootstrap = bootstrap(coral_cli::enables_stderr_logs()).await?;
    let ctx = coral_app::RunContext {
        trace_parent: coral_cli::env::trace_parent(),
    };
    let result = coral_cli::run(bootstrap.app.clone(), ctx).await;
    bootstrap.shutdown().await;
    tokio::task::spawn_blocking(coral_app::shutdown_tracing)
        .await
        .ok();
    match result {
        Ok(()) => Ok(()),
        Err(error) => {
            if let Some(rendered_stderr) = error.rendered_stderr() {
                eprint!("{rendered_stderr}");
                std::process::exit(1);
            }
            Err(error.into())
        }
    }
}
