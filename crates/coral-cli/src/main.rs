//! `CLI` entrypoint for the local Coral app.

#![allow(
    clippy::print_stderr,
    unused_crate_dependencies,
    reason = "The thin binary delegates command logic to the shared coral-cli library and owns stderr rendering for exit paths."
)]

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let result = coral_cli::run_from_env().await;
    let _shutdown_result = tokio::task::spawn_blocking(coral_app::shutdown_tracing).await;
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
