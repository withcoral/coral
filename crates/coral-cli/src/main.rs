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
    let bootstrap::Bootstrap { app, _server } = bootstrap().await?;
    match coral_cli::run(app).await {
        Ok(()) => Ok(()),
        Err(error) => {
            if let Some(cli_error) = error.downcast_ref::<coral_cli::CliExitError>() {
                eprint!("{}", cli_error.rendered_stderr());
                std::process::exit(1);
            }
            Err(error)
        }
    }
}
