//! `openapi-hydrate` xtask subcommand.

use std::io::{self, Write as _};

use anyhow::Result;

mod lib;

use lib::hydrate_openapi_from_location;

/// Arguments for the `openapi-hydrate` subcommand.
#[derive(Debug, clap::Args)]
pub(crate) struct HydrateArgs {
    /// HTTPS URL or local file path to the `OpenAPI` descriptor.
    location: String,
}

/// Hydrate the descriptor and write pretty JSON to standard output.
pub(crate) fn hydrate(args: &HydrateArgs) -> Result<bool> {
    let value = hydrate_openapi_from_location(&args.location)?;
    serde_json::to_writer_pretty(io::stdout().lock(), &value)?;
    writeln!(io::stdout().lock())?;
    Ok(true)
}
