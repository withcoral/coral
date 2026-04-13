//! Internal CLI runner and adapters for Coral.

#![allow(
    missing_docs,
    reason = "This library target exists to expose the CLI implementation to internal tests."
)]
#![allow(
    unused_crate_dependencies,
    reason = "The package dependency set is shared by the CLI library and binary targets."
)]

mod branding;
mod cli;
mod host;
mod onboard;
mod runner;
mod services;
mod source_ops;

pub use cli::{Cli, Command, OutputFormat, SourceAddArgs, SourceArgs, SourceCommand, SqlArgs};
pub use host::{CliHost, CliPrompter, DialoguerCliPrompter, RealCliHost};
pub use runner::run;
pub use services::CliServices;
