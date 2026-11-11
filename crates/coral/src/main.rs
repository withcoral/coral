//! The `coral` executable that a plain `cargo build` produces. It runs as the
//! Community Edition and holds no `ee/` code.

#![allow(
    unused_crate_dependencies,
    reason = "the library target uses the package's dependencies"
)]

use std::process::ExitCode;

fn main() -> ExitCode {
    coral::run(coral_app::Extensions::default())
}
