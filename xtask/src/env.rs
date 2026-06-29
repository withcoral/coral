//! xtask-owned process environment accessors.

use std::path::PathBuf;

use anyhow::{Context, Result};

#[expect(
    clippy::disallowed_methods,
    reason = "xtask owns process environment access for repository automation."
)]
pub(crate) fn required_var(name: &str) -> Result<String> {
    std::env::var(name).with_context(|| format!("{name} is required"))
}

#[expect(
    clippy::disallowed_methods,
    reason = "xtask owns process environment access for repository automation."
)]
pub(crate) fn home_dir() -> Result<PathBuf> {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .context("HOME is required")
}
