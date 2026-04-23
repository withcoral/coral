//! CLI-owned process environment accessors.
//!
//! `coral-cli` is allowed to read process environment when the CLI surface
//! explicitly defines an env-backed workflow, such as test-only bootstrap
//! overrides today and install-time input collection for source setup.

#![allow(
    clippy::disallowed_methods,
    reason = "coral-cli owns CLI-surface process environment access in this module."
)]

/// Reads a CLI-owned UTF-8 environment variable.
#[must_use]
pub fn var(key: &str) -> Option<String> {
    std::env::var(key).ok()
}

#[cfg(feature = "cli-test-server")]
const CORAL_ENDPOINT_ENV: &str = "CORAL_ENDPOINT";

/// Reads the feature-gated endpoint override used by CLI integration tests.
#[cfg(feature = "cli-test-server")]
#[must_use]
pub fn bootstrap_endpoint() -> Option<String> {
    std::env::var_os(CORAL_ENDPOINT_ENV)
        .and_then(|value| value.into_string().ok())
        .filter(|value| !value.is_empty())
}
