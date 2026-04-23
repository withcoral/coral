//! CLI-owned process environment accessors.
//!
//! `coral-cli` is allowed to read process environment when the CLI surface
//! explicitly defines an env-backed workflow.

#[cfg(feature = "cli-test-server")]
const CORAL_ENDPOINT_ENV: &str = "CORAL_ENDPOINT";

/// Reads the feature-gated endpoint override used by CLI integration tests.
#[cfg(feature = "cli-test-server")]
#[allow(
    clippy::disallowed_methods,
    reason = "This feature-gated test hook owns the CORAL_ENDPOINT bootstrap override."
)]
#[must_use]
pub fn bootstrap_endpoint() -> Option<String> {
    std::env::var_os(CORAL_ENDPOINT_ENV)
        .and_then(|value| value.into_string().ok())
        .filter(|value| !value.is_empty())
}
