//! CLI-owned process environment accessors.
//!
//! `coral-cli` is allowed to read process environment when the CLI surface
//! explicitly defines an env-backed workflow.

#[cfg(feature = "cli-test-server")]
const CORAL_ENDPOINT_ENV: &str = "CORAL_ENDPOINT";

/// Reads the feature-gated endpoint override used by CLI integration tests.
#[cfg(feature = "cli-test-server")]
#[expect(
    clippy::disallowed_methods,
    reason = "This feature-gated test hook owns the CORAL_ENDPOINT bootstrap override."
)]
#[must_use]
pub fn bootstrap_endpoint() -> Option<String> {
    std::env::var_os(CORAL_ENDPOINT_ENV)
        .and_then(|value| value.into_string().ok())
        .filter(|value| !value.is_empty())
}

#[cfg(test)]
const CORAL_REQUIRE_UI_DIST_ENV: &str = "CORAL_REQUIRE_UI_DIST";

/// Reads the release-guard flag used by the embedded-UI smoke test.
#[cfg(test)]
#[expect(
    clippy::disallowed_methods,
    reason = "This test-only guard reads the release validation env var owned by coral-cli."
)]
#[must_use]
pub fn require_ui_dist_guard() -> bool {
    std::env::var_os(CORAL_REQUIRE_UI_DIST_ENV).is_some()
}

const CORAL_TRACE_PARENT_ENV: &str = "CORAL_TRACE_PARENT";

/// Reads the optional W3C `traceparent` used to link CLI spans to a parent trace.
#[expect(
    clippy::disallowed_methods,
    reason = "CORAL_TRACE_PARENT is a CLI-owned per-invocation distributed tracing seed."
)]
#[must_use]
pub fn trace_parent() -> Option<String> {
    std::env::var(CORAL_TRACE_PARENT_ENV).ok()
}
