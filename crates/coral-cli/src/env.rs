//! CLI-owned process environment accessors.
//!
//! `coral-cli` is allowed to read process environment when the CLI surface
//! explicitly defines an env-backed workflow.

use std::env::VarError;

const CORAL_ENDPOINT_ENV: &str = "CORAL_ENDPOINT";
const CORAL_AUTH_ENDPOINT_ENV: &str = "CORAL_AUTH_ENDPOINT";
const CORAL_AUTH_TOKEN_ENV: &str = "CORAL_AUTH_TOKEN";

/// A fixed CLI connection environment variable contained non-Unicode data.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[error("environment variable {name} contains non-Unicode data")]
pub(crate) struct ConnectionEnvError {
    name: &'static str,
}

/// Reads the optional remote Coral endpoint without collapsing an explicit
/// empty value into absence.
#[expect(
    clippy::disallowed_methods,
    reason = "CORAL_ENDPOINT is a public CLI connection setting."
)]
pub(crate) fn endpoint() -> Result<Option<String>, ConnectionEnvError> {
    read_connection_env(CORAL_ENDPOINT_ENV, |name| std::env::var(name))
}

/// Reads the optional OAuth authorization server without collapsing an
/// explicit empty value into absence.
#[expect(
    clippy::disallowed_methods,
    reason = "CORAL_AUTH_ENDPOINT is a public CLI login setting."
)]
pub(crate) fn auth_endpoint() -> Result<Option<String>, ConnectionEnvError> {
    read_connection_env(CORAL_AUTH_ENDPOINT_ENV, |name| std::env::var(name))
}

/// Reads the optional bearer token without collapsing an explicit empty value
/// into absence.
#[expect(
    clippy::disallowed_methods,
    reason = "CORAL_AUTH_TOKEN is a public CLI connection setting."
)]
pub(crate) fn auth_token() -> Result<Option<String>, ConnectionEnvError> {
    read_connection_env(CORAL_AUTH_TOKEN_ENV, |name| std::env::var(name))
}

fn read_connection_env(
    name: &'static str,
    read_env: impl FnOnce(&str) -> Result<String, VarError>,
) -> Result<Option<String>, ConnectionEnvError> {
    match read_env(name) {
        Ok(value) => Ok(Some(value)),
        Err(VarError::NotPresent) => Ok(None),
        Err(VarError::NotUnicode(_value)) => Err(ConnectionEnvError { name }),
    }
}

const CORAL_TRACE_PARENT_ENV: &str = "CORAL_TRACE_PARENT";
const CORAL_WORKSPACE_ENV: &str = "CORAL_WORKSPACE";

/// Reads the optional W3C `traceparent` used to link CLI spans to a parent trace.
#[expect(
    clippy::disallowed_methods,
    reason = "CORAL_TRACE_PARENT is a CLI-owned per-invocation distributed tracing seed."
)]
#[must_use]
pub fn trace_parent() -> Option<String> {
    std::env::var(CORAL_TRACE_PARENT_ENV).ok()
}

/// Reads the optional default workspace for CLI commands.
#[expect(
    clippy::disallowed_methods,
    reason = "CORAL_WORKSPACE is a CLI-owned per-invocation workspace selector."
)]
#[must_use]
pub fn workspace() -> Option<String> {
    std::env::var(CORAL_WORKSPACE_ENV)
        .ok()
        .filter(|value| !value.is_empty())
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;

    use super::*;

    #[test]
    fn connection_environment_preserves_missing_empty_and_whitespace_values() {
        assert_eq!(
            read_connection_env(CORAL_ENDPOINT_ENV, |_| Err(VarError::NotPresent)),
            Ok(None)
        );
        assert_eq!(
            read_connection_env(CORAL_ENDPOINT_ENV, |_| Ok(String::new())),
            Ok(Some(String::new()))
        );
        assert_eq!(
            read_connection_env(CORAL_ENDPOINT_ENV, |_| Ok(" \t ".to_string())),
            Ok(Some(" \t ".to_string()))
        );
    }

    #[test]
    fn non_unicode_connection_environment_error_is_value_redacted() {
        let error = read_connection_env(CORAL_AUTH_ENDPOINT_ENV, |name| {
            assert_eq!(name, CORAL_AUTH_ENDPOINT_ENV);
            Err(VarError::NotUnicode(OsString::from(
                "authorization-secret-sentinel",
            )))
        })
        .expect_err("non-Unicode value");

        let rendered = format!("{error:?} {error}");
        assert!(rendered.contains(CORAL_AUTH_ENDPOINT_ENV));
        assert!(!rendered.contains("authorization-secret-sentinel"));
    }
}
