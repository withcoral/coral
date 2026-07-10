//! CLI-owned process environment accessors.
//!
//! `coral-cli` is allowed to read process environment when the CLI surface
//! explicitly defines an env-backed workflow.

const CORAL_ENDPOINT_ENV: &str = "CORAL_ENDPOINT";
const CORAL_AUTH_TOKEN_ENV: &str = "CORAL_AUTH_TOKEN";

/// Resolved endpoint and bearer credential for one CLI invocation.
#[derive(Default)]
pub(crate) struct ConnectionOptions {
    pub(crate) endpoint: Option<String>,
    pub(crate) token: Option<String>,
}

/// Resolves CLI connection flags over their environment fallbacks.
#[expect(
    clippy::disallowed_methods,
    reason = "CORAL_ENDPOINT and CORAL_AUTH_TOKEN are public CLI connection settings."
)]
#[must_use]
pub(crate) fn connection_options(
    cli_endpoint: Option<String>,
    cli_token: Option<String>,
) -> ConnectionOptions {
    resolve_connection_options(cli_endpoint, cli_token, |name| std::env::var(name).ok())
}

fn resolve_connection_options(
    cli_endpoint: Option<String>,
    cli_token: Option<String>,
    mut read_env: impl FnMut(&str) -> Option<String>,
) -> ConnectionOptions {
    ConnectionOptions {
        endpoint: resolve_value(cli_endpoint, CORAL_ENDPOINT_ENV, &mut read_env),
        token: resolve_value(cli_token, CORAL_AUTH_TOKEN_ENV, &mut read_env),
    }
}

fn resolve_value(
    cli_value: Option<String>,
    env_name: &str,
    read_env: &mut impl FnMut(&str) -> Option<String>,
) -> Option<String> {
    match cli_value {
        Some(value) => non_empty_trimmed(&value),
        None => read_env(env_name).and_then(|value| non_empty_trimmed(&value)),
    }
}

fn non_empty_trimmed(value: &str) -> Option<String> {
    let value = value.trim();
    (!value.is_empty()).then(|| value.to_string())
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
    use super::*;

    #[test]
    fn connection_flags_override_environment() {
        let options = resolve_connection_options(
            Some(" https://flag.example ".to_string()),
            Some(" flag-token ".to_string()),
            |name| match name {
                CORAL_ENDPOINT_ENV => Some("https://env.example".to_string()),
                CORAL_AUTH_TOKEN_ENV => Some("env-token".to_string()),
                _ => None,
            },
        );

        assert_eq!(options.endpoint.as_deref(), Some("https://flag.example"));
        assert_eq!(options.token.as_deref(), Some("flag-token"));
    }

    #[test]
    fn empty_explicit_token_suppresses_environment_token() {
        let options = resolve_connection_options(None, Some("  ".to_string()), |name| {
            (name == CORAL_AUTH_TOKEN_ENV).then(|| "env-token".to_string())
        });

        assert_eq!(options.token, None);
    }

    #[test]
    fn whitespace_environment_values_are_unset() {
        let options = resolve_connection_options(None, None, |_| Some(" \t\n ".to_string()));

        assert_eq!(options.endpoint, None);
        assert_eq!(options.token, None);
    }
}
