//! App-owned environment accessors for local runtime setup.

use std::env::VarError;
use std::path::PathBuf;

use super::consts::{
    CORAL_CONFIG_DIR, CORAL_DESKTOP, CORAL_LOCAL_WEB_URL, CORAL_STOP_ON_STDIN_CLOSE,
    CORAL_TRACE_PARENT,
};
use super::error::AppError;
use super::runtime::Launch;
use crate::state::AppStateLayout;
use crate::telemetry::RunContext;

#[derive(Debug, Clone, Default)]
pub(crate) struct AppEnvironment {
    coral_config_dir_override: Option<PathBuf>,
}

impl AppEnvironment {
    pub(crate) fn discover() -> Self {
        Self {
            coral_config_dir_override: coral_config_dir_override(),
        }
    }

    pub(crate) fn coral_config_dir_override(&self) -> Option<PathBuf> {
        self.coral_config_dir_override.clone()
    }

    pub(crate) fn app_state_layout(
        &self,
        config_dir_override: Option<PathBuf>,
    ) -> Result<AppStateLayout, AppError> {
        AppStateLayout::discover(config_dir_override.or_else(|| self.coral_config_dir_override()))
    }

    pub(crate) fn env_var(name: &str) -> Result<Option<String>, VarError> {
        env_var(name)
    }

    /// Reads the environment variable `name`, which the configuration setting
    /// `setting` (for example `credentials.key_env`) selected. The value is
    /// required: if the variable is unset, or its value is not valid UTF-8,
    /// startup is refused with an error that names both the setting and the
    /// variable, so the operator knows which line of `config.toml` to check.
    pub(crate) fn required_env_var(setting: &str, name: &str) -> Result<String, AppError> {
        env_var(name)
            .map_err(|_not_unicode| {
                AppError::FailedPrecondition(format!(
                    "{setting} requires environment variable `{name}` to contain valid UTF-8"
                ))
            })?
            .ok_or_else(|| {
                AppError::FailedPrecondition(format!(
                    "{setting} requires environment variable `{name}`"
                ))
            })
    }
}

/// Trims the environment variable name a deployment setting holds. A blank
/// name, or one that `std::env::var` would panic on (`=` or NUL), is a
/// configuration error that names `setting`; the caller wraps the message in
/// its own error type.
pub(crate) fn env_var_name(setting: &str, configured: &str) -> Result<String, String> {
    let name = configured.trim();
    if name.is_empty() {
        return Err(format!("{setting} must not be empty"));
    }
    if name.contains(['=', '\0']) {
        return Err(format!("{setting} must not contain '=' or NUL"));
    }
    Ok(name.to_owned())
}

#[expect(
    clippy::disallowed_methods,
    reason = "coral-app is the single owner of process environment access."
)]
fn coral_config_dir_override() -> Option<PathBuf> {
    std::env::var_os(CORAL_CONFIG_DIR).map(PathBuf::from)
}

#[expect(
    clippy::disallowed_methods,
    reason = "coral-app is the single owner of process environment access."
)]
fn env_var(name: &str) -> Result<Option<String>, VarError> {
    match std::env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(VarError::NotPresent) => Ok(None),
        Err(error @ VarError::NotUnicode(_)) => Err(error),
    }
}

impl Launch {
    /// Reads what the launching process set for this launch.
    ///
    /// Desktop sets `CORAL_DESKTOP=1`. A launcher that stops the runtime by
    /// closing its stdin sets `CORAL_STOP_ON_STDIN_CLOSE=1`; it is opt-in
    /// because a container without an attached stdin starts with stdin closed.
    /// `CORAL_LOCAL_WEB_URL` is a local launcher's web origin.
    ///
    /// # Errors
    ///
    /// Returns a message naming the variable when `CORAL_LOCAL_WEB_URL` is not
    /// Unicode or not a valid human interface origin.
    pub fn from_env() -> Result<Self, String> {
        let local_web_url = match env_var(CORAL_LOCAL_WEB_URL) {
            Ok(Some(value)) => Some(
                coral_mcp::HumanInterfaceUrl::parse(&value)
                    .map_err(|error| format!("{CORAL_LOCAL_WEB_URL} {error}"))?,
            ),
            Ok(None) => None,
            Err(_not_unicode) => return Err(format!("{CORAL_LOCAL_WEB_URL} must be Unicode")),
        };
        Ok(Self {
            desktop: flag(CORAL_DESKTOP),
            local_web_url,
            stop_on_stdin_close: flag(CORAL_STOP_ON_STDIN_CLOSE),
            service_version: None,
        })
    }
}

impl RunContext {
    /// Reads the optional W3C `traceparent` that links the process span to a
    /// parent trace.
    #[must_use]
    pub fn from_env() -> Self {
        Self {
            trace_parent: env_var(CORAL_TRACE_PARENT).ok().flatten(),
        }
    }
}

fn flag(name: &str) -> bool {
    matches!(env_var(name).as_ref().map(Option::as_deref), Ok(Some("1")))
}

#[cfg(test)]
mod tests {
    use super::env_var_name;

    #[test]
    fn env_var_names_are_trimmed_and_fail_closed() {
        assert_eq!(
            env_var_name("credentials.key_env", " CORAL_CREDENTIALS_KEY \n").expect("valid name"),
            "CORAL_CREDENTIALS_KEY"
        );
        for (configured, expected) in [
            ("", "credentials.key_env must not be empty"),
            (" \t", "credentials.key_env must not be empty"),
            (
                "CORAL=KEY",
                "credentials.key_env must not contain '=' or NUL",
            ),
            (
                "CORAL\0KEY",
                "credentials.key_env must not contain '=' or NUL",
            ),
        ] {
            assert_eq!(
                env_var_name("credentials.key_env", configured).expect_err(configured),
                expected,
                "{configured:?}"
            );
        }
    }
}
