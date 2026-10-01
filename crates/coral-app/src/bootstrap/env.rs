//! App-owned environment accessors for local runtime setup.

use std::env::VarError;
use std::path::PathBuf;

use coral_engine::QueryRuntimeContext;

use super::consts::CORAL_CONFIG_DIR;
use super::error::AppError;
use crate::state::AppStateLayout;

#[derive(Debug, Clone, Default)]
pub(crate) struct AppEnvironment {
    coral_config_dir_override: Option<PathBuf>,
    user_home_dir: Option<PathBuf>,
}

impl AppEnvironment {
    pub(crate) fn discover() -> Self {
        Self {
            coral_config_dir_override: coral_config_dir_override(),
            user_home_dir: etcetera::home_dir().ok(),
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

    pub(crate) fn query_runtime_context(&self) -> QueryRuntimeContext {
        QueryRuntimeContext {
            home_dir: self.user_home_dir.clone(),
            ..QueryRuntimeContext::default()
        }
    }

    pub(crate) fn env_var(name: &str) -> Result<Option<String>, VarError> {
        env_var(name)
    }
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

#[cfg(test)]
mod tests {
    use super::{AppEnvironment, CORAL_CONFIG_DIR, coral_config_dir_override};
    use std::path::PathBuf;

    #[test]
    #[expect(
        clippy::disallowed_methods,
        reason = "This test intentionally controls CORAL_CONFIG_DIR to validate the app-owned accessor."
    )]
    fn coral_config_dir_override_reads_env_once_through_app_accessor() {
        if std::env::var_os("CORAL_RUN_CORAL_CONFIG_DIR_TEST").is_some() {
            let expected = std::env::var_os(CORAL_CONFIG_DIR)
                .map(PathBuf::from)
                .expect("CORAL_CONFIG_DIR should be set in subprocess");
            assert_eq!(
                coral_config_dir_override().as_deref(),
                Some(expected.as_path())
            );
            let env = AppEnvironment::discover();
            assert_eq!(
                env.coral_config_dir_override().as_deref(),
                Some(expected.as_path())
            );
            return;
        }

        let override_dir =
            std::env::temp_dir().join(format!("coral-config-dir-override-{}", std::process::id()));
        let status = std::process::Command::new(std::env::current_exe().expect("current exe"))
            .env("CORAL_RUN_CORAL_CONFIG_DIR_TEST", "1")
            .env(CORAL_CONFIG_DIR, override_dir)
            .arg("--exact")
            .arg(
                "bootstrap::env::tests::coral_config_dir_override_reads_env_once_through_app_accessor",
            )
            .arg("--nocapture")
            .status()
            .expect("run subprocess");
        assert!(status.success(), "subprocess should pass");
    }

    #[cfg(unix)]
    #[test]
    #[expect(
        clippy::disallowed_methods,
        reason = "This test intentionally controls process environment values to validate the app-owned accessor."
    )]
    fn env_var_preserves_non_utf8_errors() {
        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt as _;

        const RUN_FLAG: &str = "CORAL_RUN_NON_UTF8_ENV_TEST";
        const VALUE_ENV: &str = "CORAL_NON_UTF8_ENV_TEST";

        if std::env::var_os(RUN_FLAG).is_some() {
            let error = AppEnvironment::env_var(VALUE_ENV)
                .expect_err("non-UTF8 env var should be reported");
            assert!(matches!(error, std::env::VarError::NotUnicode(_)));
            return;
        }

        let status = std::process::Command::new(std::env::current_exe().expect("current exe"))
            .env(RUN_FLAG, "1")
            .env(VALUE_ENV, OsString::from_vec(vec![0xFF]))
            .arg("--exact")
            .arg("bootstrap::env::tests::env_var_preserves_non_utf8_errors")
            .arg("--nocapture")
            .status()
            .expect("run subprocess");
        assert!(status.success(), "subprocess should pass");
    }
}
