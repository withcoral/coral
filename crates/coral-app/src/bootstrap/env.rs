//! App-owned environment accessors for local runtime setup.

use std::ffi::OsString;
use std::path::PathBuf;

use coral_sql::QueryRuntimeContext;

use super::consts::{CORAL_CONFIG_DIR, CORAL_RUNTIME_EXPOSURE};
use super::error::AppError;
use crate::RuntimeExposureMode;
use crate::state::AppStateLayout;

#[derive(Debug, Clone, Default)]
pub(crate) struct AppEnvironment {
    coral_config_dir_override: Option<PathBuf>,
    runtime_exposure_override: Option<String>,
    user_home_dir: Option<PathBuf>,
}

impl AppEnvironment {
    pub(crate) fn discover() -> Self {
        Self {
            coral_config_dir_override: coral_config_dir_override(),
            runtime_exposure_override: runtime_exposure_override(),
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

    pub(crate) fn runtime_exposure_override(
        &self,
    ) -> Result<Option<RuntimeExposureMode>, AppError> {
        self.runtime_exposure_override
            .as_deref()
            .map(str::parse)
            .transpose()
    }

    pub(crate) fn query_runtime_context(&self) -> QueryRuntimeContext {
        QueryRuntimeContext {
            home_dir: self.user_home_dir.clone(),
            ..QueryRuntimeContext::default()
        }
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
fn runtime_exposure_override() -> Option<String> {
    std::env::var_os(CORAL_RUNTIME_EXPOSURE).and_then(|value| value.into_string().ok())
}

#[expect(
    clippy::disallowed_methods,
    reason = "coral-app is the single owner of process environment access."
)]
pub(crate) fn stdio_path_env() -> Option<OsString> {
    std::env::var_os("PATH")
}

#[cfg(test)]
mod tests {
    use super::{
        AppEnvironment, CORAL_CONFIG_DIR, CORAL_RUNTIME_EXPOSURE, RuntimeExposureMode,
        coral_config_dir_override,
    };
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

    #[test]
    #[expect(
        clippy::disallowed_methods,
        reason = "This test intentionally controls CORAL_RUNTIME_EXPOSURE to validate the app-owned accessor."
    )]
    fn runtime_exposure_override_reads_env_once_through_app_accessor() {
        if std::env::var_os("CORAL_RUN_RUNTIME_EXPOSURE_TEST").is_some() {
            let env = AppEnvironment::discover();
            assert_eq!(
                env.runtime_exposure_override()
                    .expect("runtime exposure override"),
                Some(RuntimeExposureMode::TypeScript)
            );
            return;
        }

        let status = std::process::Command::new(std::env::current_exe().expect("current exe"))
            .env("CORAL_RUN_RUNTIME_EXPOSURE_TEST", "1")
            .env(CORAL_RUNTIME_EXPOSURE, "typescript")
            .arg("--exact")
            .arg(
                "bootstrap::env::tests::runtime_exposure_override_reads_env_once_through_app_accessor",
            )
            .arg("--nocapture")
            .status()
            .expect("run subprocess");
        assert!(status.success(), "subprocess should pass");
    }
}
