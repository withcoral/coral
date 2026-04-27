//! App-owned environment accessors for local runtime setup.

use std::path::PathBuf;

use coral_engine::QueryRuntimeContext;

use super::consts::CORAL_CONFIG_DIR;

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

    pub(crate) fn query_runtime_context(&self) -> QueryRuntimeContext {
        QueryRuntimeContext {
            home_dir: self.user_home_dir.clone(),
        }
    }
}

#[allow(
    clippy::disallowed_methods,
    reason = "coral-app is the single owner of process environment access."
)]
fn coral_config_dir_override() -> Option<PathBuf> {
    std::env::var_os(CORAL_CONFIG_DIR).map(PathBuf::from)
}

#[cfg(test)]
mod tests {
    use super::{AppEnvironment, CORAL_CONFIG_DIR, coral_config_dir_override};

    #[test]
    #[allow(
        clippy::disallowed_methods,
        reason = "This test intentionally controls CORAL_CONFIG_DIR to validate the app-owned accessor."
    )]
    fn coral_config_dir_override_reads_env_once_through_app_accessor() {
        if std::env::var_os("CORAL_RUN_CORAL_CONFIG_DIR_TEST").is_some() {
            assert_eq!(
                coral_config_dir_override().as_deref(),
                Some(std::path::Path::new("/tmp/coral-config-dir-override"))
            );
            let env = AppEnvironment::discover();
            assert_eq!(
                env.coral_config_dir_override().as_deref(),
                Some(std::path::Path::new("/tmp/coral-config-dir-override"))
            );
            return;
        }

        let status = std::process::Command::new(std::env::current_exe().expect("current exe"))
            .env("CORAL_RUN_CORAL_CONFIG_DIR_TEST", "1")
            .env(CORAL_CONFIG_DIR, "/tmp/coral-config-dir-override")
            .arg("--exact")
            .arg(
                "bootstrap::env::tests::coral_config_dir_override_reads_env_once_through_app_accessor",
            )
            .arg("--nocapture")
            .status()
            .expect("run subprocess");
        assert!(status.success(), "subprocess should pass");
    }
}
