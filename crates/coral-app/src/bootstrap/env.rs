//! App-owned environment accessors for local runtime setup.

use std::path::PathBuf;

use coral_engine::QueryRuntimeContext;
use directories::BaseDirs;

use super::consts::{CORAL_CONFIG_DIR, CORAL_NEEDLES_FILE};

#[derive(Debug, Clone, Default)]
pub(crate) struct AppEnvironment {
    coral_config_dir_override: Option<PathBuf>,
    coral_needles_file: Option<PathBuf>,
    user_home_dir: Option<PathBuf>,
}

impl AppEnvironment {
    pub(crate) fn discover() -> Self {
        Self {
            coral_config_dir_override: coral_config_dir_override(),
            coral_needles_file: coral_needles_file(),
            user_home_dir: BaseDirs::new().map(|dirs| dirs.home_dir().to_path_buf()),
        }
    }

    pub(crate) fn coral_config_dir_override(&self) -> Option<PathBuf> {
        self.coral_config_dir_override.clone()
    }

    pub(crate) fn query_runtime_context(&self) -> QueryRuntimeContext {
        QueryRuntimeContext::new(self.user_home_dir.clone())
            .with_needles_file(self.coral_needles_file.clone())
    }
}

#[allow(
    clippy::disallowed_methods,
    reason = "coral-app is the single owner of process environment access."
)]
fn coral_config_dir_override() -> Option<PathBuf> {
    std::env::var_os(CORAL_CONFIG_DIR).map(PathBuf::from)
}

#[allow(
    clippy::disallowed_methods,
    reason = "coral-app is the single owner of process environment access."
)]
fn coral_needles_file() -> Option<PathBuf> {
    std::env::var_os(CORAL_NEEDLES_FILE).map(PathBuf::from)
}

#[cfg(test)]
mod tests {
    use super::{
        AppEnvironment, CORAL_CONFIG_DIR, CORAL_NEEDLES_FILE, coral_config_dir_override,
        coral_needles_file,
    };

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

    #[test]
    #[allow(
        clippy::disallowed_methods,
        reason = "This test intentionally controls CORAL_NEEDLES_FILE to validate the app-owned accessor."
    )]
    fn coral_needles_file_reads_env_through_app_accessor() {
        if std::env::var_os("CORAL_RUN_CORAL_NEEDLES_FILE_TEST").is_some() {
            assert_eq!(
                coral_needles_file().as_deref(),
                Some(std::path::Path::new("/tmp/benchmark-needles.yaml"))
            );
            let env = AppEnvironment::discover();
            let ctx = env.query_runtime_context();
            assert_eq!(
                ctx.needles_file.as_deref(),
                Some(std::path::Path::new("/tmp/benchmark-needles.yaml"))
            );
            return;
        }

        let status = std::process::Command::new(std::env::current_exe().expect("current exe"))
            .env("CORAL_RUN_CORAL_NEEDLES_FILE_TEST", "1")
            .env(CORAL_NEEDLES_FILE, "/tmp/benchmark-needles.yaml")
            .arg("--exact")
            .arg("bootstrap::env::tests::coral_needles_file_reads_env_through_app_accessor")
            .arg("--nocapture")
            .status()
            .expect("run subprocess");
        assert!(status.success(), "subprocess should pass");
    }
}
