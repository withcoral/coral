//! App-owned environment accessors for local runtime setup.

use std::path::PathBuf;

use coral_engine::QueryRuntimeContext;

use super::consts::{
    CORAL_COMMUNITY_REGISTRY_URL, CORAL_CONFIG_DIR, DEFAULT_COMMUNITY_REGISTRY_URL,
};

#[derive(Debug, Clone, Default)]
pub(crate) struct AppEnvironment {
    coral_config_dir_override: Option<PathBuf>,
    community_registry_url: String,
    user_home_dir: Option<PathBuf>,
}

impl AppEnvironment {
    pub(crate) fn discover() -> Self {
        Self {
            coral_config_dir_override: coral_config_dir_override(),
            community_registry_url: community_registry_url(),
            user_home_dir: etcetera::home_dir().ok(),
        }
    }

    pub(crate) fn coral_config_dir_override(&self) -> Option<PathBuf> {
        self.coral_config_dir_override.clone()
    }

    pub(crate) fn query_runtime_context(&self) -> QueryRuntimeContext {
        QueryRuntimeContext {
            home_dir: self.user_home_dir.clone(),
            ..QueryRuntimeContext::default()
        }
    }

    pub(crate) fn community_registry_url(&self) -> &str {
        &self.community_registry_url
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
fn community_registry_url() -> String {
    std::env::var(CORAL_COMMUNITY_REGISTRY_URL)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| DEFAULT_COMMUNITY_REGISTRY_URL.to_string())
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
}
