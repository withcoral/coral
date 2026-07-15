//! App-owned environment accessors for local runtime setup.

use std::env::VarError;
use std::ffi::OsStr;
use std::path::PathBuf;

use coral_engine::QueryRuntimeContext;

use super::consts::{CORAL_CONFIG_DIR, CORAL_SEARCH_PROVIDER_MODE};
use super::error::AppError;
use crate::search::engine::SearchProviderMode;
use crate::state::AppStateLayout;

#[derive(Debug, Clone, Default)]
pub(crate) struct AppEnvironment {
    coral_config_dir_override: Option<PathBuf>,
    search_provider_mode: SearchProviderMode,
    user_home_dir: Option<PathBuf>,
}

impl AppEnvironment {
    pub(crate) fn discover() -> Result<Self, AppError> {
        Ok(Self {
            coral_config_dir_override: coral_config_dir_override(),
            search_provider_mode: search_provider_mode()?,
            user_home_dir: etcetera::home_dir().ok(),
        })
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

    pub(crate) fn search_provider_mode(&self) -> SearchProviderMode {
        self.search_provider_mode
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

#[expect(
    clippy::disallowed_methods,
    reason = "coral-app is the single owner of process environment access."
)]
fn search_provider_mode() -> Result<SearchProviderMode, AppError> {
    parse_search_provider_mode(std::env::var_os(CORAL_SEARCH_PROVIDER_MODE).as_deref())
}

fn parse_search_provider_mode(value: Option<&OsStr>) -> Result<SearchProviderMode, AppError> {
    let Some(value) = value else {
        return Ok(SearchProviderMode::CatalogAndObserved);
    };
    let Some(value) = value.to_str() else {
        return Err(AppError::FailedPrecondition(format!(
            "{CORAL_SEARCH_PROVIDER_MODE} must be valid Unicode and exactly one of: default, catalog_only"
        )));
    };
    match value {
        "default" => Ok(SearchProviderMode::CatalogAndObserved),
        "catalog_only" => Ok(SearchProviderMode::CatalogOnly),
        _ => Err(AppError::FailedPrecondition(format!(
            "invalid {CORAL_SEARCH_PROVIDER_MODE} value '{value}'; expected exactly one of: default, catalog_only"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AppEnvironment, CORAL_CONFIG_DIR, CORAL_SEARCH_PROVIDER_MODE, coral_config_dir_override,
        parse_search_provider_mode,
    };
    use crate::search::engine::SearchProviderMode;
    use std::ffi::OsStr;
    use std::path::PathBuf;

    #[test]
    fn search_provider_mode_defaults_when_unset_or_explicitly_default() {
        assert_eq!(
            parse_search_provider_mode(None).expect("unset mode should use default providers"),
            SearchProviderMode::CatalogAndObserved
        );
        assert_eq!(
            parse_search_provider_mode(Some(OsStr::new("default")))
                .expect("explicit default mode should use default providers"),
            SearchProviderMode::CatalogAndObserved
        );
    }

    #[test]
    fn search_provider_mode_accepts_catalog_only() {
        assert_eq!(
            parse_search_provider_mode(Some(OsStr::new("catalog_only")))
                .expect("catalog-only mode should parse"),
            SearchProviderMode::CatalogOnly
        );
    }

    #[test]
    fn search_provider_mode_rejects_other_literals_without_normalizing() {
        for value in ["", "DEFAULT", " catalog_only", "catalog_only "] {
            let error = parse_search_provider_mode(Some(OsStr::new(value)))
                .expect_err("unknown mode should fail closed");
            let message = error.to_string();
            assert!(message.contains(CORAL_SEARCH_PROVIDER_MODE));
            assert!(message.contains("default, catalog_only"));
        }
    }

    #[cfg(unix)]
    #[test]
    fn search_provider_mode_rejects_non_unicode_values() {
        use std::os::unix::ffi::OsStrExt as _;

        let value = OsStr::from_bytes(b"catalog_only\xFF");
        let error = parse_search_provider_mode(Some(value))
            .expect_err("non-Unicode mode should fail closed");
        let message = error.to_string();
        assert!(message.contains(CORAL_SEARCH_PROVIDER_MODE));
        assert!(message.contains("valid Unicode"));
    }

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
            let env = AppEnvironment::discover().expect("discover app environment");
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
