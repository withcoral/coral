//! Derives and creates the filesystem layout used by process bootstrap.

use std::path::{Path, PathBuf};

use etcetera::app_strategy::{AppStrategy, AppStrategyArgs, choose_native_strategy};

use super::fs::ensure_dir;
use crate::bootstrap::AppError;

#[derive(Debug, Clone)]
pub(crate) struct AppStateLayout {
    config_dir: PathBuf,
    config_file: PathBuf,
}

impl AppStateLayout {
    pub(crate) fn discover(config_dir_override: Option<PathBuf>) -> Result<Self, AppError> {
        let config_dir = if let Some(config_dir) = config_dir_override {
            config_dir
        } else {
            let strategy = choose_native_strategy(AppStrategyArgs {
                top_level_domain: "com".to_string(),
                author: "withcoral".to_string(),
                app_name: "coral".to_string(),
            })
            .map_err(|_error| AppError::MissingConfigDir)?;
            #[cfg(target_os = "macos")]
            let dir = strategy.data_dir();
            #[cfg(not(target_os = "macos"))]
            let dir = strategy.config_dir();
            dir
        };

        Ok(Self {
            config_file: config_dir.join("config.toml"),
            config_dir,
        })
    }

    pub(crate) fn ensure(&self) -> Result<(), std::io::Error> {
        ensure_dir(&self.config_dir)?;
        Ok(())
    }

    pub(crate) fn config_file(&self) -> &Path {
        &self.config_file
    }

    pub(crate) fn config_dir(&self) -> &Path {
        &self.config_dir
    }

    pub(crate) fn database_file(&self) -> PathBuf {
        self.config_dir.join("coral.db")
    }
}
