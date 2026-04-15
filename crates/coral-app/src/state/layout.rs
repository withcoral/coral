//! Derives and creates the filesystem layout used by the local app.

use std::path::{Path, PathBuf};

use directories::ProjectDirs;

use crate::bootstrap::AppError;
use crate::storage::fs::ensure_dir;

pub(crate) const INSTALLED_MANIFEST_FILE_NAME: &str = "manifest.yaml";
pub(crate) const INSTALLED_SECRETS_FILE_NAME: &str = "secrets.env";

#[derive(Debug, Clone)]
pub(crate) struct AppStateLayout {
    config_dir: PathBuf,
    config_file: PathBuf,
    state_lock: PathBuf,
}

impl AppStateLayout {
    pub(crate) fn discover(config_dir_override: Option<PathBuf>) -> Result<Self, AppError> {
        let config_dir = if let Some(config_dir) = config_dir_override {
            config_dir
        } else {
            let dirs =
                ProjectDirs::from("com", "withcoral", "coral").ok_or(AppError::MissingConfigDir)?;
            dirs.config_dir().to_path_buf()
        };

        Ok(Self {
            config_file: config_dir.join("config.toml"),
            state_lock: config_dir.join(".lock"),
            config_dir,
        })
    }

    pub(crate) fn ensure(&self) -> Result<(), std::io::Error> {
        ensure_dir(&self.config_dir)?;
        ensure_dir(self.state_lock.parent().unwrap_or_else(|| Path::new(".")))?;
        Ok(())
    }

    pub(crate) fn config_file(&self) -> &Path {
        &self.config_file
    }

    pub(crate) fn state_lock(&self) -> &Path {
        &self.state_lock
    }

    pub(crate) fn workspaces_root(&self) -> PathBuf {
        self.config_dir.join("workspaces")
    }

    pub(crate) fn workspace_dir(&self, workspace: &coral_api::v1::Workspace) -> PathBuf {
        self.workspaces_root().join(&workspace.name)
    }

    pub(crate) fn sources_root(&self, workspace: &coral_api::v1::Workspace) -> PathBuf {
        self.workspace_dir(workspace).join("sources")
    }

    pub(crate) fn source_dir(
        &self,
        workspace: &coral_api::v1::Workspace,
        source_name: &str,
    ) -> PathBuf {
        self.sources_root(workspace).join(source_name)
    }

    pub(crate) fn manifest_file(
        &self,
        workspace: &coral_api::v1::Workspace,
        source_name: &str,
    ) -> PathBuf {
        self.source_dir(workspace, source_name)
            .join(INSTALLED_MANIFEST_FILE_NAME)
    }

    pub(crate) fn secret_file(
        &self,
        workspace: &coral_api::v1::Workspace,
        source_name: &str,
    ) -> PathBuf {
        self.source_dir(workspace, source_name)
            .join(INSTALLED_SECRETS_FILE_NAME)
    }
}

#[cfg(test)]
mod tests {
    use coral_api::v1::Workspace;

    use super::AppStateLayout;

    #[test]
    fn derives_top_level_config_and_source_artifact_paths() {
        let layout = AppStateLayout::discover(Some("/tmp/coral-config".into())).expect("layout");
        let workspace = Workspace {
            name: "default".to_string(),
        };

        assert_eq!(
            layout.config_file(),
            std::path::Path::new("/tmp/coral-config/config.toml")
        );
        assert_eq!(
            layout.manifest_file(&workspace, "github"),
            std::path::Path::new(
                "/tmp/coral-config/workspaces/default/sources/github/manifest.yaml"
            )
        );
        assert_eq!(
            layout.secret_file(&workspace, "github"),
            std::path::Path::new("/tmp/coral-config/workspaces/default/sources/github/secrets.env")
        );
    }
}
