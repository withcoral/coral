//! Derives and creates the filesystem layout used by the local app.

use std::path::{Path, PathBuf};

use etcetera::app_strategy::{AppStrategy, AppStrategyArgs, choose_native_strategy};

use crate::bootstrap::AppError;
use crate::sources::SourceName;
use crate::sources::materialization::{
    DIAGNOSTICS_FILENAME, FINGERPRINT_FILENAME, PARAMETER_METADATA_OVERRIDE_FILENAME,
    PROJECTIONS_FILENAME,
};
use crate::storage::fs::ensure_dir;
use crate::workspaces::{WorkspaceName, WorkspacePaths};

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
            let strategy = choose_native_strategy(AppStrategyArgs {
                top_level_domain: "com".to_string(),
                author: "withcoral".to_string(),
                app_name: "coral".to_string(),
            })
            .map_err(|_err| AppError::MissingConfigDir)?;
            #[cfg(target_os = "macos")]
            let dir = strategy.data_dir();
            #[cfg(not(target_os = "macos"))]
            let dir = strategy.config_dir();
            dir
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

    pub(crate) fn config_dir(&self) -> &Path {
        &self.config_dir
    }

    pub(crate) fn state_lock(&self) -> &Path {
        &self.state_lock
    }

    pub(crate) fn local_trace_store_dir(&self) -> PathBuf {
        self.config_dir.join("telemetry").join("traces")
    }

    pub(crate) fn workspaces_root(&self) -> PathBuf {
        self.config_dir.join("workspaces")
    }

    pub(crate) fn workspace_dir(&self, workspace_name: &WorkspaceName) -> PathBuf {
        self.workspaces_root().join(workspace_name.as_str())
    }

    pub(crate) fn sources_root(&self, workspace_name: &WorkspaceName) -> PathBuf {
        self.workspace_dir(workspace_name).join("sources")
    }

    pub(crate) fn feedback_dir(&self, workspace_name: &WorkspaceName) -> PathBuf {
        self.workspace_dir(workspace_name).join("feedback")
    }

    pub(crate) fn feedback_reports_file(&self, workspace_name: &WorkspaceName) -> PathBuf {
        self.feedback_dir(workspace_name).join("reports.jsonl")
    }

    pub(crate) fn search_dir(&self, workspace_name: &WorkspaceName) -> PathBuf {
        self.workspace_dir(workspace_name).join("search")
    }

    pub(crate) fn search_sqlite_file(&self, workspace_name: &WorkspaceName) -> PathBuf {
        self.search_dir(workspace_name).join("search.sqlite3")
    }

    /// Per-workspace task lifecycle event log (JSONL).
    pub(crate) fn task_events_file(&self, workspace_name: &WorkspaceName) -> PathBuf {
        self.workspace_dir(workspace_name)
            .join("tasks")
            .join("tasks.jsonl")
    }

    pub(crate) fn source_dir(
        &self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
    ) -> PathBuf {
        self.sources_root(workspace_name).join(source_name.as_str())
    }

    pub(crate) fn manifest_file(
        &self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
    ) -> PathBuf {
        self.source_dir(workspace_name, source_name)
            .join(INSTALLED_MANIFEST_FILE_NAME)
    }

    pub(crate) fn secret_file(
        &self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
    ) -> PathBuf {
        self.source_dir(workspace_name, source_name)
            .join(INSTALLED_SECRETS_FILE_NAME)
    }

    pub(crate) fn credential_refresh_lock_file(
        &self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
    ) -> PathBuf {
        self.config_dir
            .join("locks")
            .join("credentials")
            .join(workspace_name.as_str())
            .join(format!("{}.refresh.lock", source_name.as_str()))
    }

    pub(crate) fn v4_materialized_dir(
        &self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
    ) -> PathBuf {
        self.source_dir(workspace_name, source_name)
            .join("materialized")
            .join("v4")
    }

    pub(crate) fn v4_override_dir(
        &self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
    ) -> PathBuf {
        self.source_dir(workspace_name, source_name)
            .join("overrides")
    }

    pub(crate) fn v4_materialized_tmp_dir(
        &self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
        suffix: &str,
    ) -> PathBuf {
        self.source_dir(workspace_name, source_name)
            .join("materialized")
            .join(format!("v4.{suffix}"))
    }

    pub(crate) fn v4_fingerprint_file(
        &self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
    ) -> PathBuf {
        self.v4_materialized_dir(workspace_name, source_name)
            .join(FINGERPRINT_FILENAME)
    }

    fn v4_overridden_or_materialized(
        &self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
        path: &str,
    ) -> PathBuf {
        let override_file = self.v4_override_dir(workspace_name, source_name).join(path);
        if override_file.exists() {
            override_file
        } else {
            self.v4_materialized_dir(workspace_name, source_name)
                .join(path)
        }
    }

    pub(crate) fn v4_projections_file(
        &self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
    ) -> PathBuf {
        self.v4_overridden_or_materialized(workspace_name, source_name, PROJECTIONS_FILENAME)
    }

    pub(crate) fn v4_projections_override_file(
        &self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
    ) -> PathBuf {
        self.v4_override_dir(workspace_name, source_name)
            .join(PROJECTIONS_FILENAME)
    }

    pub(crate) fn v4_diagnostics_file(
        &self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
    ) -> PathBuf {
        self.v4_materialized_dir(workspace_name, source_name)
            .join(DIAGNOSTICS_FILENAME)
    }

    pub(crate) fn v4_parameter_metadata_override_file(
        &self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
        surface_id: &str,
    ) -> PathBuf {
        self.v4_override_dir(workspace_name, source_name)
            .join("surfaces")
            .join(surface_id)
            .join(PARAMETER_METADATA_OVERRIDE_FILENAME)
    }

    pub(crate) fn v4_surface_dir(
        &self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
        surface_id: &str,
    ) -> PathBuf {
        self.v4_materialized_dir(workspace_name, source_name)
            .join("surfaces")
            .join(surface_id)
    }
}

impl WorkspacePaths for AppStateLayout {
    fn workspace_dir(&self, workspace_name: &WorkspaceName) -> PathBuf {
        AppStateLayout::workspace_dir(self, workspace_name)
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::AppStateLayout;
    use crate::sources::SourceName;
    use crate::sources::materialization::PROJECTIONS_FILENAME;
    use crate::workspaces::WorkspaceName;
    use tempfile::tempdir;

    #[test]
    fn derives_top_level_config_and_source_artifact_paths() {
        let temp = tempdir().expect("tempdir");
        let config_dir = temp.path().join("coral-config");
        let layout = AppStateLayout::discover(Some(config_dir.clone())).expect("layout");
        let workspace_name = WorkspaceName::parse("default").expect("workspace");
        let source_name = SourceName::parse("github").expect("source");

        assert_eq!(layout.config_file(), config_dir.join("config.toml"));
        assert_eq!(
            layout.manifest_file(&workspace_name, &source_name),
            config_dir
                .join("workspaces")
                .join("default")
                .join("sources")
                .join("github")
                .join("manifest.yaml")
        );
        assert_eq!(
            layout.secret_file(&workspace_name, &source_name),
            config_dir
                .join("workspaces")
                .join("default")
                .join("sources")
                .join("github")
                .join("secrets.env")
        );
        assert_eq!(
            layout.feedback_reports_file(&workspace_name),
            config_dir
                .join("workspaces")
                .join("default")
                .join("feedback")
                .join("reports.jsonl")
        );
        assert_eq!(
            layout.search_sqlite_file(&workspace_name),
            config_dir
                .join("workspaces")
                .join("default")
                .join("search")
                .join("search.sqlite3")
        );
        assert_eq!(
            layout.task_events_file(&workspace_name),
            config_dir
                .join("workspaces")
                .join("default")
                .join("tasks")
                .join("tasks.jsonl")
        );
        assert_eq!(
            layout.local_trace_store_dir(),
            config_dir.join("telemetry").join("traces")
        );
    }

    #[test]
    fn v4_projections_file_returns_override_if_present() {
        let temp = tempdir().expect("tempdir");
        let config_dir = temp.path().join("coral-config");
        let layout = AppStateLayout::discover(Some(config_dir.clone())).expect("layout");
        let workspace_name = WorkspaceName::parse("default").expect("workspace");
        let source_name = SourceName::parse("github").expect("source");

        assert_eq!(
            layout.v4_projections_file(&workspace_name, &source_name),
            config_dir.join("workspaces/default/sources/github/materialized/v4/projections.yaml")
        );

        let override_dir = config_dir.join("workspaces/default/sources/github/overrides");
        fs::create_dir_all(&override_dir).expect("override dir");
        let override_file = override_dir.join(PROJECTIONS_FILENAME);
        fs::write(&override_file, "{}").expect("write to override file");

        assert_eq!(
            layout.v4_projections_file(&workspace_name, &source_name),
            override_file
        );
        assert_eq!(
            layout.v4_projections_override_file(&workspace_name, &source_name),
            override_file
        );
        assert_eq!(
            layout.v4_parameter_metadata_override_file(&workspace_name, &source_name, "rest"),
            config_dir
                .join("workspaces/default/sources/github/overrides/surfaces/rest")
                .join("parameter_metadata.yaml")
        );
    }
}
