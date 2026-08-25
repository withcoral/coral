//! Derives and creates the filesystem layout used by the local app.

use std::path::{Path, PathBuf};

use etcetera::app_strategy::{AppStrategy, AppStrategyArgs, choose_native_strategy};

use crate::bootstrap::AppError;
use crate::functions::FunctionName;
use crate::sources::SourceName;
use crate::sources::materialization::{
    DIAGNOSTICS_FILENAME, FINGERPRINT_FILENAME, OPERATION_METADATA_FILENAME, PROJECTIONS_FILENAME,
};
use crate::storage::fs::{ensure_dir, remove_file_if_exists};
use crate::workspaces::{WorkspaceName, WorkspacePaths};

pub(crate) const INSTALLED_MANIFEST_FILE_NAME: &str = "manifest.yaml";
pub(crate) const INSTALLED_FUNCTION_FILE_NAME: &str = "function.sql";
pub(crate) const INSTALLED_SECRETS_FILE_NAME: &str = "secrets.env";

/// Names the per-workspace directory holding installed source state.
const SOURCES_DIR_NAME: &str = "sources";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum V4ProjectionCatalogOrigin {
    Materialized,
    Override,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V4ProjectionCatalogFile {
    pub(crate) path: PathBuf,
    pub(crate) origin: V4ProjectionCatalogOrigin,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum V4OperationMetadataOrigin {
    Materialized,
    Override,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V4OperationMetadataFile {
    pub(crate) path: PathBuf,
    pub(crate) origin: V4OperationMetadataOrigin,
}

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

    pub(crate) fn database_file(&self) -> PathBuf {
        self.config_dir.join("coral.db")
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

    /// Where a workspace directory waits between the deletion committing and
    /// its contents being removed.
    ///
    /// A sibling of [`Self::workspaces_root`] rather than a directory inside
    /// it, so the rename stays on one filesystem while every entry a scan of
    /// the workspaces root finds is a live workspace. A workspace name is free
    /// to look like anything, staging included, so the location is the only
    /// evidence that separates the two.
    pub(crate) fn deleted_workspaces_root(&self) -> PathBuf {
        self.config_dir.join("deleted-workspaces")
    }

    pub(crate) fn remove_legacy_task_event_logs(&self) -> Result<(), std::io::Error> {
        let workspace_entries = match std::fs::read_dir(self.workspaces_root()) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error),
        };
        for entry in workspace_entries {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            remove_file_if_exists(&entry.path().join("tasks").join("tasks.jsonl"))?;
        }
        Ok(())
    }

    pub(crate) fn workspace_dir(&self, workspace_name: &WorkspaceName) -> PathBuf {
        self.workspaces_root().join(workspace_name.as_str())
    }

    pub(crate) fn sources_root(&self, workspace_name: &WorkspaceName) -> PathBuf {
        self.workspace_dir(workspace_name).join(SOURCES_DIR_NAME)
    }

    /// The secret file for `source_name` inside a workspace directory that has
    /// already been staged for deletion.
    ///
    /// Deletion moves the directory out of the workspaces root before it
    /// commits, so the live layout no longer reaches the material and asking it
    /// to erase one would find nothing and report success. Cleanup addresses
    /// the staged copy through here instead, and composes it from the same
    /// pieces [`Self::secret_file`] does so the two cannot drift.
    pub(crate) fn staged_secret_file(
        staged_workspace_dir: &Path,
        source_name: &SourceName,
    ) -> PathBuf {
        staged_workspace_dir
            .join(SOURCES_DIR_NAME)
            .join(source_name.as_str())
            .join(INSTALLED_SECRETS_FILE_NAME)
    }

    pub(crate) fn feedback_dir(&self, workspace_name: &WorkspaceName) -> PathBuf {
        self.workspace_dir(workspace_name).join("feedback")
    }

    pub(crate) fn feedback_reports_file(&self, workspace_name: &WorkspaceName) -> PathBuf {
        self.feedback_dir(workspace_name).join("reports.jsonl")
    }

    pub(crate) fn functions_root(&self, workspace_name: &WorkspaceName) -> PathBuf {
        self.workspace_dir(workspace_name).join("functions")
    }

    pub(crate) fn function_dir(
        &self,
        workspace_name: &WorkspaceName,
        function_name: &FunctionName,
    ) -> PathBuf {
        self.functions_root(workspace_name)
            .join(function_name.as_str())
    }

    pub(crate) fn function_file(
        &self,
        workspace_name: &WorkspaceName,
        function_name: &FunctionName,
    ) -> PathBuf {
        self.function_dir(workspace_name, function_name)
            .join(INSTALLED_FUNCTION_FILE_NAME)
    }

    pub(crate) fn credential_encryption_key_file(&self) -> PathBuf {
        self.config_dir.join("credentials").join("encryption.key")
    }

    pub(crate) fn search_dir(&self, workspace_name: &WorkspaceName) -> PathBuf {
        self.workspace_dir(workspace_name).join("search")
    }

    pub(crate) fn search_sqlite_file(&self, workspace_name: &WorkspaceName) -> PathBuf {
        self.search_dir(workspace_name).join("search.sqlite3")
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

    pub(crate) fn v4_projection_catalog_file(
        &self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
    ) -> V4ProjectionCatalogFile {
        let override_file = self
            .v4_override_dir(workspace_name, source_name)
            .join(PROJECTIONS_FILENAME);
        if override_file.exists() {
            V4ProjectionCatalogFile {
                path: override_file,
                origin: V4ProjectionCatalogOrigin::Override,
            }
        } else {
            V4ProjectionCatalogFile {
                path: self
                    .v4_materialized_dir(workspace_name, source_name)
                    .join(PROJECTIONS_FILENAME),
                origin: V4ProjectionCatalogOrigin::Materialized,
            }
        }
    }

    pub(crate) fn v4_diagnostics_file(
        &self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
    ) -> PathBuf {
        self.v4_materialized_dir(workspace_name, source_name)
            .join(DIAGNOSTICS_FILENAME)
    }

    pub(crate) fn v4_operation_metadata_file(
        &self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
    ) -> Result<V4OperationMetadataFile, AppError> {
        let override_file = self
            .v4_override_dir(workspace_name, source_name)
            .join(OPERATION_METADATA_FILENAME);
        match std::fs::symlink_metadata(&override_file) {
            Ok(_) => Ok(V4OperationMetadataFile {
                path: override_file,
                origin: V4OperationMetadataOrigin::Override,
            }),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                Ok(V4OperationMetadataFile {
                    path: self
                        .v4_materialized_dir(workspace_name, source_name)
                        .join(OPERATION_METADATA_FILENAME),
                    origin: V4OperationMetadataOrigin::Materialized,
                })
            }
            Err(error) => Err(error.into()),
        }
    }
}

impl WorkspacePaths for AppStateLayout {
    fn workspace_dir(&self, workspace_name: &WorkspaceName) -> PathBuf {
        AppStateLayout::workspace_dir(self, workspace_name)
    }

    fn deleted_workspaces_root(&self) -> PathBuf {
        AppStateLayout::deleted_workspaces_root(self)
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::AppStateLayout;
    use crate::functions::FunctionName;
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
        let function_name = FunctionName::parse("review_queue").expect("function");

        assert_eq!(layout.config_file(), config_dir.join("config.toml"));
        assert_eq!(layout.database_file(), config_dir.join("coral.db"));
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
            layout.function_file(&workspace_name, &function_name),
            config_dir
                .join("workspaces")
                .join("default")
                .join("functions")
                .join("review_queue")
                .join("function.sql")
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
            layout.local_trace_store_dir(),
            config_dir.join("telemetry").join("traces")
        );
    }

    #[test]
    fn removes_legacy_task_event_logs_for_each_workspace() {
        let temp = tempdir().expect("tempdir");
        let config_dir = temp.path().join("coral-config");
        let layout = AppStateLayout::discover(Some(config_dir.clone())).expect("layout");
        layout
            .remove_legacy_task_event_logs()
            .expect("missing workspace root is already clean");

        let default_task_log = config_dir.join("workspaces/default/tasks/tasks.jsonl");
        let analytics_task_log = config_dir.join("workspaces/analytics/tasks/tasks.jsonl");
        for path in [&default_task_log, &analytics_task_log] {
            fs::create_dir_all(path.parent().expect("task directory"))
                .expect("create task directory");
            fs::write(path, "sensitive task intent").expect("write legacy task log");
        }
        let unrelated = config_dir.join("workspaces/default/tasks/notes.jsonl");
        fs::write(&unrelated, "keep").expect("write unrelated file");
        let non_workspace_entry = config_dir.join("workspaces/README");
        fs::write(&non_workspace_entry, "keep").expect("write non-workspace entry");

        layout
            .remove_legacy_task_event_logs()
            .expect("remove legacy task logs");

        assert!(!default_task_log.exists());
        assert!(!analytics_task_log.exists());
        assert!(unrelated.exists());
        assert!(non_workspace_entry.exists());

        fs::write(&default_task_log, "reappeared").expect("recreate legacy task log");
        layout
            .remove_legacy_task_event_logs()
            .expect("remove recreated legacy task log");
        assert!(!default_task_log.exists());
    }

    #[test]
    fn v4_projection_catalog_file_returns_override_if_present() {
        let temp = tempdir().expect("tempdir");
        let config_dir = temp.path().join("coral-config");
        let layout = AppStateLayout::discover(Some(config_dir.clone())).expect("layout");
        let workspace_name = WorkspaceName::parse("default").expect("workspace");
        let source_name = SourceName::parse("github").expect("source");

        let generated = layout.v4_projection_catalog_file(&workspace_name, &source_name);
        assert_eq!(
            generated.path,
            config_dir.join("workspaces/default/sources/github/materialized/v4/projections.yaml")
        );
        assert_eq!(
            generated.origin,
            super::V4ProjectionCatalogOrigin::Materialized
        );

        let override_dir = config_dir.join("workspaces/default/sources/github/overrides");
        fs::create_dir_all(&override_dir).expect("override dir");
        let override_file = override_dir.join(PROJECTIONS_FILENAME);
        fs::write(&override_file, "{}").expect("write to override file");

        let overridden = layout.v4_projection_catalog_file(&workspace_name, &source_name);
        assert_eq!(overridden.path, override_file);
        assert_eq!(
            overridden.origin,
            super::V4ProjectionCatalogOrigin::Override
        );
        let generated_metadata = layout
            .v4_operation_metadata_file(&workspace_name, &source_name)
            .expect("generated metadata path");
        assert_eq!(
            generated_metadata.path,
            config_dir
                .join("workspaces/default/sources/github/materialized/v4/operation-metadata.yaml")
        );
        assert_eq!(
            generated_metadata.origin,
            super::V4OperationMetadataOrigin::Materialized
        );

        let metadata_override =
            override_dir.join(crate::sources::materialization::OPERATION_METADATA_FILENAME);
        fs::write(&metadata_override, "{}").expect("write operation metadata override");
        let overridden_metadata = layout
            .v4_operation_metadata_file(&workspace_name, &source_name)
            .expect("overridden metadata path");
        assert_eq!(overridden_metadata.path, metadata_override);
        assert_eq!(
            overridden_metadata.origin,
            super::V4OperationMetadataOrigin::Override
        );
    }
    /// The staged path must stay the live path's tail, or deletion cleanup
    /// would erase nothing while reporting success.
    #[test]
    fn the_staged_secret_path_mirrors_the_live_one() {
        let temp = tempfile::tempdir().expect("temp dir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace = WorkspaceName::parse("work").expect("workspace name");
        let source = SourceName::parse("secured_messages").expect("source name");

        let live = layout.secret_file(&workspace, &source);
        let staged_root = temp.path().join("deleted-workspaces").join("work.staged");
        let staged = AppStateLayout::staged_secret_file(&staged_root, &source);

        let live_tail = live
            .strip_prefix(layout.workspace_dir(&workspace))
            .expect("live secret sits under the workspace dir");
        let staged_tail = staged
            .strip_prefix(&staged_root)
            .expect("staged secret sits under the staged dir");
        assert_eq!(
            live_tail, staged_tail,
            "the staged secret must sit at the same place inside the directory as the live one"
        );
    }
}
