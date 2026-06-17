//! Source artifact storage extension seam.

use std::any::Any;
use std::fmt;
use std::path::{Path, PathBuf};

use coral_spec::v4::{V4MaterializedSource, V4SourceManifest};
use uuid::Uuid;

use crate::bootstrap::AppError;
use crate::sources::SourceName;
use crate::sources::materialization::{
    load_v4_materialization, replace_v4_materialization, restore_materialization_backup,
};
use crate::state::AppStateLayout;
use crate::storage::fs;
use crate::workspaces::WorkspaceName;

/// Store-specific source artifact backup used to roll back failed source mutations.
pub trait SourceArtifactBackup: fmt::Debug + Send + Sync + 'static {
    /// Returns this backup as [`Any`] so the store that created it can recover
    /// its concrete state.
    fn as_any(&self) -> &dyn Any;
}

impl<T> SourceArtifactBackup for T
where
    T: fmt::Debug + Send + Sync + 'static,
{
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Durable storage backend for source manifest and DSL v4 materialized artifacts.
///
/// The default implementation stores artifacts under the local config
/// directory. Product runtimes can install an implementation that stores the
/// authoritative artifact package elsewhere while preserving the source
/// lifecycle logic in `SourceManager`.
pub trait SourceArtifactStore: fmt::Debug + Send + Sync + 'static {
    /// Reads the installed imported-source manifest, returning `None` when no
    /// manifest artifact exists.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when the artifact store cannot be read.
    fn read_manifest_artifact(
        &self,
        workspace_id: &str,
        source_name: &str,
    ) -> Result<Option<String>, AppError>;

    /// Writes or removes the installed imported-source manifest artifact.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when the artifact store cannot be updated.
    fn persist_manifest_artifact(
        &self,
        workspace_id: &str,
        source_name: &str,
        manifest_yaml: Option<&str>,
    ) -> Result<(), AppError>;

    /// Removes all artifacts for one source and returns a rollback backup when
    /// the store had prior artifacts.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when the artifact store cannot be updated.
    fn remove_source_artifacts(
        &self,
        workspace_id: &str,
        source_name: &str,
    ) -> Result<Option<Box<dyn SourceArtifactBackup>>, AppError>;

    /// Restores a backup produced by [`SourceArtifactStore::remove_source_artifacts`].
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when the artifact store cannot be restored.
    fn restore_source_artifacts_backup(
        &self,
        workspace_id: &str,
        source_name: &str,
        backup: Option<Box<dyn SourceArtifactBackup>>,
    ) -> Result<(), AppError>;

    /// Cleans up a backup produced by [`SourceArtifactStore::remove_source_artifacts`].
    fn cleanup_source_artifacts_backup(&self, backup: Option<Box<dyn SourceArtifactBackup>>);

    /// Replaces the installed DSL v4 materialization from a prepared temporary
    /// materialization directory.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when the artifact store cannot be updated.
    fn replace_v4_materialization(
        &self,
        workspace_id: &str,
        source_name: &str,
        temp_dir: &Path,
    ) -> Result<Option<Box<dyn SourceArtifactBackup>>, AppError>;

    /// Restores a backup produced by [`SourceArtifactStore::replace_v4_materialization`].
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when the artifact store cannot be restored.
    fn restore_v4_materialization_backup(
        &self,
        workspace_id: &str,
        source_name: &str,
        backup: Option<Box<dyn SourceArtifactBackup>>,
    ) -> Result<(), AppError>;

    /// Cleans up a backup produced by [`SourceArtifactStore::replace_v4_materialization`].
    fn cleanup_v4_materialization_backup(&self, backup: Option<Box<dyn SourceArtifactBackup>>);

    /// Loads and validates the installed DSL v4 materialization for query-time
    /// runtime assembly.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when artifacts are missing, incompatible, or cannot
    /// be read.
    fn load_v4_materialization(
        &self,
        workspace_id: &str,
        source_name: &str,
        manifest_yaml: &str,
        manifest: &V4SourceManifest,
    ) -> Result<V4MaterializedSource, AppError>;
}

#[derive(Debug)]
pub(crate) struct LocalSourceArtifactStore {
    layout: AppStateLayout,
}

#[derive(Debug)]
struct LocalPathBackup {
    path: PathBuf,
}

impl LocalSourceArtifactStore {
    pub(crate) fn new(layout: AppStateLayout) -> Self {
        Self { layout }
    }

    fn source_paths(
        workspace_id: &str,
        source_name: &str,
    ) -> Result<(WorkspaceName, SourceName), AppError> {
        Ok((
            WorkspaceName::parse(workspace_id)?,
            SourceName::parse(source_name)?,
        ))
    }

    fn backup_path(backup: &dyn SourceArtifactBackup) -> Result<PathBuf, AppError> {
        backup
            .as_any()
            .downcast_ref::<LocalPathBackup>()
            .map(|backup| backup.path.clone())
            .ok_or_else(|| {
                AppError::InvalidInput(
                    "source artifact backup was not produced by the local artifact store"
                        .to_string(),
                )
            })
    }
}

impl SourceArtifactStore for LocalSourceArtifactStore {
    fn read_manifest_artifact(
        &self,
        workspace_id: &str,
        source_name: &str,
    ) -> Result<Option<String>, AppError> {
        let (workspace_name, source_name) = Self::source_paths(workspace_id, source_name)?;
        let manifest_path = self.layout.manifest_file(&workspace_name, &source_name);
        if !manifest_path.exists() {
            return Ok(None);
        }
        std::fs::read_to_string(manifest_path)
            .map(Some)
            .map_err(Into::into)
    }

    fn persist_manifest_artifact(
        &self,
        workspace_id: &str,
        source_name: &str,
        manifest_yaml: Option<&str>,
    ) -> Result<(), AppError> {
        let (workspace_name, source_name) = Self::source_paths(workspace_id, source_name)?;
        let manifest_path = self.layout.manifest_file(&workspace_name, &source_name);
        match manifest_yaml {
            Some(manifest_yaml) => {
                if let Some(parent) = manifest_path.parent() {
                    fs::ensure_dir(parent)?;
                }
                fs::write_atomic(&manifest_path, manifest_yaml.as_bytes())?;
            }
            None if manifest_path.exists() => {
                std::fs::remove_file(&manifest_path)?;
            }
            None => {}
        }
        cleanup_empty_parent(&self.layout.workspaces_root(), manifest_path.parent());
        Ok(())
    }

    fn remove_source_artifacts(
        &self,
        workspace_id: &str,
        source_name: &str,
    ) -> Result<Option<Box<dyn SourceArtifactBackup>>, AppError> {
        let (workspace_name, source_name) = Self::source_paths(workspace_id, source_name)?;
        let source_dir = self.layout.source_dir(&workspace_name, &source_name);
        if !source_dir.exists() {
            return Ok(None);
        }
        let backup =
            source_dir.with_file_name(format!("{source_name}.delete.rollback.{}", Uuid::new_v4()));
        if backup.exists() {
            std::fs::remove_dir_all(&backup)?;
        }
        std::fs::rename(&source_dir, &backup)?;
        Ok(Some(Box::new(LocalPathBackup { path: backup })))
    }

    fn restore_source_artifacts_backup(
        &self,
        workspace_id: &str,
        source_name: &str,
        backup: Option<Box<dyn SourceArtifactBackup>>,
    ) -> Result<(), AppError> {
        let (workspace_name, source_name) = Self::source_paths(workspace_id, source_name)?;
        let source_dir = self.layout.source_dir(&workspace_name, &source_name);
        if let Some(backup) = backup {
            let backup = Self::backup_path(backup.as_ref())?;
            if !backup.exists() {
                return Err(AppError::FailedPrecondition(format!(
                    "source artifact backup '{}' does not exist",
                    backup.display()
                )));
            }
            if source_dir.exists() {
                std::fs::remove_dir_all(&source_dir)?;
            }
            std::fs::rename(backup, source_dir)?;
        } else if source_dir.exists() {
            std::fs::remove_dir_all(&source_dir)?;
            cleanup_empty_parent(&self.layout.workspaces_root(), source_dir.parent());
        }
        Ok(())
    }

    fn cleanup_source_artifacts_backup(&self, backup: Option<Box<dyn SourceArtifactBackup>>) {
        let Some(backup) = backup else {
            return;
        };
        if let Ok(path) = Self::backup_path(backup.as_ref()) {
            let parent = path.parent().map(Path::to_path_buf);
            if path.exists() {
                drop(std::fs::remove_dir_all(path));
            }
            cleanup_empty_parent(&self.layout.workspaces_root(), parent.as_deref());
        }
    }

    fn replace_v4_materialization(
        &self,
        workspace_id: &str,
        source_name: &str,
        temp_dir: &Path,
    ) -> Result<Option<Box<dyn SourceArtifactBackup>>, AppError> {
        let (workspace_name, source_name) = Self::source_paths(workspace_id, source_name)?;
        replace_v4_materialization(&self.layout, &workspace_name, &source_name, temp_dir).map(
            |backup| {
                backup
                    .map(|path| Box::new(LocalPathBackup { path }) as Box<dyn SourceArtifactBackup>)
            },
        )
    }

    fn restore_v4_materialization_backup(
        &self,
        workspace_id: &str,
        source_name: &str,
        backup: Option<Box<dyn SourceArtifactBackup>>,
    ) -> Result<(), AppError> {
        let (workspace_name, source_name) = Self::source_paths(workspace_id, source_name)?;
        let backup = backup.as_deref().map(Self::backup_path).transpose()?;
        restore_materialization_backup(&self.layout, &workspace_name, &source_name, backup)
    }

    fn cleanup_v4_materialization_backup(&self, backup: Option<Box<dyn SourceArtifactBackup>>) {
        self.cleanup_source_artifacts_backup(backup);
    }

    fn load_v4_materialization(
        &self,
        workspace_id: &str,
        source_name: &str,
        manifest_yaml: &str,
        manifest: &V4SourceManifest,
    ) -> Result<V4MaterializedSource, AppError> {
        let (workspace_name, source_name) = Self::source_paths(workspace_id, source_name)?;
        load_v4_materialization(
            &self.layout,
            &workspace_name,
            &source_name,
            manifest_yaml,
            manifest,
        )
    }
}

fn cleanup_empty_parent(root: &Path, path: Option<&Path>) {
    let Some(mut current) = path.map(Path::to_path_buf) else {
        return;
    };
    while current.starts_with(root) && current != root {
        let Ok(mut entries) = std::fs::read_dir(&current) else {
            break;
        };
        if entries.next().is_some() {
            break;
        }
        let next = current.parent().unwrap_or(root).to_path_buf();
        if std::fs::remove_dir(&current).is_err() {
            break;
        }
        current = next;
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::{LocalSourceArtifactStore, SourceArtifactStore};
    use crate::state::AppStateLayout;
    use crate::{sources::SourceName, workspaces::WorkspaceName};

    fn test_layout(temp: &TempDir) -> AppStateLayout {
        AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout")
    }

    fn default_workspace() -> WorkspaceName {
        WorkspaceName::parse("default").expect("workspace")
    }

    fn source_name() -> SourceName {
        SourceName::parse("github_v4_test").expect("source")
    }

    #[test]
    fn restore_source_artifacts_backup_errors_when_backup_path_is_missing() {
        let temp = TempDir::new().expect("temp dir");
        let layout = test_layout(&temp);
        let store = LocalSourceArtifactStore::new(layout.clone());
        let workspace = default_workspace();
        let source = source_name();
        let source_dir = layout.source_dir(&workspace, &source);
        std::fs::create_dir_all(&source_dir).expect("create source dir");
        std::fs::write(source_dir.join("manifest.yaml"), "old").expect("write old manifest");

        let backup = store
            .remove_source_artifacts(workspace.as_str(), source.as_str())
            .expect("remove artifacts")
            .expect("backup");
        let backup_path =
            LocalSourceArtifactStore::backup_path(backup.as_ref()).expect("backup path");
        std::fs::remove_dir_all(&backup_path).expect("remove backup");
        std::fs::create_dir_all(&source_dir).expect("recreate source dir");
        std::fs::write(source_dir.join("manifest.yaml"), "current")
            .expect("write current manifest");

        let error = store
            .restore_source_artifacts_backup(workspace.as_str(), source.as_str(), Some(backup))
            .expect_err("missing backup should fail");

        assert!(
            error
                .to_string()
                .contains(&backup_path.display().to_string())
        );
        assert_eq!(
            std::fs::read_to_string(source_dir.join("manifest.yaml")).expect("current manifest"),
            "current"
        );
    }

    #[test]
    fn restore_v4_materialization_backup_errors_when_backup_path_is_missing() {
        let temp = TempDir::new().expect("temp dir");
        let layout = test_layout(&temp);
        let store = LocalSourceArtifactStore::new(layout.clone());
        let workspace = default_workspace();
        let source = source_name();
        let materialized = layout.v4_materialized_dir(&workspace, &source);
        std::fs::create_dir_all(&materialized).expect("create materialization");
        std::fs::write(materialized.join("fingerprint"), "old").expect("write old materialization");
        let replacement = temp.path().join("replacement");
        std::fs::create_dir_all(&replacement).expect("create replacement");
        std::fs::write(replacement.join("fingerprint"), "new").expect("write replacement");

        let backup = store
            .replace_v4_materialization(workspace.as_str(), source.as_str(), &replacement)
            .expect("replace materialization")
            .expect("backup");
        let backup_path =
            LocalSourceArtifactStore::backup_path(backup.as_ref()).expect("backup path");
        std::fs::remove_dir_all(&backup_path).expect("remove backup");

        let error = store
            .restore_v4_materialization_backup(workspace.as_str(), source.as_str(), Some(backup))
            .expect_err("missing backup should fail");

        assert!(
            error
                .to_string()
                .contains(&backup_path.display().to_string())
        );
        assert_eq!(
            std::fs::read_to_string(materialized.join("fingerprint"))
                .expect("current materialization"),
            "new"
        );
    }
}
