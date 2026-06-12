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
/// The default implementation stores artifacts under Coral's local config
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
            if source_dir.exists() {
                std::fs::remove_dir_all(&source_dir)?;
            }
            if backup.exists() {
                std::fs::rename(backup, source_dir)?;
            }
        } else if source_dir.exists() {
            std::fs::remove_dir_all(&source_dir)?;
        }
        Ok(())
    }

    fn cleanup_source_artifacts_backup(&self, backup: Option<Box<dyn SourceArtifactBackup>>) {
        if let Some(backup) = backup
            && let Ok(path) = Self::backup_path(backup.as_ref())
            && path.exists()
        {
            drop(std::fs::remove_dir_all(path));
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

fn cleanup_empty_parent(root: &std::path::Path, path: Option<&std::path::Path>) {
    let Some(path) = path else {
        return;
    };
    if path == root || !path.starts_with(root) {
        return;
    }
    if std::fs::remove_dir(path).is_ok() {
        cleanup_empty_parent(root, path.parent());
    }
}
