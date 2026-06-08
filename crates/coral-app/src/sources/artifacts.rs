//! Filesystem artifact ownership for installed source state.

use std::path::{Path, PathBuf};

use uuid::Uuid;

use crate::bootstrap::AppError;
use crate::sources::SourceName;
use crate::sources::model::{InstalledSource, SourceOrigin};
use crate::state::AppStateLayout;
use crate::storage::fs as storage_fs;
use crate::workspaces::WorkspaceName;

#[derive(Clone)]
pub(crate) struct SourceArtifactStore {
    layout: AppStateLayout,
}

#[derive(Debug)]
pub(crate) struct SourceDirRollback {
    target: PathBuf,
    staged: PathBuf,
}

#[derive(Debug)]
pub(crate) struct MaterializationBuild {
    temp_dir: PathBuf,
}

/// Rollback token for a materialization install that has not yet been
/// committed by the source config update.
#[derive(Debug)]
pub(crate) struct MaterializationRollback {
    target: PathBuf,
    previous: Option<PathBuf>,
    marker: PathBuf,
}

const MATERIALIZATION_ROLLBACK_MARKER_PREFIX: &str = ".install.rollback.";

impl SourceArtifactStore {
    pub(crate) fn new(layout: AppStateLayout) -> Self {
        Self { layout }
    }

    pub(crate) fn manifest_snapshot(
        &self,
        workspace_name: &WorkspaceName,
        source: &InstalledSource,
    ) -> Result<Option<String>, AppError> {
        match source.origin {
            SourceOrigin::Bundled => Ok(None),
            SourceOrigin::Imported => self
                .read_imported_manifest(workspace_name, &source.name)
                .map(Some),
        }
    }

    pub(crate) fn read_imported_manifest(
        &self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
    ) -> Result<String, AppError> {
        Ok(std::fs::read_to_string(
            self.layout.manifest_file(workspace_name, source_name),
        )?)
    }

    pub(crate) fn persist_manifest(
        &self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
        manifest_yaml: Option<&str>,
    ) -> Result<(), AppError> {
        let manifest_path = self.layout.manifest_file(workspace_name, source_name);
        match manifest_yaml {
            Some(manifest_yaml) => {
                if let Some(parent) = manifest_path.parent() {
                    storage_fs::ensure_private_dir(parent)?;
                }
                storage_fs::write_atomic(&manifest_path, manifest_yaml.as_bytes())?;
            }
            None => {
                storage_fs::remove_file_if_exists(&manifest_path)?;
            }
        }
        self.cleanup_empty_source_parents_from(manifest_path.parent());
        Ok(())
    }

    pub(crate) fn stage_source_dir_for_delete(
        &self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
    ) -> Result<Option<SourceDirRollback>, AppError> {
        let target = self.layout.source_dir(workspace_name, source_name);
        if !target.exists() {
            return Ok(None);
        }

        let staged =
            target.with_file_name(format!("{source_name}.delete.rollback.{}", Uuid::new_v4()));
        storage_fs::remove_dir_all_if_exists(&staged)?;
        storage_fs::rename_path(&target, &staged)?;
        Ok(Some(SourceDirRollback { target, staged }))
    }

    pub(crate) fn restore_source_dir_rollback(
        &self,
        rollback: &SourceDirRollback,
    ) -> Result<(), AppError> {
        if rollback.target.exists() {
            return Err(AppError::FailedPrecondition(format!(
                "cannot restore source directory from '{}': target '{}' already exists",
                rollback.staged.display(),
                rollback.target.display()
            )));
        }
        if rollback.staged.exists() {
            storage_fs::rename_path(&rollback.staged, &rollback.target)?;
        }
        self.cleanup_empty_source_parents_from(rollback.staged.parent());
        Ok(())
    }

    pub(crate) fn discard_source_dir_rollback(
        &self,
        rollback: SourceDirRollback,
    ) -> Result<(), AppError> {
        let SourceDirRollback { target, staged } = rollback;
        storage_fs::remove_dir_all_if_exists(&staged)?;
        self.cleanup_empty_source_parents_from(target.parent());
        Ok(())
    }

    pub(crate) fn remove_source_dir_if(
        &self,
        should_remove: bool,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
    ) -> Result<(), AppError> {
        if !should_remove {
            return Ok(());
        }
        let source_dir = self.layout.source_dir(workspace_name, source_name);
        storage_fs::remove_dir_all_if_exists(&source_dir)?;
        self.cleanup_empty_source_parents_from(source_dir.parent());
        Ok(())
    }

    pub(crate) fn prepare_v4_materialization_tmp(
        &self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
        temp_suffix: &str,
    ) -> Result<MaterializationBuild, AppError> {
        let temp_dir =
            self.layout
                .v4_materialized_tmp_dir(workspace_name, source_name, temp_suffix);
        storage_fs::remove_dir_all_if_exists(&temp_dir)?;
        storage_fs::ensure_private_dir(&temp_dir)?;
        Ok(MaterializationBuild { temp_dir })
    }

    pub(crate) fn install_v4_materialization(
        &self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
        build: &MaterializationBuild,
    ) -> Result<MaterializationRollback, AppError> {
        let target = self.layout.v4_materialized_dir(workspace_name, source_name);
        let previous = self.layout.v4_materialized_tmp_dir(
            workspace_name,
            source_name,
            &format!("rollback.{}", Uuid::new_v4()),
        );
        if let Some(parent) = target.parent() {
            storage_fs::ensure_private_dir(parent)?;
        }
        let marker_name = format!("{MATERIALIZATION_ROLLBACK_MARKER_PREFIX}{}", Uuid::new_v4());
        storage_fs::write_atomic(build.temp_dir().join(&marker_name).as_path(), b"pending\n")?;
        storage_fs::remove_dir_all_if_exists(&previous)?;
        let had_existing = target.exists();
        if had_existing {
            storage_fs::rename_path(&target, &previous)?;
        }
        if let Err(error) = storage_fs::rename_path(build.temp_dir(), &target) {
            if had_existing
                && previous.exists()
                && let Err(rollback_error) = storage_fs::rename_path(&previous, &target)
            {
                return Err(AppError::FailedPrecondition(format!(
                    "failed to install DSL v4 materialization for source '{source_name}': {error}; failed to restore previous materialization from '{}': {rollback_error}",
                    previous.display()
                )));
            }
            return Err(error.into());
        }
        Ok(MaterializationRollback {
            marker: target.join(marker_name),
            target,
            previous: had_existing.then_some(previous),
        })
    }

    pub(crate) fn restore_v4_materialization_rollback(
        &self,
        rollback: Option<MaterializationRollback>,
    ) -> Result<(), AppError> {
        if let Some(rollback) = rollback {
            if rollback.target.exists() {
                if !rollback.marker.exists() {
                    return Err(AppError::FailedPrecondition(format!(
                        "cannot restore DSL v4 materialization from '{}': target '{}' is not marked as this rollback's install",
                        rollback
                            .previous
                            .as_ref()
                            .unwrap_or(&rollback.target)
                            .display(),
                        rollback.target.display()
                    )));
                }
                storage_fs::remove_dir_all_if_exists(&rollback.target)?;
            }
            if let Some(previous) = rollback.previous
                && previous.exists()
            {
                storage_fs::rename_path(&previous, &rollback.target)?;
            }
        }
        Ok(())
    }

    pub(crate) fn cleanup_v4_materialization_tmp(&self, build: Option<&MaterializationBuild>) {
        if let Some(build) = build {
            drop(storage_fs::remove_dir_all_if_exists(build.temp_dir()));
        }
    }

    pub(crate) fn discard_v4_materialization_rollback(
        &self,
        rollback: Option<MaterializationRollback>,
    ) {
        if let Some(rollback) = rollback {
            drop(storage_fs::remove_file_if_exists(&rollback.marker));
            if let Some(previous) = rollback.previous {
                drop(storage_fs::remove_dir_all_if_exists(&previous));
            }
        }
    }

    fn cleanup_empty_source_parents_from(&self, path: Option<&Path>) {
        let Some(mut current) = path.map(Path::to_path_buf) else {
            return;
        };
        let root = self.layout.workspaces_root();
        while current.starts_with(&root) && current != root {
            let Ok(mut entries) = std::fs::read_dir(&current) else {
                break;
            };
            if entries.next().is_some() {
                break;
            }
            let next = current.parent().unwrap_or(&root).to_path_buf();
            if storage_fs::remove_empty_dir(&current).is_err() {
                break;
            }
            current = next;
        }
    }
}

impl SourceDirRollback {
    pub(crate) fn staged_path(&self) -> &Path {
        &self.staged
    }
}

impl MaterializationBuild {
    pub(crate) fn temp_dir(&self) -> &Path {
        &self.temp_dir
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    fn layout(temp: &TempDir) -> AppStateLayout {
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("ensure layout");
        layout
    }

    fn workspace_name() -> WorkspaceName {
        WorkspaceName::default()
    }

    fn source_name() -> SourceName {
        SourceName::parse("github_v4_artifact_test").expect("source name")
    }

    #[test]
    fn restoring_source_dir_rollback_refuses_occupied_target() {
        let temp = TempDir::new().expect("temp dir");
        let layout = layout(&temp);
        let store = SourceArtifactStore::new(layout.clone());
        let workspace = workspace_name();
        let source = source_name();
        let target = layout.source_dir(&workspace, &source);
        std::fs::create_dir_all(&target).expect("create source dir");
        std::fs::write(target.join("marker"), "original").expect("write original marker");
        let rollback = store
            .stage_source_dir_for_delete(&workspace, &source)
            .expect("stage source dir")
            .expect("source dir rollback");
        std::fs::create_dir_all(&target).expect("recreate source dir");
        std::fs::write(target.join("marker"), "concurrent").expect("write concurrent marker");

        let error = store
            .restore_source_dir_rollback(&rollback)
            .expect_err("occupied target should block rollback");

        assert!(
            error.to_string().contains("target"),
            "unexpected error: {error}"
        );
        assert_eq!(
            std::fs::read_to_string(target.join("marker")).expect("read target marker"),
            "concurrent"
        );
        assert_eq!(
            std::fs::read_to_string(rollback.staged_path().join("marker"))
                .expect("read staged marker"),
            "original"
        );
    }

    #[test]
    fn restoring_new_v4_materialization_removes_installed_target() {
        let temp = TempDir::new().expect("temp dir");
        let layout = layout(&temp);
        let store = SourceArtifactStore::new(layout.clone());
        let workspace = workspace_name();
        let source = source_name();
        let build = store
            .prepare_v4_materialization_tmp(&workspace, &source, "new")
            .expect("prepare temp materialization");
        std::fs::write(build.temp_dir().join("marker"), "new").expect("write marker");

        let rollback = store
            .install_v4_materialization(&workspace, &source, &build)
            .expect("install materialization");
        let target = layout.v4_materialized_dir(&workspace, &source);
        assert_eq!(
            std::fs::read_to_string(target.join("marker")).expect("read marker"),
            "new"
        );

        store
            .restore_v4_materialization_rollback(Some(rollback))
            .expect("restore rollback");

        assert!(
            !target.exists(),
            "new materialization should be removed on rollback"
        );
    }

    #[test]
    fn restoring_replaced_v4_materialization_reinstalls_previous_target() {
        let temp = TempDir::new().expect("temp dir");
        let layout = layout(&temp);
        let store = SourceArtifactStore::new(layout.clone());
        let workspace = workspace_name();
        let source = source_name();
        let target = layout.v4_materialized_dir(&workspace, &source);
        std::fs::create_dir_all(&target).expect("create existing target");
        std::fs::write(target.join("marker"), "old").expect("write old marker");
        let build = store
            .prepare_v4_materialization_tmp(&workspace, &source, "replacement")
            .expect("prepare temp materialization");
        std::fs::write(build.temp_dir().join("marker"), "new").expect("write new marker");

        let rollback = store
            .install_v4_materialization(&workspace, &source, &build)
            .expect("install materialization");
        assert_eq!(
            std::fs::read_to_string(target.join("marker")).expect("read marker"),
            "new"
        );

        store
            .restore_v4_materialization_rollback(Some(rollback))
            .expect("restore rollback");

        assert_eq!(
            std::fs::read_to_string(target.join("marker")).expect("read marker"),
            "old"
        );
    }

    #[test]
    fn restoring_v4_materialization_refuses_unmarked_occupied_target() {
        let temp = TempDir::new().expect("temp dir");
        let layout = layout(&temp);
        let store = SourceArtifactStore::new(layout.clone());
        let workspace = workspace_name();
        let source = source_name();
        let target = layout.v4_materialized_dir(&workspace, &source);
        std::fs::create_dir_all(&target).expect("create existing target");
        std::fs::write(target.join("marker"), "old").expect("write old marker");
        let build = store
            .prepare_v4_materialization_tmp(&workspace, &source, "replacement")
            .expect("prepare temp materialization");
        std::fs::write(build.temp_dir().join("marker"), "new").expect("write new marker");
        let rollback = store
            .install_v4_materialization(&workspace, &source, &build)
            .expect("install materialization");
        let previous = rollback.previous.clone().expect("previous materialization");
        std::fs::remove_dir_all(&target).expect("remove installed target");
        std::fs::create_dir_all(&target).expect("recreate target");
        std::fs::write(target.join("marker"), "concurrent").expect("write concurrent marker");

        let error = store
            .restore_v4_materialization_rollback(Some(rollback))
            .expect_err("unmarked target should block rollback");

        assert!(
            error.to_string().contains("not marked"),
            "unexpected error: {error}"
        );
        assert_eq!(
            std::fs::read_to_string(target.join("marker")).expect("read target marker"),
            "concurrent"
        );
        assert_eq!(
            std::fs::read_to_string(previous.join("marker")).expect("read previous marker"),
            "old"
        );
    }
}
