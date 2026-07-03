//! Filesystem-backed store for source-spec registrations.

use std::path::{Path, PathBuf};

use crate::bootstrap::AppError;
use crate::sources::SourceName;
use crate::state::AppStateLayout;
use crate::storage::fs;

#[derive(Debug, Clone)]
pub(crate) struct GlobalSourceSpecManifest {
    pub(crate) manifest_yaml: String,
}

pub(crate) trait GlobalSourceSpecStore: Send + Sync + 'static {
    fn load(&self, name: &SourceName) -> Result<GlobalSourceSpecManifest, AppError> {
        self.load_optional(name)?
            .ok_or_else(|| AppError::MissingGlobalSourceSpec {
                source_name: name.to_string(),
            })
    }

    fn load_optional(
        &self,
        name: &SourceName,
    ) -> Result<Option<GlobalSourceSpecManifest>, AppError>;

    fn write_manifest(&self, name: &SourceName, manifest_yaml: &str) -> Result<(), AppError>;

    fn remove(&self, name: &SourceName) -> Result<Box<dyn RemovedGlobalSourceSpec>, AppError>;
}

pub(crate) trait RemovedGlobalSourceSpec {
    fn backup_path(&self) -> &Path;

    fn restore(&self) -> Result<(), AppError>;

    fn commit(self: Box<Self>) -> Result<(), AppError>;
}

#[derive(Clone)]
pub(crate) struct FsGlobalSourceSpecStore {
    layout: AppStateLayout,
}

impl FsGlobalSourceSpecStore {
    pub(crate) fn new(layout: AppStateLayout) -> Self {
        Self { layout }
    }
}

impl GlobalSourceSpecStore for FsGlobalSourceSpecStore {
    fn load_optional(
        &self,
        name: &SourceName,
    ) -> Result<Option<GlobalSourceSpecManifest>, AppError> {
        let manifest_path = self.layout.source_spec_manifest_file(name);
        let manifest_yaml = match std::fs::read_to_string(&manifest_path) {
            Ok(manifest_yaml) => manifest_yaml,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(AppError::Io(error)),
        };
        Ok(Some(GlobalSourceSpecManifest { manifest_yaml }))
    }

    fn write_manifest(&self, name: &SourceName, manifest_yaml: &str) -> Result<(), AppError> {
        let manifest_path = self.layout.source_spec_manifest_file(name);
        if let Some(parent) = manifest_path.parent() {
            fs::ensure_dir(parent)?;
        }
        fs::write_atomic(&manifest_path, manifest_yaml.as_bytes())?;
        Ok(())
    }

    fn remove(&self, name: &SourceName) -> Result<Box<dyn RemovedGlobalSourceSpec>, AppError> {
        let source_spec_dir = self.layout.source_spec_dir(name);
        Ok(Box::new(FsRemovedGlobalSourceSpec {
            parent: source_spec_dir.parent().map(Path::to_path_buf),
            root: self.layout.source_specs_root(),
            backup: fs::DirectoryBackup::move_for_delete(&source_spec_dir, name)?,
        }))
    }
}

struct FsRemovedGlobalSourceSpec {
    backup: fs::DirectoryBackup,
    parent: Option<PathBuf>,
    root: PathBuf,
}

impl RemovedGlobalSourceSpec for FsRemovedGlobalSourceSpec {
    fn backup_path(&self) -> &Path {
        self.backup.backup_path()
    }

    fn restore(&self) -> Result<(), AppError> {
        self.backup.restore().map_err(AppError::from)
    }

    fn commit(self: Box<Self>) -> Result<(), AppError> {
        self.backup.commit()?;
        fs::cleanup_empty_parent_dirs(&self.root, self.parent.as_deref());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::{FsGlobalSourceSpecStore, GlobalSourceSpecStore};
    use crate::bootstrap::AppError;
    use crate::sources::SourceName;
    use crate::state::AppStateLayout;

    fn test_store() -> (TempDir, AppStateLayout, FsGlobalSourceSpecStore) {
        let temp = TempDir::new().expect("temp dir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let store = FsGlobalSourceSpecStore::new(layout.clone());
        (temp, layout, store)
    }

    #[test]
    fn load_missing_returns_global_source_spec_error() {
        let (_temp, _layout, store) = test_store();
        let source_name = SourceName::parse("linear").expect("source name");

        let error = store
            .load(&source_name)
            .expect_err("missing manifest should be explicit");

        assert!(matches!(error, AppError::MissingGlobalSourceSpec { .. }));
    }

    #[test]
    fn writes_loads_removes_and_restores_manifest_tree() {
        let (_temp, layout, store) = test_store();
        let source_name = SourceName::parse("linear").expect("source name");
        let manifest_yaml = "name: linear\nversion: 1.0.0\n";

        store
            .write_manifest(&source_name, manifest_yaml)
            .expect("write manifest");
        assert_eq!(
            store
                .load(&source_name)
                .expect("load manifest")
                .manifest_yaml,
            manifest_yaml
        );

        let removed = store.remove(&source_name).expect("remove manifest tree");
        assert!(matches!(
            store.load(&source_name),
            Err(AppError::MissingGlobalSourceSpec { .. })
        ));
        removed.restore().expect("restore manifest tree");
        assert_eq!(
            store
                .load(&source_name)
                .expect("load restored manifest")
                .manifest_yaml,
            manifest_yaml
        );

        let removed = store.remove(&source_name).expect("remove manifest tree");
        removed.commit().expect("commit deletion");
        assert!(!layout.source_spec_dir(&source_name).exists());
    }
}
