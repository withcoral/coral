//! App-owned storage and resolution for bundled source manifests.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::io::ErrorKind;
use std::path::PathBuf;

use coral_api::v1::BundledManifestState;
use coral_spec::parse_source_manifest_yaml;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::bootstrap::AppError;
use crate::sources::catalog::{
    BundledSourceManifest, bundled_source_manifests, find_bundled_source,
};
use crate::sources::model::ManagedSource;
use crate::state::{AppStateLayout, ConfigStore, INSTALLED_MANIFEST_FILE_NAME};
use crate::storage::fs::{self, FileLock};

#[derive(Debug, Clone)]
pub(crate) struct BundledStore {
    layout: AppStateLayout,
}

#[derive(Debug, Clone)]
pub(crate) struct ResolvedManifest {
    pub(crate) manifest_path: PathBuf,
    pub(crate) version: String,
    pub(crate) bundled_manifest_state: BundledManifestState,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BundledManifestTracking {
    bundle_id: String,
}

impl BundledStore {
    pub(crate) fn new(layout: AppStateLayout) -> Self {
        Self { layout }
    }

    pub(crate) fn bundle_id_for(source_name: &str, manifest_yaml: &str) -> String {
        bundle_id_for(source_name, manifest_yaml)
    }

    pub(crate) fn ensure_current_bundle_available(
        &self,
        bundled: &BundledSourceManifest,
    ) -> Result<String, AppError> {
        let bundle_id = Self::bundle_id_for(&bundled.name, &bundled.manifest_yaml);
        self.materialize_bundle(&bundled.name, &bundle_id, &bundled.manifest_yaml)
            .map_err(|error| {
                AppError::FailedPrecondition(format!(
                    "failed to materialize bundled manifest cache for '{}': {error}. rerun the command or reinstall Coral if the problem persists",
                    bundled.name
                ))
            })?;
        Ok(bundle_id)
    }

    pub(crate) fn startup_maintenance(&self, config_store: &ConfigStore) -> Result<(), AppError> {
        let mut first_error = None;

        if let Err(error) = self.materialize_current_bundle_cache() {
            first_error = Some(error);
        }
        record_first_error(
            &mut first_error,
            self.migrate_legacy_bundled_sources(config_store).err(),
        );
        record_first_error(
            &mut first_error,
            self.sync_bundled_source_versions(config_store).err(),
        );

        if let Some(error) = first_error {
            Err(error)
        } else {
            Ok(())
        }
    }

    pub(crate) fn write_tracking_file(
        &self,
        workspace: &coral_api::v1::Workspace,
        source_name: &str,
        bundle_id: &str,
    ) -> Result<(), AppError> {
        let path = self
            .layout
            .bundled_manifest_tracking_file(workspace, source_name);
        if let Some(parent) = path.parent() {
            fs::ensure_dir(parent)?;
        }
        let raw = toml::to_string(&BundledManifestTracking {
            bundle_id: bundle_id.to_string(),
        })?;
        fs::write_atomic(&path, raw.as_bytes())?;
        Ok(())
    }

    pub(crate) fn read_tracking_file(
        &self,
        workspace: &coral_api::v1::Workspace,
        source_name: &str,
    ) -> Result<Option<String>, AppError> {
        let path = self
            .layout
            .bundled_manifest_tracking_file(workspace, source_name);
        match std::fs::read_to_string(path) {
            Ok(raw) => Ok(Some(
                toml::from_str::<BundledManifestTracking>(&raw)?.bundle_id,
            )),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    pub(crate) fn resolve_manifest(
        &self,
        source: &ManagedSource,
    ) -> Result<ResolvedManifest, AppError> {
        if !source.origin.is_bundled() {
            return Ok(ResolvedManifest {
                manifest_path: self.layout.manifest_file(&source.workspace, &source.name),
                version: source.version.clone(),
                bundled_manifest_state: BundledManifestState::NotApplicable,
            });
        }

        let workspace_manifest = self.layout.manifest_file(&source.workspace, &source.name);
        if workspace_manifest.exists() {
            let manifest_yaml = std::fs::read_to_string(&workspace_manifest)?;
            return Ok(ResolvedManifest {
                manifest_path: workspace_manifest,
                version: manifest_version(&manifest_yaml)?,
                bundled_manifest_state: BundledManifestState::LocalOverride,
            });
        }

        let recorded_bundle_id = self
            .read_tracking_file(&source.workspace, &source.name)?
            .ok_or_else(|| {
                AppError::FailedPrecondition(format!(
                    "bundled source '{}' is missing bundled-manifest.toml. rerun `coral source add {}` to relink it",
                    source.name, source.name
                ))
            })?;
        let current_bundle_manifest = find_bundled_source(&source.name).map(|bundled| {
            (
                Self::bundle_id_for(&bundled.name, &bundled.manifest_yaml),
                bundled.manifest_yaml,
            )
        });

        if let Some((bundle_id, manifest_yaml)) = current_bundle_manifest {
            let current_path = self
                .layout
                .bundled_source_manifest_file(&source.name, &bundle_id);
            if current_path.exists() {
                return Ok(ResolvedManifest {
                    manifest_path: current_path,
                    version: manifest_version(&manifest_yaml)?,
                    bundled_manifest_state: BundledManifestState::FollowingCurrent,
                });
            }
        }

        let recorded_path = self
            .layout
            .bundled_source_manifest_file(&source.name, &recorded_bundle_id);
        if recorded_path.exists() {
            let manifest_yaml = std::fs::read_to_string(&recorded_path)?;
            return Ok(ResolvedManifest {
                manifest_path: recorded_path,
                version: manifest_version(&manifest_yaml)?,
                bundled_manifest_state: BundledManifestState::Unspecified,
            });
        }

        Err(AppError::FailedPrecondition(format!(
            "bundled source '{}' has no usable bundled manifest cache. rerun `coral source add {}` to restore it",
            source.name, source.name
        )))
    }

    fn materialize_current_bundle_cache(&self) -> Result<(), AppError> {
        let mut first_error = None;

        for bundled in bundled_source_manifests() {
            let bundle_id = Self::bundle_id_for(&bundled.name, &bundled.manifest_yaml);
            if let Err(error) =
                self.materialize_bundle(&bundled.name, &bundle_id, &bundled.manifest_yaml)
            {
                tracing::warn!(
                    source = %bundled.name,
                    detail = %error,
                    "failed to materialize bundled manifest cache"
                );
                if first_error.is_none() {
                    first_error = Some(AppError::Io(error));
                }
            }
        }

        if let Some(error) = first_error {
            Err(error)
        } else {
            Ok(())
        }
    }

    fn migrate_legacy_bundled_sources(&self, config_store: &ConfigStore) -> Result<(), AppError> {
        let mut first_error = None;

        for source in config_store
            .list_all_sources()?
            .into_iter()
            .filter(|source| source.origin.is_bundled())
        {
            match self.read_tracking_file(&source.workspace, &source.name) {
                Ok(Some(_)) => continue,
                Ok(None) => {}
                Err(error) => {
                    tracing::warn!(
                        source = %source.name,
                        detail = %error,
                        "failed to inspect bundled tracking file during migration"
                    );
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                    continue;
                }
            }

            let Some(bundled) = find_bundled_source(&source.name) else {
                let error = AppError::FailedPrecondition(format!(
                    "bundled source '{}' is no longer compiled into this Coral build",
                    source.name
                ));
                tracing::warn!(source = %source.name, detail = %error, "failed to migrate bundled source");
                if first_error.is_none() {
                    first_error = Some(error);
                }
                continue;
            };

            let bundle_id = match self.ensure_current_bundle_available(&bundled) {
                Ok(bundle_id) => bundle_id,
                Err(error) => {
                    tracing::warn!(
                        source = %source.name,
                        detail = %error,
                        "failed to ensure bundled cache during migration"
                    );
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                    continue;
                }
            };

            if let Err(error) =
                self.write_tracking_file(&source.workspace, &source.name, &bundle_id)
            {
                tracing::warn!(
                    source = %source.name,
                    detail = %error,
                    "failed to write bundled tracking file during migration"
                );
                if first_error.is_none() {
                    first_error = Some(error);
                }
                continue;
            }

            let manifest_path = self.layout.manifest_file(&source.workspace, &source.name);
            if manifest_path.exists()
                && let Err(error) = std::fs::remove_file(&manifest_path)
            {
                tracing::warn!(
                    source = %source.name,
                    detail = %error,
                    "failed to remove legacy bundled workspace manifest during migration"
                );
                if first_error.is_none() {
                    first_error = Some(error.into());
                }
            }
        }

        if let Some(error) = first_error {
            Err(error)
        } else {
            Ok(())
        }
    }

    fn sync_bundled_source_versions(&self, config_store: &ConfigStore) -> Result<(), AppError> {
        let mut first_error = None;

        for mut source in config_store
            .list_all_sources()?
            .into_iter()
            .filter(|source| source.origin.is_bundled())
        {
            let resolved = match self.resolve_manifest(&source) {
                Ok(resolved) => resolved,
                Err(error) => {
                    tracing::warn!(
                        source = %source.name,
                        detail = %error,
                        "failed to resolve bundled manifest while syncing versions"
                    );
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                    continue;
                }
            };

            if source.version == resolved.version {
                continue;
            }

            source.version = resolved.version;
            if let Err(error) = config_store.upsert_source(source) {
                tracing::warn!(
                    detail = %error,
                    "failed to update bundled source version in config"
                );
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }

        if let Some(error) = first_error {
            Err(error)
        } else {
            Ok(())
        }
    }

    fn materialize_bundle(
        &self,
        source_name: &str,
        bundle_id: &str,
        manifest_yaml: &str,
    ) -> Result<(), std::io::Error> {
        let _lock = FileLock::exclusive(self.layout.state_lock())?;
        let source_root = self.layout.bundled_source_root(source_name);
        fs::ensure_dir(&source_root)?;
        let bundle_dir = self
            .layout
            .bundled_source_bundle_dir(source_name, bundle_id);
        if bundle_dir.exists() {
            return Ok(());
        }

        let temp_dir = source_root.join(format!(".tmp-{bundle_id}-{}", std::process::id()));
        if temp_dir.exists() {
            std::fs::remove_dir_all(&temp_dir)?;
        }
        fs::ensure_dir(&temp_dir)?;
        fs::write_atomic(
            &temp_dir.join(INSTALLED_MANIFEST_FILE_NAME),
            manifest_yaml.as_bytes(),
        )?;
        match std::fs::rename(&temp_dir, &bundle_dir) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == ErrorKind::AlreadyExists => {
                let _ = std::fs::remove_dir_all(&temp_dir);
                Ok(())
            }
            Err(error) => {
                let _ = std::fs::remove_dir_all(&temp_dir);
                Err(error)
            }
        }
    }
}

fn manifest_version(manifest_yaml: &str) -> Result<String, AppError> {
    Ok(parse_source_manifest_yaml(manifest_yaml)
        .map_err(|error| AppError::InvalidInput(error.to_string()))?
        .source_version()
        .to_string())
}

fn bundle_id_for(source_name: &str, manifest_yaml: &str) -> String {
    let mut parts = BTreeMap::new();
    parts.insert("manifest_yaml", manifest_yaml);
    parts.insert("name", source_name);

    let mut hasher = Sha256::new();
    for (key, value) in parts {
        hasher.update(key.as_bytes());
        hasher.update([0]);
        hasher.update(value.as_bytes());
        hasher.update([0]);
    }
    let digest = hasher.finalize();
    let mut value = String::with_capacity(12);
    for byte in &digest[..6] {
        let _ = write!(&mut value, "{byte:02x}");
    }
    value
}

fn record_first_error(slot: &mut Option<AppError>, error: Option<AppError>) {
    if slot.is_none() {
        *slot = error;
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Write as _;

    use coral_api::v1::{BundledManifestState, Workspace};
    use tempfile::TempDir;

    use super::{BundledStore, bundle_id_for};
    use crate::sources::catalog::load_bundled_source;
    use crate::sources::model::{ManagedSource, ManagedSourceOrigin};
    use crate::state::AppStateLayout;
    use crate::workspaces::WorkspaceManager;

    fn default_workspace() -> Workspace {
        WorkspaceManager::new().default_workspace()
    }

    fn manifest_with_version(manifest_yaml: &str, version: &str) -> String {
        let mut rewritten = String::new();
        let mut replaced = false;

        for line in manifest_yaml.lines() {
            if !replaced && line.starts_with("version: ") {
                let _ = writeln!(&mut rewritten, "version: {version}");
                replaced = true;
            } else {
                rewritten.push_str(line);
                rewritten.push('\n');
            }
        }

        assert!(replaced, "manifest fixture must contain a version line");
        rewritten
    }

    #[test]
    fn bundle_ids_are_content_addressed() {
        let first = bundle_id_for("demo", "name: demo\nversion: 1.0.0\n");
        let second = bundle_id_for("demo", "name: demo\nversion: 1.0.0\n");
        let changed = bundle_id_for("demo", "name: demo\nversion: 1.1.0\n");

        assert_eq!(first.len(), 12);
        assert_eq!(first, second);
        assert_ne!(first, changed);
    }

    #[test]
    fn tracking_file_round_trips() {
        let temp = TempDir::new().expect("temp dir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("ensure");
        let store = BundledStore::new(layout);

        store
            .write_tracking_file(&default_workspace(), "github", "bundle-123")
            .expect("write tracking");

        assert_eq!(
            store
                .read_tracking_file(&default_workspace(), "github")
                .expect("read tracking"),
            Some("bundle-123".to_string())
        );
    }

    #[test]
    fn ensure_current_bundle_available_materializes_expected_path() {
        let temp = TempDir::new().expect("temp dir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("ensure");
        let store = BundledStore::new(layout.clone());
        let bundled = load_bundled_source("github").expect("bundled source");

        let bundle_id = store
            .ensure_current_bundle_available(&bundled)
            .expect("materialize bundle");

        assert!(
            layout
                .bundled_source_manifest_file("github", &bundle_id)
                .exists()
        );
    }

    #[test]
    fn falling_back_to_recorded_bundle_reports_unspecified_state() {
        let temp = TempDir::new().expect("temp dir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("ensure");
        let store = BundledStore::new(layout.clone());
        let bundled = load_bundled_source("github").expect("bundled source");
        let stale_manifest = manifest_with_version(&bundled.manifest_yaml, "0.0.1");
        let stale_bundle_id = bundle_id_for("github", &stale_manifest);

        std::fs::create_dir_all(layout.source_dir(&default_workspace(), "github"))
            .expect("create source dir");
        store
            .write_tracking_file(&default_workspace(), "github", &stale_bundle_id)
            .expect("write tracking");
        std::fs::create_dir_all(layout.bundled_source_bundle_dir("github", &stale_bundle_id))
            .expect("create stale bundle dir");
        std::fs::write(
            layout.bundled_source_manifest_file("github", &stale_bundle_id),
            stale_manifest,
        )
        .expect("write stale manifest");

        let resolved = store
            .resolve_manifest(&ManagedSource {
                workspace: default_workspace(),
                name: "github".to_string(),
                version: "ignored".to_string(),
                variables: std::collections::BTreeMap::default(),
                secrets: Vec::new(),
                origin: ManagedSourceOrigin::Bundled,
            })
            .expect("resolve recorded fallback");

        assert_eq!(resolved.version, "0.0.1");
        assert_eq!(
            resolved.bundled_manifest_state,
            BundledManifestState::Unspecified
        );
    }
}
