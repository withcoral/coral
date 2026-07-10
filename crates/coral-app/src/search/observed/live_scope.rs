//! Live observed-value source-scope loading.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use crate::bootstrap::AppError;
use crate::search::observed::source_scope::source_surface_scopes;
use crate::search::observed::sqlite_queue::ObservedValuesGeneration;
use crate::search::observed::{ObservedValuesLiveScope, ObservedValuesLiveScopeLoadFailure};
use crate::sources::catalog::resolve_installed_manifest;
use crate::sources::model::InstalledSource;
use crate::sources::runtime_package::query_source_from_installed_manifest;
use crate::state::{AppStateLayout, ConfigStore};
use crate::workspaces::WorkspaceName;

#[derive(Debug, Clone)]
pub(crate) struct ObservedValuesLiveScopeLoader {
    config_store: ConfigStore,
    layout: AppStateLayout,
    cache: Arc<Mutex<BTreeMap<WorkspaceName, ObservedValuesLiveScopeCacheEntry>>>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ObservedValuesLiveScopeLoad {
    pub(crate) live_scopes: Vec<ObservedValuesLiveScope>,
    pub(crate) failed_sources: Vec<ObservedValuesLiveScopeLoadFailure>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ObservedValuesLiveScopeCacheEntry {
    key: ObservedValuesLiveScopeCacheKey,
    load: ObservedValuesLiveScopeLoad,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ObservedValuesLiveScopeCacheKey {
    sources: Vec<ObservedValuesLiveScopeCacheSource>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ObservedValuesLiveScopeCacheSource {
    source: InstalledSource,
    manifest: Option<FileFingerprint>,
    v4_fingerprint: Option<FileFingerprint>,
    v4_projections: Option<FileFingerprint>,
    v4_projections_override: Option<FileFingerprint>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FileFingerprint {
    len: u64,
    modified: Option<SystemTime>,
}

impl ObservedValuesLiveScopeLoader {
    pub(crate) fn new(layout: AppStateLayout, config_store: ConfigStore) -> Self {
        Self {
            config_store,
            layout,
            cache: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    pub(crate) fn load(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<ObservedValuesLiveScopeLoad, AppError> {
        let (sources, cache_key) = {
            let _state_lock = self.config_store.state_lock_shared()?;
            let config = self.config_store.load_config_unlocked()?;
            config.require_workspace(workspace_name)?;
            let sources = config.workspace_sources(workspace_name);
            let cache_key = self.cache_key(workspace_name, &sources);
            (sources, cache_key)
        };
        if let Some(load) = self.cached_load(workspace_name, &cache_key)? {
            return Ok(load);
        }
        let load = self.load_uncached(workspace_name, sources);
        self.store_cached_load(workspace_name, cache_key, load.clone())?;
        Ok(load)
    }

    fn load_uncached(
        &self,
        workspace_name: &WorkspaceName,
        sources: Vec<InstalledSource>,
    ) -> ObservedValuesLiveScopeLoad {
        let mut live_scopes = Vec::new();
        let mut failed_sources = Vec::new();
        for source in sources {
            let source_name = source.name.as_str().to_string();
            match self.load_source_scopes(workspace_name, &source) {
                Ok(source_scopes) => live_scopes.extend(source_scopes),
                Err(error) => {
                    tracing::debug!(
                        workspace = %workspace_name,
                        source = %source_name,
                        error = %error,
                        "skipping observed-value live scope for source"
                    );
                    failed_sources.push(ObservedValuesLiveScopeLoadFailure {
                        source_name,
                        message: error.to_string(),
                    });
                }
            }
        }
        ObservedValuesLiveScopeLoad {
            live_scopes,
            failed_sources,
        }
    }

    fn load_source_scopes(
        &self,
        workspace_name: &WorkspaceName,
        source: &InstalledSource,
    ) -> Result<Vec<ObservedValuesLiveScope>, AppError> {
        let installed = resolve_installed_manifest(workspace_name, source, &self.layout)?;
        let query_source = query_source_from_installed_manifest(
            &self.layout,
            workspace_name,
            source,
            &installed,
            BTreeMap::new(),
        )?;
        Ok(
            source_surface_scopes(&query_source, ObservedValuesGeneration::ZERO)
                .into_iter()
                .map(|scope| scope.live_scope())
                .collect(),
        )
    }

    fn cache_key(
        &self,
        workspace_name: &WorkspaceName,
        sources: &[InstalledSource],
    ) -> ObservedValuesLiveScopeCacheKey {
        ObservedValuesLiveScopeCacheKey {
            sources: sources
                .iter()
                .map(|source| ObservedValuesLiveScopeCacheSource {
                    source: source.clone(),
                    manifest: file_fingerprint(
                        self.layout.manifest_file(workspace_name, &source.name),
                    ),
                    v4_fingerprint: file_fingerprint(
                        self.layout
                            .v4_fingerprint_file(workspace_name, &source.name),
                    ),
                    v4_projections: file_fingerprint(
                        self.layout
                            .v4_projections_file(workspace_name, &source.name),
                    ),
                    v4_projections_override: file_fingerprint(
                        self.layout
                            .v4_projections_override_file(workspace_name, &source.name),
                    ),
                })
                .collect(),
        }
    }

    fn cached_load(
        &self,
        workspace_name: &WorkspaceName,
        cache_key: &ObservedValuesLiveScopeCacheKey,
    ) -> Result<Option<ObservedValuesLiveScopeLoad>, AppError> {
        let cache = self.cache.lock().map_err(live_scope_cache_error)?;
        Ok(cache
            .get(workspace_name)
            .filter(|entry| &entry.key == cache_key)
            .map(|entry| entry.load.clone()))
    }

    fn store_cached_load(
        &self,
        workspace_name: &WorkspaceName,
        key: ObservedValuesLiveScopeCacheKey,
        load: ObservedValuesLiveScopeLoad,
    ) -> Result<(), AppError> {
        let mut cache = self.cache.lock().map_err(live_scope_cache_error)?;
        cache.insert(
            workspace_name.clone(),
            ObservedValuesLiveScopeCacheEntry { key, load },
        );
        Ok(())
    }
}

fn file_fingerprint(path: impl AsRef<Path>) -> Option<FileFingerprint> {
    let metadata = std::fs::metadata(path).ok()?;
    Some(FileFingerprint {
        len: metadata.len(),
        modified: metadata.modified().ok(),
    })
}

fn live_scope_cache_error(error: impl std::fmt::Display) -> AppError {
    AppError::FailedPrecondition(format!(
        "observed-values live scope cache is unavailable: {error}"
    ))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use tempfile::tempdir;

    use super::ObservedValuesLiveScopeLoader;
    use crate::search::observed::sqlite_queue::ObservedValuesSurfaceKind;
    use crate::sources::SourceName;
    use crate::sources::model::{InstalledSource, SourceOrigin};
    use crate::state::{AppStateLayout, ConfigStore};
    use crate::workspaces::WorkspaceName;

    #[test]
    fn live_scope_changes_when_http_request_shape_changes() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let config_store = ConfigStore::new(layout.clone());
        let workspace = WorkspaceName::parse("work").expect("workspace");
        let source = SourceName::parse("github").expect("source");
        config_store
            .create_workspace(&workspace)
            .expect("create workspace");
        install_source(&layout, &config_store, &workspace, &source, "/repos/issues");
        let loader = ObservedValuesLiveScopeLoader::new(layout.clone(), config_store.clone());

        let first = loader.load(&workspace).expect("first live scope");
        install_source(
            &layout,
            &config_store,
            &workspace,
            &source,
            "/search/issues",
        );
        let second = loader.load(&workspace).expect("second live scope");

        assert!(first.failed_sources.is_empty());
        assert!(second.failed_sources.is_empty());
        assert_eq!(first.live_scopes.len(), 1);
        assert_eq!(second.live_scopes.len(), 1);
        let first_scope = first.live_scopes.first().expect("first live scope");
        let second_scope = second.live_scopes.first().expect("second live scope");
        assert_eq!(first_scope.source_name, "github");
        assert_eq!(first_scope.surface_kind, ObservedValuesSurfaceKind::Table);
        assert_eq!(first_scope.surface_name, "issues");
        assert_ne!(first_scope.source_scope_id, second_scope.source_scope_id);
    }

    #[test]
    fn one_broken_source_does_not_block_other_live_scopes() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let config_store = ConfigStore::new(layout.clone());
        let workspace = WorkspaceName::parse("work").expect("workspace");
        let github = SourceName::parse("github").expect("source");
        let broken = SourceName::parse("broken").expect("source");
        config_store
            .create_workspace(&workspace)
            .expect("create workspace");
        install_source(
            &layout,
            &config_store,
            &workspace,
            &github,
            "/search/issues",
        );
        install_broken_source(&layout, &config_store, &workspace, &broken);
        let loader = ObservedValuesLiveScopeLoader::new(layout, config_store);

        let load = loader.load(&workspace).expect("live scope load");

        assert_eq!(load.live_scopes.len(), 1);
        let live_scope = load.live_scopes.first().expect("live scope");
        assert_eq!(live_scope.source_name, "github");
        assert_eq!(load.failed_sources.len(), 1);
        let failed_source = load.failed_sources.first().expect("failed source");
        assert_eq!(failed_source.source_name, "broken");
    }

    fn install_source(
        layout: &AppStateLayout,
        config_store: &ConfigStore,
        workspace: &WorkspaceName,
        source: &SourceName,
        path: &str,
    ) {
        std::fs::create_dir_all(layout.source_dir(workspace, source)).expect("source dir");
        std::fs::write(
            layout.manifest_file(workspace, source),
            format!(
                r"
name: {source}
version: 1.0.0
dsl_version: 3
backend: http
base_url: https://example.com
tables:
  - name: issues
    description: Issues
    request:
      method: GET
      path: {path}
    columns:
      - name: title
        type: Utf8
"
            ),
        )
        .expect("write manifest");
        config_store
            .upsert_source(
                workspace,
                InstalledSource {
                    name: source.clone(),
                    version: None,
                    variables: BTreeMap::new(),
                    secrets: Vec::new(),
                    credential_storage: None,
                    origin: SourceOrigin::Imported,
                },
            )
            .expect("upsert source");
    }

    fn install_broken_source(
        layout: &AppStateLayout,
        config_store: &ConfigStore,
        workspace: &WorkspaceName,
        source: &SourceName,
    ) {
        std::fs::create_dir_all(layout.source_dir(workspace, source)).expect("source dir");
        std::fs::write(layout.manifest_file(workspace, source), "name: [").expect("write manifest");
        config_store
            .upsert_source(
                workspace,
                InstalledSource {
                    name: source.clone(),
                    version: None,
                    variables: BTreeMap::new(),
                    secrets: Vec::new(),
                    credential_storage: None,
                    origin: SourceOrigin::Imported,
                },
            )
            .expect("upsert source");
    }
}
