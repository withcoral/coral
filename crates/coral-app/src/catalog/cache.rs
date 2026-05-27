//! Workspace-scoped cache for query-visible catalog metadata.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::{Arc, Mutex};

use coral_engine::CatalogInfo;

use crate::workspaces::WorkspaceName;

#[derive(Clone, Default)]
pub(crate) struct CatalogMetadataCache {
    state: Arc<Mutex<CatalogMetadataCacheState>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct CatalogMetadataSignature(u64);

impl CatalogMetadataSignature {
    pub(crate) fn from_hash(hash: u64) -> Self {
        Self(hash)
    }
}

#[derive(Default)]
struct CatalogMetadataCacheState {
    entries: BTreeMap<WorkspaceName, CatalogMetadataCacheEntry>,
    versions: BTreeMap<WorkspaceName, u64>,
}

struct CatalogMetadataCacheEntry {
    version: u64,
    signature: CatalogMetadataSignature,
    snapshot: Arc<CatalogInfo>,
}

impl CatalogMetadataCache {
    pub(crate) async fn get_or_insert_with<E, F, Fut>(
        &self,
        workspace_name: &WorkspaceName,
        signature: CatalogMetadataSignature,
        mut build: F,
    ) -> Result<Arc<CatalogInfo>, E>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<CatalogInfo, E>>,
    {
        loop {
            let observed_version = {
                let mut state = self
                    .state
                    .lock()
                    .expect("catalog metadata cache mutex poisoned");
                let version = state.workspace_version(workspace_name);
                if let Some(entry) = state.entries.get(workspace_name)
                    && entry.version == version
                    && entry.signature == signature
                {
                    return Ok(entry.snapshot.clone());
                }
                state.entries.remove(workspace_name);
                version
            };

            let snapshot = Arc::new(build().await?);
            let mut state = self
                .state
                .lock()
                .expect("catalog metadata cache mutex poisoned");
            let current_version = state.workspace_version(workspace_name);
            if current_version == observed_version {
                state.entries.insert(
                    workspace_name.clone(),
                    CatalogMetadataCacheEntry {
                        version: current_version,
                        signature,
                        snapshot: snapshot.clone(),
                    },
                );
                return Ok(snapshot);
            }
        }
    }

    pub(crate) fn invalidate_workspace(&self, workspace_name: &WorkspaceName) {
        let mut state = self
            .state
            .lock()
            .expect("catalog metadata cache mutex poisoned");
        let next_version = state
            .versions
            .get(workspace_name)
            .copied()
            .unwrap_or_default()
            .saturating_add(1);
        state.versions.insert(workspace_name.clone(), next_version);
        state.entries.remove(workspace_name);
    }
}

impl CatalogMetadataCacheState {
    fn workspace_version(&self, workspace_name: &WorkspaceName) -> u64 {
        self.versions
            .get(workspace_name)
            .copied()
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    #[tokio::test]
    async fn repeated_workspace_lookup_builds_once() {
        let cache = CatalogMetadataCache::default();
        let workspace = WorkspaceName::default();
        let builds = AtomicUsize::new(0);

        let first = cache
            .get_or_insert_with(&workspace, signature(1), || async {
                builds.fetch_add(1, Ordering::SeqCst);
                Ok::<_, ()>(empty_catalog())
            })
            .await
            .expect("first catalog");
        let second = cache
            .get_or_insert_with(&workspace, signature(1), || async {
                builds.fetch_add(1, Ordering::SeqCst);
                Ok::<_, ()>(empty_catalog())
            })
            .await
            .expect("cached catalog");

        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(builds.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn changed_signature_rebuilds_snapshot() {
        let cache = CatalogMetadataCache::default();
        let workspace = WorkspaceName::default();
        let builds = AtomicUsize::new(0);

        cache
            .get_or_insert_with(&workspace, signature(1), || async {
                builds.fetch_add(1, Ordering::SeqCst);
                Ok::<_, ()>(empty_catalog())
            })
            .await
            .expect("first catalog");
        cache
            .get_or_insert_with(&workspace, signature(2), || async {
                builds.fetch_add(1, Ordering::SeqCst);
                Ok::<_, ()>(empty_catalog())
            })
            .await
            .expect("changed catalog");

        assert_eq!(builds.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn invalidation_rebuilds_only_that_workspace() {
        let cache = CatalogMetadataCache::default();
        let default_workspace = WorkspaceName::default();
        let other_workspace = WorkspaceName::parse("other").expect("workspace");
        let builds = AtomicUsize::new(0);

        cache
            .get_or_insert_with(&default_workspace, signature(1), || async {
                builds.fetch_add(1, Ordering::SeqCst);
                Ok::<_, ()>(empty_catalog())
            })
            .await
            .expect("default catalog");
        cache
            .get_or_insert_with(&other_workspace, signature(1), || async {
                builds.fetch_add(1, Ordering::SeqCst);
                Ok::<_, ()>(empty_catalog())
            })
            .await
            .expect("other catalog");

        cache.invalidate_workspace(&default_workspace);

        cache
            .get_or_insert_with(&other_workspace, signature(1), || async {
                builds.fetch_add(1, Ordering::SeqCst);
                Ok::<_, ()>(empty_catalog())
            })
            .await
            .expect("other cached catalog");
        cache
            .get_or_insert_with(&default_workspace, signature(1), || async {
                builds.fetch_add(1, Ordering::SeqCst);
                Ok::<_, ()>(empty_catalog())
            })
            .await
            .expect("rebuilt default catalog");

        assert_eq!(builds.load(Ordering::SeqCst), 3);
    }

    fn empty_catalog() -> CatalogInfo {
        CatalogInfo {
            sources: Vec::new(),
            tables: Vec::new(),
            table_functions: Vec::new(),
        }
    }

    fn signature(value: u64) -> CatalogMetadataSignature {
        CatalogMetadataSignature::from_hash(value)
    }
}
