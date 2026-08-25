//! Dual-backend storage seam for Universal Search.
//!
//! [`SearchStorage`] is configured once per process and opens one
//! [`SearchStore`] per Workspace, where `SqliteSearchStore::open_workspace`
//! used to be called directly. A store serves the catalog projection through
//! [`CatalogStore`] and the clear/compaction operations that span every data
//! class. Observed values are reached through [`ObservedValuesStore`] when the
//! backend keeps them; only `SQLite` does, and that stays so until observed
//! memory is redesigned identity-scoped (lagoon #37).
//!
//! The seam is synchronous on purpose. The provider registry owns the blocking
//! boundary (`provider::run_provider`), so retrievers can borrow request-scoped
//! state; a backend that talks to an async driver blocks inside these calls.
//! Retrieval order is the ranking and must be a deterministic total order,
//! whichever backend serves it.

mod config;
mod sqlite;

use std::sync::Arc;

use crate::bootstrap::AppError;
use crate::search::catalog::index::{
    CatalogClearResult, CatalogDocumentClass, CatalogIndexSnapshot, CatalogRebuildResult,
    CatalogRefreshResult, CatalogSearchHits,
};
use crate::search::maintenance::SearchStorageCleanupResult;
use crate::search::observed::{
    ObservedValuesClearResult, ObservedValuesDrainBudget, ObservedValuesDrainResult,
    ObservedValuesRebuildResult, ObservedValuesRetrievalPolicy, ObservedValuesSearchHits,
};
use crate::search::sqlite_store::{SqliteSearchError, SqliteSearchStore};
use crate::state::AppStateLayout;
use crate::workspaces::WorkspaceName;

pub(crate) use config::{ResolvedSearchConfig, SearchConfig, SearchConfigError};
use sqlite::SqliteSearchStorage;

/// Catalog projection storage for one Workspace.
///
/// Snapshots arrive already fingerprinted; the store decides whether the
/// projection is current and replaces it atomically when it is not.
pub(crate) trait CatalogStore: Send + Sync {
    fn projection_is_current(&self, fingerprint: &str) -> Result<bool, SearchStoreError>;

    fn refresh_projection(
        &self,
        snapshot: &CatalogIndexSnapshot,
    ) -> Result<CatalogRefreshResult, SearchStoreError>;

    fn rebuild_projection(
        &self,
        snapshot: &CatalogIndexSnapshot,
        force: bool,
    ) -> Result<CatalogRebuildResult, SearchStoreError>;

    fn document_count(&self) -> Result<u32, SearchStoreError>;

    /// Retrieves one class of document in ranked order.
    ///
    /// Terms are normalized upstream (`result::query_terms`); the backend owns
    /// turning them into its query form and must return a deterministic total
    /// order, because the returned position *is* the ranking.
    fn search(
        &self,
        terms: &[String],
        limit: usize,
        class: CatalogDocumentClass,
    ) -> Result<CatalogSearchHits, SearchStoreError>;

    fn clear_source(&self, source_name: &str) -> Result<CatalogClearResult, SearchStoreError>;

    fn clear_workspace(&self) -> Result<CatalogClearResult, SearchStoreError>;
}

/// Observed-values queue, projection, and retrieval for every Workspace of a
/// backend.
///
/// Queue and projection share one trait because they interleave in one
/// transaction (drain-then-search, savepoint per job). The retrieval policy
/// travels as an argument on every call, never as connection state, so a
/// pooled backend cannot leak scope across requests.
pub(crate) trait ObservedValuesStore: std::fmt::Debug + Send + Sync {
    fn search(
        &self,
        workspace_name: &WorkspaceName,
        terms: &[String],
        limit: usize,
        policy: &ObservedValuesRetrievalPolicy,
    ) -> Result<ObservedValuesSearchHits, SearchStoreError>;

    fn drain_queue(
        &self,
        workspace_name: &WorkspaceName,
        budget: ObservedValuesDrainBudget,
    ) -> Result<ObservedValuesDrainResult, SearchStoreError>;

    fn rebuild_fts(
        &self,
        workspace_name: &WorkspaceName,
        policy: &ObservedValuesRetrievalPolicy,
    ) -> Result<ObservedValuesRebuildResult, SearchStoreError>;

    fn pending_queue_job_count(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<usize, SearchStoreError>;

    fn clear_workspace_and_advance_epoch(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<ObservedValuesClearResult, SearchStoreError>;

    fn clear_source_and_advance_epoch(
        &self,
        workspace_name: &WorkspaceName,
        source_name: &str,
    ) -> Result<ObservedValuesClearResult, SearchStoreError>;

    fn compact_after_clear(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<SearchStorageCleanupResult, SearchStoreError>;
}

/// The configured search storage backend; opens per-Workspace stores.
#[derive(Debug, Clone)]
pub(crate) struct SearchStorage {
    backend: SearchStorageBackend,
}

#[derive(Debug, Clone)]
enum SearchStorageBackend {
    Sqlite(SqliteSearchStorage),
}

impl SearchStorage {
    /// One `SQLite` sidecar per Workspace under the app-state layout.
    pub(crate) fn sqlite(layout: AppStateLayout) -> Self {
        Self {
            backend: SearchStorageBackend::Sqlite(SqliteSearchStorage::new(layout)),
        }
    }

    pub(crate) fn backend_name(&self) -> &'static str {
        match &self.backend {
            SearchStorageBackend::Sqlite(_) => "sqlite",
        }
    }

    /// Opens the Workspace's store, creating and migrating it when needed.
    ///
    /// Capability probing and migrations run here, fail-loud: a backend that
    /// cannot serve search reports which feature is missing instead of
    /// serving degraded results.
    pub(crate) fn open_workspace(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<SearchStore, SearchStoreError> {
        match &self.backend {
            SearchStorageBackend::Sqlite(storage) => Ok(SearchStore {
                backend: SearchStoreBackend::Sqlite(storage.open_workspace(workspace_name)?),
            }),
        }
    }

    /// Opens the Workspace's store only when search state already exists for
    /// it, so best-effort cleanup never creates state as a side effect.
    pub(crate) fn open_existing_workspace(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<Option<SearchStore>, SearchStoreError> {
        match &self.backend {
            SearchStorageBackend::Sqlite(storage) => Ok(storage
                .open_existing_workspace(workspace_name)?
                .map(|store| SearchStore {
                    backend: SearchStoreBackend::Sqlite(store),
                })),
        }
    }

    /// Whether this backend keeps observed values at all; when it does not,
    /// the capture pipeline has nowhere to write and must not start.
    pub(crate) fn keeps_observed_values(&self) -> bool {
        self.observed_values().is_some()
    }

    /// The observed-values store, when this backend keeps observed values.
    #[expect(
        clippy::unnecessary_wraps,
        reason = "backends without observed values answer None; SQLite is the only backend until the Postgres store lands"
    )]
    pub(crate) fn observed_values(&self) -> Option<Arc<dyn ObservedValuesStore>> {
        match &self.backend {
            SearchStorageBackend::Sqlite(storage) => Some(Arc::new(storage.observed_values())),
        }
    }
}

/// One Workspace's search store, opened through [`SearchStorage`].
#[derive(Debug, Clone)]
pub(crate) struct SearchStore {
    backend: SearchStoreBackend,
}

#[derive(Debug, Clone)]
enum SearchStoreBackend {
    Sqlite(SqliteSearchStore),
}

/// Outcome of clearing every data class of a source or a Workspace at once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SearchClearAllResult {
    pub(crate) catalog: CatalogClearResult,
    pub(crate) observed: ObservedValuesClearResult,
}

impl SearchStore {
    pub(crate) fn backend_name(&self) -> &'static str {
        match &self.backend {
            SearchStoreBackend::Sqlite(_) => "sqlite",
        }
    }

    pub(crate) fn catalog(&self) -> &dyn CatalogStore {
        match &self.backend {
            SearchStoreBackend::Sqlite(store) => store,
        }
    }

    /// Clears one source's catalog and observed state in one transaction, so a
    /// failure in either data class leaves both untouched.
    pub(crate) fn clear_source_all(
        &self,
        source_name: &str,
    ) -> Result<SearchClearAllResult, SearchStoreError> {
        match &self.backend {
            SearchStoreBackend::Sqlite(store) => {
                let (catalog, observed) = store.clear_source_all(source_name)?;
                Ok(SearchClearAllResult { catalog, observed })
            }
        }
    }

    /// Clears the Workspace's catalog and observed state in one transaction.
    pub(crate) fn clear_workspace_all(&self) -> Result<SearchClearAllResult, SearchStoreError> {
        match &self.backend {
            SearchStoreBackend::Sqlite(store) => {
                let (catalog, observed) = store.clear_workspace_all()?;
                Ok(SearchClearAllResult { catalog, observed })
            }
        }
    }

    /// Reclaims storage after a clear. Best effort: the result reports what
    /// completed instead of failing the clear that already committed.
    pub(crate) fn compact_after_clear(&self) -> SearchStorageCleanupResult {
        match &self.backend {
            SearchStoreBackend::Sqlite(store) => {
                sqlite::storage_cleanup_result(&store.compact_after_clear())
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum SearchStoreError {
    #[error(transparent)]
    Sqlite(#[from] SqliteSearchError),
}

impl SearchStoreError {
    /// Another writer holds the store; the caller may serve cached state.
    pub(crate) fn is_lock_contention(&self) -> bool {
        match self {
            Self::Sqlite(error) => error.is_lock_contention(),
        }
    }

    pub(crate) fn is_storage_exhaustion(&self) -> bool {
        match self {
            Self::Sqlite(error) => error.is_storage_exhaustion(),
        }
    }

    /// The backend cannot serve search at all: a required capability is
    /// missing, or the stored schema is newer than this binary supports.
    pub(crate) fn is_unsupported(&self) -> bool {
        match self {
            Self::Sqlite(error) => matches!(
                error,
                SqliteSearchError::UnsupportedCapability { .. }
                    | SqliteSearchError::UnsupportedSchemaVersion { .. }
            ),
        }
    }
}

/// Maps a storage failure during search maintenance onto the app error
/// taxonomy the maintenance RPCs expose.
pub(crate) fn search_maintenance_app_error(error: &SearchStoreError) -> AppError {
    if error.is_lock_contention() {
        AppError::Unavailable(format!("search maintenance storage is busy: {error}"))
    } else if error.is_storage_exhaustion() {
        AppError::ResourceExhausted(format!("search maintenance storage is exhausted: {error}"))
    } else if error.is_unsupported() {
        AppError::FailedPrecondition(format!("search maintenance is not supported: {error}"))
    } else {
        AppError::Internal(format!("search maintenance storage failed: {error}"))
    }
}

#[cfg(test)]
mod tests;
