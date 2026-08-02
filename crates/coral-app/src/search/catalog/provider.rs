//! Catalog metadata Universal Search provider.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use coral_engine::{CatalogInfo, TableFunctionInfo, TableInfo};

use crate::bootstrap::AppError;
use crate::catalog::model::CatalogResolution;
use crate::query::manager::QueryManagerError;
use crate::search::catalog::snapshot::{
    CatalogSearchSnapshot, field_role_from_str, surface_kind_from_str,
};
use crate::search::catalog::sqlite_index::{
    CatalogDocumentClass, CatalogRefreshResult, CatalogSearchHit,
};
use crate::search::maintenance::{
    CatalogClearMaintenanceResult, CatalogRebuildMaintenanceResult, SearchClearTarget,
    SearchDataScope, SearchMaintenanceDetail, SearchMaintenanceResult, SearchMaintenanceState,
    SearchProviderClearOutcome, SearchProviderClearRequest, SearchStorageCleanupResult,
};
use crate::search::provider::{
    LocalSearchWriteCoordinator, PreparedRetrievers, ProviderFailure, Retriever, RetrieverError,
    SearchExecutionContext, SearchProvider,
};
use crate::search::result::{
    CatalogSurface, Field, FieldRef, FieldRole, MatchEvidence, ProviderCoverage, RetrieverId,
    SearchManagerError, SearchProviderKind, SearchProviderState, SearchRequest, SearchResult,
    SearchSurfaceId, SearchSurfaceKind, SurfaceMatch, SurfaceShape,
};
use crate::search::sqlite_store::{
    SqliteSearchCompactionResult, SqliteSearchError, SqliteSearchStore,
};
use crate::state::AppStateLayout;

const CATALOG_PROVIDER_RETRIEVAL_MULTIPLIER: usize = 5;
const CATALOG_PROVIDER_MIN_RETRIEVAL_LIMIT: usize = 25;

impl SearchProvider for CatalogMetadataProvider {
    fn kind(&self) -> SearchProviderKind {
        SearchProviderKind::CatalogMetadata
    }

    fn retrievers(
        &self,
        context: &SearchExecutionContext,
    ) -> Result<PreparedRetrievers, ProviderFailure> {
        self.prepared_retrievers(context)
    }
}

struct CatalogProjection {
    store: SqliteSearchStore,
    refresh: CatalogRefreshResult,
    stale_index: bool,
    refresh_lock_error: Option<String>,
    expected_document_count: u32,
}

struct CatalogProjectionState {
    refresh: CatalogRefreshResult,
    stale_index: bool,
    refresh_lock_error: Option<String>,
    expected_document_count: u32,
}

#[derive(Clone)]
pub(crate) struct CatalogMetadataProvider {
    layout: AppStateLayout,
    write_coordinator: LocalSearchWriteCoordinator,
}

impl CatalogMetadataProvider {
    pub(crate) fn with_write_coordinator(
        layout: AppStateLayout,
        write_coordinator: LocalSearchWriteCoordinator,
    ) -> Self {
        Self {
            layout,
            write_coordinator,
        }
    }

    /// Prepares the projection once, then binds both retrievers to it.
    ///
    /// Only the refresh needs the write coordinator; retrieval is read-only, so
    /// it runs outside the serialised section.
    fn prepared_retrievers(
        &self,
        context: &SearchExecutionContext,
    ) -> Result<PreparedRetrievers, ProviderFailure> {
        let request = &context.request;
        let resolution = context
            .catalog_resolution
            .as_ref()
            .map_err(catalog_query_failure)?;

        let projection = self
            .write_coordinator
            .run(&request.workspace_name, || {
                self.prepare_projection(request, resolution)
            })
            .map_err(|error| catalog_index_failure(&error))?;

        if let Some(failure) = missing_cached_projection_failure(&projection) {
            return Err(failure);
        }

        let limit = usize::try_from(request.limit)
            .unwrap_or(usize::MAX)
            .saturating_mul(CATALOG_PROVIDER_RETRIEVAL_MULTIPLIER)
            .max(CATALOG_PROVIDER_MIN_RETRIEVAL_LIMIT);
        let documents = projection.refresh.document_count;
        let failed_sources = &resolution.failed_source_names;
        let stale = projection.stale_index || !failed_sources.is_empty();
        let degraded = stale.then(|| degraded_note(&projection, failed_sources));

        let retrieval_limited = Arc::new(AtomicBool::new(false));
        Ok(PreparedRetrievers {
            retrievers: vec![
                Box::new(EntryRetriever {
                    store: projection.store.clone(),
                    limit,
                    limited: Arc::clone(&retrieval_limited),
                }),
                Box::new(FieldRetriever {
                    store: projection.store,
                    limit,
                    limited: Arc::clone(&retrieval_limited),
                }),
            ],
            coverage: Some(ProviderCoverage {
                eligible_units: documents,
                searched_units: documents,
                failed_units: u32::try_from(failed_sources.len()).unwrap_or(u32::MAX),
                stale_index: stale,
                ..ProviderCoverage::default()
            }),
            degraded,
            retrieval_limited,
        })
    }

    fn prepare_projection(
        &self,
        request: &SearchRequest,
        resolution: &CatalogResolution,
    ) -> Result<CatalogProjection, SqliteSearchError> {
        let catalog_fingerprint =
            CatalogSearchSnapshot::fingerprint_catalog_with_runtime_schema_owners(
                &resolution.catalog,
                &resolution.runtime_schema_owners,
            );
        let store = SqliteSearchStore::open_workspace(&self.layout, &request.workspace_name)?;
        let capabilities = store.capabilities();
        tracing::debug!(
            workspace = %request.workspace_name,
            sqlite_version = %capabilities.sqlite_version,
            fts5 = capabilities.fts5,
            trigram = capabilities.trigram,
            "using SQLite catalog search store"
        );
        let state =
            Self::prepare_projection_state(request, resolution, &store, &catalog_fingerprint)?;
        tracing::debug!(
            workspace = %request.workspace_name,
            refreshed = state.refresh.refreshed,
            document_count = state.refresh.document_count,
            stale_index = state.stale_index,
            "prepared SQLite catalog search projection"
        );
        Ok(CatalogProjection {
            store,
            refresh: state.refresh,
            stale_index: state.stale_index,
            refresh_lock_error: state.refresh_lock_error,
            expected_document_count: state.expected_document_count,
        })
    }

    fn prepare_projection_state(
        request: &SearchRequest,
        resolution: &CatalogResolution,
        store: &SqliteSearchStore,
        catalog_fingerprint: &str,
    ) -> Result<CatalogProjectionState, SqliteSearchError> {
        if store.catalog_projection_is_current(catalog_fingerprint)? {
            let document_count = store.catalog_document_count()?;
            return Ok(CatalogProjectionState {
                refresh: CatalogRefreshResult {
                    refreshed: false,
                    document_count,
                },
                stale_index: false,
                refresh_lock_error: None,
                expected_document_count: document_count,
            });
        }

        let snapshot = CatalogSearchSnapshot::from_catalog_with_runtime_schema_owners(
            &resolution.catalog,
            &resolution.runtime_schema_owners,
        );
        let expected_document_count = u32::try_from(snapshot.documents.len()).unwrap_or(u32::MAX);
        let index_snapshot = snapshot.index_snapshot();
        match store.refresh_catalog_projection(&index_snapshot) {
            Ok(refresh) => Ok(CatalogProjectionState {
                refresh,
                stale_index: false,
                refresh_lock_error: None,
                expected_document_count,
            }),
            Err(error) if error.is_lock_contention() => {
                tracing::debug!(
                    workspace = %request.workspace_name,
                    error = %error,
                    "using cached SQLite catalog search projection after refresh lock contention"
                );
                let document_count = store.catalog_document_count()?;
                Ok(CatalogProjectionState {
                    refresh: CatalogRefreshResult {
                        refreshed: false,
                        document_count,
                    },
                    stale_index: true,
                    refresh_lock_error: Some(error.to_string()),
                    expected_document_count,
                })
            }
            Err(error) => Err(error),
        }
    }
}

impl CatalogMetadataProvider {
    pub(crate) fn rebuild_index(
        &self,
        workspace_name: &crate::workspaces::WorkspaceName,
        resolution: &CatalogResolution,
        force: bool,
    ) -> Result<SearchMaintenanceResult, SearchManagerError> {
        let snapshot = CatalogSearchSnapshot::from_catalog_with_runtime_schema_owners(
            &resolution.catalog,
            &resolution.runtime_schema_owners,
        )
        .index_snapshot();
        let store = SqliteSearchStore::open_workspace(&self.layout, workspace_name)
            .map_err(|error| search_sqlite_app_error(&error))?;
        let result = store
            .rebuild_catalog_projection(&snapshot, force)
            .map_err(|error| search_sqlite_app_error(&error))?;
        Ok(catalog_rebuild_provider_result(
            result.old_document_count,
            result.new_document_count,
            result.projection_changed,
            result.rebuild_performed,
            force,
        ))
    }

    pub(crate) fn clear_data(
        &self,
        request: SearchProviderClearRequest<'_>,
    ) -> Result<SearchProviderClearOutcome, SearchManagerError> {
        if request.scope != SearchDataScope::All {
            return Err(AppError::InvalidInput(
                "catalog search provider supports only all search-data clear scope".to_string(),
            )
            .into());
        }
        match request.target {
            SearchClearTarget::Workspace => {
                let store = SqliteSearchStore::open_workspace(&self.layout, request.workspace_name)
                    .map_err(|error| search_sqlite_app_error(&error))?;
                let result = store
                    .clear_catalog_workspace()
                    .map_err(|error| search_sqlite_app_error(&error))?;
                Ok(SearchProviderClearOutcome {
                    result: catalog_clear_provider_result(result.deleted_document_count),
                    storage_cleanup: request.compact_after_clear.then(|| {
                        let compaction = store.compact_after_clear();
                        search_storage_cleanup_result(&compaction)
                    }),
                })
            }
            SearchClearTarget::Source(source_name) => {
                let store = SqliteSearchStore::open_workspace(&self.layout, request.workspace_name)
                    .map_err(|error| search_sqlite_app_error(&error))?;
                let result = store
                    .clear_catalog_source(source_name.as_str())
                    .map_err(|error| search_sqlite_app_error(&error))?;
                Ok(SearchProviderClearOutcome {
                    result: catalog_clear_provider_result(result.deleted_document_count),
                    storage_cleanup: request.compact_after_clear.then(|| {
                        let compaction = store.compact_after_clear();
                        search_storage_cleanup_result(&compaction)
                    }),
                })
            }
        }
    }
}

fn catalog_rebuild_provider_result(
    old_document_count: u32,
    new_document_count: u32,
    projection_changed: bool,
    rebuild_performed: bool,
    force: bool,
) -> SearchMaintenanceResult {
    let note = if rebuild_performed && force {
        "force rebuilt catalog search projection"
    } else if rebuild_performed {
        "rebuilt catalog search projection"
    } else {
        "catalog search projection already current"
    }
    .to_string();
    SearchMaintenanceResult {
        provider: SearchProviderKind::CatalogMetadata,
        state: if rebuild_performed {
            SearchMaintenanceState::Completed
        } else {
            SearchMaintenanceState::Noop
        },
        note,
        detail: Some(SearchMaintenanceDetail::CatalogRebuild(
            CatalogRebuildMaintenanceResult {
                old_document_count,
                new_document_count,
                projection_changed,
                rebuild_performed,
            },
        )),
    }
}

pub(crate) fn catalog_clear_provider_result(
    deleted_document_count: u32,
) -> SearchMaintenanceResult {
    SearchMaintenanceResult {
        provider: SearchProviderKind::CatalogMetadata,
        state: if deleted_document_count == 0 {
            SearchMaintenanceState::Noop
        } else {
            SearchMaintenanceState::Completed
        },
        note: "cleared catalog search projection".to_string(),
        detail: Some(SearchMaintenanceDetail::CatalogClear(
            CatalogClearMaintenanceResult {
                deleted_document_count,
            },
        )),
    }
}

fn search_storage_cleanup_result(
    result: &SqliteSearchCompactionResult,
) -> SearchStorageCleanupResult {
    let (state, note) = match (
        result.wal_checkpoint_truncate_completed,
        result.vacuum_completed,
    ) {
        (true, true) => (
            SearchMaintenanceState::Completed,
            "local search storage cleanup completed",
        ),
        (true, false) | (false, true) => (
            SearchMaintenanceState::Partial,
            "local search storage cleanup partially completed",
        ),
        (false, false) => (
            SearchMaintenanceState::Failed,
            "local search storage cleanup did not complete",
        ),
    };
    if state != SearchMaintenanceState::Completed {
        tracing::warn!(
            wal_checkpoint_truncate_completed = result.wal_checkpoint_truncate_completed,
            vacuum_completed = result.vacuum_completed,
            detail = %result.note,
            "local search storage cleanup did not fully complete"
        );
    }
    SearchStorageCleanupResult {
        state,
        note: note.to_string(),
    }
}

fn search_sqlite_app_error(error: &SqliteSearchError) -> AppError {
    if error.is_lock_contention() {
        AppError::Unavailable(format!("search maintenance storage is busy: {error}"))
    } else if error.is_storage_exhaustion() {
        AppError::ResourceExhausted(format!("search maintenance storage is exhausted: {error}"))
    } else if matches!(
        error,
        SqliteSearchError::UnsupportedCapability { .. }
            | SqliteSearchError::UnsupportedSchemaVersion { .. }
    ) {
        AppError::FailedPrecondition(format!("search maintenance is not supported: {error}"))
    } else {
        AppError::Internal(format!("search maintenance storage failed: {error}"))
    }
}

fn missing_cached_projection_failure(projection: &CatalogProjection) -> Option<ProviderFailure> {
    if !(projection.stale_index
        && projection.refresh.document_count == 0
        && projection.expected_document_count > 0)
    {
        return None;
    }
    Some(ProviderFailure {
        state: SearchProviderState::Error,
        note: format!(
            "Catalog metadata search index is unavailable: refresh could not acquire the SQLite writer lock and no cached projection exists{}",
            projection
                .refresh_lock_error
                .as_deref()
                .map_or_else(String::new, |error| format!(": {error}"))
        ),
        coverage: None,
    })
}

fn degraded_note(projection: &CatalogProjection, failed_source_names: &BTreeSet<String>) -> String {
    let mut notes = Vec::new();
    if projection.stale_index {
        notes.push("serving catalog results from a stale index".to_string());
    }
    if !failed_source_names.is_empty() {
        notes.push(format!(
            "{} source(s) failed to load: {}",
            failed_source_names.len(),
            failed_source_names
                .iter()
                .cloned()
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    notes.join("; ")
}

/// Ranks entries by how well their own name, description, and guide text match.
/// Ranks entries by how well their own name, description, and guide text match.
///
/// Entries get their own candidate window. Sharing one window with field
/// documents starves them: measured, a 50-document shared window holds 45 field
/// documents and yields 7 distinct entries.
struct EntryRetriever {
    store: SqliteSearchStore,
    limit: usize,
    limited: Arc<AtomicBool>,
}

impl Retriever for EntryRetriever {
    fn id(&self) -> RetrieverId {
        RetrieverId::CatalogEntries
    }

    fn retrieve(&self, request: &SearchRequest) -> Result<Vec<SurfaceMatch>, RetrieverError> {
        let hits = self
            .store
            .search_catalog(&request.terms, self.limit, CatalogDocumentClass::Entries)
            .map_err(|error| retriever_error(&error))?;
        record_retrieval_limit(&self.limited, hits.retrieval_limited);
        Ok(hits
            .hits
            .iter()
            .filter_map(surface_id)
            .map(|id| SurfaceMatch {
                id,
                evidence: MatchEvidence::default(),
            })
            .collect())
    }
}

/// Ranks entries by their best-matching field, and carries the matching field
/// names up as evidence for the entry that owns them.
struct FieldRetriever {
    store: SqliteSearchStore,
    limit: usize,
    limited: Arc<AtomicBool>,
}

impl Retriever for FieldRetriever {
    fn id(&self) -> RetrieverId {
        RetrieverId::CatalogFields
    }

    fn retrieve(&self, request: &SearchRequest) -> Result<Vec<SurfaceMatch>, RetrieverError> {
        let hits = self
            .store
            .search_catalog(&request.terms, self.limit, CatalogDocumentClass::Fields)
            .map_err(|error| retriever_error(&error))?;
        record_retrieval_limit(&self.limited, hits.retrieval_limited);
        // Scoring reorders the lane, so an entry takes the position of its
        // best-scoring field rather than its first-retrieved one.
        let mut matches = Vec::<SurfaceMatch>::new();
        for hit in &hits.hits {
            let (Some(id), Some(role)) = (surface_id(hit), field_role_from_str(&hit.field_role))
            else {
                continue;
            };
            let field = FieldRef {
                name: hit.field_name.clone(),
                role,
            };
            // Retrieval order is the ranking, so an entry keeps the position of
            // its best field and accumulates the rest as evidence.
            if let Some(existing) = matches.iter_mut().find(|existing| existing.id == id) {
                if !existing.evidence.matched_fields.contains(&field) {
                    existing.evidence.matched_fields.push(field);
                }
            } else {
                matches.push(SurfaceMatch {
                    id,
                    evidence: MatchEvidence {
                        matched_fields: vec![field],
                        matching_values: Vec::new(),
                    },
                });
            }
        }

        // A field that matched only because its table's name matched is not
        // field-level evidence. Retrieval cannot tell them apart — an exact
        // match on `surface_name` returns every column of that surface, ordered
        // by document id — so without this a query for `channels purpose` shows
        // the alphabetically-first five columns and drops `purpose`.
        for entry in &mut matches {
            entry
                .evidence
                .matched_fields
                .sort_by_key(|field| !query_names_field(&request.terms, &field.name));
        }
        Ok(matches)
    }
}

/// True when a query term is the field's name or one of its identifier parts.
fn query_names_field(terms: &[String], field_name: &str) -> bool {
    let field_name = field_name.to_lowercase();
    terms.iter().any(|term| {
        let term = term.to_lowercase();
        field_name == term
            || field_name
                .split(|ch: char| !ch.is_alphanumeric())
                .any(|part| !part.is_empty() && part == term)
    })
}

fn record_retrieval_limit(limited: &Arc<AtomicBool>, retrieval_limited: bool) {
    if retrieval_limited {
        limited.store(true, Ordering::Relaxed);
    }
}

fn surface_id(hit: &CatalogSearchHit) -> Option<SearchSurfaceId> {
    surface_kind_from_str(&hit.surface_kind).map(|kind| SearchSurfaceId {
        catalog_name: hit.catalog_name.clone(),
        schema_name: hit.source_name.clone(),
        name: hit.surface_name.clone(),
        kind,
    })
}

fn retriever_error(error: &SqliteSearchError) -> RetrieverError {
    RetrieverError {
        note: error.to_string(),
    }
}

/// Number of query-matching fields shown beyond the entry's required ones.
const MATCHING_FIELD_LIMIT: usize = 5;

/// Resolves one fused entry into a result.
///
/// Retrievers only ever emitted an identity and evidence, so this is where
/// catalog metadata is read — once per surviving entry rather than once per
/// matched field, which is what keeps the parent from being repeated.
pub(crate) fn resolve_entry(
    catalog: &CatalogInfo,
    id: &SearchSurfaceId,
    evidence: &MatchEvidence,
    providers: &BTreeSet<SearchProviderKind>,
) -> Option<SearchResult> {
    let id = resolve_surface_id(catalog, id)?;
    let (surface, omitted) = match id.kind {
        SearchSurfaceKind::Table => {
            let table = find_table(
                catalog,
                id.catalog_name.as_deref(),
                &id.schema_name,
                &id.name,
            )?;
            let (fields, omitted) = table_fields(table, evidence);
            (
                CatalogSurface {
                    id,
                    description: table.description.clone(),
                    guide: table.guide.clone(),
                    shape: SurfaceShape::Table { fields },
                },
                omitted,
            )
        }
        SearchSurfaceKind::TableFunction => {
            let function = find_function(catalog, &id.schema_name, &id.name)?;
            let (arguments, returns, omitted) = function_fields(function, evidence);
            (
                CatalogSurface {
                    id,
                    description: function.description.clone(),
                    guide: function.guide.clone(),
                    shape: SurfaceShape::Function { arguments, returns },
                },
                omitted,
            )
        }
    };
    Some(SearchResult {
        surface,
        providers: providers.iter().copied().collect(),
        matching_values: evidence.matching_values.clone(),
        omitted_matching_field_count: u32::try_from(omitted).unwrap_or(u32::MAX),
    })
}

/// Resolves a retriever identity to the catalog's canonical SQL identity.
///
/// Observed-value records created before catalog-qualified discovery do not
/// carry a catalog name. They can still map safely when the schema/table pair
/// is unique; ambiguous pairs stay unresolved and are not returned.
pub(crate) fn resolve_surface_id(
    catalog: &CatalogInfo,
    id: &SearchSurfaceId,
) -> Option<SearchSurfaceId> {
    match id.kind {
        SearchSurfaceKind::Table => {
            let table = find_table(
                catalog,
                id.catalog_name.as_deref(),
                &id.schema_name,
                &id.name,
            )
            .or_else(|| {
                id.catalog_name
                    .is_none()
                    .then(|| unique_catalog_table(catalog, &id.schema_name, &id.name))
                    .flatten()
            })?;
            Some(SearchSurfaceId {
                catalog_name: table.catalog_name.clone(),
                schema_name: table.schema_name.clone(),
                name: table.table_name.clone(),
                kind: SearchSurfaceKind::Table,
            })
        }
        SearchSurfaceKind::TableFunction => {
            let function = find_function(catalog, &id.schema_name, &id.name)?;
            Some(SearchSurfaceId {
                catalog_name: None,
                schema_name: function.schema_name.clone(),
                name: function.function_name.clone(),
                kind: SearchSurfaceKind::TableFunction,
            })
        }
    }
}

fn unique_catalog_table<'a>(
    catalog: &'a CatalogInfo,
    schema_name: &str,
    table_name: &str,
) -> Option<&'a TableInfo> {
    let mut matches = catalog
        .tables
        .iter()
        .filter(|table| table.schema_name == schema_name && table.table_name == table_name);
    let first = matches.next()?;
    matches.next().is_none().then_some(first)
}

/// Required filters always appear; they do not consume a matching-field slot,
/// because an entry you cannot query is not a useful result.
fn table_fields(table: &TableInfo, evidence: &MatchEvidence) -> (Vec<Field>, usize) {
    let required = table.required_filters.iter().collect::<BTreeSet<_>>();
    let mut fields = table
        .columns
        .iter()
        .filter(|column| required.contains(&column.name))
        .map(|column| Field {
            name: column.name.clone(),
            data_type: column.data_type.clone(),
            required: true,
        })
        .collect::<Vec<_>>();
    let mut omitted = 0_usize;
    for matched in &evidence.matched_fields {
        if required.contains(&matched.name) {
            continue;
        }
        let Some(column) = table
            .columns
            .iter()
            .find(|column| column.name == matched.name)
        else {
            continue;
        };
        if fields.len().saturating_sub(required.len()) >= MATCHING_FIELD_LIMIT {
            omitted = omitted.saturating_add(1);
            continue;
        }
        fields.push(Field {
            name: column.name.clone(),
            data_type: column.data_type.clone(),
            required: false,
        });
    }
    (fields, omitted)
}

fn function_fields(
    function: &TableFunctionInfo,
    evidence: &MatchEvidence,
) -> (Vec<Field>, Vec<Field>, usize) {
    let mut arguments = function
        .arguments
        .iter()
        .filter(|argument| argument.required)
        .map(|argument| Field {
            name: argument.name.clone(),
            data_type: argument.data_type.clone(),
            required: true,
        })
        .collect::<Vec<_>>();
    let required_count = arguments.len();
    let mut returns = Vec::new();
    let mut omitted = 0_usize;
    for matched in &evidence.matched_fields {
        let selected = arguments.len().saturating_sub(required_count) + returns.len();
        if selected >= MATCHING_FIELD_LIMIT {
            omitted = omitted.saturating_add(1);
            continue;
        }
        match matched.role {
            FieldRole::Argument => {
                if let Some(argument) = function
                    .arguments
                    .iter()
                    .find(|argument| argument.name == matched.name && !argument.required)
                {
                    arguments.push(Field {
                        name: argument.name.clone(),
                        data_type: argument.data_type.clone(),
                        required: false,
                    });
                }
            }
            FieldRole::ResultColumn => {
                if let Some(column) = function
                    .result_columns
                    .iter()
                    .find(|column| column.name == matched.name)
                {
                    returns.push(Field {
                        name: column.name.clone(),
                        data_type: column.data_type.clone(),
                        required: false,
                    });
                }
            }
            FieldRole::Column | FieldRole::Filter => {}
        }
    }
    (arguments, returns, omitted)
}

fn find_table<'a>(
    catalog: &'a CatalogInfo,
    catalog_name: Option<&str>,
    schema_name: &str,
    table_name: &str,
) -> Option<&'a TableInfo> {
    catalog.tables.iter().find(|table| {
        table.catalog_name.as_deref() == catalog_name
            && table.schema_name == schema_name
            && table.table_name == table_name
    })
}

fn find_function<'a>(
    catalog: &'a CatalogInfo,
    schema_name: &str,
    function_name: &str,
) -> Option<&'a TableFunctionInfo> {
    catalog.table_functions.iter().find(|function| {
        function.schema_name == schema_name && function.function_name == function_name
    })
}

fn catalog_query_failure(error: &QueryManagerError) -> ProviderFailure {
    ProviderFailure {
        state: SearchProviderState::Error,
        note: format!("catalog metadata is unavailable: {error:?}"),
        coverage: None,
    }
}

fn catalog_index_failure(error: &SqliteSearchError) -> ProviderFailure {
    ProviderFailure {
        state: SearchProviderState::Error,
        note: format!("catalog metadata search index is unavailable: {error}"),
        coverage: None,
    }
}

#[cfg(test)]
mod tests {
    use coral_engine::{CatalogInfo, TableInfo};

    use super::resolve_surface_id;
    use crate::search::result::{SearchSurfaceId, SearchSurfaceKind};

    #[test]
    fn unqualified_identity_resolves_one_catalog_backed_table() {
        let catalog = CatalogInfo {
            tables: vec![table(Some("warehouse"))],
            table_functions: Vec::new(),
        };

        let resolved = resolve_surface_id(&catalog, &unqualified_id()).expect("unique table");

        assert_eq!(resolved.catalog_name.as_deref(), Some("warehouse"));
    }

    #[test]
    fn unqualified_identity_rejects_ambiguous_catalog_backed_tables() {
        let catalog = CatalogInfo {
            tables: vec![table(Some("primary")), table(Some("archive"))],
            table_functions: Vec::new(),
        };

        assert!(resolve_surface_id(&catalog, &unqualified_id()).is_none());
    }

    fn table(catalog_name: Option<&str>) -> TableInfo {
        TableInfo {
            catalog_name: catalog_name.map(str::to_string),
            schema_name: "analytics".to_string(),
            table_name: "events".to_string(),
            description: String::new(),
            guide: String::new(),
            require_guide_read: false,
            columns: Vec::new(),
            required_filters: Vec::new(),
        }
    }

    fn unqualified_id() -> SearchSurfaceId {
        SearchSurfaceId {
            catalog_name: None,
            schema_name: "analytics".to_string(),
            name: "events".to_string(),
            kind: SearchSurfaceKind::Table,
        }
    }
}
