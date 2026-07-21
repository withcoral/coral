//! Catalog metadata Universal Search provider.

use std::collections::{BTreeMap, BTreeSet};

use coral_engine::{CatalogInfo, TableFunctionInfo, TableInfo};

use crate::bootstrap::AppError;
use crate::catalog::discovery::CatalogItem;
use crate::catalog::model::CatalogResolution;
use crate::query::manager::QueryManagerError;
use crate::search::catalog::ranking::{RankedCatalogHit, rank_catalog_hits};
use crate::search::catalog::snapshot::{
    CatalogSearchSnapshot, field_role_from_str, surface_kind_from_str,
};
use crate::search::catalog::sqlite_index::{
    CatalogIndexDocumentKind, CatalogRefreshResult, CatalogSearchHit, CatalogSearchHits,
};
use crate::search::maintenance::{
    CatalogClearMaintenanceResult, CatalogRebuildMaintenanceResult, SearchClearTarget,
    SearchDataScope, SearchMaintenanceDetail, SearchMaintenanceResult, SearchMaintenanceState,
    SearchProviderClearOutcome, SearchProviderClearRequest, SearchStorageCleanupResult,
};
use crate::search::provider::ProviderSearchOutcome;
use crate::search::result::{
    CatalogMetadataResult, ColumnHintResult, ProviderCoverage, ProviderStatus, SearchCandidate,
    SearchFieldRole, SearchManagerError, SearchPayload, SearchProviderKind, SearchProviderState,
    SearchRequest, SearchSurfaceKind, TableColumnPreview, TableColumnPreviewColumn,
};
use crate::search::sqlite_store::{
    SqliteSearchCompactionResult, SqliteSearchError, SqliteSearchStore,
};
use crate::state::AppStateLayout;

const CATALOG_PROVIDER_RETRIEVAL_MULTIPLIER: usize = 5;
const CATALOG_PROVIDER_MIN_RETRIEVAL_LIMIT: usize = 25;
const MAX_COLUMN_HINTS_PER_SURFACE: usize = 3;
const TABLE_COLUMN_PREVIEW_LIMIT: usize = 5;

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
}

impl CatalogMetadataProvider {
    pub(crate) fn new(layout: AppStateLayout) -> Self {
        Self { layout }
    }

    pub(crate) fn search(
        &self,
        request: &SearchRequest,
        resolution: Result<&CatalogResolution, &QueryManagerError>,
    ) -> ProviderSearchOutcome {
        let resolution = match resolution {
            Ok(resolution) => resolution,
            Err(error) => return catalog_query_error_outcome(error),
        };
        let projection = match self.prepare_projection(request, resolution) {
            Ok(projection) => projection,
            Err(error) => return catalog_index_error_outcome(&error),
        };
        let search_hits = match Self::search_projection(request, &projection.store) {
            Ok(search_hits) => search_hits,
            Err(error) => return catalog_index_error_outcome(&error),
        };
        if let Some(outcome) = missing_cached_projection_outcome(&projection, &search_hits) {
            return outcome;
        }

        catalog_search_outcome(
            request,
            &resolution.catalog,
            &resolution.failed_source_names,
            search_hits,
            &projection,
        )
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

    fn search_projection(
        request: &SearchRequest,
        store: &SqliteSearchStore,
    ) -> Result<CatalogSearchHits, SqliteSearchError> {
        let retrieval_limit = usize::try_from(request.limit)
            .unwrap_or(usize::MAX)
            .saturating_mul(CATALOG_PROVIDER_RETRIEVAL_MULTIPLIER)
            .max(CATALOG_PROVIDER_MIN_RETRIEVAL_LIMIT);
        store.search_catalog(&request.terms, retrieval_limit)
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

fn catalog_search_outcome(
    request: &SearchRequest,
    catalog: &CatalogInfo,
    failed_source_names: &BTreeSet<String>,
    search_hits: CatalogSearchHits,
    projection: &CatalogProjection,
) -> ProviderSearchOutcome {
    let retrieved_hit_count = search_hits.hits.len();
    let ranked_hits = rank_catalog_hits(search_hits.hits, &request.terms);
    let candidate_set = catalog_candidates_from_hits(catalog, ranked_hits);
    let candidate_count = candidate_set.candidates.len();
    if retrieved_hit_count != candidate_count {
        tracing::debug!(
            workspace = %request.workspace_name,
            retrieved_hit_count,
            candidate_count,
            omitted_column_hint_count = candidate_set.omitted_column_hint_count,
            "mapped SQLite catalog hits into provider candidates"
        );
    }
    let has_more = search_hits.retrieval_limited || candidate_set.omitted_column_hint_count > 0;
    let has_failures = !failed_source_names.is_empty();
    let state = if has_more || has_failures {
        SearchProviderState::Partial
    } else if candidate_set.candidates.is_empty() {
        SearchProviderState::Empty
    } else {
        SearchProviderState::ResultsFound
    };
    let note = catalog_provider_note(
        state,
        candidate_count,
        projection.refresh.refreshed,
        projection.stale_index,
        search_hits.retrieval_limited,
        candidate_set.omitted_column_hint_count,
        failed_source_names,
    );
    ProviderSearchOutcome {
        candidates: candidate_set.candidates,
        status: ProviderStatus {
            provider: SearchProviderKind::CatalogMetadata,
            state,
            note,
            coverage: Some(ProviderCoverage {
                eligible_units: search_hits.document_count,
                searched_units: search_hits.document_count,
                failed_units: u32::try_from(failed_source_names.len()).unwrap_or(u32::MAX),
                returned_count: u32::try_from(candidate_count).unwrap_or(u32::MAX),
                has_more,
                stale_index: projection.stale_index || has_failures,
                ..ProviderCoverage::default()
            }),
        },
    }
}

fn missing_cached_projection_outcome(
    projection: &CatalogProjection,
    search_hits: &CatalogSearchHits,
) -> Option<ProviderSearchOutcome> {
    if !(projection.stale_index
        && search_hits.document_count == 0
        && projection.expected_document_count > 0)
    {
        return None;
    }
    Some(catalog_index_note_outcome(format!(
        "Catalog metadata search index is unavailable: refresh could not acquire the SQLite writer lock and no cached projection exists{}",
        projection
            .refresh_lock_error
            .as_deref()
            .map_or_else(String::new, |error| format!(": {error}"))
    )))
}

fn catalog_candidates_from_hits(
    catalog: &CatalogInfo,
    hits: Vec<RankedCatalogHit>,
) -> CatalogCandidateSet {
    let mut candidates = Vec::new();
    let mut column_hints_by_surface = BTreeMap::<(SearchSurfaceKind, String, String), usize>::new();
    let mut omitted_column_hint_count = 0;

    for ranked_hit in hits {
        let hit = ranked_hit.hit;
        match hit.doc_kind {
            CatalogIndexDocumentKind::CatalogTable => {
                if let Some(table) = find_table(catalog, &hit.source_name, &hit.surface_name) {
                    candidates.push(table_candidate(table, &hit, ranked_hit.score));
                }
            }
            CatalogIndexDocumentKind::CatalogTableFunction => {
                if let Some(function) = find_function(catalog, &hit.source_name, &hit.surface_name)
                {
                    candidates.push(table_function_candidate(function, &hit, ranked_hit.score));
                }
            }
            CatalogIndexDocumentKind::ColumnHint => {
                let Some(surface_kind) = surface_kind_from_str(&hit.surface_kind) else {
                    continue;
                };
                let count_key = (
                    surface_kind,
                    hit.source_name.clone(),
                    hit.surface_name.clone(),
                );
                let count = column_hints_by_surface.entry(count_key).or_default();
                if *count >= MAX_COLUMN_HINTS_PER_SURFACE {
                    omitted_column_hint_count += 1;
                    continue;
                }
                candidates.push(column_hint_candidate(
                    catalog,
                    &hit,
                    surface_kind,
                    ranked_hit.score,
                ));
                *count += 1;
            }
        }
    }

    CatalogCandidateSet {
        candidates,
        omitted_column_hint_count,
    }
}

struct CatalogCandidateSet {
    candidates: Vec<SearchCandidate>,
    omitted_column_hint_count: usize,
}

fn table_candidate(table: &TableInfo, hit: &CatalogSearchHit, score: u32) -> SearchCandidate {
    SearchCandidate {
        key: hit.doc_id.clone(),
        score,
        provider: SearchProviderKind::CatalogMetadata,
        payload: SearchPayload::CatalogMetadata(CatalogMetadataResult {
            item: CatalogItem::Table(table_summary(table)),
            matched_fields: hit.matched_fields.clone(),
            table_column_preview: Some(table_column_preview(table)),
        }),
    }
}

fn table_function_candidate(
    function: &TableFunctionInfo,
    hit: &CatalogSearchHit,
    score: u32,
) -> SearchCandidate {
    SearchCandidate {
        key: hit.doc_id.clone(),
        score,
        provider: SearchProviderKind::CatalogMetadata,
        payload: SearchPayload::CatalogMetadata(CatalogMetadataResult {
            item: CatalogItem::TableFunction(function.clone()),
            matched_fields: hit.matched_fields.clone(),
            table_column_preview: None,
        }),
    }
}

fn column_hint_candidate(
    catalog: &CatalogInfo,
    hit: &CatalogSearchHit,
    surface_kind: SearchSurfaceKind,
    score: u32,
) -> SearchCandidate {
    let metadata = column_hint_metadata(catalog, hit, surface_kind);
    SearchCandidate {
        key: hit.doc_id.clone(),
        score,
        provider: SearchProviderKind::CatalogMetadata,
        payload: SearchPayload::ColumnHint(ColumnHintResult {
            schema_name: hit.source_name.clone(),
            surface_name: hit.surface_name.clone(),
            surface_kind,
            name: hit.field_name.clone(),
            data_type: metadata.data_type,
            required: metadata.required,
            description: metadata.description,
            matched_fields: hit.matched_fields.clone(),
            field_role: metadata.field_role,
        }),
    }
}

struct ColumnHintMetadata {
    data_type: String,
    required: bool,
    description: String,
    field_role: SearchFieldRole,
}

fn column_hint_metadata(
    catalog: &CatalogInfo,
    hit: &CatalogSearchHit,
    surface_kind: SearchSurfaceKind,
) -> ColumnHintMetadata {
    let fallback_role = match surface_kind {
        SearchSurfaceKind::Table => SearchFieldRole::TableColumn,
        SearchSurfaceKind::TableFunction => SearchFieldRole::TableFunctionResultColumn,
    };
    let field_role = field_role_from_str(&hit.field_role).unwrap_or(fallback_role);
    match (surface_kind, field_role) {
        (SearchSurfaceKind::Table, SearchFieldRole::TableColumn) => {
            find_table(catalog, &hit.source_name, &hit.surface_name)
                .and_then(|table| {
                    table
                        .columns
                        .iter()
                        .find(|column| column.name == hit.field_name)
                })
                .map_or_else(
                    || fallback_column_hint_metadata(hit, field_role),
                    |column| ColumnHintMetadata {
                        data_type: column.data_type.clone(),
                        required: column.is_required_filter,
                        description: column.description.clone(),
                        field_role,
                    },
                )
        }
        (SearchSurfaceKind::Table, SearchFieldRole::TableFilter) => {
            let column =
                find_table(catalog, &hit.source_name, &hit.surface_name).and_then(|table| {
                    table
                        .columns
                        .iter()
                        .find(|column| column.name == hit.field_name)
                });
            ColumnHintMetadata {
                data_type: column.map_or_else(String::new, |column| column.data_type.clone()),
                required: true,
                description: column.map_or_else(
                    || "Required table filter".to_string(),
                    |column| column.description.clone(),
                ),
                field_role,
            }
        }
        (SearchSurfaceKind::TableFunction, SearchFieldRole::TableFunctionArgument) => {
            find_function(catalog, &hit.source_name, &hit.surface_name)
                .and_then(|function| {
                    function
                        .arguments
                        .iter()
                        .find(|argument| argument.name == hit.field_name)
                })
                .map_or_else(
                    || fallback_column_hint_metadata(hit, field_role),
                    |argument| ColumnHintMetadata {
                        data_type: String::new(),
                        required: argument.required,
                        description: "Table function argument".to_string(),
                        field_role,
                    },
                )
        }
        (SearchSurfaceKind::TableFunction, SearchFieldRole::TableFunctionResultColumn) => {
            find_function(catalog, &hit.source_name, &hit.surface_name)
                .and_then(|function| {
                    function
                        .result_columns
                        .iter()
                        .find(|column| column.name == hit.field_name)
                })
                .map_or_else(
                    || fallback_column_hint_metadata(hit, field_role),
                    |column| ColumnHintMetadata {
                        data_type: column.data_type.clone(),
                        required: false,
                        description: column.description.clone(),
                        field_role,
                    },
                )
        }
        _ => fallback_column_hint_metadata(hit, field_role),
    }
}

fn fallback_column_hint_metadata(
    hit: &CatalogSearchHit,
    field_role: SearchFieldRole,
) -> ColumnHintMetadata {
    ColumnHintMetadata {
        data_type: String::new(),
        required: matches!(field_role, SearchFieldRole::TableFilter),
        description: hit.description.clone(),
        field_role,
    }
}

fn table_summary(table: &TableInfo) -> TableInfo {
    let mut table = table.clone();
    table.columns.clear();
    table
}

fn table_column_preview(table: &TableInfo) -> TableColumnPreview {
    let columns = table
        .columns
        .iter()
        .take(TABLE_COLUMN_PREVIEW_LIMIT)
        .cloned()
        .map(|column| TableColumnPreviewColumn {
            column,
            matched_fields: Vec::new(),
        })
        .collect::<Vec<_>>();
    let column_count = u32::try_from(table.columns.len()).unwrap_or(u32::MAX);
    let omitted_column_count = table.columns.len().saturating_sub(columns.len());
    TableColumnPreview {
        column_count,
        columns,
        omitted_column_count: u32::try_from(omitted_column_count).unwrap_or(u32::MAX),
    }
}

fn find_table<'a>(
    catalog: &'a CatalogInfo,
    schema_name: &str,
    table_name: &str,
) -> Option<&'a TableInfo> {
    catalog
        .tables
        .iter()
        .find(|table| table.schema_name == schema_name && table.table_name == table_name)
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

fn catalog_query_error_outcome(error: &QueryManagerError) -> ProviderSearchOutcome {
    let detail = match error {
        QueryManagerError::App(error) => error.to_string(),
        QueryManagerError::Core(error) => error.to_string(),
    };
    ProviderSearchOutcome {
        candidates: Vec::new(),
        status: ProviderStatus {
            provider: SearchProviderKind::CatalogMetadata,
            state: SearchProviderState::Error,
            note: format!("Workspace catalog is unavailable: {detail}"),
            coverage: Some(ProviderCoverage::default()),
        },
    }
}

fn catalog_index_error_outcome(error: &SqliteSearchError) -> ProviderSearchOutcome {
    catalog_index_note_outcome(format!(
        "Catalog metadata search index is unavailable: {error}"
    ))
}

fn catalog_index_note_outcome(note: String) -> ProviderSearchOutcome {
    ProviderSearchOutcome {
        candidates: Vec::new(),
        status: ProviderStatus {
            provider: SearchProviderKind::CatalogMetadata,
            state: SearchProviderState::Error,
            note,
            coverage: Some(ProviderCoverage::default()),
        },
    }
}

fn catalog_provider_note(
    state: SearchProviderState,
    total_count: usize,
    refreshed: bool,
    stale_index: bool,
    retrieval_limited: bool,
    omitted_column_hint_count: usize,
    failed_source_names: &BTreeSet<String>,
) -> String {
    let refresh_note = if stale_index {
        " from cached SQLite projection because refresh is currently locked"
    } else if refreshed {
        " after refreshing the SQLite projection"
    } else {
        ""
    };
    match state {
        SearchProviderState::ResultsFound => {
            format!("Catalog metadata returned {total_count} search hints{refresh_note}")
        }
        SearchProviderState::Partial => {
            let reason = partial_catalog_provider_reason(
                retrieval_limited,
                omitted_column_hint_count,
                failed_source_names,
            );
            format!("Catalog metadata returned {total_count} search hints; {reason}{refresh_note}")
        }
        SearchProviderState::Empty => {
            format!("Catalog metadata returned no search hints{refresh_note}")
        }
        SearchProviderState::NotEnabled
        | SearchProviderState::Skipped
        | SearchProviderState::Error => String::new(),
    }
}

fn partial_catalog_provider_reason(
    retrieval_limited: bool,
    omitted_column_hint_count: usize,
    failed_source_names: &BTreeSet<String>,
) -> String {
    const MAX_FAILED_SOURCE_NAMES_IN_NOTE: usize = 3;

    let mut reasons = Vec::new();
    if retrieval_limited {
        reasons.push("local retrieval cap was reached".to_string());
    }
    if omitted_column_hint_count > 0 {
        reasons.push(format!(
            "{omitted_column_hint_count} matching column hint(s) exceeded the per-surface cap"
        ));
    }
    if !failed_source_names.is_empty() {
        let displayed_names = failed_source_names
            .iter()
            .take(MAX_FAILED_SOURCE_NAMES_IN_NOTE)
            .cloned()
            .collect::<Vec<_>>()
            .join(", ");
        let omitted_count = failed_source_names
            .len()
            .saturating_sub(MAX_FAILED_SOURCE_NAMES_IN_NOTE);
        let omitted_note = if omitted_count > 0 {
            format!(", and {omitted_count} more")
        } else {
            String::new()
        };
        let source_label = if failed_source_names.len() == 1 {
            "source"
        } else {
            "sources"
        };
        reasons.push(format!(
            "catalog preparation skipped {} {source_label}: {displayed_names}{omitted_note}",
            failed_source_names.len(),
        ));
    }
    if reasons.is_empty() {
        reasons.push("partial results were returned".to_string());
    }
    reasons.join("; ")
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use coral_engine::{CatalogInfo, ColumnInfo, TableInfo};
    use tempfile::tempdir;

    use super::{
        CatalogProjection, MAX_COLUMN_HINTS_PER_SURFACE, catalog_provider_note,
        catalog_search_outcome, search_sqlite_app_error,
    };
    use crate::bootstrap::AppError;
    use crate::search::catalog::sqlite_index::{
        CatalogIndexDocumentKind, CatalogRefreshResult, CatalogSearchHit, CatalogSearchHits,
    };
    use crate::search::result::{SearchProviderState, SearchRequest};
    use crate::search::sqlite_store::{SqliteSearchError, SqliteSearchStore};
    use crate::workspaces::WorkspaceName;

    #[test]
    fn catalog_provider_note_reports_cached_projection_fallback() {
        let note = catalog_provider_note(
            SearchProviderState::ResultsFound,
            3,
            false,
            true,
            false,
            0,
            &BTreeSet::new(),
        );

        assert_eq!(
            note,
            "Catalog metadata returned 3 search hints from cached SQLite projection because refresh is currently locked"
        );
    }

    #[test]
    fn maintenance_error_mapping_preserves_retry_and_failure_categories() {
        assert!(matches!(
            search_sqlite_app_error(&sqlite_failure(rusqlite::ffi::SQLITE_BUSY)),
            AppError::Unavailable(_)
        ));
        assert!(matches!(
            search_sqlite_app_error(&sqlite_failure(rusqlite::ffi::SQLITE_FULL)),
            AppError::ResourceExhausted(_)
        ));
        assert!(matches!(
            search_sqlite_app_error(&SqliteSearchError::Io(std::io::Error::new(
                std::io::ErrorKind::StorageFull,
                "fixture disk full",
            ))),
            AppError::ResourceExhausted(_)
        ));
        assert!(matches!(
            search_sqlite_app_error(&sqlite_failure(rusqlite::ffi::SQLITE_CORRUPT)),
            AppError::Internal(_)
        ));
        assert!(matches!(
            search_sqlite_app_error(&SqliteSearchError::UnsupportedCapability {
                feature: "FTS5",
                sqlite_version: "fixture".to_string(),
            }),
            AppError::FailedPrecondition(_)
        ));
        assert!(matches!(
            search_sqlite_app_error(&SqliteSearchError::Io(std::io::Error::other(
                "fixture storage failure"
            ))),
            AppError::Internal(_)
        ));
    }

    #[test]
    fn column_hint_cap_reports_partial_provider_coverage() {
        let temp = tempdir().expect("tempdir");
        let workspace_name = WorkspaceName::default();
        let projection = CatalogProjection {
            store: SqliteSearchStore::open(
                temp.path().join("search.sqlite3"),
                workspace_name.clone(),
            )
            .expect("search store"),
            refresh: CatalogRefreshResult {
                refreshed: false,
                document_count: 5,
            },
            stale_index: false,
            refresh_lock_error: None,
            expected_document_count: 5,
        };
        let request = SearchRequest::new(workspace_name, "alpha", 10).expect("search request");
        let catalog = column_cap_catalog(5);
        let outcome = catalog_search_outcome(
            &request,
            &catalog,
            &BTreeSet::new(),
            CatalogSearchHits {
                hits: column_cap_hits(5),
                document_count: 5,
                retrieval_limited: false,
            },
            &projection,
        );

        assert_eq!(outcome.candidates.len(), MAX_COLUMN_HINTS_PER_SURFACE);
        assert_eq!(outcome.status.state, SearchProviderState::Partial);
        assert!(outcome.status.coverage.as_ref().expect("coverage").has_more);
        assert!(
            outcome
                .status
                .note
                .contains("2 matching column hint(s) exceeded the per-surface cap")
        );
    }

    #[test]
    fn skipped_sources_report_partial_coverage_without_hiding_healthy_results() {
        let temp = tempdir().expect("tempdir");
        let workspace_name = WorkspaceName::default();
        let projection = CatalogProjection {
            store: SqliteSearchStore::open(
                temp.path().join("search.sqlite3"),
                workspace_name.clone(),
            )
            .expect("search store"),
            refresh: CatalogRefreshResult {
                refreshed: false,
                document_count: 1,
            },
            stale_index: false,
            refresh_lock_error: None,
            expected_document_count: 1,
        };
        let request = SearchRequest::new(workspace_name, "alpha", 10).expect("search request");
        let catalog = column_cap_catalog(1);
        let failed_source_names = BTreeSet::from(["broken_source".to_string()]);
        let degraded = catalog_search_outcome(
            &request,
            &catalog,
            &failed_source_names,
            CatalogSearchHits {
                hits: column_cap_hits(1),
                document_count: 1,
                retrieval_limited: false,
            },
            &projection,
        );

        assert_eq!(degraded.candidates.len(), 1);
        assert_eq!(degraded.status.state, SearchProviderState::Partial);
        let coverage = degraded.status.coverage.as_ref().expect("coverage");
        assert_eq!(coverage.failed_units, 1);
        assert!(coverage.stale_index);
        assert!(!coverage.has_more);
        assert_eq!(
            degraded.status.note,
            "Catalog metadata returned 1 search hints; catalog preparation skipped 1 source: broken_source"
        );

        let recovered = catalog_search_outcome(
            &request,
            &catalog,
            &BTreeSet::new(),
            CatalogSearchHits {
                hits: column_cap_hits(1),
                document_count: 1,
                retrieval_limited: false,
            },
            &projection,
        );
        assert_eq!(recovered.status.state, SearchProviderState::ResultsFound);
        let coverage = recovered.status.coverage.as_ref().expect("coverage");
        assert_eq!(coverage.failed_units, 0);
        assert!(!coverage.stale_index);
    }

    #[test]
    fn skipped_source_note_bounds_source_names() {
        let failed_source_names = BTreeSet::from([
            "alpha".to_string(),
            "bravo".to_string(),
            "charlie".to_string(),
            "delta".to_string(),
            "echo".to_string(),
        ]);

        let note = catalog_provider_note(
            SearchProviderState::Partial,
            1,
            false,
            false,
            false,
            0,
            &failed_source_names,
        );

        assert_eq!(
            note,
            "Catalog metadata returned 1 search hints; catalog preparation skipped 5 sources: alpha, bravo, charlie, and 2 more"
        );
    }

    fn sqlite_failure(code: i32) -> SqliteSearchError {
        SqliteSearchError::Sqlite(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(code),
            None,
        ))
    }

    fn column_cap_catalog(column_count: usize) -> CatalogInfo {
        CatalogInfo {
            tables: vec![TableInfo {
                catalog_name: String::new(),
                schema_name: "fixture".to_string(),
                table_name: "payments".to_string(),
                description: "Payments".to_string(),
                guide: String::new(),
                columns: (0..column_count)
                    .map(|index| ColumnInfo {
                        name: format!("alpha_{index}"),
                        data_type: "Utf8".to_string(),
                        nullable: true,
                        is_virtual: false,
                        is_required_filter: false,
                        description: format!("Alpha {index}"),
                        ordinal_position: u32::try_from(index).unwrap_or(u32::MAX),
                    })
                    .collect(),
                required_filters: Vec::new(),
            }],
            table_functions: Vec::new(),
        }
    }

    fn column_cap_hits(column_count: usize) -> Vec<CatalogSearchHit> {
        (0..column_count)
            .map(|index| CatalogSearchHit {
                doc_id: format!("column:table:fixture.payments:alpha_{index}"),
                doc_kind: CatalogIndexDocumentKind::ColumnHint,
                source_name: "fixture".to_string(),
                surface_kind: "table".to_string(),
                surface_name: "payments".to_string(),
                field_name: format!("alpha_{index}"),
                field_role: "table_column".to_string(),
                description: format!("Alpha {index}"),
                matched_fields: vec!["field_name".to_string()],
                retrieval_score: 1,
            })
            .collect()
    }
}
