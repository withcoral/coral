//! Implements the gRPC `SearchService`.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::sync::Arc;

use chrono::{Duration, SecondsFormat, Utc};
use coral_api::v1::search_result::Payload;
use coral_api::v1::search_service_server::SearchService as SearchServiceApi;
use coral_api::v1::{
    CatalogMetadata, ColumnHint, NativeSearchPath, ObservedValue, SearchFieldRole, SearchProvider,
    SearchProviderState, SearchProviderStatus, SearchRequest, SearchResponse, SearchResult,
    SearchResultTruncation, SearchSurfaceKind, SearchTableColumnPreview,
    SearchTableColumnPreviewColumn,
};
use coral_engine::{
    CatalogInfo, ColumnInfo, TableFilterInfo, TableFunctionArgumentInfo, TableFunctionInfo,
    TableFunctionResultColumnInfo, TableInfo,
};
use sha2::{Digest as _, Sha256};
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::bootstrap::{AppError, app_status};
use crate::query::manager::{QueryManager, QueryManagerError};
use crate::search::index::{
    CatalogSearchFieldRole, CatalogSearchHit, CatalogSearchResultType, CatalogSearchSurfaceKind,
    ObservedValueSearchHit, ObservedValueSurfaceKind, SearchIndexError, SearchIndexStore,
};
use crate::sources::SourceName;
use crate::state::AppStateLayout;
use crate::transport::{
    catalog_item_to_proto, grpc_span, instrument_grpc, query_status, table_function_to_proto,
    workspace_name_from_proto, workspace_to_proto,
};
use crate::workspaces::WorkspaceName;
use tokio::sync::Mutex;

const DEFAULT_SEARCH_LIMIT: u32 = 50;
const MAX_SEARCH_LIMIT: u32 = 100;
const MAX_QUERY_BYTES: usize = 512;
const COLUMN_PREVIEW_LIMIT: usize = 8;
const MAX_COLUMN_HINTS_PER_SURFACE: usize = 2;
const SOURCE_EXACT_MATCH_BOOST: u32 = 2_000;
const SURFACE_NAME_EXACT_BOOST: u32 = 1_500;
const SURFACE_NAME_PLURAL_BOOST: u32 = 1_250;
const SURFACE_NAME_TOKEN_BOOST: u32 = 1_000;
const SURFACE_NAME_TOKEN_PLURAL_BOOST: u32 = 900;
const SURFACE_NAME_SUBSTRING_BOOST: u32 = 500;
const QUERY_FIELD_MATCH_BOOST: u32 = 1_000;
const FIELD_PATH_EXACT_BOOST: u32 = 1_000;
const FIELD_PATH_TOKEN_BOOST: u32 = 750;
const FIELD_PATH_SUBSTRING_BOOST: u32 = 500;
const VALUE_EXACT_MATCH_BOOST: u32 = 1_000;
const VALUE_TOKEN_MATCH_BOOST: u32 = 750;
const OBSERVED_CHILD_PATH_BOOST: u32 = 1_000;
const OBSERVED_VALUES_PER_FIELD_LIMIT: usize = 3;
const CATALOG_FINGERPRINT_FILE_NAME: &str = "catalog.sha256";
const CATALOG_DIRTY_FILE_NAME: &str = "catalog.dirty";
const OBSERVED_VALUE_RETENTION_DAYS: i64 = 90;

#[derive(Clone)]
pub(crate) struct SearchService {
    search: UniversalSearch,
}

impl SearchService {
    pub(crate) fn new(query_manager: QueryManager, indexes: SearchIndexRefresher) -> Self {
        Self {
            search: UniversalSearch::new(query_manager, indexes),
        }
    }
}

#[derive(Clone)]
pub(crate) struct SearchIndexRefresher {
    layout: AppStateLayout,
    refresh_lock: Arc<Mutex<()>>,
}

impl SearchIndexRefresher {
    pub(crate) fn new(layout: AppStateLayout) -> Self {
        Self {
            layout,
            refresh_lock: Arc::new(Mutex::new(())),
        }
    }

    async fn refresh_catalog_if_needed(
        &self,
        workspace_name: &WorkspaceName,
        catalog: &CatalogInfo,
    ) -> Result<SearchIndexStore, SearchIndexError> {
        let _guard = self.refresh_lock.lock().await;
        let fingerprint = catalog_fingerprint(catalog);
        if self.catalog_index_is_fresh(workspace_name, &fingerprint)? {
            return SearchIndexStore::open_workspace(&self.layout, workspace_name);
        }

        let dirty_marker = self.read_catalog_dirty_marker(workspace_name)?;
        let index =
            SearchIndexStore::replace_workspace_catalog(&self.layout, workspace_name, catalog)?;
        if self.read_catalog_dirty_marker(workspace_name)? == dirty_marker {
            self.clear_catalog_dirty_marker(workspace_name)?;
            self.write_catalog_fingerprint(workspace_name, &fingerprint)?;
        } else {
            tracing::debug!(
                workspace = %workspace_name,
                "catalog search index was marked dirty during refresh; leaving fingerprint stale"
            );
        }
        Ok(index)
    }

    pub(crate) fn mark_catalog_dirty(&self, workspace_name: &WorkspaceName) {
        let path = self.catalog_fingerprint_file(workspace_name);
        match fs::remove_file(&path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                tracing::warn!(
                    path = %path.display(),
                    error = %error,
                    "failed to remove stale catalog search fingerprint"
                );
            }
        }
        if let Err(error) = self.write_catalog_dirty_marker(workspace_name) {
            let path = self.catalog_dirty_marker_file(workspace_name);
            tracing::warn!(
                path = %path.display(),
                error = %error,
                "failed to write catalog search dirty marker"
            );
        }
    }

    fn catalog_index_is_fresh(
        &self,
        workspace_name: &WorkspaceName,
        fingerprint: &str,
    ) -> Result<bool, SearchIndexError> {
        if self.read_catalog_dirty_marker(workspace_name)?.is_some()
            || !SearchIndexStore::workspace_index_is_usable(&self.layout, workspace_name)
        {
            return Ok(false);
        }
        let path = self.catalog_fingerprint_file(workspace_name);
        match fs::read_to_string(&path) {
            Ok(stored) => Ok(stored.trim() == fingerprint),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error.into()),
        }
    }

    fn write_catalog_fingerprint(
        &self,
        workspace_name: &WorkspaceName,
        fingerprint: &str,
    ) -> Result<(), SearchIndexError> {
        let path = self.catalog_fingerprint_file(workspace_name);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, format!("{fingerprint}\n"))?;
        Ok(())
    }

    fn read_catalog_dirty_marker(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<Option<String>, SearchIndexError> {
        let path = self.catalog_dirty_marker_file(workspace_name);
        match fs::read_to_string(&path) {
            Ok(stored) => Ok(Some(stored)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    fn write_catalog_dirty_marker(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<(), SearchIndexError> {
        let path = self.catalog_dirty_marker_file(workspace_name);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, format!("{}\n", Uuid::new_v4()))?;
        Ok(())
    }

    fn clear_catalog_dirty_marker(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<(), SearchIndexError> {
        let path = self.catalog_dirty_marker_file(workspace_name);
        match fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    fn catalog_fingerprint_file(&self, workspace_name: &WorkspaceName) -> std::path::PathBuf {
        self.layout
            .search_dir(workspace_name)
            .join(CATALOG_FINGERPRINT_FILE_NAME)
    }

    fn catalog_dirty_marker_file(&self, workspace_name: &WorkspaceName) -> std::path::PathBuf {
        self.layout
            .search_dir(workspace_name)
            .join(CATALOG_DIRTY_FILE_NAME)
    }

    pub(crate) fn discard_source_observed_values(
        &self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
    ) {
        match SearchIndexStore::open_existing_workspace(&self.layout, workspace_name) {
            Ok(Some(index)) => {
                if let Err(error) =
                    index.delete_observed_values_for_source(workspace_name, source_name)
                {
                    tracing::warn!(
                        workspace = %workspace_name,
                        source = %source_name,
                        error = %error,
                        "failed to discard observed values for mutated source"
                    );
                }
            }
            Ok(None) => {}
            Err(error) => {
                tracing::warn!(
                    workspace = %workspace_name,
                    source = %source_name,
                    error = %error,
                    "failed to open search index while discarding observed values"
                );
            }
        }
    }
}

#[tonic::async_trait]
impl SearchServiceApi for SearchService {
    async fn search(
        &self,
        request: Request<SearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        let span = grpc_span(&request);
        let search = self.search.clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            let limit = search_limit(request.limit).map_err(app_status)?;
            let response = search
                .search(&workspace_name, &request.query, limit)
                .await
                .map_err(query_status)?;
            Ok(Response::new(response))
        })
        .await
    }
}

#[derive(Clone)]
struct UniversalSearch {
    queries: QueryManager,
    indexes: SearchIndexRefresher,
}

impl UniversalSearch {
    fn new(query_manager: QueryManager, indexes: SearchIndexRefresher) -> Self {
        Self {
            queries: query_manager,
            indexes,
        }
    }

    async fn search(
        &self,
        workspace_name: &WorkspaceName,
        query: &str,
        limit: u32,
    ) -> Result<SearchResponse, QueryManagerError> {
        let terms = query_terms(query).map_err(QueryManagerError::App)?;
        let catalog = self
            .queries
            .list_stored_catalog(workspace_name, None)
            .await?;
        let (mut candidates, catalog_status) = self
            .catalog_metadata_candidates(workspace_name, &catalog, &terms, limit)
            .await;
        let (observed_candidates, observed_status) =
            self.observed_value_candidates(workspace_name, &catalog, &terms, limit);
        candidates.extend(observed_candidates);
        candidates.sort();

        let total_count = candidates.len();
        let max_results = usize::try_from(limit).unwrap_or(usize::MAX);
        let truncated = total_count > max_results;
        let results = candidates
            .into_iter()
            .take(max_results)
            .map(|candidate| candidate.result)
            .collect::<Vec<_>>();
        let returned_count = u32::try_from(results.len()).unwrap_or(u32::MAX);
        Ok(SearchResponse {
            results,
            provider_statuses: vec![
                SearchProviderStatus {
                    provider: SearchProvider::CatalogMetadata as i32,
                    state: catalog_status.state as i32,
                    note: catalog_status.note,
                },
                SearchProviderStatus {
                    provider: SearchProvider::ObservedValues as i32,
                    state: observed_status.state as i32,
                    note: observed_status.note,
                },
            ],
            truncation: Some(SearchResultTruncation {
                truncated,
                returned_count,
                max_results: limit,
                note: truncation_note(truncated, total_count, max_results),
            }),
        })
    }

    async fn catalog_metadata_candidates(
        &self,
        workspace_name: &WorkspaceName,
        catalog: &CatalogInfo,
        terms: &QueryTerms,
        limit: u32,
    ) -> (Vec<Candidate>, CatalogProviderStatus) {
        let index = match self
            .indexes
            .refresh_catalog_if_needed(workspace_name, catalog)
            .await
        {
            Ok(index) => index,
            Err(error) => return (Vec::new(), catalog_index_error_status(&error)),
        };
        let capabilities = index.capabilities();
        tracing::debug!(
            workspace = %workspace_name,
            tantivy_version = %capabilities.tantivy_version,
            tokenizer = %capabilities.tokenizer,
            "using Tantivy catalog search index"
        );
        let search_limit = usize::try_from(limit)
            .unwrap_or(usize::MAX)
            .saturating_mul(5)
            .max(25);
        let page = match index.search_catalog_page(workspace_name, &terms.terms, search_limit) {
            Ok(page) => page,
            Err(error) => return (Vec::new(), catalog_index_error_status(&error)),
        };
        let candidates = dedupe_candidates(catalog_candidates_from_hits(
            workspace_name,
            catalog,
            terms,
            page.hits,
        ));
        let state = if page.has_more {
            SearchProviderState::Partial
        } else if candidates.is_empty() {
            SearchProviderState::Empty
        } else {
            SearchProviderState::ResultsFound
        };
        let note = catalog_provider_note(state, candidates.len(), page.has_more);
        (candidates, CatalogProviderStatus { state, note })
    }

    fn observed_value_candidates(
        &self,
        workspace_name: &WorkspaceName,
        catalog: &CatalogInfo,
        terms: &QueryTerms,
        limit: u32,
    ) -> (Vec<Candidate>, ObservedProviderStatus) {
        let index = match SearchIndexStore::open_workspace(&self.indexes.layout, workspace_name) {
            Ok(index) => index,
            Err(error) => return (Vec::new(), observed_index_error_status(&error)),
        };
        let retention_cutoff = observed_value_retention_cutoff();
        if let Err(error) = index.purge_observed_values_before(workspace_name, &retention_cutoff) {
            tracing::warn!(
                workspace = %workspace_name,
                error = %error,
                "failed to purge stale observed values before search"
            );
        }
        let search_limit = usize::try_from(limit)
            .unwrap_or(usize::MAX)
            .saturating_mul(3)
            .max(25);
        let hits = match index.search_observed_values(workspace_name, &terms.terms, search_limit) {
            Ok(hits) => hits,
            Err(error) => return (Vec::new(), observed_index_error_status(&error)),
        };
        let live_sources = catalog_source_names(catalog);
        let candidates = observed_value_candidates_from_hits(
            workspace_name,
            terms,
            hits.into_iter()
                .filter(|hit| live_sources.contains(&hit.source_name)),
        );
        let state = if candidates.is_empty() {
            SearchProviderState::Empty
        } else {
            SearchProviderState::ResultsFound
        };
        let note = observed_provider_note(state, candidates.len());
        (candidates, ObservedProviderStatus { state, note })
    }
}

struct CatalogProviderStatus {
    state: SearchProviderState,
    note: String,
}

struct ObservedProviderStatus {
    state: SearchProviderState,
    note: String,
}

#[derive(Clone)]
struct Candidate {
    key: String,
    score: u32,
    type_order: u8,
    result: SearchResult,
}

impl Eq for Candidate {}

impl PartialEq for Candidate {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (Reverse(self.score), self.type_order, self.key.as_str()).cmp(&(
            Reverse(other.score),
            other.type_order,
            other.key.as_str(),
        ))
    }
}

impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

fn dedupe_candidates(candidates: Vec<Candidate>) -> Vec<Candidate> {
    let mut deduped = BTreeMap::<String, Candidate>::new();
    for candidate in candidates {
        deduped
            .entry(candidate.key.clone())
            .and_modify(|existing| {
                if candidate.score > existing.score {
                    *existing = candidate.clone();
                }
            })
            .or_insert(candidate);
    }
    deduped.into_values().collect()
}

#[derive(Clone, Debug)]
struct QueryTerms {
    terms: Vec<String>,
}

fn search_limit(limit: u32) -> Result<u32, AppError> {
    if limit == 0 {
        return Ok(DEFAULT_SEARCH_LIMIT);
    }
    if limit > MAX_SEARCH_LIMIT {
        return Err(AppError::InvalidInput(format!(
            "search limit must be between 1 and {MAX_SEARCH_LIMIT}"
        )));
    }
    Ok(limit)
}

fn query_terms(query: &str) -> Result<QueryTerms, AppError> {
    let query = query.trim();
    if query.is_empty() {
        return Err(AppError::InvalidInput(
            "argument 'query' must not be empty".to_string(),
        ));
    }
    if query.len() > MAX_QUERY_BYTES {
        return Err(AppError::InvalidInput(format!(
            "argument 'query' must be at most {MAX_QUERY_BYTES} bytes"
        )));
    }

    let normalized_query = normalize(query);
    let mut terms = query
        .split(|ch: char| !is_query_token_char(ch))
        .filter_map(|part| {
            let part = normalize(part);
            (part.len() > 1).then_some(part)
        })
        .collect::<Vec<_>>();
    if !terms.iter().any(|term| term == &normalized_query) {
        terms.push(normalized_query.clone());
    }
    terms.sort();
    terms.dedup();

    Ok(QueryTerms { terms })
}

fn catalog_fingerprint(catalog: &CatalogInfo) -> String {
    let mut hasher = Sha256::new();
    hash_str(&mut hasher, "coral-search-catalog-v1");

    let mut tables = catalog.tables.iter().collect::<Vec<_>>();
    tables.sort_by(|left, right| {
        (left.schema_name.as_str(), left.table_name.as_str())
            .cmp(&(right.schema_name.as_str(), right.table_name.as_str()))
    });
    hash_str(&mut hasher, "tables");
    hash_usize(&mut hasher, tables.len());
    for table in tables {
        hash_table(&mut hasher, table);
    }

    let mut functions = catalog.table_functions.iter().collect::<Vec<_>>();
    functions.sort_by(|left, right| {
        (left.schema_name.as_str(), left.function_name.as_str())
            .cmp(&(right.schema_name.as_str(), right.function_name.as_str()))
    });
    hash_str(&mut hasher, "table_functions");
    hash_usize(&mut hasher, functions.len());
    for function in functions {
        hash_table_function(&mut hasher, function);
    }

    format!("{:x}", hasher.finalize())
}

fn hash_table(hasher: &mut Sha256, table: &TableInfo) {
    hash_str(hasher, "table");
    hash_str(hasher, &table.schema_name);
    hash_str(hasher, &table.table_name);
    hash_str(hasher, &table.description);
    hash_str(hasher, &table.guide);

    let mut columns = table.columns.iter().collect::<Vec<_>>();
    columns.sort_by(|left, right| {
        (left.ordinal_position, left.name.as_str())
            .cmp(&(right.ordinal_position, right.name.as_str()))
    });
    hash_usize(hasher, columns.len());
    for column in columns {
        hash_column(hasher, column);
    }

    let mut filters = table.filters.iter().collect::<Vec<_>>();
    filters.sort_by(|left, right| left.name.cmp(&right.name));
    hash_usize(hasher, filters.len());
    for filter in filters {
        hash_table_filter(hasher, filter);
    }

    let mut required_filters = table.required_filters.iter().collect::<Vec<_>>();
    required_filters.sort();
    hash_usize(hasher, required_filters.len());
    for filter in required_filters {
        hash_str(hasher, filter);
    }
}

fn hash_column(hasher: &mut Sha256, column: &ColumnInfo) {
    hash_str(hasher, "column");
    hash_str(hasher, &column.name);
    hash_str(hasher, &column.data_type);
    hash_bool(hasher, column.nullable);
    hash_bool(hasher, column.is_virtual);
    hash_bool(hasher, column.is_required_filter);
    hash_str(hasher, &column.description);
    hash_u32(hasher, column.ordinal_position);
}

fn hash_table_filter(hasher: &mut Sha256, filter: &TableFilterInfo) {
    hash_str(hasher, "filter");
    hash_str(hasher, &filter.name);
    hash_str(hasher, &filter.mode);
    hash_bool(hasher, filter.required);
    hash_str(hasher, &filter.data_type);
    hash_str(hasher, &filter.description);
}

fn hash_table_function(hasher: &mut Sha256, function: &TableFunctionInfo) {
    hash_str(hasher, "table_function");
    hash_str(hasher, &function.schema_name);
    hash_str(hasher, &function.function_name);
    hash_str(hasher, &function.description);
    hash_str(hasher, &function.kind);
    hash_option_str(hasher, function.search_limits_json.as_deref());

    let mut arguments = function.arguments.iter().collect::<Vec<_>>();
    arguments.sort_by(|left, right| left.name.cmp(&right.name));
    hash_usize(hasher, arguments.len());
    for argument in arguments {
        hash_table_function_argument(hasher, argument);
    }

    let mut result_columns = function.result_columns.iter().collect::<Vec<_>>();
    result_columns.sort_by(|left, right| left.name.cmp(&right.name));
    hash_usize(hasher, result_columns.len());
    for column in result_columns {
        hash_table_function_result_column(hasher, column);
    }
}

fn hash_table_function_argument(hasher: &mut Sha256, argument: &TableFunctionArgumentInfo) {
    hash_str(hasher, "argument");
    hash_str(hasher, &argument.name);
    hash_bool(hasher, argument.required);

    let mut values = argument.values.iter().collect::<Vec<_>>();
    values.sort();
    hash_usize(hasher, values.len());
    for value in values {
        hash_str(hasher, value);
    }
}

fn hash_table_function_result_column(hasher: &mut Sha256, column: &TableFunctionResultColumnInfo) {
    hash_str(hasher, "result_column");
    hash_str(hasher, &column.name);
    hash_str(hasher, &column.data_type);
    hash_bool(hasher, column.nullable);
    hash_str(hasher, &column.description);
}

fn hash_str(hasher: &mut Sha256, value: &str) {
    hasher.update(value.len().to_le_bytes());
    hasher.update(value.as_bytes());
}

fn hash_option_str(hasher: &mut Sha256, value: Option<&str>) {
    match value {
        Some(value) => {
            hash_bool(hasher, true);
            hash_str(hasher, value);
        }
        None => hash_bool(hasher, false),
    }
}

fn hash_bool(hasher: &mut Sha256, value: bool) {
    hasher.update([u8::from(value)]);
}

fn hash_u32(hasher: &mut Sha256, value: u32) {
    hasher.update(value.to_le_bytes());
}

fn hash_usize(hasher: &mut Sha256, value: usize) {
    let value = u64::try_from(value).unwrap_or(u64::MAX);
    hasher.update(value.to_le_bytes());
}

fn is_query_token_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.' | '#' | '/' | '@')
}

fn normalize(value: &str) -> String {
    value.trim().to_ascii_lowercase()
}

fn catalog_candidates_from_hits(
    workspace_name: &WorkspaceName,
    catalog: &CatalogInfo,
    terms: &QueryTerms,
    hits: Vec<CatalogSearchHit>,
) -> Vec<Candidate> {
    let mut candidates = Vec::new();
    let mut column_hints_by_surface =
        BTreeMap::<(CatalogSearchSurfaceKind, String, String), usize>::new();
    for hit in hits {
        match hit.result_type {
            Some(CatalogSearchResultType::CatalogTable) => {
                if let Some(table) = find_table(catalog, &hit.schema_name, &hit.surface_name)
                    && let Some(candidate) =
                        catalog_table_candidate(workspace_name, table, hit, terms)
                {
                    candidates.push(candidate);
                }
            }
            Some(CatalogSearchResultType::CatalogTableFunction) => {
                if let Some(function) = find_function(catalog, &hit.schema_name, &hit.surface_name)
                    && let Some(candidate) =
                        catalog_function_candidate(workspace_name, function, hit, terms)
                {
                    candidates.push(candidate);
                }
            }
            Some(CatalogSearchResultType::ColumnHint) => {
                let Some(surface_kind) = hit.surface_kind else {
                    continue;
                };
                let score = catalog_hit_score(&hit, terms);
                let count_key = (
                    surface_kind,
                    hit.schema_name.clone(),
                    hit.surface_name.clone(),
                );
                let count = column_hints_by_surface.entry(count_key).or_default();
                if *count >= MAX_COLUMN_HINTS_PER_SURFACE {
                    continue;
                }
                candidates.push(column_hint_candidate(
                    ColumnHintCandidate {
                        workspace_name,
                        schema_name: &hit.schema_name,
                        surface_name: &hit.surface_name,
                        surface_kind: surface_kind.to_proto(),
                        field_role: hit
                            .field_role
                            .unwrap_or(CatalogSearchFieldRole::Unspecified)
                            .to_proto(),
                        name: &hit.name,
                        data_type: &hit.data_type,
                        required: hit.required,
                        description: &hit.description,
                        matched_fields: hit.matched_fields,
                    },
                    score,
                ));
                *count += 1;
            }
            Some(CatalogSearchResultType::NativeSearchPath) => {
                if let Some(function) = find_function(catalog, &hit.schema_name, &hit.surface_name)
                    && function.kind == "search"
                {
                    let score = catalog_hit_score(&hit, terms);
                    if let Some(candidate) = native_search_path_candidate(
                        workspace_name,
                        function,
                        hit.matched_fields,
                        score,
                    ) {
                        candidates.push(candidate);
                    }
                }
            }
            None => {}
        }
    }

    candidates
}

fn catalog_table_candidate(
    workspace_name: &WorkspaceName,
    table: &TableInfo,
    hit: CatalogSearchHit,
    terms: &QueryTerms,
) -> Option<Candidate> {
    let score = catalog_hit_score(&hit, terms);
    let matched_fields = table_matched_fields(table, &hit, terms);
    let table_column_preview = table_column_preview(table, terms);
    let item = match catalog_item_to_proto(
        workspace_name,
        crate::catalog::discovery::CatalogItem::Table(table_summary(table)),
    ) {
        Ok(item) => item,
        Err(status) => {
            tracing::warn!(
                schema = %table.schema_name,
                table = %table.table_name,
                error = %status.message(),
                "skipping catalog table search result with invalid metadata"
            );
            return None;
        }
    };
    Some(Candidate {
        key: hit.entity_key,
        score,
        type_order: 1,
        result: SearchResult {
            provider: SearchProvider::CatalogMetadata as i32,
            payload: Some(Payload::CatalogMetadata(CatalogMetadata {
                item: Some(item),
                matched_fields,
                table_column_preview: Some(table_column_preview),
            })),
        },
    })
}

fn catalog_function_candidate(
    workspace_name: &WorkspaceName,
    function: &TableFunctionInfo,
    hit: CatalogSearchHit,
    terms: &QueryTerms,
) -> Option<Candidate> {
    let score = catalog_hit_score(&hit, terms);
    let item = match catalog_item_to_proto(
        workspace_name,
        crate::catalog::discovery::CatalogItem::TableFunction(function.clone()),
    ) {
        Ok(item) => item,
        Err(status) => {
            tracing::warn!(
                schema = %function.schema_name,
                table_function = %function.function_name,
                error = %status.message(),
                "skipping catalog table-function search result with invalid metadata"
            );
            return None;
        }
    };
    Some(Candidate {
        key: hit.entity_key,
        score,
        type_order: 1,
        result: SearchResult {
            provider: SearchProvider::CatalogMetadata as i32,
            payload: Some(Payload::CatalogMetadata(CatalogMetadata {
                item: Some(item),
                matched_fields: hit.matched_fields,
                table_column_preview: None,
            })),
        },
    })
}

fn catalog_hit_score(hit: &CatalogSearchHit, terms: &QueryTerms) -> u32 {
    hit.score
        .saturating_add(source_name_exact_match_boost(&hit.schema_name, terms))
        .saturating_add(surface_name_match_boost(&hit.surface_name, terms))
        .saturating_add(query_field_match_boost(hit))
}

fn source_name_exact_match_boost(source_name: &str, terms: &QueryTerms) -> u32 {
    if exact_query_term_matches(source_name, terms) {
        SOURCE_EXACT_MATCH_BOOST
    } else {
        0
    }
}

fn surface_name_match_boost(surface_name: &str, terms: &QueryTerms) -> u32 {
    let surface_name = normalize(surface_name);
    let tokens = search_tokens(&surface_name);
    terms
        .terms
        .iter()
        .map(|term| literal_surface_name_boost(&surface_name, &tokens, term))
        .max()
        .unwrap_or(0)
}

fn literal_surface_name_boost(surface_name: &str, tokens: &[&str], term: &str) -> u32 {
    if surface_name == term {
        return SURFACE_NAME_EXACT_BOOST;
    }
    if plural_variants_match(surface_name, term) {
        return SURFACE_NAME_PLURAL_BOOST;
    }
    if tokens.iter().any(|token| token == &term) {
        return SURFACE_NAME_TOKEN_BOOST;
    }
    if tokens
        .iter()
        .any(|token| plural_variants_match(token, term))
    {
        return SURFACE_NAME_TOKEN_PLURAL_BOOST;
    }
    if term.chars().count() >= 3 && surface_name.contains(term) {
        return SURFACE_NAME_SUBSTRING_BOOST;
    }
    0
}

fn query_field_match_boost(hit: &CatalogSearchHit) -> u32 {
    if matches!(
        hit.field_role,
        Some(
            CatalogSearchFieldRole::TableColumn
                | CatalogSearchFieldRole::TableFilter
                | CatalogSearchFieldRole::TableFunctionArgument
                | CatalogSearchFieldRole::TableFunctionResultColumn
        )
    ) {
        QUERY_FIELD_MATCH_BOOST
    } else {
        0
    }
}

fn exact_query_term_matches(value: &str, terms: &QueryTerms) -> bool {
    let value = normalize(value);
    terms.terms.iter().any(|term| term == &value)
}

fn search_tokens(value: &str) -> Vec<&str> {
    value
        .split(|character: char| !character.is_ascii_alphanumeric())
        .filter(|token| !token.is_empty())
        .collect()
}

fn plural_variants_match(left: &str, right: &str) -> bool {
    if left.len().min(right.len()) < 3 {
        return false;
    }
    left.strip_suffix('s') == Some(right)
        || right.strip_suffix('s') == Some(left)
        || left.strip_suffix("es") == Some(right)
        || right.strip_suffix("es") == Some(left)
}

fn table_matched_fields(
    table: &TableInfo,
    hit: &CatalogSearchHit,
    terms: &QueryTerms,
) -> Vec<String> {
    let mut fields = hit.matched_fields.clone();
    if table
        .filters
        .iter()
        .any(|filter| !filter_matched_fields(filter, terms).is_empty())
    {
        fields.push("filters".to_string());
    } else if table
        .required_filters
        .iter()
        .any(|filter| value_matches_terms(filter, terms))
    {
        fields.push("required_filters".to_string());
    }
    if table
        .columns
        .iter()
        .any(|column| !column_matched_fields(column, terms).is_empty())
    {
        fields.push("columns".to_string());
    }
    fields.sort();
    fields.dedup();
    fields
}

fn table_column_preview(table: &TableInfo, terms: &QueryTerms) -> SearchTableColumnPreview {
    let mut selected_columns = Vec::new();
    push_column_preview_columns(table, &mut selected_columns, |table, column| {
        column_is_required_filter(table, column)
    });
    push_column_preview_columns(table, &mut selected_columns, |_, column| {
        !column_matched_fields(column, terms).is_empty()
    });
    push_column_preview_columns(table, &mut selected_columns, |_, column| {
        is_query_starter_column(&column.name)
    });

    selected_columns.sort_by_key(|column| column.ordinal_position);
    let columns = selected_columns
        .into_iter()
        .map(|column| SearchTableColumnPreviewColumn {
            name: column.name.clone(),
            data_type: column.data_type.clone(),
            is_required_filter: column_is_required_filter(table, column),
            description: column.description.clone(),
            matched_fields: column_matched_fields(column, terms),
        })
        .collect::<Vec<_>>();
    let column_count = u32::try_from(table.columns.len()).unwrap_or(u32::MAX);
    let preview_count = u32::try_from(columns.len()).unwrap_or(u32::MAX);
    SearchTableColumnPreview {
        column_count,
        columns,
        omitted_column_count: column_count.saturating_sub(preview_count),
    }
}

fn push_column_preview_columns<'a>(
    table: &'a TableInfo,
    selected_columns: &mut Vec<&'a ColumnInfo>,
    predicate: impl Fn(&TableInfo, &ColumnInfo) -> bool,
) {
    if selected_columns.len() >= COLUMN_PREVIEW_LIMIT {
        return;
    }
    for column in &table.columns {
        if selected_columns.len() >= COLUMN_PREVIEW_LIMIT {
            return;
        }
        if selected_columns
            .iter()
            .any(|selected| selected.name == column.name)
        {
            continue;
        }
        if predicate(table, column) {
            selected_columns.push(column);
        }
    }
}

fn column_is_required_filter(table: &TableInfo, column: &ColumnInfo) -> bool {
    column.is_required_filter
        || table
            .required_filters
            .iter()
            .any(|filter| filter == &column.name)
}

fn filter_matched_fields(filter: &TableFilterInfo, terms: &QueryTerms) -> Vec<String> {
    let mut fields = [
        ("name", filter.name.as_str()),
        ("mode", filter.mode.as_str()),
        ("data_type", filter.data_type.as_str()),
        ("description", filter.description.as_str()),
    ]
    .into_iter()
    .filter_map(|(field, value)| value_matches_terms(value, terms).then_some(field.to_string()))
    .collect::<Vec<_>>();
    fields.sort();
    fields.dedup();
    fields
}

fn column_matched_fields(column: &ColumnInfo, terms: &QueryTerms) -> Vec<String> {
    let mut fields = [
        ("name", column.name.as_str()),
        ("data_type", column.data_type.as_str()),
        ("description", column.description.as_str()),
    ]
    .into_iter()
    .filter_map(|(field, value)| value_matches_terms(value, terms).then_some(field.to_string()))
    .collect::<Vec<_>>();
    fields.sort();
    fields.dedup();
    fields
}

fn value_matches_terms(value: &str, terms: &QueryTerms) -> bool {
    let value = normalize(value);
    terms.terms.iter().any(|term| value.contains(term.as_str()))
}

fn is_query_starter_column(name: &str) -> bool {
    let original_name = name;
    let name = normalize(name);
    if name == "id"
        || name.ends_with("_id")
        || name.ends_with("-id")
        || original_name.ends_with("Id")
        || original_name.ends_with("ID")
    {
        return true;
    }
    let tokens_match = name
        .split(|character: char| !character.is_ascii_alphanumeric())
        .any(|token| {
            matches!(
                token,
                "name"
                    | "title"
                    | "status"
                    | "state"
                    | "url"
                    | "user"
                    | "login"
                    | "time"
                    | "date"
                    | "created"
                    | "updated"
                    | "timestamp"
            )
        });
    if tokens_match {
        return true;
    }
    [
        "name",
        "title",
        "status",
        "state",
        "url",
        "user",
        "login",
        "created",
        "updated",
        "timestamp",
    ]
    .into_iter()
    .any(|token| name.contains(token))
        || name.ends_with("_time")
        || name.ends_with("_date")
}

struct ColumnHintCandidate<'a> {
    workspace_name: &'a WorkspaceName,
    schema_name: &'a str,
    surface_name: &'a str,
    surface_kind: SearchSurfaceKind,
    field_role: SearchFieldRole,
    name: &'a str,
    data_type: &'a str,
    required: bool,
    description: &'a str,
    matched_fields: Vec<String>,
}

impl CatalogSearchSurfaceKind {
    fn to_proto(self) -> SearchSurfaceKind {
        match self {
            Self::Table => SearchSurfaceKind::Table,
            Self::TableFunction => SearchSurfaceKind::TableFunction,
        }
    }
}

impl CatalogSearchFieldRole {
    fn to_proto(self) -> SearchFieldRole {
        match self {
            Self::Unspecified => SearchFieldRole::Unspecified,
            Self::TableColumn => SearchFieldRole::TableColumn,
            Self::TableFilter => SearchFieldRole::TableFilter,
            Self::TableFunctionArgument => SearchFieldRole::TableFunctionArgument,
            Self::TableFunctionResultColumn => SearchFieldRole::TableFunctionResultColumn,
        }
    }
}

impl ObservedValueSurfaceKind {
    fn to_proto(self) -> SearchSurfaceKind {
        match self {
            Self::Table => SearchSurfaceKind::Table,
            Self::TableFunction => SearchSurfaceKind::TableFunction,
        }
    }
}

fn column_hint_candidate(input: ColumnHintCandidate<'_>, score: u32) -> Candidate {
    Candidate {
        key: format!(
            "column:{}:{}:{}.{}:{}",
            input.surface_kind.as_str_name(),
            input.field_role.as_str_name(),
            input.schema_name,
            input.surface_name,
            input.name
        ),
        score,
        type_order: 2,
        result: SearchResult {
            provider: SearchProvider::CatalogMetadata as i32,
            payload: Some(Payload::ColumnHint(ColumnHint {
                workspace: Some(workspace_to_proto(input.workspace_name)),
                schema_name: input.schema_name.to_string(),
                surface_name: input.surface_name.to_string(),
                surface_kind: input.surface_kind as i32,
                field_role: input.field_role as i32,
                name: input.name.to_string(),
                data_type: input.data_type.to_string(),
                required: input.required,
                description: input.description.to_string(),
                matched_fields: input.matched_fields,
            })),
        },
    }
}

fn native_search_path_candidate(
    workspace_name: &WorkspaceName,
    function: &TableFunctionInfo,
    matched_fields: Vec<String>,
    score: u32,
) -> Option<Candidate> {
    let table_function = match table_function_to_proto(workspace_name, function.clone()) {
        Ok(table_function) => table_function,
        Err(status) => {
            tracing::warn!(
                schema = %function.schema_name,
                table_function = %function.function_name,
                error = %status.message(),
                "skipping native search path with invalid metadata"
            );
            return None;
        }
    };
    Some(Candidate {
        key: format!(
            "native_search:{}.{}",
            function.schema_name, function.function_name
        ),
        score,
        type_order: 0,
        result: SearchResult {
            provider: SearchProvider::CatalogMetadata as i32,
            payload: Some(Payload::NativeSearchPath(NativeSearchPath {
                table_function: Some(table_function),
                sql_call_example: sql_call_example(function),
                matched_fields,
            })),
        },
    })
}

fn observed_value_candidates_from_hits(
    workspace_name: &WorkspaceName,
    terms: &QueryTerms,
    hits: impl IntoIterator<Item = ObservedValueSearchHit>,
) -> Vec<Candidate> {
    let mut per_field_counts = BTreeMap::<String, usize>::new();
    let mut candidates = Vec::new();
    for hit in hits {
        let field_key = observed_value_field_key(&hit);
        let count = per_field_counts.entry(field_key).or_default();
        if *count >= OBSERVED_VALUES_PER_FIELD_LIMIT {
            continue;
        }
        *count += 1;
        candidates.push(observed_value_candidate(workspace_name, hit, terms));
    }
    candidates
}

fn observed_value_field_key(hit: &ObservedValueSearchHit) -> String {
    format!(
        "{}:{}:{}:{}",
        hit.source_name,
        hit.surface_kind.as_str(),
        hit.surface_name,
        hit.column_name
    )
}

fn observed_value_candidate(
    _workspace_name: &WorkspaceName,
    hit: ObservedValueSearchHit,
    terms: &QueryTerms,
) -> Candidate {
    let value = observed_value_display(&hit.display_value);
    let field_path = hit.column_name.clone();
    let column_name = observed_base_column_name(&field_path).to_string();
    let score = observed_value_score(&hit, &field_path, terms);
    Candidate {
        key: format!(
            "observed:{}:{}:{}:{}:{}:{}",
            hit.source_name,
            hit.surface_kind.as_str(),
            hit.surface_name,
            field_path,
            hit.normalized_value_key,
            hit.last_observed_at
        ),
        score,
        type_order: 0,
        result: SearchResult {
            provider: SearchProvider::ObservedValues as i32,
            payload: Some(Payload::ObservedValue(ObservedValue {
                value,
                schema_name: hit.source_name,
                surface_name: hit.surface_name,
                column_name,
                surface_kind: hit.surface_kind.to_proto() as i32,
                field_path,
                observed_count: hit.observed_count,
                last_observed_at: hit.last_observed_at,
            })),
        },
    }
}

fn observed_value_score(hit: &ObservedValueSearchHit, field_path: &str, terms: &QueryTerms) -> u32 {
    hit.score
        .saturating_add(source_name_exact_match_boost(&hit.source_name, terms))
        .saturating_add(surface_name_match_boost(&hit.surface_name, terms))
        .saturating_add(field_path_match_boost(field_path, terms))
        .saturating_add(observed_value_match_boost_for_hit(hit, field_path, terms))
        .saturating_add(observed_field_path_boost(field_path))
}

fn observed_base_column_name(field_path: &str) -> &str {
    field_path.split('.').next().unwrap_or(field_path)
}

fn field_path_match_boost(field_path: &str, terms: &QueryTerms) -> u32 {
    let field_path = normalize(field_path);
    let tokens = search_tokens(&field_path);
    terms
        .terms
        .iter()
        .map(|term| literal_field_path_boost(&field_path, &tokens, term))
        .max()
        .unwrap_or(0)
}

fn literal_field_path_boost(field_path: &str, tokens: &[&str], term: &str) -> u32 {
    if field_path == term {
        return FIELD_PATH_EXACT_BOOST;
    }
    if tokens.iter().any(|token| token == &term) {
        return FIELD_PATH_TOKEN_BOOST;
    }
    if term.chars().count() >= 3 && field_path.contains(term) {
        return FIELD_PATH_SUBSTRING_BOOST;
    }
    0
}

#[cfg(test)]
fn observed_value_match_boost(
    display_value: &str,
    normalized_value_key: &str,
    terms: &QueryTerms,
) -> u32 {
    observed_value_match_boost_with_filter(display_value, normalized_value_key, terms, |_| false)
}

fn observed_value_match_boost_for_hit(
    hit: &ObservedValueSearchHit,
    field_path: &str,
    terms: &QueryTerms,
) -> u32 {
    observed_value_match_boost_with_filter(
        &hit.display_value,
        &hit.normalized_value_key,
        terms,
        |term| observed_context_matches_term(hit, field_path, term),
    )
}

fn observed_value_match_boost_with_filter(
    display_value: &str,
    normalized_value_key: &str,
    terms: &QueryTerms,
    mut skip_term: impl FnMut(&str) -> bool,
) -> u32 {
    let display_value = normalize(display_value);
    let normalized_value_key = normalize(normalized_value_key);
    if terms
        .terms
        .iter()
        .filter(|term| !skip_term(term))
        .any(|term| term == &display_value || term == &normalized_value_key)
    {
        return VALUE_EXACT_MATCH_BOOST;
    }

    let display_tokens = search_tokens(&display_value);
    let value_key_tokens = search_tokens(&normalized_value_key);
    for term in &terms.terms {
        if skip_term(term) {
            continue;
        }
        if display_tokens.iter().any(|token| token == term)
            || value_key_tokens.iter().any(|token| token == term)
        {
            return VALUE_TOKEN_MATCH_BOOST;
        }
        if is_identifier_query_term(term)
            && (display_value.contains(term) || normalized_value_key.contains(term))
        {
            return VALUE_TOKEN_MATCH_BOOST;
        }
    }

    0
}

fn observed_context_matches_term(
    hit: &ObservedValueSearchHit,
    field_path: &str,
    term: &str,
) -> bool {
    if normalize(&hit.source_name) == term {
        return true;
    }

    let surface_name = normalize(&hit.surface_name);
    let surface_tokens = search_tokens(&surface_name);
    if literal_surface_name_boost(&surface_name, &surface_tokens, term) > 0 {
        return true;
    }

    let field_path = normalize(field_path);
    let field_tokens = search_tokens(&field_path);
    literal_field_path_boost(&field_path, &field_tokens, term) > 0
}

fn is_identifier_query_term(term: &str) -> bool {
    term.chars()
        .any(|character| matches!(character, '_' | '-' | '.' | '#' | '/' | '@'))
}

fn observed_field_path_boost(field_path: &str) -> u32 {
    if field_path.contains('.') {
        OBSERVED_CHILD_PATH_BOOST
    } else {
        0
    }
}

fn observed_value_display(value: &str) -> String {
    const MAX_DISPLAY_CHARS: usize = 240;
    let mut chars = value.chars();
    let display = chars.by_ref().take(MAX_DISPLAY_CHARS).collect::<String>();
    if chars.next().is_some() {
        format!("{display}...")
    } else {
        display
    }
}

fn table_summary(table: &TableInfo) -> TableInfo {
    let mut table = table.clone();
    table.columns.clear();
    table
}

fn sql_call_example(function: &TableFunctionInfo) -> String {
    let args = function
        .arguments
        .iter()
        .filter(|argument| argument.required)
        .map(|argument| {
            format!(
                "{} => '<{}>'",
                quote_sql_identifier(&argument.name),
                argument.name
            )
        })
        .collect::<Vec<_>>();
    format!(
        "SELECT * FROM {}.{}({}) LIMIT 10",
        quote_sql_identifier(&function.schema_name),
        quote_sql_identifier(&function.function_name),
        args.join(", ")
    )
}

fn quote_sql_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
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

fn catalog_provider_note(state: SearchProviderState, total_count: usize, has_more: bool) -> String {
    match state {
        SearchProviderState::Partial if has_more => {
            format!(
                "Catalog metadata returned {total_count} candidate search hints from a bounded retrieval window; more index matches may exist"
            )
        }
        SearchProviderState::ResultsFound => {
            format!("Catalog metadata returned {total_count} candidate search hints")
        }
        SearchProviderState::Empty => "Catalog metadata returned no search hints".to_string(),
        _ => String::new(),
    }
}

fn observed_provider_note(state: SearchProviderState, total_count: usize) -> String {
    match state {
        SearchProviderState::ResultsFound => {
            format!("Observed values returned {total_count} candidate search hints")
        }
        SearchProviderState::Empty => "Observed values returned no search hints".to_string(),
        _ => String::new(),
    }
}

fn catalog_index_error_status(error: &SearchIndexError) -> CatalogProviderStatus {
    CatalogProviderStatus {
        state: SearchProviderState::Error,
        note: format!("Catalog metadata search index is unavailable: {error}"),
    }
}

fn observed_index_error_status(error: &SearchIndexError) -> ObservedProviderStatus {
    ObservedProviderStatus {
        state: SearchProviderState::Error,
        note: format!("Observed-value search index is unavailable: {error}"),
    }
}

fn catalog_source_names(catalog: &CatalogInfo) -> BTreeSet<String> {
    catalog
        .tables
        .iter()
        .map(|table| table.schema_name.clone())
        .chain(
            catalog
                .table_functions
                .iter()
                .map(|function| function.schema_name.clone()),
        )
        .collect()
}

fn observed_value_retention_cutoff() -> String {
    (Utc::now() - Duration::days(OBSERVED_VALUE_RETENTION_DAYS))
        .to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn truncation_note(truncated: bool, total_count: usize, max_results: usize) -> String {
    if !truncated {
        return String::new();
    }

    let visible_count = total_count.min(max_results);
    format!("Returned {visible_count} of {total_count} search hints")
}

#[cfg(test)]
mod tests {
    use coral_api::v1::SearchSurfaceKind;
    use coral_engine::{CatalogInfo, TableFunctionArgumentInfo, TableFunctionInfo, TableInfo};
    use tantivy::Index;
    use tantivy::schema::{STORED, STRING, Schema};
    use tempfile::tempdir;

    use super::{
        FIELD_PATH_EXACT_BOOST, SOURCE_EXACT_MATCH_BOOST, SURFACE_NAME_EXACT_BOOST,
        SearchIndexRefresher, VALUE_EXACT_MATCH_BOOST, VALUE_TOKEN_MATCH_BOOST,
        catalog_fingerprint, observed_value_match_boost, observed_value_score, query_terms,
        sql_call_example,
    };
    use crate::search::index::{ObservedValueSearchHit, ObservedValueSurfaceKind};
    use crate::state::AppStateLayout;
    use crate::workspaces::WorkspaceName;

    #[test]
    fn query_terms_preserve_identifier_punctuation() {
        let terms = query_terms("payments-api #eng acme/repo").expect("terms");

        assert!(terms.terms.iter().any(|term| term == "payments-api"));
        assert!(terms.terms.iter().any(|term| term == "#eng"));
        assert!(terms.terms.iter().any(|term| term == "acme/repo"));
    }

    #[test]
    fn surface_kind_has_stable_proto_names() {
        assert_eq!(
            SearchSurfaceKind::Table.as_str_name(),
            "SEARCH_SURFACE_KIND_TABLE"
        );
    }

    #[test]
    fn sql_call_example_quotes_case_preserving_identifiers() {
        let function = TableFunctionInfo {
            schema_name: "Search".to_string(),
            function_name: "Search_Issues".to_string(),
            description: String::new(),
            arguments: vec![TableFunctionArgumentInfo {
                name: "Q".to_string(),
                required: true,
                values: Vec::new(),
            }],
            result_columns: Vec::new(),
            kind: "search".to_string(),
            search_limits_json: None,
        };

        assert_eq!(
            sql_call_example(&function),
            "SELECT * FROM \"Search\".\"Search_Issues\"(\"Q\" => '<Q>') LIMIT 10"
        );
    }

    #[test]
    fn observed_value_score_boosts_source_surface_field_and_exact_value() {
        let terms = query_terms("linear issues identifier BENCH-457").expect("terms");
        let hit = observed_hit("linear", "issues", "identifier", "BENCH-457", 100);

        assert_eq!(
            observed_value_score(&hit, &hit.column_name, &terms),
            100 + SOURCE_EXACT_MATCH_BOOST
                + SURFACE_NAME_EXACT_BOOST
                + FIELD_PATH_EXACT_BOOST
                + VALUE_EXACT_MATCH_BOOST
        );
    }

    #[test]
    fn observed_value_boosts_value_tokens_without_broad_substring_match() {
        let pr_terms = query_terms("1017").expect("terms");
        assert_eq!(
            observed_value_match_boost(
                "https://github.com/withcoral/coral/pull/1017",
                "https://github.com/withcoral/coral/pull/1017",
                &pr_terms,
            ),
            VALUE_TOKEN_MATCH_BOOST
        );

        let repo_terms = query_terms("withcoral/coral").expect("terms");
        assert_eq!(
            observed_value_match_boost(
                "https://github.com/withcoral/coral/pull/1017",
                "https://github.com/withcoral/coral/pull/1017",
                &repo_terms,
            ),
            VALUE_TOKEN_MATCH_BOOST
        );

        let substring_terms = query_terms("ora").expect("terms");
        assert_eq!(
            observed_value_match_boost("Coral Pivot", "coral pivot", &substring_terms),
            0
        );
    }

    #[test]
    fn observed_value_score_ignores_value_terms_already_matched_by_context() {
        let terms = query_terms("github 1017").expect("terms");
        let generic_github_url = observed_hit(
            "github",
            "pulls",
            "user",
            "https://github.com/antonmry",
            100,
        );
        let pull_number = observed_hit("github", "pulls", "number", "1017", 100);

        assert_eq!(
            observed_value_score(&generic_github_url, &generic_github_url.column_name, &terms,),
            100 + SOURCE_EXACT_MATCH_BOOST
        );
        assert_eq!(
            observed_value_score(&pull_number, &pull_number.column_name, &terms),
            100 + SOURCE_EXACT_MATCH_BOOST + VALUE_EXACT_MATCH_BOOST
        );
        assert!(
            observed_value_score(&pull_number, &pull_number.column_name, &terms)
                > observed_value_score(
                    &generic_github_url,
                    &generic_github_url.column_name,
                    &terms,
                )
        );
    }

    #[tokio::test]
    async fn search_index_refresher_refreshes_when_catalog_fingerprint_changes() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let first_refresher = SearchIndexRefresher::new(layout.clone());
        let second_refresher = SearchIndexRefresher::new(layout.clone());

        let first_catalog = catalog_with_table("messages", "Fixture messages");
        let second_catalog = catalog_with_table("tasks", "Fixture tasks");

        let index = first_refresher
            .refresh_catalog_if_needed(&workspace, &first_catalog)
            .await
            .expect("initial refresh");
        assert!(catalog_has_surface(&index, &workspace, "messages"));

        let index = second_refresher
            .refresh_catalog_if_needed(&workspace, &second_catalog)
            .await
            .expect("changed catalog refresh");
        assert!(!catalog_has_surface(&index, &workspace, "messages"));
        assert!(catalog_has_surface(&index, &workspace, "tasks"));

        first_refresher.mark_catalog_dirty(&workspace);
        let third_catalog = catalog_with_table("events", "Fixture events");
        let index = second_refresher
            .refresh_catalog_if_needed(&workspace, &third_catalog)
            .await
            .expect("dirty refresh");
        assert!(!catalog_has_surface(&index, &workspace, "tasks"));
        assert!(catalog_has_surface(&index, &workspace, "events"));
    }

    #[tokio::test]
    async fn search_index_refresher_rebuilds_unusable_fresh_index() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let refresher = SearchIndexRefresher::new(layout.clone());
        let catalog = catalog_with_table("messages", "Fixture messages");

        std::fs::create_dir_all(layout.search_index_dir(&workspace)).expect("index dir");
        let mut schema = Schema::builder();
        schema.add_text_field("doc_key", STRING | STORED);
        Index::create_in_dir(layout.search_index_dir(&workspace), schema.build())
            .expect("stale schema index");
        refresher
            .write_catalog_fingerprint(&workspace, &catalog_fingerprint(&catalog))
            .expect("fingerprint");

        let index = refresher
            .refresh_catalog_if_needed(&workspace, &catalog)
            .await
            .expect("refresh");

        assert!(catalog_has_surface(&index, &workspace, "messages"));
    }

    fn catalog_with_table(table_name: &str, description: &str) -> CatalogInfo {
        CatalogInfo {
            tables: vec![TableInfo {
                schema_name: "fixture".to_string(),
                table_name: table_name.to_string(),
                description: description.to_string(),
                guide: String::new(),
                columns: Vec::new(),
                filters: Vec::new(),
                required_filters: Vec::new(),
            }],
            table_functions: Vec::new(),
        }
    }

    fn catalog_has_surface(
        index: &crate::search::index::SearchIndexStore,
        workspace: &WorkspaceName,
        surface_name: &str,
    ) -> bool {
        index
            .search_catalog(workspace, &[surface_name.to_string()], 10)
            .expect("search catalog")
            .iter()
            .any(|hit| hit.surface_name == surface_name)
    }

    fn observed_hit(
        source_name: &str,
        surface_name: &str,
        column_name: &str,
        display_value: &str,
        score: u32,
    ) -> ObservedValueSearchHit {
        ObservedValueSearchHit {
            source_name: source_name.to_string(),
            surface_kind: ObservedValueSurfaceKind::Table,
            surface_name: surface_name.to_string(),
            column_name: column_name.to_string(),
            normalized_value_key: display_value.to_ascii_lowercase(),
            display_value: display_value.to_string(),
            last_observed_at: "2026-06-04T10:00:00.000Z".to_string(),
            observed_count: 1,
            score,
        }
    }
}
