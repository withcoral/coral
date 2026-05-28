//! Implements the gRPC `SearchService`.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use chrono::{Duration, SecondsFormat, Utc};
use coral_api::v1::search_result::Payload;
use coral_api::v1::search_service_server::SearchService as SearchServiceApi;
use coral_api::v1::{
    ColumnHint, NativeSearchPath, ObservedValue, SearchProvider, SearchProviderState,
    SearchProviderStatus, SearchRequest, SearchResponse, SearchResult, SearchResultTruncation,
    SearchResultType, SearchSurfaceKind,
};
use coral_engine::{CatalogInfo, TableFunctionInfo, TableInfo};
use tonic::{Request, Response, Status};

use crate::bootstrap::{AppError, app_status};
use crate::query::manager::{QueryManager, QueryManagerError};
use crate::search::index::{
    CatalogSearchHit, CatalogSearchResultType, CatalogSearchSurfaceKind, ObservedValueSearchHit,
    SearchIndexError, SearchIndexStore,
};
use crate::sources::SourceName;
use crate::state::AppStateLayout;
use crate::transport::{
    catalog_item_to_proto, grpc_span, instrument_grpc, query_status, table_function_to_proto,
    workspace_name_from_proto, workspace_to_proto,
};
use crate::workspaces::WorkspaceName;
use tokio::sync::Mutex;

const DEFAULT_SEARCH_LIMIT: u32 = 10;
const MAX_SEARCH_LIMIT: u32 = 50;
const MAX_QUERY_BYTES: usize = 512;
const MAX_COLUMN_HINTS_PER_SURFACE: usize = 2;
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
    refreshed_workspaces: Arc<Mutex<BTreeSet<WorkspaceName>>>,
}

impl SearchIndexRefresher {
    pub(crate) fn new(layout: AppStateLayout) -> Self {
        Self {
            layout,
            refreshed_workspaces: Arc::new(Mutex::new(BTreeSet::new())),
        }
    }

    async fn refresh_catalog_if_needed(
        &self,
        workspace_name: &WorkspaceName,
        catalog: &CatalogInfo,
    ) -> Result<SearchIndexStore, SearchIndexError> {
        let mut refreshed_workspaces = self.refreshed_workspaces.lock().await;
        let index = if refreshed_workspaces.contains(workspace_name) {
            SearchIndexStore::open_workspace(&self.layout, workspace_name)?
        } else {
            let index =
                SearchIndexStore::replace_workspace_catalog(&self.layout, workspace_name, catalog)?;
            refreshed_workspaces.insert(workspace_name.clone());
            index
        };
        Ok(index)
    }

    pub(crate) async fn mark_catalog_dirty(&self, workspace_name: &WorkspaceName) {
        self.refreshed_workspaces
            .lock()
            .await
            .remove(workspace_name);
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
        let catalog = self.queries.list_catalog(workspace_name, None).await?;
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
        let hits = match index.search_catalog(workspace_name, &terms.terms, search_limit) {
            Ok(hits) => hits,
            Err(error) => return (Vec::new(), catalog_index_error_status(&error)),
        };
        let candidates =
            dedupe_candidates(catalog_candidates_from_hits(workspace_name, catalog, hits));
        let state = if candidates.is_empty() {
            SearchProviderState::Empty
        } else {
            SearchProviderState::ResultsFound
        };
        let note = catalog_provider_note(state, candidates.len());
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
        let candidates = hits
            .into_iter()
            .filter(|hit| live_sources.contains(&hit.source_name))
            .map(|hit| observed_value_candidate(workspace_name, hit))
            .collect::<Vec<_>>();
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

fn is_query_token_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.' | '#' | '/' | '@')
}

fn normalize(value: &str) -> String {
    value.trim().to_ascii_lowercase()
}

fn catalog_candidates_from_hits(
    workspace_name: &WorkspaceName,
    catalog: &CatalogInfo,
    hits: Vec<CatalogSearchHit>,
) -> Vec<Candidate> {
    let mut candidates = Vec::new();
    let mut column_hints_by_surface =
        BTreeMap::<(CatalogSearchSurfaceKind, String, String), usize>::new();
    for hit in hits {
        match hit.result_type {
            Some(CatalogSearchResultType::CatalogTable) => {
                if let Some(table) = find_table(catalog, &hit.schema_name, &hit.surface_name) {
                    candidates.push(Candidate {
                        key: hit.entity_key,
                        score: hit.score,
                        type_order: 1,
                        result: SearchResult {
                            r#type: SearchResultType::CatalogItem as i32,
                            payload: Some(Payload::CatalogItem(catalog_item_to_proto(
                                workspace_name,
                                crate::catalog::discovery::CatalogItem::Table(table_summary(table)),
                            ))),
                        },
                    });
                }
            }
            Some(CatalogSearchResultType::CatalogTableFunction) => {
                if let Some(function) = find_function(catalog, &hit.schema_name, &hit.surface_name)
                {
                    candidates.push(Candidate {
                        key: hit.entity_key,
                        score: hit.score,
                        type_order: 1,
                        result: SearchResult {
                            r#type: SearchResultType::CatalogItem as i32,
                            payload: Some(Payload::CatalogItem(catalog_item_to_proto(
                                workspace_name,
                                crate::catalog::discovery::CatalogItem::TableFunction(
                                    function.clone(),
                                ),
                            ))),
                        },
                    });
                }
            }
            Some(CatalogSearchResultType::ColumnHint) => {
                let Some(surface_kind) = hit.surface_kind else {
                    continue;
                };
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
                        name: &hit.name,
                        data_type: &hit.data_type,
                        required: hit.required,
                        description: &hit.description,
                        matched_fields: hit.matched_fields,
                    },
                    hit.score,
                ));
                *count += 1;
            }
            Some(CatalogSearchResultType::NativeSearchPath) => {
                if let Some(function) = find_function(catalog, &hit.schema_name, &hit.surface_name)
                    && function.kind == "search"
                {
                    candidates.push(native_search_path_candidate(
                        workspace_name,
                        function,
                        hit.matched_fields,
                        hit.score,
                    ));
                }
            }
            None => {}
        }
    }

    candidates
}

struct ColumnHintCandidate<'a> {
    workspace_name: &'a WorkspaceName,
    schema_name: &'a str,
    surface_name: &'a str,
    surface_kind: SearchSurfaceKind,
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

fn column_hint_candidate(input: ColumnHintCandidate<'_>, score: u32) -> Candidate {
    Candidate {
        key: format!(
            "column:{}:{}.{}:{}",
            input.surface_kind.as_str_name(),
            input.schema_name,
            input.surface_name,
            input.name
        ),
        score,
        type_order: 2,
        result: SearchResult {
            r#type: SearchResultType::ColumnHint as i32,
            payload: Some(Payload::ColumnHint(ColumnHint {
                workspace: Some(workspace_to_proto(input.workspace_name)),
                schema_name: input.schema_name.to_string(),
                surface_name: input.surface_name.to_string(),
                surface_kind: input.surface_kind as i32,
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
) -> Candidate {
    Candidate {
        key: format!(
            "native_search:{}.{}",
            function.schema_name, function.function_name
        ),
        score,
        type_order: 0,
        result: SearchResult {
            r#type: SearchResultType::NativeSearchPath as i32,
            payload: Some(Payload::NativeSearchPath(NativeSearchPath {
                table_function: Some(table_function_to_proto(workspace_name, function.clone())),
                sql_call_example: sql_call_example(function),
                matched_fields,
            })),
        },
    }
}

fn observed_value_candidate(
    _workspace_name: &WorkspaceName,
    hit: ObservedValueSearchHit,
) -> Candidate {
    let value = observed_value_display(&hit.display_value);
    Candidate {
        key: format!(
            "observed:{}:{}:{}:{}:{}",
            hit.source_name,
            hit.surface_name,
            hit.column_name,
            hit.normalized_value_key,
            hit.last_observed_at
        ),
        score: hit.score,
        type_order: 0,
        result: SearchResult {
            r#type: SearchResultType::ObservedValue as i32,
            payload: Some(Payload::ObservedValue(ObservedValue {
                value,
                schema_name: hit.source_name,
                surface_name: hit.surface_name,
                column_name: hit.column_name,
            })),
        },
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
        .map(|argument| format!("{} => '<{}>'", argument.name, argument.name))
        .collect::<Vec<_>>();
    format!(
        "SELECT * FROM {}.{}({}) LIMIT 10",
        function.schema_name,
        function.function_name,
        args.join(", ")
    )
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

fn catalog_provider_note(state: SearchProviderState, total_count: usize) -> String {
    match state {
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
    if truncated {
        format!("Returned {max_results} of {total_count} search hints")
    } else {
        String::new()
    }
}

#[cfg(test)]
mod tests {
    use coral_api::v1::SearchSurfaceKind;
    use coral_engine::{CatalogInfo, TableInfo};
    use tempfile::tempdir;

    use super::{SearchIndexRefresher, query_terms};
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

    #[tokio::test]
    async fn search_index_refresher_only_refreshes_once_until_forced() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let refresher = SearchIndexRefresher::new(layout);

        let first_catalog = catalog_with_table("messages", "Fixture messages");
        let second_catalog = catalog_with_table("tasks", "Fixture tasks");

        let index = refresher
            .refresh_catalog_if_needed(&workspace, &first_catalog)
            .await
            .expect("initial refresh");
        assert!(catalog_has_surface(&index, &workspace, "messages"));

        let index = refresher
            .refresh_catalog_if_needed(&workspace, &second_catalog)
            .await
            .expect("skipped refresh");
        assert!(catalog_has_surface(&index, &workspace, "messages"));
        assert!(!catalog_has_surface(&index, &workspace, "tasks"));

        refresher.mark_catalog_dirty(&workspace).await;
        let index = refresher
            .refresh_catalog_if_needed(&workspace, &second_catalog)
            .await
            .expect("dirty refresh");
        assert!(!catalog_has_surface(&index, &workspace, "messages"));
        assert!(catalog_has_surface(&index, &workspace, "tasks"));
    }

    fn catalog_with_table(table_name: &str, description: &str) -> CatalogInfo {
        CatalogInfo {
            tables: vec![TableInfo {
                schema_name: "fixture".to_string(),
                table_name: table_name.to_string(),
                description: description.to_string(),
                guide: String::new(),
                columns: Vec::new(),
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
}
