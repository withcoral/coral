#![expect(
    clippy::indexing_slicing,
    reason = "proto regression assertions intentionally fail loudly in tests"
)]

use coral_api::v1::{
    ClearSearchDataRequest, DrainSearchQueueRequest, RebuildSearchIndexRequest, SearchClearTarget,
    SearchDataScope, SearchFieldRole, SearchIndexProvider, SearchMaintenanceState, SearchProvider,
    SearchProviderState, SearchRequest, SearchSurfaceKind, TableFunctionKind,
    ValidateSourceRequest, Workspace, catalog_item, search_clear_target, search_maintenance_result,
    search_result,
};
use coral_client::default_workspace;
use serde_json::json;
use tonic::{Code, Request};

use super::harness::{GrpcHarness, manifest_yaml, source_dir};

#[tokio::test]
async fn search_service_returns_structured_shell_response() {
    let harness = GrpcHarness::new().await;

    let response = harness
        .search_client()
        .search(Request::new(SearchRequest {
            workspace: Some(default_workspace()),
            query: "github issue".to_string(),
            limit: 0,
        }))
        .await
        .expect("search")
        .into_inner();

    assert!(response.results.is_empty());
    let truncation = response.truncation.as_ref().expect("truncation");
    assert!(!truncation.truncated);
    assert_eq!(truncation.returned_count, 0);
    assert_eq!(truncation.max_results, 10);
    assert_eq!(response.provider_statuses.len(), 3);
    let catalog_status = assert_provider_state(
        &response,
        SearchProvider::CatalogMetadata,
        SearchProviderState::Empty,
    );
    assert_empty_provider_coverage(catalog_status);
    let observed_status = assert_provider_state(
        &response,
        SearchProvider::ObservedValues,
        SearchProviderState::Empty,
    );
    assert_empty_provider_coverage(observed_status);
    let native_status = assert_provider_state(
        &response,
        SearchProvider::NativeFanout,
        SearchProviderState::NotEnabled,
    );
    assert_no_coverage(native_status);
}

#[tokio::test]
async fn search_service_rejects_unknown_workspace() {
    let harness = GrpcHarness::new().await;

    let status = harness
        .search_client()
        .search(Request::new(SearchRequest {
            workspace: Some(Workspace {
                name: "missing".to_string(),
            }),
            query: "github issue".to_string(),
            limit: 0,
        }))
        .await
        .expect_err("unknown workspace should fail");

    assert_eq!(status.code(), Code::NotFound);
    assert!(
        status.message().contains("workspace 'missing' not found"),
        "expected workspace not found message, got: {}",
        status.message()
    );
}

#[tokio::test]
async fn search_service_rejects_empty_query() {
    let harness = GrpcHarness::new().await;

    let status = harness
        .search_client()
        .search(Request::new(SearchRequest {
            workspace: Some(default_workspace()),
            query: " ".to_string(),
            limit: 0,
        }))
        .await
        .expect_err("empty query should fail");

    assert_eq!(status.code(), Code::InvalidArgument);
    assert!(
        status.message().contains("search query must not be empty"),
        "unexpected message: {}",
        status.message()
    );
}

#[tokio::test]
async fn table_function_proto_exposes_search_metadata() {
    let harness = GrpcHarness::new().await;
    harness
        .import_source(searchable_manifest_yaml(), Vec::new(), Vec::new())
        .await;

    let response = harness
        .source_client()
        .validate_source(Request::new(ValidateSourceRequest {
            workspace: Some(default_workspace()),
            name: "searchable".to_string(),
        }))
        .await
        .expect("validate source")
        .into_inner();

    assert_eq!(response.table_functions.len(), 1);
    let function = &response.table_functions[0];
    assert_eq!(
        TableFunctionKind::try_from(function.kind).expect("kind"),
        TableFunctionKind::Search
    );
    let search_limits = function.search_limits.as_ref().expect("search limits");
    assert_eq!(search_limits.default_top_k, 5);
    assert_eq!(search_limits.max_top_k, 20);
    assert_eq!(search_limits.max_calls_per_query, 2);
}

#[tokio::test]
async fn search_returns_catalog_metadata_for_search_functions_and_column_hints() {
    let harness = GrpcHarness::new().await;
    harness
        .import_source(searchable_manifest_yaml(), Vec::new(), Vec::new())
        .await;

    let response = harness
        .search_client()
        .search(Request::new(SearchRequest {
            workspace: Some(default_workspace()),
            query: "messages title".to_string(),
            limit: 10,
        }))
        .await
        .expect("search")
        .into_inner();

    assert_provider_state(
        &response,
        SearchProvider::CatalogMetadata,
        SearchProviderState::ResultsFound,
    );
    assert_provider_state(
        &response,
        SearchProvider::ObservedValues,
        SearchProviderState::Empty,
    );
    assert_provider_state(
        &response,
        SearchProvider::NativeFanout,
        SearchProviderState::NotEnabled,
    );
    assert!(
        response
            .truncation
            .as_ref()
            .is_some_and(|truncation| !truncation.truncated)
    );
    assert!(
        harness
            .config_dir()
            .join("workspaces")
            .join("default")
            .join("search")
            .join("search.sqlite3")
            .exists(),
        "search should create the workspace SQLite search database"
    );
    assert!(response.results.iter().any(|result| matches!(
        result.payload.as_ref(),
        Some(search_result::Payload::CatalogMetadata(metadata))
            if metadata.item.as_ref().and_then(|item| item.item.as_ref()).is_some_and(|item| {
                match item {
                    catalog_item::Item::TableFunction(function) =>
                        function.name == "search_messages"
                            && TableFunctionKind::try_from(function.kind)
                                .is_ok_and(|kind| kind == TableFunctionKind::Search)
                            && function.search_limits.as_ref().is_some_and(|limits| {
                                limits.default_top_k == 5
                                    && limits.max_top_k == 20
                                    && limits.max_calls_per_query == 2
                            }),
                    catalog_item::Item::Table(_) => false,
                }
            })
    )));
    assert!(response.results.iter().any(|result| matches!(
        result.payload.as_ref(),
        Some(search_result::Payload::ColumnHint(hint))
            if hint.surface_name == "search_messages"
                && hint.name == "title"
                && SearchSurfaceKind::try_from(hint.surface_kind)
                    .is_ok_and(|kind| kind == SearchSurfaceKind::TableFunction)
                && SearchFieldRole::try_from(hint.field_role)
                    .is_ok_and(|role| role == SearchFieldRole::TableFunctionResultColumn)
                && hint.data_type == "Utf8"
    )));
}

#[tokio::test]
async fn rebuild_search_index_forces_catalog_projection_refresh() {
    let harness = GrpcHarness::new().await;
    harness
        .import_source(searchable_manifest_yaml(), Vec::new(), Vec::new())
        .await;

    let first = harness
        .search_client()
        .rebuild_search_index(Request::new(RebuildSearchIndexRequest {
            workspace: Some(default_workspace()),
            provider: SearchIndexProvider::Catalog as i32,
            force: false,
        }))
        .await
        .expect("rebuild catalog")
        .into_inner();
    let first_detail = catalog_rebuild_detail(&first);
    assert_eq!(first_detail.old_document_count, 0);
    assert!(first_detail.new_document_count > 0);
    assert!(first_detail.projection_changed);
    assert!(first_detail.rebuild_performed);
    assert_eq!(
        SearchMaintenanceState::try_from(catalog_rebuild_result(&first).state)
            .expect("maintenance state"),
        SearchMaintenanceState::Completed
    );

    let forced = harness
        .search_client()
        .rebuild_search_index(Request::new(RebuildSearchIndexRequest {
            workspace: Some(default_workspace()),
            provider: SearchIndexProvider::Catalog as i32,
            force: true,
        }))
        .await
        .expect("force rebuild catalog")
        .into_inner();
    let forced_detail = catalog_rebuild_detail(&forced);
    assert_eq!(
        forced_detail.old_document_count,
        first_detail.new_document_count
    );
    assert_eq!(
        forced_detail.new_document_count,
        first_detail.new_document_count
    );
    assert!(!forced_detail.projection_changed);
    assert!(
        forced_detail.rebuild_performed,
        "force should rebuild even when the fingerprint is current"
    );
    assert_eq!(
        SearchMaintenanceState::try_from(catalog_rebuild_result(&forced).state)
            .expect("maintenance state"),
        SearchMaintenanceState::Completed
    );
}

#[tokio::test]
async fn rebuild_search_index_unspecified_rebuilds_catalog_and_observed_values() {
    let harness = GrpcHarness::new().await;
    harness
        .import_source(searchable_manifest_yaml(), Vec::new(), Vec::new())
        .await;

    let response = harness
        .search_client()
        .rebuild_search_index(Request::new(RebuildSearchIndexRequest {
            workspace: Some(default_workspace()),
            provider: SearchIndexProvider::Unspecified as i32,
            force: false,
        }))
        .await
        .expect("rebuild all search indexes")
        .into_inner();

    assert_eq!(response.results.len(), 2);
    let catalog_detail = catalog_rebuild_detail(&response);
    assert!(catalog_detail.new_document_count > 0);

    let observed = rebuild_result(&response, SearchProvider::ObservedValues);
    assert_eq!(
        SearchMaintenanceState::try_from(observed.state).expect("observed maintenance state"),
        SearchMaintenanceState::Completed
    );

    let observed_rebuild = observed_rebuild_detail(&response);
    assert_eq!(observed_rebuild.canonical_rows_scanned, 0);
    assert_eq!(observed_rebuild.fts_rows_rebuilt, 0);
}

#[tokio::test]
async fn rebuild_search_index_observed_values_rebuilds_projection() {
    let harness = GrpcHarness::new().await;

    let response = harness
        .search_client()
        .rebuild_search_index(Request::new(RebuildSearchIndexRequest {
            workspace: Some(default_workspace()),
            provider: SearchIndexProvider::ObservedValues as i32,
            force: false,
        }))
        .await
        .expect("rebuild observed-value search index")
        .into_inner();

    assert_eq!(response.results.len(), 1);
    let observed = rebuild_result(&response, SearchProvider::ObservedValues);
    assert_eq!(
        SearchMaintenanceState::try_from(observed.state).expect("observed maintenance state"),
        SearchMaintenanceState::Completed
    );
    let detail = observed_rebuild_detail(&response);
    assert_eq!(detail.canonical_rows_scanned, 0);
    assert_eq!(detail.fts_rows_rebuilt, 0);
}

#[tokio::test]
async fn drain_search_queue_reports_observed_provider_detail() {
    let harness = GrpcHarness::new().await;

    let response = harness
        .search_client()
        .drain_search_queue(Request::new(DrainSearchQueueRequest {
            workspace: Some(default_workspace()),
            budget_ms: 1_000,
        }))
        .await
        .expect("drain observed-value search queue")
        .into_inner();

    assert_eq!(response.results.len(), 1);
    let observed = response
        .results
        .iter()
        .find(|result| result.provider == SearchProvider::ObservedValues as i32)
        .expect("observed provider result");
    assert_eq!(
        SearchMaintenanceState::try_from(observed.state).expect("observed maintenance state"),
        SearchMaintenanceState::Noop
    );
    match observed.detail.as_ref() {
        Some(search_maintenance_result::Detail::ObservedDrain(detail)) => {
            assert_eq!(detail.queue_jobs_processed, 0);
            assert_eq!(detail.remaining_queue_depth, 0);
            assert!(!detail.budget_exhausted);
        }
        other => panic!("expected observed drain detail, got {other:?}"),
    }
}

#[tokio::test]
async fn clear_search_data_removes_catalog_projection_and_next_search_recreates_it() {
    let harness = GrpcHarness::new().await;
    harness
        .import_source(searchable_manifest_yaml(), Vec::new(), Vec::new())
        .await;

    harness
        .search_client()
        .rebuild_search_index(Request::new(RebuildSearchIndexRequest {
            workspace: Some(default_workspace()),
            provider: SearchIndexProvider::Catalog as i32,
            force: false,
        }))
        .await
        .expect("seed catalog projection");
    let clear = harness
        .search_client()
        .clear_search_data(Request::new(ClearSearchDataRequest {
            workspace: Some(default_workspace()),
            scope: SearchDataScope::All as i32,
            target: Some(SearchClearTarget {
                target: Some(search_clear_target::Target::Workspace(true)),
            }),
        }))
        .await
        .expect("clear search data")
        .into_inner();

    let clear_detail = catalog_clear_detail(&clear);
    assert!(clear_detail.deleted_document_count > 0);
    let cleanup = clear
        .storage_cleanup
        .as_ref()
        .expect("storage cleanup result");
    assert_eq!(
        SearchMaintenanceState::try_from(cleanup.state).expect("cleanup state"),
        SearchMaintenanceState::Completed
    );
    assert!(!cleanup.note.contains("SQLite"));
    assert!(!cleanup.note.contains("WAL"));
    assert!(!cleanup.note.contains("VACUUM"));

    let response = harness
        .search_client()
        .search(Request::new(SearchRequest {
            workspace: Some(default_workspace()),
            query: "messages title".to_string(),
            limit: 10,
        }))
        .await
        .expect("search after clear")
        .into_inner();

    assert_provider_state(
        &response,
        SearchProvider::CatalogMetadata,
        SearchProviderState::ResultsFound,
    );
    assert!(
        response.results.iter().any(|result| matches!(
            result.payload.as_ref(),
            Some(search_result::Payload::CatalogMetadata(metadata))
                if metadata.item.as_ref().and_then(|item| item.item.as_ref()).is_some()
        )),
        "next search should recreate and use the catalog projection"
    );
}

#[tokio::test]
async fn source_scoped_all_clear_does_not_load_manifest_and_bumps_generation() {
    let harness = GrpcHarness::new().await;
    harness
        .import_source(searchable_manifest_yaml(), Vec::new(), Vec::new())
        .await;
    harness
        .search_client()
        .rebuild_search_index(Request::new(RebuildSearchIndexRequest {
            workspace: Some(default_workspace()),
            provider: SearchIndexProvider::Catalog as i32,
            force: false,
        }))
        .await
        .expect("seed catalog projection");
    let sqlite_path = harness
        .config_dir()
        .join("workspaces/default/search/search.sqlite3");
    let initial_generation: i64 = rusqlite::Connection::open(&sqlite_path)
        .expect("open search sqlite before clear")
        .query_row(
            "SELECT generation FROM observed_source_generations WHERE workspace = 'default' AND source_name = 'searchable'",
            [],
            |row| row.get(0),
        )
        .expect("initial source generation");
    std::fs::remove_file(source_dir(harness.config_dir(), "searchable").join("manifest.yaml"))
        .expect("remove manifest before clear");

    let clear = harness
        .search_client()
        .clear_search_data(Request::new(ClearSearchDataRequest {
            workspace: Some(default_workspace()),
            scope: SearchDataScope::All as i32,
            target: Some(SearchClearTarget {
                target: Some(search_clear_target::Target::SourceName(
                    "searchable".to_string(),
                )),
            }),
        }))
        .await
        .expect("source-scoped all clear")
        .into_inner();

    assert_eq!(clear.results.len(), 2);
    assert!(catalog_clear_detail(&clear).deleted_document_count > 0);
    assert!(clear.results.iter().any(|result| matches!(
        result.detail.as_ref(),
        Some(search_maintenance_result::Detail::ObservedClear(_))
    )));
    let connection = rusqlite::Connection::open(sqlite_path).expect("open search sqlite");
    let generation: i64 = connection
        .query_row(
            "SELECT generation FROM observed_source_generations WHERE workspace = 'default' AND source_name = 'searchable'",
            [],
            |row| row.get(0),
        )
        .expect("source generation");
    assert_eq!(generation, initial_generation + 1);
}

#[tokio::test]
async fn search_table_preview_columns_do_not_inherit_table_matched_fields() {
    let harness = GrpcHarness::new().await;
    harness
        .import_source(table_preview_manifest_yaml(), Vec::new(), Vec::new())
        .await;

    let response = harness
        .search_client()
        .search(Request::new(SearchRequest {
            workspace: Some(default_workspace()),
            query: "conversation archive".to_string(),
            limit: 10,
        }))
        .await
        .expect("search")
        .into_inner();

    let metadata = response
        .results
        .iter()
        .find_map(|result| {
            let Some(search_result::Payload::CatalogMetadata(metadata)) = result.payload.as_ref()
            else {
                return None;
            };
            metadata
                .item
                .as_ref()
                .and_then(|item| item.item.as_ref())
                .is_some_and(|item| {
                    matches!(item, catalog_item::Item::Table(table) if table.name == "messages")
                })
                .then_some(metadata)
        })
        .expect("messages table metadata");
    assert!(
        metadata
            .matched_fields
            .iter()
            .any(|field| field == "description"),
        "table metadata should explain that the table description matched"
    );
    let preview = metadata
        .table_column_preview
        .as_ref()
        .expect("table column preview");
    assert_eq!(preview.column_count, 2);
    assert!(
        preview
            .columns
            .iter()
            .all(|column| column.matched_fields.is_empty()),
        "preview columns should not inherit table-level matched fields"
    );
}

#[tokio::test]
async fn search_provider_coverage_counts_mapped_candidates() {
    let harness = GrpcHarness::new().await;
    harness
        .import_source(
            many_matching_columns_manifest_yaml(),
            Vec::new(),
            Vec::new(),
        )
        .await;

    let response = harness
        .search_client()
        .search(Request::new(SearchRequest {
            workspace: Some(default_workspace()),
            query: "alpha".to_string(),
            limit: 10,
        }))
        .await
        .expect("search")
        .into_inner();

    let catalog_status = assert_provider_state(
        &response,
        SearchProvider::CatalogMetadata,
        SearchProviderState::Partial,
    );
    let coverage = catalog_status.coverage.as_ref().expect("coverage");
    assert_eq!(
        coverage.returned_count,
        u32::try_from(response.results.len()).expect("result count"),
        "coverage should count mapped provider candidates, not raw SQLite hits"
    );
    assert_eq!(coverage.returned_count, 4);
}

#[tokio::test]
async fn search_truncation_reflects_provider_retrieval_limit() {
    let harness = GrpcHarness::new().await;
    harness
        .import_source(
            many_retrieved_columns_manifest_yaml(),
            Vec::new(),
            Vec::new(),
        )
        .await;

    let response = harness
        .search_client()
        .search(Request::new(SearchRequest {
            workspace: Some(default_workspace()),
            query: "alpha".to_string(),
            limit: 10,
        }))
        .await
        .expect("search")
        .into_inner();

    let catalog_status = assert_provider_state(
        &response,
        SearchProvider::CatalogMetadata,
        SearchProviderState::Partial,
    );
    let coverage = catalog_status.coverage.as_ref().expect("coverage");
    assert!(
        coverage.has_more,
        "catalog provider should report that SQLite retrieval was capped"
    );
    let truncation = response.truncation.as_ref().expect("truncation");
    assert!(
        truncation.truncated,
        "top-level truncation should include provider retrieval caps"
    );
}

fn assert_provider_state(
    response: &coral_api::v1::SearchResponse,
    provider: SearchProvider,
    state: SearchProviderState,
) -> &coral_api::v1::SearchProviderStatus {
    let status = response
        .provider_statuses
        .iter()
        .find(|status| status.provider == provider as i32)
        .expect("provider status");
    assert_eq!(
        SearchProviderState::try_from(status.state).expect("provider state"),
        state
    );
    status
}

fn assert_no_coverage(status: &coral_api::v1::SearchProviderStatus) {
    assert!(
        status.coverage.is_none(),
        "disabled provider coverage should be absent"
    );
}

fn assert_empty_provider_coverage(status: &coral_api::v1::SearchProviderStatus) {
    let coverage = status.coverage.as_ref().expect("provider coverage");
    assert_eq!(coverage.failed_units, 0);
    assert_eq!(coverage.returned_count, 0);
    assert!(!coverage.has_more);
    assert!(!coverage.budget_exhausted);
    assert!(!coverage.timed_out);
    assert!(!coverage.stale_index);
}

fn catalog_rebuild_detail(
    response: &coral_api::v1::RebuildSearchIndexResponse,
) -> &coral_api::v1::CatalogRebuildResult {
    let result = catalog_rebuild_result(response);
    match result.detail.as_ref() {
        Some(search_maintenance_result::Detail::CatalogRebuild(detail)) => detail,
        other => panic!("expected catalog rebuild detail, got {other:?}"),
    }
}

fn catalog_rebuild_result(
    response: &coral_api::v1::RebuildSearchIndexResponse,
) -> &coral_api::v1::SearchMaintenanceResult {
    response
        .results
        .iter()
        .find(|result| result.provider == SearchProvider::CatalogMetadata as i32)
        .expect("provider result")
}

fn rebuild_result(
    response: &coral_api::v1::RebuildSearchIndexResponse,
    provider: SearchProvider,
) -> &coral_api::v1::SearchMaintenanceResult {
    response
        .results
        .iter()
        .find(|result| result.provider == provider as i32)
        .expect("maintenance result")
}

fn observed_rebuild_detail(
    response: &coral_api::v1::RebuildSearchIndexResponse,
) -> &coral_api::v1::ObservedRebuildResult {
    response
        .results
        .iter()
        .find_map(|result| match result.detail.as_ref()? {
            search_maintenance_result::Detail::ObservedRebuild(detail) => Some(detail),
            _ => None,
        })
        .expect("observed rebuild detail")
}

fn catalog_clear_detail(
    response: &coral_api::v1::ClearSearchDataResponse,
) -> &coral_api::v1::CatalogClearResult {
    response
        .results
        .iter()
        .find_map(|result| match result.detail.as_ref()? {
            search_maintenance_result::Detail::CatalogClear(detail) => Some(detail),
            _ => None,
        })
        .expect("catalog clear detail")
}

fn searchable_manifest_yaml() -> String {
    manifest_yaml(&json!({
        "name": "searchable",
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "http",
        "base_url": "https://example.test",
        "functions": [{
            "name": "search_messages",
            "kind": "search",
            "description": "Search messages",
            "args": [{
                "name": "query",
                "required": true,
                "bind": { "arg": "query" }
            }],
            "request": {
                "method": "GET",
                "path": "/messages",
                "query": [{ "name": "q", "from": "arg", "key": "query" }]
            },
            "response": { "rows_path": ["items"] },
            "columns": [{ "name": "title", "type": "Utf8" }],
            "search_limits": {
                "default_top_k": 5,
                "max_top_k": 20,
                "max_calls_per_query": 2
            }
        }]
    }))
}

fn table_preview_manifest_yaml() -> String {
    manifest_yaml(&json!({
        "name": "table_preview",
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "http",
        "base_url": "https://example.test",
        "tables": [{
            "name": "messages",
            "description": "Conversation archive",
            "request": { "method": "GET", "path": "/messages" },
            "response": {},
            "pagination": { "mode": "none" },
            "columns": [
                {
                    "name": "id",
                    "type": "Utf8",
                    "nullable": false,
                    "description": "Message id",
                    "expr": { "kind": "path", "path": ["id"] }
                },
                {
                    "name": "author",
                    "type": "Utf8",
                    "nullable": true,
                    "description": "Author name",
                    "expr": { "kind": "path", "path": ["author"] }
                }
            ]
        }]
    }))
}

fn many_matching_columns_manifest_yaml() -> String {
    manifest_yaml(&json!({
        "name": "many_matching_columns",
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "http",
        "base_url": "https://example.test",
        "tables": [{
            "name": "records",
            "description": "Alpha archive",
            "request": { "method": "GET", "path": "/records" },
            "response": {},
            "pagination": { "mode": "none" },
            "columns": [
                {
                    "name": "alpha_one",
                    "type": "Utf8",
                    "nullable": true,
                    "description": "",
                    "expr": { "kind": "path", "path": ["alpha_one"] }
                },
                {
                    "name": "alpha_two",
                    "type": "Utf8",
                    "nullable": true,
                    "description": "",
                    "expr": { "kind": "path", "path": ["alpha_two"] }
                },
                {
                    "name": "alpha_three",
                    "type": "Utf8",
                    "nullable": true,
                    "description": "",
                    "expr": { "kind": "path", "path": ["alpha_three"] }
                },
                {
                    "name": "alpha_four",
                    "type": "Utf8",
                    "nullable": true,
                    "description": "",
                    "expr": { "kind": "path", "path": ["alpha_four"] }
                },
                {
                    "name": "alpha_five",
                    "type": "Utf8",
                    "nullable": true,
                    "description": "",
                    "expr": { "kind": "path", "path": ["alpha_five"] }
                }
            ]
        }]
    }))
}

fn many_retrieved_columns_manifest_yaml() -> String {
    let columns = (0..60)
        .map(|index| {
            json!({
                "name": format!("alpha_{index:02}"),
                "type": "Utf8",
                "nullable": true,
                "description": "",
                "expr": { "kind": "path", "path": [format!("alpha_{index:02}")] }
            })
        })
        .collect::<Vec<_>>();

    manifest_yaml(&json!({
        "name": "many_retrieved_columns",
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "http",
        "base_url": "https://example.test",
        "tables": [{
            "name": "records",
            "description": "Alpha archive",
            "request": { "method": "GET", "path": "/records" },
            "response": {},
            "pagination": { "mode": "none" },
            "columns": columns
        }]
    }))
}
