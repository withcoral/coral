#![allow(
    clippy::indexing_slicing,
    reason = "Proto regression assertions intentionally fail loudly in tests."
)]

use coral_api::v1::search_result::Payload;
use coral_api::v1::{
    DeleteSourceRequest, SearchProvider, SearchProviderState, SearchRequest, SearchResultType,
    SearchSurfaceKind,
};
use coral_client::default_workspace;
use rusqlite::Connection;
use tonic::Request;

use super::harness::{
    GrpcHarness, fixture_manifest_with_functions_yaml, fixture_manifest_with_test_queries_yaml,
};

#[tokio::test]
async fn search_returns_typed_metadata_and_native_search_results() {
    let harness = GrpcHarness::new().await;
    harness
        .import_source(
            fixture_manifest_with_functions_yaml(),
            Vec::new(),
            Vec::new(),
        )
        .await;

    let response = harness
        .search_client()
        .search(Request::new(SearchRequest {
            workspace: Some(default_workspace()),
            query: "issue search title".to_string(),
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
    assert!(!response.truncation.expect("truncation").truncated);
    assert!(
        harness
            .config_dir()
            .join("workspaces")
            .join("default")
            .join("search")
            .join("search.sqlite")
            .exists(),
        "search should create the workspace SQLite search index"
    );
    assert!(response.results.iter().any(|result| result.r#type
        == SearchResultType::NativeSearchPath as i32
        && matches!(
            result.payload.as_ref(),
            Some(Payload::NativeSearchPath(path))
                if path
                    .table_function
                    .as_ref()
                    .is_some_and(|function| function.name == "search_issues")
                    && path.sql_call_example.contains("searchy.search_issues")
                    && path.sql_call_example.contains("q => '<q>'")
        )));
    assert!(response.results.iter().any(|result| result.r#type
        == SearchResultType::CatalogItem as i32
        && matches!(
            result.payload.as_ref(),
            Some(Payload::CatalogItem(item))
                if item.item.as_ref().is_some_and(|item| match item {
                    coral_api::v1::catalog_item::Item::TableFunction(function) =>
                        function.name == "search_issues" && function.kind == "search",
                    coral_api::v1::catalog_item::Item::Table(_) => false,
                })
        )));
    assert!(response.results.iter().any(|result| result.r#type
        == SearchResultType::ColumnHint as i32
        && matches!(
            result.payload.as_ref(),
            Some(Payload::ColumnHint(hint))
                if hint.surface_name == "search_issues"
                    && hint.surface_kind == SearchSurfaceKind::TableFunction as i32
                    && hint.name == "title"
        )));
}

#[tokio::test]
async fn search_rejects_empty_and_too_large_queries() {
    let harness = GrpcHarness::new().await;

    let empty = harness
        .search_client()
        .search(Request::new(SearchRequest {
            workspace: Some(default_workspace()),
            query: "   ".to_string(),
            limit: 10,
        }))
        .await
        .expect_err("empty query should fail");
    assert_eq!(empty.code(), tonic::Code::InvalidArgument);
    assert!(empty.message().contains("query"));

    let too_long = harness
        .search_client()
        .search(Request::new(SearchRequest {
            workspace: Some(default_workspace()),
            query: "a".repeat(513),
            limit: 10,
        }))
        .await
        .expect_err("long query should fail");
    assert_eq!(too_long.code(), tonic::Code::InvalidArgument);
    assert!(too_long.message().contains("at most 512 bytes"));
}

#[tokio::test]
async fn search_returns_observed_values_after_successful_sql() {
    let harness = GrpcHarness::new().await;
    harness
        .import_source(
            fixture_manifest_with_test_queries_yaml(harness.temp_path(), &[]),
            Vec::new(),
            Vec::new(),
        )
        .await;

    let rows = harness
        .execute_sql_rows("SELECT text FROM local_messages.messages WHERE text = 'hello'")
        .await;
    assert_eq!(rows.len(), 1);

    let response = harness
        .search_client()
        .search(Request::new(SearchRequest {
            workspace: Some(default_workspace()),
            query: "hello".to_string(),
            limit: 10,
        }))
        .await
        .expect("search")
        .into_inner();

    assert_provider_state(
        &response,
        SearchProvider::ObservedValues,
        SearchProviderState::ResultsFound,
    );
    assert!(response.results.iter().any(|result| result.r#type
        == SearchResultType::ObservedValue as i32
        && matches!(
            result.payload.as_ref(),
            Some(Payload::ObservedValue(value))
                if value.schema_name == "local_messages"
                    && value.surface_name == "messages"
                    && value.column_name == "text"
                    && value.value == "hello"
        )));
}

#[tokio::test]
async fn search_applies_limit_and_reports_truncation() {
    let harness = GrpcHarness::new().await;
    harness
        .import_source(
            fixture_manifest_with_functions_yaml(),
            Vec::new(),
            Vec::new(),
        )
        .await;

    let response = harness
        .search_client()
        .search(Request::new(SearchRequest {
            workspace: Some(default_workspace()),
            query: "issue".to_string(),
            limit: 1,
        }))
        .await
        .expect("search")
        .into_inner();
    let truncation = response.truncation.expect("truncation");

    assert_eq!(response.results.len(), 1);
    assert!(truncation.truncated);
    assert_eq!(truncation.returned_count, 1);
    assert_eq!(truncation.max_results, 1);
}

#[tokio::test]
async fn source_mutations_mark_catalog_search_index_dirty() {
    let harness = GrpcHarness::new().await;
    let manifest_yaml = fixture_manifest_with_functions_yaml();
    harness
        .import_source(manifest_yaml.clone(), Vec::new(), Vec::new())
        .await;

    assert!(!search_index_path(&harness).exists());
    search(&harness, "issue search title").await;
    assert!(catalog_entity_count(&harness, "search_issues") > 0);

    let updated_manifest_yaml = manifest_yaml.replace("search_issues", "search_tasks");
    harness
        .import_source(updated_manifest_yaml, Vec::new(), Vec::new())
        .await;
    assert!(catalog_entity_count(&harness, "search_issues") > 0);
    assert_eq!(catalog_entity_count(&harness, "search_tasks"), 0);

    search(&harness, "task search title").await;
    assert_eq!(catalog_entity_count(&harness, "search_issues"), 0);
    assert!(catalog_entity_count(&harness, "search_tasks") > 0);

    harness
        .source_client()
        .delete_source(Request::new(DeleteSourceRequest {
            workspace: Some(default_workspace()),
            name: "searchy".to_string(),
        }))
        .await
        .expect("delete source");
    assert!(total_catalog_entity_count(&harness) > 0);

    search(&harness, "task search title").await;
    assert_eq!(total_catalog_entity_count(&harness), 0);
}

async fn search(harness: &GrpcHarness, query: &str) {
    harness
        .search_client()
        .search(Request::new(SearchRequest {
            workspace: Some(default_workspace()),
            query: query.to_string(),
            limit: 10,
        }))
        .await
        .expect("search");
}

fn assert_provider_state(
    response: &coral_api::v1::SearchResponse,
    provider: SearchProvider,
    state: SearchProviderState,
) {
    let status = response
        .provider_statuses
        .iter()
        .find(|status| status.provider == provider as i32)
        .expect("provider status");
    assert_eq!(status.state, state as i32);
}

fn catalog_entity_count(harness: &GrpcHarness, surface_name: &str) -> u32 {
    let connection = Connection::open(search_index_path(harness)).expect("open search index");
    connection
        .query_row(
            "SELECT count(*) FROM catalog_entities WHERE workspace = 'default' AND surface_name = ?1",
            [surface_name],
            |row| row.get(0),
        )
        .expect("catalog entity count")
}

fn total_catalog_entity_count(harness: &GrpcHarness) -> u32 {
    let connection = Connection::open(search_index_path(harness)).expect("open search index");
    connection
        .query_row(
            "SELECT count(*) FROM catalog_entities WHERE workspace = 'default'",
            [],
            |row| row.get(0),
        )
        .expect("total catalog entity count")
}

fn search_index_path(harness: &GrpcHarness) -> std::path::PathBuf {
    harness
        .config_dir()
        .join("workspaces")
        .join("default")
        .join("search")
        .join("search.sqlite")
}
