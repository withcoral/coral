#![allow(
    clippy::indexing_slicing,
    reason = "JSON/proto regression assertions intentionally fail loudly in tests."
)]

use coral_api::v1::{
    DeleteSourceRequest, DescribeTableRequest, ListCatalogRequest, ListCatalogResponse,
    ListColumnsRequest, PaginationRequest, SearchCatalogRequest, SearchColumnsRequest,
    catalog_item,
};
use coral_app::{EngineExtensions, EngineExtensionsProvider, QuerySource};
use coral_client::default_workspace;
use std::fs;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tonic::Request;

use super::harness::{
    GrpcHarness, fixture_manifest_with_functions_yaml, fixture_manifest_with_multiple_tables_yaml,
    fixture_manifest_with_required_filter_yaml,
};

struct CountingEngineExtensionsProvider {
    runtime_builds: Arc<AtomicUsize>,
}

impl EngineExtensionsProvider for CountingEngineExtensionsProvider {
    fn extensions_for(&self, _selected_sources: &[QuerySource]) -> EngineExtensions {
        self.runtime_builds.fetch_add(1, Ordering::SeqCst);
        EngineExtensions::default()
    }
}

#[tokio::test]
async fn search_catalog_matches_metadata_and_paginates_after_filtering() {
    let harness = GrpcHarness::new().await;
    harness
        .import_source(
            fixture_manifest_with_functions_yaml(),
            Vec::new(),
            Vec::new(),
        )
        .await;

    let response = harness
        .catalog_client()
        .search_catalog(Request::new(SearchCatalogRequest {
            workspace: Some(default_workspace()),
            pattern: "Issue".to_string(),
            ignore_case: true,
            schema_name: "searchy".to_string(),
            kind: 0,
            pagination: Some(PaginationRequest {
                limit: 2,
                offset: 0,
            }),
        }))
        .await
        .expect("search catalog")
        .into_inner();

    let pagination = response.pagination.expect("pagination");
    assert_eq!(pagination.total_count, 2);
    assert_eq!(pagination.limit, 2);
    assert_eq!(pagination.offset, 0);
    assert!(!pagination.has_more);
    assert_eq!(response.sources.len(), 1);
    assert_eq!(response.sources[0].schema_name, "searchy");
    assert_eq!(response.items.len(), 2);
    let function = match response.items[0]
        .item
        .as_ref()
        .expect("search result")
        .item
        .as_ref()
        .expect("catalog item")
    {
        catalog_item::Item::TableFunction(function) => function,
        catalog_item::Item::Table(_) => panic!("expected table function"),
    };
    assert_eq!(function.name, "lookup_issue");
    assert!(
        response.items[0]
            .matched_fields
            .iter()
            .any(|field| field == "description")
    );
    assert!(
        response.items[0]
            .matched_fields
            .iter()
            .any(|field| field == "result_columns")
    );
}

#[tokio::test]
async fn catalog_metadata_reuses_snapshot_until_source_changes() {
    let runtime_builds = Arc::new(AtomicUsize::new(0));
    let harness = GrpcHarness::new_with_engine_extensions_provider(Arc::new(
        CountingEngineExtensionsProvider {
            runtime_builds: runtime_builds.clone(),
        },
    ))
    .await;
    harness
        .import_source(
            fixture_manifest_with_functions_yaml(),
            Vec::new(),
            Vec::new(),
        )
        .await;

    call_cached_metadata_endpoints(&harness).await;
    assert_eq!(runtime_builds.load(Ordering::SeqCst), 1);

    harness
        .import_source(
            fixture_manifest_with_multiple_tables_yaml(harness.temp_path()),
            Vec::new(),
            Vec::new(),
        )
        .await;
    let refreshed = list_catalog_page(&harness, "", 0, "list refreshed catalog").await;

    assert_eq!(runtime_builds.load(Ordering::SeqCst), 2);
    assert_catalog_has_source(&refreshed, "local_messages");

    edit_local_messages_manifest(&harness);
    let edited = list_catalog_page(&harness, "local_messages", 1, "list edited catalog").await;

    assert_eq!(runtime_builds.load(Ordering::SeqCst), 3);
    assert_edited_events_description(&edited);

    delete_source(&harness, "local_messages").await;
    let after_delete = list_catalog_page(&harness, "", 0, "list catalog after delete").await;

    assert_eq!(runtime_builds.load(Ordering::SeqCst), 4);
    assert_catalog_missing_source(&after_delete, "local_messages");
}

async fn call_cached_metadata_endpoints(harness: &GrpcHarness) {
    harness
        .catalog_client()
        .list_catalog(Request::new(ListCatalogRequest {
            workspace: Some(default_workspace()),
            schema_name: String::new(),
            kind: 0,
            pagination: None,
        }))
        .await
        .expect("list catalog");
    harness
        .catalog_client()
        .search_catalog(Request::new(SearchCatalogRequest {
            workspace: Some(default_workspace()),
            pattern: "Issue".to_string(),
            ignore_case: true,
            schema_name: String::new(),
            kind: 0,
            pagination: None,
        }))
        .await
        .expect("search catalog");
    harness
        .catalog_client()
        .describe_table(Request::new(DescribeTableRequest {
            workspace: Some(default_workspace()),
            schema_name: "searchy".to_string(),
            table_name: "placeholder".to_string(),
        }))
        .await
        .expect("describe table");
    harness
        .catalog_client()
        .list_columns(Request::new(ListColumnsRequest {
            workspace: Some(default_workspace()),
            schema_name: "searchy".to_string(),
            table_name: "placeholder".to_string(),
            pattern: None,
            ignore_case: true,
            required_only: false,
            pagination: None,
        }))
        .await
        .expect("list columns");
}

async fn list_catalog_page(
    harness: &GrpcHarness,
    schema_name: &str,
    kind: i32,
    context: &str,
) -> ListCatalogResponse {
    harness
        .catalog_client()
        .list_catalog(Request::new(ListCatalogRequest {
            workspace: Some(default_workspace()),
            schema_name: schema_name.to_string(),
            kind,
            pagination: Some(PaginationRequest {
                limit: 50,
                offset: 0,
            }),
        }))
        .await
        .expect(context)
        .into_inner()
}

fn edit_local_messages_manifest(harness: &GrpcHarness) {
    let manifest_path = harness
        .config_dir()
        .join("workspaces/default/sources/local_messages/manifest.yaml");
    let edited_manifest = fs::read_to_string(&manifest_path)
        .expect("read imported manifest")
        .replace("Fixture events", "Edited fixture events");
    fs::write(&manifest_path, edited_manifest).expect("edit imported manifest");
}

fn assert_catalog_has_source(response: &ListCatalogResponse, schema_name: &str) {
    assert!(
        response
            .sources
            .iter()
            .any(|source| source.schema_name == schema_name)
    );
}

fn assert_catalog_missing_source(response: &ListCatalogResponse, schema_name: &str) {
    assert!(
        response
            .sources
            .iter()
            .all(|source| source.schema_name != schema_name)
    );
}

fn assert_edited_events_description(response: &ListCatalogResponse) {
    let events = response
        .items
        .iter()
        .find_map(|item| match item.item.as_ref().expect("catalog item") {
            catalog_item::Item::Table(table) if table.name == "events" => Some(table),
            catalog_item::Item::Table(_) | catalog_item::Item::TableFunction(_) => None,
        })
        .expect("events table");
    assert_eq!(events.description, "Edited fixture events");
}

async fn delete_source(harness: &GrpcHarness, name: &str) {
    harness
        .source_client()
        .delete_source(Request::new(DeleteSourceRequest {
            workspace: Some(default_workspace()),
            name: name.to_string(),
        }))
        .await
        .expect("delete source");
}

#[tokio::test]
async fn list_catalog_returns_tables_and_table_functions_with_filters_and_pagination() {
    let harness = GrpcHarness::new().await;
    harness
        .import_source(
            fixture_manifest_with_functions_yaml(),
            Vec::new(),
            Vec::new(),
        )
        .await;

    let response = harness
        .catalog_client()
        .list_catalog(Request::new(ListCatalogRequest {
            workspace: Some(default_workspace()),
            schema_name: "searchy".to_string(),
            kind: 0,
            pagination: Some(PaginationRequest {
                limit: 2,
                offset: 0,
            }),
        }))
        .await
        .expect("list catalog")
        .into_inner();

    let pagination = response.pagination.expect("pagination");
    assert_eq!(pagination.total_count, 3);
    assert_eq!(pagination.limit, 2);
    assert_eq!(pagination.offset, 0);
    assert!(pagination.has_more);
    assert_eq!(pagination.next_offset, 2);
    assert_eq!(response.items.len(), 2);
    let function = match response.items[0].item.as_ref().expect("catalog item") {
        catalog_item::Item::TableFunction(function) => function,
        catalog_item::Item::Table(_) => panic!("expected table function"),
    };
    assert_eq!(function.schema_name, "searchy");
    assert_eq!(function.name, "lookup_issue");
    let table = match response.items[1].item.as_ref().expect("catalog item") {
        catalog_item::Item::Table(table) => table,
        catalog_item::Item::TableFunction(_) => panic!("expected table"),
    };
    assert_eq!(table.schema_name, "searchy");
    assert_eq!(table.name, "placeholder");
    assert_eq!(table.description, "Placeholder table");

    let function_only = harness
        .catalog_client()
        .list_catalog(Request::new(ListCatalogRequest {
            workspace: Some(default_workspace()),
            schema_name: "searchy".to_string(),
            kind: 2,
            pagination: Some(PaginationRequest {
                limit: 10,
                offset: 0,
            }),
        }))
        .await
        .expect("list table function catalog")
        .into_inner();
    assert_eq!(
        function_only
            .pagination
            .as_ref()
            .expect("pagination")
            .total_count,
        2
    );
    assert!(function_only.items.iter().all(|item| {
        matches!(
            item.item.as_ref().expect("catalog item"),
            catalog_item::Item::TableFunction(_)
        )
    }));
}

#[tokio::test]
async fn list_columns_filters_required_columns_and_patterns() {
    let harness = GrpcHarness::new().await;
    harness
        .import_source(
            fixture_manifest_with_required_filter_yaml(),
            Vec::new(),
            Vec::new(),
        )
        .await;

    let required = harness
        .catalog_client()
        .list_columns(Request::new(ListColumnsRequest {
            workspace: Some(default_workspace()),
            schema_name: "filtered_messages".to_string(),
            table_name: "messages".to_string(),
            pattern: None,
            ignore_case: true,
            required_only: true,
            pagination: None,
        }))
        .await
        .expect("list required columns")
        .into_inner();
    let pagination = required.pagination.expect("required pagination");
    assert_eq!(pagination.total_count, 1);
    assert_eq!(pagination.limit, 50);
    let required_column = required.columns[0].column.as_ref().expect("column");
    assert_eq!(required_column.name, "channel");
    assert!(required_column.is_required_filter);

    let filtered = harness
        .catalog_client()
        .list_columns(Request::new(ListColumnsRequest {
            workspace: Some(default_workspace()),
            schema_name: "filtered_messages".to_string(),
            table_name: "messages".to_string(),
            pattern: Some("TEXT".to_string()),
            ignore_case: true,
            required_only: false,
            pagination: None,
        }))
        .await
        .expect("list filtered columns")
        .into_inner();
    assert_eq!(
        filtered
            .pagination
            .expect("filtered pagination")
            .total_count,
        1
    );
    assert_eq!(
        filtered.columns[0]
            .column
            .as_ref()
            .expect("filtered column")
            .name,
        "text"
    );
    assert!(
        filtered.columns[0]
            .matched_fields
            .iter()
            .any(|field| field == "column_name")
    );
}

#[tokio::test]
async fn search_columns_matches_across_tables_and_paginates_after_filtering() {
    let harness = GrpcHarness::new().await;
    harness
        .import_source(
            fixture_manifest_with_multiple_tables_yaml(harness.temp_path()),
            Vec::new(),
            Vec::new(),
        )
        .await;

    let response = harness
        .catalog_client()
        .search_columns(Request::new(SearchColumnsRequest {
            workspace: Some(default_workspace()),
            pattern: "session".to_string(),
            ignore_case: true,
            schema_name: "local_messages".to_string(),
            required_only: false,
            pagination: Some(PaginationRequest {
                limit: 2,
                offset: 0,
            }),
        }))
        .await
        .expect("search columns")
        .into_inner();

    let pagination = response.pagination.expect("pagination");
    assert_eq!(pagination.total_count, 3);
    assert_eq!(pagination.limit, 2);
    assert_eq!(pagination.offset, 0);
    assert!(pagination.has_more);
    assert_eq!(pagination.next_offset, 2);
    assert_eq!(response.columns.len(), 2);
    assert_eq!(response.columns[0].schema_name, "local_messages");
    assert_eq!(response.columns[0].table_name, "events");
    let column = response.columns[0].column.as_ref().expect("column");
    assert_eq!(column.name, "sessionId");
    assert!(
        response.columns[0]
            .matched_fields
            .iter()
            .any(|field| field == "column_name")
    );
}

#[tokio::test]
async fn search_columns_filters_required_only() {
    let harness = GrpcHarness::new().await;
    harness
        .import_source(
            fixture_manifest_with_required_filter_yaml(),
            Vec::new(),
            Vec::new(),
        )
        .await;

    let response = harness
        .catalog_client()
        .search_columns(Request::new(SearchColumnsRequest {
            workspace: Some(default_workspace()),
            pattern: "Utf8".to_string(),
            ignore_case: true,
            schema_name: String::new(),
            required_only: true,
            pagination: None,
        }))
        .await
        .expect("search required columns")
        .into_inner();

    let pagination = response.pagination.expect("pagination");
    assert_eq!(pagination.total_count, 1);
    assert_eq!(pagination.limit, 20);
    assert_eq!(response.columns[0].table_name, "messages");
    let column = response.columns[0].column.as_ref().expect("column");
    assert_eq!(column.name, "channel");
    assert!(column.is_required_filter);
    assert!(
        response.columns[0]
            .matched_fields
            .iter()
            .any(|field| field == "data_type")
    );
}

#[tokio::test]
async fn column_endpoint_pagination_limits_match_endpoint_contracts() {
    let harness = GrpcHarness::new().await;
    harness
        .import_source(
            fixture_manifest_with_required_filter_yaml(),
            Vec::new(),
            Vec::new(),
        )
        .await;

    let error = harness
        .catalog_client()
        .list_columns(Request::new(ListColumnsRequest {
            workspace: Some(default_workspace()),
            schema_name: "filtered_messages".to_string(),
            table_name: "messages".to_string(),
            pattern: None,
            ignore_case: true,
            required_only: false,
            pagination: Some(PaginationRequest {
                limit: 201,
                offset: 0,
            }),
        }))
        .await
        .expect_err("list_columns should reject limits above 200");
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert!(error.message().contains("between 1 and 200"));

    let error = harness
        .catalog_client()
        .search_columns(Request::new(SearchColumnsRequest {
            workspace: Some(default_workspace()),
            pattern: "Utf8".to_string(),
            ignore_case: true,
            schema_name: String::new(),
            required_only: false,
            pagination: Some(PaginationRequest {
                limit: 101,
                offset: 0,
            }),
        }))
        .await
        .expect_err("search_columns should reject limits above 100");
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert!(error.message().contains("between 1 and 100"));
}

#[tokio::test]
async fn describe_missing_table_returns_catalog_suggestions() {
    let harness = GrpcHarness::new().await;
    harness
        .import_source(
            fixture_manifest_with_multiple_tables_yaml(harness.temp_path()),
            Vec::new(),
            Vec::new(),
        )
        .await;

    let response = harness
        .catalog_client()
        .describe_table(Request::new(DescribeTableRequest {
            workspace: Some(default_workspace()),
            schema_name: "local_messages".to_string(),
            table_name: "messeges".to_string(),
        }))
        .await
        .expect("describe missing table")
        .into_inner();

    assert!(response.table.is_none());
    assert_eq!(response.available_schemas, vec!["local_messages"]);
    assert_eq!(response.same_schema_tables.len(), 3);
    assert_eq!(response.suggestions.len(), 3);
    assert_eq!(response.suggestions[0].name, "events");
}

#[tokio::test]
async fn describe_missing_table_name_does_not_apply_regex_limits() {
    let harness = GrpcHarness::new().await;
    harness
        .import_source(
            fixture_manifest_with_multiple_tables_yaml(harness.temp_path()),
            Vec::new(),
            Vec::new(),
        )
        .await;

    let response = harness
        .catalog_client()
        .describe_table(Request::new(DescribeTableRequest {
            workspace: Some(default_workspace()),
            schema_name: "local_messages".to_string(),
            table_name: "missing_table_".repeat(40),
        }))
        .await
        .expect("describe long missing table name")
        .into_inner();

    assert!(response.table.is_none());
    assert_eq!(response.same_schema_tables.len(), 3);
    assert_eq!(response.suggestions.len(), 3);
}

#[tokio::test]
async fn list_columns_missing_table_takes_precedence_over_invalid_pattern() {
    let harness = GrpcHarness::new().await;
    harness
        .import_source(
            fixture_manifest_with_multiple_tables_yaml(harness.temp_path()),
            Vec::new(),
            Vec::new(),
        )
        .await;

    let error = harness
        .catalog_client()
        .list_columns(Request::new(ListColumnsRequest {
            workspace: Some(default_workspace()),
            schema_name: "local_messages".to_string(),
            table_name: "missing".to_string(),
            pattern: Some("[".to_string()),
            ignore_case: true,
            required_only: false,
            pagination: None,
        }))
        .await
        .expect_err("missing table should be reported before pattern validation");

    assert_eq!(error.code(), tonic::Code::NotFound);
}

#[tokio::test]
async fn invalid_regex_returns_invalid_argument() {
    let harness = GrpcHarness::new().await;

    let error = harness
        .catalog_client()
        .search_catalog(Request::new(SearchCatalogRequest {
            workspace: Some(default_workspace()),
            pattern: "[".to_string(),
            ignore_case: true,
            schema_name: String::new(),
            kind: 0,
            pagination: None,
        }))
        .await
        .expect_err("invalid catalog regex should fail");
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert!(error.message().contains("invalid regex pattern"));

    harness
        .import_source(
            fixture_manifest_with_required_filter_yaml(),
            Vec::new(),
            Vec::new(),
        )
        .await;
    let error = harness
        .catalog_client()
        .list_columns(Request::new(ListColumnsRequest {
            workspace: Some(default_workspace()),
            schema_name: "filtered_messages".to_string(),
            table_name: "messages".to_string(),
            pattern: Some("[".to_string()),
            ignore_case: true,
            required_only: false,
            pagination: None,
        }))
        .await
        .expect_err("invalid column regex should fail");
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert!(error.message().contains("invalid regex pattern"));

    let error = harness
        .catalog_client()
        .search_columns(Request::new(SearchColumnsRequest {
            workspace: Some(default_workspace()),
            pattern: "[".to_string(),
            ignore_case: true,
            schema_name: String::new(),
            required_only: false,
            pagination: None,
        }))
        .await
        .expect_err("invalid cross-table column regex should fail");
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert!(error.message().contains("invalid regex pattern"));
}
