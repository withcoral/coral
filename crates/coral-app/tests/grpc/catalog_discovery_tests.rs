#![allow(
    clippy::indexing_slicing,
    reason = "JSON/proto regression assertions intentionally fail loudly in tests."
)]

use coral_api::v1::{
    DescribeCatalogSurfaceRequest, ListCatalogRequest, ListColumnsRequest, PaginationRequest,
    SearchCatalogRequest, catalog_item, describe_catalog_surface_response,
};
use coral_client::default_workspace;
use tonic::Request;

use super::harness::{
    GrpcHarness, fixture_manifest_with_functions_yaml, fixture_manifest_with_multiple_tables_yaml,
    fixture_manifest_with_required_filter_yaml,
};

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
            catalog_name: String::new(),
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
            catalog_name: String::new(),
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
    let counts = response.counts.as_ref().expect("catalog counts");
    assert_eq!(counts.table_count, 1);
    assert_eq!(counts.table_function_count, 2);
    assert_eq!(response.items.len(), 2);
    let function = match response.items[0].item.as_ref().expect("catalog item") {
        catalog_item::Item::TableFunction(function) => function,
        catalog_item::Item::Table(_) => panic!("expected table function"),
    };
    assert_eq!(function.catalog_name, "");
    assert_eq!(function.schema_name, "searchy");
    assert_eq!(function.name, "lookup_issue");
    assert_eq!(function.guide, "Use this function for exact issue lookup.");
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
            catalog_name: String::new(),
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
    let counts = function_only.counts.as_ref().expect("catalog counts");
    assert_eq!(counts.table_count, 1);
    assert_eq!(counts.table_function_count, 2);
    assert!(function_only.items.iter().all(|item| {
        matches!(
            item.item.as_ref().expect("catalog item"),
            catalog_item::Item::TableFunction(_)
        )
    }));
}

#[tokio::test]
async fn catalog_discovery_table_functions_sql_exposes_empty_v3_catalog_name() {
    let harness = GrpcHarness::new().await;
    harness
        .import_source(
            fixture_manifest_with_functions_yaml(),
            Vec::new(),
            Vec::new(),
        )
        .await;

    let rows = harness
        .execute_sql_rows(
            "SELECT catalog_name FROM coral.table_functions \
             WHERE schema_name = 'searchy' ORDER BY function_name",
        )
        .await;

    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|row| row["catalog_name"] == ""));
}

#[tokio::test]
async fn installed_v4_openapi_discovery_keeps_schema_local_duplicate_names() {
    let harness = GrpcHarness::new().await;
    let _server = harness.import_v4_openapi_catalog_fixture().await;

    let response = harness
        .catalog_client()
        .list_catalog(Request::new(ListCatalogRequest {
            workspace: Some(default_workspace()),
            catalog_name: "openapi_v4".to_string(),
            schema_name: String::new(),
            kind: 0,
            pagination: None,
        }))
        .await
        .expect("list installed v4 catalog")
        .into_inner();
    let identities = response
        .items
        .iter()
        .map(|item| match item.item.as_ref().expect("catalog item") {
            catalog_item::Item::Table(table) => (
                table.catalog_name.as_str(),
                table.schema_name.as_str(),
                table.name.as_str(),
            ),
            catalog_item::Item::TableFunction(function) => (
                function.catalog_name.as_str(),
                function.schema_name.as_str(),
                function.name.as_str(),
            ),
        })
        .collect::<Vec<_>>();

    assert_eq!(identities.len(), 4);
    assert_eq!(
        identities
            .iter()
            .filter(|(_, _, name)| *name == "list")
            .count(),
        3
    );
    assert!(identities.contains(&("openapi_v4", "alpha", "list")));
    assert!(identities.contains(&("openapi_v4", "beta", "list")));
    assert!(identities.contains(&("openapi_v4", "public", "list")));
    assert!(identities.contains(&("openapi_v4", "alpha", "get")));
}

#[tokio::test]
async fn search_catalog_matches_table_function_guide() {
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
            pattern: "exact issue lookup".to_string(),
            ignore_case: true,
            catalog_name: String::new(),
            schema_name: "searchy".to_string(),
            kind: 2,
            pagination: None,
        }))
        .await
        .expect("search table function guide")
        .into_inner();

    assert_eq!(response.items.len(), 1);
    assert!(
        response.items[0]
            .matched_fields
            .iter()
            .any(|field| field == "guide")
    );
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
            catalog_name: String::new(),
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
    let required_column = required.columns[0].column.as_ref().expect("column");
    assert_eq!(required_column.name, "channel");
    assert!(required_column.is_required_filter);

    let filtered = harness
        .catalog_client()
        .list_columns(Request::new(ListColumnsRequest {
            workspace: Some(default_workspace()),
            catalog_name: String::new(),
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
async fn list_columns_empty_catalog_selects_the_two_part_table() {
    let harness = GrpcHarness::new().await;
    harness
        .import_source(
            r"
name: alpha
version: 0.1.0
dsl_version: 3
backend: http
base_url: https://example.com
tables:
  - name: list
    description: Legacy two-part list
    request: { method: GET, path: /list }
    response: {}
    columns:
      - { name: legacy_only, type: Utf8 }
"
            .to_string(),
            Vec::new(),
            Vec::new(),
        )
        .await;
    let _server = harness.import_v4_openapi_catalog_fixture().await;

    let response = harness
        .catalog_client()
        .list_columns(Request::new(ListColumnsRequest {
            workspace: Some(default_workspace()),
            catalog_name: String::new(),
            schema_name: "alpha".to_string(),
            table_name: "list".to_string(),
            pattern: None,
            ignore_case: true,
            required_only: false,
            pagination: None,
        }))
        .await
        .expect("empty catalog should select the two-part table")
        .into_inner();

    assert_eq!(response.columns.len(), 1);
    assert_eq!(
        response.columns[0].column.as_ref().expect("column").name,
        "legacy_only"
    );
}

#[tokio::test]
async fn describe_missing_surface_returns_missing() {
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
        .describe_catalog_surface(Request::new(DescribeCatalogSurfaceRequest {
            workspace: Some(default_workspace()),
            catalog_name: String::new(),
            schema_name: "local_messages".to_string(),
            surface_name: "messeges".to_string(),
        }))
        .await
        .expect("describe missing surface")
        .into_inner();

    let Some(describe_catalog_surface_response::Result::Missing(_)) = response.result else {
        panic!("expected missing surface");
    };
}

#[tokio::test]
async fn describe_catalog_surface_returns_exact_table_function() {
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
        .describe_catalog_surface(Request::new(DescribeCatalogSurfaceRequest {
            workspace: Some(default_workspace()),
            catalog_name: String::new(),
            schema_name: "searchy".to_string(),
            surface_name: "lookup_issue".to_string(),
        }))
        .await
        .expect("describe exact table function")
        .into_inner();

    let Some(describe_catalog_surface_response::Result::TableFunction(function)) = response.result
    else {
        panic!("expected table function");
    };
    assert_eq!(function.name, "lookup_issue");
    assert_eq!(function.guide, "Use this function for exact issue lookup.");
}

#[tokio::test]
async fn describe_catalog_surface_does_not_resolve_partial_function_name() {
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
        .describe_catalog_surface(Request::new(DescribeCatalogSurfaceRequest {
            workspace: Some(default_workspace()),
            catalog_name: String::new(),
            schema_name: "searchy".to_string(),
            surface_name: "lookup".to_string(),
        }))
        .await
        .expect("describe missing catalog surface")
        .into_inner();

    let Some(describe_catalog_surface_response::Result::Missing(_)) = response.result else {
        panic!("expected missing surface");
    };
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
            catalog_name: String::new(),
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
            catalog_name: String::new(),
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
            catalog_name: String::new(),
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
}
