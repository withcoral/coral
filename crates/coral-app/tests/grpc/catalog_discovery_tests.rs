#![allow(
    clippy::indexing_slicing,
    reason = "JSON/proto regression assertions intentionally fail loudly in tests."
)]

use coral_api::v1::{
    DescribeTableRequest, ListCatalogRequest, ListColumnsRequest, PaginationRequest,
    SearchCatalogRequest, catalog_item,
};
use coral_client::default_workspace;
use tonic::Request;

use super::harness::{
    GrpcHarness, fixture_manifest_with_functions_yaml, fixture_manifest_with_multiple_tables_yaml,
    fixture_manifest_with_required_filter_yaml, fixture_manifest_with_search_ranking_yaml,
    fixture_manifest_with_source_aware_search_yaml,
};

fn search_result_item_name(result: &coral_api::v1::CatalogSearchResult) -> &str {
    match result
        .item
        .as_ref()
        .expect("search result")
        .item
        .as_ref()
        .expect("catalog item")
    {
        catalog_item::Item::Table(table) => table.name.as_str(),
        catalog_item::Item::TableFunction(function) => function.name.as_str(),
    }
}

fn search_result_schema_name(result: &coral_api::v1::CatalogSearchResult) -> &str {
    match result
        .item
        .as_ref()
        .expect("search result")
        .item
        .as_ref()
        .expect("catalog item")
    {
        catalog_item::Item::Table(table) => table.schema_name.as_str(),
        catalog_item::Item::TableFunction(function) => function.schema_name.as_str(),
    }
}

async fn import_source_aware_search_sources(harness: &GrpcHarness) {
    for source in ["github", "linear", "notion"] {
        harness
            .import_source(
                fixture_manifest_with_source_aware_search_yaml(source),
                Vec::new(),
                Vec::new(),
            )
            .await;
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
async fn search_catalog_ranks_match_quality_before_pagination() {
    let harness = GrpcHarness::new().await;
    harness
        .import_source(
            fixture_manifest_with_search_ranking_yaml(),
            Vec::new(),
            Vec::new(),
        )
        .await;

    let response = harness
        .catalog_client()
        .search_catalog(Request::new(SearchCatalogRequest {
            workspace: Some(default_workspace()),
            pattern: "ranked|ticket|owner|audit".to_string(),
            ignore_case: true,
            schema_name: String::new(),
            kind: 0,
            pagination: Some(PaginationRequest {
                limit: 4,
                offset: 0,
            }),
        }))
        .await
        .expect("ranked search catalog")
        .into_inner();

    assert_eq!(response.pagination.expect("pagination").total_count, 4);
    assert_eq!(
        response
            .items
            .iter()
            .map(search_result_item_name)
            .collect::<Vec<_>>(),
        vec!["tickets", "filtered_records", "notes", "schema_only"]
    );
}

#[tokio::test]
async fn search_catalog_ranks_named_source_before_cross_source_matches() {
    let harness = GrpcHarness::new().await;
    import_source_aware_search_sources(&harness).await;

    let response = harness
        .catalog_client()
        .search_catalog(Request::new(SearchCatalogRequest {
            workspace: Some(default_workspace()),
            pattern: "linear|issue|user".to_string(),
            ignore_case: true,
            schema_name: String::new(),
            kind: 1,
            pagination: Some(PaginationRequest {
                limit: 10,
                offset: 0,
            }),
        }))
        .await
        .expect("source-aware search catalog")
        .into_inner();

    assert_eq!(
        response
            .items
            .iter()
            .map(|result| (
                search_result_schema_name(result),
                search_result_item_name(result)
            ))
            .collect::<Vec<_>>(),
        vec![
            ("linear", "issues"),
            ("linear", "users"),
            ("github", "users"),
            ("notion", "users"),
        ]
    );
}

#[tokio::test]
async fn search_catalog_preserves_stable_order_without_source_signal() {
    let harness = GrpcHarness::new().await;
    import_source_aware_search_sources(&harness).await;

    let response = harness
        .catalog_client()
        .search_catalog(Request::new(SearchCatalogRequest {
            workspace: Some(default_workspace()),
            pattern: "issue|user".to_string(),
            ignore_case: true,
            schema_name: String::new(),
            kind: 1,
            pagination: Some(PaginationRequest {
                limit: 10,
                offset: 0,
            }),
        }))
        .await
        .expect("source-neutral search catalog")
        .into_inner();

    assert_eq!(
        response
            .items
            .iter()
            .map(|result| (
                search_result_schema_name(result),
                search_result_item_name(result)
            ))
            .collect::<Vec<_>>(),
        vec![
            ("github", "users"),
            ("linear", "issues"),
            ("linear", "users"),
            ("notion", "users"),
        ]
    );
}

#[tokio::test]
async fn search_catalog_does_not_source_boost_broad_pattern() {
    let harness = GrpcHarness::new().await;
    import_source_aware_search_sources(&harness).await;

    let response = harness
        .catalog_client()
        .search_catalog(Request::new(SearchCatalogRequest {
            workspace: Some(default_workspace()),
            pattern: ".*".to_string(),
            ignore_case: true,
            schema_name: String::new(),
            kind: 1,
            pagination: Some(PaginationRequest {
                limit: 10,
                offset: 0,
            }),
        }))
        .await
        .expect("broad search catalog")
        .into_inner();

    assert_eq!(
        response
            .items
            .iter()
            .map(|result| (
                search_result_schema_name(result),
                search_result_item_name(result)
            ))
            .collect::<Vec<_>>(),
        vec![
            ("github", "users"),
            ("linear", "issues"),
            ("linear", "users"),
            ("notion", "users"),
        ]
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
}
