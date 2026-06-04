#![allow(
    clippy::indexing_slicing,
    reason = "JSON/proto regression assertions intentionally fail loudly in tests."
)]

use coral_api::v1::catalog_item;

use super::harness::{GrpcHarness, assert_pagination, assert_status_contains, page};

fn catalog_function(item: &catalog_item::Item) -> &coral_api::v1::TableFunction {
    match item {
        catalog_item::Item::TableFunction(function) => function,
        catalog_item::Item::Table(_) => panic!("expected table function"),
    }
}

fn catalog_table(item: &catalog_item::Item) -> &coral_api::v1::TableSummary {
    match item {
        catalog_item::Item::Table(table) => table,
        catalog_item::Item::TableFunction(_) => panic!("expected table"),
    }
}

fn assert_matched_field(fields: &[String], expected: &str) {
    assert!(
        fields.iter().any(|field| field == expected),
        "expected matched field {expected}, got {fields:?}"
    );
}

#[tokio::test]
async fn search_catalog_matches_metadata_and_paginates_after_filtering() {
    let harness = GrpcHarness::new().await;
    harness.import_searchy_source().await;

    let response = harness
        .search_catalog("Issue", true, "searchy", 0, Some(page(2, 0)))
        .await
        .expect("search catalog");

    assert_pagination(response.pagination, 2, 2, 0, false);
    assert_eq!(response.items.len(), 2);
    let function = catalog_function(
        response.items[0]
            .item
            .as_ref()
            .expect("search result")
            .item
            .as_ref()
            .expect("catalog item"),
    );
    assert_eq!(function.name, "lookup_issue");
    assert_matched_field(&response.items[0].matched_fields, "description");
    assert_matched_field(&response.items[0].matched_fields, "result_columns");
}

#[tokio::test]
async fn list_catalog_returns_tables_and_table_functions_with_filters_and_pagination() {
    let harness = GrpcHarness::new().await;
    harness.import_searchy_source().await;

    let response = harness.list_catalog("searchy", 0, Some(page(2, 0))).await;

    assert_pagination(response.pagination, 3, 2, 0, true);
    let counts = response.counts.as_ref().expect("catalog counts");
    assert_eq!(counts.table_count, 1);
    assert_eq!(counts.table_function_count, 2);
    assert_eq!(response.items.len(), 2);
    let function = catalog_function(response.items[0].item.as_ref().expect("catalog item"));
    assert_eq!(function.schema_name, "searchy");
    assert_eq!(function.name, "lookup_issue");
    let table = catalog_table(response.items[1].item.as_ref().expect("catalog item"));
    assert_eq!(table.schema_name, "searchy");
    assert_eq!(table.name, "placeholder");
    assert_eq!(table.description, "Placeholder table");

    let function_only = harness.list_catalog("searchy", 2, Some(page(10, 0))).await;
    assert_pagination(function_only.pagination, 2, 10, 0, false);
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
    harness.import_filtered_messages_source().await;

    let required = harness
        .list_columns("filtered_messages", "messages", None, true)
        .await
        .expect("list required columns");
    let pagination = required.pagination.expect("required pagination");
    assert_eq!(pagination.total_count, 1);
    let required_column = required.columns[0].column.as_ref().expect("column");
    assert_eq!(required_column.name, "channel");
    assert!(required_column.is_required_filter);

    let filtered = harness
        .list_columns("filtered_messages", "messages", Some("TEXT"), false)
        .await
        .expect("list filtered columns");
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
    assert_matched_field(&filtered.columns[0].matched_fields, "column_name");
}

#[tokio::test]
async fn describe_missing_table_returns_catalog_suggestions() {
    let harness = GrpcHarness::new().await;
    harness.import_multiple_table_messages_source().await;

    let response = harness.describe_table("local_messages", "messeges").await;

    assert!(response.table.is_none());
    assert_eq!(response.available_schemas, vec!["coral", "local_messages"]);
    assert_eq!(response.same_schema_tables.len(), 3);
    assert_eq!(response.suggestions.len(), 3);
    assert_eq!(response.suggestions[0].name, "events");
}

#[tokio::test]
async fn describe_missing_table_name_does_not_apply_regex_limits() {
    let harness = GrpcHarness::new().await;
    harness.import_multiple_table_messages_source().await;

    let response = harness
        .describe_table("local_messages", "missing_table_".repeat(40))
        .await;

    assert!(response.table.is_none());
    assert_eq!(response.same_schema_tables.len(), 3);
    assert_eq!(response.suggestions.len(), 3);
}

#[tokio::test]
async fn list_columns_missing_table_takes_precedence_over_invalid_pattern() {
    let harness = GrpcHarness::new().await;
    harness.import_multiple_table_messages_source().await;

    let error = harness
        .list_columns("local_messages", "missing", Some("["), false)
        .await
        .expect_err("missing table should be reported before pattern validation");

    assert_eq!(error.code(), tonic::Code::NotFound);
}

#[tokio::test]
async fn invalid_regex_returns_invalid_argument() {
    let harness = GrpcHarness::new().await;

    let error = harness
        .search_catalog("[", true, "", 0, None)
        .await
        .expect_err("invalid catalog regex should fail");
    assert_status_contains(
        &error,
        tonic::Code::InvalidArgument,
        "invalid regex pattern",
    );

    harness.import_filtered_messages_source().await;
    let error = harness
        .list_columns("filtered_messages", "messages", Some("["), false)
        .await
        .expect_err("invalid column regex should fail");
    assert_status_contains(
        &error,
        tonic::Code::InvalidArgument,
        "invalid regex pattern",
    );
}
