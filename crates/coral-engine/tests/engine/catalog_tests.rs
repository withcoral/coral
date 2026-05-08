#![allow(
    clippy::indexing_slicing,
    clippy::string_slice,
    reason = "test code: assertion-style indexing is idiomatic in tests"
)]

use std::collections::BTreeMap;

use coral_engine::{ColumnInfo, CoralQuery, QuerySource, TableInfo};
use serde_json::{Value, json};
use tempfile::TempDir;

use crate::harness::{
    assert_table_not_found, build_source, build_source_with_inputs, dir_url, execution_to_rows,
    test_runtime, write_jsonl_file,
};

fn users_manifest(dir: &std::path::Path) -> Value {
    json!({
        "name": "alpha",
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "jsonl",
        "tables": [{
            "name": "users",
            "description": "Alpha users",
            "source": {
                "location": dir_url(dir),
                "glob": "**/*.jsonl"
            },
            "columns": [
                { "name": "id", "type": "Int64" },
                { "name": "team_id", "type": "Int64" },
                { "name": "name", "type": "Utf8" }
            ]
        }]
    })
}

fn teams_manifest(dir: &std::path::Path) -> Value {
    json!({
        "name": "beta",
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "jsonl",
        "tables": [{
            "name": "teams",
            "description": "Beta teams",
            "source": {
                "location": dir_url(dir),
                "glob": "**/*.jsonl"
            },
            "columns": [
                { "name": "id", "type": "Int64" },
                { "name": "team_name", "type": "Utf8" }
            ]
        }]
    })
}

fn build_catalog_sources() -> (TempDir, Vec<QuerySource>) {
    let temp = TempDir::new().expect("temp dir");
    let alpha_dir = temp.path().join("alpha");
    let beta_dir = temp.path().join("beta");
    write_jsonl_file(
        &alpha_dir,
        "users.jsonl",
        &[
            json!({"id": 1, "team_id": 10, "name": "Ada"}),
            json!({"id": 2, "team_id": 20, "name": "Grace"}),
            json!({"id": 3, "team_id": 10, "name": "Linus"}),
        ],
    );
    write_jsonl_file(
        &beta_dir,
        "teams.jsonl",
        &[
            json!({"id": 10, "team_name": "Platform"}),
            json!({"id": 20, "team_name": "Infra"}),
        ],
    );

    let sources = vec![
        build_source(users_manifest(&alpha_dir)),
        build_source(teams_manifest(&beta_dir)),
    ];
    (temp, sources)
}

#[tokio::test]
async fn coral_tables_lists_installed_sources() {
    let (_temp, sources) = build_catalog_sources();

    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &sources,
            test_runtime(),
            "SELECT schema_name, table_name FROM coral.tables ORDER BY schema_name, table_name",
        )
        .await
        .expect("catalog query should succeed"),
    );

    assert_eq!(
        rows,
        vec![
            json!({"schema_name": "alpha", "table_name": "users"}),
            json!({"schema_name": "beta", "table_name": "teams"}),
        ]
    );
}

#[tokio::test]
async fn coral_columns_returns_metadata() {
    let (_temp, sources) = build_catalog_sources();

    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &sources,
            test_runtime(),
            "SELECT column_name, data_type, is_nullable, is_virtual, is_required_filter \
             FROM coral.columns WHERE schema_name = 'alpha' AND table_name = 'users' \
             ORDER BY ordinal_position",
        )
        .await
        .expect("catalog query should succeed"),
    );

    assert_eq!(
        rows,
        vec![
            json!({"column_name": "id", "data_type": "Int64", "is_nullable": true, "is_virtual": false, "is_required_filter": false}),
            json!({"column_name": "team_id", "data_type": "Int64", "is_nullable": true, "is_virtual": false, "is_required_filter": false}),
            json!({"column_name": "name", "data_type": "Utf8", "is_nullable": true, "is_virtual": false, "is_required_filter": false}),
        ]
    );
}

#[tokio::test]
async fn coral_columns_default_row_order_matches_ordinal_position() {
    let (_temp, sources) = build_catalog_sources();

    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &sources,
            test_runtime(),
            "SELECT column_name, ordinal_position \
             FROM coral.columns WHERE schema_name = 'alpha' AND table_name = 'users'",
        )
        .await
        .expect("catalog query should succeed"),
    );

    assert_eq!(
        rows,
        vec![
            json!({"column_name": "id", "ordinal_position": 0}),
            json!({"column_name": "team_id", "ordinal_position": 1}),
            json!({"column_name": "name", "ordinal_position": 2}),
        ]
    );
}

#[tokio::test]
async fn list_tables_matches_catalog() {
    let (_temp, sources) = build_catalog_sources();

    let listed = CoralQuery::list_tables(&sources, test_runtime(), None, None)
        .await
        .expect("list_tables should succeed");
    let catalog_rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &sources,
            test_runtime(),
            "SELECT schema_name, table_name, description FROM coral.tables ORDER BY schema_name, table_name",
        )
        .await
        .expect("catalog query should succeed"),
    );

    assert_eq!(
        listed.iter().map(table_summary).collect::<Vec<_>>(),
        catalog_rows
            .iter()
            .map(|row| {
                (
                    row["schema_name"].as_str().expect("schema").to_string(),
                    row["table_name"].as_str().expect("table").to_string(),
                    row["description"]
                        .as_str()
                        .expect("description")
                        .to_string(),
                )
            })
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn list_tables_empty_when_no_sources() {
    let tables = CoralQuery::list_tables(&[], test_runtime(), None, None)
        .await
        .expect("empty source list should succeed");

    assert!(tables.is_empty());
}

#[tokio::test]
async fn join_across_two_sources() {
    let (_temp, sources) = build_catalog_sources();

    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &sources,
            test_runtime(),
            "SELECT u.name, t.team_name \
             FROM alpha.users u \
             JOIN beta.teams t ON u.team_id = t.id \
             ORDER BY u.id",
        )
        .await
        .expect("join should succeed"),
    );

    assert_eq!(
        rows,
        vec![
            json!({"name": "Ada", "team_name": "Platform"}),
            json!({"name": "Grace", "team_name": "Infra"}),
            json!({"name": "Linus", "team_name": "Platform"}),
        ]
    );
}

#[tokio::test]
async fn query_nonexistent_schema_returns_error() {
    let (_temp, sources) = build_catalog_sources();

    let error = CoralQuery::execute_sql(&sources, test_runtime(), "SELECT * FROM missing.users")
        .await
        .expect_err("missing schema should fail");

    assert_table_not_found(error, "missing", "users");
}

fn table_summary(table: &TableInfo) -> (String, String, String) {
    (
        table.schema_name.clone(),
        table.table_name.clone(),
        table.description.clone(),
    )
}

#[expect(
    dead_code,
    reason = "Reserved for targeted schema assertions as this suite grows."
)]
fn table_column_names(table: &TableInfo) -> Vec<String> {
    table
        .columns
        .iter()
        .map(|column: &ColumnInfo| column.name.clone())
        .collect()
}

fn http_manifest_with_inputs() -> Value {
    json!({
        "name": "demo",
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "http",
        "inputs": {
            "DD_SITE": {
                "kind": "variable",
                "default": "datadoghq.com",
                "hint": "Datadog site host"
            },
            "ACCOUNT_ID": {
                "kind": "variable",
                "hint": "Numeric account identifier"
            },
            "API_TOKEN": {
                "kind": "secret",
                "hint": "Bearer token"
            }
        },
        "base_url": "https://api.{{input.DD_SITE}}",
        "tables": [{
            "name": "items",
            "description": "Example items",
            "request": {
                "method": "GET",
                "path": "/api/items"
            },
            "response": {
                "rows_path": ["data"]
            },
            "columns": [
                { "name": "id", "type": "Int64" }
            ]
        }]
    })
}

fn http_manifest_with_function() -> Value {
    json!({
        "name": "searchy",
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "http",
        "base_url": "https://example.com",
        "tables": [
            {
                "name": "placeholder",
                "description": "Placeholder table",
                "filters": [{
                    "name": "query",
                    "type": "Utf8",
                    "description": "Provider-native placeholder search text",
                    "mode": "search"
                }],
                "search_limits": {
                    "default_top_k": 10,
                    "max_top_k": 50,
                    "max_calls_per_query": 1
                },
                "request": {
                    "method": "GET",
                    "path": "/placeholder"
                },
                "columns": [
                    { "name": "id", "type": "Utf8" },
                    {
                        "name": "query",
                        "type": "Utf8",
                        "virtual": true,
                        "expr": { "kind": "from_filter", "key": "query" }
                    }
                ]
            },
            {
                "name": "issue_details",
                "description": "Issue detail rows",
                "filters": [{ "name": "issue_id", "required": true }],
                "request": {
                    "method": "GET",
                    "path": "/issues/{{filter.issue_id}}"
                },
                "columns": [
                    { "name": "id", "type": "Utf8" }
                ]
            }
        ],
        "functions": [{
            "name": "search_issues",
            "kind": "search",
            "description": "Search issues",
            "search_limits": {
                "default_top_k": 5,
                "max_top_k": 100,
                "max_calls_per_query": 1
            },
            "args": [
                {
                    "name": "q",
                    "required": true,
                    "bind": { "arg": "q" }
                },
                {
                    "name": "mode",
                    "values": ["lexical", "semantic", "hybrid"],
                    "bind": { "arg": "search_type" }
                }
            ],
            "request": {
                "method": "GET",
                "path": "/search/issues",
                "query": [
                    { "name": "q", "from": "arg", "key": "q" },
                    { "name": "search_type", "from": "arg", "key": "search_type" }
                ]
            },
            "response": {
                "rows_path": ["items"]
            },
            "columns": [
                { "name": "id", "type": "Utf8" },
                { "name": "title", "type": "Utf8" },
                { "name": "score", "type": "Float64" }
            ]
        }]
    })
}

fn build_demo_source(variables: &[(&str, &str)], secrets: &[(&str, &str)]) -> QuerySource {
    let to_map = |items: &[(&str, &str)]| -> BTreeMap<String, String> {
        items
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect()
    };
    build_source_with_inputs(
        http_manifest_with_inputs(),
        to_map(variables),
        to_map(secrets),
    )
}

fn jsonl_manifest_with_inputs(dir: &std::path::Path) -> Value {
    json!({
        "name": "jsonl_inputs",
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "jsonl",
        "inputs": {
            "DATASET": {
                "kind": "variable",
                "default": "events",
                "hint": "Dataset label"
            },
            "LOCAL_TOKEN": {
                "kind": "secret",
                "hint": "Local file source token"
            }
        },
        "tables": [{
            "name": "events",
            "description": "Input metadata regression fixture",
            "source": {
                "location": dir_url(dir),
                "glob": "**/*.jsonl"
            },
            "columns": [
                { "name": "id", "type": "Int64" }
            ]
        }]
    })
}

#[tokio::test]
async fn coral_table_functions_lists_source_functions() {
    let sources = vec![build_source(http_manifest_with_function())];

    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &sources,
            test_runtime(),
            "SELECT schema_name, function_name, kind, description, arguments_json, result_columns_json, search_limits_json \
             FROM coral.table_functions WHERE schema_name = 'searchy'",
        )
        .await
        .expect("table function catalog query should succeed"),
    );

    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(row["schema_name"], "searchy");
    assert_eq!(row["function_name"], "search_issues");
    assert_eq!(row["kind"], "search");
    assert_eq!(row["description"], "Search issues");
    assert_eq!(
        serde_json::from_str::<Value>(row["arguments_json"].as_str().unwrap()).unwrap(),
        json!([
            { "name": "q", "required": true, "values": [] },
            { "name": "mode", "required": false, "values": ["lexical", "semantic", "hybrid"] }
        ])
    );
    assert_eq!(
        serde_json::from_str::<Value>(row["result_columns_json"].as_str().unwrap()).unwrap(),
        json!([
            { "name": "id", "type": "Utf8", "nullable": true, "description": "" },
            { "name": "title", "type": "Utf8", "nullable": true, "description": "" },
            { "name": "score", "type": "Float64", "nullable": true, "description": "" }
        ])
    );
    assert_eq!(
        serde_json::from_str::<Value>(row["search_limits_json"].as_str().unwrap()).unwrap(),
        json!({
            "default_top_k": 5,
            "max_top_k": 100,
            "max_calls_per_query": 1
        })
    );
}

#[tokio::test]
async fn coral_search_metadata_appends_columns_without_shifting_existing_ordinals() {
    let sources = vec![build_source(http_manifest_with_function())];

    let table_functions = CoralQuery::execute_sql(
        &sources,
        test_runtime(),
        "SELECT * FROM coral.table_functions WHERE schema_name = 'searchy'",
    )
    .await
    .expect("table function catalog query should succeed");
    assert_eq!(
        table_functions
            .schema()
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        vec![
            "schema_name",
            "function_name",
            "description",
            "arguments_json",
            "result_columns_json",
            "kind",
            "search_limits_json",
        ]
    );

    let columns = CoralQuery::execute_sql(
        &sources,
        test_runtime(),
        "SELECT * FROM coral.columns \
         WHERE schema_name = 'searchy' AND table_name = 'placeholder' \
         LIMIT 1",
    )
    .await
    .expect("columns catalog query should succeed");
    assert_eq!(
        columns
            .schema()
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        vec![
            "schema_name",
            "table_name",
            "ordinal_position",
            "column_name",
            "data_type",
            "is_nullable",
            "is_virtual",
            "is_required_filter",
            "description",
            "filter_mode",
        ]
    );
}

#[tokio::test]
async fn coral_tables_exposes_search_limits() {
    let sources = vec![build_source(http_manifest_with_function())];

    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &sources,
            test_runtime(),
            "SELECT search_limits_json FROM coral.tables \
             WHERE schema_name = 'searchy' AND table_name = 'placeholder'",
        )
        .await
        .expect("tables catalog query should succeed"),
    );

    assert_eq!(rows.len(), 1);
    assert_eq!(
        serde_json::from_str::<Value>(rows[0]["search_limits_json"].as_str().unwrap()).unwrap(),
        json!({
            "default_top_k": 10,
            "max_top_k": 50,
            "max_calls_per_query": 1
        })
    );
}

#[tokio::test]
async fn coral_filters_lists_filter_metadata() {
    let sources = vec![build_source(http_manifest_with_function())];

    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &sources,
            test_runtime(),
            "SELECT table_name, filter_name, filter_mode, is_required, data_type, description \
             FROM coral.filters WHERE schema_name = 'searchy' AND filter_mode = 'search'",
        )
        .await
        .expect("filters catalog query should succeed"),
    );

    assert_eq!(
        rows,
        vec![json!({
            "table_name": "placeholder",
            "filter_name": "query",
            "filter_mode": "search",
            "is_required": false,
            "data_type": "Utf8",
            "description": "Provider-native placeholder search text",
        })]
    );
}

#[tokio::test]
async fn coral_columns_exposes_filter_mode_for_virtual_filters() {
    let sources = vec![build_source(http_manifest_with_function())];

    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &sources,
            test_runtime(),
            "SELECT column_name, is_virtual, is_required_filter, filter_mode \
             FROM coral.columns \
             WHERE schema_name = 'searchy' AND table_name = 'placeholder' AND column_name = 'query'",
        )
        .await
        .expect("columns catalog query should succeed"),
    );

    assert_eq!(
        rows,
        vec![json!({
            "column_name": "query",
            "is_virtual": true,
            "is_required_filter": false,
            "filter_mode": "search",
        })]
    );
}

#[tokio::test]
async fn list_catalog_matches_table_function_metadata() {
    let sources = vec![build_source(http_manifest_with_function())];

    let catalog = CoralQuery::list_catalog(&sources, test_runtime(), Some("searchy"))
        .await
        .expect("list_catalog should succeed");

    let functions = &catalog.table_functions;
    assert_eq!(functions.len(), 1);
    let function = &functions[0];
    assert_eq!(function.schema_name, "searchy");
    assert_eq!(function.function_name, "search_issues");
    assert_eq!(function.description, "Search issues");
    assert_eq!(function.arguments.len(), 2);
    assert_eq!(function.arguments[0].name, "q");
    assert!(function.arguments[0].required);
    assert_eq!(
        function.arguments[1].values,
        ["lexical", "semantic", "hybrid"]
    );
    assert_eq!(function.result_columns.len(), 3);
    assert_eq!(function.result_columns[0].name, "id");
    assert_eq!(function.result_columns[1].name, "title");
    assert_eq!(function.result_columns[2].data_type, "Float64");
}

#[tokio::test]
async fn list_catalog_collects_tables_and_functions_together() {
    let sources = vec![build_source(http_manifest_with_function())];

    let catalog = CoralQuery::list_catalog(&sources, test_runtime(), Some("searchy"))
        .await
        .expect("list_catalog should succeed");

    assert_eq!(
        catalog
            .tables
            .iter()
            .map(|table| table.table_name.as_str())
            .collect::<Vec<_>>(),
        ["issue_details", "placeholder"]
    );
    assert_eq!(catalog.table_functions.len(), 1);
    assert_eq!(catalog.table_functions[0].function_name, "search_issues");
}

#[tokio::test]
async fn coral_inputs_exposes_variable_values_and_defaults() {
    let sources = vec![build_demo_source(
        &[("ACCOUNT_ID", "123456")],
        &[("API_TOKEN", "secret-value")],
    )];

    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &sources,
            test_runtime(),
            "SELECT key, kind, value, default_value, hint, required, is_set \
             FROM coral.inputs WHERE schema_name = 'demo' ORDER BY key",
        )
        .await
        .expect("catalog query should succeed"),
    );

    // Arrow's JSON writer omits NULL fields from object output.
    assert_eq!(
        rows,
        vec![
            json!({
                "key": "ACCOUNT_ID",
                "kind": "variable",
                "value": "123456",
                "hint": "Numeric account identifier",
                "required": true,
                "is_set": true,
            }),
            json!({
                "key": "API_TOKEN",
                "kind": "secret",
                "hint": "Bearer token",
                "required": true,
                "is_set": true,
            }),
            json!({
                "key": "DD_SITE",
                "kind": "variable",
                "value": "datadoghq.com",
                "default_value": "datadoghq.com",
                "hint": "Datadog site host",
                "required": false,
                "is_set": true,
            }),
        ]
    );
}

#[tokio::test]
async fn coral_inputs_exposes_file_source_inputs() {
    let temp = TempDir::new().expect("temp dir");
    let data_dir = temp.path().join("jsonl-inputs");
    write_jsonl_file(&data_dir, "events.jsonl", &[json!({"id": 1})]);
    let sources = vec![build_source_with_inputs(
        jsonl_manifest_with_inputs(&data_dir),
        BTreeMap::from([("DATASET".to_string(), "audit".to_string())]),
        BTreeMap::from([("LOCAL_TOKEN".to_string(), "secret-value".to_string())]),
    )];

    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &sources,
            test_runtime(),
            "SELECT key, kind, value, default_value, hint, is_set \
             FROM coral.inputs WHERE schema_name = 'jsonl_inputs' ORDER BY key",
        )
        .await
        .expect("catalog query should succeed"),
    );

    assert_eq!(
        rows,
        vec![
            json!({
                "key": "DATASET",
                "kind": "variable",
                "value": "audit",
                "default_value": "events",
                "hint": "Dataset label",
                "is_set": true,
            }),
            json!({
                "key": "LOCAL_TOKEN",
                "kind": "secret",
                "hint": "Local file source token",
                "is_set": true,
            }),
        ]
    );
}

#[tokio::test]
async fn coral_inputs_marks_unset_secrets_and_missing_variables() {
    let sources = vec![build_demo_source(&[], &[])];

    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &sources,
            test_runtime(),
            "SELECT key, value, is_set FROM coral.inputs \
             WHERE schema_name = 'demo' ORDER BY key",
        )
        .await
        .expect("catalog query should succeed"),
    );

    assert_eq!(
        rows,
        vec![
            json!({"key": "ACCOUNT_ID", "is_set": false}),
            json!({"key": "API_TOKEN", "is_set": false}),
            json!({"key": "DD_SITE", "value": "datadoghq.com", "is_set": true}),
        ]
    );
}

#[tokio::test]
async fn coral_inputs_never_exposes_secret_values() {
    // Canary: secret values must never appear in coral.inputs under any filter.
    let sources = vec![build_demo_source(&[], &[("API_TOKEN", "ultra-secret")])];

    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &sources,
            test_runtime(),
            "SELECT value FROM coral.inputs WHERE kind = 'secret'",
        )
        .await
        .expect("catalog query should succeed"),
    );

    assert!(!rows.is_empty(), "expected at least one secret row");
    for row in &rows {
        assert!(
            row["value"].is_null(),
            "secret value must be NULL, got {row}"
        );
    }
}

#[tokio::test]
async fn coral_inputs_reports_explicit_empty_variable_as_set() {
    // A user-configured empty string is still "set" — HTTP input resolution
    // and required-variable validation both treat the key's presence as
    // authoritative. See crates/coral-app/src/sources/manager.rs.
    let sources = vec![build_demo_source(&[("ACCOUNT_ID", "")], &[])];

    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &sources,
            test_runtime(),
            "SELECT key, value, is_set FROM coral.inputs \
             WHERE schema_name = 'demo' AND key = 'ACCOUNT_ID'",
        )
        .await
        .expect("catalog query should succeed"),
    );

    assert_eq!(
        rows,
        vec![json!({"key": "ACCOUNT_ID", "value": "", "is_set": true})]
    );
}

#[tokio::test]
async fn coral_inputs_empty_for_sources_without_declared_inputs() {
    // The JSONL fixtures declare no inputs; coral.inputs should be empty.
    let (_temp, sources) = build_catalog_sources();

    let rows = execution_to_rows(
        &CoralQuery::execute_sql(&sources, test_runtime(), "SELECT * FROM coral.inputs")
            .await
            .expect("catalog query should succeed"),
    );

    assert!(rows.is_empty(), "expected no inputs, got {rows:?}");
}
