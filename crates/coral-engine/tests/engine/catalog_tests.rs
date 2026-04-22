use std::collections::BTreeMap;

use coral_engine::{ColumnInfo, CoralQuery, QuerySource, TableInfo};
use serde_json::{Value, json};
use tempfile::TempDir;

use crate::harness::{
    TestRuntime, assert_invalid_input, build_source, build_source_with_inputs, dir_url,
    execution_to_rows, write_jsonl_file,
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
            &TestRuntime,
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
            &TestRuntime,
            "SELECT column_name, data_type, is_virtual, is_required_filter \
             FROM coral.columns WHERE schema_name = 'alpha' AND table_name = 'users' \
             ORDER BY ordinal_position",
        )
        .await
        .expect("catalog query should succeed"),
    );

    assert_eq!(
        rows,
        vec![
            json!({"column_name": "id", "data_type": "Int64", "is_virtual": false, "is_required_filter": false}),
            json!({"column_name": "team_id", "data_type": "Int64", "is_virtual": false, "is_required_filter": false}),
            json!({"column_name": "name", "data_type": "Utf8", "is_virtual": false, "is_required_filter": false}),
        ]
    );
}

#[tokio::test]
async fn coral_columns_default_row_order_matches_ordinal_position() {
    let (_temp, sources) = build_catalog_sources();

    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &sources,
            &TestRuntime,
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

    let listed = CoralQuery::list_tables(&sources, &TestRuntime, None)
        .await
        .expect("list_tables should succeed");
    let catalog_rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &sources,
            &TestRuntime,
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
    let tables = CoralQuery::list_tables(&[], &TestRuntime, None)
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
            &TestRuntime,
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

    let error = CoralQuery::execute_sql(&sources, &TestRuntime, "SELECT * FROM missing.users")
        .await
        .expect_err("missing schema should fail");

    assert_invalid_input(error, "table 'datafusion.missing.users' not found");
}

fn table_summary(table: &TableInfo) -> (String, String, String) {
    (
        table.schema_name.clone(),
        table.table_name.clone(),
        table.description.clone(),
    )
}

#[allow(
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

#[tokio::test]
async fn coral_inputs_exposes_variable_values_and_defaults() {
    let sources = vec![build_demo_source(
        &[("ACCOUNT_ID", "123456")],
        &[("API_TOKEN", "secret-value")],
    )];

    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &sources,
            &TestRuntime,
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
async fn coral_inputs_marks_unset_secrets_and_missing_variables() {
    let sources = vec![build_demo_source(&[], &[])];

    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &sources,
            &TestRuntime,
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
            &TestRuntime,
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
            &TestRuntime,
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
        &CoralQuery::execute_sql(&sources, &TestRuntime, "SELECT * FROM coral.inputs")
            .await
            .expect("catalog query should succeed"),
    );

    assert!(rows.is_empty(), "expected no inputs, got {rows:?}");
}
