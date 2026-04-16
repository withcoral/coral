use std::collections::BTreeMap;

use coral_engine::{ColumnInfo, CoralQuery, QuerySource, TableInfo};
use serde_json::{Value, json};
use tempfile::TempDir;

use crate::harness::{
    TestRuntime, assert_invalid_input, build_source, dir_url, execution_to_rows, write_jsonl_file,
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

fn build_source_variable_sources() -> Vec<QuerySource> {
    vec![
        crate::harness::build_source_with_inputs(
            json!({
                "name": "datadog",
                "version": "2.1.0",
                "dsl_version": 3,
                "backend": "http",
                "base_url": "https://api.{{variable.DD_SITE|datadoghq.com}}",
                "auth": {
                    "headers": [{
                        "name": "DD-API-KEY",
                        "from": "secret",
                        "key": "DD_API_KEY"
                    }]
                },
                "tables": [{
                    "name": "dashboards",
                    "description": "Datadog dashboards",
                    "request": { "path": "/api/v1/dashboard" },
                    "columns": [{ "name": "id", "type": "Utf8" }]
                }]
            }),
            BTreeMap::from([("DD_SITE".to_string(), "datadoghq.eu".to_string())]),
            BTreeMap::from([("DD_API_KEY".to_string(), "secret".to_string())]),
        ),
        crate::harness::build_source_with_inputs(
            json!({
                "name": "sentry",
                "version": "3.4.5",
                "dsl_version": 3,
                "backend": "http",
                "base_url": "{{variable.SENTRY_BASE|https://sentry.io/api/0}}",
                "auth": {
                    "headers": [{
                        "name": "Authorization",
                        "from": "secret",
                        "key": "SENTRY_TOKEN"
                    }]
                },
                "tables": [{
                    "name": "issues",
                    "description": "Sentry issues",
                    "request": {
                        "path": "/organizations/{{variable.SENTRY_ORG}}/issues/"
                    },
                    "columns": [{ "name": "id", "type": "Utf8" }]
                }]
            }),
            BTreeMap::from([("SENTRY_ORG".to_string(), "withcoral".to_string())]),
            BTreeMap::from([("SENTRY_TOKEN".to_string(), "secret".to_string())]),
        ),
    ]
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
async fn coral_source_variables_returns_effective_values_without_secrets() {
    let sources = build_source_variable_sources();

    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &sources,
            &TestRuntime,
            "SELECT schema_name, variable_key, variable_value, is_defaulted, is_required, \
             default_value, source_origin, manifest_version \
             FROM coral.source_variables ORDER BY schema_name, variable_key",
        )
        .await
        .expect("catalog query should succeed"),
    );

    assert_eq!(
        rows,
        vec![
            json!({
                "schema_name": "datadog",
                "variable_key": "DD_SITE",
                "variable_value": "datadoghq.eu",
                "is_defaulted": false,
                "is_required": false,
                "default_value": "datadoghq.com",
                "source_origin": "imported",
                "manifest_version": "2.1.0"
            }),
            json!({
                "schema_name": "sentry",
                "variable_key": "SENTRY_BASE",
                "variable_value": "https://sentry.io/api/0",
                "is_defaulted": true,
                "is_required": false,
                "default_value": "https://sentry.io/api/0",
                "source_origin": "imported",
                "manifest_version": "3.4.5"
            }),
            json!({
                "schema_name": "sentry",
                "variable_key": "SENTRY_ORG",
                "variable_value": "withcoral",
                "is_defaulted": false,
                "is_required": true,
                "default_value": "",
                "source_origin": "imported",
                "manifest_version": "3.4.5"
            }),
        ]
    );
}

#[tokio::test]
async fn coral_source_variables_joins_cleanly_with_coral_tables() {
    let sources = build_source_variable_sources();

    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &sources,
            &TestRuntime,
            "SELECT sv.schema_name, sv.variable_key, t.table_name \
             FROM coral.source_variables sv \
             JOIN coral.tables t ON t.schema_name = sv.schema_name \
             WHERE sv.variable_key IN ('DD_SITE', 'SENTRY_ORG') \
             ORDER BY sv.schema_name, sv.variable_key",
        )
        .await
        .expect("join should succeed"),
    );

    assert_eq!(
        rows,
        vec![
            json!({"schema_name": "datadog", "variable_key": "DD_SITE", "table_name": "dashboards"}),
            json!({"schema_name": "sentry", "variable_key": "SENTRY_ORG", "table_name": "issues"}),
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
