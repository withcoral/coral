use coral_engine::{ColumnInfo, CoralQuery, CoreError, QuerySource, TableInfo};
use serde_json::{Value, json};
use tempfile::TempDir;

use crate::harness::{TestRuntime, build_source, dir_url, execution_to_rows, write_jsonl_file};

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

    match error {
        CoreError::InvalidInput(detail)
        | CoreError::NotFound(detail)
        | CoreError::Internal(detail) => {
            assert!(
                detail.contains("missing") || detail.contains("users"),
                "error should mention missing relation: {detail}"
            );
        }
        other => panic!("unexpected error for missing schema: {other:?}"),
    }
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
