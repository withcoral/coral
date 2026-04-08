use std::path::Path;

use coral_engine::CoralQuery;
use serde_json::{Value, json};
use tempfile::TempDir;

use crate::harness::{
    TestRuntime, assert_internal, build_source, dir_url, execution_to_rows, users_batch,
    write_parquet_file,
};

fn parquet_manifest(name: &str, dir: &Path) -> Value {
    json!({
        "name": name,
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "parquet",
        "tables": [{
            "name": "users",
            "description": "Users fixture",
            "source": {
                "location": dir_url(dir),
                "glob": "**/*.parquet"
            },
            "columns": []
        }]
    })
}

#[tokio::test]
async fn select_all_from_parquet_source() {
    let temp = TempDir::new().expect("temp dir");
    write_parquet_file(temp.path(), "users.parquet", &users_batch());
    let source = build_source(parquet_manifest("parquet_users", temp.path()));

    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &[source],
            &TestRuntime,
            "SELECT id, name, email FROM parquet_users.users ORDER BY id",
        )
        .await
        .expect("query should succeed"),
    );

    assert_eq!(
        rows,
        vec![
            json!({"id": 1, "name": "Ada", "email": "ada@example.com"}),
            json!({"id": 2, "name": "Grace", "email": "grace@example.com"}),
            json!({"id": 3, "name": "Linus", "email": "linus@example.com"}),
        ]
    );
}

#[tokio::test]
async fn select_with_column_projection() {
    let temp = TempDir::new().expect("temp dir");
    write_parquet_file(temp.path(), "users.parquet", &users_batch());
    let source = build_source(parquet_manifest("parquet_projection", temp.path()));

    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &[source],
            &TestRuntime,
            "SELECT email FROM parquet_projection.users ORDER BY email",
        )
        .await
        .expect("query should succeed"),
    );

    assert_eq!(
        rows,
        vec![
            json!({"email": "ada@example.com"}),
            json!({"email": "grace@example.com"}),
            json!({"email": "linus@example.com"}),
        ]
    );
}

#[tokio::test]
async fn select_with_where_filter() {
    let temp = TempDir::new().expect("temp dir");
    write_parquet_file(temp.path(), "users.parquet", &users_batch());
    let source = build_source(parquet_manifest("parquet_filter", temp.path()));

    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &[source],
            &TestRuntime,
            "SELECT id, name FROM parquet_filter.users WHERE id = 3",
        )
        .await
        .expect("query should succeed"),
    );

    assert_eq!(rows, vec![json!({"id": 3, "name": "Linus"})]);
}

#[tokio::test]
async fn select_with_order_by_and_limit() {
    let temp = TempDir::new().expect("temp dir");
    write_parquet_file(temp.path(), "users.parquet", &users_batch());
    let source = build_source(parquet_manifest("parquet_order", temp.path()));

    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &[source],
            &TestRuntime,
            "SELECT id, name FROM parquet_order.users ORDER BY name DESC LIMIT 2",
        )
        .await
        .expect("query should succeed"),
    );

    assert_eq!(
        rows,
        vec![
            json!({"id": 3, "name": "Linus"}),
            json!({"id": 2, "name": "Grace"})
        ]
    );
}

#[tokio::test]
async fn select_count_aggregation() {
    let temp = TempDir::new().expect("temp dir");
    write_parquet_file(temp.path(), "users.parquet", &users_batch());
    let source = build_source(parquet_manifest("parquet_count", temp.path()));

    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &[source],
            &TestRuntime,
            "SELECT COUNT(*) AS n FROM parquet_count.users",
        )
        .await
        .expect("query should succeed"),
    );

    assert_eq!(rows, vec![json!({"n": 3})]);
}

#[tokio::test]
async fn missing_file_returns_error() {
    let temp = TempDir::new().expect("temp dir");
    let missing_dir = temp.path().join("missing");
    let source = build_source(parquet_manifest("parquet_missing", &missing_dir));

    let error = CoralQuery::execute_sql(
        &[source],
        &TestRuntime,
        "SELECT * FROM parquet_missing.users",
    )
    .await
    .expect_err("missing parquet source should fail");

    assert_internal(
        error,
        "Error during planning: table 'datafusion.parquet_missing.users' not found",
    );
}
