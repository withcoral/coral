use std::path::Path;

use coral_engine::CoralQuery;
use serde_json::{Value, json};
use tempfile::TempDir;

use crate::harness::{
    TestRuntime, assert_invalid_input, build_source, build_source_with_secrets, dir_url,
    execution_to_rows, users_batch, write_parquet_file,
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
async fn parquet_manifest_with_declared_secret_inputs_registers_and_queries() {
    // Regression: parquet manifests that declare secrets via the `inputs:`
    // block must surface those names in `required_secret_names()`, otherwise
    // `load_query_source` drops the stored secrets and the source silently
    // fails to register — leaving its schema absent from the catalog.
    let temp = TempDir::new().expect("temp dir");
    write_parquet_file(temp.path(), "users.parquet", &users_batch());

    let manifest = json!({
        "name": "warehouse",
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "parquet",
        "inputs": {
            "api_token": { "kind": "secret" },
            "signing_key": { "kind": "secret" },
        },
        "tables": [{
            "name": "users",
            "description": "Warehouse users",
            "source": {
                "location": dir_url(temp.path()),
                "glob": "**/*.parquet"
            },
            "columns": []
        }]
    });

    let source = build_source_with_secrets(
        manifest,
        [("api_token", "token-value"), ("signing_key", "key-value")],
    );

    let schemata = execution_to_rows(
        &CoralQuery::execute_sql(
            &[source],
            &TestRuntime,
            "SELECT schema_name FROM information_schema.schemata \
             WHERE schema_name = 'warehouse'",
        )
        .await
        .expect("schemata query should succeed"),
    );
    assert_eq!(schemata, vec![json!({"schema_name": "warehouse"})]);

    // The table must be queryable end-to-end: declared secret inputs must
    // not block registration for a local-filesystem-backed source.
    let temp2 = TempDir::new().expect("temp dir");
    write_parquet_file(temp2.path(), "users.parquet", &users_batch());
    let manifest2 = json!({
        "name": "warehouse2",
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "parquet",
        "inputs": {
            "api_token": { "kind": "secret" },
        },
        "tables": [{
            "name": "users",
            "description": "Warehouse users",
            "source": {
                "location": dir_url(temp2.path()),
                "glob": "**/*.parquet"
            },
            "columns": []
        }]
    });
    let source2 = build_source_with_secrets(manifest2, [("api_token", "token-value")]);
    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &[source2],
            &TestRuntime,
            "SELECT id FROM warehouse2.users ORDER BY id",
        )
        .await
        .expect("query should succeed"),
    );
    assert_eq!(
        rows,
        vec![json!({"id": 1}), json!({"id": 2}), json!({"id": 3})]
    );
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

    assert_invalid_input(error, "table 'datafusion.parquet_missing.users' not found");
}
