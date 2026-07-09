use std::path::Path;

use coral_engine::{CoralQuery, StatisticsObservationScope};
use serde_json::{Value, json};
use tempfile::TempDir;

use crate::harness::{
    assert_table_not_found, build_source, build_source_with_secrets, dir_url,
    execute_sql_with_trace_observations, execution_to_rows, test_runtime, users_batch,
    write_parquet_file,
};

fn parquet_manifest(name: &str, dir: &Path) -> Value {
    json!({
        "name": name,
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "file",
        "tables": [{
            "name": "users",
            "description": "Users fixture",
            "format": "parquet",
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

    let (execution, observations) = execute_sql_with_trace_observations(
        &[source],
        "SELECT id, name, email FROM parquet_users.users ORDER BY id",
    )
    .await
    .expect("query should succeed");
    let rows = execution_to_rows(&execution);

    assert_eq!(
        rows,
        vec![
            json!({"id": 1, "name": "Ada", "email": "ada@example.com"}),
            json!({"id": 2, "name": "Grace", "email": "grace@example.com"}),
            json!({"id": 3, "name": "Linus", "email": "linus@example.com"}),
        ]
    );

    assert_eq!(observations.len(), 1);
    let observation = observations.first().expect("one observation");
    assert_eq!(observation.scope, StatisticsObservationScope::TableGlobal);
    let by_name = observation
        .columns
        .iter()
        .map(|column| (column.column_name.as_str(), column))
        .collect::<std::collections::HashMap<_, _>>();
    let id = by_name.get("id").expect("id stats");
    let name = by_name.get("name").expect("name stats");
    assert_eq!(id.approx_distinct_count.as_ref().unwrap().value, 3);
    assert_eq!(name.sample_count, 3);
}

#[tokio::test]
async fn select_with_column_projection() {
    let temp = TempDir::new().expect("temp dir");
    write_parquet_file(temp.path(), "users.parquet", &users_batch());
    let source = build_source(parquet_manifest("parquet_projection", temp.path()));

    let (execution, observations) = execute_sql_with_trace_observations(
        &[source],
        "SELECT email FROM parquet_projection.users ORDER BY email",
    )
    .await
    .expect("query should succeed");
    let rows = execution_to_rows(&execution);

    assert_eq!(
        rows,
        vec![
            json!({"email": "ada@example.com"}),
            json!({"email": "grace@example.com"}),
            json!({"email": "linus@example.com"}),
        ]
    );
    assert_eq!(observations.len(), 1);
    let observation = observations.first().expect("one observation");
    assert_eq!(observation.scope, StatisticsObservationScope::Unknown);
    assert_eq!(observation.columns.len(), 1);
    let column = observation.columns.first().expect("one column");
    assert_eq!(column.column_name, "email");
}

#[tokio::test]
async fn select_with_where_filter() {
    let temp = TempDir::new().expect("temp dir");
    write_parquet_file(temp.path(), "users.parquet", &users_batch());
    let source = build_source(parquet_manifest("parquet_filter", temp.path()));

    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &[source],
            test_runtime(),
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
            test_runtime(),
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

    let (execution, observations) = execute_sql_with_trace_observations(
        &[source],
        "SELECT COUNT(*) AS n FROM parquet_count.users",
    )
    .await
    .expect("query should succeed");
    let rows = execution_to_rows(&execution);

    assert_eq!(rows, vec![json!({"n": 3})]);
    assert!(
        observations
            .iter()
            .all(|observation| observation.scope != StatisticsObservationScope::TableGlobal),
        "aggregate scans must not persist projected parquet stats as table-global"
    );
}

#[tokio::test]
async fn multi_file_scan_emits_one_table_global_observation() {
    let temp = TempDir::new().expect("temp dir");
    write_parquet_file(temp.path(), "users-a.parquet", &users_batch());
    write_parquet_file(temp.path(), "users-b.parquet", &users_batch());
    let source = build_source(parquet_manifest("parquet_multi", temp.path()));

    let (_execution, observations) = execute_sql_with_trace_observations(
        &[source],
        "SELECT id, name, email FROM parquet_multi.users",
    )
    .await
    .expect("query should succeed");

    assert_eq!(observations.len(), 1);
    let observation = observations.first().expect("one observation");
    assert_eq!(observation.scope, StatisticsObservationScope::TableGlobal);
    let id = observation
        .columns
        .iter()
        .find(|column| column.column_name == "id")
        .expect("id stats");
    assert_eq!(id.sample_count, 6);
    assert_eq!(id.null_count.as_ref().expect("null count").value, 0);
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
        "backend": "file",
        "inputs": {
            "api_token": { "kind": "secret" },
            "signing_key": { "kind": "secret" },
        },
        "tables": [{
            "name": "users",
            "description": "Warehouse users",
            "format": "parquet",
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
            test_runtime(),
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
        "backend": "file",
        "inputs": {
            "api_token": { "kind": "secret" },
        },
        "tables": [{
            "name": "users",
            "description": "Warehouse users",
            "format": "parquet",
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
            test_runtime(),
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
        test_runtime(),
        "SELECT * FROM parquet_missing.users",
    )
    .await
    .expect_err("missing parquet source should fail");

    assert_table_not_found(error, "parquet_missing", "users");
}
