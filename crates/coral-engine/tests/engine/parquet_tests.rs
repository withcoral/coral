use serde_json::{Map, Value, json};
use tempfile::TempDir;

use crate::harness::{
    assert_table_not_found, assert_users_query_matrix, build_source, build_source_with_secrets,
    source_error, source_rows, users_batch, users_parquet_manifest, users_parquet_source,
    write_parquet_file,
};

fn parquet_source_with_secret_inputs(
    name: &str,
    secrets: &[(&'static str, &'static str)],
) -> (TempDir, coral_engine::QuerySource) {
    let temp = TempDir::new().expect("temp dir");
    write_parquet_file(temp.path(), "users.parquet", &users_batch());
    let inputs = secrets
        .iter()
        .map(|(key, _value)| ((*key).to_string(), json!({ "kind": "secret" })))
        .collect::<Map<_, _>>();
    let mut manifest = users_parquet_manifest(name, temp.path());
    manifest
        .as_object_mut()
        .expect("manifest object")
        .insert("inputs".to_string(), Value::Object(inputs));
    let source = build_source_with_secrets(manifest, secrets.iter().copied());
    (temp, source)
}

#[tokio::test]
async fn parquet_users_query_matrix() {
    assert_users_query_matrix("parquet", users_parquet_source).await;
}

#[tokio::test]
async fn parquet_manifest_with_declared_secret_inputs_registers_and_queries() {
    // Regression: parquet manifests that declare secrets via the `inputs:`
    // block must surface those names in `required_secret_names()`, otherwise
    // `load_query_source` drops the stored secrets and the source silently
    // fails to register — leaving its schema absent from the catalog.
    let (_temp, source) = parquet_source_with_secret_inputs(
        "warehouse",
        &[("api_token", "token-value"), ("signing_key", "key-value")],
    );

    let schemata = source_rows(
        &source,
        "SELECT schema_name FROM information_schema.schemata \
         WHERE schema_name = 'warehouse'",
    )
    .await;
    assert_eq!(schemata, vec![json!({"schema_name": "warehouse"})]);

    // The table must be queryable end-to-end: declared secret inputs must
    // not block registration for a local-filesystem-backed source.
    let (_temp2, source2) =
        parquet_source_with_secret_inputs("warehouse2", &[("api_token", "token-value")]);
    let rows = source_rows(&source2, "SELECT id FROM warehouse2.users ORDER BY id").await;
    assert_eq!(
        rows,
        vec![json!({"id": 1}), json!({"id": 2}), json!({"id": 3})]
    );
}

#[tokio::test]
async fn missing_file_returns_error() {
    let temp = TempDir::new().expect("temp dir");
    let missing_dir = temp.path().join("missing");
    let source = build_source(users_parquet_manifest("parquet_missing", &missing_dir));

    let error = source_error(&source, "SELECT * FROM parquet_missing.users").await;

    assert_table_not_found(error, "parquet_missing", "users");
}
