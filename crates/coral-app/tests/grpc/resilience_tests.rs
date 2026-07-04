#![allow(
    clippy::indexing_slicing,
    clippy::string_slice,
    reason = "test code: assertion-style indexing is idiomatic in tests"
)]

use std::fs;

use coral_api::v1::ExecuteSqlRequest;
use coral_client::{batches_to_json_rows, decode_execute_sql_response};
use tempfile::TempDir;
use tonic::Request;

use crate::harness::{
    GrpcHarness, default_workspace, fixture_manifest_with_inputs_yaml, fixture_manifest_yaml,
    source_dir,
};

#[tokio::test]
async fn broken_source_does_not_block_healthy_sources() {
    let temp_dir = TempDir::new().expect("config root");
    let config_dir = temp_dir.path().join("coral-config");
    seed_broken_secured_messages_source(&config_dir);
    let harness = GrpcHarness::start_with_owned_config_dir(temp_dir, config_dir).await;

    harness
        .import_source(
            fixture_manifest_yaml(harness.temp_path()),
            Vec::new(),
            Vec::new(),
        )
        .await;

    let tables = harness.list_tables().await;
    assert!(
        tables
            .iter()
            .any(|table| table.schema_name == "local_messages"),
        "healthy source should remain queryable"
    );
    assert!(
        !tables
            .iter()
            .any(|table| table.schema_name == "secured_messages"),
        "broken source should be omitted from registered tables"
    );

    let healthy = harness
        .query_client()
        .execute_sql(Request::new(ExecuteSqlRequest {
            workspace: Some(default_workspace()),
            sql: "SELECT COUNT(*) AS n FROM local_messages.messages".to_string(),
            guide_read_context: None,
            task_attribution: None,
        }))
        .await
        .expect("healthy source query should succeed")
        .into_inner();
    let healthy_rows = batches_to_json_rows(
        decode_execute_sql_response(&healthy)
            .expect("decode healthy query")
            .batches(),
    )
    .expect("healthy rows");
    assert_eq!(healthy_rows[0]["n"], 2);

    let broken = harness
        .query_client()
        .execute_sql(Request::new(ExecuteSqlRequest {
            workspace: Some(default_workspace()),
            sql: "SELECT * FROM secured_messages.messages".to_string(),
            guide_read_context: None,
            task_attribution: None,
        }))
        .await
        .expect_err("broken source query should fail");
    assert_eq!(broken.code(), tonic::Code::NotFound);
}

fn seed_broken_secured_messages_source(config_dir: &std::path::Path) {
    fs::create_dir_all(config_dir).expect("create config dir");
    fs::write(
        config_dir.join("config.toml"),
        r#"version = 1

[workspaces.default.sources.secured_messages]
version = "0.1.0"
variables = { API_BASE = "https://example.com" }
secrets = ["API_TOKEN"]
origin = "imported"
"#,
    )
    .expect("write legacy source config");
    let source_dir = source_dir(config_dir, "secured_messages");
    fs::create_dir_all(&source_dir).expect("create source dir");
    fs::write(
        source_dir.join("manifest.yaml"),
        fixture_manifest_with_inputs_yaml(),
    )
    .expect("write manifest");
}
