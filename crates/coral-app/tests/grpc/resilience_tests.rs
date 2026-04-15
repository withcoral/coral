use std::fs;

use coral_api::v1::{ExecuteSqlRequest, SourceSecret, SourceVariable};
use coral_client::{batches_to_json_rows, decode_execute_sql_response, default_workspace};
use tonic::Request;

use crate::harness::{GrpcHarness, fixture_manifest_with_inputs_yaml, fixture_manifest_yaml};

#[tokio::test]
async fn broken_source_does_not_block_healthy_sources() {
    let harness = GrpcHarness::new().await;

    harness
        .import_source(
            fixture_manifest_yaml(harness.temp_path()),
            Vec::new(),
            Vec::new(),
        )
        .await;
    harness
        .import_source(
            fixture_manifest_with_inputs_yaml(),
            vec![SourceVariable {
                key: "API_BASE".to_string(),
                value: "https://example.com".to_string(),
            }],
            vec![SourceSecret {
                key: "API_TOKEN".to_string(),
                value: "secret-token".to_string(),
            }],
        )
        .await;

    fs::remove_file(
        harness
            .config_dir()
            .join("workspaces")
            .join("default")
            .join("sources")
            .join("secured_messages")
            .join("secrets.env"),
    )
    .expect("remove broken source secret file");

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
        }))
        .await
        .expect_err("broken source query should fail");
    assert_eq!(broken.code(), tonic::Code::InvalidArgument);
}
