use std::fs;

use coral_api::v1::{ExecuteSqlRequest, ListTablesRequest, SourceSecret, SourceVariable};
use coral_client::default_workspace;
use tonic::Request;

use crate::harness::{GrpcHarness, fixture_manifest_with_inputs_yaml, fixture_manifest_yaml};

#[tokio::test]
async fn broken_source_surfaces_failed_precondition() {
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

    let list_err = harness
        .query_client()
        .list_tables(Request::new(ListTablesRequest {
            workspace: Some(default_workspace()),
        }))
        .await
        .expect_err("list tables should fail when a source is broken");
    assert_eq!(list_err.code(), tonic::Code::FailedPrecondition);
    assert!(
        list_err.message().contains("missing secret 'API_TOKEN'"),
        "error should name the missing secret, got: {}",
        list_err.message()
    );

    let query_err = harness
        .query_client()
        .execute_sql(Request::new(ExecuteSqlRequest {
            workspace: Some(default_workspace()),
            sql: "SELECT COUNT(*) AS n FROM local_messages.messages".to_string(),
        }))
        .await
        .expect_err("query should fail when a source is broken");
    assert_eq!(query_err.code(), tonic::Code::FailedPrecondition);
}
