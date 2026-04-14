use std::fs;

use coral_api::v1::{ListTablesRequest, SourceSecret, SourceVariable};
use coral_client::default_workspace;
use tonic::Request;

use crate::harness::{GrpcHarness, fixture_manifest_with_inputs_yaml, fixture_manifest_yaml};

#[tokio::test]
async fn broken_source_is_skipped_and_healthy_sources_still_load() {
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

    let response = harness
        .query_client()
        .list_tables(Request::new(ListTablesRequest {
            workspace: Some(default_workspace()),
        }))
        .await
        .expect("list tables should succeed despite a broken source");
    let tables = response.into_inner().tables;
    assert!(
        tables.iter().any(|t| t.schema_name == "local_messages"),
        "healthy source tables should still be present"
    );
    assert!(
        !tables.iter().any(|t| t.schema_name == "secured_messages"),
        "broken source tables should not appear"
    );
}
