#![allow(
    clippy::indexing_slicing,
    clippy::string_slice,
    reason = "test code: assertion-style indexing is idiomatic in tests"
)]

use std::fs;

use coral_api::v1::{
    ExecuteSqlRequest, ListCatalogRequest, PaginationRequest, SourceSecret, SourceVariable,
};
use coral_client::default_workspace;
use tonic::Request;

use crate::harness::{GrpcHarness, fixture_manifest_with_inputs_yaml, fixture_manifest_yaml};

#[tokio::test]
async fn missing_credential_file_fails_query_and_catalog_loading() {
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

    let catalog_error = harness
        .catalog_client()
        .list_catalog(Request::new(ListCatalogRequest {
            workspace: Some(default_workspace()),
            schema_name: String::new(),
            kind: 1,
            pagination: Some(PaginationRequest {
                limit: 0,
                offset: 0,
            }),
        }))
        .await
        .expect_err("missing credential file should fail catalog loading");
    assert_eq!(catalog_error.code(), tonic::Code::FailedPrecondition);

    let query_error = harness
        .query_client()
        .execute_sql(Request::new(ExecuteSqlRequest {
            workspace: Some(default_workspace()),
            sql: "SELECT COUNT(*) AS n FROM local_messages.messages".to_string(),
        }))
        .await
        .expect_err("missing credential file should fail query-source loading");
    assert_eq!(query_error.code(), tonic::Code::FailedPrecondition);
}
