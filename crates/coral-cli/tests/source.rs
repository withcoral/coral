#![allow(
    unused_crate_dependencies,
    missing_docs,
    reason = "Integration test crates only use a small subset of the package dependencies."
)]

mod common;
#[cfg(feature = "cli-test-server")]
mod harness;

use tempfile::tempdir;

use std::process::Command;

use common::assert_contains;
#[cfg(feature = "cli-test-server")]
use common::{assert_contains_all, stderr, stdout};
#[cfg(feature = "cli-test-server")]
use coral_api::v1::{
    QueryTestFailure, QueryTestResult, QueryTestSuccess, Source, SourceCredentialStorage,
    SourceOrigin, ValidateSourceResponse, Workspace, query_test_result,
};
#[cfg(feature = "cli-test-server")]
use harness::{MockServer, MockServerConfig};

#[cfg(feature = "cli-test-server")]
async fn run_source_test(
    sql: &str,
    outcome: query_test_result::Outcome,
    expect_success: bool,
) -> (String, String) {
    let server = MockServer::start_with_config(MockServerConfig::default().with_validate_source(
        ValidateSourceResponse {
            source: Some(Source {
                workspace: Some(Workspace {
                    name: "default".to_string(),
                }),
                name: "local_messages".to_string(),
                version: "0.1.0".to_string(),
                origin: SourceOrigin::Imported as i32,
                credential_storage: SourceCredentialStorage::File as i32,
                ..Default::default()
            }),
            query_tests: vec![QueryTestResult {
                sql: sql.to_string(),
                outcome: Some(outcome),
            }],
            ..Default::default()
        },
    ))
    .await;
    let assert = server
        .cmd()
        .args(["source", "test", "local_messages"])
        .assert();
    let assert = if expect_success {
        assert.success()
    } else {
        assert.failure()
    };
    let stdout = stdout(&assert);
    let stderr = stderr(&assert);
    server.shutdown().await;
    (stdout, stderr)
}

#[test]
fn source_test_errors_when_required_secret_is_missing() {
    let config_dir = tempdir().expect("failed to create temp dir");

    let manifest = r#"
    name: fake
    version: 1.0.0
    dsl_version: 3
    backend: http
    base_url: https://example.com
    inputs:
      TEST_API_KEY:
        kind: secret
    auth:
      type: HeaderAuth
      headers:
        - name: Authorization
          from: template
          template: "{{input.TEST_API_KEY}}"
    tables:
      - name: dummy
        description: dummy table
        request:
          method: GET
          path: /dummy
          query: []
        columns:
          - name: id
            type: Utf8
            description: dummy id"#;
    let manifest_dir = config_dir
        .path()
        .join("workspaces")
        .join("default")
        .join("sources")
        .join("fake");
    let manifest_file_path = manifest_dir.join("manifest.yaml");
    let secrets_env_path = manifest_dir.join("secrets.env");
    std::fs::create_dir_all(manifest_dir).expect("failed to create manifest directory");
    std::fs::write(manifest_file_path, manifest).expect("Failed to write manifest");
    std::fs::write(secrets_env_path, "").expect("failed to write secrets.env");

    // Write a basic config that references the fake source, but don't set the required secret.
    let config = r#"
        [workspaces.default.sources.fake]
        version = "1.0.0"
        variables = {}
        secrets = []
        origin = "imported"
    "#;
    std::fs::write(config_dir.path().join("config.toml"), config).expect("failed to write config");

    let output = Command::new(env!("CARGO_BIN_EXE_coral"))
        .arg("source")
        .arg("test")
        .arg("fake")
        .env("CORAL_CONFIG_DIR", config_dir.path())
        .output()
        .expect("failed to run coral source test");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success(), "expected non-zero exit status");
    assert_contains(&stderr, "source 'fake' is missing secret 'TEST_API_KEY'");
}

#[cfg(feature = "cli-test-server")]
#[tokio::test(flavor = "multi_thread")]
async fn source_test_exits_non_zero_when_query_tests_fail() {
    let (stdout, stderr) = run_source_test(
        "SELECT * FROM local_messages.missing",
        query_test_result::Outcome::Failure(QueryTestFailure {
            error_message: "invalid input: table not found".to_string(),
        }),
        false,
    )
    .await;
    assert_contains_all(
        &stdout,
        &["Query tests", "SELECT * FROM local_messages.missing"],
    );
    assert_contains(&stderr, "1 of 1 validation query failed");
}

#[cfg(feature = "cli-test-server")]
#[tokio::test(flavor = "multi_thread")]
async fn source_test_succeeds_when_query_tests_pass() {
    let (stdout, stderr) = run_source_test(
        "SELECT COUNT(*) AS n FROM local_messages.messages",
        query_test_result::Outcome::Success(QueryTestSuccess { row_count: 1 }),
        true,
    )
    .await;
    assert_contains_all(
        &stdout,
        &[
            "Query tests",
            "SELECT COUNT(*) AS n FROM local_messages.messages",
            "1 declared · 1 passed · 0 failed",
            "1 row",
        ],
    );
    assert!(
        stderr.trim().is_empty(),
        "expected no stderr output, got: {stderr}"
    );
}
