#![allow(
    unused_crate_dependencies,
    missing_docs,
    reason = "Integration test crates only use a small subset of the package dependencies."
)]

mod harness;

use tempfile::tempdir;

use std::path::{Path, PathBuf};
use std::process::Command;

use coral_api::v1::{
    QueryTestFailure, QueryTestResult, QueryTestSuccess, Source, SourceOrigin,
    ValidateSourceResponse, Workspace, query_test_result,
};

use harness::MockServer;

fn write_manifest(dir: &Path, name: &str, manifest: &str) -> PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, manifest).expect("failed to write manifest");
    path
}

fn zero_input_manifest() -> &'static str {
    r"
name: local_messages
version: 0.1.0
dsl_version: 3
backend: http
base_url: https://example.com
tables:
  - name: messages
    description: Messages
    request:
      method: GET
      path: /messages
      query: []
    columns:
      - name: id
        type: Utf8
        description: Message ID
"
}

fn manifest_with_bindings() -> &'static str {
    r#"
name: secured_messages
version: 0.1.0
dsl_version: 3
backend: http
inputs:
  API_BASE:
    kind: variable
  API_TOKEN:
    kind: secret
base_url: "{{input.API_BASE}}"
auth:
  headers:
    - name: Authorization
      from: template
      template: "Bearer {{input.API_TOKEN}}"
tables:
  - name: messages
    description: Messages
    request:
      method: GET
      path: /messages
      query: []
    columns:
      - name: id
        type: Utf8
        description: Message ID
"#
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
    assert!(
        stderr.contains("source 'fake' is missing secret 'TEST_API_KEY'"),
        "expected missing secret error in stderr, got: {stderr}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn source_test_exits_non_zero_when_query_tests_fail() {
    let server = MockServer::start_with_validate_source_response(ValidateSourceResponse {
        source: Some(Source {
            workspace: Some(Workspace {
                name: "default".to_string(),
            }),
            name: "local_messages".to_string(),
            version: "0.1.0".to_string(),
            secrets: Vec::new(),
            variables: Vec::new(),
            origin: SourceOrigin::Imported as i32,
        }),
        tables: Vec::new(),
        query_tests: vec![QueryTestResult {
            sql: "SELECT * FROM local_messages.missing".to_string(),
            outcome: Some(query_test_result::Outcome::Failure(QueryTestFailure {
                error_message: "invalid input: table not found".to_string(),
            })),
        }],
    })
    .await;

    let assert = server
        .cmd()
        .args(["source", "test", "local_messages"])
        .assert()
        .failure();
    let output = assert.get_output();

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stdout.contains("Query tests"),
        "expected query-test summary in stdout, got: {stdout}"
    );
    assert!(
        stdout.contains("SELECT * FROM local_messages.missing"),
        "expected failing query text in stdout, got: {stdout}"
    );
    assert!(
        stderr.contains("1 of 1 validation query failed"),
        "expected strict failure in stderr, got: {stderr}"
    );

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn source_test_succeeds_when_query_tests_pass() {
    let server = MockServer::start_with_validate_source_response(ValidateSourceResponse {
        source: Some(Source {
            workspace: Some(Workspace {
                name: "default".to_string(),
            }),
            name: "local_messages".to_string(),
            version: "0.1.0".to_string(),
            secrets: Vec::new(),
            variables: Vec::new(),
            origin: SourceOrigin::Imported as i32,
        }),
        tables: Vec::new(),
        query_tests: vec![QueryTestResult {
            sql: "SELECT COUNT(*) AS n FROM local_messages.messages".to_string(),
            outcome: Some(query_test_result::Outcome::Success(QueryTestSuccess {
                row_count: 1,
            })),
        }],
    })
    .await;

    let assert = server
        .cmd()
        .args(["source", "test", "local_messages"])
        .assert()
        .success();
    let output = assert.get_output();

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stdout.contains("Query tests"),
        "expected query-test summary in stdout, got: {stdout}"
    );
    assert!(
        stdout.contains("SELECT COUNT(*) AS n FROM local_messages.messages"),
        "expected passing query text in stdout, got: {stdout}"
    );
    assert!(
        stdout.contains("1 declared · 1 passed · 0 failed"),
        "expected passing query-test counts in stdout, got: {stdout}"
    );
    assert!(
        stdout.contains("1 row"),
        "expected passing query row count in stdout, got: {stdout}"
    );
    assert!(
        stderr.trim().is_empty(),
        "expected no stderr output, got: {stderr}"
    );

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn source_add_bundled_zero_input_succeeds_without_tty() {
    let server = MockServer::start().await;

    server
        .cmd()
        .args(["source", "add", "slack"])
        .assert()
        .success();

    let requests = server.create_bundled_source_requests();
    assert_eq!(requests.len(), 1, "expected one create_bundled_source call");
    assert_eq!(requests[0].name, "slack");
    assert!(requests[0].variables.is_empty(), "expected no variables");
    assert!(requests[0].secrets.is_empty(), "expected no secrets");

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn source_add_bundled_secret_flag_succeeds_without_tty() {
    let server = MockServer::start().await;

    server
        .cmd()
        .args([
            "source",
            "add",
            "github",
            "--input",
            "GITHUB_TOKEN=test-token",
        ])
        .assert()
        .success();

    let requests = server.create_bundled_source_requests();
    assert_eq!(requests.len(), 1, "expected one create_bundled_source call");
    assert_eq!(requests[0].name, "github");
    assert!(requests[0].variables.is_empty(), "expected no variables");
    assert_eq!(requests[0].secrets.len(), 1, "expected one secret");
    assert_eq!(requests[0].secrets[0].key, "GITHUB_TOKEN");
    assert_eq!(requests[0].secrets[0].value, "test-token");

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn source_add_bundled_secret_env_succeeds_without_tty() {
    let server = MockServer::start().await;

    server
        .cmd()
        .args(["source", "add", "github"])
        .env("GITHUB_TOKEN", "env-token")
        .assert()
        .success();

    let requests = server.create_bundled_source_requests();
    assert_eq!(requests.len(), 1, "expected one create_bundled_source call");
    assert_eq!(requests[0].secrets.len(), 1, "expected one secret");
    assert_eq!(requests[0].secrets[0].value, "env-token");

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn source_add_flag_overrides_env_value() {
    let server = MockServer::start().await;

    server
        .cmd()
        .args([
            "source",
            "add",
            "github",
            "--input",
            "GITHUB_TOKEN=flag-token",
        ])
        .env("GITHUB_TOKEN", "env-token")
        .assert()
        .success();

    let requests = server.create_bundled_source_requests();
    assert_eq!(requests.len(), 1, "expected one create_bundled_source call");
    assert_eq!(requests[0].secrets[0].value, "flag-token");

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn source_add_missing_required_input_without_tty_fails_locally() {
    let server = MockServer::start().await;

    let assert = server
        .cmd()
        .args(["source", "add", "github"])
        .assert()
        .failure();
    let stderr = String::from_utf8_lossy(&assert.get_output().stderr);

    assert!(
        stderr.contains("missing required source secret 'GITHUB_TOKEN'"),
        "expected missing secret error, got: {stderr}"
    );
    assert!(
        server.create_bundled_source_requests().is_empty(),
        "request should not reach the server"
    );

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn source_add_partial_input_still_fails_without_prompting() {
    let server = MockServer::start().await;
    let dir = tempdir().expect("manifest dir");
    let manifest_path = write_manifest(
        dir.path(),
        "secured_messages.yaml",
        manifest_with_bindings(),
    );

    let assert = server
        .cmd()
        .args([
            "source",
            "add",
            "--file",
            manifest_path.to_str().expect("utf-8 manifest path"),
            "--input",
            "API_BASE=https://flag.example.com",
        ])
        .assert()
        .failure();
    let stderr = String::from_utf8_lossy(&assert.get_output().stderr);

    assert!(
        stderr.contains("missing required source secret 'API_TOKEN'"),
        "expected missing secret error, got: {stderr}"
    );
    assert!(
        server.import_source_requests().is_empty(),
        "request should not reach the server"
    );

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn source_add_import_zero_input_succeeds_without_tty() {
    let server = MockServer::start().await;
    let dir = tempdir().expect("manifest dir");
    let manifest_path = write_manifest(dir.path(), "local_messages.yaml", zero_input_manifest());

    server
        .cmd()
        .args([
            "source",
            "add",
            "--file",
            manifest_path.to_str().expect("utf-8 manifest path"),
        ])
        .assert()
        .success();

    let requests = server.import_source_requests();
    assert_eq!(requests.len(), 1, "expected one import_source call");
    assert!(requests[0].variables.is_empty(), "expected no variables");
    assert!(requests[0].secrets.is_empty(), "expected no secrets");

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn source_add_import_uses_flags_and_env_without_tty() {
    let server = MockServer::start().await;
    let dir = tempdir().expect("manifest dir");
    let manifest_path = write_manifest(
        dir.path(),
        "secured_messages.yaml",
        manifest_with_bindings(),
    );

    server
        .cmd()
        .args([
            "source",
            "add",
            "--file",
            manifest_path.to_str().expect("utf-8 manifest path"),
            "--input",
            "API_BASE=https://flag.example.com",
        ])
        .env("API_TOKEN", "env-secret")
        .assert()
        .success();

    let requests = server.import_source_requests();
    assert_eq!(requests.len(), 1, "expected one import_source call");
    assert_eq!(requests[0].variables.len(), 1, "expected one variable");
    assert_eq!(requests[0].variables[0].key, "API_BASE");
    assert_eq!(requests[0].variables[0].value, "https://flag.example.com");
    assert_eq!(requests[0].secrets.len(), 1, "expected one secret");
    assert_eq!(requests[0].secrets[0].key, "API_TOKEN");
    assert_eq!(requests[0].secrets[0].value, "env-secret");

    server.shutdown().await;
}
