#![allow(
    missing_docs,
    unused_crate_dependencies,
    reason = "Integration tests only use a subset of the package dependency graph."
)]
#![cfg(feature = "cli-test-server")]

mod harness;

use coral_api::v1::{DiscoverSourcesResponse, ListSourcesResponse};
use tonic::Code;

use harness::{MockServer, MockServerConfig};

fn nonempty_lines(output: &str) -> Vec<&str> {
    output
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn sql_command_renders_table_output() {
    let server = MockServer::start().await;

    let assert = server
        .cmd()
        .args(["sql", "select 1 as value"])
        .assert()
        .success();

    let stdout = String::from_utf8_lossy(&assert.get_output().stdout);
    assert!(stdout.contains("value"), "expected column header: {stdout}");
    assert!(stdout.contains('1'), "expected row value: {stdout}");

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn source_list_renders_configured_sources() {
    let server = MockServer::start().await;

    let assert = server.cmd().args(["source", "list"]).assert().success();

    let stdout = String::from_utf8_lossy(&assert.get_output().stdout);
    assert_eq!(
        nonempty_lines(&stdout),
        vec!["github\t1.0.0\tbundled", "jira\t2.0.0\timported"],
        "expected configured source list"
    );

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn sql_command_renders_json_output() {
    let server = MockServer::start().await;

    let assert = server
        .cmd()
        .args(["sql", "--format", "json", "select 1 as value"])
        .assert()
        .success();

    let stdout = String::from_utf8_lossy(&assert.get_output().stdout);
    assert_eq!(stdout.trim(), "[{\"value\":1}]", "expected JSON rows");

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn source_discover_renders_available_sources() {
    let server = MockServer::start().await;

    let assert = server.cmd().args(["source", "discover"]).assert().success();

    let stdout = String::from_utf8_lossy(&assert.get_output().stdout);
    assert_eq!(
        nonempty_lines(&stdout),
        vec!["github\t1.0.0\tinstalled", "slack\t2.1.0\tavailable"],
        "expected discover source list"
    );

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn source_discover_renders_empty_state() {
    let server = MockServer::start_with_config(MockServerConfig::default().with_discover_sources(
        DiscoverSourcesResponse {
            sources: Vec::new(),
        },
    ))
    .await;

    let assert = server.cmd().args(["source", "discover"]).assert().success();

    let stdout = String::from_utf8_lossy(&assert.get_output().stdout);
    assert_eq!(
        stdout.trim(),
        "No bundled sources available.",
        "expected empty state"
    );

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn source_list_renders_empty_state() {
    let server = MockServer::start_with_config(MockServerConfig::default().with_list_sources(
        ListSourcesResponse {
            sources: Vec::new(),
        },
    ))
    .await;

    let assert = server.cmd().args(["source", "list"]).assert().success();

    let stdout = String::from_utf8_lossy(&assert.get_output().stdout);
    assert_eq!(
        stdout.trim(),
        "No sources configured.",
        "expected empty state"
    );

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn source_test_renders_validation_summary() {
    let server = MockServer::start().await;

    let assert = server
        .cmd()
        .args(["source", "test", "github"])
        .assert()
        .success();

    let stdout = String::from_utf8_lossy(&assert.get_output().stdout);
    assert!(
        stdout.contains("github connected successfully"),
        "expected success summary: {stdout}"
    );
    assert!(
        stdout.contains("github (2 tables)"),
        "expected schema summary: {stdout}"
    );
    assert!(stdout.contains("issues"), "expected issues table: {stdout}");
    assert!(
        stdout.contains("pull_requests"),
        "expected pull_requests table: {stdout}"
    );

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn source_remove_reports_removed_source() {
    let server = MockServer::start().await;

    let assert = server
        .cmd()
        .args(["source", "remove", "github"])
        .assert()
        .success();

    let stdout = String::from_utf8_lossy(&assert.get_output().stdout);
    assert_eq!(
        stdout.trim(),
        "Removed source github",
        "expected remove confirmation"
    );

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn sql_command_surfaces_server_errors() {
    let server = MockServer::start_with_config(
        MockServerConfig::default().with_execute_sql_error(Code::Internal, "mock SQL failure"),
    )
    .await;

    let assert = server
        .cmd()
        .args(["sql", "select 1 as value"])
        .assert()
        .failure();

    let stderr = String::from_utf8_lossy(&assert.get_output().stderr);
    assert!(
        stderr.contains("mock SQL failure"),
        "expected server error in stderr: {stderr}"
    );

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn source_test_surfaces_validation_errors() {
    let server = MockServer::start_with_config(
        MockServerConfig::default()
            .with_validate_source_error(Code::FailedPrecondition, "mock validate failure"),
    )
    .await;

    let assert = server
        .cmd()
        .args(["source", "test", "github"])
        .assert()
        .failure();

    let stderr = String::from_utf8_lossy(&assert.get_output().stderr);
    assert!(
        stderr.contains("mock validate failure"),
        "expected validation error in stderr: {stderr}"
    );

    server.shutdown().await;
}
