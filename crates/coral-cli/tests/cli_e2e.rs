#![allow(
    missing_docs,
    unused_crate_dependencies,
    reason = "Integration tests only use a subset of the package dependency graph."
)]
#![cfg(feature = "cli-test-server")]

mod harness;

use harness::MockServer;

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
    assert!(
        stdout.contains("github\t1.0.0\tbundled"),
        "expected github source: {stdout}"
    );
    assert!(
        stdout.contains("jira\t2.0.0\timported"),
        "expected jira source: {stdout}"
    );

    server.shutdown().await;
}
