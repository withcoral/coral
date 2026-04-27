#![allow(
    missing_docs,
    unused_crate_dependencies,
    reason = "Integration tests only use a subset of the package dependency graph."
)]
#![cfg(feature = "cli-test-server")]

mod harness;

use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use coral_api::v1::{
    AvailableSource, DiscoverSourcesResponse, ExecuteSqlResponse, ListSourcesResponse, SourceOrigin,
};
use tonic::Code;

use harness::{MockServer, MockServerConfig, encode_arrow_ipc_stream};

fn nonempty_lines(output: &str) -> Vec<&str> {
    output
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect()
}

fn assert_default_workspace(workspace: Option<&coral_api::v1::Workspace>) {
    assert_eq!(
        workspace.map(|w| w.name.as_str()),
        Some("default"),
        "expected default workspace, got {workspace:?}"
    );
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

    let requests = server.execute_sql_requests();
    assert_eq!(requests.len(), 1, "expected one execute_sql call");
    assert_eq!(requests[0].sql, "select 1 as value");
    assert_default_workspace(requests[0].workspace.as_ref());

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn source_list_renders_configured_sources() {
    let server = MockServer::start().await;

    let assert = server.cmd().args(["source", "list"]).assert().success();

    let stdout = String::from_utf8_lossy(&assert.get_output().stdout);
    assert_eq!(
        nonempty_lines(&stdout),
        vec![
            "Source  Version  Origin",
            "------  -------  --------",
            "github  1.0.0    bundled",
            "jira    2.0.0    imported",
        ],
        "expected configured source list"
    );

    let requests = server.list_sources_requests();
    assert_eq!(requests.len(), 1, "expected one list_sources call");
    assert_default_workspace(requests[0].workspace.as_ref());

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

    let requests = server.execute_sql_requests();
    assert_eq!(requests.len(), 1, "expected one execute_sql call");
    assert_eq!(requests[0].sql, "select 1 as value");
    assert_default_workspace(requests[0].workspace.as_ref());

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn source_discover_renders_available_sources() {
    let server = MockServer::start().await;

    let assert = server.cmd().args(["source", "discover"]).assert().success();

    let stdout = String::from_utf8_lossy(&assert.get_output().stdout);
    assert_eq!(
        nonempty_lines(&stdout),
        vec![
            "Source  Version  Status",
            "------  -------  ---------",
            "github  1.0.0    installed",
            "slack   2.1.0    available",
        ],
        "expected discover source list"
    );

    let requests = server.discover_sources_requests();
    assert_eq!(requests.len(), 1, "expected one discover_sources call");
    assert_default_workspace(requests[0].workspace.as_ref());

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

    let requests = server.discover_sources_requests();
    assert_eq!(requests.len(), 1, "expected one discover_sources call");
    assert_default_workspace(requests[0].workspace.as_ref());

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

    let requests = server.list_sources_requests();
    assert_eq!(requests.len(), 1, "expected one list_sources call");
    assert_default_workspace(requests[0].workspace.as_ref());

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

    let requests = server.validate_source_requests();
    assert_eq!(requests.len(), 1, "expected one validate_source call");
    assert_eq!(requests[0].name, "github");
    assert_default_workspace(requests[0].workspace.as_ref());

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

    let requests = server.delete_source_requests();
    assert_eq!(requests.len(), 1, "expected one delete_source call");
    assert_eq!(requests[0].name, "github");
    assert_default_workspace(requests[0].workspace.as_ref());

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

    let requests = server.execute_sql_requests();
    assert_eq!(requests.len(), 1, "expected one execute_sql call");
    assert_eq!(requests[0].sql, "select 1 as value");

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

    let requests = server.validate_source_requests();
    assert_eq!(requests.len(), 1, "expected one validate_source call");
    assert_eq!(requests[0].name, "github");

    server.shutdown().await;
}

// ---------------------------------------------------------------------------
// SQL output shape
// ---------------------------------------------------------------------------

fn sql_response(schema: &Schema, batches: &[RecordBatch], row_count: i64) -> ExecuteSqlResponse {
    ExecuteSqlResponse {
        arrow_ipc_stream: encode_arrow_ipc_stream(schema, batches).expect("encode arrow ipc"),
        row_count,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn sql_table_output_renders_multiple_columns_and_rows() {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]);
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec![Some("alice"), Some("bob"), None])),
        ],
    )
    .expect("batch");

    let server = MockServer::start_with_config(
        MockServerConfig::default().with_execute_sql(sql_response(&schema, &[batch], 3)),
    )
    .await;

    let assert = server
        .cmd()
        .args(["sql", "select id, name from users"])
        .assert()
        .success();

    let stdout = String::from_utf8_lossy(&assert.get_output().stdout);
    let lines = nonempty_lines(&stdout);

    // Arrow pretty table: border, header, border, data rows, border.
    assert!(lines[0].starts_with('+'), "top border: {}", lines[0]);
    assert!(
        lines[1].contains("id") && lines[1].contains("name"),
        "header: {}",
        lines[1]
    );
    assert!(lines[2].starts_with('+'), "separator: {}", lines[2]);
    assert!(
        lines[3].contains('1') && lines[3].contains("alice"),
        "row 1: {}",
        lines[3]
    );
    assert!(
        lines[4].contains('2') && lines[4].contains("bob"),
        "row 2: {}",
        lines[4]
    );
    assert!(lines[5].contains('3'), "row 3: {}", lines[5]);
    assert!(lines[6].starts_with('+'), "bottom border: {}", lines[6]);
    assert_eq!(lines.len(), 7, "expected 7 lines, got: {stdout}");

    let requests = server.execute_sql_requests();
    assert_eq!(requests.len(), 1, "expected one execute_sql call");
    assert_eq!(requests[0].sql, "select id, name from users");
    assert_default_workspace(requests[0].workspace.as_ref());

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn sql_json_output_renders_multiple_rows() {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]);
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec![Some("alice"), None])),
        ],
    )
    .expect("batch");

    let server = MockServer::start_with_config(
        MockServerConfig::default().with_execute_sql(sql_response(&schema, &[batch], 2)),
    )
    .await;

    let assert = server
        .cmd()
        .args(["sql", "--format", "json", "select id, name from users"])
        .assert()
        .success();

    let stdout = String::from_utf8_lossy(&assert.get_output().stdout);
    let rows: Vec<serde_json::Map<String, serde_json::Value>> =
        serde_json::from_str(stdout.trim()).expect("sql --format json should emit a JSON array");

    assert_eq!(rows.len(), 2, "expected two rows: {rows:?}");
    assert_eq!(rows[0].get("id"), Some(&serde_json::json!(1)));
    assert_eq!(rows[0].get("name"), Some(&serde_json::json!("alice")));
    assert_eq!(rows[1].get("id"), Some(&serde_json::json!(2)));
    // Arrow JSON omits null fields rather than emitting "name":null.
    assert_eq!(
        rows[1].len(),
        1,
        "null name should be omitted from row 2: {:?}",
        rows[1]
    );

    let requests = server.execute_sql_requests();
    assert_eq!(requests.len(), 1, "expected one execute_sql call");
    assert_eq!(requests[0].sql, "select id, name from users");
    assert_default_workspace(requests[0].workspace.as_ref());

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn sql_table_output_renders_empty_result() {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]);
    let batch = RecordBatch::new_empty(Arc::new(schema.clone()));

    let server = MockServer::start_with_config(
        MockServerConfig::default().with_execute_sql(sql_response(&schema, &[batch], 0)),
    )
    .await;

    let assert = server
        .cmd()
        .args(["sql", "select id, name from empty_table"])
        .assert()
        .success();

    let stdout = String::from_utf8_lossy(&assert.get_output().stdout);
    let lines = nonempty_lines(&stdout);

    // Empty result: border, header, border, border (no data rows).
    assert!(lines[0].starts_with('+'), "top border: {}", lines[0]);
    assert!(
        lines[1].contains("id") && lines[1].contains("name"),
        "header: {}",
        lines[1]
    );
    assert!(lines[2].starts_with('+'), "separator: {}", lines[2]);
    assert!(lines[3].starts_with('+'), "bottom border: {}", lines[3]);
    assert_eq!(
        lines.len(),
        4,
        "expected 4 lines (no data rows), got: {stdout}"
    );

    let requests = server.execute_sql_requests();
    assert_eq!(requests.len(), 1, "expected one execute_sql call");
    assert_eq!(requests[0].sql, "select id, name from empty_table");
    assert_default_workspace(requests[0].workspace.as_ref());

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn sql_json_output_renders_empty_result() {
    let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
    let batch = RecordBatch::new_empty(Arc::new(schema.clone()));

    let server = MockServer::start_with_config(
        MockServerConfig::default().with_execute_sql(sql_response(&schema, &[batch], 0)),
    )
    .await;

    let assert = server
        .cmd()
        .args(["sql", "--format", "json", "select id from empty_table"])
        .assert()
        .success();

    let stdout = String::from_utf8_lossy(&assert.get_output().stdout);
    assert_eq!(stdout.trim(), "[]", "expected empty JSON array");

    let requests = server.execute_sql_requests();
    assert_eq!(requests.len(), 1, "expected one execute_sql call");
    assert_eq!(requests[0].sql, "select id from empty_table");
    assert_default_workspace(requests[0].workspace.as_ref());

    server.shutdown().await;
}

// ---------------------------------------------------------------------------
// Clap argument validation
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn source_add_requires_name_or_file() {
    let server = MockServer::start().await;

    let assert = server.cmd().args(["source", "add"]).assert().failure();

    let stderr = String::from_utf8_lossy(&assert.get_output().stderr);
    assert!(
        stderr.contains("required") || stderr.contains("must be provided"),
        "expected clap error about required arguments: {stderr}"
    );

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn source_add_rejects_name_and_file_together() {
    let server = MockServer::start().await;

    let assert = server
        .cmd()
        .args(["source", "add", "github", "--file", "manifest.yaml"])
        .assert()
        .failure();

    let stderr = String::from_utf8_lossy(&assert.get_output().stderr);
    assert!(
        stderr.contains("cannot be used with"),
        "expected clap conflict error: {stderr}"
    );

    server.shutdown().await;
}

// ---------------------------------------------------------------------------
// Name validation
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn source_test_rejects_invalid_name() {
    let server = MockServer::start().await;

    let assert = server
        .cmd()
        .args(["source", "test", "a/b"])
        .assert()
        .failure();

    let stderr = String::from_utf8_lossy(&assert.get_output().stderr);
    assert!(
        stderr.contains("must not contain"),
        "expected name validation error: {stderr}"
    );

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn source_remove_rejects_invalid_name() {
    let server = MockServer::start().await;

    let assert = server
        .cmd()
        .args(["source", "remove", "a/b"])
        .assert()
        .failure();

    let stderr = String::from_utf8_lossy(&assert.get_output().stderr);
    assert!(
        stderr.contains("must not contain"),
        "expected name validation error: {stderr}"
    );

    server.shutdown().await;
}

// ---------------------------------------------------------------------------
// Interactive-mode gating
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn source_add_reports_missing_env_vars_without_interactive() {
    let server = MockServer::start().await;

    let assert = server
        .cmd()
        .args(["source", "add", "github"])
        .env_remove("GITHUB_TOKEN")
        .assert()
        .failure();

    let stderr = String::from_utf8_lossy(&assert.get_output().stderr);
    assert!(
        stderr.contains("missing required environment variable"),
        "expected missing env var error: {stderr}"
    );
    assert!(
        stderr.contains("GITHUB_TOKEN"),
        "expected missing env var to name GITHUB_TOKEN: {stderr}"
    );

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn source_add_interactive_requires_tty() {
    let server = MockServer::start().await;

    let assert = server
        .cmd()
        .args(["source", "add", "--interactive", "github"])
        .assert()
        .failure();

    let stderr = String::from_utf8_lossy(&assert.get_output().stderr);
    assert!(
        stderr.contains("requires a TTY"),
        "expected TTY requirement error: {stderr}"
    );

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn source_test_suggests_add_for_uninstalled_bundled_source() {
    let server = MockServer::start_with_config(
        MockServerConfig::default()
            .with_validate_source_error(Code::NotFound, "source 'default:demo_bundled' not found")
            .with_discover_sources(DiscoverSourcesResponse {
                sources: vec![AvailableSource {
                    name: "demo_bundled".to_string(),
                    description: "A demo bundled source for testing".to_string(),
                    version: "1.0.0".to_string(),
                    inputs: Vec::new(),
                    installed: false,
                    origin: SourceOrigin::Bundled as i32,
                }],
            }),
    )
    .await;

    let assert = server
        .cmd()
        .args(["source", "test", "demo_bundled"])
        .assert()
        .failure();

    let stderr = String::from_utf8_lossy(&assert.get_output().stderr);
    assert!(
        stderr.contains("source 'demo_bundled' is not installed"),
        "expected not-installed error in stderr: {stderr}"
    );
    assert!(
        stderr.contains("coral source add demo_bundled"),
        "expected add suggestion in stderr: {stderr}"
    );
    assert!(
        stderr.contains("coral source test demo_bundled"),
        "expected retry suggestion in stderr: {stderr}"
    );
    assert!(
        !stderr.contains("default:demo_bundled"),
        "should not expose workspace-qualified source name: {stderr}"
    );

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn source_test_normalizes_error_for_unknown_source() {
    let server = MockServer::start_with_config(
        MockServerConfig::default()
            .with_validate_source_error(
                Code::NotFound,
                "source 'default:totally_unknown' not found",
            )
            .with_discover_sources(DiscoverSourcesResponse {
                sources: Vec::new(),
            }),
    )
    .await;

    let assert = server
        .cmd()
        .args(["source", "test", "totally_unknown"])
        .assert()
        .failure();

    let stderr = String::from_utf8_lossy(&assert.get_output().stderr);
    assert!(
        stderr.contains("source 'totally_unknown' was not found"),
        "expected normalized not-found error in stderr: {stderr}"
    );
    assert!(
        stderr.contains("coral source list"),
        "expected list suggestion in stderr: {stderr}"
    );
    assert!(
        stderr.contains("coral source discover"),
        "expected discover suggestion in stderr: {stderr}"
    );
    assert!(
        !stderr.contains("default:totally_unknown"),
        "should not expose workspace-qualified source name: {stderr}"
    );
    assert!(
        !stderr.contains("coral source add"),
        "should not contain add suggestion for unknown source: {stderr}"
    );

    server.shutdown().await;
}
