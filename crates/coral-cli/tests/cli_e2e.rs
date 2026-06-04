#![allow(
    clippy::indexing_slicing,
    clippy::string_slice,
    missing_docs,
    unused_crate_dependencies,
    reason = "Integration test: assertion-style indexing is idiomatic; only a subset of dependencies are used."
)]
#![cfg(feature = "cli-test-server")]

mod common;
mod harness;

use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
#[cfg(feature = "embedded-ui")]
use assert_cmd::Command;
use coral_api::v1::{
    DiscoverSourcesResponse, ExecuteSqlResponse, ListSourcesResponse, Source,
    SourceCredentialStorage, SourceInfo, SourceOrigin,
};
use tempfile::tempdir;
use tonic::Code;

use common::{assert_contains, assert_contains_all, assert_not_contains, stderr, stdout};
use harness::{MockServer, MockServerConfig, encode_arrow_ipc_stream};

#[cfg(feature = "embedded-ui")]
#[test]
fn ui_help_does_not_require_app_bootstrap() {
    let assert = Command::cargo_bin("coral")
        .expect("cargo bin")
        .args(["ui", "--help"])
        .assert()
        .success();

    let stdout = stdout(&assert);
    assert_contains_all(
        &stdout,
        &["embedded Coral UI", "--port <PORT>", "--no-open"],
    );
}

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

fn success_stdout<const N: usize>(server: &MockServer, args: [&str; N]) -> String {
    stdout(&server.cmd().args(args).assert().success())
}

fn failure_stderr<const N: usize>(server: &MockServer, args: [&str; N]) -> String {
    stderr(&server.cmd().args(args).assert().failure())
}

async fn configured_failure_stderr(config: MockServerConfig, args: &[&str]) -> String {
    let server = MockServer::start_with_config(config).await;
    let stderr = stderr(&server.cmd().args(args).assert().failure());
    server.shutdown().await;
    stderr
}

fn only_request<T>(requests: Vec<T>, label: &str) -> T {
    assert_eq!(requests.len(), 1, "expected one {label} call");
    requests.into_iter().next().expect("single request")
}

enum CliErrorExpectation {
    Contains(&'static str),
    RequiredArgument,
}

#[tokio::test(flavor = "multi_thread")]
async fn sql_command_renders_table_output() {
    let server = MockServer::start().await;

    let stdout = success_stdout(&server, ["sql", "select 1 as value"]);
    assert_contains(&stdout, "value");
    assert_contains(&stdout, "1");

    let request = only_request(server.execute_sql_requests(), "execute_sql");
    assert_eq!(request.sql, "select 1 as value");
    assert_default_workspace(request.workspace.as_ref());

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn source_list_renders_configured_sources() {
    let server = MockServer::start().await;

    let stdout = success_stdout(&server, ["source", "list"]);
    assert_eq!(
        nonempty_lines(&stdout),
        vec![
            "Source  Version  Origin    Secrets",
            "------  -------  --------  ----------------",
            "github  1.0.0    bundled   file (plaintext)",
            "jira    2.0.0    imported  file (plaintext)",
        ],
        "expected configured source list"
    );

    let request = only_request(server.list_sources_requests(), "list_sources");
    assert_default_workspace(request.workspace.as_ref());

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn source_list_renders_dash_for_missing_authored_version() {
    let server = MockServer::start_with_config(MockServerConfig::default().with_list_sources(
        ListSourcesResponse {
            sources: vec![Source {
                workspace: None,
                name: "versionless".to_string(),
                version: String::new(),
                secrets: Vec::new(),
                variables: Vec::new(),
                origin: SourceOrigin::Imported as i32,
                credential_storage: SourceCredentialStorage::File as i32,
            }],
        },
    ))
    .await;

    let assert = server.cmd().args(["source", "list"]).assert().success();

    let stdout = String::from_utf8_lossy(&assert.get_output().stdout);
    assert_eq!(
        nonempty_lines(&stdout),
        vec![
            "Source       Version  Origin    Secrets",
            "-----------  -------  --------  ----------------",
            "versionless  -        imported  file (plaintext)",
        ],
        "expected versionless source list"
    );

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn sql_command_renders_json_output() {
    let server = MockServer::start().await;

    let stdout = success_stdout(&server, ["sql", "--format", "json", "select 1 as value"]);
    assert_eq!(stdout.trim(), "[{\"value\":1}]", "expected JSON rows");

    let request = only_request(server.execute_sql_requests(), "execute_sql");
    assert_eq!(request.sql, "select 1 as value");
    assert_default_workspace(request.workspace.as_ref());

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn source_discover_renders_available_sources() {
    let server = MockServer::start().await;

    let stdout = success_stdout(&server, ["source", "discover"]);
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

    let request = only_request(server.discover_sources_requests(), "discover_sources");
    assert_default_workspace(request.workspace.as_ref());

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn source_discover_renders_empty_state() {
    let server = MockServer::start_with_config(
        MockServerConfig::default().with_discover_sources(DiscoverSourcesResponse::default()),
    )
    .await;

    let stdout = success_stdout(&server, ["source", "discover"]);
    assert_eq!(
        stdout.trim(),
        "No bundled sources available.",
        "expected empty state"
    );

    let request = only_request(server.discover_sources_requests(), "discover_sources");
    assert_default_workspace(request.workspace.as_ref());

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn source_info_renders_metadata_variants() {
    let server = MockServer::start().await;

    let stdout = success_stdout(&server, ["source", "info", "github"]);
    assert_contains_all(
        &stdout,
        &[
            "github",
            "installed",
            "1.0.0",
            "GitHub data",
            "GITHUB_TOKEN",
            "secret",
            "required",
        ],
    );
    assert_not_contains(&stdout, "github.com/settings/tokens");

    let stdout = success_stdout(&server, ["source", "info", "github", "--verbose"]);
    assert_contains(&stdout, "github.com/settings/tokens");

    let stdout = success_stdout(&server, ["source", "info", "slack"]);
    assert_contains_all(&stdout, &["not installed", "2.1.0", "Slack data"]);

    let stdout = success_stdout(&server, ["source", "info", "jira"]);
    assert_contains_all(&stdout, &["jira", "installed", "imported", "2.0.0"]);

    let requests = server.get_source_info_requests();
    assert_eq!(
        requests
            .iter()
            .map(|request| request.name.as_str())
            .collect::<Vec<_>>(),
        vec!["github", "github", "slack", "jira"]
    );
    for request in requests {
        assert_default_workspace(request.workspace.as_ref());
    }

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn source_info_omits_missing_authored_version() {
    let server = MockServer::start().await;

    let assert = server
        .cmd()
        .args(["source", "info", "versionless"])
        .assert()
        .success();

    let stdout = String::from_utf8_lossy(&assert.get_output().stdout);
    assert!(
        stdout.contains("versionless"),
        "expected source name: {stdout}"
    );
    assert!(
        !stdout.contains("Version:"),
        "expected version line to be omitted: {stdout}"
    );

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn source_info_errors_for_unknown_source() {
    let server = MockServer::start().await;

    let stderr = failure_stderr(&server, ["source", "info", "nope"]);
    assert_contains(&stderr, "unknown source 'nope'");

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn source_list_renders_empty_state() {
    let server = MockServer::start_with_config(
        MockServerConfig::default().with_list_sources(ListSourcesResponse::default()),
    )
    .await;

    let stdout = success_stdout(&server, ["source", "list"]);
    assert_eq!(
        stdout.trim(),
        "No sources configured.",
        "expected empty state"
    );

    let request = only_request(server.list_sources_requests(), "list_sources");
    assert_default_workspace(request.workspace.as_ref());

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn source_test_renders_validation_summary() {
    let server = MockServer::start().await;

    let stdout = success_stdout(&server, ["source", "test", "github"]);
    assert_contains_all(
        &stdout,
        &[
            "github connected successfully",
            "github (2 tables)",
            "issues",
            "pull_requests",
        ],
    );

    let request = only_request(server.validate_source_requests(), "validate_source");
    assert_eq!(request.name, "github");
    assert_default_workspace(request.workspace.as_ref());

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn source_remove_reports_removed_source() {
    let server = MockServer::start().await;

    let stdout = success_stdout(&server, ["source", "remove", "github"]);
    assert_eq!(
        stdout.trim(),
        "Removed source github",
        "expected remove confirmation"
    );

    let request = only_request(server.delete_source_requests(), "delete_source");
    assert_eq!(request.name, "github");
    assert_default_workspace(request.workspace.as_ref());

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn sql_command_surfaces_server_errors() {
    let server = MockServer::start_with_config(
        MockServerConfig::default().with_execute_sql_error(Code::Internal, "mock SQL failure"),
    )
    .await;

    let stderr = failure_stderr(&server, ["sql", "select 1 as value"]);
    assert_contains(&stderr, "mock SQL failure");

    let request = only_request(server.execute_sql_requests(), "execute_sql");
    assert_eq!(request.sql, "select 1 as value");

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn source_test_surfaces_validation_errors() {
    let server = MockServer::start_with_config(
        MockServerConfig::default()
            .with_validate_source_error(Code::FailedPrecondition, "mock validate failure"),
    )
    .await;

    let stderr = failure_stderr(&server, ["source", "test", "github"]);
    assert_contains(&stderr, "mock validate failure");

    let request = only_request(server.validate_source_requests(), "validate_source");
    assert_eq!(request.name, "github");

    server.shutdown().await;
}

fn sql_response(schema: &Schema, batches: &[RecordBatch], row_count: i64) -> ExecuteSqlResponse {
    ExecuteSqlResponse {
        arrow_ipc_stream: encode_arrow_ipc_stream(schema, batches).expect("encode arrow ipc"),
        row_count,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn source_commands_reject_invalid_cli_arguments() {
    let server = MockServer::start().await;

    for (case, args, expectation) in [
        (
            "source add requires name or file",
            &["source", "add"][..],
            CliErrorExpectation::RequiredArgument,
        ),
        (
            "source add rejects name and file together",
            &["source", "add", "github", "--file", "manifest.yaml"][..],
            CliErrorExpectation::Contains("cannot be used with"),
        ),
        (
            "source test rejects invalid name",
            &["source", "test", "a/b"][..],
            CliErrorExpectation::Contains("must not contain"),
        ),
        (
            "source remove rejects invalid name",
            &["source", "remove", "a/b"][..],
            CliErrorExpectation::Contains("must not contain"),
        ),
        (
            "source add interactive requires tty",
            &["source", "add", "--interactive", "github"][..],
            CliErrorExpectation::Contains("requires a TTY"),
        ),
    ] {
        let stderr = stderr(&server.cmd().args(args).assert().failure());
        match expectation {
            CliErrorExpectation::Contains(expected) => assert_contains(&stderr, expected),
            CliErrorExpectation::RequiredArgument => assert!(
                stderr.contains("required") || stderr.contains("must be provided"),
                "{case}: expected clap error about required arguments: {stderr}"
            ),
        }
    }

    server.shutdown().await;
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

    let request = only_request(server.execute_sql_requests(), "execute_sql");
    assert_eq!(request.sql, "select id, name from users");
    assert_default_workspace(request.workspace.as_ref());

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
    assert_eq!(
        rows[1].get("name"),
        Some(&serde_json::Value::Null),
        "null name should be explicit in row 2: {:?}",
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

#[tokio::test(flavor = "multi_thread")]
async fn source_add_file_resolves_v4_relative_descriptor_from_manifest_dir() {
    let server = MockServer::start().await;
    let source_dir = tempdir().expect("source dir");
    let openapi_file = source_dir.path().join("openapi.yaml");
    std::fs::write(
        &openapi_file,
        r"
openapi: 3.0.3
paths: {}
",
    )
    .expect("write descriptor");
    let manifest_file = source_dir.path().join("manifest.yaml");
    std::fs::write(
        &manifest_file,
        r"
name: github
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    file: openapi.yaml
",
    )
    .expect("write manifest");

    server
        .cmd()
        .args([
            "source",
            "add",
            "--file",
            manifest_file.to_str().expect("manifest path utf8"),
        ])
        .assert()
        .success();

    let requests = server.import_source_requests();
    assert_eq!(requests.len(), 1, "expected one import_source call");
    let manifest_yaml = &requests[0].manifest_yaml;
    let canonical = openapi_file.canonicalize().expect("canonical descriptor");
    let canonical = canonical.to_string_lossy();
    assert!(
        manifest_yaml.contains(canonical.as_ref()),
        "expected import manifest to contain canonical descriptor path '{canonical}', got: {manifest_yaml}"
    );
    assert!(
        !manifest_yaml.contains("file: openapi.yaml"),
        "expected relative descriptor to be replaced before import: {manifest_yaml}"
    );

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn source_add_reports_missing_env_vars_without_interactive() {
    let server = MockServer::start().await;

    let assert = server
        .cmd()
        .args(["source", "add", "github"])
        .env_remove("GITHUB_TOKEN")
        .assert()
        .failure();

    let stderr = stderr(&assert);
    assert_contains_all(
        &stderr,
        &[
            "missing required environment variable",
            "GITHUB_TOKEN",
            "coral source add --interactive github",
        ],
    );

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn source_test_suggests_add_for_uninstalled_bundled_source() {
    let stderr = configured_failure_stderr(
        MockServerConfig::default()
            .with_validate_source_not_found("default:demo_bundled")
            .with_discover_sources(DiscoverSourcesResponse {
                sources: vec![SourceInfo {
                    name: "demo_bundled".to_string(),
                    description: "A demo bundled source for testing".to_string(),
                    version: "1.0.0".to_string(),
                    inputs: Vec::new(),
                    installed: false,
                    origin: SourceOrigin::Bundled as i32,
                    credential_storage: SourceCredentialStorage::Unspecified as i32,
                }],
            }),
        &["source", "test", "demo_bundled"],
    )
    .await;

    assert_contains_all(
        &stderr,
        &[
            "source 'demo_bundled' is not installed",
            "coral source add demo_bundled",
        ],
    );
    assert_not_contains(&stderr, "default:demo_bundled");
}

#[tokio::test(flavor = "multi_thread")]
async fn source_commands_normalize_error_for_unknown_source() {
    for (config, args, expected, raw_name) in [
        (
            MockServerConfig::default()
                .with_validate_source_not_found("default:totally_unknown")
                .with_discover_sources(DiscoverSourcesResponse::default()),
            ["source", "test", "totally_unknown"],
            [
                "source 'totally_unknown' was not found",
                "coral source discover",
            ],
            "default:totally_unknown",
        ),
        (
            MockServerConfig::default().with_delete_source_not_found("default:unknown_source"),
            ["source", "remove", "unknown_source"],
            ["source 'unknown_source' was not found", "coral source list"],
            "default:unknown_source",
        ),
    ] {
        let stderr = configured_failure_stderr(config, &args).await;
        assert_contains_all(&stderr, &expected);
        assert_not_contains(&stderr, raw_name);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn source_remove_preserves_unrelated_not_found_errors() {
    // Server can return Code::NotFound for reasons other than a missing
    // catalog entry (e.g. a missing manifest file mapped from
    // io::ErrorKind::NotFound). The CLI must not rewrite those into the
    // friendly "source was not found" message.
    let raw_message = "manifest file missing: No such file or directory (os error 2)";
    let stderr = configured_failure_stderr(
        MockServerConfig::default().with_delete_source_error(Code::NotFound, raw_message),
        &["source", "remove", "broken_source"],
    )
    .await;

    assert_contains(&stderr, raw_message);
    assert_not_contains(&stderr, "source 'broken_source' was not found");
}
