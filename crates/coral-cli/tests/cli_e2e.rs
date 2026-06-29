#![allow(
    clippy::indexing_slicing,
    clippy::string_slice,
    missing_docs,
    unused_crate_dependencies,
    reason = "Integration test: assertion-style indexing is idiomatic; only a subset of dependencies are used."
)]
#![cfg(feature = "cli-test-server")]

mod harness;

use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
#[cfg(feature = "embedded-ui")]
use assert_cmd::Command;
use coral_api::v1::{
    DiscoverSourcesResponse, ExecuteSqlResponse, ListSourcesResponse,
    ResolveBundledSourceHostsResponse, Source, SourceCredentialStorage, SourceInfo,
    SourceInputSpec, SourceOrigin, SourceVariable, SourceVariableInput,
    source_input_spec::Input as ProtoSourceInput,
};
use tempfile::tempdir;
use tonic::Code;

use harness::{MockServer, MockServerConfig, encode_arrow_ipc_stream, script_command, sh_quote};

#[cfg(feature = "embedded-ui")]
#[test]
fn ui_help_does_not_require_app_bootstrap() {
    let assert = Command::cargo_bin("coral")
        .expect("cargo bin")
        .args(["ui", "--help"])
        .assert()
        .success();

    let stdout = String::from_utf8_lossy(&assert.get_output().stdout);
    assert!(
        stdout.contains("embedded Coral UI"),
        "expected ui help text: {stdout}"
    );
    assert!(
        stdout.contains("--port <PORT>"),
        "expected ui port option: {stdout}"
    );
    assert!(
        stdout.contains("--no-open"),
        "expected ui no-open option: {stdout}"
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

const GITLAB_API_BASE_KEY: &str = "GITLAB_API_BASE";
const GITLAB_API_BASE_VALUE: &str = "https://gitlab.internal/api/v4";

fn gitlab_host_confirmation_config() -> MockServerConfig {
    MockServerConfig::default()
        .with_discover_sources(DiscoverSourcesResponse {
            sources: vec![SourceInfo {
                name: "gitlab".to_string(),
                description: "GitLab data".to_string(),
                version: "1.0.0".to_string(),
                inputs: vec![SourceInputSpec {
                    key: GITLAB_API_BASE_KEY.to_string(),
                    required: false,
                    hint: "GitLab API base URL".to_string(),
                    input: Some(ProtoSourceInput::Variable(SourceVariableInput {
                        default_value: "https://gitlab.com/api/v4".to_string(),
                    })),
                }],
                installed: false,
                origin: SourceOrigin::Bundled as i32,
                credential_storage: SourceCredentialStorage::Unspecified as i32,
            }],
        })
        .with_resolve_bundled_source_hosts(ResolveBundledSourceHostsResponse {
            hosts: vec!["gitlab.internal".to_string()],
            unresolved_hosts: Vec::new(),
        })
}

fn assert_single_source_variable(variables: &[SourceVariable], key: &str, value: &str) {
    let [variable] = variables else {
        panic!("expected one source variable, got {variables:?}");
    };
    assert_eq!(variable.key, key);
    assert_eq!(variable.value, value);
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
            "Source  Version  Origin    Secrets",
            "------  -------  --------  ----------------",
            "github  1.0.0    bundled   file (plaintext)",
            "jira    2.0.0    imported  file (plaintext)",
        ],
        "expected configured source list"
    );

    let requests = server.list_sources_requests();
    assert_eq!(requests.len(), 1, "expected one list_sources call");
    assert_default_workspace(requests[0].workspace.as_ref());

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
async fn source_info_renders_metadata_for_installed_source() {
    let server = MockServer::start().await;

    let assert = server
        .cmd()
        .args(["source", "info", "github"])
        .assert()
        .success();

    let stdout = String::from_utf8_lossy(&assert.get_output().stdout);
    assert!(stdout.contains("github"), "expected source name: {stdout}");
    assert!(
        stdout.contains("installed"),
        "expected installed status: {stdout}"
    );
    assert!(stdout.contains("1.0.0"), "expected version: {stdout}");
    assert!(
        stdout.contains("GitHub data"),
        "expected description: {stdout}"
    );
    assert!(
        stdout.contains("GITHUB_TOKEN"),
        "expected input key: {stdout}"
    );
    assert!(stdout.contains("secret"), "expected input kind: {stdout}");
    assert!(
        stdout.contains("required"),
        "expected input requirement: {stdout}"
    );
    assert!(
        !stdout.contains("github.com/settings/tokens"),
        "expected hint to be hidden without --verbose: {stdout}"
    );

    let requests = server.get_source_info_requests();
    assert_eq!(requests.len(), 1, "expected one get_source_info call");
    assert_eq!(requests[0].name, "github");
    assert_default_workspace(requests[0].workspace.as_ref());

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn source_info_verbose_includes_input_hints() {
    let server = MockServer::start().await;

    let assert = server
        .cmd()
        .args(["source", "info", "github", "--verbose"])
        .assert()
        .success();

    let stdout = String::from_utf8_lossy(&assert.get_output().stdout);
    assert!(
        stdout.contains("github.com/settings/tokens"),
        "expected hint with --verbose: {stdout}"
    );

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn source_info_renders_metadata_for_available_source() {
    let server = MockServer::start().await;

    let assert = server
        .cmd()
        .args(["source", "info", "slack"])
        .assert()
        .success();

    let stdout = String::from_utf8_lossy(&assert.get_output().stdout);
    assert!(
        stdout.contains("not installed"),
        "expected not-installed status: {stdout}"
    );
    assert!(stdout.contains("2.1.0"), "expected version: {stdout}");
    assert!(
        stdout.contains("Slack data"),
        "expected description: {stdout}"
    );

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn source_info_renders_installed_imported_source() {
    let server = MockServer::start().await;

    let assert = server
        .cmd()
        .args(["source", "info", "jira"])
        .assert()
        .success();

    let stdout = String::from_utf8_lossy(&assert.get_output().stdout);
    assert!(stdout.contains("jira"), "expected source name: {stdout}");
    assert!(
        stdout.contains("installed"),
        "expected installed status: {stdout}"
    );
    assert!(
        stdout.contains("imported"),
        "expected imported origin: {stdout}"
    );
    assert!(stdout.contains("2.0.0"), "expected version: {stdout}");

    let requests = server.get_source_info_requests();
    assert_eq!(requests.len(), 1, "expected one get_source_info call");
    assert_eq!(requests[0].name, "jira");
    assert_default_workspace(requests[0].workspace.as_ref());

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

    let assert = server
        .cmd()
        .args(["source", "info", "nope"])
        .assert()
        .failure();

    let stderr = String::from_utf8_lossy(&assert.get_output().stderr);
    assert!(
        stderr.contains("unknown source 'nope'"),
        "expected unknown source error: {stderr}"
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
async fn source_add_file_confirms_hosts_from_local_manifest_variables() {
    let server = MockServer::start().await;
    let source_dir = tempdir().expect("source dir");
    let manifest_file = source_dir.path().join("manifest.yaml");
    std::fs::write(
        &manifest_file,
        r#"
name: local_gitlab
version: 1.0.0
dsl_version: 3
backend: http
inputs:
  GITLAB_API_BASE:
    kind: variable
    default: https://gitlab.com/api/v4
base_url: "{{input.GITLAB_API_BASE}}"
tables:
  - name: projects
    description: Projects
    request:
      path: /projects
"#,
    )
    .expect("write manifest");

    let assert = server
        .cmd()
        .args([
            "source",
            "add",
            "--file",
            manifest_file.to_str().expect("manifest path utf8"),
        ])
        .env(GITLAB_API_BASE_KEY, GITLAB_API_BASE_VALUE)
        .assert()
        .success();

    let stdout = String::from_utf8_lossy(&assert.get_output().stdout);
    assert!(
        stdout.contains("gitlab.internal"),
        "expected local manifest host in stdout: {stdout}"
    );
    assert!(
        !stdout.contains("gitlab.com"),
        "should not confirm default host when env overrides it: {stdout}"
    );
    assert!(
        server.resolve_bundled_source_hosts_requests().is_empty(),
        "file imports should resolve hosts locally without the bundled host RPC"
    );

    let requests = server.import_source_requests();
    assert_eq!(requests.len(), 1, "expected one import_source call");
    assert_single_source_variable(
        &requests[0].variables,
        GITLAB_API_BASE_KEY,
        GITLAB_API_BASE_VALUE,
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
    assert!(
        stderr.contains("coral source add --interactive github"),
        "expected exact interactive recovery command: {stderr}"
    );

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn source_add_confirms_hosts_resolved_from_env_variables() {
    let server = MockServer::start_with_config(gitlab_host_confirmation_config()).await;

    let assert = server
        .cmd()
        .args(["source", "add", "gitlab"])
        .env(GITLAB_API_BASE_KEY, GITLAB_API_BASE_VALUE)
        .assert()
        .success();

    let stdout = String::from_utf8_lossy(&assert.get_output().stdout);
    assert!(
        stdout.contains("gitlab.internal"),
        "expected resolved env host in stdout: {stdout}"
    );
    assert!(
        !stdout.contains("gitlab.com"),
        "should not confirm default host when env overrides it: {stdout}"
    );

    let host_requests = server.resolve_bundled_source_hosts_requests();
    assert_eq!(host_requests.len(), 1, "expected one host resolution call");
    assert_eq!(host_requests[0].name, "gitlab");
    assert_single_source_variable(
        &host_requests[0].variables,
        GITLAB_API_BASE_KEY,
        GITLAB_API_BASE_VALUE,
    );

    let create_requests = server.create_bundled_source_requests();
    assert_eq!(create_requests.len(), 1, "expected one create call");
    assert_single_source_variable(
        &create_requests[0].variables,
        GITLAB_API_BASE_KEY,
        GITLAB_API_BASE_VALUE,
    );

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn source_add_prints_unresolved_hosts_separately() {
    let server = MockServer::start_with_config(
        gitlab_host_confirmation_config().with_resolve_bundled_source_hosts(
            ResolveBundledSourceHostsResponse {
                hosts: vec!["gitlab.internal".to_string()],
                unresolved_hosts: vec!["{{input.RUNTIME_HOST}}".to_string()],
            },
        ),
    )
    .await;

    let assert = server
        .cmd()
        .args(["source", "add", "gitlab"])
        .env(GITLAB_API_BASE_KEY, GITLAB_API_BASE_VALUE)
        .assert()
        .success();

    let stdout = String::from_utf8_lossy(&assert.get_output().stdout);
    assert!(
        stdout.contains("This source will connect to the following hosts:"),
        "expected concrete host section in stdout: {stdout}"
    );
    assert!(
        stdout.contains("gitlab.internal"),
        "expected concrete host in stdout: {stdout}"
    );
    assert!(
        stdout.contains("Some outbound hosts could not be determined before setup:"),
        "expected unresolved host section in stdout: {stdout}"
    );
    assert!(
        stdout.contains("{{input.RUNTIME_HOST}}"),
        "expected unresolved host marker in stdout: {stdout}"
    );

    let create_requests = server.create_bundled_source_requests();
    assert_eq!(create_requests.len(), 1, "expected one create call");
    assert_single_source_variable(
        &create_requests[0].variables,
        GITLAB_API_BASE_KEY,
        GITLAB_API_BASE_VALUE,
    );

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn source_add_interactive_declining_hosts_does_not_create_source() {
    let server = MockServer::start_with_config(gitlab_host_confirmation_config()).await;

    // GITLAB_API_BASE is supplied via the environment, so the only interactive
    // prompt before secrets is the host confirmation. Drive it through a
    // pseudo-tty and answer "no".
    let command = format!(
        "env CORAL_ENDPOINT={} CORAL_CONFIG_DIR={} {}={} {} source add --interactive gitlab",
        sh_quote(server.endpoint_uri()),
        sh_quote(&server.config_dir().display().to_string()),
        GITLAB_API_BASE_KEY,
        sh_quote(GITLAB_API_BASE_VALUE),
        sh_quote(env!("CARGO_BIN_EXE_coral")),
    );
    let shell = format!("printf 'n\\r' | {}", script_command(&command));
    let output = std::process::Command::new("sh")
        .arg("-c")
        .arg(shell)
        .output()
        .expect("run source add through pseudo-tty");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "declining hosts should exit cleanly\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        stdout.contains("gitlab.internal"),
        "expected the resolved host to be shown before the prompt: {stdout}"
    );
    assert!(
        stdout.contains("was not connected"),
        "expected the cancellation message: {stdout}"
    );

    // The hosts were resolved for display, but declining must prevent any
    // source from being created.
    assert_eq!(
        server.resolve_bundled_source_hosts_requests().len(),
        1,
        "expected exactly one host resolution"
    );
    assert!(
        server.create_bundled_source_requests().is_empty(),
        "declining the host confirmation must not create the source"
    );
    assert_eq!(
        server.source_operation_events(),
        vec!["resolve_bundled_source_hosts"],
        "expected host resolution with no subsequent create"
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
        !stderr.contains("default:demo_bundled"),
        "should not expose workspace-qualified source name: {stderr}"
    );

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn source_test_normalizes_error_for_unknown_source() {
    let server = MockServer::start_with_config(
        MockServerConfig::default()
            .with_validate_source_not_found("default:totally_unknown")
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
        stderr.contains("coral source discover"),
        "expected discover suggestion in stderr: {stderr}"
    );
    assert!(
        !stderr.contains("default:totally_unknown"),
        "should not expose workspace-qualified source name: {stderr}"
    );

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn source_remove_normalizes_error_for_unknown_source() {
    let server = MockServer::start_with_config(
        MockServerConfig::default().with_delete_source_not_found("default:unknown_source"),
    )
    .await;

    let assert = server
        .cmd()
        .args(["source", "remove", "unknown_source"])
        .assert()
        .failure();

    let stderr = String::from_utf8_lossy(&assert.get_output().stderr);
    assert!(
        stderr.contains("source 'unknown_source' was not found"),
        "expected normalized not-found error in stderr: {stderr}"
    );
    assert!(
        stderr.contains("coral source list"),
        "expected list suggestion in stderr: {stderr}"
    );
    assert!(
        !stderr.contains("default:unknown_source"),
        "should not expose workspace-qualified source name: {stderr}"
    );

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn source_remove_preserves_unrelated_not_found_errors() {
    // Server can return Code::NotFound for reasons other than a missing
    // catalog entry (e.g. a missing manifest file mapped from
    // io::ErrorKind::NotFound). The CLI must not rewrite those into the
    // friendly "source was not found" message.
    let raw_message = "manifest file missing: No such file or directory (os error 2)";
    let server = MockServer::start_with_config(
        MockServerConfig::default().with_delete_source_error(Code::NotFound, raw_message),
    )
    .await;

    let assert = server
        .cmd()
        .args(["source", "remove", "broken_source"])
        .assert()
        .failure();

    let stderr = String::from_utf8_lossy(&assert.get_output().stderr);
    assert!(
        stderr.contains(raw_message),
        "expected raw server error to surface unchanged: {stderr}"
    );
    assert!(
        !stderr.contains("source 'broken_source' was not found"),
        "should not rewrite non-source-missing NotFound: {stderr}"
    );

    server.shutdown().await;
}
