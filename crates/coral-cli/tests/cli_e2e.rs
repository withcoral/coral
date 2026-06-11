#![allow(
    clippy::indexing_slicing,
    clippy::string_slice,
    missing_docs,
    unused_crate_dependencies,
    reason = "Integration test: assertion-style indexing is idiomatic; only a subset of dependencies are used."
)]
#![cfg(feature = "cli-test-server")]

mod harness;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use assert_cmd::Command;
use assert_cmd::assert::Assert;
use coral_api::v1::{
    DiscoverSourcesResponse, ExecuteSqlResponse, GetIdentitySpecResponse, IdentitySpec,
    ListSourcesResponse, Source, SourceCredentialStorage, SourceInfo, SourceOrigin,
};
use tempfile::{TempDir, tempdir};
use tonic::Code;

use harness::{MockServer, MockServerConfig, encode_arrow_ipc_stream};

#[cfg(feature = "embedded-ui")]
#[test]
fn ui_help_does_not_require_app_bootstrap() {
    let assert = Command::cargo_bin("coral")
        .expect("cargo bin")
        .args(["ui", "--help"])
        .assert()
        .success();

    let stdout = stdout_text(&assert);
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

/// Runs `coral <args>` against the mock server and returns the assertion.
fn run_cli(server: &MockServer, args: &[&str]) -> Assert {
    server.cmd().args(args).assert()
}

fn stdout_text(assert: &Assert) -> String {
    String::from_utf8_lossy(&assert.get_output().stdout).into_owned()
}

fn stderr_text(assert: &Assert) -> String {
    String::from_utf8_lossy(&assert.get_output().stderr).into_owned()
}

fn write_yaml(dir: &TempDir, file_name: &str, yaml: &str) -> PathBuf {
    let path = dir.path().join(file_name);
    std::fs::write(&path, yaml).expect("write yaml fixture");
    path
}

/// Prepares `coral <subcommand> add --file <file>` against the mock server.
fn add_file_cmd(server: &MockServer, subcommand: &str, file: &Path) -> Command {
    let mut cmd = server.cmd();
    cmd.args([
        subcommand,
        "add",
        "--file",
        file.to_str().expect("fixture path utf8"),
    ]);
    cmd
}

#[tokio::test(flavor = "multi_thread")]
async fn sql_command_renders_table_output() {
    let server = MockServer::start().await;

    let assert = run_cli(&server, &["sql", "select 1 as value"]).success();

    let stdout = stdout_text(&assert);
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

    let assert = run_cli(&server, &["source", "list"]).success();

    let stdout = stdout_text(&assert);
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

    let assert = run_cli(&server, &["source", "list"]).success();

    let stdout = stdout_text(&assert);
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

    let assert = run_cli(&server, &["sql", "--format", "json", "select 1 as value"]).success();

    let stdout = stdout_text(&assert);
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

    let assert = run_cli(&server, &["source", "discover"]).success();

    let stdout = stdout_text(&assert);
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

    let assert = run_cli(&server, &["source", "discover"]).success();

    let stdout = stdout_text(&assert);
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

    let assert = run_cli(&server, &["source", "info", "github"]).success();

    let stdout = stdout_text(&assert);
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

    let assert = run_cli(&server, &["source", "info", "github", "--verbose"]).success();

    let stdout = stdout_text(&assert);
    assert!(
        stdout.contains("github.com/settings/tokens"),
        "expected hint with --verbose: {stdout}"
    );

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn source_info_renders_metadata_for_available_source() {
    let server = MockServer::start().await;

    let assert = run_cli(&server, &["source", "info", "slack"]).success();

    let stdout = stdout_text(&assert);
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

    let assert = run_cli(&server, &["source", "info", "jira"]).success();

    let stdout = stdout_text(&assert);
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

    let assert = run_cli(&server, &["source", "info", "versionless"]).success();

    let stdout = stdout_text(&assert);
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

    let assert = run_cli(&server, &["source", "info", "nope"]).failure();

    let stderr = stderr_text(&assert);
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

    let assert = run_cli(&server, &["source", "list"]).success();

    let stdout = stdout_text(&assert);
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

    let assert = run_cli(&server, &["source", "test", "github"]).success();

    let stdout = stdout_text(&assert);
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

    let assert = run_cli(&server, &["source", "remove", "github"]).success();

    let stdout = stdout_text(&assert);
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

    let assert = run_cli(&server, &["sql", "select 1 as value"]).failure();

    let stderr = stderr_text(&assert);
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

    let assert = run_cli(&server, &["source", "test", "github"]).failure();

    let stderr = stderr_text(&assert);
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

    let assert = run_cli(&server, &["sql", "select id, name from users"]).success();

    let stdout = stdout_text(&assert);
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

    let assert = run_cli(
        &server,
        &["sql", "--format", "json", "select id, name from users"],
    )
    .success();

    let stdout = stdout_text(&assert);
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

    let assert = run_cli(&server, &["sql", "select id, name from empty_table"]).success();

    let stdout = stdout_text(&assert);
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

    let assert = run_cli(
        &server,
        &["sql", "--format", "json", "select id from empty_table"],
    )
    .success();

    let stdout = stdout_text(&assert);
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

    let assert = run_cli(&server, &["source", "add"]).failure();

    let stderr = stderr_text(&assert);
    assert!(
        stderr.contains("required") || stderr.contains("must be provided"),
        "expected clap error about required arguments: {stderr}"
    );

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn source_add_rejects_name_and_file_together() {
    let server = MockServer::start().await;

    let assert = run_cli(
        &server,
        &["source", "add", "github", "--file", "manifest.yaml"],
    )
    .failure();

    let stderr = stderr_text(&assert);
    assert!(
        stderr.contains("cannot be used with"),
        "expected clap conflict error: {stderr}"
    );

    server.shutdown().await;
}

/// DSL v4 github manifest whose single surface references a relative
/// `openapi.yaml` descriptor.
const V4_GITHUB_MANIFEST: &str = r"
name: github
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    file: openapi.yaml
    sha256: 0693619bd2b15b9257926af5d5738c75f504186daf51acb9ec247e24b493da89
";

/// Writes the v4 github manifest plus its `openapi.yaml` descriptor into a
/// fresh temp dir; `manifest_suffix` extends the single surface entry.
fn v4_github_source_dir(manifest_suffix: &str) -> (TempDir, PathBuf) {
    let source_dir = tempdir().expect("source dir");
    write_yaml(&source_dir, "openapi.yaml", "\nopenapi: 3.0.3\npaths: {}\n");
    let manifest_file = write_yaml(
        &source_dir,
        "manifest.yaml",
        &format!("{V4_GITHUB_MANIFEST}{manifest_suffix}"),
    );
    (source_dir, manifest_file)
}

/// Minimal fixed-token identity spec manifest for the given spec name.
fn fixed_token_spec_yaml(name: &str) -> String {
    format!(
        r"
kind: identity
spec_version: 1
name: {name}
version: 0.1.0
issuer: github
type: fixed_token
"
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn source_add_file_resolves_v4_relative_descriptor_from_manifest_dir() {
    let server = MockServer::start().await;
    let (source_dir, manifest_file) = v4_github_source_dir("");

    add_file_cmd(&server, "source", &manifest_file)
        .assert()
        .success();

    let requests = server.import_source_requests();
    assert_eq!(requests.len(), 1, "expected one import_source call");
    let manifest_yaml = &requests[0].manifest_yaml;
    let canonical = source_dir
        .path()
        .join("openapi.yaml")
        .canonicalize()
        .expect("canonical descriptor");
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
async fn source_add_file_with_v4_identity_requirements_requires_tty() {
    let server = MockServer::start().await;
    let (_source_dir, manifest_file) = v4_github_source_dir(
        r"    identity_requirements:
      accepts:
        - id: github-rest-read
          identity_specs:
            - github_oauth
          audience:
            host: github.com
",
    );

    let assert = add_file_cmd(&server, "source", &manifest_file)
        .assert()
        .failure();

    let stderr = stderr_text(&assert);
    assert!(
        stderr.contains("source identity setup requires a TTY"),
        "expected source identity TTY requirement error: {stderr}"
    );
    assert!(
        server.import_source_requests().is_empty(),
        "source import must not run before identity setup"
    );

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn source_add_file_forwards_identity_specs_from_manifest_bundle() {
    let server = MockServer::start().await;
    let source_dir = tempdir().expect("source dir");
    let manifest_file = write_yaml(
        &source_dir,
        "bundle.yaml",
        &format!(
            r"
---{spec}---
name: github
version: 0.1.0
dsl_version: 3
backend: http
base_url: https://example.com
tables:
  - name: users
    description: Demo users
    request:
      method: GET
      path: /users
    columns:
      - name: id
        type: Utf8
",
            spec = fixed_token_spec_yaml("github_oauth")
        ),
    );

    add_file_cmd(&server, "source", &manifest_file)
        .assert()
        .success();

    let requests = server.import_source_requests();
    assert_eq!(requests.len(), 1, "expected one import_source call");
    assert!(
        requests[0].replace_identity_bindings,
        "source add should replace source identity bindings on import"
    );
    assert_eq!(
        requests[0].identity_spec_manifest_yamls.len(),
        1,
        "expected bundled identity spec to be forwarded"
    );
    assert!(
        requests[0].identity_spec_manifest_yamls[0].contains("name: github_oauth"),
        "expected github_oauth identity spec in import request"
    );
    assert!(
        !requests[0].manifest_yaml.contains("kind: identity"),
        "source manifest sent to import should contain only the source document"
    );

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn identity_spec_commands_use_identity_spec_service() {
    let server = MockServer::start().await;
    let source_dir = tempdir().expect("identity spec dir");
    let identity_file = write_yaml(
        &source_dir,
        "github_oauth.yaml",
        &fixed_token_spec_yaml("github_oauth"),
    );

    add_file_cmd(&server, "identity-spec", &identity_file)
        .assert()
        .success();
    run_cli(&server, &["identity-spec", "list"]).success();
    run_cli(&server, &["identity-spec", "info", "github_oauth"]).success();
    run_cli(
        &server,
        &["identity-spec", "remove", "github_oauth", "--force"],
    )
    .success();

    assert_eq!(server.add_identity_spec_requests().len(), 1);
    assert_eq!(server.list_identity_specs_requests().len(), 1);
    assert_eq!(server.get_identity_spec_requests()[0].name, "github_oauth");
    let delete_requests = server.delete_identity_spec_requests();
    assert_eq!(delete_requests[0].name, "github_oauth");
    assert!(delete_requests[0].force);

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn identity_spec_add_noninteractive_does_not_send_manifest_defaults_as_inputs() {
    let server = MockServer::start().await;
    let source_dir = tempdir().expect("identity spec dir");
    let identity_file = write_yaml(
        &source_dir,
        "demo_oauth.yaml",
        r"
kind: identity
spec_version: 1
name: demo_oauth
version: 0.1.0
description: Demo OAuth identity.
issuer: demo
type: oauth
audience:
  host: example.test
inputs:
  DEMO_TENANT:
    kind: variable
    default: tenant-a
  DEMO_OAUTH_CLIENT_SECRET:
    kind: secret
oauth:
  method:
    flow:
      type: authorization_code
      pkce: required
    redirect_uri: http://127.0.0.1:53682/oauth/callback
    endpoints:
      authorization_url: https://{{input.DEMO_TENANT}}.example.test/oauth/authorize
      token_url: https://{{input.DEMO_TENANT}}.example.test/oauth/token
    client:
      id:
        default: demo-client
      secret:
        input: DEMO_OAUTH_CLIENT_SECRET
        transport: request_body
",
    );

    add_file_cmd(&server, "identity-spec", &identity_file)
        .env_remove("DEMO_TENANT")
        .env("DEMO_OAUTH_CLIENT_SECRET", "client-secret")
        .assert()
        .success();

    let requests = server.add_identity_spec_requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].inputs.len(), 1);
    assert_eq!(requests[0].inputs[0].key, "DEMO_OAUTH_CLIENT_SECRET");
    assert_eq!(requests[0].inputs[0].value, "client-secret");

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn identity_commands_use_identity_service() {
    let server = MockServer::start().await;

    run_cli(&server, &["identity", "list"]).success();
    run_cli(&server, &["identity", "info", "github_local"]).success();
    run_cli(&server, &["identity", "remove", "github_local"]).success();

    assert_eq!(server.list_user_owned_identities_requests().len(), 1);
    assert_eq!(
        server.get_user_owned_identity_requests()[0].name,
        "github_local"
    );
    assert_eq!(
        server.delete_user_owned_identity_requests()[0].name,
        "github_local"
    );

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn identity_add_fixed_token_reads_token_from_stdin() {
    let server = MockServer::start_with_config(MockServerConfig::default().with_get_identity_spec(
        GetIdentitySpecResponse {
            identity_spec: Some(IdentitySpec {
                name: "github_pat".to_string(),
                version: "0.1.0".to_string(),
                description: "GitHub PAT identity.".to_string(),
                issuer: "github".to_string(),
                identity_type: "fixed_token".to_string(),
                manifest_yaml: fixed_token_spec_yaml("github_pat"),
            }),
        },
    ))
    .await;

    server
        .cmd()
        .args([
            "identity",
            "add",
            "github_local",
            "--identity-spec",
            "github_pat",
            "--token-stdin",
        ])
        .write_stdin("pat-token\n")
        .assert()
        .success();

    assert_eq!(server.get_identity_spec_requests()[0].name, "github_pat");
    let requests = server.create_user_owned_identity_with_fixed_token_requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].name, "github_local");
    assert_eq!(requests[0].identity_spec, "github_pat");
    assert_eq!(requests[0].token, "pat-token");
    assert!(
        server
            .create_user_owned_identity_with_oauth_requests()
            .is_empty(),
        "fixed-token identity add must not use OAuth creation"
    );

    server.shutdown().await;
}

// ---------------------------------------------------------------------------
// Name validation
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn source_test_rejects_invalid_name() {
    let server = MockServer::start().await;

    let assert = run_cli(&server, &["source", "test", "a/b"]).failure();

    let stderr = stderr_text(&assert);
    assert!(
        stderr.contains("must not contain"),
        "expected name validation error: {stderr}"
    );

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn source_remove_rejects_invalid_name() {
    let server = MockServer::start().await;

    let assert = run_cli(&server, &["source", "remove", "a/b"]).failure();

    let stderr = stderr_text(&assert);
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

    let stderr = stderr_text(&assert);
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
async fn source_add_interactive_requires_tty() {
    let server = MockServer::start().await;

    let assert = run_cli(&server, &["source", "add", "--interactive", "github"]).failure();

    let stderr = stderr_text(&assert);
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

    let assert = run_cli(&server, &["source", "test", "demo_bundled"]).failure();

    let stderr = stderr_text(&assert);
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

    let assert = run_cli(&server, &["source", "test", "totally_unknown"]).failure();

    let stderr = stderr_text(&assert);
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

    let assert = run_cli(&server, &["source", "remove", "unknown_source"]).failure();

    let stderr = stderr_text(&assert);
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

    let assert = run_cli(&server, &["source", "remove", "broken_source"]).failure();

    let stderr = stderr_text(&assert);
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
