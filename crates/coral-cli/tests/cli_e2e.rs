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
    AddFunctionResponse, CatalogRebuildResult, DiscoverSourcesResponse, ExecuteSqlResponse,
    Function, FunctionArgument, FunctionRuntimeInvalid, FunctionRuntimeReady, FunctionWriteSurface,
    ListFunctionsResponse, ListSourcesResponse, ListWorkspacesResponse, RebuildSearchIndexResponse,
    SearchDataScope, SearchIndexProvider, SearchMaintenanceResult, SearchMaintenanceState,
    SearchProvider, Source, SourceCredentialStorage, SourceInfo, SourceOrigin, Workspace, function,
    search_clear_target, search_maintenance_result,
};
use tempfile::tempdir;
use tonic::Code;

use harness::{
    MockServer, MockServerConfig, assert_default_workspace, assert_workspace_name,
    encode_arrow_ipc_stream,
};

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
async fn sql_command_uses_workspace_flag() {
    let server = MockServer::start().await;

    server
        .cmd()
        .args(["--workspace", "work", "sql", "select 1 as value"])
        .assert()
        .success();

    let requests = server.execute_sql_requests();
    assert_eq!(requests.len(), 1, "expected one execute_sql call");
    assert_workspace_name(requests[0].workspace.as_ref(), "work");

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn sql_command_uses_workspace_env() {
    let server = MockServer::start().await;

    server
        .cmd()
        .env("CORAL_WORKSPACE", "work")
        .args(["sql", "select 1 as value"])
        .assert()
        .success();

    let requests = server.execute_sql_requests();
    assert_eq!(requests.len(), 1, "expected one execute_sql call");
    assert_workspace_name(requests[0].workspace.as_ref(), "work");

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn workspace_flag_overrides_workspace_env() {
    let server = MockServer::start().await;

    server
        .cmd()
        .env("CORAL_WORKSPACE", "env-work")
        .args(["--workspace", "flag-work", "sql", "select 1 as value"])
        .assert()
        .success();

    let requests = server.execute_sql_requests();
    assert_eq!(requests.len(), 1, "expected one execute_sql call");
    assert_workspace_name(requests[0].workspace.as_ref(), "flag-work");

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn source_list_uses_workspace_flag() {
    let server = MockServer::start().await;

    server
        .cmd()
        .args(["--workspace", "work", "source", "list"])
        .assert()
        .success();

    let requests = server.list_sources_requests();
    assert_eq!(requests.len(), 1, "expected one list_sources call");
    assert_workspace_name(requests[0].workspace.as_ref(), "work");

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn source_list_accepts_workspace_flag_after_subcommand() {
    let server = MockServer::start().await;

    server
        .cmd()
        .args(["source", "list", "--workspace", "work"])
        .assert()
        .success();

    let requests = server.list_sources_requests();
    assert_eq!(requests.len(), 1, "expected one list_sources call");
    assert_workspace_name(requests[0].workspace.as_ref(), "work");

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn functions_list_uses_workspace_flag() {
    let server = MockServer::start().await;

    let assert = server
        .cmd()
        .args(["--workspace", "work", "functions", "list"])
        .assert()
        .success();

    let stdout = String::from_utf8_lossy(&assert.get_output().stdout);
    assert_eq!(stdout.trim(), "No installed functions.");
    let requests = server.list_functions_requests();
    assert_eq!(requests.len(), 1, "expected one list_functions call");
    assert_workspace_name(requests[0].workspace.as_ref(), "work");

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn functions_list_renders_runtime_details() {
    let server = MockServer::start_with_config(MockServerConfig::default().with_list_functions(
        ListFunctionsResponse {
            functions: vec![
                Function {
                    name: "github_issues".to_string(),
                    runtime: Some(function::Runtime::Ready(FunctionRuntimeReady {
                        arguments: vec![
                            FunctionArgument {
                                name: "owner".to_string(),
                                data_type: "Utf8".to_string(),
                            },
                            FunctionArgument {
                                name: "repo".to_string(),
                                data_type: "Utf8".to_string(),
                            },
                        ],
                        ..FunctionRuntimeReady::default()
                    })),
                    ..Function::default()
                },
                Function {
                    name: "broken_function".to_string(),
                    runtime: Some(function::Runtime::Invalid(FunctionRuntimeInvalid {
                        reason: "could not plan function\nsource is unavailable\nHint: reinstall the source"
                            .to_string(),
                    })),
                    ..Function::default()
                },
            ],
        },
    ))
    .await;

    let assert = server.cmd().args(["functions", "list"]).assert().success();

    let stdout = String::from_utf8_lossy(&assert.get_output().stdout);
    assert!(
        stdout.contains("Arguments"),
        "missing arguments header: {stdout}"
    );
    assert!(
        stdout.contains("owner: Utf8, repo: Utf8"),
        "missing inferred arguments: {stdout}"
    );
    let invalid_row = stdout
        .lines()
        .find(|line| line.starts_with("broken_function"))
        .expect("invalid function row");
    assert!(invalid_row.contains("invalid"), "invalid status: {stdout}");
    assert!(
        !invalid_row.contains("could not plan function"),
        "validation reason leaked into table: {stdout}"
    );
    assert!(
        stdout.contains(
            "Invalid functions:\n  broken_function:\n    could not plan function\n    source is unavailable\n    Hint: reinstall the source"
        ),
        "missing indented validation reason: {stdout}"
    );

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn functions_add_sends_file_to_selected_workspace() {
    let temp = tempdir().expect("temp dir");
    let function_file = temp.path().join("echo_value.sql");
    let sql = "/* name: echo_value */ select 1 as value\n";
    std::fs::write(&function_file, sql).expect("write function");
    let server = MockServer::start_with_config(MockServerConfig::default().with_add_function(
        AddFunctionResponse {
            function: Some(Function {
                name: "echo_value".to_string(),
                runtime: Some(function::Runtime::Ready(FunctionRuntimeReady::default())),
                ..Function::default()
            }),
            replaced: false,
        },
    ))
    .await;

    let assert = server
        .cmd()
        .args(["--workspace", "work", "functions", "add", "--file"])
        .arg(&function_file)
        .assert()
        .success();
    assert_eq!(
        String::from_utf8_lossy(&assert.get_output().stdout).trim(),
        "Added function echo_value"
    );
    let requests = server.add_function_requests();
    assert_eq!(requests.len(), 1);
    assert_workspace_name(requests[0].workspace.as_ref(), "work");
    assert_eq!(requests[0].sql, sql);
    assert!(!requests[0].fail_if_exists);
    assert_eq!(requests[0].write_surface, FunctionWriteSurface::Cli as i32);

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn functions_remove_uses_selected_workspace() {
    let server = MockServer::start().await;

    let assert = server
        .cmd()
        .args(["--workspace", "work", "functions", "remove", "echo_value"])
        .assert()
        .success();
    assert_eq!(
        String::from_utf8_lossy(&assert.get_output().stdout).trim(),
        "Removed function echo_value"
    );
    let requests = server.delete_function_requests();
    assert_eq!(requests.len(), 1);
    assert_workspace_name(requests[0].workspace.as_ref(), "work");
    assert_eq!(requests[0].name, "echo_value");

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn workspace_list_renders_configured_workspaces() {
    let server = MockServer::start_with_config(MockServerConfig::default().with_list_workspaces(
        ListWorkspacesResponse {
            workspaces: vec![
                Workspace {
                    name: "default".to_string(),
                },
                Workspace {
                    name: "work".to_string(),
                },
            ],
        },
    ))
    .await;

    let assert = server.cmd().args(["workspace", "list"]).assert().success();

    let stdout = String::from_utf8_lossy(&assert.get_output().stdout);
    assert_eq!(
        nonempty_lines(&stdout),
        vec!["Workspace", "---------", "default", "work"],
        "expected workspace list"
    );
    assert_eq!(
        server.list_workspaces_requests().len(),
        1,
        "expected one list_workspaces call"
    );

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn workspace_create_sends_request() {
    let server = MockServer::start().await;

    let assert = server
        .cmd()
        .args(["workspace", "create", "work"])
        .assert()
        .success();

    let stdout = String::from_utf8_lossy(&assert.get_output().stdout);
    assert_eq!(stdout.trim(), "Created workspace work");
    let requests = server.create_workspace_requests();
    assert_eq!(requests.len(), 1, "expected one create_workspace call");
    assert_workspace_name(requests[0].workspace.as_ref(), "work");

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn workspace_remove_sends_request() {
    let server = MockServer::start().await;

    let assert = server
        .cmd()
        .args(["workspace", "remove", "work"])
        .assert()
        .success();

    let stdout = String::from_utf8_lossy(&assert.get_output().stdout);
    assert_eq!(stdout.trim(), "Removed workspace work");
    let requests = server.delete_workspace_requests();
    assert_eq!(requests.len(), 1, "expected one delete_workspace call");
    assert_workspace_name(requests[0].workspace.as_ref(), "work");

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
    assert!(
        stdout.contains("github (1 table function)"),
        "expected table-function schema summary: {stdout}"
    );
    assert!(
        stdout.contains("search_issues"),
        "expected table-function name: {stdout}"
    );
    assert!(
        !stdout.contains("search_issues()"),
        "table-function summary must not imply a zero-argument signature: {stdout}"
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
        guide_required: None,
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
async fn search_command_renders_text_output_and_provider_statuses() {
    let server = MockServer::start().await;

    let assert = server
        .cmd()
        .args(["search", "messages", "text"])
        .assert()
        .success();

    let stdout = String::from_utf8_lossy(&assert.get_output().stdout);
    assert!(
        stdout.contains("Results"),
        "expected results section: {stdout}"
    );
    assert!(
        stdout.contains("[table] local_messages.messages"),
        "expected catalog table result: {stdout}"
    );
    assert!(
        stdout.contains("required: owner"),
        "expected required filters on the entry: {stdout}"
    );
    assert!(
        stdout.contains("Provider statuses"),
        "expected provider statuses section: {stdout}"
    );
    assert!(
        stdout.contains("- observed_values: not_enabled"),
        "disabled provider should remain visible: {stdout}"
    );
    assert!(
        stdout.contains("- native_fanout: skipped"),
        "skipped provider should remain visible: {stdout}"
    );

    let requests = server.search_requests();
    assert_eq!(requests.len(), 1, "expected one search call");
    assert_eq!(requests[0].query, "messages text");
    assert_eq!(requests[0].limit, 10);
    assert_default_workspace(requests[0].workspace.as_ref());

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn search_json_output_preserves_typed_payloads_and_statuses() {
    let server = MockServer::start().await;

    let assert = server
        .cmd()
        .args(["search", "--json", "--limit", "5", "messages"])
        .assert()
        .success();

    let stdout = String::from_utf8_lossy(&assert.get_output().stdout);
    let response: serde_json::Value =
        serde_json::from_str(stdout.trim()).expect("search --json should emit JSON");

    assert_eq!(response["results"][0]["kind"], "table");
    assert_eq!(
        response["results"][0]["sql_reference"],
        "local_messages.messages"
    );
    // Matching columns nest under the entry rather than arriving as peers.
    assert_eq!(response["results"][0]["fields"]["text"], "Utf8");
    assert_eq!(response["results"][0]["required"][0], "owner");
    assert_eq!(response["results"].as_array().map(Vec::len), Some(1));
    assert_eq!(
        response["provider_statuses"][0]["coverage"]["searched_units"],
        3
    );
    assert_eq!(
        response["provider_statuses"][1]["provider"],
        "observed_values"
    );
    assert!(response["provider_statuses"][1]["coverage"].is_null());
    assert_eq!(response["provider_statuses"][2]["state"], "skipped");
    assert!(response["provider_statuses"][2]["coverage"].is_null());
    assert_eq!(response["truncation"]["returned_count"], 2);

    let requests = server.search_requests();
    assert_eq!(requests.len(), 1, "expected one search call");
    assert_eq!(requests[0].query, "messages");
    assert_eq!(requests[0].limit, 5);
    assert_default_workspace(requests[0].workspace.as_ref());

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn search_rebuild_remains_free_text_query() {
    let server = MockServer::start().await;

    server.cmd().args(["search", "rebuild"]).assert().success();

    let requests = server.search_requests();
    assert_eq!(requests.len(), 1, "expected one search call");
    assert_eq!(requests[0].query, "rebuild");
    assert!(
        server.rebuild_search_index_requests().is_empty(),
        "plain search must not call maintenance"
    );

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn search_index_rebuild_calls_app_maintenance_rpc() {
    let server = MockServer::start().await;

    let assert = server
        .cmd()
        .args(["search-index", "rebuild", "--force"])
        .assert()
        .success();

    let stdout = String::from_utf8_lossy(&assert.get_output().stdout);
    assert!(
        stdout.contains("Rebuilt catalog search index"),
        "expected rebuild output: {stdout}"
    );
    assert!(
        stdout.contains("Rebuilt observed-values search index")
            && stdout.contains("Pre-rebuild queue: processed 2")
            && stdout.contains("upserted 2")
            && stdout.contains("wrote 2 FTS rows")
            && stdout.contains("failed 1")
            && stdout.contains("remaining 1"),
        "expected structured observed pre-rebuild drain output: {stdout}"
    );

    assert!(
        server.search_requests().is_empty(),
        "maintenance command must not call Search"
    );
    let requests = server.rebuild_search_index_requests();
    assert_eq!(requests.len(), 1, "expected one rebuild call");
    assert_default_workspace(requests[0].workspace.as_ref());
    assert_eq!(requests[0].provider, SearchIndexProvider::All as i32);
    assert!(requests[0].force);

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn search_index_rebuild_reports_current_projection_as_skipped() {
    let server = MockServer::start_with_config(
        MockServerConfig::default().with_rebuild_search_index(RebuildSearchIndexResponse {
            results: vec![SearchMaintenanceResult {
                provider: SearchProvider::CatalogMetadata as i32,
                state: SearchMaintenanceState::Noop as i32,
                note: "catalog search projection already current".to_string(),
                detail: Some(search_maintenance_result::Detail::CatalogRebuild(
                    CatalogRebuildResult {
                        old_document_count: 3,
                        new_document_count: 3,
                        projection_changed: false,
                        rebuild_performed: false,
                    },
                )),
            }],
        }),
    )
    .await;

    let assert = server
        .cmd()
        .args(["search-index", "rebuild"])
        .assert()
        .success();
    let stdout = String::from_utf8_lossy(&assert.get_output().stdout);

    assert!(
        stdout.contains(
            "Skipped rebuilding catalog search index: projection already current with 3 documents."
        ),
        "expected no-op rebuild output: {stdout}"
    );
    assert!(
        !stdout.contains("Rebuilt catalog"),
        "no-op rebuild must not claim a rebuild: {stdout}"
    );

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn search_index_drain_calls_app_maintenance_rpc() {
    let server = MockServer::start().await;

    let assert = server
        .cmd()
        .args(["search-index", "drain", "--budget-ms", "2500"])
        .assert()
        .success();

    let stdout = String::from_utf8_lossy(&assert.get_output().stdout);
    assert!(
        stdout.contains("Drained observed-values search queue"),
        "expected drain output: {stdout}"
    );
    assert!(
        stdout.contains("dropped 1"),
        "expected dropped storage job count: {stdout}"
    );

    assert!(
        server.search_requests().is_empty(),
        "maintenance command must not call Search"
    );
    let requests = server.drain_search_queue_requests();
    assert_eq!(requests.len(), 1, "expected one drain call");
    assert_default_workspace(requests[0].workspace.as_ref());
    assert_eq!(requests[0].budget_ms, 2500);

    server
        .cmd()
        .args(["search-index", "drain"])
        .assert()
        .success();
    let requests = server.drain_search_queue_requests();
    assert_eq!(requests.len(), 2, "expected second drain call");
    assert_eq!(requests[1].budget_ms, 0);

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn search_index_clear_calls_app_maintenance_rpc() {
    let server = MockServer::start().await;

    let assert = server
        .cmd()
        .args([
            "search-index",
            "clear",
            "--scope",
            "all",
            "--workspace",
            "default",
            "--yes",
        ])
        .assert()
        .success();

    let stdout = String::from_utf8_lossy(&assert.get_output().stdout);
    assert!(
        stdout.contains("Cleared catalog search data"),
        "expected clear output: {stdout}"
    );
    assert!(
        stdout.contains("Storage cleanup: local search storage cleanup completed."),
        "expected storage cleanup output: {stdout}"
    );

    assert!(
        server.search_requests().is_empty(),
        "maintenance command must not call Search"
    );
    let requests = server.clear_search_data_requests();
    assert_eq!(requests.len(), 1, "expected one clear call");
    assert_default_workspace(requests[0].workspace.as_ref());
    assert_eq!(requests[0].scope, SearchDataScope::All as i32);
    match requests[0]
        .target
        .as_ref()
        .and_then(|target| target.target.as_ref())
    {
        Some(search_clear_target::Target::Workspace(workspace_scope)) => {
            assert!(*workspace_scope);
        }
        other => panic!("expected workspace clear target, got {other:?}"),
    }

    server
        .cmd()
        .args([
            "search-index",
            "clear",
            "--scope",
            "all",
            "--workspace",
            "work",
            "--yes",
        ])
        .assert()
        .success();
    let requests = server.clear_search_data_requests();
    assert_eq!(requests.len(), 2, "expected second clear call");
    assert_workspace_name(requests[1].workspace.as_ref(), "work");

    server
        .cmd()
        .args([
            "search-index",
            "clear",
            "--scope",
            "all",
            "--source",
            "searchable",
            "--workspace",
            "default",
            "--yes",
        ])
        .assert()
        .success();
    let requests = server.clear_search_data_requests();
    assert_eq!(requests.len(), 3, "expected source clear call");
    assert_default_workspace(requests[2].workspace.as_ref());
    assert_eq!(requests[2].scope, SearchDataScope::All as i32);
    match requests[2]
        .target
        .as_ref()
        .and_then(|target| target.target.as_ref())
    {
        Some(search_clear_target::Target::SourceName(source_name)) => {
            assert_eq!(source_name, "searchable");
        }
        other => panic!("expected source clear target, got {other:?}"),
    }

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn search_index_clear_requires_explicit_workspace_even_when_env_is_set() {
    let server = MockServer::start().await;

    let assert = server
        .cmd()
        .env("CORAL_WORKSPACE", "work")
        .args(["search-index", "clear", "--scope", "all", "--yes"])
        .assert()
        .failure();
    let stderr = String::from_utf8_lossy(&assert.get_output().stderr);
    assert!(
        stderr.contains("requires an explicit `--workspace NAME` and `--yes`"),
        "expected explicit workspace error: {stderr}"
    );

    assert!(
        server.clear_search_data_requests().is_empty(),
        "implicit environment selection must not authorize destructive clear"
    );
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
surface:
    type: openapi
    file: openapi.yaml
",
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
        .assert()
        .success();

    let stdout = String::from_utf8_lossy(&assert.get_output().stdout);
    assert!(
        stdout.contains("github (1 table function)"),
        "expected table-function schema summary after source add: {stdout}"
    );
    assert!(
        stdout.contains("search_issues"),
        "expected table-function name after source add: {stdout}"
    );
    assert!(
        !stdout.contains("search_issues()"),
        "table-function summary must not imply a zero-argument signature after source add: {stdout}"
    );

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
