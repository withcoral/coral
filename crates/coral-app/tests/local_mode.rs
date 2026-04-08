//! Integration tests for the local application mode.

#![allow(
    unused_crate_dependencies,
    reason = "Integration tests inherit the library crate's dependency set and intentionally exercise only a subset of it."
)]

use std::fs;
use std::path::{Path, PathBuf};

use coral_api::v1::{
    CreateBundledSourceRequest, DeleteSourceRequest, DiscoverSourcesRequest, ExecuteSqlRequest,
    ImportSourceRequest, ListSourcesRequest, ListTablesRequest, SourceSecret, SourceVariable,
    ValidateSourceRequest, Workspace,
};
use coral_client::{
    AppClient, batches_to_json_rows, decode_execute_sql_response, default_workspace,
    local::{ServerBuilder, connect_running_server},
};
use tempfile::TempDir;
use tonic::Request;

fn fixture_manifest_yaml(root: &Path) -> String {
    let data_dir = root.join("fixture-data");
    fs::create_dir_all(&data_dir).expect("create data dir");
    fs::write(
        data_dir.join("messages.jsonl"),
        r#"{"type":"user","sessionId":"s1","text":"hello"}
{"type":"assistant","sessionId":"s1","text":"world"}
"#,
    )
    .expect("write jsonl");
    format!(
        r#"
name: local_messages
version: 0.1.0
dsl_version: 3
backend: jsonl
tables:
  - name: messages
    description: Fixture messages
    source:
      location: file://{}/
      glob: "**/*.jsonl"
    columns:
      - name: type
        type: Utf8
      - name: sessionId
        type: Utf8
      - name: text
        type: Utf8
"#,
        data_dir.display()
    )
}

fn fixture_manifest_with_inputs_yaml() -> String {
    r#"
name: secured_messages
version: 0.1.0
dsl_version: 3
backend: http
base_url: "{{variable.API_BASE|https://example.com}}"
auth:
  headers:
    - name: Authorization
      from: template
      template: Bearer {{secret.API_TOKEN}}
tables:
  - name: messages
    description: Secured messages
    request:
      method: GET
      path: /messages
    response: {}
    columns:
      - name: id
        type: Utf8
"#
    .to_string()
}

async fn local_client(config_dir: impl Into<PathBuf>) -> AppClient {
    let server = ServerBuilder::new()
        .with_config_dir(config_dir)
        .start()
        .await
        .expect("start server");
    connect_running_server(server)
        .await
        .expect("connect client")
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "This integration test intentionally exercises the full local app flow in one place."
)]
async fn local_mode_source_lifecycle_and_query_work() {
    let temp = TempDir::new().expect("temp dir");
    let manifest_yaml = fixture_manifest_yaml(temp.path());
    let coral_config_dir = temp.path().join("coral-config");
    let app = local_client(&coral_config_dir).await;
    let mut source_client = app.source_client();
    let mut query_client = app.query_client();

    let added = source_client
        .import_source(Request::new(ImportSourceRequest {
            workspace: Some(default_workspace()),
            manifest_yaml: manifest_yaml.clone(),
            variables: Vec::new(),
            secrets: Vec::new(),
        }))
        .await
        .expect("import source")
        .into_inner();
    assert_eq!(added.name, "local_messages");
    assert_eq!(added.version, "0.1.0");
    assert_eq!(added.variables.len(), 0);
    assert_eq!(added.secrets.len(), 0);

    let config_path = coral_config_dir.join("config.toml");
    let config_raw = fs::read_to_string(&config_path).expect("read config");
    assert!(config_raw.contains("[workspaces.default.sources.local_messages]"));
    assert!(!config_raw.contains("manifest_yaml = "));
    assert!(!config_raw.contains("manifest_file = "));

    let installed_manifest = coral_config_dir
        .join("workspaces")
        .join("default")
        .join("sources")
        .join("local_messages")
        .join("manifest.yaml");
    assert_eq!(
        fs::read_to_string(&installed_manifest).expect("read installed manifest"),
        manifest_yaml
    );

    let listed = source_client
        .list_sources(Request::new(ListSourcesRequest {
            workspace: Some(default_workspace()),
        }))
        .await
        .expect("list sources")
        .into_inner();
    assert_eq!(listed.sources.len(), 1);

    let tested = source_client
        .validate_source(Request::new(ValidateSourceRequest {
            workspace: Some(default_workspace()),
            name: "local_messages".to_string(),
        }))
        .await
        .expect("test source")
        .into_inner();
    assert_eq!(tested.tables.len(), 1);

    let sql_response = query_client
        .execute_sql(Request::new(ExecuteSqlRequest {
            workspace: Some(default_workspace()),
            sql: "SELECT type, text FROM local_messages.messages ORDER BY text".to_string(),
        }))
        .await
        .expect("sql")
        .into_inner();
    let result = decode_execute_sql_response(&sql_response).expect("decode query");
    let rows = batches_to_json_rows(result.batches()).expect("rows");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["text"], "hello");

    let tables = query_client
        .list_tables(Request::new(ListTablesRequest {
            workspace: Some(default_workspace()),
        }))
        .await
        .expect("list tables")
        .into_inner()
        .tables;
    assert_eq!(tables.len(), 1);
    assert_eq!(tables[0].schema_name, "local_messages");
    assert_eq!(tables[0].name, "messages");
    assert!(tables[0].required_filters.is_empty());

    source_client
        .delete_source(Request::new(DeleteSourceRequest {
            workspace: Some(default_workspace()),
            name: "local_messages".to_string(),
        }))
        .await
        .expect("remove source");

    let listed_after_remove = source_client
        .list_sources(Request::new(ListSourcesRequest {
            workspace: Some(default_workspace()),
        }))
        .await
        .expect("list sources after remove")
        .into_inner();
    assert!(listed_after_remove.sources.is_empty());
    assert!(!installed_manifest.exists());
}

#[tokio::test]
async fn query_execution_rejects_non_read_only_sql() {
    let temp = TempDir::new().expect("temp dir");
    let manifest_yaml = fixture_manifest_yaml(temp.path());
    let app = local_client(temp.path().join("coral-config")).await;
    let mut source_client = app.source_client();
    let mut query_client = app.query_client();

    source_client
        .import_source(Request::new(ImportSourceRequest {
            workspace: Some(default_workspace()),
            manifest_yaml,
            variables: Vec::new(),
            secrets: Vec::new(),
        }))
        .await
        .expect("import source");

    let copy_target = temp.path().join("copied.arrow");
    let copy_error = query_client
        .execute_sql(Request::new(ExecuteSqlRequest {
            workspace: Some(default_workspace()),
            sql: format!(
                "COPY local_messages.messages TO '{}' STORED AS ARROW",
                copy_target.display()
            ),
        }))
        .await
        .expect_err("COPY TO should be rejected");
    assert_eq!(copy_error.code(), tonic::Code::InvalidArgument);
    assert!(copy_error.message().contains("DML not supported: COPY"));

    let create_error = query_client
        .execute_sql(Request::new(ExecuteSqlRequest {
            workspace: Some(default_workspace()),
            sql: "CREATE TABLE copied AS SELECT * FROM local_messages.messages".to_string(),
        }))
        .await
        .expect_err("CREATE TABLE should be rejected");
    assert_eq!(create_error.code(), tonic::Code::InvalidArgument);
    assert!(create_error.message().contains("DDL not supported"));

    let set_error = query_client
        .execute_sql(Request::new(ExecuteSqlRequest {
            workspace: Some(default_workspace()),
            sql: "SET datafusion.execution.batch_size = 1".to_string(),
        }))
        .await
        .expect_err("SET should be rejected");
    assert_eq!(set_error.code(), tonic::Code::InvalidArgument);
    assert!(set_error.message().contains("Statement not supported"));
}

#[tokio::test]
async fn missing_source_manifest_file_returns_not_found() {
    let temp = TempDir::new().expect("temp dir");
    let config_dir = temp.path().join("coral-config");
    fs::create_dir_all(&config_dir).expect("create config dir");
    fs::write(
        config_dir.join("config.toml"),
        r#"
version = 1

[workspaces.default.sources.demo]
version = "0.1.0"
origin = "imported"
"#,
    )
    .expect("write config");

    let app = local_client(&config_dir).await;
    let mut source_client = app.source_client();
    let error = source_client
        .validate_source(Request::new(ValidateSourceRequest {
            workspace: Some(default_workspace()),
            name: "demo".to_string(),
        }))
        .await
        .expect_err("missing manifest file should fail");
    assert_eq!(error.code(), tonic::Code::NotFound);
}

#[tokio::test]
async fn config_persists_across_rebuilds_without_local_trace_state() {
    let temp = TempDir::new().expect("temp dir");
    let manifest_yaml = fixture_manifest_yaml(temp.path());
    let config_dir = temp.path().join("coral-config");

    {
        let app = local_client(&config_dir).await;
        let mut source_client = app.source_client();
        let mut query_client = app.query_client();
        source_client
            .import_source(Request::new(ImportSourceRequest {
                workspace: Some(default_workspace()),
                manifest_yaml,
                variables: Vec::new(),
                secrets: Vec::new(),
            }))
            .await
            .expect("import source");
        let sql_response = query_client
            .execute_sql(Request::new(ExecuteSqlRequest {
                workspace: Some(default_workspace()),
                sql: "SELECT COUNT(*) AS n FROM local_messages.messages".to_string(),
            }))
            .await
            .expect("sql")
            .into_inner();
        let result = decode_execute_sql_response(&sql_response).expect("decode query");
        let _ = batches_to_json_rows(result.batches()).expect("rows");
    }

    let app = local_client(&config_dir).await;
    let mut source_client = app.source_client();
    let mut query_client = app.query_client();
    let listed = source_client
        .list_sources(Request::new(ListSourcesRequest {
            workspace: Some(default_workspace()),
        }))
        .await
        .expect("list sources")
        .into_inner();
    assert_eq!(listed.sources.len(), 1);

    let sql_response = query_client
        .execute_sql(Request::new(ExecuteSqlRequest {
            workspace: Some(default_workspace()),
            sql: "SELECT COUNT(*) AS n FROM local_messages.messages".to_string(),
        }))
        .await
        .expect("sql after rebuild")
        .into_inner();
    let result = decode_execute_sql_response(&sql_response).expect("decode query after rebuild");
    let rows = batches_to_json_rows(result.batches()).expect("rows after rebuild");
    assert_eq!(rows[0]["n"], 2);
    assert!(
        !config_dir.join("state").join("state.sqlite3").exists(),
        "trace/state sqlite should not be created"
    );
}

#[tokio::test]
async fn bundled_github_source_initializes_tables_with_template_secret_binding() {
    let temp = TempDir::new().expect("temp dir");
    let app = local_client(temp.path().join("coral-config")).await;
    let mut source_client = app.source_client();
    let mut query_client = app.query_client();

    source_client
        .create_bundled_source(Request::new(CreateBundledSourceRequest {
            workspace: Some(default_workspace()),
            name: "github".to_string(),
            variables: vec![SourceVariable {
                key: "GITHUB_API_BASE".to_string(),
                value: "https://api.github.com".to_string(),
            }],
            secrets: vec![SourceSecret {
                key: "GITHUB_TOKEN".to_string(),
                value: "fake-token".to_string(),
            }],
        }))
        .await
        .expect("create bundled github source");

    let tables = query_client
        .list_tables(Request::new(ListTablesRequest {
            workspace: Some(default_workspace()),
        }))
        .await
        .expect("list tables")
        .into_inner()
        .tables;
    assert!(
        tables.iter().any(|table| table.schema_name == "github"),
        "github tables should register once the template secret dependency is provided"
    );
}

#[tokio::test]
async fn broken_source_does_not_block_healthy_sources() {
    let temp = TempDir::new().expect("temp dir");
    let config_dir = temp.path().join("coral-config");
    let app = local_client(&config_dir).await;
    let mut source_client = app.source_client();
    let mut query_client = app.query_client();

    source_client
        .import_source(Request::new(ImportSourceRequest {
            workspace: Some(default_workspace()),
            manifest_yaml: fixture_manifest_yaml(temp.path()),
            variables: Vec::new(),
            secrets: Vec::new(),
        }))
        .await
        .expect("import healthy source");

    source_client
        .import_source(Request::new(ImportSourceRequest {
            workspace: Some(default_workspace()),
            manifest_yaml: fixture_manifest_with_inputs_yaml(),
            variables: vec![SourceVariable {
                key: "API_BASE".to_string(),
                value: "https://example.com".to_string(),
            }],
            secrets: vec![SourceSecret {
                key: "API_TOKEN".to_string(),
                value: "secret-token".to_string(),
            }],
        }))
        .await
        .expect("import broken source");

    fs::remove_file(
        config_dir
            .join("workspaces")
            .join("default")
            .join("sources")
            .join("secured_messages")
            .join("secrets.env"),
    )
    .expect("remove broken source secret file");

    let tables = query_client
        .list_tables(Request::new(ListTablesRequest {
            workspace: Some(default_workspace()),
        }))
        .await
        .expect("list tables with one broken source")
        .into_inner()
        .tables;
    assert!(
        tables
            .iter()
            .any(|table| table.schema_name == "local_messages"),
        "healthy source should remain queryable"
    );
    assert!(
        !tables
            .iter()
            .any(|table| table.schema_name == "secured_messages"),
        "broken source should be omitted from registered tables"
    );

    let healthy = query_client
        .execute_sql(Request::new(ExecuteSqlRequest {
            workspace: Some(default_workspace()),
            sql: "SELECT COUNT(*) AS n FROM local_messages.messages".to_string(),
        }))
        .await
        .expect("healthy source query should succeed")
        .into_inner();
    let healthy_rows = batches_to_json_rows(
        decode_execute_sql_response(&healthy)
            .expect("decode")
            .batches(),
    )
    .expect("healthy rows");
    assert_eq!(healthy_rows[0]["n"], 2);

    let broken = query_client
        .execute_sql(Request::new(ExecuteSqlRequest {
            workspace: Some(default_workspace()),
            sql: "SELECT * FROM secured_messages.messages".to_string(),
        }))
        .await
        .expect_err("broken source query should fail");
    assert_eq!(broken.code(), tonic::Code::Internal);
}

#[tokio::test]
async fn discover_surfaces_corrupted_config_instead_of_marking_sources_uninstalled() {
    let temp = TempDir::new().expect("temp dir");
    let config_dir = temp.path().join("coral-config");
    fs::create_dir_all(&config_dir).expect("create config dir");
    fs::write(config_dir.join("config.toml"), "[[sources]\n").expect("write invalid config");

    let app = local_client(&config_dir).await;
    let mut source_client = app.source_client();
    let error = source_client
        .discover_sources(Request::new(DiscoverSourcesRequest {
            workspace: Some(default_workspace()),
        }))
        .await
        .expect_err("corrupted config should surface as an error");
    assert_eq!(error.code(), tonic::Code::Internal);
}

#[cfg(unix)]
#[tokio::test]
async fn import_rolls_back_written_artifacts_when_config_write_fails() {
    use std::os::unix::fs::PermissionsExt;

    let temp = TempDir::new().expect("temp dir");
    let config_dir = temp.path().join("coral-config");
    fs::create_dir_all(&config_dir).expect("create config dir");
    let app = local_client(&config_dir).await;
    let sources_root = config_dir
        .join("workspaces")
        .join("default")
        .join("sources");
    fs::create_dir_all(&sources_root).expect("create sources root");
    fs::set_permissions(&config_dir, fs::Permissions::from_mode(0o500))
        .expect("make config dir read-only");
    let mut source_client = app.source_client();
    let error = source_client
        .import_source(Request::new(ImportSourceRequest {
            workspace: Some(default_workspace()),
            manifest_yaml: fixture_manifest_with_inputs_yaml(),
            variables: vec![SourceVariable {
                key: "API_BASE".to_string(),
                value: "https://example.com".to_string(),
            }],
            secrets: vec![SourceSecret {
                key: "API_TOKEN".to_string(),
                value: "secret-token".to_string(),
            }],
        }))
        .await
        .expect_err("config write should fail");

    fs::set_permissions(&config_dir, fs::Permissions::from_mode(0o700))
        .expect("restore config dir permissions");

    assert_eq!(error.code(), tonic::Code::Internal);
    assert!(
        !config_dir
            .join("workspaces")
            .join("default")
            .join("sources")
            .join("secured_messages")
            .exists()
    );

    let listed = source_client
        .list_sources(Request::new(ListSourcesRequest {
            workspace: Some(default_workspace()),
        }))
        .await
        .expect("list sources after rollback")
        .into_inner();
    assert!(listed.sources.is_empty());
}

#[cfg(unix)]
#[tokio::test]
async fn delete_restores_artifacts_when_manifest_cleanup_fails() {
    use std::os::unix::fs::PermissionsExt;

    let temp = TempDir::new().expect("temp dir");
    let config_dir = temp.path().join("coral-config");
    let app = local_client(&config_dir).await;
    let mut source_client = app.source_client();

    source_client
        .import_source(Request::new(ImportSourceRequest {
            workspace: Some(default_workspace()),
            manifest_yaml: fixture_manifest_with_inputs_yaml(),
            variables: vec![SourceVariable {
                key: "API_BASE".to_string(),
                value: "https://example.com".to_string(),
            }],
            secrets: vec![SourceSecret {
                key: "API_TOKEN".to_string(),
                value: "secret-token".to_string(),
            }],
        }))
        .await
        .expect("import source");

    let sources_root = config_dir
        .join("workspaces")
        .join("default")
        .join("sources");
    let manifest_path = sources_root.join("secured_messages").join("manifest.yaml");
    let secret_path = sources_root.join("secured_messages").join("secrets.env");
    fs::set_permissions(&sources_root, fs::Permissions::from_mode(0o500))
        .expect("make sources dir read-only");

    let error = source_client
        .delete_source(Request::new(DeleteSourceRequest {
            workspace: Some(default_workspace()),
            name: "secured_messages".to_string(),
        }))
        .await
        .expect_err("manifest cleanup should fail");

    fs::set_permissions(&sources_root, fs::Permissions::from_mode(0o700))
        .expect("restore sources dir permissions");

    assert_eq!(error.code(), tonic::Code::Internal);
    assert!(manifest_path.exists(), "manifest should be restored");
    assert!(secret_path.exists(), "secret file should be restored");

    let listed = source_client
        .list_sources(Request::new(ListSourcesRequest {
            workspace: Some(default_workspace()),
        }))
        .await
        .expect("list sources after failed delete")
        .into_inner();
    assert_eq!(listed.sources.len(), 1);
    assert_eq!(listed.sources[0].name, "secured_messages");
}

#[tokio::test]
async fn missing_source_requests_return_not_found() {
    let temp = TempDir::new().expect("temp dir");
    let app = local_client(temp.path().join("coral-config")).await;
    let mut source_client = app.source_client();

    let missing_source = source_client
        .validate_source(Request::new(ValidateSourceRequest {
            workspace: Some(default_workspace()),
            name: "missing".to_string(),
        }))
        .await
        .expect_err("missing source should fail");
    assert_eq!(missing_source.code(), tonic::Code::NotFound);
}

#[tokio::test]
async fn rejects_backslashes_in_workspace_and_source_names() {
    let temp = TempDir::new().expect("temp dir");
    let app = local_client(temp.path().join("coral-config")).await;
    let mut source_client = app.source_client();
    let mut query_client = app.query_client();

    let invalid_workspace = query_client
        .list_tables(Request::new(ListTablesRequest {
            workspace: Some(Workspace {
                name: r"bad\workspace".to_string(),
            }),
        }))
        .await
        .expect_err("workspace with backslash should fail");
    assert_eq!(invalid_workspace.code(), tonic::Code::InvalidArgument);

    let invalid_source_name = source_client
        .validate_source(Request::new(ValidateSourceRequest {
            workspace: Some(default_workspace()),
            name: r"bad\source".to_string(),
        }))
        .await
        .expect_err("source name with backslash should fail");
    assert_eq!(invalid_source_name.code(), tonic::Code::InvalidArgument);
}
