#![allow(
    clippy::indexing_slicing,
    clippy::string_slice,
    reason = "test code: assertion-style indexing is idiomatic in tests"
)]

use std::fs;

use coral_api::v1::{
    DeleteSourceRequest, DiscoverSourcesRequest, ExecuteSqlRequest, ExplainSqlRequest,
    GetSourceInfoRequest, GetSourceRequest, ImportSourceRequest, QueryTestFailure,
    QueryTestSuccess, SourceCredentialStorage, SourceOrigin, SourceSecret, SourceVariable,
    ValidateSourceRequest, Workspace, import_source_response, query_test_result,
    source_input_spec::Input as ProtoSourceInput,
};
use coral_client::default_workspace;
use tempfile::TempDir;
use tonic::Request;

use crate::harness::{
    GrpcHarness, fixture_function_only_manifest_yaml, fixture_manifest_with_inputs_yaml,
    fixture_manifest_with_required_inputs_yaml, fixture_manifest_with_test_queries_yaml,
    fixture_manifest_yaml, invalid_manifest_yaml, source_dir,
};

#[tokio::test]
async fn import_source_persists_and_lists() {
    let harness = GrpcHarness::new().await;
    let manifest_yaml = fixture_manifest_yaml(harness.temp_path());

    let added = harness
        .import_source(manifest_yaml.clone(), Vec::new(), Vec::new())
        .await;

    assert_eq!(added.name, "local_messages");
    assert!(added.version.is_empty());
    assert_eq!(added.origin, SourceOrigin::Imported as i32);
    assert_eq!(added.interface_ids, vec!["read_files".to_string()]);
    assert_eq!(
        added.credential_storage,
        SourceCredentialStorage::Unspecified as i32
    );
    assert!(added.variables.is_empty());
    assert!(added.secrets.is_empty());

    let config_raw =
        fs::read_to_string(harness.config_dir().join("config.toml")).expect("read config");
    assert!(config_raw.contains("[workspaces.default.sources.local_messages]"));
    assert!(config_raw.contains("secrets = []"));
    assert!(!config_raw.contains("credential_storage"));
    assert!(!config_raw.contains("credential_set_id"));
    assert!(!config_raw.contains("[workspaces.default.credentials"));
    assert!(!config_raw.contains("manifest_yaml = "));
    assert!(!config_raw.contains("manifest_file = "));

    let installed_manifest =
        source_dir(harness.config_dir(), "local_messages").join("manifest.yaml");
    assert_eq!(
        fs::read_to_string(&installed_manifest).expect("read installed manifest"),
        manifest_yaml
    );

    let listed = harness.list_sources().await;
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].name, "local_messages");
    assert_eq!(listed[0].interface_ids, vec!["read_files".to_string()]);
    assert_eq!(
        listed[0].credential_storage,
        SourceCredentialStorage::Unspecified as i32
    );
}

#[tokio::test]
async fn import_source_with_interface_filter_ignores_unselected_required_inputs() {
    let harness = GrpcHarness::new().await;
    let data_file = harness.temp_path().join("events.jsonl");
    fs::write(&data_file, "{\"id\":\"1\",\"text\":\"hello\"}\n").expect("write events fixture");
    let manifest_yaml = format!(
        r"
spec_version: 1
kind: source
name: filtered_messages
description: Filtered messages
inputs:
  - key: API_TOKEN
    kind: secret
interfaces:
  - id: rest
    type: openapi
    url: https://example.com/openapi.json
    auth:
      kind: bearer_input
      key: API_TOKEN
  - id: files
    type: file
    files:
      - {}
    format:
      kind: jsonl
",
        data_file.display()
    );

    let mut stream = harness
        .source_client()
        .import_source(Request::new(ImportSourceRequest {
            workspace: Some(default_workspace()),
            manifest_yaml,
            variables: Vec::new(),
            secrets: Vec::new(),
            oauth_credential_retrievals: Vec::new(),
            interface_ids: vec!["files".to_string()],
        }))
        .await
        .expect("selected file interface import should not require REST credentials")
        .into_inner();
    let imported = stream
        .message()
        .await
        .expect("import stream")
        .and_then(|response| match response.event {
            Some(import_source_response::Event::Source(source)) => Some(source),
            _ => None,
        })
        .expect("import source response");

    assert_eq!(imported.name, "filtered_messages");
    assert_eq!(imported.interface_ids, vec!["files".to_string()]);
    assert!(imported.secrets.is_empty());

    let installed_manifest = fs::read_to_string(
        source_dir(harness.config_dir(), "filtered_messages").join("manifest.yaml"),
    )
    .expect("read installed manifest");
    assert!(installed_manifest.contains("id: files"));
    assert!(!installed_manifest.contains("id: rest"));
    assert!(!installed_manifest.contains("API_TOKEN"));
}

#[tokio::test]
async fn import_source_with_secrets_and_variables_get_source_returns_details() {
    let harness = GrpcHarness::new().await;

    let imported = harness
        .import_source(
            fixture_manifest_with_inputs_yaml(harness.temp_path()),
            vec![SourceVariable {
                key: "API_BASE".to_string(),
                value: "https://example.com".to_string(),
            }],
            vec![SourceSecret {
                key: "API_TOKEN".to_string(),
                value: "secret-token".to_string(),
            }],
        )
        .await;
    assert_eq!(imported.variables.len(), 1);
    assert_eq!(imported.variables[0].key, "API_BASE");
    assert_eq!(imported.variables[0].value, "https://example.com");
    assert_eq!(imported.secrets.len(), 1);
    assert_eq!(imported.secrets[0].key, "API_TOKEN");
    assert!(imported.secrets[0].value.is_empty());

    let fetched = harness
        .source_client()
        .get_source(Request::new(GetSourceRequest {
            workspace: Some(default_workspace()),
            name: "secured_messages".to_string(),
        }))
        .await
        .expect("get source")
        .into_inner()
        .source
        .expect("get source response");
    assert_eq!(fetched.name, "secured_messages");
    assert!(fetched.version.is_empty());
    assert_eq!(fetched.origin, SourceOrigin::Imported as i32);
    assert_eq!(fetched.interface_ids, vec!["read_files".to_string()]);
    assert_eq!(
        fetched.credential_storage,
        SourceCredentialStorage::File as i32
    );
    assert_eq!(fetched.variables, imported.variables);
    assert_eq!(fetched.secrets, imported.secrets);
}

#[tokio::test]
async fn import_duplicate_source_overwrites_existing_source() {
    let harness = GrpcHarness::new().await;
    let manifest_yaml = fixture_manifest_yaml(harness.temp_path());
    harness
        .import_source(manifest_yaml.clone(), Vec::new(), Vec::new())
        .await;

    let mut import_stream = harness
        .source_client()
        .import_source(Request::new(ImportSourceRequest {
            workspace: Some(default_workspace()),
            manifest_yaml: manifest_yaml.replace("Fixture messages", "Updated fixture messages"),
            variables: Vec::new(),
            secrets: Vec::new(),
            oauth_credential_retrievals: Vec::new(),
            interface_ids: Vec::new(),
        }))
        .await
        .expect("duplicate import should overwrite")
        .into_inner();
    let reimported = import_stream
        .message()
        .await
        .expect("duplicate import stream")
        .and_then(|response| match response.event {
            Some(import_source_response::Event::Source(source)) => Some(source),
            _ => None,
        })
        .expect("import source response");
    assert!(reimported.version.is_empty());

    let fetched = harness
        .source_client()
        .get_source(Request::new(GetSourceRequest {
            workspace: Some(default_workspace()),
            name: "local_messages".to_string(),
        }))
        .await
        .expect("get overwritten source")
        .into_inner()
        .source
        .expect("get source response");
    assert!(fetched.version.is_empty());
}

#[tokio::test]
async fn import_invalid_manifest_returns_invalid_argument() {
    let harness = GrpcHarness::new().await;

    let error = harness
        .source_client()
        .import_source(Request::new(ImportSourceRequest {
            workspace: Some(default_workspace()),
            manifest_yaml: invalid_manifest_yaml(),
            variables: Vec::new(),
            secrets: Vec::new(),
            oauth_credential_retrievals: Vec::new(),
            interface_ids: Vec::new(),
        }))
        .await
        .expect_err("invalid manifest should fail");
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn delete_source_removes_from_list_and_disk() {
    let harness = GrpcHarness::new().await;
    let manifest_yaml = fixture_manifest_yaml(harness.temp_path());
    harness
        .import_source(manifest_yaml, Vec::new(), Vec::new())
        .await;

    harness
        .source_client()
        .delete_source(Request::new(DeleteSourceRequest {
            workspace: Some(default_workspace()),
            name: "local_messages".to_string(),
        }))
        .await
        .expect("delete source");

    assert!(harness.list_sources().await.is_empty());
    assert!(!source_dir(harness.config_dir(), "local_messages").exists());

    let query_error = harness
        .query_client()
        .execute_sql(Request::new(ExecuteSqlRequest {
            workspace: Some(default_workspace()),
            sql: "SELECT * FROM local_messages.read_files".to_string(),
        }))
        .await
        .expect_err("query should fail after delete");
    assert!(!query_error.message().is_empty());
}

#[tokio::test]
async fn delete_nonexistent_source_returns_not_found() {
    let harness = GrpcHarness::new().await;

    let error = harness
        .source_client()
        .delete_source(Request::new(DeleteSourceRequest {
            workspace: Some(default_workspace()),
            name: "missing".to_string(),
        }))
        .await
        .expect_err("missing delete should fail");
    assert_eq!(error.code(), tonic::Code::NotFound);
}

#[tokio::test]
async fn validate_source_returns_tables() {
    let harness = GrpcHarness::new().await;
    let manifest_yaml = fixture_manifest_yaml(harness.temp_path());
    harness
        .import_source(manifest_yaml, Vec::new(), Vec::new())
        .await;

    let validated = harness.validate_source("local_messages").await;
    assert_eq!(validated.tables.len(), 1);
    assert_eq!(validated.tables[0].schema_name, "local_messages");
    assert_eq!(validated.tables[0].name, "read_files");
    assert!(validated.tables[0].required_filters.is_empty());
    assert!(validated.query_tests.is_empty());

    let rows = harness
        .execute_sql_rows("SELECT type, text FROM local_messages.read_files ORDER BY text")
        .await;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["text"], "hello");
}

#[tokio::test]
async fn validate_source_omits_unexecutable_upstream_sql_functions() {
    let harness = GrpcHarness::new().await;
    harness
        .import_source(
            fixture_function_only_manifest_yaml(harness.temp_path()),
            Vec::new(),
            Vec::new(),
        )
        .await;

    let validated = harness.validate_source("searchy").await;
    assert!(validated.tables.is_empty());
    assert!(validated.table_functions.is_empty());
    assert!(validated.query_tests.is_empty());
}

#[tokio::test]
async fn explain_sql_returns_logical_and_physical_plans() {
    let harness = GrpcHarness::new().await;
    harness
        .import_source(
            fixture_manifest_yaml(harness.temp_path()),
            Vec::new(),
            Vec::new(),
        )
        .await;

    let response = harness
        .query_client()
        .explain_sql(Request::new(ExplainSqlRequest {
            workspace: Some(default_workspace()),
            sql: "SELECT text FROM local_messages.read_files ORDER BY text".to_string(),
        }))
        .await
        .expect("explain sql")
        .into_inner();
    let plan = response.plan.expect("query plan");

    assert!(
        plan.unoptimized_logical_plan
            .contains("local_messages.read_files")
    );
    assert!(
        plan.optimized_logical_plan
            .contains("local_messages.read_files")
    );
    assert!(plan.physical_plan.contains("Exec"));
}

#[tokio::test]
async fn validate_source_returns_query_test_results_without_unary_error() {
    let harness = GrpcHarness::new().await;
    let manifest_yaml = fixture_manifest_with_test_queries_yaml(
        harness.temp_path(),
        &[
            "SELECT COUNT(*) AS n FROM local_messages.read_files",
            "SELECT * FROM local_messages.missing",
        ],
    );
    harness
        .import_source(manifest_yaml, Vec::new(), Vec::new())
        .await;

    let validated = harness.validate_source("local_messages").await;
    assert_eq!(validated.tables.len(), 1);
    assert_eq!(validated.query_tests.len(), 2);
    assert!(matches!(
        &validated.query_tests[0].outcome,
        Some(query_test_result::Outcome::Success(QueryTestSuccess { row_count })) if *row_count == 1
    ));
    assert!(matches!(
        &validated.query_tests[1].outcome,
        Some(query_test_result::Outcome::Failure(QueryTestFailure { error_message }))
            if !error_message.is_empty()
    ));
}

#[tokio::test]
async fn query_execution_rejects_non_read_only_sql() {
    let harness = GrpcHarness::new().await;
    let manifest_yaml = fixture_manifest_yaml(harness.temp_path());
    harness
        .import_source(manifest_yaml, Vec::new(), Vec::new())
        .await;

    let copy_target = harness.temp_path().join("copied.arrow");
    let copy_error = harness
        .query_client()
        .execute_sql(Request::new(ExecuteSqlRequest {
            workspace: Some(default_workspace()),
            sql: format!(
                "COPY local_messages.read_files TO '{}' STORED AS ARROW",
                copy_target.display()
            ),
        }))
        .await
        .expect_err("COPY TO should be rejected");
    assert_eq!(copy_error.code(), tonic::Code::InvalidArgument);
    assert!(copy_error.message().contains("DML not supported: COPY"));

    let create_error = harness
        .query_client()
        .execute_sql(Request::new(ExecuteSqlRequest {
            workspace: Some(default_workspace()),
            sql: "CREATE TABLE copied AS SELECT * FROM local_messages.read_files".to_string(),
        }))
        .await
        .expect_err("CREATE TABLE should be rejected");
    assert_eq!(create_error.code(), tonic::Code::InvalidArgument);
    assert!(create_error.message().contains("DDL not supported"));

    let set_error = harness
        .query_client()
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
async fn validate_source_with_non_read_only_test_query_returns_stable_query_error() {
    let harness = GrpcHarness::new().await;
    let manifest_yaml = fixture_manifest_with_test_queries_yaml(
        harness.temp_path(),
        &["SET datafusion.execution.batch_size = 1"],
    );
    harness
        .import_source(manifest_yaml, Vec::new(), Vec::new())
        .await;

    let validated = harness.validate_source("local_messages").await;
    assert_eq!(validated.query_tests.len(), 1);
    assert!(matches!(
        &validated.query_tests[0].outcome,
        Some(query_test_result::Outcome::Failure(QueryTestFailure { error_message }))
            if error_message == "test query must be read-only SQL"
    ));
}

#[tokio::test]
async fn import_source_with_missing_file_returns_not_found() {
    let harness = GrpcHarness::new().await;
    let missing_file = harness.temp_path().join("missing").join("messages.jsonl");
    let manifest_yaml = serde_yaml::to_string(&serde_json::json!({
        "spec_version": 1,
        "kind": "source",
        "name": "missing_messages",
        "description": "Missing messages",
        "interfaces": [{
            "id": "files",
            "type": "file",
            "files": [missing_file.display().to_string()],
            "format": {
                "kind": "jsonl",
            },
        }],
    }))
    .expect("serialize manifest yaml");

    let error = harness
        .source_client()
        .import_source(Request::new(ImportSourceRequest {
            workspace: Some(default_workspace()),
            manifest_yaml,
            variables: Vec::new(),
            secrets: Vec::new(),
            oauth_credential_retrievals: Vec::new(),
            interface_ids: Vec::new(),
        }))
        .await
        .expect_err("source add should fail when an explicit file is missing");
    assert_eq!(error.code(), tonic::Code::NotFound);
    assert!(error.message().contains("No such file"));
    assert!(harness.list_sources().await.is_empty());
    assert!(
        !source_dir(harness.config_dir(), "missing_messages")
            .join("manifest.yaml")
            .exists(),
        "failed materialization must not persist a manifest"
    );
}

#[tokio::test]
async fn import_source_missing_required_secret_returns_invalid_argument() {
    let harness = GrpcHarness::new().await;

    let error = harness
        .source_client()
        .import_source(Request::new(ImportSourceRequest {
            workspace: Some(default_workspace()),
            manifest_yaml: fixture_manifest_with_inputs_yaml(harness.temp_path()),
            variables: vec![SourceVariable {
                key: "API_BASE".to_string(),
                value: "https://example.com".to_string(),
            }],
            secrets: Vec::new(),
            oauth_credential_retrievals: Vec::new(),
            interface_ids: Vec::new(),
        }))
        .await
        .expect_err("missing required secret should fail");
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert!(
        error
            .message()
            .contains("missing required source secret 'API_TOKEN'")
    );
}

#[tokio::test]
async fn import_source_missing_required_variable_returns_invalid_argument() {
    let harness = GrpcHarness::new().await;

    let error = harness
        .source_client()
        .import_source(Request::new(ImportSourceRequest {
            workspace: Some(default_workspace()),
            manifest_yaml: fixture_manifest_with_required_inputs_yaml(harness.temp_path()),
            variables: Vec::new(),
            secrets: vec![SourceSecret {
                key: "API_TOKEN".to_string(),
                value: "secret-token".to_string(),
            }],
            oauth_credential_retrievals: Vec::new(),
            interface_ids: Vec::new(),
        }))
        .await
        .expect_err("missing required variable should fail");
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert!(
        error
            .message()
            .contains("missing required source variable 'API_BASE'")
    );
}

#[tokio::test]
async fn import_source_unknown_variable_returns_invalid_argument() {
    let harness = GrpcHarness::new().await;

    let error = harness
        .source_client()
        .import_source(Request::new(ImportSourceRequest {
            workspace: Some(default_workspace()),
            manifest_yaml: fixture_manifest_with_inputs_yaml(harness.temp_path()),
            variables: vec![SourceVariable {
                key: "UNUSED".to_string(),
                value: "value".to_string(),
            }],
            secrets: vec![SourceSecret {
                key: "API_TOKEN".to_string(),
                value: "secret-token".to_string(),
            }],
            oauth_credential_retrievals: Vec::new(),
            interface_ids: Vec::new(),
        }))
        .await
        .expect_err("unknown variable should fail");
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert!(error.message().contains("unknown source variable 'UNUSED'"));
}

#[tokio::test]
async fn import_source_unknown_secret_returns_invalid_argument() {
    let harness = GrpcHarness::new().await;

    let error = harness
        .source_client()
        .import_source(Request::new(ImportSourceRequest {
            workspace: Some(default_workspace()),
            manifest_yaml: fixture_manifest_with_inputs_yaml(harness.temp_path()),
            variables: vec![SourceVariable {
                key: "API_BASE".to_string(),
                value: "https://example.com".to_string(),
            }],
            secrets: vec![
                SourceSecret {
                    key: "API_TOKEN".to_string(),
                    value: "secret-token".to_string(),
                },
                SourceSecret {
                    key: "EXTRA_SECRET".to_string(),
                    value: "unused".to_string(),
                },
            ],
            oauth_credential_retrievals: Vec::new(),
            interface_ids: Vec::new(),
        }))
        .await
        .expect_err("unknown secret should fail");
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert!(
        error
            .message()
            .contains("unknown source secret 'EXTRA_SECRET'")
    );
}

#[tokio::test]
async fn import_source_repeated_variable_returns_invalid_argument() {
    let harness = GrpcHarness::new().await;

    let error = harness
        .source_client()
        .import_source(Request::new(ImportSourceRequest {
            workspace: Some(default_workspace()),
            manifest_yaml: fixture_manifest_with_inputs_yaml(harness.temp_path()),
            variables: vec![
                SourceVariable {
                    key: "API_BASE".to_string(),
                    value: "https://example.com".to_string(),
                },
                SourceVariable {
                    key: "API_BASE".to_string(),
                    value: "https://override.example.com".to_string(),
                },
            ],
            secrets: vec![SourceSecret {
                key: "API_TOKEN".to_string(),
                value: "secret-token".to_string(),
            }],
            oauth_credential_retrievals: Vec::new(),
            interface_ids: Vec::new(),
        }))
        .await
        .expect_err("repeated variable should fail");
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert!(
        error
            .message()
            .contains("source variable 'API_BASE' is repeated")
    );
}

#[tokio::test]
async fn import_source_repeated_secret_returns_invalid_argument() {
    let harness = GrpcHarness::new().await;

    let error = harness
        .source_client()
        .import_source(Request::new(ImportSourceRequest {
            workspace: Some(default_workspace()),
            manifest_yaml: fixture_manifest_with_inputs_yaml(harness.temp_path()),
            variables: vec![SourceVariable {
                key: "API_BASE".to_string(),
                value: "https://example.com".to_string(),
            }],
            secrets: vec![
                SourceSecret {
                    key: "API_TOKEN".to_string(),
                    value: "secret-token".to_string(),
                },
                SourceSecret {
                    key: "API_TOKEN".to_string(),
                    value: "shadow-token".to_string(),
                },
            ],
            oauth_credential_retrievals: Vec::new(),
            interface_ids: Vec::new(),
        }))
        .await
        .expect_err("repeated secret should fail");
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert!(
        error
            .message()
            .contains("source secret 'API_TOKEN' is repeated")
    );
}

#[tokio::test]
async fn discover_sources_returns_active_bundled_source_specs() {
    let harness = GrpcHarness::new().await;

    let discovered = harness
        .source_client()
        .discover_sources(Request::new(DiscoverSourcesRequest {
            workspace: Some(default_workspace()),
        }))
        .await
        .expect("discover sources")
        .into_inner()
        .sources;
    assert!(
        discovered.iter().any(|source| source.name == "github"),
        "expected active bundled github SourceSpec: {discovered:#?}"
    );
}

#[tokio::test]
async fn get_source_info_uses_effective_installed_imported_manifest() {
    let harness = GrpcHarness::new().await;

    harness
        .import_source(
            fixture_manifest_with_inputs_yaml(harness.temp_path()),
            vec![SourceVariable {
                key: "API_BASE".to_string(),
                value: "https://example.com".to_string(),
            }],
            vec![SourceSecret {
                key: "API_TOKEN".to_string(),
                value: "secret-token".to_string(),
            }],
        )
        .await;

    let info = harness
        .source_client()
        .get_source_info(Request::new(GetSourceInfoRequest {
            workspace: Some(default_workspace()),
            name: "secured_messages".to_string(),
            interface_ids: Vec::new(),
            catalog_only: false,
        }))
        .await
        .expect("get source info")
        .into_inner()
        .source_info
        .expect("get source info response");

    assert_eq!(info.name, "secured_messages");
    assert!(info.version.is_empty());
    assert_eq!(info.origin, SourceOrigin::Imported as i32);
    assert!(info.installed);
    assert_eq!(info.interface_ids, vec!["read_files".to_string()]);
    assert_eq!(
        info.credential_storage,
        SourceCredentialStorage::File as i32
    );
    assert_eq!(info.inputs.len(), 2);
    assert_eq!(info.inputs[0].key, "API_BASE");
    match info.inputs[0].input.as_ref().expect("input metadata") {
        ProtoSourceInput::Variable(variable) => {
            assert_eq!(variable.default_value, "https://example.com");
        }
        ProtoSourceInput::Secret(_) => panic!("expected variable input"),
    }
    assert_eq!(info.inputs[1].key, "API_TOKEN");
}

#[tokio::test]
async fn get_nonexistent_source_returns_not_found() {
    let harness = GrpcHarness::new().await;

    let error = harness
        .source_client()
        .get_source(Request::new(GetSourceRequest {
            workspace: Some(default_workspace()),
            name: "missing".to_string(),
        }))
        .await
        .expect_err("missing source should fail");
    assert_eq!(error.code(), tonic::Code::NotFound);
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

    let harness = GrpcHarness::start_with_config_dir(config_dir).await;
    let error = harness
        .source_client()
        .validate_source(Request::new(ValidateSourceRequest {
            workspace: Some(default_workspace()),
            name: "demo".to_string(),
        }))
        .await
        .expect_err("missing manifest file should fail");
    assert_eq!(error.code(), tonic::Code::NotFound);
}

#[tokio::test]
async fn config_persists_across_rebuilds_without_trace_history_state() {
    let temp = TempDir::new().expect("temp dir");
    let manifest_yaml = fixture_manifest_yaml(temp.path());
    let config_dir = temp.path().join("coral-config");
    fs::create_dir_all(&config_dir).expect("create config dir");
    fs::write(
        config_dir.join("config.toml"),
        r"
version = 1

[trace_history]
enabled = false
",
    )
    .expect("write config");

    {
        let harness = GrpcHarness::start_with_config_dir(config_dir.clone()).await;
        harness
            .import_source(manifest_yaml, Vec::new(), Vec::new())
            .await;
        let rows = harness
            .execute_sql_rows("SELECT COUNT(*) AS n FROM local_messages.read_files")
            .await;
        assert_eq!(rows[0]["n"], 2);
    }

    let harness = GrpcHarness::start_with_config_dir(config_dir.clone()).await;
    let listed = harness.list_sources().await;
    assert_eq!(listed.len(), 1);
    let rows = harness
        .execute_sql_rows("SELECT COUNT(*) AS n FROM local_messages.read_files")
        .await;
    assert_eq!(rows[0]["n"], 2);
    assert!(
        !config_dir.join("telemetry").join("traces").exists(),
        "local trace store should not be created"
    );
}

#[tokio::test]
async fn corrupted_config_fails_at_startup() {
    use coral_app::ServerBuilder;

    let temp = TempDir::new().expect("temp dir");
    let config_dir = temp.path().join("coral-config");
    fs::create_dir_all(&config_dir).expect("create config dir");
    fs::write(config_dir.join("config.toml"), "[[sources]\n").expect("write invalid config");

    assert!(
        ServerBuilder::new()
            .with_config_dir(&config_dir)
            .start()
            .await
            .is_err(),
        "corrupted config should prevent server startup"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn import_rolls_back_on_config_write_failure() {
    use std::os::unix::fs::PermissionsExt;

    let temp = TempDir::new().expect("temp dir");
    let config_dir = temp.path().join("coral-config");
    fs::create_dir_all(&config_dir).expect("create config dir");
    let harness = GrpcHarness::start_with_config_dir(config_dir).await;
    let sources_root = harness
        .config_dir()
        .join("workspaces")
        .join("default")
        .join("sources");
    fs::create_dir_all(&sources_root).expect("create sources root");
    fs::set_permissions(harness.config_dir(), fs::Permissions::from_mode(0o500))
        .expect("make config dir read-only");

    let error = harness
        .source_client()
        .import_source(Request::new(ImportSourceRequest {
            workspace: Some(default_workspace()),
            manifest_yaml: fixture_manifest_with_inputs_yaml(harness.temp_path()),
            variables: vec![SourceVariable {
                key: "API_BASE".to_string(),
                value: "https://example.com".to_string(),
            }],
            secrets: vec![SourceSecret {
                key: "API_TOKEN".to_string(),
                value: "secret-token".to_string(),
            }],
            oauth_credential_retrievals: Vec::new(),
            interface_ids: Vec::new(),
        }))
        .await
        .expect_err("config write should fail");

    fs::set_permissions(harness.config_dir(), fs::Permissions::from_mode(0o700))
        .expect("restore config dir permissions");

    assert_eq!(error.code(), tonic::Code::Internal);
    assert!(!source_dir(harness.config_dir(), "secured_messages").exists());
    assert!(harness.list_sources().await.is_empty());
}

#[cfg(unix)]
#[tokio::test]
async fn delete_restores_artifacts_on_cleanup_failure() {
    use std::os::unix::fs::PermissionsExt;

    let harness = GrpcHarness::new().await;
    harness
        .import_source(
            fixture_manifest_with_inputs_yaml(harness.temp_path()),
            vec![SourceVariable {
                key: "API_BASE".to_string(),
                value: "https://example.com".to_string(),
            }],
            vec![SourceSecret {
                key: "API_TOKEN".to_string(),
                value: "secret-token".to_string(),
            }],
        )
        .await;

    let sources_root = harness
        .config_dir()
        .join("workspaces")
        .join("default")
        .join("sources");
    let manifest_path = source_dir(harness.config_dir(), "secured_messages").join("manifest.yaml");
    let secret_path = source_dir(harness.config_dir(), "secured_messages").join("secrets.env");
    fs::set_permissions(&sources_root, fs::Permissions::from_mode(0o500))
        .expect("make sources dir read-only");

    let error = harness
        .source_client()
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

    let listed = harness.list_sources().await;
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].name, "secured_messages");
}

#[tokio::test]
async fn rejects_invalid_workspace_and_source_names() {
    let harness = GrpcHarness::new().await;

    let invalid_workspace = harness
        .query_client()
        .execute_sql(Request::new(ExecuteSqlRequest {
            workspace: Some(Workspace {
                name: r"bad\workspace".to_string(),
            }),
            sql: "SELECT 1".to_string(),
        }))
        .await
        .expect_err("workspace with backslash should fail");
    assert_eq!(invalid_workspace.code(), tonic::Code::InvalidArgument);

    let invalid_source_name = harness
        .source_client()
        .validate_source(Request::new(ValidateSourceRequest {
            workspace: Some(default_workspace()),
            name: r"bad\source".to_string(),
        }))
        .await
        .expect_err("source name with backslash should fail");
    assert_eq!(invalid_source_name.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn adding_source_preserves_otel_config_and_existing_sources() {
    let temp = TempDir::new().expect("temp dir");
    let config_dir = temp.path().join("coral-config");
    fs::create_dir_all(&config_dir).expect("create config dir");

    // Pre-populate the config with an [otel] section and an existing source.
    fs::write(
        config_dir.join("config.toml"),
        r#"version = 1

[otel]
endpoint = "http://localhost:4318"
headers = "from=config"

[trace_history]
enabled = false
retention_days = 3

[workspaces.default.sources.demo]
version = "0.1.0"
variables = {}
secrets = []
origin = "imported"
"#,
    )
    .expect("write initial config");

    let harness = GrpcHarness::start_with_config_dir(config_dir.clone()).await;
    let manifest_yaml = fixture_manifest_yaml(temp.path());

    harness
        .import_source(manifest_yaml, Vec::new(), Vec::new())
        .await;

    let config_raw =
        fs::read_to_string(config_dir.join("config.toml")).expect("read config after import");

    // The [otel] section and its values must survive the round-trip.
    assert!(
        config_raw.contains("[otel]"),
        "otel section should be preserved"
    );
    assert!(
        config_raw.contains("endpoint = \"http://localhost:4318\""),
        "otel endpoint should be preserved"
    );
    assert!(
        config_raw.contains("headers = \"from=config\""),
        "otel headers should be preserved"
    );
    assert!(
        config_raw.contains("[trace_history]"),
        "trace history section should be preserved"
    );
    assert!(
        config_raw.contains("enabled = false"),
        "trace history enabled flag should be preserved"
    );
    assert!(
        config_raw.contains("retention_days = 3"),
        "trace history retention should be preserved"
    );

    // The pre-existing source must still be present.
    assert!(
        config_raw.contains("[workspaces.default.sources.demo]"),
        "pre-existing source should be preserved"
    );

    // The newly imported source must now also be present.
    assert!(
        config_raw.contains("[workspaces.default.sources.local_messages]"),
        "newly added source should be in config"
    );
}
