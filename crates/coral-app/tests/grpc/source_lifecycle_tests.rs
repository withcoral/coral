use std::fs;

use coral_api::v1::{
    CreateBundledSourceRequest, DeleteSourceRequest, DiscoverSourcesRequest, ExecuteSqlRequest,
    GetSourceRequest, ImportSourceRequest, ListTablesRequest, QueryTestFailure, QueryTestSuccess,
    SourceOrigin, SourceSecret, SourceVariable, ValidateSourceRequest, Workspace,
    query_test_result,
};
use coral_client::default_workspace;
use tempfile::TempDir;
use tonic::Request;

use crate::harness::{
    FailingHttpFixture, GrpcHarness, fixture_manifest_with_inputs_yaml,
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
    assert_eq!(added.version, "0.1.0");
    assert_eq!(added.origin, SourceOrigin::Imported as i32);
    assert!(added.variables.is_empty());
    assert!(added.secrets.is_empty());

    let config_raw =
        fs::read_to_string(harness.config_dir().join("config.toml")).expect("read config");
    assert!(config_raw.contains("[workspaces.default.sources.local_messages]"));
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
}

#[tokio::test]
async fn import_source_with_secrets_and_variables_get_source_returns_details() {
    let harness = GrpcHarness::new().await;

    let imported = harness
        .import_source(
            fixture_manifest_with_inputs_yaml(),
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
        .into_inner();
    assert_eq!(fetched.name, "secured_messages");
    assert_eq!(fetched.version, "0.1.0");
    assert_eq!(fetched.origin, SourceOrigin::Imported as i32);
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

    let reimported = harness
        .source_client()
        .import_source(Request::new(ImportSourceRequest {
            workspace: Some(default_workspace()),
            manifest_yaml: manifest_yaml.replace("0.1.0", "0.2.0"),
            variables: Vec::new(),
            secrets: Vec::new(),
        }))
        .await
        .expect("duplicate import should overwrite")
        .into_inner();
    assert_eq!(reimported.version, "0.2.0");

    let fetched = harness
        .source_client()
        .get_source(Request::new(GetSourceRequest {
            workspace: Some(default_workspace()),
            name: "local_messages".to_string(),
        }))
        .await
        .expect("get overwritten source")
        .into_inner();
    assert_eq!(fetched.version, "0.2.0");
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
            sql: "SELECT * FROM local_messages.messages".to_string(),
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
    assert_eq!(validated.tables[0].name, "messages");
    assert!(validated.tables[0].required_filters.is_empty());
    assert!(validated.query_tests.is_empty());

    let rows = harness
        .execute_sql_rows("SELECT type, text FROM local_messages.messages ORDER BY text")
        .await;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["text"], "hello");
}

#[tokio::test]
async fn validate_source_returns_query_test_results_without_unary_error() {
    let harness = GrpcHarness::new().await;
    let manifest_yaml = fixture_manifest_with_test_queries_yaml(
        harness.temp_path(),
        &[
            "SELECT COUNT(*) AS n FROM local_messages.messages",
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
                "COPY local_messages.messages TO '{}' STORED AS ARROW",
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
            sql: "CREATE TABLE copied AS SELECT * FROM local_messages.messages".to_string(),
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
async fn validate_source_with_unreachable_api_returns_declared_tables() {
    let harness = GrpcHarness::new().await;
    let failing_http = FailingHttpFixture::new().await;
    harness
        .import_source(failing_http.manifest_yaml(), Vec::new(), Vec::new())
        .await;

    let validated = harness
        .source_client()
        .validate_source(Request::new(ValidateSourceRequest {
            workspace: Some(default_workspace()),
            name: "unreachable_messages".to_string(),
        }))
        .await
        .expect("unreachable source validation should still enumerate tables")
        .into_inner();
    assert_eq!(validated.tables.len(), 1);
    assert_eq!(validated.tables[0].schema_name, "unreachable_messages");
    assert_eq!(validated.tables[0].name, "messages");
    assert!(validated.query_tests.is_empty());
}

#[tokio::test]
async fn validate_source_with_unreachable_api_and_test_queries_returns_query_failures() {
    let harness = GrpcHarness::new().await;
    let failing_http = FailingHttpFixture::new().await;
    harness
        .import_source(
            failing_http
                .manifest_yaml_with_test_queries(&["SELECT * FROM unreachable_messages.messages"]),
            Vec::new(),
            Vec::new(),
        )
        .await;

    let validated = harness.validate_source("unreachable_messages").await;
    assert_eq!(validated.tables.len(), 1);
    assert_eq!(validated.query_tests.len(), 1);
    assert!(matches!(
        &validated.query_tests[0].outcome,
        Some(query_test_result::Outcome::Failure(QueryTestFailure { error_message }))
            if !error_message.is_empty()
    ));
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
async fn validate_source_skipped_registration_returns_unary_failed_precondition() {
    let harness = GrpcHarness::new().await;
    let missing_dir = harness.temp_path().join("missing");
    let manifest_yaml = serde_yaml::to_string(&serde_json::json!({
        "name": "missing_messages",
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "jsonl",
        "tables": [{
            "name": "messages",
            "description": "Missing messages",
            "source": {
                "location": format!("file://{}/", missing_dir.display()),
                "glob": "**/*.jsonl",
            },
            "columns": [
                {"name": "type", "type": "Utf8"},
            ],
        }],
    }))
    .expect("serialize manifest yaml");
    harness
        .import_source(manifest_yaml, Vec::new(), Vec::new())
        .await;

    let error = harness
        .source_client()
        .validate_source(Request::new(ValidateSourceRequest {
            workspace: Some(default_workspace()),
            name: "missing_messages".to_string(),
        }))
        .await
        .expect_err("validation should fail when the source never registers");
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(error.message().contains("is not a directory"));
}

#[tokio::test]
async fn execute_sql_with_unreachable_api_returns_internal_error() {
    let harness = GrpcHarness::new().await;
    let failing_http = FailingHttpFixture::new().await;
    harness
        .import_source(failing_http.manifest_yaml(), Vec::new(), Vec::new())
        .await;

    let error = harness
        .query_client()
        .execute_sql(Request::new(ExecuteSqlRequest {
            workspace: Some(default_workspace()),
            sql: "SELECT * FROM unreachable_messages.messages".to_string(),
        }))
        .await
        .expect_err("unreachable source query should fail");
    assert_eq!(error.code(), tonic::Code::Internal);
}

#[tokio::test]
async fn import_source_missing_required_secret_returns_invalid_argument() {
    let harness = GrpcHarness::new().await;

    let error = harness
        .source_client()
        .import_source(Request::new(ImportSourceRequest {
            workspace: Some(default_workspace()),
            manifest_yaml: fixture_manifest_with_inputs_yaml(),
            variables: vec![SourceVariable {
                key: "API_BASE".to_string(),
                value: "https://example.com".to_string(),
            }],
            secrets: Vec::new(),
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
            manifest_yaml: fixture_manifest_with_required_inputs_yaml(),
            variables: Vec::new(),
            secrets: vec![SourceSecret {
                key: "API_TOKEN".to_string(),
                value: "secret-token".to_string(),
            }],
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
            manifest_yaml: fixture_manifest_with_inputs_yaml(),
            variables: vec![SourceVariable {
                key: "UNUSED".to_string(),
                value: "value".to_string(),
            }],
            secrets: vec![SourceSecret {
                key: "API_TOKEN".to_string(),
                value: "secret-token".to_string(),
            }],
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
            manifest_yaml: fixture_manifest_with_inputs_yaml(),
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
            manifest_yaml: fixture_manifest_with_inputs_yaml(),
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
            manifest_yaml: fixture_manifest_with_inputs_yaml(),
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
async fn discover_bundled_sources_returns_catalog_and_marks_installed_sources() {
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
    assert!(!discovered.is_empty());
    let github = discovered
        .iter()
        .find(|source| source.name == "github")
        .expect("github bundled source");
    assert!(!github.installed);

    harness
        .source_client()
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

    let rediscovered = harness
        .source_client()
        .discover_sources(Request::new(DiscoverSourcesRequest {
            workspace: Some(default_workspace()),
        }))
        .await
        .expect("rediscover sources")
        .into_inner()
        .sources;
    let github = rediscovered
        .iter()
        .find(|source| source.name == "github")
        .expect("github bundled source after install");
    assert!(github.installed);
}

#[tokio::test]
async fn create_bundled_source_registers_tables() {
    let harness = GrpcHarness::new().await;

    harness
        .source_client()
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

    let tables = harness.list_tables().await;
    assert!(
        tables.iter().any(|table| table.schema_name == "github"),
        "github tables should register once the template secret dependency is provided"
    );
}

#[tokio::test]
async fn create_bundled_source_missing_required_secret_returns_invalid_argument() {
    let harness = GrpcHarness::new().await;

    let error = harness
        .source_client()
        .create_bundled_source(Request::new(CreateBundledSourceRequest {
            workspace: Some(default_workspace()),
            name: "sentry".to_string(),
            variables: vec![SourceVariable {
                key: "SENTRY_ORG".to_string(),
                value: "phoebe".to_string(),
            }],
            secrets: Vec::new(),
        }))
        .await
        .expect_err("missing bundled secret should fail");
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert!(
        error
            .message()
            .contains("missing required source secret 'SENTRY_TOKEN'")
    );
}

#[tokio::test]
async fn create_bundled_source_missing_required_variable_returns_invalid_argument() {
    let harness = GrpcHarness::new().await;

    let error = harness
        .source_client()
        .create_bundled_source(Request::new(CreateBundledSourceRequest {
            workspace: Some(default_workspace()),
            name: "sentry".to_string(),
            variables: Vec::new(),
            secrets: vec![SourceSecret {
                key: "SENTRY_TOKEN".to_string(),
                value: "secret-token".to_string(),
            }],
        }))
        .await
        .expect_err("missing bundled variable should fail");
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert!(
        error
            .message()
            .contains("missing required source variable 'SENTRY_ORG'")
    );
}

#[tokio::test]
async fn create_bundled_source_unknown_input_returns_invalid_argument() {
    let harness = GrpcHarness::new().await;

    let error = harness
        .source_client()
        .create_bundled_source(Request::new(CreateBundledSourceRequest {
            workspace: Some(default_workspace()),
            name: "sentry".to_string(),
            variables: vec![
                SourceVariable {
                    key: "SENTRY_ORG".to_string(),
                    value: "phoebe".to_string(),
                },
                SourceVariable {
                    key: "EXTRA".to_string(),
                    value: "value".to_string(),
                },
            ],
            secrets: vec![SourceSecret {
                key: "SENTRY_TOKEN".to_string(),
                value: "secret-token".to_string(),
            }],
        }))
        .await
        .expect_err("unknown bundled input should fail");
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert!(error.message().contains("unknown source variable 'EXTRA'"));
}

#[tokio::test]
async fn create_bundled_source_repeated_secret_returns_invalid_argument() {
    let harness = GrpcHarness::new().await;

    let error = harness
        .source_client()
        .create_bundled_source(Request::new(CreateBundledSourceRequest {
            workspace: Some(default_workspace()),
            name: "sentry".to_string(),
            variables: vec![SourceVariable {
                key: "SENTRY_ORG".to_string(),
                value: "phoebe".to_string(),
            }],
            secrets: vec![
                SourceSecret {
                    key: "SENTRY_TOKEN".to_string(),
                    value: "secret-token".to_string(),
                },
                SourceSecret {
                    key: "SENTRY_TOKEN".to_string(),
                    value: "shadow-token".to_string(),
                },
            ],
        }))
        .await
        .expect_err("repeated bundled secret should fail");
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert!(
        error
            .message()
            .contains("source secret 'SENTRY_TOKEN' is repeated")
    );
}

#[tokio::test]
async fn create_bundled_source_does_not_persist_manifest_to_config_dir() {
    let harness = GrpcHarness::new().await;

    let created = harness
        .source_client()
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
        .expect("create bundled github source")
        .into_inner();

    assert_eq!(created.name, "github");
    assert_eq!(created.origin, SourceOrigin::Bundled as i32);
    assert!(
        !created.version.is_empty(),
        "version should be resolved from the binary"
    );

    // Bundled sources must not persist a manifest.yaml to the config directory;
    // they resolve the manifest from the compiled-in BUNDLED_SOURCES constant.
    let manifest_path = source_dir(harness.config_dir(), "github").join("manifest.yaml");
    assert!(
        !manifest_path.exists(),
        "bundled source should not write manifest.yaml to the config directory"
    );

    // The source should still be fully functional despite no on-disk manifest.
    let tables = harness.list_tables().await;
    assert!(
        tables.iter().any(|table| table.schema_name == "github"),
        "bundled source should register tables resolved from the binary"
    );

    let config_raw =
        fs::read_to_string(harness.config_dir().join("config.toml")).expect("read config");
    assert!(
        !config_raw.contains("version = \""),
        "bundled source config should not persist a version field"
    );
}

#[tokio::test]
async fn validate_bundled_source_missing_required_variable_returns_failed_precondition() {
    let temp = TempDir::new().expect("temp dir");
    let config_dir = temp.path().join("coral-config");
    fs::create_dir_all(&config_dir).expect("create config dir");

    // Simulate a bundled source installed without the required SENTRY_ORG variable.
    // This models the case where the binary is updated to require a new variable
    // that was not provided during the original installation.
    fs::write(
        config_dir.join("config.toml"),
        r#"
version = 1

[workspaces.default.sources.sentry]
variables = {}
secrets = ["SENTRY_TOKEN"]
origin = "bundled"
"#,
    )
    .expect("write config");

    // Write the secret file so the secret store can find it.
    let secret_dir = config_dir
        .join("workspaces")
        .join("default")
        .join("sources")
        .join("sentry");
    fs::create_dir_all(&secret_dir).expect("create secret dir");
    fs::write(secret_dir.join("secrets.env"), "SENTRY_TOKEN=fake-token\n").expect("write secrets");

    let harness = GrpcHarness::start_with_config_dir(config_dir).await;
    let error = harness
        .source_client()
        .validate_source(Request::new(ValidateSourceRequest {
            workspace: Some(default_workspace()),
            name: "sentry".to_string(),
        }))
        .await
        .expect_err("validation should fail when a required variable is missing");
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(
        error.message().contains("missing variable 'SENTRY_ORG'"),
        "error should identify the missing variable, got: {}",
        error.message()
    );
}

#[tokio::test]
async fn validate_bundled_source_missing_required_secret_returns_failed_precondition() {
    let temp = TempDir::new().expect("temp dir");
    let config_dir = temp.path().join("coral-config");
    fs::create_dir_all(&config_dir).expect("create config dir");

    // Simulate a bundled source installed without the required SENTRY_TOKEN secret.
    fs::write(
        config_dir.join("config.toml"),
        r#"
version = 1

[workspaces.default.sources.sentry]
variables = { SENTRY_ORG = "test-org" }
secrets = []
origin = "bundled"
"#,
    )
    .expect("write config");

    let harness = GrpcHarness::start_with_config_dir(config_dir).await;
    let error = harness
        .source_client()
        .validate_source(Request::new(ValidateSourceRequest {
            workspace: Some(default_workspace()),
            name: "sentry".to_string(),
        }))
        .await
        .expect_err("validation should fail when a required secret is missing");
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(
        error.message().contains("missing secret 'SENTRY_TOKEN'"),
        "error should identify the missing secret, got: {}",
        error.message()
    );
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
async fn config_persists_across_rebuilds_without_local_trace_state() {
    let temp = TempDir::new().expect("temp dir");
    let manifest_yaml = fixture_manifest_yaml(temp.path());
    let config_dir = temp.path().join("coral-config");

    {
        let harness = GrpcHarness::start_with_config_dir(config_dir.clone()).await;
        harness
            .import_source(manifest_yaml, Vec::new(), Vec::new())
            .await;
        let rows = harness
            .execute_sql_rows("SELECT COUNT(*) AS n FROM local_messages.messages")
            .await;
        assert_eq!(rows[0]["n"], 2);
    }

    let harness = GrpcHarness::start_with_config_dir(config_dir.clone()).await;
    let listed = harness.list_sources().await;
    assert_eq!(listed.len(), 1);
    let rows = harness
        .execute_sql_rows("SELECT COUNT(*) AS n FROM local_messages.messages")
        .await;
    assert_eq!(rows[0]["n"], 2);
    assert!(
        !config_dir.join("state").join("state.sqlite3").exists(),
        "trace/state sqlite should not be created"
    );
}

#[tokio::test]
async fn corrupted_config_surfaces_internal_error() {
    let temp = TempDir::new().expect("temp dir");
    let config_dir = temp.path().join("coral-config");
    fs::create_dir_all(&config_dir).expect("create config dir");
    fs::write(config_dir.join("config.toml"), "[[sources]\n").expect("write invalid config");

    let harness = GrpcHarness::start_with_config_dir(config_dir).await;
    let error = harness
        .source_client()
        .discover_sources(Request::new(DiscoverSourcesRequest {
            workspace: Some(default_workspace()),
        }))
        .await
        .expect_err("corrupted config should surface as an error");
    assert_eq!(error.code(), tonic::Code::Internal);
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
            fixture_manifest_with_inputs_yaml(),
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
        .list_tables(Request::new(ListTablesRequest {
            workspace: Some(Workspace {
                name: r"bad\workspace".to_string(),
            }),
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
