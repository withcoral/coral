#![allow(
    clippy::indexing_slicing,
    clippy::string_slice,
    reason = "test code: assertion-style indexing is idiomatic in tests"
)]

use std::fs;

use coral_api::v1::{
    ListCatalogRequest, OauthCredentialFlowType, OauthCredentialScopeDelimiter,
    SourceCredentialStorage, SourceOrigin, Workspace, catalog_item, query_test_result,
    source_credential_method::Method as ProtoCredentialMethod,
    source_input_spec::Input as ProtoSourceInput,
};
use tempfile::TempDir;
use tonic::Request;

use crate::harness::{
    FailingHttpFixture, GrpcHarness, assert_pagination, assert_query_test_failure,
    assert_status_contains, assert_table_present, fixture_function_only_manifest_yaml,
    fixture_manifest_with_inputs_yaml, fixture_manifest_with_required_inputs_yaml,
    fixture_manifest_with_test_queries_yaml, fixture_manifest_yaml, invalid_manifest_yaml, page,
    source_dir, source_secret, source_secrets, source_variable, source_variables, sources_root,
    write_source_secrets,
};

async fn create_github_source(harness: &GrpcHarness) -> coral_api::v1::Source {
    harness
        .create_bundled_source(
            "github",
            vec![source_variable("GITHUB_API_BASE", "https://api.github.com")],
            vec![source_secret("GITHUB_TOKEN", "fake-token")],
        )
        .await
}

fn assert_contains_all(output: &str, expected: &[&str]) {
    for item in expected {
        assert!(output.contains(item), "missing {item:?}: {output}");
    }
}

fn assert_contains_none(output: &str, unexpected: &[&str]) {
    for item in unexpected {
        assert!(!output.contains(item), "unexpected {item:?}: {output}");
    }
}

struct InputValidationCase {
    variables: &'static [(&'static str, &'static str)],
    secrets: &'static [(&'static str, &'static str)],
    message: &'static str,
}

async fn assert_import_input_error(manifest_yaml: fn() -> String, case: &InputValidationCase) {
    let harness = GrpcHarness::new().await;
    let error = harness
        .import_source_error(
            manifest_yaml(),
            source_variables(case.variables),
            source_secrets(case.secrets),
        )
        .await;
    assert_status_contains(&error, tonic::Code::InvalidArgument, case.message);
}

async fn assert_bundled_input_error(source_name: &str, case: &InputValidationCase) {
    let harness = GrpcHarness::new().await;
    let error = harness
        .create_bundled_source_error(
            source_name,
            source_variables(case.variables),
            source_secrets(case.secrets),
        )
        .await;
    assert_status_contains(&error, tonic::Code::InvalidArgument, case.message);
}

#[tokio::test]
async fn import_source_persists_and_lists() {
    let harness = GrpcHarness::new().await;
    let manifest_yaml = fixture_manifest_yaml(harness.temp_path());

    let added = harness
        .import_source_without_inputs(manifest_yaml.clone())
        .await;

    assert_eq!(added.name, "local_messages");
    assert_eq!(added.version, "0.1.0");
    assert_eq!(added.origin, SourceOrigin::Imported as i32);
    assert_eq!(
        added.credential_storage,
        SourceCredentialStorage::Unspecified as i32
    );
    assert!(added.variables.is_empty());
    assert!(added.secrets.is_empty());

    let config_raw = harness.config_raw();
    assert_contains_all(
        &config_raw,
        &[
            "[workspaces.default.sources.local_messages]",
            "secrets = []",
        ],
    );
    assert_contains_none(
        &config_raw,
        &[
            "credential_storage",
            "credential_set_id",
            "[workspaces.default.credentials",
            "manifest_yaml = ",
            "manifest_file = ",
        ],
    );

    let installed_manifest =
        source_dir(harness.config_dir(), "local_messages").join("manifest.yaml");
    assert_eq!(
        fs::read_to_string(&installed_manifest).expect("read installed manifest"),
        manifest_yaml
    );

    let listed = harness.list_sources().await;
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].name, "local_messages");
    assert_eq!(
        listed[0].credential_storage,
        SourceCredentialStorage::Unspecified as i32
    );
}

#[tokio::test]
async fn import_source_with_secrets_and_variables_get_source_returns_details() {
    let harness = GrpcHarness::new().await;

    let imported = harness.import_secured_messages_source().await;
    assert_eq!(imported.variables.len(), 1);
    assert_eq!(imported.variables[0].key, "API_BASE");
    assert_eq!(imported.variables[0].value, "https://example.com");
    assert_eq!(imported.secrets.len(), 1);
    assert_eq!(imported.secrets[0].key, "API_TOKEN");
    assert!(imported.secrets[0].value.is_empty());

    let fetched = harness.get_source("secured_messages").await;
    assert_eq!(fetched.name, "secured_messages");
    assert_eq!(fetched.version, "0.1.0");
    assert_eq!(fetched.origin, SourceOrigin::Imported as i32);
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
        .import_source_without_inputs(manifest_yaml.clone())
        .await;

    let reimported = harness
        .import_source_without_inputs(manifest_yaml.replace("0.1.0", "0.2.0"))
        .await;
    assert_eq!(reimported.version, "0.2.0");

    let fetched = harness.get_source("local_messages").await;
    assert_eq!(fetched.version, "0.2.0");
}

#[tokio::test]
async fn import_invalid_manifest_returns_invalid_argument() {
    let harness = GrpcHarness::new().await;

    let error = harness
        .import_source_error(invalid_manifest_yaml(), Vec::new(), Vec::new())
        .await;
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn delete_source_removes_from_list_and_disk() {
    let harness = GrpcHarness::new().await;
    harness.import_local_messages_source().await;

    harness.delete_source("local_messages").await;

    assert!(harness.list_sources().await.is_empty());
    assert!(!source_dir(harness.config_dir(), "local_messages").exists());

    let query_error = harness
        .execute_sql_error("SELECT * FROM local_messages.messages")
        .await;
    assert!(!query_error.message().is_empty());
}

#[tokio::test]
async fn delete_nonexistent_source_returns_not_found() {
    let harness = GrpcHarness::new().await;

    let error = harness.delete_source_error("missing").await;
    assert_eq!(error.code(), tonic::Code::NotFound);
}

#[tokio::test]
async fn validate_source_returns_tables() {
    let harness = GrpcHarness::new().await;
    harness.import_local_messages_source().await;

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
async fn validate_source_returns_table_functions() {
    let harness = GrpcHarness::new().await;
    harness
        .import_source_without_inputs(fixture_function_only_manifest_yaml())
        .await;

    let validated = harness.validate_source("searchy").await;
    assert!(validated.tables.is_empty());
    assert_eq!(validated.table_functions.len(), 1);
    let function = &validated.table_functions[0];
    assert_eq!(function.schema_name, "searchy");
    assert_eq!(function.name, "search_issues");
    assert_eq!(function.arguments.len(), 1);
    assert_eq!(function.arguments[0].name, "q");
    assert!(function.arguments[0].required);
    assert_eq!(function.result_columns.len(), 1);
    assert_eq!(function.result_columns[0].name, "title");
    assert!(validated.query_tests.is_empty());
}

#[tokio::test]
async fn list_catalog_supports_table_kind_and_pagination() {
    let harness = GrpcHarness::new().await;
    harness.import_multiple_table_messages_source().await;

    let catalog_page = harness
        .list_catalog("local_messages", 1, Some(page(2, 0)))
        .await;
    assert_pagination(catalog_page.pagination, 3, 2, 0, true);
    let counts = catalog_page.counts.as_ref().expect("catalog counts");
    assert_eq!(counts.table_count, 3);
    assert_eq!(counts.table_function_count, 0);
    assert_eq!(
        catalog_page
            .items
            .iter()
            .filter_map(|item| match item.item.as_ref().expect("catalog item") {
                catalog_item::Item::Table(table) => Some(table.name.as_str()),
                catalog_item::Item::TableFunction(_) => None,
            })
            .collect::<Vec<_>>(),
        vec!["events", "messages"]
    );

    let unknown_schema = harness.list_catalog("missing", 1, Some(page(2, 0))).await;
    assert_pagination(unknown_schema.pagination, 0, 2, 0, false);
    let unknown_counts = unknown_schema.counts.as_ref().expect("catalog counts");
    assert_eq!(unknown_counts.table_count, 0);
    assert_eq!(unknown_counts.table_function_count, 0);
    assert!(unknown_schema.items.is_empty());
}

#[tokio::test]
async fn explain_sql_returns_logical_and_physical_plans() {
    let harness = GrpcHarness::new().await;
    harness.import_local_messages_source().await;

    let plan = harness
        .explain_sql("SELECT text FROM local_messages.messages ORDER BY text")
        .await;

    for logical_plan in [&plan.unoptimized_logical_plan, &plan.optimized_logical_plan] {
        assert!(logical_plan.contains("local_messages.messages"));
    }
    assert!(plan.physical_plan.contains("Exec"));
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
    harness.import_source_without_inputs(manifest_yaml).await;

    let validated = harness.validate_source("local_messages").await;
    assert_eq!(validated.tables.len(), 1);
    assert_eq!(validated.query_tests.len(), 2);
    assert!(matches!(
        &validated.query_tests[0].outcome,
        Some(query_test_result::Outcome::Success(success)) if success.row_count == 1
    ));
    assert_query_test_failure(&validated.query_tests[1], None);
}

#[tokio::test]
async fn query_execution_rejects_non_read_only_sql() {
    let harness = GrpcHarness::new().await;
    harness.import_local_messages_source().await;

    let copy_target = harness.temp_path().join("copied.arrow");
    for (sql, message) in [
        (
            format!(
                "COPY local_messages.messages TO '{}' STORED AS ARROW",
                copy_target.display()
            ),
            "DML not supported: COPY",
        ),
        (
            "CREATE TABLE copied AS SELECT * FROM local_messages.messages".to_string(),
            "DDL not supported",
        ),
        (
            "SET datafusion.execution.batch_size = 1".to_string(),
            "Statement not supported",
        ),
    ] {
        let error = harness.execute_sql_error(sql).await;
        assert_status_contains(&error, tonic::Code::InvalidArgument, message);
    }
}

#[tokio::test]
async fn validate_source_with_unreachable_api_returns_declared_tables_and_query_failures() {
    for (case, test_queries, expect_query_failure) in [
        ("no test queries", &[][..], false),
        (
            "with test query",
            &["SELECT * FROM unreachable_messages.messages"][..],
            true,
        ),
    ] {
        let harness = GrpcHarness::new().await;
        let failing_http = FailingHttpFixture::new().await;
        harness
            .import_source_without_inputs(
                failing_http.manifest_yaml_with_test_queries(test_queries),
            )
            .await;

        let validated = harness.validate_source("unreachable_messages").await;
        assert_eq!(validated.tables.len(), 1, "{case}");
        assert_eq!(
            validated.tables[0].schema_name, "unreachable_messages",
            "{case}"
        );
        assert_eq!(validated.tables[0].name, "messages", "{case}");
        if expect_query_failure {
            assert_eq!(validated.query_tests.len(), 1, "{case}");
            assert_query_test_failure(&validated.query_tests[0], None);
        } else {
            assert!(validated.query_tests.is_empty(), "{case}");
        }
    }
}

#[tokio::test]
async fn validate_source_with_non_read_only_test_query_returns_stable_query_error() {
    let harness = GrpcHarness::new().await;
    let manifest_yaml = fixture_manifest_with_test_queries_yaml(
        harness.temp_path(),
        &["SET datafusion.execution.batch_size = 1"],
    );
    harness.import_source_without_inputs(manifest_yaml).await;

    let validated = harness.validate_source("local_messages").await;
    assert_eq!(validated.query_tests.len(), 1);
    assert_query_test_failure(
        &validated.query_tests[0],
        Some("test query must be read-only SQL"),
    );
}

#[tokio::test]
async fn validate_source_skipped_registration_returns_unary_failed_precondition() {
    let harness = GrpcHarness::new().await;
    let missing_dir = harness.temp_path().join("missing");
    let manifest_yaml = serde_yaml::to_string(&serde_json::json!({
        "name": "missing_messages",
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "file",
        "tables": [{
            "name": "messages",
            "description": "Missing messages",
            "format": "jsonl",
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
    harness.import_source_without_inputs(manifest_yaml).await;

    let error = harness.validate_source_error("missing_messages").await;
    assert_status_contains(
        &error,
        tonic::Code::FailedPrecondition,
        "is not a directory",
    );
}

#[tokio::test]
async fn execute_sql_with_unreachable_api_returns_unavailable_error() {
    let harness = GrpcHarness::new().await;
    let failing_http = FailingHttpFixture::new().await;
    harness
        .import_source_without_inputs(failing_http.manifest_yaml())
        .await;

    let error = harness
        .execute_sql_error("SELECT * FROM unreachable_messages.messages")
        .await;
    assert_eq!(error.code(), tonic::Code::Unavailable);
}

#[tokio::test]
async fn import_source_input_validation_errors_return_invalid_argument() {
    for (manifest_yaml, case) in [
        (
            fixture_manifest_with_inputs_yaml as fn() -> String,
            InputValidationCase {
                variables: &[("API_BASE", "https://example.com")],
                secrets: &[],
                message: "missing required source secret 'API_TOKEN'",
            },
        ),
        (
            fixture_manifest_with_required_inputs_yaml,
            InputValidationCase {
                variables: &[],
                secrets: &[("API_TOKEN", "secret-token")],
                message: "missing required source variable 'API_BASE'",
            },
        ),
        (
            fixture_manifest_with_inputs_yaml,
            InputValidationCase {
                variables: &[("UNUSED", "value")],
                secrets: &[("API_TOKEN", "secret-token")],
                message: "unknown source variable 'UNUSED'",
            },
        ),
        (
            fixture_manifest_with_inputs_yaml,
            InputValidationCase {
                variables: &[("API_BASE", "https://example.com")],
                secrets: &[("API_TOKEN", "secret-token"), ("EXTRA_SECRET", "unused")],
                message: "unknown source secret 'EXTRA_SECRET'",
            },
        ),
        (
            fixture_manifest_with_inputs_yaml,
            InputValidationCase {
                variables: &[
                    ("API_BASE", "https://example.com"),
                    ("API_BASE", "https://override.example.com"),
                ],
                secrets: &[("API_TOKEN", "secret-token")],
                message: "source variable 'API_BASE' is repeated",
            },
        ),
        (
            fixture_manifest_with_inputs_yaml,
            InputValidationCase {
                variables: &[("API_BASE", "https://example.com")],
                secrets: &[("API_TOKEN", "secret-token"), ("API_TOKEN", "shadow-token")],
                message: "source secret 'API_TOKEN' is repeated",
            },
        ),
    ] {
        assert_import_input_error(manifest_yaml, &case).await;
    }
}

#[tokio::test]
async fn discover_bundled_sources_returns_catalog_and_marks_installed_sources() {
    let harness = GrpcHarness::new().await;

    let discovered = harness.discover_sources().await;
    assert!(!discovered.is_empty());
    let github = discovered
        .iter()
        .find(|source| source.name == "github")
        .expect("github bundled source");
    assert!(!github.installed);

    create_github_source(&harness).await;

    let rediscovered = harness.discover_sources().await;
    let github = rediscovered
        .iter()
        .find(|source| source.name == "github")
        .expect("github bundled source after install");
    assert!(github.installed);
}

#[tokio::test]
async fn get_source_info_returns_available_bundled_metadata() {
    let harness = GrpcHarness::new().await;

    let info = harness.get_source_info("github").await;

    assert_eq!(info.name, "github");
    assert_eq!(info.origin, SourceOrigin::Bundled as i32);
    assert!(!info.installed);
    assert_eq!(
        info.credential_storage,
        SourceCredentialStorage::Unspecified as i32
    );
    assert!(!info.description.is_empty());
    assert!(!info.version.is_empty());
    assert!(
        info.inputs.iter().any(|input| input.key == "GITHUB_TOKEN"),
        "expected bundled manifest inputs"
    );
}

#[tokio::test]
async fn get_source_info_returns_sentry_oauth_credential_metadata() {
    let harness = GrpcHarness::new().await;

    let info = harness.get_source_info("sentry").await;

    let token = info
        .inputs
        .iter()
        .find(|input| input.key == "SENTRY_TOKEN")
        .expect("SENTRY_TOKEN input");
    let secret = match token.input.as_ref().expect("input metadata") {
        ProtoSourceInput::Secret(secret) => secret,
        ProtoSourceInput::Variable(_) => panic!("expected secret input"),
    };
    let credential = secret.credential.as_ref().expect("credential metadata");
    assert_eq!(credential.methods.len(), 2);

    let oauth_method = &credential.methods[0];
    let oauth = match oauth_method.method.as_ref().expect("oauth method") {
        ProtoCredentialMethod::Oauth(oauth) => oauth,
        ProtoCredentialMethod::SourceConfig(_) => panic!("expected oauth method"),
    };
    assert_eq!(oauth.flow(), OauthCredentialFlowType::DeviceCode);
    assert!(oauth.redirect_uri.is_empty());
    let endpoints = oauth.endpoints.as_ref().expect("oauth endpoints");
    assert_eq!(
        endpoints.device_authorization_url,
        "https://sentry.io/oauth/device/code/"
    );
    assert!(endpoints.authorization_url.is_empty());
    assert_eq!(endpoints.token_url, "https://sentry.io/oauth/token/");
    let client = oauth.client.as_ref().expect("oauth client");
    assert_eq!(
        client.id.as_ref().expect("oauth client id").input,
        "SENTRY_OAUTH_CLIENT_ID"
    );
    assert!(client.secret.is_none());
    let scope = oauth
        .scopes
        .as_ref()
        .expect("oauth scopes")
        .scope
        .as_ref()
        .expect("oauth scope");
    assert_eq!(scope.delimiter(), OauthCredentialScopeDelimiter::Space);
    assert_eq!(
        scope.values,
        vec![
            "org:read".to_string(),
            "event:read".to_string(),
            "member:read".to_string(),
            "project:read".to_string(),
            "project:releases".to_string(),
            "team:read".to_string()
        ]
    );

    assert!(matches!(
        credential.methods[1].method.as_ref(),
        Some(ProtoCredentialMethod::SourceConfig(_))
    ));
}

#[tokio::test]
async fn get_source_info_uses_effective_installed_imported_manifest() {
    let harness = GrpcHarness::new().await;

    harness.import_secured_messages_source().await;

    let info = harness.get_source_info("secured_messages").await;

    assert_eq!(info.name, "secured_messages");
    assert_eq!(info.version, "0.1.0");
    assert_eq!(info.origin, SourceOrigin::Imported as i32);
    assert!(info.installed);
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
async fn create_bundled_source_input_validation_errors_return_invalid_argument() {
    for case in [
        InputValidationCase {
            variables: &[("SENTRY_ORG", "phoebe")],
            secrets: &[],
            message: "missing required source secret 'SENTRY_TOKEN'",
        },
        InputValidationCase {
            variables: &[],
            secrets: &[("SENTRY_TOKEN", "secret-token")],
            message: "missing required source variable 'SENTRY_ORG'",
        },
        InputValidationCase {
            variables: &[("SENTRY_ORG", "phoebe"), ("EXTRA", "value")],
            secrets: &[("SENTRY_TOKEN", "secret-token")],
            message: "unknown source variable 'EXTRA'",
        },
        InputValidationCase {
            variables: &[("SENTRY_ORG", "phoebe")],
            secrets: &[
                ("SENTRY_TOKEN", "secret-token"),
                ("SENTRY_TOKEN", "shadow-token"),
            ],
            message: "source secret 'SENTRY_TOKEN' is repeated",
        },
    ] {
        assert_bundled_input_error("sentry", &case).await;
    }
}

#[tokio::test]
async fn create_bundled_source_does_not_persist_manifest_to_config_dir() {
    let harness = GrpcHarness::new().await;

    let created = create_github_source(&harness).await;

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
    assert_table_present(&harness.list_tables().await, "github");

    assert_contains_none(&harness.config_raw(), &["version = \""]);
}

#[tokio::test]
async fn validate_bundled_source_missing_required_inputs_returns_failed_precondition() {
    for (raw_config, secret_file, expected) in [
        (
            r#"
version = 1

[workspaces.default.sources.sentry]
variables = {}
secrets = ["SENTRY_TOKEN"]
origin = "bundled"
"#,
            Some("SENTRY_TOKEN=fake-token\n"),
            "missing variable 'SENTRY_ORG'",
        ),
        (
            r#"
version = 1

[workspaces.default.sources.sentry]
variables = { SENTRY_ORG = "test-org" }
secrets = []
origin = "bundled"
"#,
            None,
            "missing secret 'SENTRY_TOKEN'",
        ),
    ] {
        let harness = GrpcHarness::start_with_config(raw_config).await;
        if let Some(raw) = secret_file {
            write_source_secrets(harness.config_dir(), "sentry", raw);
        }

        let error = harness.validate_source_error("sentry").await;
        assert_status_contains(&error, tonic::Code::FailedPrecondition, expected);
    }
}

#[tokio::test]
async fn get_nonexistent_source_returns_not_found() {
    let harness = GrpcHarness::new().await;

    let error = harness.get_source_error("missing").await;
    assert_eq!(error.code(), tonic::Code::NotFound);
}

#[tokio::test]
async fn missing_source_manifest_file_returns_not_found() {
    let harness = GrpcHarness::start_with_config(
        r#"
version = 1

[workspaces.default.sources.demo]
version = "0.1.0"
origin = "imported"
"#,
    )
    .await;

    let error = harness.validate_source_error("demo").await;
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
        harness.import_source_without_inputs(manifest_yaml).await;
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
        !config_dir.join("telemetry").join("traces").exists(),
        "local trace store should not be created"
    );
}

#[tokio::test]
async fn corrupted_config_fails_at_startup() {
    use coral_client::local::ServerBuilder;

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

    let harness = GrpcHarness::new().await;
    let sources_root = sources_root(harness.config_dir());
    fs::create_dir_all(&sources_root).expect("create sources root");
    fs::set_permissions(harness.config_dir(), fs::Permissions::from_mode(0o500))
        .expect("make config dir read-only");

    let error = harness
        .import_source_error(
            fixture_manifest_with_inputs_yaml(),
            vec![source_variable("API_BASE", "https://example.com")],
            vec![source_secret("API_TOKEN", "secret-token")],
        )
        .await;

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
    harness.import_secured_messages_source().await;

    let sources_root = sources_root(harness.config_dir());
    let manifest_path = source_dir(harness.config_dir(), "secured_messages").join("manifest.yaml");
    let secret_path = source_dir(harness.config_dir(), "secured_messages").join("secrets.env");
    fs::set_permissions(&sources_root, fs::Permissions::from_mode(0o500))
        .expect("make sources dir read-only");

    let error = harness.delete_source_error("secured_messages").await;

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
        .catalog_client()
        .list_catalog(Request::new(ListCatalogRequest {
            workspace: Some(Workspace {
                name: r"bad\workspace".to_string(),
            }),
            kind: 1,
            ..Default::default()
        }))
        .await
        .expect_err("workspace with backslash should fail");
    assert_eq!(invalid_workspace.code(), tonic::Code::InvalidArgument);

    let invalid_source_name = harness.validate_source_error(r"bad\source").await;
    assert_eq!(invalid_source_name.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn adding_source_preserves_otel_config_and_existing_sources() {
    // Pre-populate the config with an [otel] section and an existing source.
    let harness = GrpcHarness::start_with_config(
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
    .await;

    harness.import_local_messages_source().await;

    // The [otel] section and its values must survive the round-trip.
    assert_contains_all(
        &harness.config_raw(),
        &[
            "[otel]",
            "endpoint = \"http://localhost:4318\"",
            "headers = \"from=config\"",
            "[trace_history]",
            "enabled = false",
            "retention_days = 3",
            "[workspaces.default.sources.demo]",
            "[workspaces.default.sources.local_messages]",
        ],
    );
}
