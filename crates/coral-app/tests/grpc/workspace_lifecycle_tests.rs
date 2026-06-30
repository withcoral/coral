use std::fs;

use coral_api::CORAL_ERROR_REASON_WORKSPACE_NOT_FOUND;
use coral_api::v1::{
    CreateWorkspaceRequest, DeleteWorkspaceRequest, DeleteWorkspaceResponse, GetCurrentUserRequest,
    ImportSourceRequest, ListSourcesRequest, ListUsersRequest, ListWorkspaceMembersRequest,
    ListWorkspaceMembersResponse, ListWorkspacesRequest, Source, SourceSecret, SourceVariable,
    ValidateSourceRequest, WorkspaceRole, import_source_response,
};
use coral_client::AppClient;
use prost::Message as _;
use serde_json::json;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use tempfile::TempDir;
use tonic::{Code, Request, Response, Status};

use crate::harness::{
    GrpcHarness, SharedDeployment, TEST_ISSUER, add_member, concealed_refusal, create_workspace,
    fixture_manifest_with_inputs_yaml, fixture_manifest_yaml, membership_rows,
    named_workspace as workspace, remove_member,
};

static TRACE_STORE_DELETE_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn workspace_names(harness: &GrpcHarness) -> Vec<String> {
    harness
        .workspace_client()
        .list_workspaces(Request::new(ListWorkspacesRequest {}))
        .await
        .expect("list workspaces")
        .into_inner()
        .memberships
        .into_iter()
        .map(|membership| membership.workspace.expect("listed workspace").name)
        .collect()
}

async fn import_source_in_workspace(
    harness: &GrpcHarness,
    workspace_name: &str,
    manifest_yaml: String,
    variables: Vec<SourceVariable>,
    secrets: Vec<SourceSecret>,
) -> Source {
    let mut stream = harness
        .source_client()
        .import_source(Request::new(ImportSourceRequest {
            workspace: Some(workspace(workspace_name)),
            manifest_yaml,
            variables,
            secrets,
            oauth_credential_retrievals: Vec::new(),
        }))
        .await
        .expect("import source")
        .into_inner();
    stream
        .message()
        .await
        .expect("import source stream")
        .and_then(|response| match response.event {
            Some(import_source_response::Event::Source(source)) => Some(source),
            _ => None,
        })
        .expect("import source response")
}

async fn seed_stale_workspace_row(config_dir: &std::path::Path, workspace_name: &str) {
    fs::create_dir_all(config_dir).expect("create config dir");
    let options = SqliteConnectOptions::new()
        .filename(config_dir.join("coral.db"))
        .create_if_missing(true);
    let pool = SqlitePoolOptions::new()
        .connect_with(options)
        .await
        .expect("open stale shadow db");
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS workspaces (
            id TEXT NOT NULL PRIMARY KEY,
            created_at_unix_nanos BIGINT NOT NULL
        )",
    )
    .execute(&pool)
    .await
    .expect("create workspaces table");
    sqlx::query("INSERT INTO workspaces (id, created_at_unix_nanos) VALUES (?, ?)")
        .bind(workspace_name)
        .bind(1_i64)
        .execute(&pool)
        .await
        .expect("insert stale workspace row");
    pool.close().await;
}

#[cfg(unix)]
fn find_workspace_delete_backup(
    config_dir: &std::path::Path,
    workspace_name: &str,
) -> std::path::PathBuf {
    let prefix = format!("{workspace_name}.delete.rollback.");
    fs::read_dir(config_dir.join("deleted-workspaces"))
        .expect("read deleted workspaces dir")
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .find(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with(&prefix))
        })
        .expect("workspace delete backup dir")
}

/// Every entry a scan of the workspaces root finds is read as a live
/// workspace, so a deletion must leave none of its own behind there — the
/// staged directory included, wherever its removal got to.
fn workspace_root_entries(config_dir: &std::path::Path) -> Vec<String> {
    match fs::read_dir(config_dir.join("workspaces")) {
        Ok(entries) => entries
            .filter_map(Result::ok)
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .collect(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(error) => panic!("read workspaces dir: {error}"),
    }
}

fn local_trace_store_dir(harness: &GrpcHarness) -> std::path::PathBuf {
    harness
        .local_trace_store_dir()
        .expect("trace history should be enabled")
        .to_path_buf()
}

#[tokio::test]
async fn lists_no_workspace_when_config_is_missing() {
    let harness = GrpcHarness::new().await;

    assert!(
        workspace_names(&harness).await.is_empty(),
        "a fresh install owns no workspace until someone creates one",
    );
}

#[tokio::test]
async fn startup_cutover_removes_stale_shadow_workspace_rows() {
    let temp = TempDir::new().expect("temp dir");
    let config_dir = temp.path().join("coral-config");
    seed_stale_workspace_row(&config_dir, "stale").await;

    let harness = GrpcHarness::start_with_config_dir(config_dir).await;

    assert!(
        workspace_names(&harness).await.is_empty(),
        "the cutover keeps what legacy config held and invents nothing",
    );
}

#[tokio::test]
async fn create_workspace_persists_database_row_without_config_scaffolding() {
    let harness = GrpcHarness::new().await;

    let created = harness
        .workspace_client()
        .create_workspace(Request::new(CreateWorkspaceRequest {
            workspace: Some(workspace("work")),
        }))
        .await
        .expect("create workspace")
        .into_inner()
        .workspace
        .expect("create workspace response");

    assert_eq!(created.name, "work");
    assert_eq!(workspace_names(&harness).await, vec!["work"]);

    let raw = fs::read_to_string(harness.config_dir().join("config.toml")).expect("read config");
    assert!(
        !raw.contains("[workspaces.work]"),
        "created empty workspace should not persist config scaffolding: {raw}"
    );

    let sources = harness
        .source_client()
        .list_sources(Request::new(ListSourcesRequest {
            workspace: Some(workspace("work")),
        }))
        .await
        .expect("list sources in empty workspace")
        .into_inner()
        .sources;
    assert!(sources.is_empty());
}

#[tokio::test]
async fn list_workspaces_reads_database_after_config_becomes_invalid() {
    let harness = GrpcHarness::new().await;

    harness
        .workspace_client()
        .create_workspace(Request::new(CreateWorkspaceRequest {
            workspace: Some(workspace("work")),
        }))
        .await
        .expect("create workspace");
    fs::write(harness.config_dir().join("config.toml"), "[[workspaces]\n").expect("corrupt config");

    assert_eq!(workspace_names(&harness).await, vec!["work"]);
}

#[tokio::test]
async fn list_sources_uses_database_workspace_after_config_entry_is_missing() {
    let harness = GrpcHarness::new().await;

    harness
        .workspace_client()
        .create_workspace(Request::new(CreateWorkspaceRequest {
            workspace: Some(workspace("work")),
        }))
        .await
        .expect("create workspace");
    fs::write(
        harness.config_dir().join("config.toml"),
        "[credentials]\nstorage = \"file\"\n",
    )
    .expect("remove workspace compatibility entry");

    let sources = harness
        .source_client()
        .list_sources(Request::new(ListSourcesRequest {
            workspace: Some(workspace("work")),
        }))
        .await
        .expect("db workspace should authorize source catalog read")
        .into_inner()
        .sources;
    assert!(sources.is_empty());
}

#[tokio::test]
async fn create_duplicate_workspace_checks_database_not_config() {
    let harness = GrpcHarness::new().await;

    harness
        .workspace_client()
        .create_workspace(Request::new(CreateWorkspaceRequest {
            workspace: Some(workspace("work")),
        }))
        .await
        .expect("create workspace");
    fs::write(
        harness.config_dir().join("config.toml"),
        "[credentials]\nstorage = \"file\"\n",
    )
    .expect("remove workspace compatibility entry");

    let error = harness
        .workspace_client()
        .create_workspace(Request::new(CreateWorkspaceRequest {
            workspace: Some(workspace("work")),
        }))
        .await
        .expect_err("duplicate workspace should be rejected from database state");
    assert_eq!(error.code(), tonic::Code::AlreadyExists);
}

#[tokio::test]
async fn create_duplicate_workspace_returns_already_exists() {
    let harness = GrpcHarness::new().await;

    harness
        .workspace_client()
        .create_workspace(Request::new(CreateWorkspaceRequest {
            workspace: Some(workspace("work")),
        }))
        .await
        .expect("first create workspace");

    let error = harness
        .workspace_client()
        .create_workspace(Request::new(CreateWorkspaceRequest {
            workspace: Some(workspace("work")),
        }))
        .await
        .expect_err("duplicate workspace should fail");
    assert_eq!(error.code(), tonic::Code::AlreadyExists);
}

#[tokio::test]
async fn delete_missing_workspace_checks_database_not_config() {
    let harness = GrpcHarness::new().await;
    fs::write(
        harness.config_dir().join("config.toml"),
        "[credentials]\nstorage = \"file\"\n\n[workspaces.ghost]\n",
    )
    .expect("write stale workspace compatibility entry");

    let error = harness
        .workspace_client()
        .delete_workspace(Request::new(DeleteWorkspaceRequest {
            workspace: Some(workspace("ghost")),
        }))
        .await
        .expect_err("missing workspace should be rejected from database state");

    assert_eq!(error.code(), tonic::Code::NotFound);
}

#[tokio::test]
async fn source_requests_reject_unknown_workspace() {
    let harness = GrpcHarness::new().await;

    let error = harness
        .source_client()
        .list_sources(Request::new(ListSourcesRequest {
            workspace: Some(workspace("missing")),
        }))
        .await
        .expect_err("unknown workspace should fail");

    assert_eq!(error.code(), tonic::Code::NotFound);
    assert!(
        error.message().contains("workspace 'missing' not found"),
        "expected workspace not found message, got: {}",
        error.message()
    );
}

#[tokio::test]
async fn delete_workspace_removes_config_entry_and_workspace_artifacts() {
    let _trace_store_guard = TRACE_STORE_DELETE_TEST_LOCK.lock().await;
    let harness = GrpcHarness::new().await;
    harness
        .workspace_client()
        .create_workspace(Request::new(CreateWorkspaceRequest {
            workspace: Some(workspace("work")),
        }))
        .await
        .expect("create workspace");

    let imported = import_source_in_workspace(
        &harness,
        "work",
        fixture_manifest_yaml(harness.temp_path()),
        Vec::new(),
        Vec::new(),
    )
    .await;
    assert_eq!(imported.name, "local_messages");

    let workspace_dir = harness.config_dir().join("workspaces").join("work");
    let artifact = workspace_dir.join("artifacts").join("marker");
    fs::create_dir_all(artifact.parent().expect("artifact parent")).expect("create artifact dir");
    fs::write(&artifact, "workspace artifact").expect("write workspace artifact");
    assert!(artifact.exists(), "test should create a workspace artifact");

    let deleted = harness
        .workspace_client()
        .delete_workspace(Request::new(DeleteWorkspaceRequest {
            workspace: Some(workspace("work")),
        }))
        .await
        .expect("delete workspace")
        .into_inner()
        .workspace
        .expect("delete workspace response");
    assert_eq!(deleted.name, "work");

    assert!(
        workspace_names(&harness).await.is_empty(),
        "deleting the only workspace leaves the deployment with none",
    );
    assert!(
        !workspace_dir.exists(),
        "delete should remove workspace artifact directory"
    );
    assert_eq!(
        workspace_root_entries(harness.config_dir()),
        Vec::<String>::new(),
        "delete should leave nothing in the workspaces root for a later scan to read as live"
    );

    let raw = fs::read_to_string(harness.config_dir().join("config.toml")).expect("read config");
    assert!(
        !raw.contains("[workspaces.work"),
        "deleted workspace should be removed from config: {raw}"
    );

    harness
        .workspace_client()
        .create_workspace(Request::new(CreateWorkspaceRequest {
            workspace: Some(workspace("work")),
        }))
        .await
        .expect("recreate workspace");
    let sources = harness
        .source_client()
        .list_sources(Request::new(ListSourcesRequest {
            workspace: Some(workspace("work")),
        }))
        .await
        .expect("list recreated workspace sources")
        .into_inner()
        .sources;
    assert!(sources.is_empty(), "deleted DB sources should not reappear");
}

#[cfg(unix)]
#[tokio::test]
async fn delete_workspace_restores_db_sources_when_config_delete_fails() {
    use std::os::unix::fs::PermissionsExt;

    let temp = TempDir::new().expect("temp dir");
    let config_dir = temp.path().join("coral-config");
    let db_path = temp.path().join("db").join("coral.db");
    fs::create_dir_all(db_path.parent().expect("db parent")).expect("create db dir");
    fs::create_dir_all(&config_dir).expect("create config dir");
    fs::write(
        config_dir.join("config.toml"),
        format!(
            "[database]\nbackend = \"sqlite\"\npath = \"{}\"\n",
            db_path.display()
        ),
    )
    .expect("write database config");
    let harness = GrpcHarness::start_with_config_dir(config_dir.clone()).await;
    harness
        .workspace_client()
        .create_workspace(Request::new(CreateWorkspaceRequest {
            workspace: Some(workspace("work")),
        }))
        .await
        .expect("create workspace");
    import_source_in_workspace(
        &harness,
        "work",
        fixture_manifest_yaml(harness.temp_path()),
        Vec::new(),
        Vec::new(),
    )
    .await;
    fs::set_permissions(&config_dir, fs::Permissions::from_mode(0o500))
        .expect("make config dir read-only");
    let result = harness
        .workspace_client()
        .delete_workspace(Request::new(DeleteWorkspaceRequest {
            workspace: Some(workspace("work")),
        }))
        .await;
    fs::set_permissions(&config_dir, fs::Permissions::from_mode(0o700))
        .expect("restore config dir permissions");

    result.expect_err("config delete should fail");
    let sources = harness
        .source_client()
        .list_sources(Request::new(ListSourcesRequest {
            workspace: Some(workspace("work")),
        }))
        .await
        .expect("list restored workspace sources")
        .into_inner()
        .sources;
    assert!(sources.iter().any(|source| source.name == "local_messages"));
    let validated = harness
        .source_client()
        .validate_source(Request::new(ValidateSourceRequest {
            workspace: Some(workspace("work")),
            name: "local_messages".to_string(),
        }))
        .await
        .expect("restored source should validate")
        .into_inner();
    assert_eq!(validated.tables.len(), 1);
}

#[cfg(unix)]
#[tokio::test]
async fn delete_workspace_handles_imported_source_without_manifest_row() {
    use std::os::unix::fs::PermissionsExt;

    let temp = TempDir::new().expect("temp dir");
    let config_dir = temp.path().join("coral-config");
    let db_path = temp.path().join("db").join("coral.db");
    fs::create_dir_all(db_path.parent().expect("db parent")).expect("create db dir");
    fs::create_dir_all(&config_dir).expect("create config dir");
    fs::write(
        config_dir.join("config.toml"),
        format!(
            r#"
[database]
backend = "sqlite"
path = "{}"

[workspaces.work.sources.demo]
version = "0.1.0"
origin = "imported"
"#,
            db_path.display()
        ),
    )
    .expect("write database config");
    let harness = GrpcHarness::start_with_config_dir(config_dir.clone()).await;
    let sources = harness
        .source_client()
        .list_sources(Request::new(ListSourcesRequest {
            workspace: Some(workspace("work")),
        }))
        .await
        .expect("list degraded imported source")
        .into_inner()
        .sources;
    assert!(sources.iter().any(|source| source.name == "demo"));

    fs::set_permissions(&config_dir, fs::Permissions::from_mode(0o500))
        .expect("make config dir read-only");
    let result = harness
        .workspace_client()
        .delete_workspace(Request::new(DeleteWorkspaceRequest {
            workspace: Some(workspace("work")),
        }))
        .await;
    fs::set_permissions(&config_dir, fs::Permissions::from_mode(0o700))
        .expect("restore config dir permissions");

    result.expect_err("config delete should fail");
    let sources = harness
        .source_client()
        .list_sources(Request::new(ListSourcesRequest {
            workspace: Some(workspace("work")),
        }))
        .await
        .expect("list restored degraded source")
        .into_inner()
        .sources;
    assert!(sources.iter().any(|source| source.name == "demo"));
    let error = harness
        .source_client()
        .validate_source(Request::new(ValidateSourceRequest {
            workspace: Some(workspace("work")),
            name: "demo".to_string(),
        }))
        .await
        .expect_err("restored degraded source should still report missing manifest");
    assert_eq!(error.code(), tonic::Code::NotFound);

    let deleted = harness
        .workspace_client()
        .delete_workspace(Request::new(DeleteWorkspaceRequest {
            workspace: Some(workspace("work")),
        }))
        .await
        .expect("delete degraded workspace")
        .into_inner()
        .workspace
        .expect("delete workspace response");
    assert_eq!(deleted.name, "work");
    assert_eq!(workspace_names(&harness).await, vec!["default"]);
}

#[tokio::test]
async fn delete_workspace_removes_workspace_trace_history() {
    let _trace_store_guard = TRACE_STORE_DELETE_TEST_LOCK.lock().await;
    let harness = GrpcHarness::new().await;
    harness
        .workspace_client()
        .create_workspace(Request::new(CreateWorkspaceRequest {
            workspace: Some(workspace("work")),
        }))
        .await
        .expect("create workspace");

    let trace_dir = local_trace_store_dir(&harness);
    fs::create_dir_all(&trace_dir).expect("create trace dir");
    let trace_file = trace_dir.join("spans-00000000000000000001-1-0000000000000000.jsonl");
    fs::write(
        &trace_file,
        [
            local_trace_line("work-trace", "work-root", None, &json!({})),
            local_trace_line(
                "work-trace",
                "work-child",
                Some("work-root"),
                &json!({ "workspace": "work" }),
            ),
            local_trace_line(
                "other-trace",
                "other-root",
                None,
                &json!({ "workspace": "other" }),
            ),
            local_trace_line("unscoped-trace", "unscoped-root", None, &json!({})),
        ]
        .join("\n")
            + "\n",
    )
    .expect("write local trace history");

    let deleted = harness
        .workspace_client()
        .delete_workspace(Request::new(DeleteWorkspaceRequest {
            workspace: Some(workspace("work")),
        }))
        .await
        .expect("delete workspace")
        .into_inner()
        .workspace
        .expect("delete workspace response");
    assert_eq!(deleted.name, "work");

    let raw = fs::read_to_string(&trace_file).expect("read trace file");
    assert!(
        !raw.contains("work-trace"),
        "workspace trace should remove its unattributed root and attributed child: {raw}"
    );
    assert!(
        raw.contains("other-trace"),
        "other workspace trace should remain in local trace history: {raw}"
    );
    assert!(
        raw.contains("unscoped-trace"),
        "unscoped trace should remain in local trace history: {raw}"
    );
}

#[tokio::test]
async fn delete_workspace_ignores_malformed_trace_history() {
    let _trace_store_guard = TRACE_STORE_DELETE_TEST_LOCK.lock().await;
    let harness = GrpcHarness::new().await;
    harness
        .workspace_client()
        .create_workspace(Request::new(CreateWorkspaceRequest {
            workspace: Some(workspace("work")),
        }))
        .await
        .expect("create workspace");

    let imported = import_source_in_workspace(
        &harness,
        "work",
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
    assert_eq!(imported.name, "secured_messages");
    let source_dir = harness
        .config_dir()
        .join("workspaces")
        .join("work")
        .join("sources")
        .join("secured_messages");
    let secret_path = source_dir.join("secrets.env");
    assert!(source_dir.exists(), "import should create source artifacts");
    assert!(
        secret_path.exists(),
        "import should create file-backed secret material"
    );

    let trace_dir = local_trace_store_dir(&harness);
    fs::create_dir_all(&trace_dir).expect("create trace dir");
    fs::write(
        trace_dir.join("spans-00000000000000000001-1-0000000000000000.jsonl"),
        "not-json\n",
    )
    .expect("write malformed local trace history");

    let deleted = harness
        .workspace_client()
        .delete_workspace(Request::new(DeleteWorkspaceRequest {
            workspace: Some(workspace("work")),
        }))
        .await
        .expect("delete workspace should ignore malformed diagnostic trace history")
        .into_inner()
        .workspace
        .expect("delete workspace response");
    assert_eq!(deleted.name, "work");
    assert!(
        workspace_names(&harness).await.is_empty(),
        "deleting the only workspace leaves the deployment with none",
    );

    let raw = fs::read_to_string(harness.config_dir().join("config.toml")).expect("read config");
    assert!(
        !raw.contains("[workspaces.work"),
        "workspace config should be removed despite malformed trace history: {raw}"
    );
    assert!(
        !source_dir.exists(),
        "workspace artifact dir should be removed despite malformed trace history"
    );
    assert!(
        !secret_path.exists(),
        "workspace credential material should be removed despite malformed trace history"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn delete_workspace_removes_state_when_trace_cleanup_fails() {
    use std::os::unix::fs::PermissionsExt;

    let _trace_store_guard = TRACE_STORE_DELETE_TEST_LOCK.lock().await;
    let harness = GrpcHarness::new().await;
    harness
        .workspace_client()
        .create_workspace(Request::new(CreateWorkspaceRequest {
            workspace: Some(workspace("work")),
        }))
        .await
        .expect("create workspace");

    let imported = import_source_in_workspace(
        &harness,
        "work",
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
    assert_eq!(imported.name, "secured_messages");
    let source_dir = harness
        .config_dir()
        .join("workspaces")
        .join("work")
        .join("sources")
        .join("secured_messages");
    let secret_path = source_dir.join("secrets.env");
    assert!(source_dir.exists(), "import should create source artifacts");
    assert!(
        secret_path.exists(),
        "import should create file-backed secret material"
    );

    let trace_dir = local_trace_store_dir(&harness);
    fs::create_dir_all(&trace_dir).expect("create trace dir");
    let trace_file = trace_dir.join("spans-00000000000000000001-1-0000000000000000.jsonl");
    fs::write(
        &trace_file,
        local_trace_line(
            "work-trace",
            "work-root",
            None,
            &json!({ "workspace": "work" }),
        ) + "\n",
    )
    .expect("write local trace history");
    fs::set_permissions(&trace_file, fs::Permissions::from_mode(0o000))
        .expect("make trace file unreadable");

    let deleted = harness
        .workspace_client()
        .delete_workspace(Request::new(DeleteWorkspaceRequest {
            workspace: Some(workspace("work")),
        }))
        .await
        .expect("workspace delete should succeed when diagnostic trace cleanup fails")
        .into_inner()
        .workspace
        .expect("delete workspace response");
    fs::set_permissions(&trace_file, fs::Permissions::from_mode(0o600))
        .expect("restore trace file permissions");

    assert_eq!(deleted.name, "work");
    assert!(
        workspace_names(&harness).await.is_empty(),
        "deleting the only workspace leaves the deployment with none",
    );

    let raw = fs::read_to_string(harness.config_dir().join("config.toml")).expect("read config");
    assert!(
        !raw.contains("[workspaces.work"),
        "workspace config should be removed when trace cleanup fails: {raw}"
    );
    assert!(
        !source_dir.exists(),
        "workspace artifact dir should be removed when trace cleanup fails"
    );
    assert!(
        !secret_path.exists(),
        "workspace credential material should be removed when trace cleanup fails"
    );

    let raw_trace = fs::read_to_string(&trace_file).expect("read trace file");
    assert!(
        raw_trace.contains("work-trace"),
        "failed trace cleanup should leave the diagnostic trace file untouched"
    );
}

#[tokio::test]
async fn delete_workspace_ignores_stale_trace_files_when_trace_history_disabled() {
    let _trace_store_guard = TRACE_STORE_DELETE_TEST_LOCK.lock().await;
    GrpcHarness::new().await.shutdown().await;

    let temp = TempDir::new().expect("temp dir");
    let config_dir = temp.path().join("coral-config");
    fs::create_dir_all(config_dir.join("telemetry").join("traces")).expect("create trace dir");
    fs::write(
        config_dir.join("config.toml"),
        "[trace_history]\nenabled = false\n",
    )
    .expect("write config");
    let stale_trace_file = config_dir
        .join("telemetry")
        .join("traces")
        .join("spans-00000000000000000001-1-0000000000000000.jsonl");
    fs::write(&stale_trace_file, "not-json\n").expect("write stale malformed trace history");
    let harness = GrpcHarness::start_with_config_dir(config_dir).await;
    harness
        .workspace_client()
        .create_workspace(Request::new(CreateWorkspaceRequest {
            workspace: Some(workspace("work")),
        }))
        .await
        .expect("create workspace");

    let deleted = harness
        .workspace_client()
        .delete_workspace(Request::new(DeleteWorkspaceRequest {
            workspace: Some(workspace("work")),
        }))
        .await
        .expect("delete workspace")
        .into_inner()
        .workspace
        .expect("delete workspace response");

    assert_eq!(deleted.name, "work");
    assert!(
        workspace_names(&harness).await.is_empty(),
        "deleting the only workspace leaves the deployment with none",
    );
    assert!(
        stale_trace_file.exists(),
        "disabled trace history should skip trace cleanup"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn delete_workspace_succeeds_when_backup_cleanup_fails_after_config_delete() {
    use std::os::unix::fs::PermissionsExt;

    let _trace_store_guard = TRACE_STORE_DELETE_TEST_LOCK.lock().await;
    let harness = GrpcHarness::new().await;
    harness
        .workspace_client()
        .create_workspace(Request::new(CreateWorkspaceRequest {
            workspace: Some(workspace("work")),
        }))
        .await
        .expect("create workspace");

    let imported = import_source_in_workspace(
        &harness,
        "work",
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
    assert_eq!(imported.name, "secured_messages");

    let workspace_dir = harness.config_dir().join("workspaces").join("work");
    let sources_root = workspace_dir.join("sources");
    fs::create_dir_all(sources_root.join("secured_messages"))
        .expect("create legacy source artifact dir");
    fs::write(
        sources_root
            .join("secured_messages")
            .join("legacy-artifact"),
        "legacy",
    )
    .expect("write legacy source artifact");
    fs::set_permissions(&sources_root, fs::Permissions::from_mode(0o500))
        .expect("make nested sources dir read-only");

    let deleted = harness
        .workspace_client()
        .delete_workspace(Request::new(DeleteWorkspaceRequest {
            workspace: Some(workspace("work")),
        }))
        .await
        .expect("delete workspace should succeed after committed config removal")
        .into_inner()
        .workspace
        .expect("delete workspace response");
    assert_eq!(deleted.name, "work");

    // The staged directory outlived the deletion, which is the whole point of
    // this test — but it did so outside the workspaces root, so nothing left
    // there can be read back as a live workspace.
    assert_eq!(
        workspace_root_entries(harness.config_dir()),
        Vec::<String>::new(),
        "a deletion whose cleanup failed still leaves the workspaces root empty"
    );

    let backup = find_workspace_delete_backup(harness.config_dir(), "work");
    fs::set_permissions(backup.join("sources"), fs::Permissions::from_mode(0o700))
        .expect("restore backup sources permissions");
    fs::remove_dir_all(&backup).expect("remove backup after assertion");

    assert!(
        workspace_names(&harness).await.is_empty(),
        "deleting the only workspace leaves the deployment with none",
    );
    assert!(
        !workspace_dir.exists(),
        "deleted workspace should no longer exist at its normal artifact path"
    );

    let raw = fs::read_to_string(harness.config_dir().join("config.toml")).expect("read config");
    assert!(
        !raw.contains("[workspaces.work"),
        "deleted workspace should be removed from config: {raw}"
    );
}

/// `default` is a name like any other now that nothing provisions it. A caller
/// who wants one creates it, and may delete it again — the reserved-name guard
/// that used to refuse that is gone, so a deployment left holding an
/// undeletable workspace nobody asked for would be the regression.
#[tokio::test]
async fn default_like_names_are_created_and_deleted_like_any_other() {
    let harness = GrpcHarness::new().await;

    for name in ["default", "default-2", "work"] {
        harness
            .workspace_client()
            .create_workspace(Request::new(CreateWorkspaceRequest {
                workspace: Some(workspace(name)),
            }))
            .await
            .unwrap_or_else(|error| panic!("create workspace '{name}': {error}"));
        assert_eq!(workspace_names(&harness).await, vec![name.to_string()]);

        harness
            .workspace_client()
            .delete_workspace(Request::new(DeleteWorkspaceRequest {
                workspace: Some(workspace(name)),
            }))
            .await
            .unwrap_or_else(|error| panic!("delete workspace '{name}': {error}"));
        assert!(workspace_names(&harness).await.is_empty());
    }
}

async fn delete_workspace(
    client: &AppClient,
    name: &str,
) -> Result<DeleteWorkspaceResponse, Status> {
    client
        .workspace_client()
        .delete_workspace(Request::new(DeleteWorkspaceRequest {
            workspace: Some(workspace(name)),
        }))
        .await
        .map(Response::into_inner)
}

async fn list_members(
    client: &AppClient,
    name: &str,
) -> Result<ListWorkspaceMembersResponse, Status> {
    client
        .workspace_client()
        .list_workspace_members(Request::new(ListWorkspaceMembersRequest {
            workspace: Some(workspace(name)),
        }))
        .await
        .map(Response::into_inner)
}

fn member_rows(response: ListWorkspaceMembersResponse) -> Vec<(String, WorkspaceRole, String)> {
    response
        .members
        .into_iter()
        .map(|member| {
            (
                member.user_id,
                member.role.try_into().expect("member role"),
                member.display_name,
            )
        })
        .collect()
}

/// Probes one workspace name across every control-plane RPC and reports what
/// each of them told the caller. Two names that agree here are
/// indistinguishable to that caller.
async fn control_plane_refusals(
    client: &AppClient,
    name: &str,
    user_id: &str,
) -> Vec<(Code, String, Vec<String>)> {
    [
        list_members(client, name)
            .await
            .expect_err("a non-member must not read the roster"),
        delete_workspace(client, name)
            .await
            .expect_err("a non-member must not delete the workspace"),
        add_member(client, name, user_id, WorkspaceRole::Member)
            .await
            .expect_err("a non-member must not grant membership"),
        remove_member(client, name, user_id)
            .await
            .expect_err("a non-member must not revoke membership"),
    ]
    .iter()
    .map(|status| concealed_refusal(status, name))
    .collect()
}

/// A caller who belongs to nothing is answered, not refused: the listing is
/// their own view, and an empty view is a complete answer to it.
///
/// Signing in also creates nothing to belong to. The empty listing alone would
/// not show that — it reads the same whether the deployment holds no workspace
/// or one this caller cannot reach — so the deployment's own state is checked
/// beside it.
#[tokio::test]
async fn a_caller_with_no_membership_is_listed_nothing_rather_than_denied() {
    let deployment = SharedDeployment::start().await;
    let ada = deployment.seed_user("ada", "Ada").await;
    let newcomer = deployment.as_person(&ada).await;

    assert_eq!(membership_rows(&newcomer).await, Vec::new());
    assert!(
        deployment.workspace_names().await.is_empty(),
        "provisioning a login must not create a workspace",
    );
}

#[tokio::test]
async fn creating_a_workspace_makes_its_caller_its_only_owner() {
    let deployment = SharedDeployment::start().await;
    let ada = deployment.seed_user("ada", "Ada").await;
    let creator = deployment.as_person(&ada).await;

    let created = create_workspace(&creator, "team")
        .await
        .expect("create workspace")
        .workspace
        .expect("create workspace response");

    assert_eq!(created.name, "team");
    assert_eq!(
        membership_rows(&creator).await,
        vec![("team".to_string(), WorkspaceRole::Owner)],
        "the creator reaches the workspace they asked for, as its owner"
    );
    assert_eq!(
        member_rows(list_members(&creator, "team").await.expect("list members")),
        vec![(ada, WorkspaceRole::Owner, "Ada".to_string())],
    );
}

/// The identity RPC reports who the caller is and nothing else: not where they
/// sign in, and not where they should be routed.
#[tokio::test]
async fn the_current_user_carries_identity_alone_and_the_directory_follows_ownership() {
    let deployment = SharedDeployment::start().await;
    let ada = deployment.seed_user("ada", "Ada").await;
    let bob = deployment.seed_user("bob", "Bob").await;
    let owner = deployment.as_person(&ada).await;
    let outsider = deployment.as_person(&bob).await;
    create_workspace(&owner, "team").await.expect("create");

    let response = owner
        .user_client()
        .get_current_user(Request::new(GetCurrentUserRequest {}))
        .await
        .expect("a provisioned caller reads their own entry")
        .into_inner();

    let user = response.user.clone().expect("the response carries a user");
    assert_eq!(user.user_id, ada);
    assert_eq!(user.display_name, "Ada");
    let wire = String::from_utf8_lossy(&response.encode_to_vec()).into_owned();
    for absent in ["upstream-subject-ada", TEST_ISSUER, "default"] {
        assert!(
            !wire.contains(absent),
            "the current-user response must not carry '{absent}': {wire}"
        );
    }

    assert_eq!(
        owner
            .user_client()
            .list_users(Request::new(ListUsersRequest {}))
            .await
            .expect("an owner may name people")
            .into_inner()
            .users
            .into_iter()
            .map(|user| user.user_id)
            .collect::<Vec<_>>(),
        vec![ada, bob],
    );
    assert_eq!(
        outsider
            .user_client()
            .list_users(Request::new(ListUsersRequest {}))
            .await
            .expect_err("a caller who owns nothing has nobody to name")
            .code(),
        Code::PermissionDenied,
    );
}

#[tokio::test]
async fn member_changes_follow_the_error_contract() {
    let deployment = SharedDeployment::start().await;
    let ada = deployment.seed_user("ada", "Ada").await;
    let bob = deployment.seed_user("bob", "Bob").await;
    let owner = deployment.as_person(&ada).await;
    create_workspace(&owner, "team").await.expect("create");

    let granted = add_member(&owner, "team", &bob, WorkspaceRole::Member)
        .await
        .expect("grant membership")
        .member
        .expect("added member");
    assert_eq!(granted.user_id, bob);
    assert_eq!(granted.role, i32::from(WorkspaceRole::Member));
    assert_eq!(granted.display_name, "Bob");

    add_member(&owner, "team", &bob, WorkspaceRole::Member)
        .await
        .expect("a repeated invitation reads the same as the first");
    assert_eq!(
        member_rows(list_members(&owner, "team").await.expect("roster")),
        vec![
            (ada.clone(), WorkspaceRole::Owner, "Ada".to_string()),
            (bob.clone(), WorkspaceRole::Member, "Bob".to_string()),
        ],
        "a repeated invitation must not double the roster",
    );

    for role in [WorkspaceRole::Owner, WorkspaceRole::Member] {
        assert_eq!(
            add_member(&owner, "team", &bob, role)
                .await
                .expect("change an existing member's role")
                .member
                .expect("updated member")
                .role,
            i32::from(role),
        );
    }

    for (status, expected, case) in [
        (
            add_member(&owner, "team", &ada, WorkspaceRole::Member)
                .await
                .expect_err("demoting the last owner"),
            Code::FailedPrecondition,
            "the owner floor holds against demotion",
        ),
        (
            add_member(&owner, "team", "user-nobody", WorkspaceRole::Member)
                .await
                .expect_err("granting membership to an unknown user"),
            Code::NotFound,
            "an unknown user cannot be named",
        ),
        (
            add_member(&owner, "team", &bob, WorkspaceRole::Unspecified)
                .await
                .expect_err("granting an unrecorded role"),
            Code::InvalidArgument,
            "an unspecified role is caller input, not a default",
        ),
    ] {
        assert_eq!(status.code(), expected, "{case}: {}", status.message());
    }

    remove_member(&owner, "team", &bob)
        .await
        .expect("revoke membership");
    assert_eq!(
        member_rows(list_members(&owner, "team").await.expect("roster")),
        vec![(ada.clone(), WorkspaceRole::Owner, "Ada".to_string())],
    );
    assert_eq!(
        remove_member(&owner, "team", &bob)
            .await
            .expect_err("revoking a membership nobody holds")
            .code(),
        Code::NotFound,
    );
    assert_eq!(
        remove_member(&owner, "team", &ada)
            .await
            .expect_err("removing the last owner")
            .code(),
        Code::FailedPrecondition,
        "the owner floor holds against removal too",
    );
}

/// The workspace namespace must not answer questions its caller may not ask.
/// A name they hold no membership in reads exactly like a name nobody ever
/// created — otherwise the refusals themselves enumerate the deployment.
#[tokio::test]
async fn a_non_member_cannot_tell_an_existing_workspace_from_an_absent_one() {
    let deployment = SharedDeployment::start().await;
    let ada = deployment.seed_user("ada", "Ada").await;
    let bob = deployment.seed_user("bob", "Bob").await;
    let owner = deployment.as_person(&ada).await;
    let outsider = deployment.as_person(&bob).await;
    create_workspace(&owner, "team").await.expect("create");

    let existing = control_plane_refusals(&outsider, "team", &bob).await;
    assert_eq!(
        existing,
        control_plane_refusals(&outsider, "ghost", &bob).await,
        "an existing workspace must be indistinguishable from one that never existed",
    );
    assert!(
        existing
            .iter()
            .all(|(code, _, reasons)| *code == Code::NotFound
                && reasons.as_slice() == [CORAL_ERROR_REASON_WORKSPACE_NOT_FOUND]),
        "both must read as the absent workspace, not as a denial that confirms one: {existing:?}",
    );
    assert_eq!(
        membership_rows(&owner).await,
        vec![("team".to_string(), WorkspaceRole::Owner)],
        "a refused probe must not have changed the workspace it probed",
    );
}

/// Deleting a workspace takes its memberships with it. Re-creating the name is
/// the proof: a membership row that outlived its workspace would reappear as
/// access nobody granted.
#[tokio::test]
async fn deleting_a_workspace_takes_its_memberships_with_it() {
    let deployment = SharedDeployment::start().await;
    let ada = deployment.seed_user("ada", "Ada").await;
    let bob = deployment.seed_user("bob", "Bob").await;
    let owner = deployment.as_person(&ada).await;
    let member = deployment.as_person(&bob).await;
    create_workspace(&owner, "team").await.expect("create");
    add_member(&owner, "team", &bob, WorkspaceRole::Member)
        .await
        .expect("grant membership");
    assert_eq!(
        membership_rows(&member).await,
        vec![("team".to_string(), WorkspaceRole::Member)],
    );

    let deleted = delete_workspace(&owner, "team")
        .await
        .expect("an owner deletes their workspace")
        .workspace
        .expect("delete workspace response");

    assert_eq!(deleted.name, "team");
    for (client, whose) in [(&owner, "owner"), (&member, "member")] {
        assert_eq!(
            membership_rows(client).await,
            Vec::new(),
            "the deleted workspace must leave the {whose}'s listing",
        );
    }

    create_workspace(&owner, "team")
        .await
        .expect("the name is free again");
    assert_eq!(
        member_rows(list_members(&owner, "team").await.expect("roster")),
        vec![(ada, WorkspaceRole::Owner, "Ada".to_string())],
        "the re-created workspace must not inherit the deleted one's members",
    );
}

fn local_trace_line(
    trace_id: &str,
    span_id: &str,
    parent_span_id: Option<&str>,
    attributes: &serde_json::Value,
) -> String {
    json!({
        "trace_id": trace_id,
        "span_id": span_id,
        "parent_span_id": parent_span_id,
        "attributes_json": attributes.to_string(),
    })
    .to_string()
}
