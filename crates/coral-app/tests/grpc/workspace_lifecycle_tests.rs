use std::fs;

use coral_api::v1::{
    AddWorkspaceMemberRequest, CreateWorkspaceRequest, DeleteWorkspaceRequest,
    GetCurrentUserRequest, ImportSourceRequest, ListSourcesRequest, ListWorkspaceMembersRequest,
    ListWorkspacesRequest, RemoveWorkspaceMemberRequest, Source, SourceSecret, SourceVariable,
    Workspace, WorkspaceMember, WorkspaceRole, import_source_response,
};
use coral_client::default_workspace;
use serde_json::json;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use tempfile::TempDir;
use tonic::Request;

use crate::harness::{
    GrpcHarness, WorkspaceAccessControlHarness, fixture_manifest_with_inputs_yaml,
    fixture_manifest_yaml,
};

static TRACE_STORE_DELETE_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn workspace(name: &str) -> Workspace {
    Workspace {
        name: name.to_string(),
    }
}

async fn workspace_names(harness: &GrpcHarness) -> Vec<String> {
    harness
        .workspace_client()
        .list_workspaces(Request::new(ListWorkspacesRequest {}))
        .await
        .expect("list workspaces")
        .into_inner()
        .memberships
        .into_iter()
        .filter_map(|membership| membership.workspace)
        .map(|workspace| workspace.name)
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
    fs::read_dir(config_dir.join("workspaces"))
        .expect("read workspaces dir")
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .find(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with(&prefix))
        })
        .expect("workspace delete backup dir")
}

fn local_trace_store_dir(harness: &GrpcHarness) -> std::path::PathBuf {
    harness
        .local_trace_store_dir()
        .expect("trace history should be enabled")
        .to_path_buf()
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "the end-to-end scenario verifies one complete access-control lifecycle"
)]
async fn workspace_access_control_grant_revoke_and_owner_floor() {
    let harness = WorkspaceAccessControlHarness::new().await;
    let owner = harness.owner();
    let member = harness.member();
    let shared = workspace("owner-workspace");

    let current = owner
        .user_client()
        .get_current_user(Request::new(GetCurrentUserRequest {}))
        .await
        .expect("mounted user service")
        .into_inner()
        .user
        .expect("current user");
    assert_eq!(current.user_id, harness.owner_id());
    owner
        .workspace_client()
        .create_workspace(Request::new(CreateWorkspaceRequest {
            workspace: Some(shared.clone()),
        }))
        .await
        .expect("owner creates shared workspace");
    member
        .workspace_client()
        .create_workspace(Request::new(CreateWorkspaceRequest {
            workspace: Some(workspace("member-workspace")),
        }))
        .await
        .expect("member creates own workspace");

    owner
        .workspace_client()
        .add_workspace_member(Request::new(AddWorkspaceMemberRequest {
            workspace: Some(shared.clone()),
            member: Some(WorkspaceMember {
                user_id: harness.member_id().to_string(),
                role: WorkspaceRole::Member as i32,
                display_name: String::new(),
            }),
        }))
        .await
        .expect("owner grants membership");
    let member_memberships = member
        .workspace_client()
        .list_workspaces(Request::new(ListWorkspacesRequest {}))
        .await
        .expect("member lists visible workspaces")
        .into_inner()
        .memberships;
    assert!(member_memberships.iter().any(|membership| {
        membership.workspace.as_ref() == Some(&shared)
            && membership.role == WorkspaceRole::Member as i32
    }));
    assert!(member_memberships.iter().any(|membership| {
        membership.workspace.as_ref() == Some(&workspace("member-workspace"))
            && membership.role == WorkspaceRole::Owner as i32
    }));

    let denied_management = member
        .workspace_client()
        .list_workspace_members(Request::new(ListWorkspaceMembersRequest {
            workspace: Some(shared.clone()),
        }))
        .await
        .expect_err("member cannot manage memberships");
    assert_eq!(denied_management.code(), tonic::Code::PermissionDenied);
    let denied_delete = member
        .workspace_client()
        .delete_workspace(Request::new(DeleteWorkspaceRequest {
            workspace: Some(shared.clone()),
        }))
        .await
        .expect_err("member cannot delete workspace");
    assert_eq!(denied_delete.code(), tonic::Code::PermissionDenied);

    owner
        .workspace_client()
        .remove_workspace_member(Request::new(RemoveWorkspaceMemberRequest {
            workspace: Some(shared.clone()),
            user_id: harness.member_id().to_string(),
        }))
        .await
        .expect("owner revokes membership");
    let concealed = member
        .workspace_client()
        .list_workspace_members(Request::new(ListWorkspaceMembersRequest {
            workspace: Some(shared.clone()),
        }))
        .await
        .expect_err("revoked membership is concealed immediately");
    assert_eq!(concealed.code(), tonic::Code::NotFound);
    let member_memberships = member
        .workspace_client()
        .list_workspaces(Request::new(ListWorkspacesRequest {}))
        .await
        .expect("member lists after revocation")
        .into_inner()
        .memberships;
    assert!(
        member_memberships
            .iter()
            .all(|membership| membership.workspace.as_ref() != Some(&shared))
    );

    let owner_floor = owner
        .workspace_client()
        .remove_workspace_member(Request::new(RemoveWorkspaceMemberRequest {
            workspace: Some(shared),
            user_id: harness.owner_id().to_string(),
        }))
        .await
        .expect_err("last owner cannot be removed");
    assert_eq!(owner_floor.code(), tonic::Code::FailedPrecondition);
}

#[tokio::test]
async fn lists_default_workspace_when_config_is_missing() {
    let harness = GrpcHarness::new().await;

    assert_eq!(workspace_names(&harness).await, vec!["default"]);
}

#[tokio::test]
async fn startup_cutover_removes_stale_shadow_workspace_rows() {
    let temp = TempDir::new().expect("temp dir");
    let config_dir = temp.path().join("coral-config");
    seed_stale_workspace_row(&config_dir, "stale").await;

    let harness = GrpcHarness::start_with_config_dir(config_dir).await;

    assert_eq!(workspace_names(&harness).await, vec!["default"]);
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
    assert_eq!(workspace_names(&harness).await, vec!["default", "work"]);

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

    assert_eq!(workspace_names(&harness).await, vec!["default", "work"]);
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

    let source_dir = harness
        .config_dir()
        .join("workspaces")
        .join("work")
        .join("sources")
        .join("local_messages");
    assert!(source_dir.exists(), "import should create source artifacts");

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

    assert_eq!(workspace_names(&harness).await, vec!["default"]);
    assert!(
        !source_dir.exists(),
        "delete should remove workspace artifacts"
    );

    let raw = fs::read_to_string(harness.config_dir().join("config.toml")).expect("read config");
    assert!(
        !raw.contains("[workspaces.work"),
        "deleted workspace should be removed from config: {raw}"
    );
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
    assert_eq!(workspace_names(&harness).await, vec!["default"]);

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
    assert_eq!(workspace_names(&harness).await, vec!["default"]);

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
    assert_eq!(workspace_names(&harness).await, vec!["default"]);
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
    assert!(
        sources_root
            .join("secured_messages")
            .join("secrets.env")
            .exists(),
        "import should persist file-backed credential material"
    );
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

    let backup = find_workspace_delete_backup(harness.config_dir(), "work");
    fs::set_permissions(backup.join("sources"), fs::Permissions::from_mode(0o700))
        .expect("restore backup sources permissions");
    fs::remove_dir_all(&backup).expect("remove backup after assertion");

    assert_eq!(workspace_names(&harness).await, vec!["default"]);
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

#[tokio::test]
async fn delete_default_workspace_returns_failed_precondition() {
    let harness = GrpcHarness::new().await;

    let error = harness
        .workspace_client()
        .delete_workspace(Request::new(DeleteWorkspaceRequest {
            workspace: Some(default_workspace()),
        }))
        .await
        .expect_err("default workspace delete should fail");

    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(
        error
            .message()
            .contains("default workspace cannot be removed"),
        "expected default workspace guard, got: {}",
        error.message()
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
