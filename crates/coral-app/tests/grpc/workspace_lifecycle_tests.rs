use std::fs;

use coral_api::v1::{
    CreateWorkspaceRequest, DeleteWorkspaceRequest, ImportSourceRequest, ListSourcesRequest,
    ListWorkspacesRequest, Source, SourceSecret, SourceVariable, Workspace, import_source_response,
};
use coral_client::default_workspace;
use serde_json::json;
use tempfile::TempDir;
use tonic::Request;

use crate::harness::{GrpcHarness, fixture_manifest_with_inputs_yaml, fixture_manifest_yaml};

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
        .workspaces
        .into_iter()
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
async fn lists_default_workspace_when_config_is_missing() {
    let harness = GrpcHarness::new().await;

    assert_eq!(workspace_names(&harness).await, vec!["default"]);
}

#[tokio::test]
async fn create_workspace_persists_empty_workspace_table() {
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
        raw.contains("[workspaces.work]"),
        "created empty workspace should persist as an explicit table: {raw}"
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
            local_trace_line(
                "work-trace",
                "work-root",
                None,
                &json!({ "workspace": "work" }),
            ),
            local_trace_line("work-trace", "work-child", Some("work-root"), &json!({})),
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
        "workspace trace should be removed from local trace history: {raw}"
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
#[expect(
    clippy::too_many_lines,
    reason = "Boundary test keeps the trace cleanup failure fixture and assertions together for readability."
)]
#[tokio::test]
async fn delete_workspace_leaves_state_untouched_when_trace_cleanup_fails() {
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

    let result = harness
        .workspace_client()
        .delete_workspace(Request::new(DeleteWorkspaceRequest {
            workspace: Some(workspace("work")),
        }))
        .await;
    fs::set_permissions(&trace_file, fs::Permissions::from_mode(0o600))
        .expect("restore trace file permissions");

    let error = result.expect_err("workspace delete should fail when trace cleanup cannot read");
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(
        error
            .message()
            .contains("cannot be removed until local trace history can be cleaned up"),
        "expected trace cleanup failure message, got: {}",
        error.message()
    );
    assert_eq!(workspace_names(&harness).await, vec!["default", "work"]);

    let raw = fs::read_to_string(harness.config_dir().join("config.toml")).expect("read config");
    assert!(
        raw.contains("[workspaces.work]"),
        "workspace config should remain when trace cleanup fails: {raw}"
    );
    assert!(
        raw.contains("[workspaces.work.sources.secured_messages]"),
        "workspace source config should remain when trace cleanup fails: {raw}"
    );
    assert!(
        source_dir.exists(),
        "workspace artifact dir should remain when trace cleanup fails"
    );
    assert!(
        secret_path.exists(),
        "workspace credential material should remain when trace cleanup fails"
    );

    let sources = harness
        .source_client()
        .list_sources(Request::new(ListSourcesRequest {
            workspace: Some(workspace("work")),
        }))
        .await
        .expect("list sources after failed delete")
        .into_inner()
        .sources;
    assert_eq!(
        sources
            .iter()
            .map(|source| source.name.as_str())
            .collect::<Vec<_>>(),
        vec!["secured_messages"]
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
