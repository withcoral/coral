use std::fs;

use coral_api::v1::{
    CreateWorkspaceRequest, DeleteWorkspaceRequest, ImportSourceRequest, ListSourcesRequest,
    ListWorkspacesRequest, Source, SourceSecret, SourceVariable, Workspace, import_source_response,
};
use coral_client::default_workspace;
use tonic::Request;

use crate::harness::{GrpcHarness, fixture_manifest_with_inputs_yaml, fixture_manifest_yaml};

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

#[cfg(unix)]
#[tokio::test]
async fn delete_workspace_succeeds_when_backup_cleanup_fails_after_config_delete() {
    use std::os::unix::fs::PermissionsExt;

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
