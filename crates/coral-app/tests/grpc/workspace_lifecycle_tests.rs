use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use coral_api::CORAL_ERROR_REASON_WORKSPACE_NOT_FOUND;
use coral_api::v1::{
    AddWorkspaceMemberRequest, AddWorkspaceMemberResponse, CreateWorkspaceRequest,
    CreateWorkspaceResponse, DeleteWorkspaceRequest, DeleteWorkspaceResponse,
    GetCurrentUserRequest, ImportSourceRequest, ListSourcesRequest, ListUsersRequest,
    ListWorkspaceMembersRequest, ListWorkspaceMembersResponse, ListWorkspacesRequest,
    ListWorkspacesResponse, RemoveWorkspaceMemberRequest, RemoveWorkspaceMemberResponse, Source,
    SourceSecret, SourceVariable, Workspace, WorkspaceRole, import_source_response,
};
use coral_app::{Principal, PrincipalKind, PrincipalProvider, PrincipalProviderError};
use coral_client::local::{RunningServer, ServerBuilder, connect_with_loopback_bearer};
use coral_client::{AppClient, BearerToken, default_workspace};
use prost::Message as _;
use serde_json::json;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use tempfile::TempDir;
use tonic::{Code, Request, Response, Status};
use tonic_types::{ErrorDetail, StatusExt as _};

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

/// Upstream issuer written into every seeded directory row, so a response that
/// leaked it would be recognizable on the wire.
const TEST_ISSUER: &str = "https://issuer.test/authorization";

/// Authenticates a `"<kind>:<user_id>"` bearer token.
///
/// Installing any provider of its own is what makes a deployment a shared one:
/// it retires the implicit local owner, so each request over these tests'
/// transport arrives as a distinct person or agent instead of as the host.
#[derive(Debug)]
struct TokenPrincipals;

#[tonic::async_trait]
impl PrincipalProvider for TokenPrincipals {
    async fn principal_for_metadata(
        &self,
        metadata: &tonic::metadata::MetadataMap,
    ) -> Result<Principal, PrincipalProviderError> {
        let credential = metadata
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))
            .ok_or_else(|| PrincipalProviderError::unauthenticated("missing bearer credential"))?;
        let (kind, user_id) = credential
            .split_once(':')
            .ok_or_else(|| PrincipalProviderError::unauthenticated("malformed test credential"))?;
        let kind = if kind == "agent" {
            PrincipalKind::Agent
        } else {
            PrincipalKind::User
        };
        Principal::parse(user_id, kind)
            .map_err(|error| PrincipalProviderError::unauthenticated(error.to_string()))
    }
}

/// A running server that authenticates its callers, plus the directory rows
/// login provisioning would have written for them.
struct SharedDeployment {
    _temp: Option<TempDir>,
    config_dir: PathBuf,
    endpoint_uri: String,
    _server: RunningServer,
}

impl SharedDeployment {
    async fn start() -> Self {
        let temp = TempDir::new().expect("temp dir");
        let config_dir = temp.path().join("coral-config");
        fs::create_dir_all(&config_dir).expect("create config dir");
        fs::write(
            config_dir.join("config.toml"),
            "[credentials]\nstorage = \"file\"\n",
        )
        .expect("write test credential config");
        let server = ServerBuilder::new()
            .with_config_dir(&config_dir)
            .with_principal_provider(Arc::new(TokenPrincipals))
            .start()
            .await
            .expect("start an authenticated server");
        let endpoint_uri = server.endpoint_uri().to_string();
        // The local trace store is installed once per process, by whichever
        // server starts first, and the trace-history tests write into whatever
        // directory that turned out to be. When it is this one's, the temp dir
        // must outlive the deployment: removing it would delete the installed
        // store out from under a concurrently running test.
        let temp = if server
            .local_trace_store_dir()
            .is_some_and(|dir| dir.starts_with(temp.path()))
        {
            let _installed_store_root: PathBuf = temp.keep();
            None
        } else {
            Some(temp)
        };
        Self {
            _temp: temp,
            config_dir,
            endpoint_uri,
            _server: server,
        }
    }

    /// Writes one directory row the way a completed login would.
    ///
    /// The login flow itself is upstream of this contract, so it is the row —
    /// not the OIDC round trip — that these transport tests need. Every row
    /// shares one timestamp, leaving the directory ordered by user id.
    async fn seed_user(&self, handle: &str, display_name: &str) -> String {
        let user_id = format!("user-{handle}");
        let pool = SqlitePoolOptions::new()
            .connect_with(SqliteConnectOptions::new().filename(self.config_dir.join("coral.db")))
            .await
            .expect("open the app database");
        sqlx::query(
            "INSERT INTO users (user_id, issuer, subject, display_name, created_at_unix_nanos, last_login_at_unix_nanos) VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(&user_id)
        .bind(TEST_ISSUER)
        .bind(format!("upstream-subject-{handle}"))
        .bind(display_name)
        .bind(1_i64)
        .bind(1_i64)
        .execute(&pool)
        .await
        .expect("seed a provisioned login");
        pool.close().await;
        user_id
    }

    async fn as_person(&self, user_id: &str) -> AppClient {
        self.connect("user", user_id).await
    }

    async fn as_agent(&self, user_id: &str) -> AppClient {
        self.connect("agent", user_id).await
    }

    async fn connect(&self, kind: &str, user_id: &str) -> AppClient {
        connect_with_loopback_bearer(
            &self.endpoint_uri,
            BearerToken::new(format!("{kind}:{user_id}")).expect("test bearer token"),
        )
        .await
        .expect("connect a test client")
    }
}

async fn create_workspace(
    client: &AppClient,
    name: &str,
) -> Result<CreateWorkspaceResponse, Status> {
    client
        .workspace_client()
        .create_workspace(Request::new(CreateWorkspaceRequest {
            workspace: Some(workspace(name)),
        }))
        .await
        .map(Response::into_inner)
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

async fn list_workspaces(client: &AppClient) -> Result<ListWorkspacesResponse, Status> {
    client
        .workspace_client()
        .list_workspaces(Request::new(ListWorkspacesRequest {}))
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

async fn add_member(
    client: &AppClient,
    name: &str,
    user_id: &str,
    role: WorkspaceRole,
) -> Result<AddWorkspaceMemberResponse, Status> {
    client
        .workspace_client()
        .add_workspace_member(Request::new(AddWorkspaceMemberRequest {
            workspace: Some(workspace(name)),
            user_id: user_id.to_string(),
            role: role.into(),
        }))
        .await
        .map(Response::into_inner)
}

async fn remove_member(
    client: &AppClient,
    name: &str,
    user_id: &str,
) -> Result<RemoveWorkspaceMemberResponse, Status> {
    client
        .workspace_client()
        .remove_workspace_member(Request::new(RemoveWorkspaceMemberRequest {
            workspace: Some(workspace(name)),
            user_id: user_id.to_string(),
        }))
        .await
        .map(Response::into_inner)
}

/// Reads a listing the way a client does: workspace name beside the caller's
/// own role, with no second request needed to learn it.
fn membership_rows(response: ListWorkspacesResponse) -> Vec<(String, WorkspaceRole)> {
    response
        .memberships
        .into_iter()
        .map(|membership| {
            (
                membership.workspace.expect("listed workspace").name,
                membership.role.try_into().expect("listed role"),
            )
        })
        .collect()
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

/// Probes one workspace name across every control-plane RPC and reports only
/// what the caller is told: the code, the message with the name they supplied
/// themselves factored out, and the structured reason. Two names that agree
/// here are indistinguishable to that caller.
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
    .map(|status| {
        (
            status.code(),
            status.message().replace(name, "<workspace>"),
            status
                .get_error_details_vec()
                .iter()
                .filter_map(|detail| match detail {
                    ErrorDetail::ErrorInfo(info) => Some(info.reason.clone()),
                    _ => None,
                })
                .collect(),
        )
    })
    .collect()
}

/// A caller who belongs to nothing is answered, not refused: the listing is
/// their own view, and an empty view is a complete answer to it.
#[tokio::test]
async fn a_caller_with_no_membership_is_listed_nothing_rather_than_denied() {
    let deployment = SharedDeployment::start().await;
    let ada = deployment.seed_user("ada", "Ada").await;
    let newcomer = deployment.as_person(&ada).await;

    let listing = list_workspaces(&newcomer)
        .await
        .expect("a caller with no membership is still answered");

    assert_eq!(membership_rows(listing), Vec::new());
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
        membership_rows(list_workspaces(&creator).await.expect("list workspaces")),
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
        membership_rows(list_workspaces(&owner).await.expect("the owner's listing")),
        vec![("team".to_string(), WorkspaceRole::Owner)],
        "a refused probe must not have changed the workspace it probed",
    );
}

/// Agent credentials hold no human control-plane permission, so the boundary
/// is the credential rather than the person: the same owner reaches every
/// membership RPC with their own token and none of them with an agent one.
#[tokio::test]
async fn an_agent_credential_holds_no_workspace_control_plane() {
    let deployment = SharedDeployment::start().await;
    let ada = deployment.seed_user("ada", "Ada").await;
    let bob = deployment.seed_user("bob", "Bob").await;
    let owner = deployment.as_person(&ada).await;
    let agent = deployment.as_agent(&ada).await;

    assert_eq!(
        create_workspace(&agent, "agent-made")
            .await
            .expect_err("an agent credential cannot create a workspace")
            .code(),
        Code::PermissionDenied,
    );
    create_workspace(&owner, "team").await.expect("create");

    for (code, _, reasons) in control_plane_refusals(&agent, "team", &bob).await {
        assert_eq!(
            code,
            Code::PermissionDenied,
            "an agent credential must be refused the control plane of its owner's workspace",
        );
        assert!(reasons.is_empty(), "a denial must carry no Coral reason");
    }

    assert_eq!(
        membership_rows(list_workspaces(&agent).await.expect("an agent's listing")),
        vec![("team".to_string(), WorkspaceRole::Owner)],
        "the restriction is the control plane, not the workspace itself",
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
        membership_rows(
            list_workspaces(&member)
                .await
                .expect("the member's listing")
        ),
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
            membership_rows(list_workspaces(client).await.expect("listing")),
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
