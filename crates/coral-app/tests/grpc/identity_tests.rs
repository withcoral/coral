use std::collections::{BTreeMap, BTreeSet};
use std::io::ErrorKind;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use coral_api::v1::create_user_owned_identity_response::Event as UserCreateEvent;
use coral_api::v1::create_workspace_owned_identity_response::Event as WorkspaceCreateEvent;
use coral_api::v1::{
    AddIdentitySpecRequest, CreateUserOwnedIdentityRequest, CreateWorkspaceOwnedIdentityRequest,
    CreateWorkspaceRequest, CredentialMetadata, DeleteIdentitySpecRequest,
    DeleteUserOwnedIdentityRequest, DeleteWorkspaceOwnedIdentityRequest,
    FixedTokenUserOwnedIdentitySetup, FixedTokenWorkspaceOwnedIdentitySetup,
    GetUserOwnedIdentityRequest, GetWorkspaceOwnedIdentityRequest, Identity, IdentityOwner,
    ListUserOwnedIdentitiesRequest, ListWorkspaceOwnedIdentitiesRequest, Workspace,
    create_user_owned_identity_request, create_user_owned_identity_response,
    create_workspace_owned_identity_request, create_workspace_owned_identity_response,
};
use coral_api::{
    CORAL_ERROR_REASON_IDENTITY_NOT_FOUND, CORAL_ERROR_REASON_IDENTITY_SPEC_NOT_FOUND,
    CORAL_ERROR_REASON_WORKSPACE_NOT_FOUND,
};
use coral_app::{ServerBuilder, UserPrincipal, UserPrincipalProvider, UserPrincipalProviderError};
use coral_client::AppClient;
use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt as _, AsyncReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::sync::{Semaphore, mpsc};
use tokio_stream::StreamExt as _;
use tonic::metadata::MetadataMap;
use tonic::{Code, Request, Status};
use tonic_types::{ErrorDetail, StatusExt as _};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const USER_HEADER: &str = "x-test-user";
const DEVICE_RESPONSE: &str = r#"{"device_code":"device-code","user_code":"ABCD-1234","verification_uri":"https://provider.example/device","verification_uri_complete":"https://provider.example/device?user_code=ABCD-1234","expires_in":60,"interval":1}"#;
const TOKEN_RESPONSE: &str = r#"{"access_token":"access-token","refresh_token":"refresh-token","token_type":"Bearer","scope":"repo user"}"#;
const TEST_PHASE_TIMEOUT: Duration = Duration::from_secs(5);
const SERVER_SHUTDOWN_CANCELLED_MESSAGE: &str = "server is shutting down";

#[derive(Debug)]
struct HeaderPrincipalProvider;

#[tonic::async_trait]
impl UserPrincipalProvider for HeaderPrincipalProvider {
    async fn principal_for_metadata(
        &self,
        metadata: &MetadataMap,
    ) -> Result<UserPrincipal, UserPrincipalProviderError> {
        let user = metadata
            .get(USER_HEADER)
            .and_then(|value| value.to_str().ok())
            .ok_or_else(|| UserPrincipalProviderError::unauthenticated("missing test user"))?;
        UserPrincipal::for_user(user)
            .map_err(|_error| UserPrincipalProviderError::internal("invalid test user"))
    }
}

#[tokio::test]
async fn user_identity_service_is_scoped_validated_and_restart_safe() {
    let temp = TempDir::new().expect("temp dir");
    let config_dir = temp.path().join("coral-config");
    let (server, app) = start(&config_dir).await;

    let missing_principal = app
        .identity_client()
        .list_user_owned_identities(Request::new(ListUserOwnedIdentitiesRequest {}))
        .await
        .expect_err("missing principal must fail closed");
    assert_eq!(missing_principal.code(), Code::Unauthenticated);

    add_spec(&app, "alice_spec", "alice", "fixed_token").await;
    add_spec(&app, "bob_spec", "bob", "fixed_token").await;
    add_spec(&app, "oauth_spec", "oauth", "oauth").await;
    assert_create_validation(&app).await;

    let mut alice = create(&app, "alice", "shared", "alice_spec", "alice-token").await;
    let bob = create(&app, "bob", "shared", "bob_spec", "bob-token").await;
    assert_identity(&alice, "alice_spec", "alice");
    assert_identity(&bob, "bob_spec", "bob");
    assert_eq!(list(&app, "alice").await, vec![alice.clone()]);
    assert_eq!(list(&app, "bob").await, vec![bob.clone()]);

    let orphaned = app
        .identity_spec_client()
        .delete_identity_spec(for_user(
            DeleteIdentitySpecRequest {
                name: "alice_spec".to_string(),
                workspace: None,
                force: true,
            },
            "alice",
        ))
        .await
        .expect("force delete referenced spec")
        .into_inner();
    assert_eq!(orphaned.orphaned_identities, 1);
    server.shutdown().await.expect("shutdown first server");

    persist_safe_metadata(&config_dir, "alice", "shared").await;
    let encryption_key = config_dir.join("credentials").join("encryption.key");
    std::fs::rename(
        &encryption_key,
        config_dir
            .join("credentials")
            .join("encryption.key.unavailable"),
    )
    .expect("make original identity encryption key unavailable");
    alice.metadata = vec![
        CredentialMetadata {
            key: "scope".to_string(),
            value: "repo user".to_string(),
        },
        CredentialMetadata {
            key: "token_type".to_string(),
            value: "Bearer".to_string(),
        },
    ];

    let (server, restarted) = start(&config_dir).await;
    let listed = list(&restarted, "alice").await;
    let loaded = get(&restarted, "alice", "shared").await;
    assert_eq!(listed, vec![alice.clone()]);
    assert_eq!(loaded, alice);
    assert!(!format!("{listed:?}{loaded:?}").contains("alice-token"));
    assert_eq!(get(&restarted, "bob", "shared").await, bob);
    restarted
        .identity_client()
        .delete_user_owned_identity(for_user(
            DeleteUserOwnedIdentityRequest {
                name: "shared".to_string(),
            },
            "alice",
        ))
        .await
        .expect("delete Alice identity");
    let missing = restarted
        .identity_client()
        .get_user_owned_identity(for_user(
            GetUserOwnedIdentityRequest {
                name: "shared".to_string(),
            },
            "alice",
        ))
        .await
        .expect_err("Alice identity was deleted");
    assert_eq!(missing.code(), Code::NotFound);
    assert_error_reason(&missing, CORAL_ERROR_REASON_IDENTITY_NOT_FOUND);
    let missing_delete = restarted
        .identity_client()
        .delete_user_owned_identity(for_user(
            DeleteUserOwnedIdentityRequest {
                name: "shared".to_string(),
            },
            "alice",
        ))
        .await
        .expect_err("repeated delete must remain typed");
    assert_eq!(missing_delete.code(), Code::NotFound);
    assert_error_reason(&missing_delete, CORAL_ERROR_REASON_IDENTITY_NOT_FOUND);
    assert_eq!(get(&restarted, "bob", "shared").await, bob);
    server.shutdown().await.expect("shutdown restarted server");
}

#[tokio::test]
#[expect(clippy::too_many_lines, reason = "end-to-end OAuth lifecycle contract")]
async fn oauth_identity_creation_streams_ordered_user_and_workspace_events() {
    let temp = TempDir::new().expect("temp dir");
    let config_dir = temp.path().join("coral-config");
    let (server, app) = start(&config_dir).await;
    let workspace = create_workspace(&app, "oauth_owner").await;
    let other_workspace = create_workspace(&app, "oauth_other").await;
    let colliding_owner_key = workspace.name.clone();
    let identity_name = "same-oauth";
    let spec = "shared_oauth";
    let provider = oauth_fixture(&app, &workspace, spec).await;
    let user_events = app
        .identity_client()
        .create_user_owned_identity(user_oauth_request(
            identity_name,
            spec,
            &colliding_owner_key,
        ))
        .await
        .expect("create user OAuth identity")
        .into_inner()
        .map(|response| response.expect("OAuth event").event.expect("event"))
        .collect::<Vec<_>>()
        .await;
    let [
        UserCreateEvent::OauthAuthorization(user_authorization),
        UserCreateEvent::OauthCompleted(user_completed),
        UserCreateEvent::Identity(user),
    ] = user_events.as_slice()
    else {
        panic!("expected authorization, completion, identity, and EOF: {user_events:?}");
    };
    assert_eq!(user.owner, IdentityOwner::User as i32);
    assert_eq!(user.issuer, "global_oauth");
    assert!(user.owner_workspace.is_none());
    assert!(user.identity_spec_workspace.is_none());

    let workspace_request =
        workspace_oauth_request(&workspace, identity_name, spec, "workspace-caller");
    let workspace_events = app
        .workspace_identity_client()
        .create_workspace_owned_identity(workspace_request)
        .await
        .expect("create workspace OAuth identity")
        .into_inner()
        .map(|response| response.expect("OAuth event").event.expect("event"))
        .collect::<Vec<_>>()
        .await;
    let [
        WorkspaceCreateEvent::OauthAuthorization(workspace_authorization),
        WorkspaceCreateEvent::OauthCompleted(workspace_completed),
        WorkspaceCreateEvent::Identity(shared),
    ] = workspace_events.as_slice()
    else {
        panic!("expected authorization, completion, identity, and EOF: {workspace_events:?}");
    };
    assert_eq!(shared.owner, IdentityOwner::Workspace as i32);
    assert_eq!(shared.issuer, "workspace_oauth");
    assert_eq!(shared.owner_workspace.as_ref(), Some(&workspace));
    assert_eq!(shared.identity_spec_workspace.as_ref(), Some(&workspace));

    assert_eq!(workspace_authorization, user_authorization);
    assert_eq!(
        user_authorization.authorization_url,
        "https://provider.example/device?user_code=ABCD-1234"
    );
    assert_eq!(user_authorization.expires_in_seconds, 60);
    assert_eq!(user_authorization.user_code, "ABCD-1234");
    assert_eq!(
        user_authorization.verification_uri,
        "https://provider.example/device"
    );
    assert_eq!(
        user_authorization.verification_uri_complete,
        user_authorization.authorization_url
    );
    let metadata =
        [("scope", "repo user"), ("token_type", "Bearer")].map(|(key, value)| CredentialMetadata {
            key: key.to_string(),
            value: value.to_string(),
        });
    assert_eq!(user_completed.metadata, metadata);
    assert_eq!(workspace_completed.metadata, metadata);
    assert_eq!(user.metadata, metadata);
    assert_eq!(shared.metadata, metadata);

    let requests = provider
        .received_requests()
        .await
        .expect("recorded OAuth requests");
    for (request, (endpoint, client)) in requests.iter().zip([
        ("/device", "global-client"),
        ("/token", "global-client"),
        ("/device", "workspace-client"),
        ("/token", "workspace-client"),
    ]) {
        assert_eq!(request.url.path(), endpoint);
        assert!(String::from_utf8_lossy(&request.body).contains(&format!("client_id={client}")));
    }
    assert_eq!(requests.len(), 4);
    let public_responses = format!("{user_events:?}{workspace_events:?}");
    for secret in ["access-token", "refresh-token", "device-code"] {
        assert!(!public_responses.contains(secret));
    }
    server.shutdown().await.expect("shutdown server");
    drop(app);

    let (server, restarted) = start(&config_dir).await;
    assert_eq!(
        get(&restarted, &colliding_owner_key, identity_name).await,
        *user
    );
    assert_eq!(
        workspace_get(&restarted, "workspace-caller", &workspace, identity_name,).await,
        *shared
    );
    let other_user = user_get_status(&restarted, "other-user", identity_name).await;
    assert_eq!(other_user.code(), Code::NotFound);
    assert_error_reason(&other_user, CORAL_ERROR_REASON_IDENTITY_NOT_FOUND);
    let other_workspace = workspace_get_status(
        &restarted,
        "workspace-caller",
        &other_workspace,
        identity_name,
    )
    .await;
    assert_eq!(other_workspace.code(), Code::NotFound);
    assert_error_reason(&other_workspace, CORAL_ERROR_REASON_IDENTITY_NOT_FOUND);
    server.shutdown().await.expect("shutdown restarted server");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dropping_native_oauth_stream_cancels_before_persistence() {
    const IDENTITY: &str = "dropped-user-oauth";

    let temp = TempDir::new().expect("temp dir");
    let config_dir = temp.path().join("coral-config");
    let (server, app) = start(&config_dir).await;
    let workspace = create_workspace(&app, "oauth_drop_workspace").await;
    let mut provider = GatedDeviceOAuthProvider::new(1).await;
    add_oauth_specs(
        &app,
        &workspace,
        "drop_oauth",
        &provider.device_url,
        &provider.token_url,
    )
    .await;

    let mut client = app.identity_client();
    let mut stream = client
        .create_user_owned_identity(user_oauth_request(IDENTITY, "drop_oauth", "alice"))
        .await
        .expect("start user OAuth stream")
        .into_inner();
    let event = tokio::time::timeout(TEST_PHASE_TIMEOUT, stream.message())
        .await
        .expect("OAuth authorization timed out")
        .expect("read OAuth authorization")
        .expect("OAuth authorization response")
        .event
        .expect("OAuth authorization event");
    assert!(matches!(event, UserCreateEvent::OauthAuthorization(_)));
    assert_eq!(
        provider.wait_for_token_clients().await,
        BTreeSet::from(["global-client".to_string()])
    );

    drop(stream);
    drop(client);
    provider.wait_for_token_disconnect().await;
    assert_identity_tables_empty(&config_dir).await;

    provider.release_token_responses();
    let requests = provider.received_requests();
    provider.finish().await;
    assert_eq!(
        requests
            .iter()
            .map(|request| request.path.as_str())
            .collect::<Vec<_>>(),
        ["/device", "/token"]
    );
    assert_identity_tables_empty(&config_dir).await;

    server.shutdown().await.expect("shutdown server");
    drop(app);
    let (server, restarted) = start(&config_dir).await;
    let missing = user_get_status(&restarted, "alice", IDENTITY).await;
    assert_eq!(missing.code(), Code::NotFound);
    assert_error_reason(&missing, CORAL_ERROR_REASON_IDENTITY_NOT_FOUND);
    server.shutdown().await.expect("shutdown restarted server");
}

#[tokio::test]
async fn native_oauth_surfaces_terminal_provider_failure_after_authorization() {
    const HOSTILE_SECRET: &str = "hostile-access-refresh-device-secret";

    let temp = TempDir::new().expect("temp dir");
    let config_dir = temp.path().join("coral-config");
    let (server, app) = start(&config_dir).await;
    let workspace = create_workspace(&app, "oauth_failure_workspace").await;
    let provider = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/device"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(DEVICE_RESPONSE, "application/json"))
        .expect(1)
        .mount(&provider)
        .await;
    Mock::given(method("POST"))
        .and(path("/token"))
        .respond_with(ResponseTemplate::new(500).set_body_raw(
            format!(r#"{{"error":"server_error","error_description":"{HOSTILE_SECRET}"}}"#),
            "application/json",
        ))
        .expect(1)
        .mount(&provider)
        .await;
    add_oauth_specs(
        &app,
        &workspace,
        "failure_oauth",
        &format!("{}/device", provider.uri()),
        &format!("{}/token", provider.uri()),
    )
    .await;

    let mut stream = app
        .identity_client()
        .create_user_owned_identity(user_oauth_request(
            "failed-user-oauth",
            "failure_oauth",
            "alice",
        ))
        .await
        .expect("start user OAuth stream")
        .into_inner();
    let authorization = stream
        .message()
        .await
        .expect("read OAuth authorization")
        .expect("OAuth authorization response")
        .event
        .expect("OAuth authorization event");
    assert!(matches!(
        authorization,
        UserCreateEvent::OauthAuthorization(_)
    ));
    let status = stream
        .message()
        .await
        .expect_err("token HTTP failure must terminate the OAuth stream");
    assert_eq!(status.code(), Code::FailedPrecondition);
    assert_eq!(
        status.message(),
        "failed precondition: OAuth device token request failed with HTTP 500"
    );
    assert!(!status.message().contains(HOSTILE_SECRET));
    assert!(
        stream
            .message()
            .await
            .expect("read terminal OAuth EOF")
            .is_none()
    );
    provider.verify().await;
    let requests = provider
        .received_requests()
        .await
        .expect("recorded OAuth requests");
    assert_eq!(
        requests
            .iter()
            .map(|request| request.url.path())
            .collect::<Vec<_>>(),
        ["/device", "/token"]
    );
    assert_identity_tables_empty(&config_dir).await;
    server.shutdown().await.expect("shutdown server");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn server_shutdown_cancels_active_identity_oauth_without_persisting() {
    const USER_IDENTITY: &str = "shutdown-user-oauth";
    const WORKSPACE_IDENTITY: &str = "shutdown-workspace-oauth";

    let temp = TempDir::new().expect("temp dir");
    let config_dir = temp.path().join("coral-config");
    let (server, app) = start(&config_dir).await;
    let workspace = create_workspace(&app, "oauth_shutdown_workspace").await;
    let mut provider = GatedDeviceOAuthProvider::new(2).await;
    add_oauth_specs(
        &app,
        &workspace,
        "shutdown_oauth",
        &provider.device_url,
        &provider.token_url,
    )
    .await;

    let mut user_client = app.identity_client();
    let mut workspace_client = app.workspace_identity_client();
    let (user_stream, workspace_stream) = tokio::time::timeout(TEST_PHASE_TIMEOUT, async {
        tokio::join!(
            user_client.create_user_owned_identity(user_oauth_request(
                USER_IDENTITY,
                "shutdown_oauth",
                "alice",
            )),
            workspace_client.create_workspace_owned_identity(workspace_oauth_request(
                &workspace,
                WORKSPACE_IDENTITY,
                "shutdown_oauth",
                "bob",
            )),
        )
    })
    .await
    .expect("identity OAuth stream acquisition timed out");
    let mut user_stream = user_stream.expect("start user OAuth stream").into_inner();
    let mut workspace_stream = workspace_stream
        .expect("start workspace OAuth stream")
        .into_inner();

    let user_event = tokio::time::timeout(TEST_PHASE_TIMEOUT, user_stream.message())
        .await
        .expect("user OAuth authorization timed out")
        .expect("read user OAuth authorization")
        .expect("user OAuth authorization response")
        .event
        .expect("user OAuth authorization event");
    let UserCreateEvent::OauthAuthorization(user_authorization) = user_event else {
        panic!("expected user OAuth authorization, got {user_event:?}");
    };
    let workspace_event = tokio::time::timeout(TEST_PHASE_TIMEOUT, workspace_stream.message())
        .await
        .expect("workspace OAuth authorization timed out")
        .expect("read workspace OAuth authorization")
        .expect("workspace OAuth authorization response")
        .event
        .expect("workspace OAuth authorization event");
    let WorkspaceCreateEvent::OauthAuthorization(workspace_authorization) = workspace_event else {
        panic!("expected workspace OAuth authorization, got {workspace_event:?}");
    };
    assert_eq!(workspace_authorization, user_authorization);

    let user_tail = tokio::spawn(terminal_oauth_stream_status(user_stream, "user"));
    let workspace_tail = tokio::spawn(terminal_oauth_stream_status(workspace_stream, "workspace"));
    let token_clients = provider.wait_for_token_clients().await;
    assert_eq!(
        token_clients,
        BTreeSet::from(["global-client".to_string(), "workspace-client".to_string()])
    );

    tokio::time::timeout(Duration::from_secs(2), server.shutdown())
        .await
        .expect("server shutdown waited on active identity OAuth streams")
        .expect("shutdown server");
    assert_identity_tables_empty(&config_dir).await;

    provider.release_token_responses();
    let user_status = tokio::time::timeout(TEST_PHASE_TIMEOUT, user_tail)
        .await
        .expect("user OAuth stream did not terminate")
        .expect("join user OAuth stream");
    let workspace_status = tokio::time::timeout(TEST_PHASE_TIMEOUT, workspace_tail)
        .await
        .expect("workspace OAuth stream did not terminate")
        .expect("join workspace OAuth stream");
    assert_shutdown_cancelled(&user_status);
    assert_shutdown_cancelled(&workspace_status);
    provider.finish().await;
    assert_identity_tables_empty(&config_dir).await;

    drop(user_client);
    drop(workspace_client);
    drop(app);
    let (restarted_server, restarted) = start(&config_dir).await;
    let user_missing = user_get_status(&restarted, "alice", USER_IDENTITY).await;
    assert_eq!(user_missing.code(), Code::NotFound);
    assert_error_reason(&user_missing, CORAL_ERROR_REASON_IDENTITY_NOT_FOUND);
    let workspace_missing =
        workspace_get_status(&restarted, "bob", &workspace, WORKSPACE_IDENTITY).await;
    assert_eq!(workspace_missing.code(), Code::NotFound);
    assert_error_reason(&workspace_missing, CORAL_ERROR_REASON_IDENTITY_NOT_FOUND);
    restarted_server
        .shutdown()
        .await
        .expect("shutdown restarted server");
}

#[tokio::test]
async fn workspace_identity_service_pins_scope_and_isolates_workspaces() {
    let temp = TempDir::new().expect("temp dir");
    let (server, app) = start(&temp.path().join("coral-config")).await;
    let alpha = create_workspace(&app, "alpha").await;
    let beta = create_workspace(&app, "beta").await;

    add_spec(&app, "shared_spec", "global", "fixed_token").await;
    add_workspace_spec(&app, &alpha, "shared_spec", "alpha").await;
    let alpha_identity = create_workspace_identity(
        &app,
        "alice",
        &alpha,
        "shared",
        "shared_spec",
        "alpha-token",
    )
    .await;
    let beta_fallback =
        create_workspace_identity(&app, "bob", &beta, "shared", "shared_spec", "beta-token").await;
    assert_workspace_identity(
        &alpha_identity,
        &alpha,
        Some(&alpha),
        "alpha",
        "alpha-token",
    );
    assert_workspace_identity(&beta_fallback, &beta, None, "global", "beta-token");

    // Workspace ownership is request-selected on the authenticated local-app transport;
    // it is deliberately independent of the authenticated user principal.
    assert_eq!(
        workspace_list(&app, "bob", &alpha).await,
        vec![alpha_identity.clone()]
    );

    add_workspace_spec(&app, &beta, "shared_spec", "beta_late").await;
    assert_eq!(
        workspace_get(&app, "alice", &beta, "shared").await,
        beta_fallback
    );
    let beta_shadowed = create_workspace_identity(
        &app,
        "alice",
        &beta,
        "shared",
        "shared_spec",
        "beta-replacement-token",
    )
    .await;
    assert_workspace_identity(
        &beta_shadowed,
        &beta,
        Some(&beta),
        "beta_late",
        "beta-replacement-token",
    );

    let orphaned = app
        .identity_spec_client()
        .delete_identity_spec(for_user(
            DeleteIdentitySpecRequest {
                name: "shared_spec".to_string(),
                workspace: Some(alpha.clone()),
                force: true,
            },
            "bob",
        ))
        .await
        .expect("force delete exact workspace spec")
        .into_inner();
    assert_eq!(orphaned.orphaned_identities, 1);
    assert_eq!(
        workspace_get(&app, "bob", &alpha, "shared").await,
        alpha_identity
    );
    delete_workspace_identity(&app, "bob", &alpha, "shared").await;
    let missing = workspace_get_status(&app, "alice", &alpha, "shared").await;
    assert_eq!(missing.code(), Code::NotFound);
    assert_error_reason(&missing, CORAL_ERROR_REASON_IDENTITY_NOT_FOUND);
    assert_eq!(
        workspace_get(&app, "bob", &beta, "shared").await,
        beta_shadowed
    );
    server.shutdown().await.expect("shutdown server");
}

#[tokio::test]
async fn workspace_identity_service_validates_workspace_boundaries() {
    let temp = TempDir::new().expect("temp dir");
    let (server, app) = start(&temp.path().join("coral-config")).await;
    let missing = workspace("missing");

    let unauthenticated = app
        .workspace_identity_client()
        .list_workspace_owned_identities(Request::new(ListWorkspaceOwnedIdentitiesRequest {
            workspace: Some(missing.clone()),
        }))
        .await
        .expect_err("missing principal must fail closed");
    assert_eq!(unauthenticated.code(), Code::Unauthenticated);

    for workspace in [None, Some(workspace("bad/name"))] {
        let invalid = app
            .workspace_identity_client()
            .list_workspace_owned_identities(for_user(
                ListWorkspaceOwnedIdentitiesRequest { workspace },
                "alice",
            ))
            .await
            .expect_err("invalid workspace must fail");
        assert_eq!(invalid.code(), Code::InvalidArgument);
    }

    let statuses = vec![
        workspace_create_status(
            &app,
            workspace_create_request(&missing, "bad/name", "bad/name", " ", "alice"),
        )
        .await,
        workspace_list_status(&app, "alice", &missing).await,
        workspace_get_status(&app, "alice", &missing, "bad/name").await,
        workspace_delete_status(&app, "alice", &missing, "bad/name").await,
    ];
    for status in statuses {
        assert_eq!(status.code(), Code::NotFound);
        assert_error_reason(&status, CORAL_ERROR_REASON_WORKSPACE_NOT_FOUND);
    }

    let existing = create_workspace(&app, "existing").await;
    let missing_identity = workspace_get_status(&app, "alice", &existing, "missing").await;
    assert_eq!(missing_identity.code(), Code::NotFound);
    assert_error_reason(&missing_identity, CORAL_ERROR_REASON_IDENTITY_NOT_FOUND);
    let omitted = workspace_oauth_status(
        &app,
        workspace_oauth_request(&existing, "shared", "missing", "alice"),
    )
    .await;
    assert_eq!(omitted.code(), Code::NotFound);
    assert_error_reason(&omitted, CORAL_ERROR_REASON_IDENTITY_SPEC_NOT_FOUND);
    server.shutdown().await.expect("shutdown server");
}

async fn assert_create_validation(app: &AppClient) {
    let omitted = user_oauth_status(app, user_oauth_request("shared", "alice_spec", "alice")).await;
    assert_eq!(omitted.code(), Code::InvalidArgument);
    assert_eq!(
        create_status(
            app,
            create_request("shared", "oauth_spec", "token", "alice")
        )
        .await
        .code(),
        Code::InvalidArgument
    );
    for request in [
        create_request("shared", "alice_spec", "  ", "alice"),
        create_request("bad/name", "alice_spec", "token", "alice"),
    ] {
        assert_eq!(
            create_status(app, request).await.code(),
            Code::InvalidArgument
        );
    }
    let missing_spec =
        user_oauth_status(app, user_oauth_request("shared", "missing", "alice")).await;
    assert_eq!(missing_spec.code(), Code::NotFound);
    assert_error_reason(&missing_spec, CORAL_ERROR_REASON_IDENTITY_SPEC_NOT_FOUND);
}

async fn start(config_dir: &std::path::Path) -> (coral_app::RunningServer, AppClient) {
    let server = ServerBuilder::new()
        .with_config_dir(config_dir)
        .with_user_principal_provider(Arc::new(HeaderPrincipalProvider))
        .start()
        .await
        .expect("start server");
    let app = AppClient::connect(server.endpoint_uri())
        .await
        .expect("connect client");
    (server, app)
}

async fn terminal_oauth_stream_status<T>(
    mut stream: tonic::Streaming<T>,
    label: &'static str,
) -> Status
where
    T: std::fmt::Debug + Send + 'static,
{
    match stream.message().await {
        Ok(Some(response)) => {
            panic!("{label} OAuth stream emitted an event after shutdown: {response:?}")
        }
        Ok(None) => panic!("{label} OAuth stream ended without cancellation status"),
        Err(status) => status,
    }
}

fn assert_shutdown_cancelled(status: &Status) {
    assert_eq!(status.code(), Code::Cancelled, "{status:?}");
    assert_eq!(status.message(), SERVER_SHUTDOWN_CANCELLED_MESSAGE);
}

async fn assert_identity_tables_empty(config_dir: &Path) {
    let options = sqlx::sqlite::SqliteConnectOptions::new()
        .filename(config_dir.join("coral.db"))
        .create_if_missing(false);
    let pool = sqlx::SqlitePool::connect_with(options)
        .await
        .expect("open identity database for shutdown assertion");
    let identity_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM identities")
        .fetch_one(&pool)
        .await
        .expect("count identities after shutdown");
    let document_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM identity_documents")
        .fetch_one(&pool)
        .await
        .expect("count identity documents after shutdown");
    pool.close().await;
    assert_eq!((identity_count, document_count), (0, 0));
}

async fn persist_safe_metadata(config_dir: &std::path::Path, owner_key: &str, name: &str) {
    let options = sqlx::sqlite::SqliteConnectOptions::new().filename(config_dir.join("coral.db"));
    let pool = sqlx::SqlitePool::connect_with(options)
        .await
        .expect("open identity database for metadata fixture");
    let result = sqlx::query(
        "UPDATE identities SET safe_metadata_json = ?
         WHERE owner_kind = 'user' AND owner_key = ? AND name = ?",
    )
    .bind(r#"{"scope":"repo user","token_type":"Bearer"}"#)
    .bind(owner_key)
    .bind(name)
    .execute(&pool)
    .await
    .expect("persist canonical safe metadata fixture");
    assert_eq!(result.rows_affected(), 1);
    pool.close().await;
}

async fn add_spec(app: &AppClient, name: &str, issuer: &str, kind: &str) {
    add_scoped_spec(app, None, name, issuer, kind).await;
}

async fn add_workspace_spec(app: &AppClient, workspace: &Workspace, name: &str, issuer: &str) {
    add_scoped_spec(app, Some(workspace.clone()), name, issuer, "fixed_token").await;
}

async fn add_scoped_spec(
    app: &AppClient,
    workspace: Option<Workspace>,
    name: &str,
    issuer: &str,
    kind: &str,
) {
    app.identity_spec_client()
        .add_identity_spec(for_user(
            AddIdentitySpecRequest {
                manifest_yaml: manifest(name, issuer, kind),
                input_values: Vec::new(),
                workspace,
            },
            "alice",
        ))
        .await
        .expect("add identity spec");
}

async fn oauth_fixture(app: &AppClient, workspace: &Workspace, spec: &str) -> MockServer {
    let provider = MockServer::start().await;
    for (endpoint, response) in [("/device", DEVICE_RESPONSE), ("/token", TOKEN_RESPONSE)] {
        Mock::given(method("POST"))
            .and(path(endpoint))
            .respond_with(ResponseTemplate::new(200).set_body_raw(response, "application/json"))
            .mount(&provider)
            .await;
    }
    let device_url = format!("{}/device", provider.uri());
    let token_url = format!("{}/token", provider.uri());
    add_oauth_specs(app, workspace, spec, &device_url, &token_url).await;
    provider
}

async fn add_oauth_specs(
    app: &AppClient,
    workspace: &Workspace,
    spec: &str,
    device_url: &str,
    token_url: &str,
) {
    let workspace_scope = Some(workspace.clone());
    for (scope, issuer, client) in [
        (None, "global_oauth", "global-client"),
        (workspace_scope, "workspace_oauth", "workspace-client"),
    ] {
        app.identity_spec_client()
            .add_identity_spec(for_user(
                AddIdentitySpecRequest {
                    manifest_yaml: device_manifest(spec, issuer, device_url, token_url, client),
                    input_values: Vec::new(),
                    workspace: scope,
                },
                "alice",
            ))
            .await
            .expect("add OAuth identity spec");
    }
}

async fn create_workspace(app: &AppClient, name: &str) -> Workspace {
    let workspace = workspace(name);
    app.workspace_client()
        .create_workspace(for_user(
            CreateWorkspaceRequest {
                workspace: Some(workspace.clone()),
            },
            "alice",
        ))
        .await
        .expect("create workspace");
    workspace
}

async fn create_workspace_identity(
    app: &AppClient,
    user: &str,
    workspace: &Workspace,
    name: &str,
    spec: &str,
    token: &str,
) -> Identity {
    let mut stream = app
        .workspace_identity_client()
        .create_workspace_owned_identity(workspace_create_request(
            workspace, name, spec, token, user,
        ))
        .await
        .expect("create workspace identity")
        .into_inner();
    let response = stream
        .message()
        .await
        .expect("read workspace create stream")
        .expect("one workspace identity event");
    assert!(stream.message().await.expect("read stream EOF").is_none());
    match response.event.expect("workspace identity event") {
        create_workspace_owned_identity_response::Event::Identity(identity) => identity,
        event => panic!("expected workspace identity event, got {event:?}"),
    }
}

async fn workspace_create_status(
    app: &AppClient,
    request: Request<CreateWorkspaceOwnedIdentityRequest>,
) -> Status {
    app.workspace_identity_client()
        .create_workspace_owned_identity(request)
        .await
        .expect_err("workspace identity creation must fail")
}

async fn workspace_oauth_status(
    app: &AppClient,
    request: Request<CreateWorkspaceOwnedIdentityRequest>,
) -> Status {
    app.workspace_identity_client()
        .create_workspace_owned_identity(request)
        .await
        .expect("workspace OAuth stream")
        .into_inner()
        .message()
        .await
        .expect_err("workspace OAuth stream must fail")
}

async fn workspace_list(app: &AppClient, user: &str, workspace: &Workspace) -> Vec<Identity> {
    app.workspace_identity_client()
        .list_workspace_owned_identities(for_user(
            ListWorkspaceOwnedIdentitiesRequest {
                workspace: Some(workspace.clone()),
            },
            user,
        ))
        .await
        .expect("list workspace identities")
        .into_inner()
        .identities
}

async fn workspace_list_status(app: &AppClient, user: &str, workspace: &Workspace) -> Status {
    app.workspace_identity_client()
        .list_workspace_owned_identities(for_user(
            ListWorkspaceOwnedIdentitiesRequest {
                workspace: Some(workspace.clone()),
            },
            user,
        ))
        .await
        .expect_err("workspace identity list must fail")
}

async fn workspace_get(app: &AppClient, user: &str, workspace: &Workspace, name: &str) -> Identity {
    app.workspace_identity_client()
        .get_workspace_owned_identity(for_user(
            GetWorkspaceOwnedIdentityRequest {
                workspace: Some(workspace.clone()),
                name: name.to_string(),
            },
            user,
        ))
        .await
        .expect("get workspace identity")
        .into_inner()
        .identity
        .expect("workspace identity response")
}

async fn workspace_get_status(
    app: &AppClient,
    user: &str,
    workspace: &Workspace,
    name: &str,
) -> Status {
    app.workspace_identity_client()
        .get_workspace_owned_identity(for_user(
            GetWorkspaceOwnedIdentityRequest {
                workspace: Some(workspace.clone()),
                name: name.to_string(),
            },
            user,
        ))
        .await
        .expect_err("workspace identity get must fail")
}

async fn user_get_status(app: &AppClient, user: &str, name: &str) -> Status {
    app.identity_client()
        .get_user_owned_identity(for_user(
            GetUserOwnedIdentityRequest {
                name: name.to_string(),
            },
            user,
        ))
        .await
        .expect_err("user identity get must fail")
}

async fn delete_workspace_identity(app: &AppClient, user: &str, workspace: &Workspace, name: &str) {
    app.workspace_identity_client()
        .delete_workspace_owned_identity(for_user(
            DeleteWorkspaceOwnedIdentityRequest {
                workspace: Some(workspace.clone()),
                name: name.to_string(),
            },
            user,
        ))
        .await
        .expect("delete workspace identity");
}

async fn workspace_delete_status(
    app: &AppClient,
    user: &str,
    workspace: &Workspace,
    name: &str,
) -> Status {
    app.workspace_identity_client()
        .delete_workspace_owned_identity(for_user(
            DeleteWorkspaceOwnedIdentityRequest {
                workspace: Some(workspace.clone()),
                name: name.to_string(),
            },
            user,
        ))
        .await
        .expect_err("workspace identity delete must fail")
}

async fn create_status(
    app: &AppClient,
    request: Request<CreateUserOwnedIdentityRequest>,
) -> Status {
    app.identity_client()
        .create_user_owned_identity(request)
        .await
        .expect_err("identity creation must fail")
}

async fn user_oauth_status(
    app: &AppClient,
    request: Request<CreateUserOwnedIdentityRequest>,
) -> Status {
    app.identity_client()
        .create_user_owned_identity(request)
        .await
        .expect("user OAuth stream")
        .into_inner()
        .message()
        .await
        .expect_err("user OAuth stream must fail")
}

async fn create(app: &AppClient, user: &str, name: &str, spec: &str, token: &str) -> Identity {
    let mut stream = app
        .identity_client()
        .create_user_owned_identity(create_request(name, spec, token, user))
        .await
        .expect("create identity")
        .into_inner();
    let response = stream
        .message()
        .await
        .expect("read create stream")
        .expect("one identity event");
    assert!(stream.message().await.expect("read stream EOF").is_none());
    match response.event.expect("identity event") {
        create_user_owned_identity_response::Event::Identity(identity) => identity,
        event => panic!("expected identity event, got {event:?}"),
    }
}

async fn list(app: &AppClient, user: &str) -> Vec<Identity> {
    app.identity_client()
        .list_user_owned_identities(for_user(ListUserOwnedIdentitiesRequest {}, user))
        .await
        .expect("list identities")
        .into_inner()
        .identities
}

async fn get(app: &AppClient, user: &str, name: &str) -> Identity {
    app.identity_client()
        .get_user_owned_identity(for_user(
            GetUserOwnedIdentityRequest {
                name: name.to_string(),
            },
            user,
        ))
        .await
        .expect("get identity")
        .into_inner()
        .identity
        .expect("identity response")
}

fn create_request(
    name: &str,
    spec: &str,
    token: &str,
    user: &str,
) -> Request<CreateUserOwnedIdentityRequest> {
    for_user(
        CreateUserOwnedIdentityRequest {
            name: name.to_string(),
            identity_spec: spec.to_string(),
            setup: Some(create_user_owned_identity_request::Setup::FixedToken(
                FixedTokenUserOwnedIdentitySetup {
                    token: token.to_string(),
                },
            )),
        },
        user,
    )
}

fn user_oauth_request(
    name: &str,
    spec: &str,
    user: &str,
) -> Request<CreateUserOwnedIdentityRequest> {
    for_user(
        CreateUserOwnedIdentityRequest {
            name: name.to_string(),
            identity_spec: spec.to_string(),
            setup: None,
        },
        user,
    )
}

fn workspace_create_request(
    workspace: &Workspace,
    name: &str,
    spec: &str,
    token: &str,
    user: &str,
) -> Request<CreateWorkspaceOwnedIdentityRequest> {
    for_user(
        CreateWorkspaceOwnedIdentityRequest {
            workspace: Some(workspace.clone()),
            name: name.to_string(),
            identity_spec: spec.to_string(),
            setup: Some(create_workspace_owned_identity_request::Setup::FixedToken(
                FixedTokenWorkspaceOwnedIdentitySetup {
                    token: token.to_string(),
                },
            )),
        },
        user,
    )
}

fn workspace_oauth_request(
    workspace: &Workspace,
    name: &str,
    spec: &str,
    user: &str,
) -> Request<CreateWorkspaceOwnedIdentityRequest> {
    for_user(
        CreateWorkspaceOwnedIdentityRequest {
            workspace: Some(workspace.clone()),
            name: name.to_string(),
            identity_spec: spec.to_string(),
            setup: None,
        },
        user,
    )
}

fn workspace(name: &str) -> Workspace {
    Workspace {
        name: name.to_string(),
    }
}

fn for_user<T>(message: T, user: &str) -> Request<T> {
    let mut request = Request::new(message);
    request.metadata_mut().insert(
        USER_HEADER,
        user.parse().expect("valid user metadata value"),
    );
    request
}

fn assert_identity(identity: &Identity, spec: &str, issuer: &str) {
    assert_eq!(identity.name, "shared");
    assert_eq!(identity.identity_spec, spec);
    assert_eq!(identity.issuer, issuer);
    assert_eq!(identity.identity_type, "fixed_token");
    assert_eq!(identity.owner, IdentityOwner::User as i32);
    assert!(identity.metadata.is_empty());
    assert!(identity.owner_workspace.is_none());
    assert!(identity.identity_spec_workspace.is_none());
}

fn assert_workspace_identity(
    identity: &Identity,
    owner_workspace: &Workspace,
    spec_workspace: Option<&Workspace>,
    issuer: &str,
    token: &str,
) {
    assert_eq!(identity.name, "shared");
    assert_eq!(identity.identity_spec, "shared_spec");
    assert_eq!(identity.issuer, issuer);
    assert_eq!(identity.identity_type, "fixed_token");
    assert_eq!(identity.owner, IdentityOwner::Workspace as i32);
    assert!(identity.metadata.is_empty());
    assert_eq!(identity.owner_workspace.as_ref(), Some(owner_workspace));
    assert_eq!(identity.identity_spec_workspace.as_ref(), spec_workspace);
    assert!(!format!("{identity:?}").contains(token));
}

struct GatedDeviceOAuthProvider {
    device_url: String,
    token_url: String,
    expected_flows: usize,
    token_requests: mpsc::UnboundedReceiver<BTreeMap<String, String>>,
    token_disconnects: mpsc::UnboundedReceiver<()>,
    token_release: Arc<Semaphore>,
    requests: Arc<Mutex<Vec<FixtureRequest>>>,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl GatedDeviceOAuthProvider {
    async fn new(expected_flows: usize) -> Self {
        assert!(expected_flows > 0, "provider must serve at least one flow");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind gated OAuth provider");
        let address = listener.local_addr().expect("gated OAuth provider address");
        let (token_requests_tx, token_requests) = mpsc::unbounded_channel();
        let (token_disconnects_tx, token_disconnects) = mpsc::unbounded_channel();
        let token_release = Arc::new(Semaphore::new(0));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let task_token_release = Arc::clone(&token_release);
        let task_requests = Arc::clone(&requests);
        let task = tokio::spawn(async move {
            let mut handlers = tokio::task::JoinSet::new();
            for _ in 0..expected_flows.saturating_mul(2) {
                let (mut socket, _) = tokio::time::timeout(TEST_PHASE_TIMEOUT, listener.accept())
                    .await
                    .expect("OAuth provider accept timed out")
                    .expect("accept OAuth provider request");
                let token_requests_tx = token_requests_tx.clone();
                let token_disconnects_tx = token_disconnects_tx.clone();
                let token_release = Arc::clone(&task_token_release);
                let requests = Arc::clone(&task_requests);
                handlers.spawn(async move {
                    let request = read_fixture_request(&mut socket).await;
                    requests
                        .lock()
                        .expect("record OAuth provider request")
                        .push(request.clone());
                    match request.path.as_str() {
                        "/device" => write_fixture_json(&mut socket, DEVICE_RESPONSE)
                            .await
                            .expect("write device authorization response"),
                        "/token" => {
                            token_requests_tx
                                .send(request.form)
                                .expect("record in-flight OAuth token request");
                            let mut disconnect_probe = [0_u8; 1];
                            tokio::select! {
                                permit = token_release.acquire_owned() => {
                                    permit.expect("token response gate closed").forget();
                                    let _response =
                                        write_fixture_json(&mut socket, TOKEN_RESPONSE).await;
                                }
                                disconnected = socket.read(&mut disconnect_probe) => {
                                    match disconnected {
                                        Ok(0) => {}
                                        Err(error)
                                            if matches!(
                                                error.kind(),
                                                ErrorKind::ConnectionReset
                                                    | ErrorKind::ConnectionAborted
                                                    | ErrorKind::NotConnected
                                            ) => {}
                                        Ok(read) => panic!(
                                            "OAuth client sent {read} unexpected bytes after its request"
                                        ),
                                        Err(error) => panic!(
                                            "unexpected token connection cancellation error: {error}"
                                        ),
                                    }
                                    token_disconnects_tx
                                        .send(())
                                        .expect("record token connection cancellation");
                                }
                            }
                        }
                        path => panic!("unexpected OAuth provider path: {path}"),
                    }
                });
            }
            drop(token_requests_tx);
            while let Some(handler) = handlers.join_next().await {
                handler.expect("OAuth provider handler");
            }
        });
        Self {
            device_url: format!("http://{address}/device"),
            token_url: format!("http://{address}/token"),
            expected_flows,
            token_requests,
            token_disconnects,
            token_release,
            requests,
            task: Some(task),
        }
    }

    async fn wait_for_token_clients(&mut self) -> BTreeSet<String> {
        let mut clients = BTreeSet::new();
        for _ in 0..self.expected_flows {
            let form = tokio::time::timeout(TEST_PHASE_TIMEOUT, self.token_requests.recv())
                .await
                .expect("OAuth token request timed out")
                .expect("OAuth provider closed before both token requests");
            assert_eq!(
                form.get("grant_type").map(String::as_str),
                Some("urn:ietf:params:oauth:grant-type:device_code")
            );
            assert_eq!(
                form.get("device_code").map(String::as_str),
                Some("device-code")
            );
            clients.insert(
                form.get("client_id")
                    .expect("OAuth token client_id")
                    .clone(),
            );
        }
        clients
    }

    async fn wait_for_token_disconnect(&mut self) {
        tokio::time::timeout(TEST_PHASE_TIMEOUT, self.token_disconnects.recv())
            .await
            .expect("OAuth token connection did not close after stream drop")
            .expect("OAuth provider closed before observing token connection cancellation");
    }

    fn release_token_responses(&self) {
        self.token_release.add_permits(self.expected_flows);
    }

    fn received_requests(&self) -> Vec<FixtureRequest> {
        self.requests
            .lock()
            .expect("read OAuth provider requests")
            .clone()
    }

    async fn finish(mut self) {
        let task = self.task.take().expect("OAuth provider task");
        tokio::time::timeout(TEST_PHASE_TIMEOUT, task)
            .await
            .expect("OAuth provider did not finish")
            .expect("join OAuth provider");
    }
}

impl Drop for GatedDeviceOAuthProvider {
    fn drop(&mut self) {
        self.token_release.add_permits(self.expected_flows);
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

#[derive(Clone)]
struct FixtureRequest {
    path: String,
    form: BTreeMap<String, String>,
}

async fn read_fixture_request(socket: &mut tokio::net::TcpStream) -> FixtureRequest {
    let mut reader = BufReader::new(socket);
    let mut line = String::new();
    let read = reader
        .read_line(&mut line)
        .await
        .expect("read OAuth provider request line");
    assert!(
        read > 0,
        "OAuth provider request closed before request line"
    );
    let path = line
        .split_whitespace()
        .nth(1)
        .expect("OAuth provider request path")
        .to_string();
    let mut content_length = 0;
    loop {
        line.clear();
        let read = reader
            .read_line(&mut line)
            .await
            .expect("read OAuth provider request header");
        assert!(read > 0, "OAuth provider request closed before headers");
        if line == "\r\n" {
            break;
        }
        if let Some((name, value)) = line.split_once(':')
            && name.eq_ignore_ascii_case("content-length")
        {
            content_length = value.trim().parse().expect("OAuth content-length");
        }
    }
    let mut body = vec![0; content_length];
    reader
        .read_exact(&mut body)
        .await
        .expect("read OAuth provider request body");
    FixtureRequest {
        path,
        form: url::form_urlencoded::parse(&body).into_owned().collect(),
    }
}

async fn write_fixture_json(socket: &mut tokio::net::TcpStream, body: &str) -> std::io::Result<()> {
    let response = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    );
    socket.write_all(response.as_bytes()).await?;
    socket.shutdown().await
}

fn assert_error_reason(status: &Status, expected: &str) {
    let reason = status
        .get_error_details_vec()
        .into_iter()
        .find_map(|detail| match detail {
            ErrorDetail::ErrorInfo(info) => Some(info.reason),
            _ => None,
        });
    assert_eq!(reason.as_deref(), Some(expected));
}

fn manifest(name: &str, issuer: &str, kind: &str) -> String {
    let oauth = if kind == "oauth" {
        "oauth:\n  method:\n    flow: {type: authorization_code, pkce: disabled}\n    redirect_uri: http://127.0.0.1:53682/oauth/callback\n    endpoints:\n      authorization_url: https://provider.example.com/authorize\n      token_url: https://provider.example.com/token\n    client:\n      id: {default: client}\n"
    } else {
        ""
    };
    format!(
        "kind: identity\nspec_version: 1\nname: {name}\nversion: 1.0.0\ndescription: {issuer} identity\nissuer: {issuer}\ntype: {kind}\n{oauth}"
    )
}

fn device_manifest(
    name: &str,
    issuer: &str,
    device_url: &str,
    token_url: &str,
    client: &str,
) -> String {
    format!(
        "kind: identity\nspec_version: 1\nname: {name}\nversion: 1.0.0\ndescription: {issuer} identity\nissuer: {issuer}\ntype: oauth\noauth:\n  method:\n    flow: {{type: device_code}}\n    endpoints:\n      device_authorization_url: {device_url}\n      token_url: {token_url}\n    client:\n      id: {{default: {client}}}\n"
    )
}
