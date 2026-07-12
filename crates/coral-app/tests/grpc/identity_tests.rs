use std::sync::Arc;

use coral_api::v1::{
    AddIdentitySpecRequest, CreateUserOwnedIdentityRequest, CreateWorkspaceOwnedIdentityRequest,
    CreateWorkspaceRequest, DeleteIdentitySpecRequest, DeleteUserOwnedIdentityRequest,
    DeleteWorkspaceOwnedIdentityRequest, FixedTokenUserOwnedIdentitySetup,
    FixedTokenWorkspaceOwnedIdentitySetup, GetUserOwnedIdentityRequest,
    GetWorkspaceOwnedIdentityRequest, Identity, IdentityOwner, ListUserOwnedIdentitiesRequest,
    ListWorkspaceOwnedIdentitiesRequest, Workspace, create_user_owned_identity_request,
    create_user_owned_identity_response, create_workspace_owned_identity_request,
    create_workspace_owned_identity_response,
};
use coral_api::{
    CORAL_ERROR_REASON_IDENTITY_NOT_FOUND, CORAL_ERROR_REASON_IDENTITY_SPEC_NOT_FOUND,
    CORAL_ERROR_REASON_WORKSPACE_NOT_FOUND,
};
use coral_app::{ServerBuilder, UserPrincipal, UserPrincipalProvider, UserPrincipalProviderError};
use coral_client::AppClient;
use tempfile::TempDir;
use tonic::metadata::MetadataMap;
use tonic::{Code, Request, Status};
use tonic_types::{ErrorDetail, StatusExt as _};

const USER_HEADER: &str = "x-test-user";

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

    let alice = create(&app, "alice", "shared", "alice_spec", "alice-token").await;
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

    let (server, restarted) = start(&config_dir).await;
    assert_eq!(get(&restarted, "alice", "shared").await, alice);
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
    let omitted = workspace_create_status(
        &app,
        for_user(
            CreateWorkspaceOwnedIdentityRequest {
                workspace: Some(existing),
                name: "shared".to_string(),
                identity_spec: "missing".to_string(),
                setup: None,
            },
            "alice",
        ),
    )
    .await;
    assert_eq!(omitted.code(), Code::Unimplemented);
    server.shutdown().await.expect("shutdown server");
}

async fn assert_create_validation(app: &AppClient) {
    let omitted = create_status(
        app,
        for_user(
            CreateUserOwnedIdentityRequest {
                name: "shared".to_string(),
                identity_spec: "oauth_spec".to_string(),
                setup: None,
            },
            "alice",
        ),
    )
    .await;
    assert_eq!(omitted.code(), Code::Unimplemented);
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
        create_status(app, create_request("shared", "missing", "token", "alice")).await;
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
