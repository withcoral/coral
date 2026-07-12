use std::sync::Arc;

use coral_api::v1::{
    AddIdentitySpecRequest, CreateUserOwnedIdentityRequest, DeleteIdentitySpecRequest,
    DeleteUserOwnedIdentityRequest, FixedTokenUserOwnedIdentitySetup, GetUserOwnedIdentityRequest,
    Identity, IdentityOwner, ListUserOwnedIdentitiesRequest, create_user_owned_identity_request,
    create_user_owned_identity_response,
};
use coral_api::{
    CORAL_ERROR_REASON_IDENTITY_NOT_FOUND, CORAL_ERROR_REASON_IDENTITY_SPEC_NOT_FOUND,
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
    app.identity_spec_client()
        .add_identity_spec(for_user(
            AddIdentitySpecRequest {
                manifest_yaml: manifest(name, issuer, kind),
                input_values: Vec::new(),
                workspace: None,
            },
            "alice",
        ))
        .await
        .expect("add identity spec");
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
