use coral_api::v1::{
    AddIdentitySpecRequest, CreateWorkspaceOwnedFixedTokenIdentityRequest, CreateWorkspaceRequest,
    DeleteWorkspaceOwnedIdentityRequest, FixedTokenIdentitySetup, GetWorkspaceOwnedIdentityRequest,
    GlobalIdentitySpecScope, Identity, IdentitySpecScope, IdentitySpecType,
    ListWorkspaceOwnedIdentitiesRequest, Workspace, identity_owner, identity_spec_scope,
};
use tonic::{Code, Request, Status};

use crate::harness::GrpcHarness;

const SPEC_NAME: &str = "example_token";
const IDENTITY_NAME: &str = "example";
const TEAM_TOKEN: &str = "team-write-only-token";
const OTHER_TOKEN: &str = "other-write-only-token";

#[tokio::test]
async fn manages_workspace_fixed_token_identities_with_exact_spec_fallback() {
    let harness = GrpcHarness::new().await;
    let team = workspace("team");
    let other = workspace("other");
    create_workspace(&harness, &team).await;
    create_workspace(&harness, &other).await;

    let global_scope = global_scope();
    let team_scope = workspace_scope(&team);
    install_spec(
        &harness,
        global_scope.clone(),
        "global",
        "global.example.com",
        Some(443),
    )
    .await;
    install_spec(
        &harness,
        team_scope.clone(),
        "team",
        "team.example.com",
        None,
    )
    .await;

    let missing_setup = harness
        .workspace_identity_client()
        .create_workspace_owned_fixed_token_identity(Request::new(
            CreateWorkspaceOwnedFixedTokenIdentityRequest {
                workspace: Some(team.clone()),
                name: IDENTITY_NAME.to_string(),
                identity_spec_name: SPEC_NAME.to_string(),
                setup: None,
            },
        ))
        .await
        .expect_err("fixed-token setup is required");
    assert_eq!(missing_setup.code(), Code::InvalidArgument);

    let missing_workspace = harness
        .workspace_identity_client()
        .list_workspace_owned_identities(Request::new(ListWorkspaceOwnedIdentitiesRequest {
            workspace: None,
        }))
        .await
        .expect_err("workspace is required");
    assert_eq!(missing_workspace.code(), Code::InvalidArgument);

    let unknown_workspace = workspace("unknown");
    let unknown = create(&harness, &unknown_workspace, TEAM_TOKEN)
        .await
        .expect_err("unknown workspace must fail");
    assert_eq!(unknown.code(), Code::NotFound);

    let team_identity = create(&harness, &team, TEAM_TOKEN)
        .await
        .expect("create team identity");
    assert_identity(
        &team_identity,
        &team,
        &team_scope,
        "team",
        "team.example.com",
        None,
    );

    let other_identity = create(&harness, &other, OTHER_TOKEN)
        .await
        .expect("create fallback identity");
    assert_identity(
        &other_identity,
        &other,
        &global_scope,
        "global",
        "global.example.com",
        Some(443),
    );

    assert_eq!(list(&harness, &team).await, vec![team_identity.clone()]);
    assert_eq!(list(&harness, &other).await, vec![other_identity]);
    assert_eq!(
        get(&harness, &team).await.expect("get team identity"),
        team_identity
    );

    harness
        .workspace_identity_client()
        .delete_workspace_owned_identity(Request::new(DeleteWorkspaceOwnedIdentityRequest {
            workspace: Some(team.clone()),
            name: IDENTITY_NAME.to_string(),
        }))
        .await
        .expect("delete team identity");
    assert_eq!(
        get(&harness, &team)
            .await
            .expect_err("deleted identity must be absent")
            .code(),
        Code::NotFound
    );
    assert!(list(&harness, &team).await.is_empty());
    assert_eq!(list(&harness, &other).await.len(), 1);
}

async fn create_workspace(harness: &GrpcHarness, workspace: &Workspace) {
    harness
        .workspace_client()
        .create_workspace(Request::new(CreateWorkspaceRequest {
            workspace: Some(workspace.clone()),
        }))
        .await
        .expect("create workspace");
}

async fn install_spec(
    harness: &GrpcHarness,
    scope: IdentitySpecScope,
    issuer: &str,
    host: &str,
    port: Option<u16>,
) {
    let port = port.map_or_else(String::new, |port| format!(", port: {port}"));
    harness
        .identity_spec_client()
        .add_identity_spec(Request::new(AddIdentitySpecRequest {
            manifest_yaml: format!(
                "kind: identity\nspec_version: 1\nname: {SPEC_NAME}\nversion: 1.0.0\ndescription: test fixed token\nissuer: {issuer}\ntype: fixed_token\naudience: {{host: {host}{port}}}\n"
            ),
            input_values: Vec::new(),
            scope: Some(scope),
        }))
        .await
        .expect("install fixed-token identity spec");
}

async fn create(
    harness: &GrpcHarness,
    workspace: &Workspace,
    token: &str,
) -> Result<Identity, Status> {
    Ok(harness
        .workspace_identity_client()
        .create_workspace_owned_fixed_token_identity(Request::new(
            CreateWorkspaceOwnedFixedTokenIdentityRequest {
                workspace: Some(workspace.clone()),
                name: IDENTITY_NAME.to_string(),
                identity_spec_name: SPEC_NAME.to_string(),
                setup: Some(FixedTokenIdentitySetup {
                    token: token.to_string(),
                }),
            },
        ))
        .await?
        .into_inner()
        .identity
        .expect("created identity"))
}

async fn list(harness: &GrpcHarness, workspace: &Workspace) -> Vec<Identity> {
    harness
        .workspace_identity_client()
        .list_workspace_owned_identities(Request::new(ListWorkspaceOwnedIdentitiesRequest {
            workspace: Some(workspace.clone()),
        }))
        .await
        .expect("list workspace identities")
        .into_inner()
        .identities
}

async fn get(harness: &GrpcHarness, workspace: &Workspace) -> Result<Identity, Status> {
    Ok(harness
        .workspace_identity_client()
        .get_workspace_owned_identity(Request::new(GetWorkspaceOwnedIdentityRequest {
            workspace: Some(workspace.clone()),
            name: IDENTITY_NAME.to_string(),
        }))
        .await?
        .into_inner()
        .identity
        .expect("identity response"))
}

fn assert_identity(
    identity: &Identity,
    workspace: &Workspace,
    scope: &IdentitySpecScope,
    issuer: &str,
    host: &str,
    port: Option<u32>,
) {
    assert_eq!(identity.name, IDENTITY_NAME);
    assert!(matches!(
        identity
            .owner
            .as_ref()
            .and_then(|owner| owner.value.as_ref()),
        Some(identity_owner::Value::Workspace(owner)) if owner == workspace
    ));
    let spec = identity
        .identity_spec
        .as_ref()
        .expect("identity spec reference");
    assert_eq!(spec.name, SPEC_NAME);
    assert_eq!(spec.scope.as_ref(), Some(scope));
    assert!(!spec.fingerprint.is_empty());
    assert_eq!(spec.issuer, issuer);
    assert_eq!(spec.identity_type, IdentitySpecType::FixedToken as i32);
    let audience = spec.audience.as_ref().expect("pinned audience");
    assert_eq!(audience.host, host);
    assert_eq!(audience.port, port);
    assert!(identity.created_at_unix_nanos > 0);
    assert!(identity.updated_at_unix_nanos >= identity.created_at_unix_nanos);
    let debug = format!("{identity:?}");
    assert!(!debug.contains(TEAM_TOKEN));
    assert!(!debug.contains(OTHER_TOKEN));
}

fn workspace(name: &str) -> Workspace {
    Workspace {
        name: name.to_string(),
    }
}

fn global_scope() -> IdentitySpecScope {
    IdentitySpecScope {
        value: Some(identity_spec_scope::Value::Global(
            GlobalIdentitySpecScope {},
        )),
    }
}

fn workspace_scope(workspace: &Workspace) -> IdentitySpecScope {
    IdentitySpecScope {
        value: Some(identity_spec_scope::Value::Workspace(workspace.clone())),
    }
}
