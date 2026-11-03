//! Access removal ends the Refresh Grants the removed access supported.
use std::sync::Arc;
use std::time::Duration;

use crate::AccessLifecycleModule;
use coral_auth::SessionTokenIssuer;
use coral_auth::authorization_codes::{AuthorizationCodes, AuthorizationGrant, CodeRedemption};
use coral_auth::user_tokens::{AccessTarget, RefreshRequest, TokenRefusal, UserTokens};
use coral_test_support::{TestDatabase, provision_user};
use coral_workspace::{
    AuthenticatedUser, ChangeContext, OrganizationModule, OrganizationRole, WorkspaceDisplayName,
    WorkspaceId, WorkspaceModule, WorkspaceRole,
};

const NOW: i64 = 1_800_000_000_000_000_000;
const CLIENT: &str = "https://client.example.test/oauth/client.json";

struct Fixture {
    database: TestDatabase,
    tokens: UserTokens,
    issuer: SessionTokenIssuer,
    organization: OrganizationModule,
    workspaces: WorkspaceModule,
    administration: AccessLifecycleModule,
    owner: AuthenticatedUser,
    member: AuthenticatedUser,
    workspace: WorkspaceId,
    sign_ins: std::sync::atomic::AtomicUsize,
}

fn change(user: &AuthenticatedUser) -> ChangeContext {
    coral_test_support::change(user.clone(), 100, '1')
}

impl Fixture {
    async fn new() -> Self {
        let database = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
        let db = database.database();
        let issuer = SessionTokenIssuer::new(
            Some("https://auth.example.test"),
            coral_test_support::p256_pkcs8(),
            Duration::from_mins(5),
        )
        .unwrap();
        let workspaces = WorkspaceModule::new(db.clone());
        let owner = provision_user(db.clone(), "user:owner", OrganizationRole::Owner).await;
        let member = provision_user(db.clone(), "user:member", OrganizationRole::Member).await;
        let workspace = workspaces
            .create(
                &change(&owner),
                WorkspaceDisplayName::parse("Work").unwrap(),
            )
            .await
            .unwrap()
            .id()
            .clone();
        let fixture = Self {
            tokens: UserTokens::new(db.clone(), issuer.clone()),
            issuer,
            organization: OrganizationModule::new(db.clone()),
            administration: AccessLifecycleModule::new(db, workspaces.clone()),
            workspaces,
            owner,
            member,
            workspace,
            database,
            sign_ins: std::sync::atomic::AtomicUsize::new(0),
        };
        fixture.join_workspace().await;
        fixture
    }

    async fn join_workspace(&self) {
        let member = self
            .organization
            .resolve_user_authority(self.member.user_id())
            .await
            .unwrap();
        self.workspaces
            .set_membership(
                &change(&self.owner),
                &self.workspace,
                &member,
                WorkspaceRole::Member,
            )
            .await
            .unwrap();
    }

    async fn peer(&self) -> UserTokens {
        UserTokens::new(
            Arc::new(self.database.connect_peer().await.unwrap()),
            self.issuer.clone(),
        )
    }

    /// The member signs in for `target` and gets a refresh token.
    async fn sign_in(&self, target: AccessTarget) -> String {
        let index = self
            .sign_ins
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let resource = match target {
            AccessTarget::PrivateMcp(_) | AccessTarget::WorkspaceResource(_) => {
                format!("https://mcp.example.test/workspaces/{}/mcp", self.workspace)
            }
            _ => "https://reef.example.test".to_owned(),
        };
        let grant = AuthorizationGrant {
            user_id: self.member.user_id().to_string(),
            client_id: CLIENT.into(),
            redirect_uri: "http://127.0.0.1/callback".into(),
            code_challenge: "challenge".into(),
            resource: resource.clone(),
            workspace_id: None,
            refresh_eligible: true,
        };
        let code = format!("code-{index}");
        AuthorizationCodes::new(self.database.database())
            .issue(&code, &format!("sign-in-{index}"), &grant, NOW)
            .await
            .unwrap();
        let issued = self
            .tokens
            .redeem_code(
                &code,
                CodeRedemption {
                    client_id: CLIENT,
                    redirect_uri: &grant.redirect_uri,
                    code_challenge: "challenge",
                    resource: &resource,
                },
                target,
                NOW + 1,
            )
            .await
            .unwrap();
        issued.refresh_token.unwrap().expose().to_owned()
    }

    /// Refreshes `token` and returns its successor, or the refusal.
    async fn refresh(&self, tokens: &UserTokens, token: &str) -> Result<String, TokenRefusal> {
        let request = RefreshRequest {
            client_id: CLIENT,
            resource: None,
            audience: None,
        };
        tokens
            .refresh(token, request, NOW + 2)
            .await
            .map(|issued| issued.refresh_token.unwrap().expose().to_owned())
    }

    async fn remove_from_workspace(&self) {
        self.administration
            .remove_workspace_member(&change(&self.owner), &self.workspace, self.member.user_id())
            .await
            .unwrap();
    }
}

/// Losing a Workspace Membership ends that Workspace's grants; losing the
/// Organization Membership ends every grant. Rejoining revives none.
#[tokio::test]
async fn removal_ends_the_grants_it_takes_away_and_rejoining_revives_none() {
    let fixture = Fixture::new().await;
    let workspace_grant = fixture
        .sign_in(AccessTarget::PrivateMcp(fixture.workspace.clone()))
        .await;
    let public_workspace_grant = fixture
        .sign_in(AccessTarget::WorkspaceResource(fixture.workspace.clone()))
        .await;
    let reef_grant = fixture.sign_in(AccessTarget::Reef).await;
    let session = fixture
        .tokens
        .start_web_session(
            &fixture.sign_in(AccessTarget::Reef).await,
            CLIENT,
            Duration::from_hours(1),
            NOW + 2,
        )
        .await
        .unwrap()
        .handle;

    fixture.remove_from_workspace().await;
    fixture.join_workspace().await;
    for grant in [workspace_grant, public_workspace_grant] {
        assert_eq!(
            fixture.refresh(&fixture.tokens, &grant).await,
            Err(TokenRefusal::InvalidGrant),
            "rejoining the Workspace revives no grant"
        );
    }
    let reef_grant = fixture
        .refresh(&fixture.tokens, &reef_grant)
        .await
        .expect("an unscoped grant survives Workspace removal");

    let membership = fixture
        .organization
        .provision_login(&change(&fixture.member))
        .await
        .unwrap();
    fixture
        .administration
        .remove_organization_member(
            &change(&fixture.owner),
            fixture.member.user_id(),
            membership.revision(),
            None,
        )
        .await
        .unwrap();
    fixture
        .organization
        .provision_login(&change(&fixture.member))
        .await
        .unwrap();
    assert_eq!(
        fixture.refresh(&fixture.tokens, &reef_grant).await,
        Err(TokenRefusal::InvalidGrant),
        "rejoining the Organization revives no grant"
    );
    assert_eq!(
        fixture
            .tokens
            .web_session_token(session.expose(), CLIENT, NOW + 3)
            .await
            .err(),
        Some(TokenRefusal::InvalidGrant),
        "removal ends the web session with its grant"
    );
}

/// Whichever of a Workspace removal and a refresh commits first, no token
/// for that Workspace refreshes once the removal has committed.
#[tokio::test]
async fn a_removal_racing_a_refresh_leaves_nothing_refreshable() {
    let fixture = Fixture::new().await;
    let peer = fixture.peer().await;
    for round in 0..3 {
        if round > 0 {
            fixture.join_workspace().await;
        }
        let token = fixture
            .sign_in(AccessTarget::PrivateMcp(fixture.workspace.clone()))
            .await;
        let (refreshed, ()) = tokio::join!(
            fixture.refresh(&peer, &token),
            fixture.remove_from_workspace(),
        );
        if let Ok(successor) = refreshed {
            assert_eq!(
                fixture.refresh(&fixture.tokens, &successor).await,
                Err(TokenRefusal::InvalidGrant),
                "round {round}"
            );
        }
    }
}
