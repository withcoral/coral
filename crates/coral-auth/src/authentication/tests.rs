use super::*;
use coral_test_support::TestDatabase;
use coral_workspace::{
    AgentDisplayName, AuthenticatedUser, ChangeContext, OrganizationRole, TraceId, UserId,
    WorkspaceDisplayName, WorkspaceRole,
};
use std::time::Duration;

const AUDIENCE: &str = "https://coral.example/workspaces/01234567-89ab-4cde-8fab-0123456789ab/mcp";
const ORGANIZATION_AUDIENCE: &str = "https://coral.example/mcp";

#[tokio::test]
async fn user_authentication_reloads_authority_without_provisioning_or_local_fallback() {
    let fixture = Fixture::new().await;
    let token = fixture.token("owner", AUDIENCE);
    let principal = fixture.authenticate(Some(&token)).await.unwrap();
    assert_eq!(principal.principal().id().as_str(), "owner");
    assert_eq!(
        principal.authenticated_user().unwrap().organization_role(),
        Some(OrganizationRole::Owner)
    );
    let changed = fixture
        .organization
        .set_role(
            &fixture.other,
            fixture.owner.actor().user_id(),
            1,
            OrganizationRole::Member,
        )
        .await
        .unwrap();
    let principal = fixture.authenticate(Some(&token)).await.unwrap();
    assert_eq!(
        principal.authenticated_user().unwrap().organization_role(),
        Some(OrganizationRole::Member)
    );
    fixture
        .organization
        .set_enabled(
            &fixture.other,
            fixture.owner.actor().user_id(),
            changed.revision(),
            false,
        )
        .await
        .unwrap();
    assert_eq!(
        fixture.authenticate(Some(&token)).await,
        Err(AuthenticationError::Unauthenticated)
    );
    assert_eq!(
        fixture.authenticate(None).await,
        Err(AuthenticationError::Unauthenticated)
    );
    let unknown = fixture.token("unprovisioned", AUDIENCE);
    assert_eq!(
        fixture.authenticate(Some(&unknown)).await,
        Err(AuthenticationError::Unauthenticated)
    );
    fixture
        .organization
        .resolve_user_authority(&UserId::parse("unprovisioned").unwrap())
        .await
        .expect_err("authentication must not provision a missing Membership");
}

#[tokio::test]
async fn authentication_rejects_wrong_audience_signature_kind_and_malformed_credentials() {
    let fixture = Fixture::new().await;
    let other_issuer = SessionTokenIssuer::new(
        Some("https://auth.example"),
        coral_test_support::p256_pkcs8(),
        Duration::from_mins(1),
    )
    .unwrap();
    let wrong_key = other_issuer
        .issue_access_token("owner", "client", AUDIENCE)
        .unwrap();
    let wrong_kind =
        crate::session::test_tokens::agent_kind_token(&fixture.issuer, "owner", "client", AUDIENCE);
    let wrong_audience = fixture.token("owner", "https://other.example");
    for token in [
        "",
        "not-a-token",
        "coral_agent_invalid",
        &wrong_key.access_token,
        &wrong_kind,
        &wrong_audience,
    ] {
        let error = fixture.authenticate(Some(token)).await.unwrap_err();
        assert_eq!(error, AuthenticationError::Unauthenticated);
        assert_eq!(error.to_string(), "authentication required");
    }
    let valid = fixture.token("owner", AUDIENCE);
    for audience in ["", " ", "https://coral.example", "https://other.example"] {
        assert_eq!(
            fixture
                .authentication
                .authenticate(&fixture.workspace, Some(&valid), audience)
                .await,
            Err(AuthenticationError::Unauthenticated)
        );
    }
}

#[tokio::test]
async fn the_credential_alone_fixes_the_workspace_at_the_organization_endpoint() {
    let fixture = Fixture::new().await;
    let workspace = fixture.workspace.to_string();
    let scoped = |audience: &str| {
        fixture
            .issuer
            .issue_workspace_access_token("owner", "client", audience, &fixture.workspace)
            .unwrap()
            .access_token
    };
    let accepted = fixture
        .authentication
        .authenticate_credential(Some(&scoped(ORGANIZATION_AUDIENCE)), ORGANIZATION_AUDIENCE)
        .await
        .unwrap();
    assert_eq!(accepted.workspace(), &fixture.workspace);
    assert_eq!(accepted.principal().principal().id().as_str(), "owner");

    let agent = fixture
        .workspaces
        .create_agent(
            &fixture.owner,
            &fixture.workspace,
            AgentDisplayName::parse("Worker").unwrap(),
            None,
        )
        .await
        .unwrap();
    let accepted = fixture
        .authentication
        .authenticate_credential(Some(agent.credential.expose()), ORGANIZATION_AUDIENCE)
        .await
        .unwrap();
    assert_eq!(accepted.workspace(), &fixture.workspace);
    assert_eq!(accepted.principal().principal(), agent.agent.principal());

    let agent_kind = crate::session::test_tokens::scoped_agent_kind_token(
        &fixture.issuer,
        "owner",
        "client",
        ORGANIZATION_AUDIENCE,
        &workspace,
    );
    for (case, bearer) in [
        ("no credential", None),
        ("a malformed credential", Some("not-a-token".to_owned())),
        (
            "an unscoped token, which names no Workspace",
            Some(fixture.token("owner", ORGANIZATION_AUDIENCE)),
        ),
        (
            "a scoped token for a Workspace MCP URL",
            Some(scoped(AUDIENCE)),
        ),
        ("an Agent-kind token", Some(agent_kind)),
        (
            "an invalid Agent credential",
            Some(format!("coral_agent_{}", "x".repeat(43))),
        ),
    ] {
        assert_eq!(
            fixture
                .authentication
                .authenticate_credential(bearer.as_deref(), ORGANIZATION_AUDIENCE)
                .await
                .map(drop),
            Err(AuthenticationError::Unauthenticated),
            "{case}"
        );
    }
}

#[tokio::test]
async fn agent_authentication_uses_workspace_registration_not_user_authority() {
    let fixture = Fixture::new().await;
    let issued = fixture
        .workspaces
        .create_agent(
            &fixture.owner,
            &fixture.workspace,
            AgentDisplayName::parse("Worker").unwrap(),
            None,
        )
        .await
        .unwrap();
    let token = issued.credential.expose();
    let principal = fixture.authenticate(Some(token)).await.unwrap();
    assert_eq!(principal.principal(), issued.agent.principal());
    assert_eq!(principal.agent_workspace_id(), Some(&fixture.workspace));
    let other_workspace = WorkspaceId::parse("11234567-89ab-4cde-8fab-0123456789ab").unwrap();
    assert_eq!(
        fixture
            .authentication
            .authenticate(&other_workspace, Some(token), AUDIENCE)
            .await,
        Err(AuthenticationError::Unauthenticated)
    );
    fixture
        .organization
        .set_enabled(&fixture.other, fixture.owner.actor().user_id(), 1, false)
        .await
        .unwrap();
    assert_eq!(fixture.authenticate(Some(token)).await.unwrap(), principal);
    let rotated = fixture
        .workspaces
        .rotate_agent_credential(&fixture.other, &fixture.workspace, issued.registration.id())
        .await
        .unwrap();
    assert_eq!(
        fixture.authenticate(Some(token)).await,
        Err(AuthenticationError::Unauthenticated)
    );
    let token = rotated.credential.expose();
    assert_eq!(
        fixture.authenticate(Some(token)).await.unwrap().principal(),
        principal.principal()
    );
    for (enabled, expiry) in [(false, None), (true, Some(1))] {
        fixture
            .workspaces
            .update_agent_registration(
                &fixture.other,
                &fixture.workspace,
                issued.registration.id(),
                enabled,
                expiry,
            )
            .await
            .unwrap();
        assert_eq!(
            fixture.authenticate(Some(token)).await,
            Err(AuthenticationError::Unauthenticated)
        );
    }
}

#[tokio::test]
async fn unavailable_authority_is_a_bounded_error_not_an_authentication_success() {
    let fixture = Fixture::new().await;
    let token = fixture.token("owner", AUDIENCE);
    fixture.database.database().close().await;
    let error = fixture.authenticate(Some(&token)).await.unwrap_err();
    assert_eq!(error, AuthenticationError::Unavailable);
    assert_eq!(error.to_string(), "authentication unavailable");
}

struct Fixture {
    database: TestDatabase,
    organization: OrganizationModule,
    workspaces: WorkspaceModule,
    owner: ChangeContext,
    other: ChangeContext,
    workspace: WorkspaceId,
    issuer: SessionTokenIssuer,
    authentication: PrincipalAuthentication,
}

impl Fixture {
    async fn new() -> Self {
        let database = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
        let organization = OrganizationModule::new(database.database());
        let workspaces = WorkspaceModule::new(database.database());
        let owner = change("owner");
        let other = change("other");
        organization.provision_login(&owner).await.unwrap();
        organization.provision_login(&other).await.unwrap();
        let workspace = workspaces
            .create(&owner, WorkspaceDisplayName::parse("Workspace").unwrap())
            .await
            .unwrap()
            .id()
            .clone();
        // A second designated Owner administers the Workspace once the first is disabled.
        workspaces
            .set_membership(&owner, &workspace, other.actor(), WorkspaceRole::Owner)
            .await
            .unwrap();
        let issuer = SessionTokenIssuer::new(
            Some("https://auth.example"),
            coral_test_support::p256_pkcs8(),
            Duration::from_mins(1),
        )
        .unwrap();
        let authentication =
            PrincipalAuthentication::new(&issuer, organization.clone(), workspaces.clone());
        Self {
            database,
            organization,
            workspaces,
            owner,
            other,
            workspace,
            issuer,
            authentication,
        }
    }

    fn token(&self, user: &str, audience: &str) -> String {
        self.issuer
            .issue_access_token(user, "untrusted-client", audience)
            .unwrap()
            .access_token
    }

    async fn authenticate(
        &self,
        token: Option<&str>,
    ) -> Result<AuthenticatedPrincipal, AuthenticationError> {
        self.authentication
            .authenticate(&self.workspace, token, AUDIENCE)
            .await
    }
}

fn change(id: &str) -> ChangeContext {
    ChangeContext::new(
        AuthenticatedUser::organization_member(UserId::parse(id).unwrap(), OrganizationRole::Owner),
        1,
        TraceId::current_or_new(),
    )
}
