use super::*;
use coral_test_support::{ManualClock, TestDatabase};
use coral_workspace::{
    AgentDisplayName, AuthenticatedUser, ChangeContext, IfMatch, IssuedAgentCredential,
    OrganizationRole, Principal, RequestId, TraceId, UserId, WorkspaceDisplayName, WorkspaceRole,
};
use std::time::Duration;

const AUDIENCE: &str = "https://coral.example/workspaces/01234567-89ab-4cde-8fab-0123456789ab/mcp";
const ORGANIZATION_AUDIENCE: &str = "https://coral.example/mcp";
/// Where the fixture's clock starts, in 2100: no real clock is near it.
const START: UnixNanos = UnixNanos::new(4_102_444_800 * 1_000_000_000);

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
            &IfMatch::any(),
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
            &IfMatch::version(changed.version()),
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
        .issue_access_token("owner", "client", AUDIENCE, fixture.clock.now())
        .unwrap();
    let wrong_kind = crate::session::test_tokens::agent_kind_token(
        &fixture.issuer,
        "owner",
        "client",
        AUDIENCE,
        fixture.clock.now(),
    );
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
                .authenticate(
                    WorkspaceFixedBy::Url(&fixture.workspace),
                    Some(&valid),
                    audience
                )
                .await,
            Err(AuthenticationError::Unauthenticated)
        );
    }
}

#[tokio::test]
async fn the_credential_alone_fixes_the_workspace_at_the_organization_endpoint() {
    let fixture = Fixture::new().await;
    let workspace = fixture.workspace.to_string();
    let scoped = |audience: &str, workspace: &WorkspaceId| {
        fixture
            .issuer
            .issue_workspace_access_token(
                "owner",
                "client",
                audience,
                workspace,
                fixture.clock.now(),
            )
            .unwrap()
            .access_token
    };
    // Two Workspaces, so a credential is seen to fix its own one.
    let second = fixture
        .workspaces
        .create(
            &fixture.owner,
            WorkspaceDisplayName::parse("Second").unwrap(),
        )
        .await
        .unwrap()
        .id()
        .clone();
    for chosen in [&fixture.workspace, &second] {
        let accepted = fixture
            .authentication
            .authenticate(
                WorkspaceFixedBy::Credential,
                Some(&scoped(ORGANIZATION_AUDIENCE, chosen)),
                ORGANIZATION_AUDIENCE,
            )
            .await
            .unwrap();
        assert_eq!(&accepted.1, chosen);
        assert_eq!(accepted.0.principal().id().as_str(), "owner");

        let agent = fixture.agent(chosen).await;
        let accepted = fixture
            .authentication
            .authenticate(
                WorkspaceFixedBy::Credential,
                Some(agent.credential.expose()),
                ORGANIZATION_AUDIENCE,
            )
            .await
            .unwrap();
        assert_eq!(&accepted.1, chosen);
        assert_eq!(accepted.0.principal(), agent.agent.principal());
    }

    let agent_kind = crate::session::test_tokens::scoped_agent_kind_token(
        &fixture.issuer,
        "owner",
        "client",
        ORGANIZATION_AUDIENCE,
        &workspace,
        fixture.clock.now(),
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
            Some(scoped(AUDIENCE, &fixture.workspace)),
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
                .authenticate(
                    WorkspaceFixedBy::Credential,
                    bearer.as_deref(),
                    ORGANIZATION_AUDIENCE
                )
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
    let issued = fixture.agent(&fixture.workspace).await;
    let token = issued.credential.expose();
    let principal = fixture.authenticate(Some(token)).await.unwrap();
    assert_eq!(principal.principal(), issued.agent.principal());
    assert_eq!(principal.agent_workspace_id(), Some(&fixture.workspace));
    let other_workspace = WorkspaceId::parse("11234567-89ab-4cde-8fab-0123456789ab").unwrap();
    assert_eq!(
        fixture
            .authentication
            .authenticate(
                WorkspaceFixedBy::Url(&other_workspace),
                Some(token),
                AUDIENCE
            )
            .await,
        Err(AuthenticationError::Unauthenticated)
    );
    fixture
        .organization
        .set_enabled(
            &fixture.other,
            fixture.owner.actor().user_id(),
            &IfMatch::any(),
            false,
        )
        .await
        .unwrap();
    assert_eq!(fixture.authenticate(Some(token)).await.unwrap(), principal);
    let rotated = fixture
        .workspaces
        .rotate_agent_credential(
            &fixture.other,
            &RequestId::generate(),
            &fixture.workspace,
            issued.registration.id(),
            &IfMatch::any(),
        )
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
                &IfMatch::any(),
                enabled,
                expiry.map(UnixNanos::new),
            )
            .await
            .unwrap();
        assert_eq!(
            fixture.authenticate(Some(token)).await,
            Err(AuthenticationError::Unauthenticated)
        );
    }
}

/// Authentication dates a credential by the clock it was built with, not by
/// the host's: when that clock passes a User token's expiry, or an Agent
/// Registration's, the credential stops authenticating.
#[tokio::test]
async fn a_credential_expires_when_the_clock_passes_its_expiry() {
    const SECOND: i64 = 1_000_000_000;
    let fixture = Fixture::new().await;
    let token = fixture.token("owner", AUDIENCE);
    let management = fixture.token("owner", "reef");
    let agent = fixture.agent(&fixture.workspace).await;
    fixture
        .workspaces
        .update_agent_registration(
            &fixture.other,
            &fixture.workspace,
            agent.registration.id(),
            &IfMatch::any(),
            true,
            Some(UnixNanos::new(fixture.clock.now().get() + 30 * SECOND)),
        )
        .await
        .unwrap();
    let agent = agent.credential.expose();
    let at_this_instant = async || {
        [
            fixture.authenticate(Some(&token)).await.map(drop),
            fixture
                .authentication
                .authenticate_user(Some(&management), "reef")
                .await
                .map(|(_user, now)| assert_eq!(now, fixture.clock.now())),
            fixture.authenticate(Some(agent)).await.map(drop),
            // Without sign-in, the same Registration and the same clock.
            fixture
                .local
                .authenticate(
                    WorkspaceFixedBy::Url(&fixture.workspace),
                    Some(agent),
                    AUDIENCE,
                )
                .await
                .map(drop),
        ]
    };
    assert_eq!(at_this_instant().await, [Ok(()); 4]);

    // The fixture's tokens last one minute, and the verifier lets the clock
    // be one more minute off.
    fixture
        .clock
        .advance(Duration::from_mins(2) + Duration::from_secs(1));

    assert_eq!(
        at_this_instant().await,
        [Err(AuthenticationError::Unauthenticated); 4]
    );
    // A token issued at the new instant authenticates at it.
    fixture
        .authenticate(Some(&fixture.token("owner", AUDIENCE)))
        .await
        .unwrap();
}

#[tokio::test]
async fn unavailable_authority_is_a_bounded_error_not_an_authentication_success() {
    let fixture = Fixture::new().await;
    let token = fixture.token("owner", AUDIENCE);
    let agent = fixture.agent(&fixture.workspace).await;
    fixture.database.database().close().await;
    for (case, authentication, bearer) in [
        (
            "a User token with sign-in",
            &fixture.authentication,
            token.as_str(),
        ),
        (
            "an Agent credential in local mode",
            &fixture.local,
            agent.credential.expose(),
        ),
    ] {
        let error = authentication
            .authenticate(
                WorkspaceFixedBy::Url(&fixture.workspace),
                Some(bearer),
                AUDIENCE,
            )
            .await
            .unwrap_err();
        assert_eq!(error, AuthenticationError::Unavailable, "{case}");
        assert_eq!(error.to_string(), "authentication unavailable", "{case}");
    }
}

/// Without sign-in, a missing credential is the local Owner wherever a URL fixes
/// the Workspace, and every presented credential must be an Agent's. With
/// sign-in, a missing credential stays `Unauthenticated` everywhere (the "no
/// credential" row of `the_credential_alone_fixes_the_workspace_at_the_organization_endpoint`).
#[tokio::test]
async fn local_mode_admits_the_local_owner_without_a_credential_and_agents_with_one() {
    use AuthenticationError::{Unauthenticated, WorkspaceUrlRequired};
    let fixture = Fixture::new().await;
    assert!(fixture.authentication.signs_in());
    assert!(!fixture.local.signs_in());
    let workspace = &fixture.workspace;
    let second = fixture
        .workspaces
        .create(
            &fixture.owner,
            WorkspaceDisplayName::parse("Second").unwrap(),
        )
        .await
        .unwrap()
        .id()
        .clone();
    let here = fixture.agent(workspace).await;
    let there = fixture.agent(&second).await;
    let user_token = fixture.token("owner", AUDIENCE);
    let owner = AuthenticatedPrincipal::user(coral_workspace::local_organization_owner())
        .principal()
        .clone();
    let own_agent = here.agent.principal().clone();
    let foreign_agent = there.agent.principal().clone();
    let accepted = |principal: &Principal, workspace: Option<&WorkspaceId>| {
        Ok((principal.clone(), workspace.cloned()))
    };
    // Each row: [Url(workspace), Credential, authenticate_scoped, authenticate_user].
    for (case, bearer, expected) in [
        (
            "no credential",
            None,
            [
                accepted(&owner, Some(workspace)),
                Err(WorkspaceUrlRequired),
                accepted(&owner, None),
                accepted(&owner, None),
            ],
        ),
        (
            "this Workspace's Agent credential",
            Some(here.credential.expose()),
            [
                accepted(&own_agent, Some(workspace)),
                accepted(&own_agent, Some(workspace)),
                accepted(&own_agent, None),
                Err(Unauthenticated),
            ],
        ),
        (
            "another Workspace's Agent credential",
            Some(there.credential.expose()),
            [
                Err(Unauthenticated),
                accepted(&foreign_agent, Some(&second)),
                Err(Unauthenticated),
                Err(Unauthenticated),
            ],
        ),
        (
            "a garbage string",
            Some("not-a-credential"),
            [const { Err(Unauthenticated) }; 4],
        ),
        (
            "a signed User token, which never falls back to the local Owner",
            Some(user_token.as_str()),
            [const { Err(Unauthenticated) }; 4],
        ),
    ] {
        let local = &fixture.local;
        // An admitted call returns the clock's reading beside its Principal.
        let seen = |actor: AuthenticatedPrincipal, workspace: Option<WorkspaceId>, now| {
            assert_eq!(now, START, "{case}: the instant the call read");
            (actor.principal().clone(), workspace)
        };
        let observed: [Result<(Principal, Option<WorkspaceId>), AuthenticationError>; 4] = [
            local
                .authenticate(WorkspaceFixedBy::Url(workspace), bearer, AUDIENCE)
                .await
                .map(|(actor, workspace, now)| seen(actor, Some(workspace), now)),
            local
                .authenticate(WorkspaceFixedBy::Credential, bearer, ORGANIZATION_AUDIENCE)
                .await
                .map(|(actor, workspace, now)| seen(actor, Some(workspace), now)),
            local
                .authenticate_scoped(workspace, bearer, AUDIENCE)
                .await
                .map(|(actor, now)| seen(actor, None, now)),
            local
                .authenticate_user(bearer, AUDIENCE)
                .await
                .map(|(actor, now)| seen(actor, None, now)),
        ];
        assert_eq!(observed, expected, "{case}");
    }
}

struct Fixture {
    database: TestDatabase,
    organization: OrganizationModule,
    workspaces: WorkspaceModule,
    owner: ChangeContext,
    other: ChangeContext,
    workspace: WorkspaceId,
    issuer: SessionTokenIssuer,
    /// The clock `authentication` and `local` read. It reads [`START`] until
    /// a test moves it.
    clock: ManualClock,
    authentication: PrincipalAuthentication,
    /// The same authority without sign-in.
    local: PrincipalAuthentication,
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
        let clock = ManualClock::starting_at(START);
        let authentication = PrincipalAuthentication::new(
            &issuer,
            organization.clone(),
            workspaces.clone(),
            Arc::new(clock.clone()),
        );
        let local = PrincipalAuthentication::local(workspaces.clone(), Arc::new(clock.clone()));
        Self {
            database,
            organization,
            workspaces,
            owner,
            other,
            workspace,
            issuer,
            clock,
            authentication,
            local,
        }
    }

    async fn agent(&self, workspace: &WorkspaceId) -> IssuedAgentCredential {
        self.workspaces
            .create_agent(
                &self.owner,
                &RequestId::generate(),
                workspace,
                AgentDisplayName::parse("Worker").unwrap(),
                None,
            )
            .await
            .unwrap()
    }

    fn token(&self, user: &str, audience: &str) -> String {
        self.issuer
            .issue_access_token(user, "untrusted-client", audience, self.clock.now())
            .unwrap()
            .access_token
    }

    async fn authenticate(
        &self,
        token: Option<&str>,
    ) -> Result<AuthenticatedPrincipal, AuthenticationError> {
        self.authentication
            .authenticate(WorkspaceFixedBy::Url(&self.workspace), token, AUDIENCE)
            .await
            .map(|(principal, workspace, now)| {
                assert_eq!(workspace, self.workspace);
                assert_eq!(now, self.clock.now(), "the instant the call read");
                principal
            })
    }
}

fn change(id: &str) -> ChangeContext {
    ChangeContext::new(
        AuthenticatedUser::organization_member(UserId::parse(id).unwrap(), OrganizationRole::Owner),
        UnixNanos::new(1),
        TraceId::current_or_new(),
    )
}
