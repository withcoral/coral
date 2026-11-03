#![expect(
    clippy::indexing_slicing,
    reason = "fixtures index claims and token lists they just built"
)]
use std::sync::Arc;
use std::time::Duration;

use coral_test_support::{TestDatabase, provision_user};
use coral_workspace::{
    AuthenticatedPrincipal, AuthenticatedUser, OrganizationModule, OrganizationRole, WorkspaceId,
    WorkspaceModule,
};

use super::{AccessTarget, IssuedTokens, RefreshRequest, TokenRefusal, UserTokens};
use crate::authorization_codes::{AuthorizationCodes, AuthorizationGrant, CodeRedemption};
use crate::{PrincipalAuthentication, PrivateAudience, SessionTokenIssuer};

const NOW: i64 = 1_800_000_000_000_000_000;
const SECOND: i64 = 1_000_000_000;
const CLIENT: &str = "https://client.example.test/oauth/client.json";
const REDIRECT: &str = "http://127.0.0.1:14554/callback";
const REEF: &str = "https://reef.example.test";

struct Fixture {
    database: TestDatabase,
    issuer: SessionTokenIssuer,
    tokens: UserTokens,
    codes: AuthorizationCodes,
    user: AuthenticatedUser,
    workspace: WorkspaceId,
    codes_issued: std::sync::atomic::AtomicUsize,
}

impl Fixture {
    async fn new() -> Self {
        Self::with_lifetime(super::DEFAULT_REFRESH_GRANT_LIFETIME).await
    }

    async fn with_lifetime(lifetime: Duration) -> Self {
        let database = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
        let issuer = SessionTokenIssuer::new(
            Some("https://auth.example.test"),
            coral_test_support::p256_pkcs8(),
            Duration::from_mins(5),
        )
        .unwrap();
        let user = provision_user(
            database.database(),
            "0199f7a2-6a6e-7c3d-9a51-2f4e8b7d1c05",
            OrganizationRole::Member,
        )
        .await;
        let workspace = WorkspaceModule::new(database.database())
            .create(
                &coral_test_support::change(user.clone(), 1, '1'),
                coral_workspace::WorkspaceDisplayName::parse("Research").unwrap(),
            )
            .await
            .unwrap()
            .id()
            .clone();
        Self {
            tokens: UserTokens::new(database.database(), issuer.clone())
                .with_refresh_grant_lifetime(lifetime),
            codes: AuthorizationCodes::new(database.database()),
            issuer,
            database,
            user,
            workspace,
            codes_issued: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// The same deployment, reached through another connection pool, as a
    /// second Coral process would.
    async fn peer(&self) -> UserTokens {
        UserTokens::new(
            Arc::new(self.database.connect_peer().await.unwrap()),
            self.issuer.clone(),
        )
    }

    fn workspace_resource(&self) -> String {
        format!("https://mcp.example.test/workspaces/{}/mcp", self.workspace)
    }

    /// Issues a fresh code for `resource`, then redeems it for `target`.
    async fn sign_in(
        &self,
        resource: &str,
        target: AccessTarget,
        refresh_eligible: bool,
    ) -> Result<IssuedTokens, TokenRefusal> {
        self.sign_in_at(resource, target, refresh_eligible, NOW)
            .await
    }

    async fn sign_in_at(
        &self,
        resource: &str,
        target: AccessTarget,
        refresh_eligible: bool,
        now: i64,
    ) -> Result<IssuedTokens, TokenRefusal> {
        let index = self
            .codes_issued
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let (code, sign_in) = (format!("code-{index}"), format!("sign-in-{index}"));
        let grant = AuthorizationGrant {
            user_id: self.user.user_id().to_string(),
            client_id: CLIENT.into(),
            redirect_uri: REDIRECT.into(),
            code_challenge: "challenge".into(),
            resource: resource.into(),
            workspace_id: matches!(target, AccessTarget::PickedWorkspace)
                .then(|| self.workspace.clone()),
            refresh_eligible,
        };
        self.codes
            .issue(&code, &sign_in, &grant, now)
            .await
            .unwrap();
        self.tokens
            .redeem_code(
                &code,
                CodeRedemption {
                    client_id: CLIENT,
                    redirect_uri: REDIRECT,
                    code_challenge: "challenge",
                    resource,
                },
                target,
                now + 1,
            )
            .await
    }

    /// Signs in to the private MCP surface of the fixture's Workspace with a
    /// refresh-eligible client and returns the first refresh token.
    async fn refreshable(&self) -> String {
        let issued = self
            .sign_in(
                &self.workspace_resource(),
                AccessTarget::PrivateMcp(self.workspace.clone()),
                true,
            )
            .await
            .unwrap();
        issued
            .refresh_token
            .expect("refresh token")
            .expose()
            .to_owned()
    }

    async fn refresh(&self, token: &str, now: i64) -> Result<IssuedTokens, TokenRefusal> {
        self.tokens.refresh(token, plain_refresh(), now).await
    }

    /// Authenticates `access_token` at the fixture Workspace's private MCP surface.
    async fn authenticates(&self, access_token: &str) -> bool {
        PrincipalAuthentication::new(
            &self.issuer,
            OrganizationModule::new(self.database.database()),
            WorkspaceModule::new(self.database.database()),
        )
        .authenticate_scoped(&self.workspace, Some(access_token), "mcp")
        .await
        .is_ok_and(|principal: AuthenticatedPrincipal| {
            principal.principal().id().as_str() == self.user.user_id().as_str()
        })
    }
}

const fn plain_refresh() -> RefreshRequest<'static> {
    RefreshRequest {
        client_id: CLIENT,
        resource: None,
        audience: None,
    }
}

fn refresh_token(issued: &IssuedTokens) -> String {
    issued
        .refresh_token
        .as_ref()
        .expect("refresh token")
        .expose()
        .to_owned()
}

#[tokio::test]
async fn a_refresh_rotates_the_token_and_keeps_the_user_and_workspace() {
    let fixture = Fixture::new().await;
    let first = fixture.refreshable().await;
    let refreshed = fixture.refresh(&first, NOW + SECOND).await.unwrap();
    assert!(
        fixture
            .authenticates(&refreshed.access_token.access_token)
            .await
    );
    let second = refresh_token(&refreshed);
    assert_ne!(second, first);
    assert!(second.starts_with("coral_rt_"));
    let again = fixture.refresh(&second, NOW + 2 * SECOND).await.unwrap();
    assert!(
        fixture
            .authenticates(&again.access_token.access_token)
            .await
    );
}

#[tokio::test]
async fn a_client_that_did_not_declare_refresh_gets_an_access_token_alone() {
    let fixture = Fixture::new().await;
    let issued = fixture
        .sign_in(
            &fixture.workspace_resource(),
            AccessTarget::PrivateMcp(fixture.workspace.clone()),
            false,
        )
        .await
        .unwrap();
    assert!(issued.refresh_token.is_none());
    assert!(
        fixture
            .authenticates(&issued.access_token.access_token)
            .await
    );
}

/// Presenting a spent token again ends the grant, so whoever holds its newest
/// token can no longer refresh either.
#[tokio::test]
async fn replaying_a_consumed_token_ends_the_whole_grant() {
    let fixture = Fixture::new().await;
    let first = fixture.refreshable().await;
    let second = refresh_token(&fixture.refresh(&first, NOW + SECOND).await.unwrap());
    let third = refresh_token(&fixture.refresh(&second, NOW + 2 * SECOND).await.unwrap());
    assert_eq!(
        fixture.refresh(&first, NOW + 3 * SECOND).await.err(),
        Some(TokenRefusal::InvalidGrant)
    );
    assert_eq!(
        fixture.refresh(&third, NOW + 4 * SECOND).await.err(),
        Some(TokenRefusal::InvalidGrant),
        "the replay ended the grant's newest token too"
    );
}

#[tokio::test]
async fn rotation_never_extends_the_grants_absolute_expiry() {
    let fixture = Fixture::with_lifetime(Duration::from_secs(10)).await;
    let mut token = fixture.refreshable().await;
    for at in [3, 6, 9] {
        token = refresh_token(&fixture.refresh(&token, NOW + at * SECOND).await.unwrap());
    }
    assert_eq!(
        fixture.refresh(&token, NOW + 1 + 10 * SECOND).await.err(),
        Some(TokenRefusal::InvalidGrant)
    );
}

/// A refresh that names another client, resource, or audience is refused and
/// changes nothing; naming the grant's own keeps working.
#[tokio::test]
async fn a_refresh_must_repeat_the_grants_bindings_and_a_mismatch_spends_nothing() {
    let fixture = Fixture::new().await;
    let resource = fixture.workspace_resource();
    let mut token = fixture.refreshable().await;
    let mut at = NOW;
    for (case, request, accepted) in [
        (
            "another client",
            RefreshRequest {
                client_id: "https://other.example.test/client.json",
                ..plain_refresh()
            },
            false,
        ),
        (
            "another resource",
            RefreshRequest {
                resource: Some("https://mcp.example.test/mcp"),
                ..plain_refresh()
            },
            false,
        ),
        (
            "another audience",
            RefreshRequest {
                audience: Some(PrivateAudience::Reef),
                ..plain_refresh()
            },
            false,
        ),
        (
            "its own resource and audience",
            RefreshRequest {
                resource: Some(&resource),
                audience: Some(PrivateAudience::Mcp),
                ..plain_refresh()
            },
            true,
        ),
    ] {
        at += SECOND;
        let outcome = fixture.tokens.refresh(&token, request, at).await;
        if accepted {
            token = refresh_token(&outcome.expect(case));
        } else {
            assert_eq!(outcome.err(), Some(TokenRefusal::InvalidGrant), "{case}");
        }
    }
    let refreshed = fixture.refresh(&token, at + SECOND).await;
    assert!(
        refreshed.is_ok(),
        "mismatches neither spent nor ended the grant"
    );
}

/// A Workspace picked at the Organization MCP Endpoint and Reef's unscoped
/// access each survive refresh unchanged.
#[tokio::test]
async fn each_target_keeps_its_audience_and_workspace_across_refresh() {
    let fixture = Fixture::new().await;
    for (resource, target, audience, workspace) in [
        (
            "https://mcp.example.test/mcp",
            AccessTarget::PickedWorkspace,
            "https://mcp.example.test/mcp",
            Some(fixture.workspace.to_string()),
        ),
        (REEF, AccessTarget::Reef, "reef", None),
        (
            fixture.workspace_resource().as_str(),
            AccessTarget::WorkspaceResource(fixture.workspace.clone()),
            fixture.workspace_resource().as_str(),
            None,
        ),
        (REEF, AccessTarget::Resource, REEF, None),
    ] {
        let first = fixture.sign_in(resource, target, true).await.unwrap();
        let refreshed = fixture
            .refresh(&refresh_token(&first), NOW + SECOND)
            .await
            .unwrap();
        let claims = jsonwebtoken::dangerous::insecure_decode::<serde_json::Value>(
            &refreshed.access_token.access_token,
        )
        .unwrap()
        .claims;
        assert_eq!(claims["aud"], audience, "{resource}");
        assert_eq!(claims["sub"], fixture.user.user_id().as_str());
        assert_eq!(claims["client_id"], CLIENT);
        assert_eq!(
            claims.get("workspace_id").and_then(|id| id.as_str()),
            workspace.as_deref(),
            "{resource}"
        );
    }
}

/// Revocation ends refresh from any token the grant issued, current or
/// consumed, but only for the client the grant belongs to.
#[tokio::test]
async fn revocation_ends_the_grant_for_its_own_client_only() {
    let fixture = Fixture::new().await;
    assert_eq!(
        fixture.tokens.revoke("coral_rt_unknown", CLIENT).await,
        Ok(())
    );
    let first = fixture.refreshable().await;
    let issued = fixture.refresh(&first, NOW + SECOND).await.unwrap();
    let current = refresh_token(&issued);
    assert_eq!(
        fixture
            .tokens
            .revoke(&first, "https://other.example.test/client.json")
            .await,
        Ok(())
    );
    let current = refresh_token(&fixture.refresh(&current, NOW + 2 * SECOND).await.unwrap());
    // The consumed first token revokes, as a logout racing a rotation would.
    assert_eq!(fixture.tokens.revoke(&first, CLIENT).await, Ok(()));
    assert_eq!(
        fixture.refresh(&current, NOW + 3 * SECOND).await.err(),
        Some(TokenRefusal::InvalidGrant)
    );
    assert!(
        fixture
            .authenticates(&issued.access_token.access_token)
            .await,
        "revocation ends refresh only; issued access tokens keep their expiry"
    );
}

/// Processes sharing the database race to rotate one token: at most one
/// rotation wins, and the losing presentation is a replay that ends the grant.
#[tokio::test]
async fn concurrent_rotations_of_one_token_never_both_succeed() {
    let fixture = Fixture::new().await;
    let peer = fixture.peer().await;
    let token = fixture.refreshable().await;
    let mut refreshes = tokio::task::JoinSet::new();
    for index in 0..6 {
        let tokens = if index % 2 == 0 {
            fixture.tokens.clone()
        } else {
            peer.clone()
        };
        let token = token.clone();
        refreshes.spawn(async move { tokens.refresh(&token, plain_refresh(), NOW + SECOND).await });
    }
    let mut successors = Vec::new();
    while let Some(outcome) = refreshes.join_next().await {
        match outcome.unwrap() {
            Ok(issued) => successors.push(refresh_token(&issued)),
            Err(refusal) => assert_eq!(refusal, TokenRefusal::InvalidGrant),
        }
    }
    assert!(successors.len() <= 1, "{} rotations won", successors.len());
    for successor in successors {
        assert_eq!(
            peer.refresh(&successor, plain_refresh(), NOW + 2 * SECOND)
                .await
                .err(),
            Some(TokenRefusal::InvalidGrant)
        );
    }
}

/// Whichever of a revocation and a refresh commits first, no token can
/// refresh once the revocation has committed.
#[tokio::test]
async fn a_revocation_racing_a_refresh_leaves_nothing_refreshable() {
    let fixture = Fixture::new().await;
    let peer = fixture.peer().await;
    for _ in 0..4 {
        let token = fixture.refreshable().await;
        let (refreshed, revoked) = tokio::join!(
            fixture.refresh(&token, NOW + SECOND),
            peer.revoke(&token, CLIENT),
        );
        assert_eq!(revoked, Ok(()));
        if let Ok(issued) = refreshed {
            assert_eq!(
                fixture
                    .refresh(&refresh_token(&issued), NOW + 2 * SECOND)
                    .await
                    .err(),
                Some(TokenRefusal::InvalidGrant)
            );
        }
    }
}

/// Disablement refuses refresh while it lasts and starts no grant, though the
/// sign-in still gets an access token. It ends no grant, so re-enabling
/// restores refresh.
#[tokio::test]
async fn refresh_rechecks_current_organization_membership() {
    let fixture = Fixture::new().await;
    let token = fixture.refreshable().await;
    let owner = provision_user(
        fixture.database.database(),
        "0199f7a2-6a6e-7c3d-9a51-2f4e8b7d1c09",
        OrganizationRole::Owner,
    )
    .await;
    let organization = OrganizationModule::new(fixture.database.database());
    let set_enabled = |enabled: bool, revision: u64| {
        let organization = organization.clone();
        let change = coral_test_support::change(owner.clone(), 5, '2');
        let user = fixture.user.user_id().clone();
        async move {
            organization
                .set_enabled(&change, &user, revision, enabled)
                .await
                .unwrap();
        }
    };
    let issued = fixture.refresh(&token, NOW + SECOND).await.unwrap();
    let token = refresh_token(&issued);
    set_enabled(false, 1).await;
    assert!(
        !fixture
            .authenticates(&issued.access_token.access_token)
            .await,
        "live checks still block an issued access token"
    );
    assert_eq!(
        fixture.refresh(&token, NOW + 2 * SECOND).await.err(),
        Some(TokenRefusal::InvalidGrant)
    );
    let access_only = fixture
        .sign_in(REEF, AccessTarget::Reef, true)
        .await
        .unwrap();
    assert!(
        access_only.refresh_token.is_none(),
        "a disabled User starts no grant"
    );
    set_enabled(true, 2).await;
    fixture.refresh(&token, NOW + 3 * SECOND).await.unwrap();
}

/// A storage outage is unavailability, never a rejected credential, and
/// leaves the presented token able to refresh once storage recovers.
#[tokio::test]
async fn a_storage_outage_is_unavailable_and_strands_no_token() {
    let fixture = Fixture::new().await;
    let token = fixture.refreshable().await;
    fixture
        .database
        .execute_fixture_sql("ALTER TABLE oauth_refresh_tokens RENAME TO refresh_tokens_away")
        .await
        .unwrap();
    assert_eq!(
        fixture.refresh(&token, NOW + SECOND).await.err(),
        Some(TokenRefusal::Unavailable)
    );
    assert_eq!(
        fixture.tokens.revoke(&token, CLIENT).await,
        Err(TokenRefusal::Unavailable)
    );
    fixture
        .database
        .execute_fixture_sql("ALTER TABLE refresh_tokens_away RENAME TO oauth_refresh_tokens")
        .await
        .unwrap();
    fixture.refresh(&token, NOW + 2 * SECOND).await.unwrap();
}

/// Each User holds a bounded number of grants; starting one past the bound
/// ends the oldest.
#[tokio::test]
async fn a_user_holds_a_bounded_number_of_grants() {
    let fixture = Fixture::new().await;
    let mut tokens = Vec::new();
    for index in 0..=super::MAX_GRANTS_PER_USER {
        let at = NOW + i64::try_from(index).unwrap();
        let issued = fixture
            .sign_in_at(REEF, AccessTarget::Reef, true, at)
            .await
            .unwrap();
        tokens.push(refresh_token(&issued));
    }
    let later = NOW + SECOND;
    assert_eq!(
        fixture.refresh(&tokens[0], later).await.err(),
        Some(TokenRefusal::InvalidGrant)
    );
    fixture.refresh(&tokens[1], later).await.unwrap();
    fixture
        .refresh(tokens.last().unwrap(), later)
        .await
        .unwrap();
}

#[test]
fn a_refresh_token_never_prints() {
    let (token, _digest) = super::RefreshToken::generate().unwrap();
    assert_eq!(format!("{token:?}"), "RefreshToken(<redacted>)");
}
