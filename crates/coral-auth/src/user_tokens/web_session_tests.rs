#![expect(
    clippy::indexing_slicing,
    reason = "fixtures index the claims of tokens they just minted"
)]
use std::time::Duration;

use super::tests::{Fixture, NOW, REEF, SECOND, real_now, refresh_token};
use super::{AccessTarget, TokenRefusal, WebSessionToken};

const HOUR: Duration = Duration::from_hours(1);

impl Fixture {
    /// Signs in to Reef with a refresh-eligible client and returns the
    /// refresh token Reef would hand to its web session.
    async fn reef_refresh_token(&self) -> String {
        refresh_token(&self.sign_in(REEF, AccessTarget::Reef, true).await.unwrap())
    }

    /// Starts a web session from a fresh Reef sign-in and returns its handle.
    async fn web_session(&self, max_age: Duration) -> String {
        self.web_session_at(max_age, NOW).await
    }

    /// [`Self::web_session`], signing in at `now`.
    async fn web_session_at(&self, max_age: Duration, now: i64) -> String {
        let issued = self
            .sign_in_at(REEF, AccessTarget::Reef, true, now)
            .await
            .unwrap();
        self.tokens
            .start_web_session(&refresh_token(&issued), &self.client, max_age, now + SECOND)
            .await
            .unwrap()
            .handle
            .expose()
            .to_owned()
    }

    async fn session_token(&self, handle: &str, now: i64) -> Result<WebSessionToken, TokenRefusal> {
        self.tokens
            .web_session_token(handle, &self.client, now)
            .await
    }
}

fn claims(token: &WebSessionToken) -> serde_json::Value {
    jsonwebtoken::dangerous::insecure_decode::<serde_json::Value>(&token.access_token.access_token)
        .unwrap()
        .claims
}

/// A web session mints Reef access tokens for its User on every request, and
/// ending it stops them.
#[tokio::test]
async fn a_web_session_mints_reef_tokens_until_it_ends() {
    let fixture = Fixture::new().await;
    // The last step authenticates a token, which reads the system clock.
    let now = real_now();
    let handle = fixture.web_session_at(HOUR, now).await;
    assert!(handle.starts_with("coral_ws_"));
    for at in [2, 3] {
        let token = fixture
            .session_token(&handle, now + at * SECOND)
            .await
            .unwrap();
        let claims = claims(&token);
        assert_eq!(claims["aud"], "reef");
        assert_eq!(claims["sub"], fixture.user.user_id().as_str());
        assert!(claims.get("workspace_id").is_none());
    }
    let issued = fixture
        .session_token(&handle, now + 4 * SECOND)
        .await
        .unwrap();
    fixture
        .tokens
        .end_web_session(&handle, &fixture.client)
        .await
        .unwrap();
    assert_eq!(
        fixture.session_token(&handle, now + 5 * SECOND).await.err(),
        Some(TokenRefusal::InvalidGrant)
    );
    // Ending a session ends refresh only: a token minted before keeps its
    // expiry and its live authority checks.
    let reef = crate::PrincipalAuthentication::new(
        &fixture.issuer,
        coral_workspace::OrganizationModule::new(fixture.database.database()),
        coral_workspace::WorkspaceModule::new(fixture.database.database()),
    )
    .authenticate_user(Some(&issued.access_token.access_token), "reef")
    .await
    .expect("the earlier token still authenticates");
    assert_eq!(
        reef.principal().id().as_str(),
        fixture.user.user_id().as_str()
    );
}

/// Only a Reef grant, presented by its own client, can back a web session; a
/// refused start changes nothing.
#[tokio::test]
async fn only_a_reef_grant_from_its_own_client_starts_a_session() {
    let fixture = Fixture::new().await;
    let mcp = refresh_token(
        &fixture
            .sign_in(
                &fixture.workspace_resource(),
                AccessTarget::PrivateMcp(fixture.workspace.clone()),
                true,
            )
            .await
            .unwrap(),
    );
    let reef = fixture.reef_refresh_token().await;
    for (case, token, client) in [
        ("an MCP grant", mcp.as_str(), &fixture.client),
        ("another client", reef.as_str(), &fixture.other),
        ("an unknown token", "coral_rt_unknown", &fixture.client),
    ] {
        assert_eq!(
            fixture
                .tokens
                .start_web_session(token, client, HOUR, NOW + SECOND)
                .await
                .err(),
            Some(TokenRefusal::InvalidGrant),
            "{case}"
        );
    }
    for token in [mcp, reef] {
        fixture
            .refresh(&token, NOW + 2 * SECOND)
            .await
            .expect("a refused start spends nothing");
    }
}

/// Starting a session spends the refresh token: presenting it again is a
/// replay that ends the grant, and the session with it.
#[tokio::test]
async fn the_spent_refresh_token_cannot_outlive_its_session() {
    let fixture = Fixture::new().await;
    let token = fixture.reef_refresh_token().await;
    let handle = fixture
        .tokens
        .start_web_session(&token, &fixture.client, HOUR, NOW + SECOND)
        .await
        .unwrap()
        .handle;
    assert_eq!(
        fixture.refresh(&token, NOW + 2 * SECOND).await.err(),
        Some(TokenRefusal::InvalidGrant)
    );
    assert_eq!(
        fixture
            .session_token(handle.expose(), NOW + 3 * SECOND)
            .await
            .err(),
        Some(TokenRefusal::InvalidGrant)
    );
    assert_eq!(format!("{handle:?}"), "WebSessionHandle(<redacted>)");
}

/// A session ends at its own maximum age or its grant's absolute expiry,
/// whichever comes first, and none of its tokens outlives it.
#[tokio::test]
async fn a_session_ends_at_the_earlier_of_its_age_and_its_grant() {
    for (grant_lifetime, max_age, ends_after) in [
        (Duration::from_hours(24), Duration::from_secs(10), 10),
        (Duration::from_secs(5), HOUR, 5),
        // A request for more than a year is shortened to one.
        (
            Duration::from_hours(24 * 730),
            Duration::from_hours(24 * 3650),
            365 * 24 * 3600,
        ),
    ] {
        let fixture = Fixture::with_lifetime(grant_lifetime).await;
        let handle = fixture.web_session(max_age).await;
        // The grant started at NOW and the session one second later.
        let session_end = NOW
            + SECOND
            + i64::try_from(max_age.min(Duration::from_hours(24 * 365)).as_nanos()).unwrap();
        let grant_end = NOW + 1 + i64::try_from(grant_lifetime.as_nanos()).unwrap();
        let end = session_end.min(grant_end);
        // Tokens carry whole seconds, so the session's last usable instant
        // is just before its last second.
        let last_second = (end / SECOND) * SECOND;
        let token = fixture
            .session_token(&handle, last_second - 1)
            .await
            .unwrap();
        assert_eq!(token.session_expires_at_unix_nanos, end, "{ends_after}s");
        let exp = claims(&token)["exp"].as_i64().unwrap();
        assert!(
            exp <= end / SECOND,
            "{ends_after}s: token outlives its session"
        );
        assert_eq!(
            fixture.session_token(&handle, end).await.err(),
            Some(TokenRefusal::InvalidGrant),
            "{ends_after}s"
        );
        // Its last second is a refusal too, never an issuance failure.
        assert_eq!(
            fixture.session_token(&handle, last_second).await.err(),
            Some(TokenRefusal::InvalidGrant),
            "{ends_after}s"
        );
    }
}

/// Ending a session is idempotent and bound to the client that started it.
#[tokio::test]
async fn only_the_sessions_own_client_ends_it() {
    let fixture = Fixture::new().await;
    let handle = fixture.web_session(HOUR).await;
    for (client, refused) in [
        (&fixture.other, None),
        (&fixture.client, Some(TokenRefusal::InvalidGrant)),
        (&fixture.client, Some(TokenRefusal::InvalidGrant)),
    ] {
        fixture
            .tokens
            .end_web_session(&handle, client)
            .await
            .unwrap();
        assert_eq!(
            fixture.session_token(&handle, NOW + 2 * SECOND).await.err(),
            refused,
            "{}",
            client.client_id()
        );
    }
    assert_eq!(
        fixture
            .tokens
            .web_session_token(&handle, &fixture.other, NOW)
            .await
            .err(),
        Some(TokenRefusal::InvalidGrant)
    );
}

/// Revoking the grant from its spent refresh token ends the session, as
/// signing out at the token endpoint would.
#[tokio::test]
async fn revoking_the_grant_ends_its_session() {
    let fixture = Fixture::new().await;
    let token = fixture.reef_refresh_token().await;
    let handle = fixture
        .tokens
        .start_web_session(&token, &fixture.client, HOUR, NOW + SECOND)
        .await
        .unwrap()
        .handle;
    fixture
        .tokens
        .revoke(&token, &fixture.client)
        .await
        .unwrap();
    assert_eq!(
        fixture
            .session_token(handle.expose(), NOW + 2 * SECOND)
            .await
            .err(),
        Some(TokenRefusal::InvalidGrant)
    );
}

/// Any number of processes resolve one session at once; nothing they do
/// competes, because minting writes nothing.
#[tokio::test]
async fn processes_resolve_one_session_concurrently() {
    let fixture = Fixture::new().await;
    let peer = fixture.peer().await;
    let handle = fixture.web_session(HOUR).await;
    let mut resolves = tokio::task::JoinSet::new();
    for index in 0..8 {
        let tokens = if index % 2 == 0 {
            fixture.tokens.clone()
        } else {
            peer.clone()
        };
        let (handle, client) = (handle.clone(), fixture.client.clone());
        resolves.spawn(async move {
            tokens
                .web_session_token(&handle, &client, NOW + 2 * SECOND)
                .await
        });
    }
    while let Some(outcome) = resolves.join_next().await {
        outcome
            .unwrap()
            .expect("every concurrent resolve mints a token");
    }
}

/// Disablement refuses a session's tokens while it lasts but ends nothing;
/// a storage outage is unavailability, never a rejected session.
#[tokio::test]
async fn resolving_rechecks_membership_and_reports_outages() {
    let fixture = Fixture::new().await;
    let handle = fixture.web_session(HOUR).await;
    let owner = coral_test_support::provision_user(
        fixture.database.database(),
        "0199f7a2-6a6e-7c3d-9a51-2f4e8b7d1c09",
        coral_workspace::OrganizationRole::Owner,
    )
    .await;
    let organization = coral_workspace::OrganizationModule::new(fixture.database.database());
    let change = coral_test_support::change(owner, 5, '2');
    let user = fixture.user.user_id();
    organization
        .set_enabled(&change, user, 1, false)
        .await
        .unwrap();
    assert_eq!(
        fixture.session_token(&handle, NOW + 2 * SECOND).await.err(),
        Some(TokenRefusal::InvalidGrant)
    );
    organization
        .set_enabled(&change, user, 2, true)
        .await
        .unwrap();
    fixture
        .session_token(&handle, NOW + 3 * SECOND)
        .await
        .unwrap();
    fixture
        .database
        .execute_fixture_sql("ALTER TABLE oauth_web_sessions RENAME TO web_sessions_away")
        .await
        .unwrap();
    assert_eq!(
        fixture.session_token(&handle, NOW + 4 * SECOND).await.err(),
        Some(TokenRefusal::Unavailable)
    );
}

/// A Web Session's tokens serve one request each, so each lasts minutes, not
/// the deployment's access-token lifetime: few outlive a sign-out.
#[tokio::test]
async fn a_web_session_mints_short_lived_tokens() {
    let fixture = Fixture::with_lifetimes(
        super::DEFAULT_REFRESH_GRANT_LIFETIME,
        Duration::from_hours(720),
    )
    .await;
    let handle = fixture.web_session(HOUR).await;
    let claims = claims(
        &fixture
            .session_token(&handle, NOW + 2 * SECOND)
            .await
            .unwrap(),
    );
    // The request's host time issues the token, not the system clock.
    assert_eq!(claims["iat"], (NOW + 2 * SECOND) / SECOND);
    let lifetime = claims["exp"].as_i64().unwrap() - claims["iat"].as_i64().unwrap();
    assert!(
        lifetime <= i64::try_from(super::WEB_SESSION_TOKEN_LIFETIME.as_secs()).unwrap(),
        "a Web Session token lasts {lifetime}s"
    );
}
