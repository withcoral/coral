use std::sync::Arc;

use coral_test_support::TestDatabase;

use super::{
    AUTHORIZATION_CODE_TTL, AuthorizationCodeError, AuthorizationCodes, AuthorizationGrant,
    CodeIssue, CodeRedemption,
};
use crate::oauth_clients::AuthenticatedClient;
use crate::oauth_clients::test_support::authenticated;

const NOW: i64 = 1_800_000_000_000_000_000;
const CLIENT: &str = "https://client.example.test/oauth/client.json";
const OTHER_CLIENT: &str = "https://other.example.test/oauth/client.json";

fn grant() -> AuthorizationGrant {
    AuthorizationGrant {
        user_id: "0199f7a2-6a6e-7c3d-9a51-2f4e8b7d1c05".into(),
        client_id: CLIENT.into(),
        redirect_uri: "http://127.0.0.1:14554/callback".into(),
        code_challenge: "client-challenge".into(),
        resource: "https://mcp.example.test/workspaces/w/mcp".into(),
        workspace_id: None,
        refresh_eligible: false,
    }
}

/// The redemption that repeats every binding of `grant`, by `client`.
fn redemption<'a>(
    grant: &'a AuthorizationGrant,
    client: &'a AuthenticatedClient,
) -> CodeRedemption<'a> {
    CodeRedemption {
        client,
        redirect_uri: &grant.redirect_uri,
        code_challenge: &grant.code_challenge,
        resource: &grant.resource,
    }
}

fn ttl_nanos() -> i64 {
    i64::try_from(AUTHORIZATION_CODE_TTL.as_nanos()).unwrap()
}

async fn codes() -> (AuthorizationCodes, TestDatabase) {
    let fixture = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
    (AuthorizationCodes::new(fixture.database()), fixture)
}

/// Two processes race to redeem one code; exactly one gets the grant.
#[tokio::test]
async fn a_code_issued_by_one_process_is_redeemed_once_by_any() {
    let (codes, fixture) = codes().await;
    let client = authenticated(fixture.database(), CLIENT).await;
    let peer = AuthorizationCodes::new(Arc::new(fixture.connect_peer().await.unwrap()));
    assert_eq!(
        codes.issue("code", "sign-in", &grant(), NOW).await,
        Ok(CodeIssue::Issued)
    );
    let mut redemptions = tokio::task::JoinSet::new();
    for index in 0..8 {
        let module = if index % 2 == 0 {
            codes.clone()
        } else {
            peer.clone()
        };
        let client = client.clone();
        redemptions.spawn(async move {
            let grant = grant();
            module
                .redeem("code", redemption(&grant, &client), NOW + 1)
                .await
        });
    }
    let mut granted = Vec::new();
    while let Some(outcome) = redemptions.join_next().await {
        granted.extend(outcome.unwrap().unwrap());
    }
    assert_eq!(granted, vec![grant()]);
}

/// A sign-in at the Organization MCP Endpoint picks a Workspace; its code
/// hands that choice to the token, on every process.
#[tokio::test]
async fn a_code_carries_the_workspace_its_sign_in_chose() {
    let (codes, fixture) = codes().await;
    let client = authenticated(fixture.database(), CLIENT).await;
    let peer = AuthorizationCodes::new(Arc::new(fixture.connect_peer().await.unwrap()));
    let chosen = AuthorizationGrant {
        resource: "https://mcp.example.test/mcp".into(),
        workspace_id: Some(
            coral_workspace::WorkspaceId::parse("0199f7a2-6a6e-7c3d-9a51-2f4e8b7d1c06").unwrap(),
        ),
        ..grant()
    };
    codes.issue("code", "sign-in", &chosen, NOW).await.unwrap();
    assert_eq!(
        peer.redeem("code", redemption(&chosen, &client), NOW + 1)
            .await,
        Ok(Some(chosen))
    );
}

/// Anyone can present a code. One that cannot be spent is answered from a
/// read, so guessed codes never wait for, or hold, the database's writer.
#[tokio::test]
async fn an_unspendable_code_is_refused_without_the_writer() {
    let (codes, fixture) = codes().await;
    let client = authenticated(fixture.database(), CLIENT).await;
    let other = authenticated(fixture.database(), OTHER_CLIENT).await;
    codes.issue("code", "sign-in", &grant(), NOW).await.unwrap();
    let database = fixture.database();
    let writer = database.begin().await.unwrap();
    let issued = grant();
    for (case, code, redemption) in [
        ("an unknown code", "guess", redemption(&issued, &client)),
        (
            "the wrong client",
            "code",
            CodeRedemption {
                client: &other,
                ..redemption(&issued, &client)
            },
        ),
    ] {
        let answer = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            codes.redeem(code, redemption, NOW + 1),
        )
        .await;
        assert_eq!(answer, Ok(Ok(None)), "{case}");
    }
    drop(writer);
    assert_eq!(
        codes
            .redeem("code", redemption(&issued, &client), NOW + 1)
            .await,
        Ok(Some(grant()))
    );
}

#[tokio::test]
async fn a_redemption_must_repeat_every_binding_and_a_mismatch_spends_nothing() {
    let (codes, fixture) = codes().await;
    let client = authenticated(fixture.database(), CLIENT).await;
    let other = authenticated(fixture.database(), OTHER_CLIENT).await;
    codes.issue("code", "sign-in", &grant(), NOW).await.unwrap();
    let issued = grant();
    for (binding, mismatched) in [
        (
            "client_id",
            CodeRedemption {
                client: &other,
                ..redemption(&issued, &client)
            },
        ),
        (
            "redirect_uri",
            CodeRedemption {
                redirect_uri: "http://127.0.0.1/other",
                ..redemption(&issued, &client)
            },
        ),
        (
            "code_challenge",
            CodeRedemption {
                code_challenge: "other",
                ..redemption(&issued, &client)
            },
        ),
        (
            "resource",
            CodeRedemption {
                resource: "https://other.example.test/mcp",
                ..redemption(&issued, &client)
            },
        ),
    ] {
        assert_eq!(
            codes.redeem("code", mismatched, NOW + 1).await,
            Ok(None),
            "{binding}"
        );
    }
    assert_eq!(
        codes
            .redeem("code", redemption(&issued, &client), NOW + 1)
            .await,
        Ok(Some(grant()))
    );
}

/// A callback replayed after its sign-in finished must not issue a second
/// code, before or after the first one is redeemed.
#[tokio::test]
async fn a_sign_in_issues_at_most_one_code() {
    let (codes, fixture) = codes().await;
    let client = authenticated(fixture.database(), CLIENT).await;
    let issued = grant();
    assert_eq!(
        codes.issue("first", "sign-in", &issued, NOW).await,
        Ok(CodeIssue::Issued)
    );
    let refuse_replay = async |replay: &str, at: i64| {
        assert_eq!(
            codes.issue(replay, "sign-in", &grant(), at).await,
            Ok(CodeIssue::SignInAlreadyUsed),
            "{replay}"
        );
        assert_eq!(
            codes
                .redeem(replay, redemption(&grant(), &client), at + 1)
                .await,
            Ok(None),
            "{replay}: the refused code must not be redeemable"
        );
    };
    refuse_replay("before-redemption", NOW + 1).await;
    assert_eq!(
        codes
            .redeem("first", redemption(&issued, &client), NOW + 3)
            .await,
        Ok(Some(grant()))
    );
    refuse_replay("after-redemption", NOW + 4).await;
}

#[tokio::test]
async fn codes_expire_after_their_lifetime() {
    let (codes, fixture) = codes().await;
    let client = authenticated(fixture.database(), CLIENT).await;
    let issued = grant();
    codes.issue("last-moment", "a", &issued, NOW).await.unwrap();
    codes.issue("too-late", "b", &issued, NOW).await.unwrap();
    assert_eq!(
        codes
            .redeem(
                "last-moment",
                redemption(&issued, &client),
                NOW + ttl_nanos() - 1
            )
            .await,
        Ok(Some(grant()))
    );
    assert_eq!(
        codes
            .redeem("too-late", redemption(&issued, &client), NOW + ttl_nanos())
            .await,
        Ok(None)
    );
}

#[tokio::test]
async fn empty_secrets_are_refused_and_store_nothing() {
    let (codes, fixture) = codes().await;
    let client = authenticated(fixture.database(), CLIENT).await;
    let issued = grant();
    for (code, sign_in) in [("", "sign-in"), ("code", "")] {
        assert_eq!(
            codes.issue(code, sign_in, &issued, NOW).await,
            Err(AuthorizationCodeError::EmptySecret)
        );
    }
    assert_eq!(
        codes
            .redeem("code", redemption(&issued, &client), NOW)
            .await,
        Ok(None)
    );
    assert_eq!(
        codes.redeem("", redemption(&issued, &client), NOW).await,
        Ok(None)
    );
}

#[tokio::test]
async fn an_unwritable_database_issues_no_code() {
    let fixture = TestDatabase::new(&coral_db::MigrationPlan::shared(&[]))
        .await
        .unwrap();
    let codes = AuthorizationCodes::new(fixture.database());
    let client = authenticated(fixture.database(), CLIENT).await;
    assert_eq!(
        codes.issue("code", "sign-in", &grant(), NOW).await,
        Err(AuthorizationCodeError::Unavailable)
    );
    assert_eq!(
        codes
            .redeem("code", redemption(&grant(), &client), NOW)
            .await,
        Err(AuthorizationCodeError::Unavailable)
    );
}
