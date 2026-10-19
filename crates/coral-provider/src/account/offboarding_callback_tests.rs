//! OAuth callbacks that are exchanging when their User is offboarded.
use std::sync::Arc;

use coral_credentials::{CredentialVault, Secret};
use coral_workspace::{OrganizationModule, OrganizationRole, WorkspaceDisplayName, WorkspaceRole};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::{AccountModule, AccountScope, AuthorizationState, OAuthClient, OAuthExchangeGate};
use crate::access::CredentialRoute;
use crate::account_tests::authorization_request;
use crate::tests::{change_context, organization_user, publish_provider};

/// Offboarding also retires Saved Functions, so the schema includes them.
async fn test_modules() -> (
    coral_test_support::TestDatabase,
    coral_workspace::WorkspaceModule,
    crate::ProviderModule,
) {
    let db = coral_test_support::TestDatabase::new(coral_db::migration_plan())
        .await
        .unwrap();
    let workspaces = coral_workspace::WorkspaceModule::new(db.database());
    let providers = crate::ProviderModule::with_running_catalog(
        coral_outbound::DestinationPolicy::default()
            .with_loopback(coral_outbound::NetworkAccess::HttpAndHttps),
        db.database(),
        workspaces.clone(),
    );
    (db, workspaces, providers)
}

fn state_of(authorization: &super::Authorization) -> String {
    url::Url::parse(authorization.authorization_url.as_deref().unwrap())
        .unwrap()
        .query_pairs()
        .find(|(key, _)| key == "state")
        .unwrap()
        .1
        .into_owned()
}

/// Offboarding ends an initial or reconnect OAuth exchange already in flight.
/// After the User rejoins, the late callback is canceled and creates no Account.
#[tokio::test(flavor = "multi_thread")]
#[expect(
    clippy::too_many_lines,
    reason = "real initial and replacement token exchanges straddle offboarding and rejoin"
)]
async fn offboarding_cancels_in_flight_initial_and_reconnect_callbacks_across_rejoin() {
    for reconnect in [false, true] {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "access-canary", "refresh_token": "refresh-canary",
                "token_type": "Bearer", "scope": "read", "expires_in": 3600
            })))
            .mount(&server)
            .await;
        let (db, workspaces, providers) = test_modules().await;
        let administrator = change_context(
            organization_user(db.database(), "user:administrator", OrganizationRole::Owner).await,
            30,
            'b',
        );
        let user = organization_user(
            db.database(),
            "user:late-callback",
            OrganizationRole::Member,
        )
        .await;
        let context = change_context(user.clone(), 10, 'a');
        let workspace = workspaces
            .create(
                &context,
                WorkspaceDisplayName::parse("Late callback").unwrap(),
            )
            .await
            .unwrap();
        // A second designated Owner, so offboarding needs no transfer target.
        crate::tests::join_as_owner(&workspaces, administrator.actor(), workspace.id()).await;
        let scope = workspace.id().clone();
        let document = serde_json::json!({
            "openapi":"3.1.0", "info":{"title":"OAuth","version":"1"},
            "servers":[{"url":server.uri()}], "security":[{"oauth":["read"]}],
            "components":{"securitySchemes":{"oauth":{"type":"oauth2","flows":{"authorizationCode":{
                "authorizationUrl":format!("{}/authorize",server.uri()),
                "tokenUrl":format!("{}/token",server.uri()), "scopes":{"read":"Read"}
            }}}}},
            "paths":{"/data":{"get":{"operationId":"getData","responses":{"200":{"description":"Data"}}}}}
        });
        let provider = publish_provider(
            &providers,
            &context,
            &scope,
            &document.to_string(),
            "late_callback",
            CredentialRoute::Caller,
        )
        .await;
        let directory = tempfile::tempdir().unwrap();
        let accounts = AccountModule::new(
            providers.clone(),
            CredentialVault::open(&directory.path().join("key")).unwrap(),
        );
        let oauth = || {
            Some(
                OAuthClient::new(
                    "registered-public-client".into(),
                    "https://reef.example/callback".into(),
                )
                .unwrap(),
            )
        };
        let pending = if reconnect {
            let initial = accounts
                .start_user_authorization(
                    &context,
                    &scope,
                    provider.id(),
                    authorization_request(),
                    oauth(),
                )
                .await
                .unwrap();
            let connected = accounts
                .complete_oauth(
                    &context,
                    &scope,
                    initial.id,
                    &state_of(&initial),
                    Secret::new("initial-code".into()).unwrap(),
                )
                .await
                .unwrap();
            accounts
                .start_reconnect(
                    &context,
                    &scope,
                    provider.id(),
                    connected.account_id.unwrap(),
                    authorization_request(),
                    oauth(),
                )
                .await
                .unwrap()
        } else {
            accounts
                .start_user_authorization(
                    &context,
                    &scope,
                    provider.id(),
                    authorization_request(),
                    oauth(),
                )
                .await
                .unwrap()
        };
        let exchanges_before = server.received_requests().await.unwrap().len();
        let gate = Arc::new(OAuthExchangeGate::default());
        let callback = {
            let accounts = accounts.clone().with_exchange_gate(gate.clone());
            let context = context.clone();
            let scope = scope.clone();
            let state = state_of(&pending);
            tokio::spawn(async move {
                accounts
                    .complete_oauth(
                        &context,
                        &scope,
                        pending.id,
                        &state,
                        Secret::new("late-code".into()).unwrap(),
                    )
                    .await
            })
        };
        tokio::time::timeout(std::time::Duration::from_secs(5), gate.entered.notified())
            .await
            .unwrap();
        coral_administration::AccessLifecycleModule::new(db.database(), workspaces.clone())
            .remove_organization_member(&administrator, user.user_id(), 1, None)
            .await
            .unwrap();
        OrganizationModule::new(db.database())
            .provision_login(&context)
            .await
            .unwrap();
        workspaces
            .set_membership(&administrator, &scope, &user, WorkspaceRole::Member)
            .await
            .unwrap();
        gate.release.notify_one();

        assert_eq!(
            callback.await.unwrap().unwrap().state,
            AuthorizationState::Canceled,
            "reconnect: {reconnect}"
        );
        assert!(
            accounts
                .get_account(&context, &scope, provider.id(), &AccountScope::User)
                .await
                .unwrap()
                .account
                .is_none(),
            "reconnect: {reconnect}"
        );
        assert_eq!(
            server.received_requests().await.unwrap().len() - exchanges_before,
            1,
            "the late code is exchanged once and never retried"
        );
    }
}
