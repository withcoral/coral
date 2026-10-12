//! Connection attribution belongs to successful Account transitions, not attempts.
use super::*;
use crate::account_tests::{CREDENTIAL_REQUIRED_OPENAPI, authorization_request, publish_provider};
use crate::tests::{change_context, organization_user};
use coral_credentials::Secret;
use coral_workspace::{OrganizationRole, WorkspaceDisplayName};

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "metadata is compared across the complete successful and unsuccessful reconnect journey"
)]
async fn account_metadata_changes_only_on_successful_connection() {
    let h = crate::tests::Harness::new().await;
    let (workspaces, providers, accounts) = (&h.workspaces, &h.providers, &h.accounts);
    let user =
        organization_user(h.database.clone(), "user:metadata", OrganizationRole::Owner).await;
    let principal = crate::setup_tests::user_principal(&user);
    let context = change_context(user.clone(), 10, 'a');
    let workspace = workspaces
        .create(&context, WorkspaceDisplayName::parse("Metadata").unwrap())
        .await
        .unwrap();
    let scope = workspace.id();
    let provider = publish_provider(
        providers,
        &context,
        scope,
        CREDENTIAL_REQUIRED_OPENAPI,
        "metadata",
        CredentialRoute::Caller,
    )
    .await;
    let initial = accounts
        .start_user_authorization(
            &context,
            scope,
            provider.id(),
            authorization_request(),
            None,
        )
        .await
        .unwrap();
    let unconnected = accounts
        .get_account(&context, scope, provider.id(), &AccountScope::User)
        .await
        .unwrap()
        .account
        .unwrap();
    assert_eq!(unconnected.state, AccountState::Disconnected);
    assert_eq!(unconnected.connected_at_unix_nanos, None);
    assert_eq!(unconnected.configured_by_user_id, None);
    accounts.cancel(&context, scope, initial.id).await.unwrap();
    assert_eq!(
        accounts
            .get_account(&context, scope, provider.id(), &AccountScope::User)
            .await
            .unwrap()
            .account
            .unwrap(),
        unconnected
    );
    let initial = accounts
        .start_user_authorization(
            &context,
            scope,
            provider.id(),
            authorization_request(),
            None,
        )
        .await
        .unwrap();
    let connected_context = change_context(user.clone(), 20, 'b');
    accounts
        .submit_secret(
            &connected_context,
            scope,
            initial.id,
            coral_credentials::CredentialValues::token(
                Secret::new("first-secret-canary".into()).unwrap(),
            ),
        )
        .await
        .unwrap();
    let connected = accounts
        .get_account(&context, scope, provider.id(), &AccountScope::User)
        .await
        .unwrap()
        .account
        .unwrap();
    assert_eq!(connected.id, unconnected.id);
    assert_eq!(connected.state, AccountState::Ready);
    assert_eq!(connected.connected_at_unix_nanos, Some(20));
    assert_eq!(
        connected.configured_by_user_id.as_ref(),
        Some(user.user_id())
    );
    let reconnect = accounts
        .start_reconnect(
            &connected_context,
            scope,
            provider.id(),
            connected.id,
            authorization_request(),
            None,
        )
        .await
        .unwrap();
    accounts
        .cancel(&connected_context, scope, reconnect.id)
        .await
        .unwrap();
    assert_eq!(
        accounts
            .get_account(&context, scope, provider.id(), &AccountScope::User)
            .await
            .unwrap()
            .account
            .unwrap(),
        connected
    );
    assert_eq!(
        crate::setup_tests::credential_for(
            &h,
            scope,
            provider.id(),
            "metadata",
            "getPrivateForecast",
            &principal,
            context.occurred_at_unix_nanos()
        )
        .await
        .unwrap()
        .get(coral_credentials::CredentialValues::TOKEN)
        .unwrap()
        .expose(),
        "first-secret-canary"
    );
    let pending_reconnect = accounts
        .start_reconnect(
            &connected_context,
            scope,
            provider.id(),
            connected.id,
            authorization_request(),
            None,
        )
        .await
        .unwrap();
    let expired_context = change_context(
        user.clone(),
        pending_reconnect.expires_at_unix_nanos + 1,
        'c',
    );
    let expired = accounts
        .submit_secret(
            &expired_context,
            scope,
            pending_reconnect.id,
            coral_credentials::CredentialValues::token(
                Secret::new("expired-canary".into()).unwrap(),
            ),
        )
        .await
        .unwrap();
    assert_eq!(expired.state, AuthorizationState::Expired);
    assert_eq!(
        accounts
            .get_account(&context, scope, provider.id(), &AccountScope::User)
            .await
            .unwrap()
            .account
            .unwrap(),
        connected
    );
    assert_eq!(
        crate::setup_tests::credential_for(
            &h,
            scope,
            provider.id(),
            "metadata",
            "getPrivateForecast",
            &principal,
            context.occurred_at_unix_nanos()
        )
        .await
        .unwrap()
        .get(coral_credentials::CredentialValues::TOKEN)
        .unwrap()
        .expose(),
        "first-secret-canary"
    );
    let winner = accounts
        .start_reconnect(
            &connected_context,
            scope,
            provider.id(),
            connected.id,
            authorization_request(),
            None,
        )
        .await
        .unwrap();
    let loser = accounts
        .start_reconnect(
            &connected_context,
            scope,
            provider.id(),
            connected.id,
            authorization_request(),
            None,
        )
        .await
        .unwrap();
    let replacement_context = change_context(user.clone(), 30, 'd');
    accounts
        .submit_secret(
            &replacement_context,
            scope,
            winner.id,
            coral_credentials::CredentialValues::token(
                Secret::new("replacement-canary".into()).unwrap(),
            ),
        )
        .await
        .unwrap();
    let replaced = accounts
        .get_account(&context, scope, provider.id(), &AccountScope::User)
        .await
        .unwrap()
        .account
        .unwrap();
    assert_eq!(replaced.id, connected.id);
    assert_eq!(replaced.connected_at_unix_nanos, Some(30));
    assert_eq!(
        replaced.configured_by_user_id.as_ref(),
        Some(user.user_id())
    );
    let failed = accounts
        .submit_secret(
            &replacement_context,
            scope,
            loser.id,
            coral_credentials::CredentialValues::token(
                Secret::new("failed-canary".into()).unwrap(),
            ),
        )
        .await
        .unwrap();
    assert_eq!(failed.state, AuthorizationState::Failed);
    assert_eq!(
        accounts
            .get_account(&context, scope, provider.id(), &AccountScope::User)
            .await
            .unwrap()
            .account
            .unwrap(),
        replaced
    );
    assert_eq!(
        crate::setup_tests::credential_for(
            &h,
            scope,
            provider.id(),
            "metadata",
            "getPrivateForecast",
            &principal,
            context.occurred_at_unix_nanos()
        )
        .await
        .unwrap()
        .get(coral_credentials::CredentialValues::TOKEN)
        .unwrap()
        .expose(),
        "replacement-canary"
    );
    accounts
        .disconnect(&replacement_context, scope, provider.id(), connected.id)
        .await
        .unwrap();
    let disconnected = accounts
        .get_account(&context, scope, provider.id(), &AccountScope::User)
        .await
        .unwrap()
        .account
        .unwrap();
    assert_eq!(disconnected.id, connected.id);
    assert_eq!(disconnected.state, AccountState::Disconnected);
    assert_eq!(
        disconnected.connected_at_unix_nanos,
        replaced.connected_at_unix_nanos
    );
    assert_eq!(
        disconnected.configured_by_user_id,
        replaced.configured_by_user_id
    );
    assert!(!format!("{disconnected:?}").contains("canary"));
    assert!(matches!(
        crate::setup_tests::credential_for(
            &h,
            scope,
            provider.id(),
            "metadata",
            "getPrivateForecast",
            &principal,
            context.occurred_at_unix_nanos()
        )
        .await,
        Err(Error::AccountRequired)
    ));
}
