use super::*;
use coral_credentials::Secret;
coral_test_support::provider_setup_fixtures!(crate);
use crate::account_tests::{authorization_request, publish_provider};
use crate::tests::{change_context, organization_user, test_modules};
use coral_workspace::{OrganizationRole, WorkspaceDisplayName};

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one Account journey verifies replay, private ownership, disconnection and reconnect"
)]
async fn account_inspection_disconnect_and_reconnect_preserve_private_ownership() {
    let (db, workspaces, providers) = test_modules().await;
    let context = change_context(
        organization_user(db.database(), "user:account", OrganizationRole::Owner).await,
        10,
        'a',
    );
    let other = change_context(
        organization_user(db.database(), "user:other-account", OrganizationRole::Owner).await,
        10,
        'b',
    );
    let workspace = workspaces
        .create(&context, WorkspaceDisplayName::parse("Accounts").unwrap())
        .await
        .unwrap();
    crate::tests::join_as_owner(&workspaces, other.actor(), workspace.id()).await;
    let scope = workspace.id();
    let provider = publish_provider(
        &providers,
        &context,
        scope,
        crate::account_tests::CREDENTIAL_REQUIRED_OPENAPI,
        "accounts",
        CredentialRoute::Caller,
    )
    .await;
    let directory = tempfile::tempdir().unwrap();
    let accounts = crate::tests::accounts_for(&providers, directory.path());
    let empty = accounts
        .get_account(&context, scope, provider.id(), &AccountScope::User)
        .await
        .unwrap();
    assert!(empty.can_connect);
    assert!(empty.account.is_none());
    let request_id = authorization_request();
    let started = accounts
        .start_user_authorization(&context, scope, provider.id(), request_id.clone(), None)
        .await
        .unwrap();
    providers
        .disable_provider(&context, scope, provider.id())
        .await
        .unwrap();
    assert_eq!(
        accounts
            .start_user_authorization(&context, scope, provider.id(), request_id, None)
            .await
            .unwrap(),
        started,
        "exact replay does not reinterpret today's Provider configuration"
    );
    providers
        .restore_provider(&context, scope, provider.id())
        .await
        .unwrap();
    let completed = accounts
        .submit_secret(
            &context,
            scope,
            started.id,
            coral_credentials::CredentialValues::token(
                Secret::new("account-secret-canary".into()).unwrap(),
            ),
        )
        .await
        .unwrap();
    let id = completed.account_id.unwrap();
    let account = accounts
        .get_account(&context, scope, provider.id(), &AccountScope::User)
        .await
        .unwrap()
        .account
        .unwrap();
    assert_eq!(account.id, id);
    assert_eq!(account.state, AccountState::Ready);
    assert!(!format!("{account:?}").contains("account-secret-canary"));
    assert!(
        !accounts
            .get_account(&context, scope, provider.id(), &AccountScope::User)
            .await
            .unwrap()
            .can_connect
    );
    assert!(
        accounts
            .get_account(&other, scope, provider.id(), &AccountScope::User)
            .await
            .unwrap()
            .account
            .is_none()
    );
    assert!(
        accounts
            .disconnect(&other, scope, provider.id(), id)
            .await
            .is_err()
    );
    assert!(matches!(
        accounts
            .start_reconnect(
                &other,
                scope,
                provider.id(),
                id,
                authorization_request(),
                None
            )
            .await,
        Err(crate::Error::AccountNotFound)
    ));
    accounts
        .disconnect(&context, scope, provider.id(), id)
        .await
        .unwrap();
    let disconnected = accounts
        .get_account(&context, scope, provider.id(), &AccountScope::User)
        .await
        .unwrap()
        .account
        .unwrap();
    assert_eq!(disconnected.id, id);
    assert_eq!(disconnected.state, AccountState::Disconnected);
    let reconnect = accounts
        .start_reconnect(
            &context,
            scope,
            provider.id(),
            id,
            authorization_request(),
            None,
        )
        .await
        .unwrap();
    accounts
        .submit_secret(
            &context,
            scope,
            reconnect.id,
            coral_credentials::CredentialValues::token(
                Secret::new("replacement-canary".into()).unwrap(),
            ),
        )
        .await
        .unwrap();
    let reconnected = accounts
        .get_account(&context, scope, provider.id(), &AccountScope::User)
        .await
        .unwrap()
        .account
        .unwrap();
    assert_eq!(reconnected.id, id);
    assert_eq!(reconnected.state, AccountState::Ready);
    workspaces.archive(&context, scope).await.unwrap();
    let archived = accounts
        .get_account(&context, scope, provider.id(), &AccountScope::User)
        .await
        .unwrap();
    assert!(!archived.can_connect);
    assert!(!archived.account.unwrap().can_manage);
    assert!(
        accounts
            .list_user_lifecycle_events(&context, scope)
            .await
            .unwrap()
            .iter()
            .any(|e| e.action() == crate::LifecycleAction::AccountDisconnected)
    );
}

#[tokio::test]
async fn pending_authorization_cap_rejects_excess_workflows_per_principal() {
    let (db, workspaces, providers) = test_modules().await;
    let context = change_context(
        organization_user(db.database(), "user:pending-cap", OrganizationRole::Owner).await,
        10,
        'a',
    );
    let workspace = workspaces
        .create(
            &context,
            WorkspaceDisplayName::parse("Pending cap").unwrap(),
        )
        .await
        .unwrap();
    let provider = publish_provider(
        &providers,
        &context,
        workspace.id(),
        crate::account_tests::CREDENTIAL_REQUIRED_OPENAPI,
        "cap",
        CredentialRoute::Caller,
    )
    .await;
    let directory = tempfile::tempdir().unwrap();
    let accounts = crate::tests::accounts_for(&providers, directory.path());
    let first_request = authorization_request();
    let first = accounts
        .start_user_authorization(
            &context,
            workspace.id(),
            provider.id(),
            first_request.clone(),
            None,
        )
        .await
        .unwrap();
    for _ in 1..super::MAX_PENDING_AUTHORIZATIONS {
        accounts
            .start_user_authorization(
                &context,
                workspace.id(),
                provider.id(),
                authorization_request(),
                None,
            )
            .await
            .unwrap();
    }
    // A replay of an accepted request resolves before the cap is consulted.
    assert_eq!(
        accounts
            .start_user_authorization(
                &context,
                workspace.id(),
                provider.id(),
                first_request.clone(),
                None,
            )
            .await
            .unwrap(),
        first
    );
    // A new workflow beyond the cap is rejected without reserving durable state.
    assert!(matches!(
        accounts
            .start_user_authorization(
                &context,
                workspace.id(),
                provider.id(),
                authorization_request(),
                None,
            )
            .await,
        Err(crate::Error::PendingAuthorizationLimit)
    ));
    let later = change_context(context.actor().clone(), first.expires_at_unix_nanos, 'b');
    accounts
        .start_user_authorization(
            &later,
            workspace.id(),
            provider.id(),
            authorization_request(),
            None,
        )
        .await
        .expect("abandoned authorizations release capacity at their deadline");
    // Replay reads the stored result without itself settling expiration.
    assert_eq!(
        accounts
            .start_user_authorization(&later, workspace.id(), provider.id(), first_request, None,)
            .await
            .unwrap()
            .state,
        AuthorizationState::Expired
    );
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one Agent journey verifies shared new-account/reconnect capacity and exact Agent ownership across initiating Owners"
)]
async fn pending_authorization_cap_counts_agent_reconnects_with_new_accounts() {
    let (db, workspaces, providers) = test_modules().await;
    let context = change_context(
        organization_user(db.database(), "user:agent-cap", OrganizationRole::Owner).await,
        10,
        'a',
    );
    let workspace = workspaces
        .create(&context, WorkspaceDisplayName::parse("Agent cap").unwrap())
        .await
        .unwrap();
    let scope = workspace.id().clone();
    let provider = publish_provider(
        &providers,
        &context,
        workspace.id(),
        crate::account_tests::CREDENTIAL_REQUIRED_OPENAPI,
        "cap",
        CredentialRoute::Caller,
    )
    .await;
    let agent = workspaces
        .create_agent(
            &context,
            workspace.id(),
            coral_workspace::AgentDisplayName::parse("Worker").unwrap(),
            None,
        )
        .await
        .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let accounts = crate::tests::accounts_for(&providers, directory.path());
    let start = async || {
        accounts
            .start_agent_authorization(
                &context,
                workspace.id(),
                agent.agent.principal().id(),
                provider.id(),
                authorization_request(),
                None,
            )
            .await
    };
    let initial = start().await.unwrap();
    start().await.unwrap(); // An initial workflow shares capacity with later reconnects.
    let account = accounts
        .submit_secret(
            &context,
            &scope,
            initial.id,
            coral_credentials::CredentialValues::token(
                Secret::new("agent-cap-secret".into()).unwrap(),
            ),
        )
        .await
        .unwrap()
        .account_id
        .unwrap();
    let reconnect = async || {
        accounts
            .start_reconnect(
                &context,
                &scope,
                provider.id(),
                account,
                authorization_request(),
                None,
            )
            .await
    };
    let first_reconnect = reconnect().await.unwrap();
    for _ in 2..super::MAX_PENDING_AUTHORIZATIONS {
        reconnect().await.unwrap();
    }
    assert!(matches!(
        reconnect().await,
        Err(Error::PendingAuthorizationLimit)
    ));
    let other = change_context(
        organization_user(
            db.database(),
            "user:other-agent-cap",
            OrganizationRole::Owner,
        )
        .await,
        10,
        'b',
    );
    crate::tests::join_as_owner(&workspaces, other.actor(), workspace.id()).await;
    assert!(matches!(
        accounts
            .start_reconnect(
                &other,
                &scope,
                provider.id(),
                account,
                authorization_request(),
                None
            )
            .await,
        Err(Error::PendingAuthorizationLimit)
    ));
    accounts
        .start_user_authorization(
            &context,
            &scope,
            provider.id(),
            authorization_request(),
            None,
        )
        .await
        .expect("Agent capacity does not consume its initiating User's allowance");
    assert_eq!(
        accounts
            .submit_secret(
                &context,
                &scope,
                first_reconnect.id,
                coral_credentials::CredentialValues::token(
                    Secret::new("agent-cap-replacement".into()).unwrap()
                ),
            )
            .await
            .unwrap()
            .account_id,
        Some(account)
    );
    reconnect()
        .await
        .expect("completion releases Agent capacity");
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one shared Account journey verifies actor and Provider isolation after disconnect"
)]
async fn pending_authorization_cap_keeps_shared_actors_and_providers_independent() {
    let (db, workspaces, providers) = test_modules().await;
    let context = change_context(
        organization_user(db.database(), "user:shared-cap", OrganizationRole::Owner).await,
        10,
        'a',
    );
    let other = change_context(
        organization_user(
            db.database(),
            "user:other-shared-cap",
            OrganizationRole::Owner,
        )
        .await,
        10,
        'b',
    );
    let workspace = workspaces
        .create(&context, WorkspaceDisplayName::parse("Shared cap").unwrap())
        .await
        .unwrap();
    crate::tests::join_as_owner(&workspaces, other.actor(), workspace.id()).await;
    let directory = tempfile::tempdir().unwrap();
    let accounts = crate::tests::accounts_for(&providers, directory.path());
    let provider = providers
        .publish_shared_fixture(
            &context,
            workspace.id(),
            crate::OpenApiDocument::parse(crate::account_tests::CREDENTIAL_REQUIRED_OPENAPI)
                .unwrap(),
            "shared_cap",
            &accounts,
            coral_credentials::CredentialValues::token(
                Secret::new("shared-cap-secret".into()).unwrap(),
            ),
        )
        .await
        .unwrap();
    let other_provider = providers
        .publish_shared_fixture(
            &context,
            workspace.id(),
            crate::OpenApiDocument::parse(crate::account_tests::CREDENTIAL_REQUIRED_OPENAPI)
                .unwrap(),
            "other_cap",
            &accounts,
            coral_credentials::CredentialValues::token(
                Secret::new("other-cap-secret".into()).unwrap(),
            ),
        )
        .await
        .unwrap();
    for current in [&provider, &other_provider] {
        let account = accounts
            .get_account(
                &context,
                workspace.id(),
                current.id(),
                &AccountScope::Shared,
            )
            .await
            .unwrap()
            .account
            .unwrap();
        accounts
            .disconnect(&context, workspace.id(), current.id(), account.id)
            .await
            .unwrap();
    }
    for _ in 0..super::MAX_PENDING_AUTHORIZATIONS {
        accounts
            .start_secret(
                &context,
                workspace.id(),
                provider.id(),
                authorization_request(),
            )
            .await
            .unwrap();
    }
    let rejected_request = authorization_request();
    assert!(matches!(
        accounts
            .start_secret(
                &context,
                workspace.id(),
                provider.id(),
                rejected_request.clone()
            )
            .await,
        Err(Error::PendingAuthorizationLimit)
    ));
    accounts
        .start_secret(&other, workspace.id(), provider.id(), rejected_request)
        .await
        .expect("another actor has independent capacity; a rejected request reserves nothing");
    accounts
        .start_secret(
            &context,
            workspace.id(),
            other_provider.id(),
            authorization_request(),
        )
        .await
        .expect("another Provider has independent capacity");
}

/// An earlier Coral could publish an OAuth Provider with no OAuth application
/// (#2501). Nobody can start a sign-in to it, so Coral offers no Connect, and
/// a Connect started anyway is refused without an attempt.
#[tokio::test]
async fn an_oauth_provider_without_an_application_offers_no_connect() {
    let (db, workspaces, providers) = test_modules().await;
    let context = change_context(
        organization_user(
            db.database(),
            "user:no-application",
            OrganizationRole::Owner,
        )
        .await,
        10,
        'a',
    );
    let workspace = workspaces
        .create(
            &context,
            WorkspaceDisplayName::parse("No application").unwrap(),
        )
        .await
        .unwrap();
    let document = serde_json::json!({
        "openapi":"3.1.0", "info":{"title":"OAuth","version":"1"},
        "servers":[{"url":"https://music.example"}], "security":[{"oauth":["read"]}],
        "components":{"securitySchemes":{"oauth":{"type":"oauth2","flows":{"authorizationCode":{
            "authorizationUrl":"https://music.example/authorize",
            "tokenUrl":"https://music.example/token", "scopes":{"read":"Read"}
        }}}}},
        "paths":{"/data":{"get":{"operationId":"getData","responses":{"200":{"description":"Data"}}}}}
    });
    let provider = publish_provider(
        &providers,
        &context,
        workspace.id(),
        &document.to_string(),
        "music",
        CredentialRoute::Caller,
    )
    .await;
    let directory = tempfile::tempdir().unwrap();
    let accounts = crate::tests::accounts_for(&providers, directory.path());
    let named = accounts
        .get_account(&context, workspace.id(), provider.id(), &AccountScope::User)
        .await
        .unwrap();
    assert!(named.can_connect, "a named application can start a sign-in");
    let stored = named.contract.to_storage().unwrap();
    let unnamed = named
        .contract
        .clone()
        .with_client_policy(None)
        .to_storage()
        .unwrap();
    assert_eq!(
        db.execute_fixture_sql(&format!(
            "UPDATE provider_generations SET credential_contract = '{unnamed}' \
             WHERE credential_contract = '{stored}'"
        ))
        .await
        .unwrap(),
        1
    );
    let view = accounts
        .get_account(&context, workspace.id(), provider.id(), &AccountScope::User)
        .await
        .unwrap();
    assert_eq!(view.contract.oauth_client(), None);
    assert!(!view.can_connect);
    assert!(!view.can_reconnect);
    let started = accounts
        .start_user_authorization(
            &context,
            workspace.id(),
            provider.id(),
            authorization_request(),
            Some(
                crate::account::OAuthClient::from_provider("https://reef.example/callback".into())
                    .unwrap(),
            ),
        )
        .await;
    assert!(
        matches!(started, Err(crate::Error::OAuthClientConfiguration)),
        "{started:?}"
    );
    assert_eq!(
        accounts
            .get_account(&context, workspace.id(), provider.id(), &AccountScope::User)
            .await
            .unwrap(),
        view,
        "the refusal created no Account and no attempt"
    );
}
