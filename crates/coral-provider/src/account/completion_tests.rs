//! Initial completion and disconnect fencing through the public Account interface.
use super::*;
use crate::account_tests::{CREDENTIAL_REQUIRED_OPENAPI, authorization_request, publish_provider};
use crate::tests::{change_context, organization_user, test_modules};
use coral_credentials::Secret;
use coral_workspace::{AgentDisplayName, OrganizationRole, WorkspaceDisplayName};
coral_test_support::provider_setup_fixtures!(crate);

#[tokio::test]
async fn user_initial_completion_cannot_resurrect_after_disconnect_and_restart() {
    Box::pin(initial_completion_fence(false, false)).await;
}

#[tokio::test]
async fn agent_initial_completion_cannot_resurrect_after_disconnect_and_restart() {
    Box::pin(initial_completion_fence(true, false)).await;
}

#[tokio::test]
async fn shared_initial_completion_cannot_resurrect_after_disconnect_and_restart() {
    Box::pin(initial_completion_fence(false, true)).await;
}

#[expect(
    clippy::too_many_lines,
    reason = "one restart journey covers the same durable fence for each Account scope"
)]
async fn initial_completion_fence(for_agent: bool, shared: bool) {
    let (db, workspaces, providers) = test_modules().await;
    let context = change_context(
        organization_user(db.database(), "user:intent", OrganizationRole::Owner).await,
        10,
        'a',
    );
    let workspace = workspaces
        .create(&context, WorkspaceDisplayName::parse("Intent").unwrap())
        .await
        .unwrap();
    let scope = workspace.id();
    let directory = tempfile::tempdir().unwrap();
    let accounts = crate::tests::accounts_for(&providers, directory.path());
    let provider = if shared {
        providers
            .publish_shared_fixture(
                &context,
                scope,
                crate::OpenApiDocument::parse(CREDENTIAL_REQUIRED_OPENAPI).unwrap(),
                "accounts",
                &accounts,
                coral_credentials::CredentialValues::token(
                    Secret::new("setup-secret".into()).unwrap(),
                ),
            )
            .await
            .unwrap()
    } else {
        publish_provider(
            &providers,
            &context,
            scope,
            CREDENTIAL_REQUIRED_OPENAPI,
            "accounts",
            CredentialRoute::Caller,
        )
        .await
    };
    let filter = if for_agent {
        let agent = workspaces
            .create_agent(
                &context,
                scope,
                AgentDisplayName::parse("Worker").unwrap(),
                None,
            )
            .await
            .unwrap();
        AccountScope::Agent(agent.agent.principal().id().clone())
    } else if shared {
        AccountScope::Shared
    } else {
        AccountScope::User
    };
    if shared {
        assert!(
            accounts
                .start_secret(&context, scope, provider.id(), authorization_request())
                .await
                .is_err(),
            "initial shared connection cannot replace published credentials"
        );
        let account = accounts
            .get_account(&context, scope, provider.id(), &filter)
            .await
            .unwrap()
            .account
            .unwrap();
        accounts
            .disconnect(&context, scope, provider.id(), account.id)
            .await
            .unwrap();
    } else {
        assert!(
            accounts
                .get_account(&context, scope, provider.id(), &filter)
                .await
                .unwrap()
                .account
                .is_none()
        );
    }
    let start = async |accounts: &AccountModule| match &filter {
        AccountScope::Agent(agent) => accounts
            .start_agent_authorization(
                &context,
                scope,
                agent,
                provider.id(),
                authorization_request(),
                None,
            )
            .await
            .unwrap(),
        AccountScope::User => accounts
            .start_user_authorization(
                &context,
                scope,
                provider.id(),
                authorization_request(),
                None,
            )
            .await
            .unwrap(),
        AccountScope::Shared => accounts
            .start_secret(&context, scope, provider.id(), authorization_request())
            .await
            .unwrap(),
    };
    let initial = start(&accounts).await;
    let older = start(&accounts).await;
    let before = accounts
        .get_account(&context, scope, provider.id(), &filter)
        .await
        .unwrap()
        .account
        .unwrap();
    assert_eq!(before.state, AccountState::Disconnected);
    if !shared {
        assert_eq!(before.connected_at_unix_nanos, None);
        assert_eq!(before.configured_by_user_id, None);
    }
    let first = accounts
        .submit_secret(
            &context,
            scope,
            initial.id,
            coral_credentials::CredentialValues::token(Secret::new("winner".into()).unwrap()),
        )
        .await
        .unwrap();
    assert_eq!(first.state, AuthorizationState::Succeeded);
    assert_eq!(first.account_id, Some(before.id));
    let connected = accounts
        .get_account(&context, scope, provider.id(), &filter)
        .await
        .unwrap()
        .account
        .unwrap();
    assert_eq!(connected.state, AccountState::Ready);
    accounts
        .disconnect(&context, scope, provider.id(), before.id)
        .await
        .unwrap();
    drop(accounts);
    let accounts = crate::tests::accounts_for(&providers, directory.path());
    let completed = accounts
        .submit_secret(
            &context,
            scope,
            older.id,
            coral_credentials::CredentialValues::token(
                Secret::new("obsolete-canary".into()).unwrap(),
            ),
        )
        .await
        .unwrap();
    assert_eq!(completed.state, AuthorizationState::Failed);
    assert_eq!(completed.account_id, None);
    let after = accounts
        .get_account(&context, scope, provider.id(), &filter)
        .await
        .unwrap()
        .account
        .unwrap();
    assert_eq!(after.id, before.id);
    assert_eq!(after.state, AccountState::Disconnected);
    assert_eq!(
        after.connected_at_unix_nanos,
        connected.connected_at_unix_nanos
    );
    assert_eq!(after.configured_by_user_id, connected.configured_by_user_id);
    assert_eq!(
        accounts
            .submit_secret(
                &context,
                scope,
                older.id,
                coral_credentials::CredentialValues::token(Secret::new("replay".into()).unwrap())
            )
            .await
            .unwrap(),
        completed
    );
    let events = if for_agent || shared {
        providers
            .list_lifecycle_events(context.actor(), scope)
            .await
            .unwrap()
    } else {
        accounts
            .list_user_lifecycle_events(&context, scope)
            .await
            .unwrap()
    };
    assert_eq!(
        events
            .iter()
            .filter(
                |e| e.action() == LifecycleAction::AccountAuthorizationCompleted
                    && e.target_id() == older.id.to_string()
            )
            .count(),
        1
    );
    assert!(!format!("{after:?}{events:?}").contains("obsolete-canary"));
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one concurrent connection journey verifies a stable Account and exactly one successful lifecycle outcome"
)]
async fn concurrent_initial_completions_have_one_winner_and_one_stable_account() {
    let (db, workspaces, providers) = test_modules().await;
    let context = change_context(
        organization_user(db.database(), "user:concurrent", OrganizationRole::Owner).await,
        10,
        'a',
    );
    let workspace = workspaces
        .create(&context, WorkspaceDisplayName::parse("Concurrent").unwrap())
        .await
        .unwrap();
    let scope = workspace.id();
    let provider = publish_provider(
        &providers,
        &context,
        scope,
        CREDENTIAL_REQUIRED_OPENAPI,
        "accounts",
        CredentialRoute::Caller,
    )
    .await;
    let directory = tempfile::tempdir().unwrap();
    let accounts = crate::tests::accounts_for(&providers, directory.path());
    let (left, right) = tokio::join!(
        accounts.start_user_authorization(
            &context,
            scope,
            provider.id(),
            authorization_request(),
            None
        ),
        accounts.start_user_authorization(
            &context,
            scope,
            provider.id(),
            authorization_request(),
            None
        )
    );
    let left = left.unwrap();
    let right = right.unwrap();
    let initial = accounts
        .get_account(&context, scope, provider.id(), &AccountScope::User)
        .await
        .unwrap()
        .account
        .unwrap();
    let (left, right) = tokio::join!(
        accounts.submit_secret(
            &context,
            scope,
            left.id,
            coral_credentials::CredentialValues::token(Secret::new("left".into()).unwrap())
        ),
        accounts.submit_secret(
            &context,
            scope,
            right.id,
            coral_credentials::CredentialValues::token(Secret::new("right".into()).unwrap())
        )
    );
    let results = [left.unwrap(), right.unwrap()];
    assert_eq!(results.iter().filter(|r| r.state == AuthorizationState::Succeeded && r.account_id == Some(initial.id)).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|r| r.state == AuthorizationState::Failed && r.account_id.is_none())
            .count(),
        1
    );
    let current = accounts
        .get_account(&context, scope, provider.id(), &AccountScope::User)
        .await
        .unwrap()
        .account
        .unwrap();
    assert_eq!(current.id, initial.id);
    assert_eq!(current.state, AccountState::Ready);
    assert!(
        accounts
            .start_user_authorization(
                &context,
                scope,
                provider.id(),
                authorization_request(),
                None
            )
            .await
            .is_err(),
        "initial connection is not replacement"
    );
    assert!(
        accounts
            .start_reconnect(
                &context,
                scope,
                provider.id(),
                ProviderAccountId::parse(&uuid::Uuid::new_v4().to_string()).unwrap(),
                authorization_request(),
                None
            )
            .await
            .is_err(),
        "obsolete references cannot reconnect"
    );
    let events = accounts
        .list_user_lifecycle_events(&context, scope)
        .await
        .unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|e| e.action() == LifecycleAction::AccountConnected)
            .count(),
        1
    );
}

/// An OpenAPI document whose one Tool needs HTTP Basic credentials.
const BASIC_OPENAPI: &str = r#"{
  "openapi": "3.1.0", "info": {"title": "Basic", "version": "1"},
  "servers": [{"url": "https://api.example.test"}],
  "security": [{"basicAuth": []}],
  "components": {"securitySchemes": {"basicAuth": {"type": "http", "scheme": "basic"}}},
  "paths": {"/data": {"get": {"operationId": "getData", "responses": {"200": {"description": "Data"}}}}}
}"#;

/// Typed-in methods other than a bearer token still connect accounts after the
/// Provider is published, for a person's own account and for a shared one.
#[tokio::test]
async fn a_published_basic_provider_connects_own_and_shared_accounts() {
    let h = crate::setup_tests::harness().await;
    let (providers, accounts) = (&h.providers, &h.accounts);
    let context = change_context(
        organization_user(h.database.clone(), "user:basic", OrganizationRole::Owner).await,
        10,
        'a',
    );
    let workspace = h
        .workspaces
        .create(&context, WorkspaceDisplayName::parse("Basic").unwrap())
        .await
        .unwrap();
    let scope = workspace.id();
    let principal = crate::setup_tests::user_principal(context.actor());
    let connected = async |provider: &crate::Provider| {
        let values = crate::setup_tests::credential_for(
            &h,
            scope,
            provider.id(),
            provider.access().call_root().name(),
            "getData",
            &principal,
            11,
        )
        .await
        .unwrap();
        ["username", "password"].map(|name| values.get(name).unwrap().expose().to_owned())
    };
    let values = || {
        coral_credentials::CredentialValues::new(
            [("password", "token"), ("username", "person@example.test")]
                .into_iter()
                .map(|(name, value)| (name.to_owned(), Secret::new(value.into()).unwrap()))
                .collect(),
        )
        .unwrap()
    };
    let own = publish_provider(
        providers,
        &context,
        scope,
        BASIC_OPENAPI,
        "basic_own",
        CredentialRoute::Caller,
    )
    .await;
    let started = accounts
        .start_user_authorization(&context, scope, own.id(), authorization_request(), None)
        .await
        .unwrap();
    let submitted = accounts
        .submit_secret(&context, scope, started.id, values())
        .await
        .unwrap();
    assert_eq!(submitted.state, AuthorizationState::Succeeded);
    assert_eq!(connected(&own).await, ["person@example.test", "token"]);

    // A shared account is connected again after it is disconnected.
    let shared = providers
        .publish_shared_fixture(
            &context,
            scope,
            crate::OpenApiDocument::parse(BASIC_OPENAPI).unwrap(),
            "basic_shared",
            accounts,
            values(),
        )
        .await
        .unwrap();
    let account = accounts
        .get_account(&context, scope, shared.id(), &AccountScope::Shared)
        .await
        .unwrap()
        .account
        .unwrap();
    accounts
        .disconnect(&context, scope, shared.id(), account.id)
        .await
        .unwrap();
    let started = accounts
        .start_secret(&context, scope, shared.id(), authorization_request())
        .await
        .unwrap();
    let submitted = accounts
        .submit_secret(&context, scope, started.id, values())
        .await
        .unwrap();
    assert_eq!(submitted.state, AuthorizationState::Succeeded);
    assert_eq!(connected(&shared).await, ["person@example.test", "token"]);
}
