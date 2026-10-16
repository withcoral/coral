//! Joint account-update and offboarding acceptance through the Administration and Provider interfaces.
mod agents;
mod creation;

use super::*;
use coral_credentials::Secret;
use coral_provider::{
    ProviderPublicationOutcome, ProviderRegenerationRequest, ProviderSetupSource,
};

#[derive(Clone, Copy, Debug)]
enum Removal {
    Workspace,
    Organization,
}

async fn remove(f: &AccountFixture, removal: Removal) {
    match removal {
        Removal::Workspace => f
            .lifecycle
            .remove_workspace_member(&change(&f.owner), f.workspace.id(), f.member.user_id())
            .await
            .unwrap(),
        Removal::Organization => f
            .lifecycle
            .remove_organization_member(&change(&f.owner), f.member.user_id(), 1, None)
            .await
            .unwrap(),
    }
}

async fn rejoin(f: &AccountFixture, removal: Removal) {
    if matches!(removal, Removal::Organization) {
        f.organization
            .provision_login(&change(&f.member))
            .await
            .unwrap();
    }
    f.workspaces
        .set_membership(
            &change(&f.owner),
            f.workspace.id(),
            &f.member,
            coral_workspace::WorkspaceRole::Owner,
        )
        .await
        .unwrap();
}

// Catalog-only, caller-to-shared, shared replacement, shared-to-caller.
const SETTINGS: [(bool, bool); 4] = [(false, false), (false, true), (true, true), (true, false)];

async fn connected_account(
    f: &AccountFixture,
    providers: &ProviderModule,
    initially_shared: bool,
) -> coral_provider::account::ProviderAccountId {
    if initially_shared {
        let initial = candidate(f, providers, true, "initial-shared-secret").await;
        providers
            .regenerate_provider(&change(&f.member), initial)
            .await
            .unwrap();
        f.accounts
            .get_account(
                &change(&f.member),
                f.workspace.id(),
                &f.provider,
                &AccountScope::Shared,
            )
            .await
            .unwrap()
            .account
            .unwrap()
            .id
    } else {
        f.connect(&f.member).await.account_id.unwrap()
    }
}

async fn candidate(
    f: &AccountFixture,
    providers: &ProviderModule,
    shared: bool,
    secret: &str,
) -> ProviderRegenerationRequest {
    let context = change(&f.member);
    let setup = providers
        .prepare_setup(
            &context,
            f.workspace.id(),
            ProviderSetupSource::Current,
            Some(&f.provider),
        )
        .await
        .unwrap();
    let review = providers
        .review_setup_configuration(&f.member, f.workspace.id(), setup.id())
        .await
        .unwrap();
    let review = providers
        .confirm_setup_configuration(
            &context,
            f.workspace.id(),
            setup.id(),
            review.revision,
            review.confirmed.unwrap(),
        )
        .await
        .unwrap();
    let request = ProviderRegenerationRequest::new(
        request(),
        f.workspace.id().clone(),
        f.provider.clone(),
        setup.id().clone(),
        review.revision,
    );
    if shared {
        let grant = f
            .accounts
            .authorize_setup(
                &context,
                f.workspace.id(),
                setup.id(),
                review.revision,
                coral_credentials::CredentialValues::token(Secret::new(secret.into()).unwrap()),
            )
            .await
            .unwrap()
            .provisional_grant_id
            .unwrap();
        request
            .with_account_setting(CredentialRoute::Fixed)
            .with_account_grant(grant)
            .with_incompatible_configuration_confirmation()
    } else {
        request
            .with_account_setting(CredentialRoute::Caller)
            .with_incompatible_configuration_confirmation()
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn offboarding_serializes_prepared_catalog_updates_replacements_and_setting_changes() {
    for removal in [Removal::Workspace, Removal::Organization] {
        for (initially_shared, shared) in SETTINGS {
            Box::pin(removal_wins(removal, initially_shared, shared)).await;
        }
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "one concurrent command verifies the active pair, canceled grant, callback fence and atomic event evidence"
)]
async fn removal_wins(removal: Removal, initially_shared: bool, shared: bool) {
    let mut f = AccountFixture::new().await;
    let providers = ProviderModule::with_running_catalog(
        coral_outbound::DestinationPolicy::default()
            .with_loopback(coral_outbound::NetworkAccess::HttpAndHttps),
        f.db.database(),
        f.workspaces.clone(),
    );
    let connected = connected_account(&f, &providers, initially_shared).await;
    let pending = f
        .accounts
        .start_reconnect(
            &change(&f.member),
            f.workspace.id(),
            &f.provider,
            connected,
            request(),
            None,
        )
        .await
        .unwrap();
    let update = candidate(&f, &providers, shared, "candidate-secret").await;
    let before = providers
        .inspect_provider(&f.owner, f.workspace.id(), &f.provider)
        .await
        .unwrap();
    let gate = Arc::new(RemovalGate::default());
    f.lifecycle.pause_after_prepare = Some(gate.clone());
    let f = Arc::new(f);
    let removing = {
        let f = f.clone();
        tokio::spawn(async move {
            remove(&f, removal).await;
        })
    };
    tokio::time::timeout(std::time::Duration::from_secs(5), gate.prepared.notified())
        .await
        .unwrap();
    let context = change(&f.member);
    // Start publication while offboarding holds current authority, then release
    // removal. No sleep or scheduler timing decides which transaction wins.
    let (result, ()) = tokio::join!(biased;
        providers.regenerate_provider(&context, update.clone()),
        async { gate.proceed.notify_one(); },
    );
    removing.await.unwrap();
    result.unwrap_err();
    assert_eq!(
        providers
            .inspect_provider(&f.owner, f.workspace.id(), &f.provider)
            .await
            .unwrap(),
        before
    );
    let events = providers
        .list_lifecycle_events(&f.owner, f.workspace.id())
        .await
        .unwrap();
    rejoin(&f, removal).await;
    assert_eq!(
        providers
            .get_setup(&f.member, f.workspace.id(), update.setup_id())
            .await
            .unwrap()
            .state(),
        coral_provider::ProviderSetupState::Canceled
    );
    assert!(
        !providers
            .setup_authentication(&f.member, f.workspace.id(), update.setup_id(), 100)
            .await
            .unwrap()
            .authorized
    );
    assert_eq!(
        f.accounts
            .submit_secret(
                &context,
                f.workspace.id(),
                pending.id,
                coral_credentials::CredentialValues::token(
                    Secret::new("late-secret".into()).unwrap()
                ),
            )
            .await
            .unwrap()
            .state,
        AuthorizationState::Canceled
    );
    providers
        .regenerate_provider(&context, update)
        .await
        .unwrap_err();
    let scope = if initially_shared {
        AccountScope::Shared
    } else {
        AccountScope::User
    };
    let account = f
        .accounts
        .get_account(&context, f.workspace.id(), &f.provider, &scope)
        .await
        .unwrap()
        .account;
    if initially_shared {
        assert_eq!(account.unwrap().id, connected);
        let evidence = credential_evidence(&f.db.database(), f.workspace.id(), &f.owner).await;
        assert_eq!(
            f.accounts
                .credentials(f.workspace.id(), &f.provider, &evidence, 100)
                .await
                .unwrap()
                .get(coral_credentials::CredentialValues::TOKEN)
                .unwrap()
                .expose(),
            "initial-shared-secret"
        );
    } else {
        assert!(account.is_none());
    }
    assert_eq!(
        providers
            .list_lifecycle_events(&f.owner, f.workspace.id())
            .await
            .unwrap(),
        events
    );
}

#[tokio::test]
async fn publication_before_offboarding_preserves_shared_accounts_but_retires_caller_accounts() {
    for removal in [Removal::Workspace, Removal::Organization] {
        for (initially_shared, shared) in SETTINGS {
            let f = AccountFixture::new().await;
            let providers = ProviderModule::with_running_catalog(
                coral_outbound::DestinationPolicy::default()
                    .with_loopback(coral_outbound::NetworkAccess::HttpAndHttps),
                f.db.database(),
                f.workspaces.clone(),
            );
            connected_account(&f, &providers, initially_shared).await;
            let update = candidate(&f, &providers, shared, "candidate-secret").await;
            let result = providers
                .regenerate_provider(&change(&f.member), update)
                .await
                .unwrap();
            assert_eq!(result.outcome(), ProviderPublicationOutcome::Succeeded);
            let before = providers
                .inspect_provider(&f.owner, f.workspace.id(), &f.provider)
                .await
                .unwrap();
            let scope = if shared {
                AccountScope::Shared
            } else {
                AccountScope::User
            };
            let account = f
                .accounts
                .get_account(&change(&f.member), f.workspace.id(), &f.provider, &scope)
                .await
                .unwrap();
            remove(&f, removal).await;
            rejoin(&f, removal).await;
            assert_eq!(
                providers
                    .inspect_provider(&f.owner, f.workspace.id(), &f.provider)
                    .await
                    .unwrap(),
                before
            );
            let after = f
                .accounts
                .get_account(&change(&f.member), f.workspace.id(), &f.provider, &scope)
                .await
                .unwrap();
            if shared {
                assert_eq!(after, account);
                let evidence =
                    credential_evidence(&f.db.database(), f.workspace.id(), &f.owner).await;
                assert_eq!(
                    f.accounts
                        .credentials(f.workspace.id(), &f.provider, &evidence, 100)
                        .await
                        .unwrap()
                        .get(coral_credentials::CredentialValues::TOKEN)
                        .unwrap()
                        .expose(),
                    "candidate-secret"
                );
            } else {
                assert!(after.account.is_none());
            }
        }
    }
}
