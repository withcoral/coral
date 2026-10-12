use crate::account::{AccountScope, AccountState};
use crate::account_tests::CREDENTIAL_REQUIRED_OPENAPI;
use crate::tests::{change_context, organization_user, test_modules};
use coral_credentials::Secret;
use coral_workspace::{OrganizationRole, WorkspaceDisplayName, WorkspaceRole};
coral_test_support::provider_setup_fixtures!(crate);

#[tokio::test]
async fn workspace_owner_inspects_one_shared_account_with_management_authority() {
    let (db, workspaces, providers) = test_modules().await;
    let admin = change_context(
        organization_user(db.database(), "user:shared-admin", OrganizationRole::Owner).await,
        10,
        'a',
    );
    let member =
        organization_user(db.database(), "user:shared-owner", OrganizationRole::Member).await;
    let context = change_context(member.clone(), 10, 'b');
    let workspace = workspaces
        .create(
            &admin,
            WorkspaceDisplayName::parse("Shared Account").unwrap(),
        )
        .await
        .unwrap();
    workspaces
        .set_membership(&admin, workspace.id(), &member, WorkspaceRole::Owner)
        .await
        .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let accounts = crate::tests::accounts_for(&providers, directory.path());
    let provider = providers
        .publish_shared_fixture(
            &admin,
            workspace.id(),
            crate::OpenApiDocument::parse(CREDENTIAL_REQUIRED_OPENAPI).unwrap(),
            "shared",
            &accounts,
            coral_credentials::CredentialValues::token(
                Secret::new("shared-secret-canary".into()).unwrap(),
            ),
        )
        .await
        .unwrap();
    let account = accounts
        .get_account(
            &context,
            workspace.id(),
            provider.id(),
            &AccountScope::Shared,
        )
        .await
        .unwrap()
        .account
        .unwrap();
    assert_eq!(account.state, AccountState::Ready);
    assert!(account.can_manage);
    assert_eq!(account.connected_at_unix_nanos, Some(10));
    assert_eq!(
        account.configured_by_user_id.as_ref(),
        Some(admin.actor().user_id())
    );
    assert!(!format!("{account:?}").contains("shared-secret-canary"));
    assert!(
        accounts
            .get_account(&context, workspace.id(), provider.id(), &AccountScope::User)
            .await
            .unwrap()
            .account
            .is_none(),
        "shared credentials never become the configuring human's private Account"
    );
    providers
        .disable_provider(&admin, workspace.id(), provider.id())
        .await
        .unwrap();
    assert_eq!(
        accounts
            .get_account(
                &context,
                workspace.id(),
                provider.id(),
                &AccountScope::Shared
            )
            .await
            .unwrap()
            .account,
        Some(account.clone())
    );
    workspaces.archive(&context, workspace.id()).await.unwrap();
    let mut archived = account;
    archived.can_manage = false;
    assert_eq!(
        accounts
            .get_account(
                &context,
                workspace.id(),
                provider.id(),
                &AccountScope::Shared
            )
            .await
            .unwrap()
            .account,
        Some(archived)
    );
}
