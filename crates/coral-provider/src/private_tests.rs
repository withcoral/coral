//! The Private Provider: one per Workspace at `coral.private`, which Coral
//! performs, with no Accounts. Its Tools are the Private Tools that
//! coral-functions keeps.
use coral_workspace::{OrganizationRole, WorkspaceDisplayName};

use super::tests::{change_context, organization_user, test_modules};
use crate::access::CredentialRoute;
use crate::{Error, LifecycleAction, LifecycleActor, ProviderCallRoot, ProviderState};

// Catches: a Workspace without its Private Provider, a second one, one that
// needs Accounts, or a boot that misses a Workspace
// created before Coral had it.
#[tokio::test]
async fn each_workspace_has_one_private_provider_installed_at_creation_or_at_boot() {
    let (fixture, workspaces, providers) = test_modules().await;
    let database = fixture.database();
    let owner = organization_user(database.clone(), "user:owner", OrganizationRole::Member).await;
    let context = change_context(owner.clone(), 10, '1');
    let created = workspaces
        .create(&context, WorkspaceDisplayName::parse("Created").unwrap())
        .await
        .unwrap();
    let earlier = workspaces
        .create(&context, WorkspaceDisplayName::parse("Earlier").unwrap())
        .await
        .unwrap();

    let mut transaction = database.begin().await.unwrap();
    let installed = providers
        .install_private_in_transaction(&mut transaction, &context, created.id())
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    assert_eq!(installed.call_root(), &ProviderCallRoot::Private);
    assert_eq!(installed.call_root().as_str(), "coral.private");
    assert_eq!(installed.credential_route(), CredentialRoute::None);
    assert_eq!(installed.state(), ProviderState::Active);
    assert_eq!(installed.display_name(), "Private");

    let mut transaction = database.begin().await.unwrap();
    let again = providers
        .install_private_in_transaction(&mut transaction, &context, created.id())
        .await;
    assert!(
        matches!(again, Err(Error::ProviderCallRootConflict)),
        "{again:?}"
    );
    transaction.rollback().await.unwrap();

    assert_eq!(
        providers.workspaces_without_private().await.unwrap(),
        [earlier.id().clone()]
    );
    let booted = providers
        .install_missing_private(earlier.id(), 20)
        .await
        .unwrap()
        .expect("boot installs the missing Private Provider");
    assert_eq!(booted.call_root(), &ProviderCallRoot::Private);
    assert_eq!(
        providers
            .install_missing_private(created.id(), 21)
            .await
            .unwrap(),
        None
    );
    assert!(
        providers
            .workspaces_without_private()
            .await
            .unwrap()
            .is_empty()
    );

    for (workspace, provider, actor_is_coral) in
        [(&created, &installed, false), (&earlier, &booted, true)]
    {
        let mut transaction = database.begin_read_snapshot().await.unwrap();
        let found = crate::private_provider_in_transaction(&mut transaction, workspace.id())
            .await
            .unwrap();
        transaction.rollback().await.unwrap();
        assert_eq!(found.as_ref(), Some(provider));

        let [event] = providers
            .list_lifecycle_events(&owner, workspace.id())
            .await
            .unwrap()
            .try_into()
            .expect("the install records one event");
        assert_eq!(event.action(), LifecycleAction::PrivateProviderInstalled);
        assert_eq!(
            matches!(event.actor(), LifecycleActor::Coral),
            actor_is_coral,
            "{event:?}"
        );
    }
}
