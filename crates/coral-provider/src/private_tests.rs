//! The Private Provider: one per Workspace at `coral.private`, which Coral
//! performs, with no Accounts. Its Tools are the Private Tools that
//! coral-tools keeps.
use coral_workspace::{OrganizationRole, WorkspaceDisplayName};

use super::tests::{change_context, organization_user, test_modules};
use crate::access::CredentialRoute;
use crate::{Error, LifecycleAction, LifecycleActor, ProviderCallRoot, ProviderState};

// Catches: a Workspace whose Private Provider is not at `coral.private`, a
// second one, or one that needs Accounts. The boot's install of a missing
// one is tested with the Built-in Provider's (`builtin_tests.rs`).
#[tokio::test]
async fn each_workspace_has_one_private_provider_installed_at_creation() {
    let (fixture, workspaces, providers) = test_modules().await;
    let database = fixture.database();
    let owner = organization_user(database.clone(), "user:owner", OrganizationRole::Member).await;
    let context = change_context(owner.clone(), 10, '1');
    let created = workspaces
        .create(&context, WorkspaceDisplayName::parse("Created").unwrap())
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

    let mut transaction = database.begin_read_snapshot().await.unwrap();
    let found = crate::private_provider_in_transaction(&mut transaction, created.id())
        .await
        .unwrap();
    transaction.rollback().await.unwrap();
    assert_eq!(found.as_ref(), Some(&installed));

    let [event] = providers
        .list_lifecycle_events(&owner, created.id())
        .await
        .unwrap()
        .try_into()
        .expect("the install records one event");
    assert_eq!(event.action(), LifecycleAction::PrivateProviderInstalled);
    assert!(
        matches!(event.actor(), LifecycleActor::User { .. }),
        "{event:?}"
    );
}
