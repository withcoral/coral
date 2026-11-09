//! The Private Provider at `coral.private` and the Shared Provider at
//! `coral.shared`: one of each per Workspace, which Coral performs, with no
//! Accounts. Their Tools are the Private and Shared Tools that coral-tools
//! keeps.
use coral_workspace::{OrganizationRole, WorkspaceDisplayName};

use super::tests::{change_context, organization_user, test_modules};
use crate::access::CredentialRoute;
use crate::{Error, LifecycleAction, LifecycleActor, ProviderCallRoot, ProviderState};

// Catches: a Workspace whose Private or Shared Provider is not at its Call
// Root, a second one, or one that needs Accounts. The boot's install of a
// missing one is tested with the Built-in Provider's (`builtin_tests.rs`).
#[tokio::test]
async fn each_workspace_has_one_private_and_one_shared_provider_installed_at_creation() {
    // (the Call Root, its text, the display name, the lifecycle action)
    let cases = [
        (
            ProviderCallRoot::Private,
            "coral.private",
            "Private",
            LifecycleAction::PrivateProviderInstalled,
        ),
        (
            ProviderCallRoot::Shared,
            "coral.shared",
            "Shared",
            LifecycleAction::SharedProviderInstalled,
        ),
    ];
    for (call_root, root, display_name, action) in cases {
        let (fixture, workspaces, providers) = test_modules().await;
        let database = fixture.database();
        let owner =
            organization_user(database.clone(), "user:owner", OrganizationRole::Member).await;
        let context = change_context(owner.clone(), 10, '1');
        let created = workspaces
            .create(&context, WorkspaceDisplayName::parse("Created").unwrap())
            .await
            .unwrap();
        let install = async |transaction: &mut coral_db::Transaction<'_>| {
            if call_root == ProviderCallRoot::Private {
                providers
                    .install_private_in_transaction(transaction, &context, created.id())
                    .await
            } else {
                providers
                    .install_shared_in_transaction(transaction, &context, created.id())
                    .await
            }
        };

        let mut transaction = database.begin().await.unwrap();
        let installed = install(&mut transaction).await.unwrap();
        transaction.commit().await.unwrap();
        assert_eq!(installed.call_root(), &call_root);
        assert_eq!(installed.call_root().as_str(), root);
        assert_eq!(installed.credential_route(), CredentialRoute::None);
        assert_eq!(installed.state(), ProviderState::Active);
        assert_eq!(installed.display_name(), display_name);

        let mut transaction = database.begin().await.unwrap();
        let again = install(&mut transaction).await;
        assert!(
            matches!(again, Err(Error::ProviderCallRootConflict)),
            "{root}: {again:?}"
        );
        transaction.rollback().await.unwrap();

        let mut transaction = database.begin_read_snapshot().await.unwrap();
        let found = if call_root == ProviderCallRoot::Private {
            crate::authored_tool_provider_in_transaction(
                &mut transaction,
                created.id(),
                coral_catalog::AuthoredToolKind::Private,
            )
            .await
        } else {
            crate::authored_tool_provider_in_transaction(
                &mut transaction,
                created.id(),
                coral_catalog::AuthoredToolKind::Shared,
            )
            .await
        }
        .unwrap();
        transaction.rollback().await.unwrap();
        assert_eq!(found.as_ref(), Some(&installed), "{root}");

        let [event] = providers
            .list_lifecycle_events(&owner, created.id())
            .await
            .unwrap()
            .try_into()
            .expect("the install records one event");
        assert_eq!(event.action(), action, "{root}");
        assert!(
            matches!(event.actor(), LifecycleActor::User { .. }),
            "{root}: {event:?}"
        );
    }
}
