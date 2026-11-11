//! The Private Provider at `coral.private` and the Shared Provider at
//! `coral.shared`: one of each per Workspace, which Coral performs, with no
//! Accounts. Their Tools are the Private and Shared Tools that coral-tools
//! keeps.
use coral_catalog::AuthoredToolKind;
use coral_workspace::{OrganizationRole, WorkspaceDisplayName};

use super::tests::{change_context, organization_user, test_modules};
use crate::access::CredentialRoute;
use crate::{
    CoralDefinedProvider, Error, LifecycleAction, LifecycleActor, ProviderState, SearchChange,
};

// Catches: a Workspace whose Private or Shared Provider is not at its Call
// Root, a second one, or one that needs Accounts. The boot's install of a
// missing one is tested with the Built-in Provider's (`coral_defined_tests.rs`).
#[tokio::test]
async fn each_workspace_has_one_private_and_one_shared_provider_installed_at_creation() {
    // (the Provider, its kind of Tool, its Call Root's text, the display
    // name, the lifecycle action)
    let cases = [
        (
            CoralDefinedProvider::Private,
            AuthoredToolKind::Private,
            "coral.private",
            "Private",
            LifecycleAction::PrivateProviderInstalled,
        ),
        (
            CoralDefinedProvider::Shared,
            AuthoredToolKind::Shared,
            "coral.shared",
            "Shared",
            LifecycleAction::SharedProviderInstalled,
        ),
    ];
    for (provider, kind, root, display_name, action) in cases {
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
            providers
                .install_coral_defined_in_transaction(transaction, &context, created.id(), provider)
                .await
        };

        let mut transaction = database.begin().await.unwrap();
        let (installed, search) = install(&mut transaction).await.unwrap().into_parts();
        transaction.commit().await.unwrap();
        // Its Generation declares no Tool, so search has nothing to index.
        assert_eq!(search, SearchChange::Unchanged, "{root}");
        assert_eq!(installed.call_root(), &provider.call_root());
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
        let found =
            crate::authored_tool_provider_in_transaction(&mut transaction, created.id(), kind)
                .await
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

// Catches: Provider Tool resolution answering for a Call Path under
// `coral.private` or `coral.shared`. Their Tools are rows coral-tools keeps,
// so a Run reaches one only through `resolve_authored_tool_in_transaction`,
// with the definition it linked.
#[tokio::test]
async fn no_provider_tool_resolves_under_the_private_or_shared_call_root() {
    let (fixture, workspaces, providers) = test_modules().await;
    let database = fixture.database();
    let owner = organization_user(database.clone(), "user:owner", OrganizationRole::Member).await;
    let context = change_context(owner.clone(), 10, '1');
    let workspace = workspaces
        .create(&context, WorkspaceDisplayName::parse("Created").unwrap())
        .await
        .unwrap();
    let mut transaction = database.begin().await.unwrap();
    for installed in providers
        .install_every_coral_defined_in_transaction(&mut transaction, &context, workspace.id())
        .await
        .unwrap()
    {
        let _provider = installed.skip_search();
    }
    transaction.commit().await.unwrap();
    let principal = coral_workspace::AuthenticatedPrincipal::user(owner);

    for call_path in [
        "coral.private.lookup",
        "coral.private.crm.lookup",
        "coral.shared.lookup",
        "coral.shared.crm.lookup",
    ] {
        let mut transaction = database.begin().await.unwrap();
        let caller = || crate::InvocationCaller::new(&workspaces, &principal, 11);
        assert!(
            crate::resolve_invocation_tool_in_transaction(
                &mut transaction,
                workspace.id(),
                caller(),
                call_path,
            )
            .await
            .unwrap()
            .is_none(),
            "{call_path}"
        );
        assert!(
            !crate::resolves_invocation_tool_in_snapshot(
                &mut transaction,
                workspace.id(),
                caller(),
                call_path,
            )
            .await
            .unwrap(),
            "{call_path}"
        );
        assert!(
            crate::resolve_owner_tool_in_transaction(&mut transaction, workspace.id(), call_path)
                .await
                .unwrap()
                .is_none(),
            "{call_path}"
        );
        transaction.rollback().await.unwrap();
    }
}
