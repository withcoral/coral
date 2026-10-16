//! The Built-in Provider: one per Workspace at `coral.workspace`, discoverable and
//! callable only by Users with effective Workspace Owner authority.
use coral_workspace::{
    AgentDisplayName, AuthenticatedPrincipal, OrganizationRole, WorkspaceDisplayName, WorkspaceRole,
};

use coral_credentials::Secret;

use super::tests::{
    CREDENTIAL_FREE_OPENAPI, Harness, change_context, organization_user, publish_provider,
    test_modules,
};
use crate::access::CredentialRoute;
use crate::{
    Error, LifecycleAction, ManagementRequestId, OpenApiDocument, ProviderCallRoot,
    ProviderDisplayName, ProviderRegenerationRequest, ProviderSetupSource, ProviderState,
};

#[tokio::test]
async fn only_users_with_effective_owner_authority_discover_and_call_the_builtin_provider() {
    let (fixture, workspaces, providers) = test_modules().await;
    let database = fixture.database();
    let owner = organization_user(database.clone(), "user:owner", OrganizationRole::Member).await;
    let context = change_context(owner.clone(), 10, '1');
    let workspace = workspaces
        .create(&context, WorkspaceDisplayName::parse("Builtin").unwrap())
        .await
        .unwrap();
    let mut transaction = database.begin().await.unwrap();
    providers
        .install_builtin_in_transaction(
            &mut transaction,
            &context,
            workspace.id(),
            crate::prepare_builtin_catalog().unwrap(),
        )
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    let member = organization_user(database.clone(), "user:member", OrganizationRole::Member).await;
    workspaces
        .set_membership(&context, workspace.id(), &member, WorkspaceRole::Member)
        .await
        .unwrap();
    let issued = workspaces
        .create_agent(
            &context,
            workspace.id(),
            AgentDisplayName::parse("Worker").unwrap(),
            None,
        )
        .await
        .unwrap();
    let agent = workspaces
        .authenticate_agent(workspace.id(), issued.credential.expose(), 11)
        .await
        .unwrap();

    for (principal, reaches) in [
        (AuthenticatedPrincipal::user(owner), true),
        (AuthenticatedPrincipal::user(member), false),
        (agent, false),
    ] {
        let mut transaction = database.begin_read_snapshot().await.unwrap();
        let authority = workspaces
            .check_discovery_in_transaction(&mut transaction, &principal, workspace.id(), 12)
            .await
            .unwrap();
        let [builtin] = crate::active_providers_in_transaction(&mut transaction, workspace.id())
            .await
            .unwrap()
            .try_into()
            .expect("the Workspace has only its Built-in Provider");
        assert_eq!(builtin.provider().call_root(), &ProviderCallRoot::Builtin);
        assert_eq!(
            crate::account::discoverable_in_transaction(
                &mut transaction,
                &builtin,
                principal.principal(),
                &authority,
            )
            .await
            .unwrap(),
            reaches,
            "discover as {principal:?}"
        );
        transaction.rollback().await.unwrap();

        let mut transaction = database.begin().await.unwrap();
        let resolved = crate::resolve_invocation_operation_in_transaction(
            &mut transaction,
            workspace.id(),
            crate::InvocationCaller::new(&workspaces, &principal, 12),
            "coral.workspace.createProvider",
        )
        .await
        .unwrap();
        assert_eq!(resolved.is_some(), reaches, "call as {principal:?}");
        transaction.rollback().await.unwrap();
    }
}

#[tokio::test]
async fn a_workspace_has_one_builtin_provider_beside_an_owner_provider_named_workspace() {
    let (fixture, workspaces, providers) = test_modules().await;
    let database = fixture.database();
    let owner = organization_user(database.clone(), "user:owner", OrganizationRole::Member).await;
    let context = change_context(owner, 10, '1');
    let workspace = workspaces
        .create(&context, WorkspaceDisplayName::parse("Builtin").unwrap())
        .await
        .unwrap();
    let mut transaction = database.begin().await.unwrap();
    let builtin = providers
        .install_builtin_in_transaction(
            &mut transaction,
            &context,
            workspace.id(),
            crate::prepare_builtin_catalog().unwrap(),
        )
        .await
        .unwrap();
    transaction.commit().await.unwrap();

    let namesake = publish_provider(
        &providers,
        &context,
        workspace.id(),
        CREDENTIAL_FREE_OPENAPI,
        "workspace",
        CredentialRoute::None,
    )
    .await;
    assert_eq!(builtin.call_root().as_str(), "coral.workspace");
    assert_eq!(namesake.call_root().as_str(), "coral.providers.workspace");

    let mut transaction = database.begin().await.unwrap();
    assert!(matches!(
        providers
            .install_builtin_in_transaction(
                &mut transaction,
                &context,
                workspace.id(),
                crate::prepare_builtin_catalog().unwrap(),
            )
            .await,
        Err(Error::ProviderCallRootConflict)
    ));
    transaction.rollback().await.unwrap();
    let mut transaction = database.begin_read_snapshot().await.unwrap();
    let mut roots = crate::active_providers_in_transaction(&mut transaction, workspace.id())
        .await
        .unwrap()
        .iter()
        .map(|provider| provider.provider().call_root().as_str().to_string())
        .collect::<Vec<_>>();
    roots.sort_unstable();
    assert_eq!(roots, ["coral.providers.workspace", "coral.workspace"]);
    transaction.rollback().await.unwrap();
}

#[tokio::test]
async fn a_member_cannot_install_the_builtin_provider() {
    let (fixture, workspaces, providers) = test_modules().await;
    let database = fixture.database();
    let owner = organization_user(database.clone(), "user:owner", OrganizationRole::Member).await;
    let context = change_context(owner, 10, '1');
    let workspace = workspaces
        .create(&context, WorkspaceDisplayName::parse("Builtin").unwrap())
        .await
        .unwrap();
    let member = organization_user(database.clone(), "user:member", OrganizationRole::Member).await;
    workspaces
        .set_membership(&context, workspace.id(), &member, WorkspaceRole::Member)
        .await
        .unwrap();

    let mut transaction = database.begin().await.unwrap();
    let denied = providers
        .install_builtin_in_transaction(
            &mut transaction,
            &change_context(member, 11, '2'),
            workspace.id(),
            crate::prepare_builtin_catalog().unwrap(),
        )
        .await;
    assert!(
        matches!(
            denied,
            Err(Error::Workspace(coral_workspace::Error::PermissionDenied(
                _
            )))
        ),
        "{denied:?}"
    );
    assert!(
        crate::active_providers_in_transaction(&mut transaction, workspace.id())
            .await
            .unwrap()
            .is_empty()
    );
    transaction.rollback().await.unwrap();
}

#[derive(Debug, Clone, Copy)]
enum BuiltinChange {
    Rename,
    Reconfigure,
    Regenerate,
    ConnectAccount,
    RotateOAuthClientSecret,
    Delete,
}

/// An Owner can only disable or restore the Built-in Provider and set its
/// Operations' policy. Every other change is rejected and changes nothing.
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one table covers every change the Built-in Provider refuses and the two it allows"
)]
async fn an_owner_can_only_disable_and_restore_the_builtin_provider() {
    let harness = Harness::new().await;
    let (database, providers) = (harness.database.clone(), &harness.providers);
    let owner = organization_user(database.clone(), "user:owner", OrganizationRole::Member).await;
    let context = change_context(owner.clone(), 10, '1');
    let workspace = harness
        .workspaces
        .create(&context, WorkspaceDisplayName::parse("Builtin").unwrap())
        .await
        .unwrap();
    let mut transaction = database.begin().await.unwrap();
    let builtin = providers
        .install_builtin_in_transaction(
            &mut transaction,
            &context,
            workspace.id(),
            crate::prepare_builtin_catalog().unwrap(),
        )
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    let document = || {
        ProviderSetupSource::OpenApiDocument(
            OpenApiDocument::parse(CREDENTIAL_FREE_OPENAPI).expect("OpenAPI input"),
        )
    };
    // A Setup for a new Provider, so regeneration reaches the Built-in check.
    let unrelated_setup = providers
        .prepare_setup(&context, workspace.id(), document(), None)
        .await
        .unwrap();
    let events = || async {
        providers
            .list_lifecycle_events(&owner, workspace.id())
            .await
            .unwrap()
    };

    for change in [
        BuiltinChange::Rename,
        BuiltinChange::Reconfigure,
        BuiltinChange::Regenerate,
        BuiltinChange::ConnectAccount,
        BuiltinChange::RotateOAuthClientSecret,
        BuiltinChange::Delete,
    ] {
        let before = events().await.len();
        let context = context.clone().with_etag(&builtin.etag()).unwrap();
        let result = match change {
            BuiltinChange::Rename => providers
                .update_provider(
                    &context,
                    workspace.id(),
                    builtin.id(),
                    ProviderDisplayName::parse("Renamed").unwrap(),
                )
                .await
                .map(drop),
            BuiltinChange::Reconfigure => providers
                .prepare_setup(&context, workspace.id(), document(), Some(builtin.id()))
                .await
                .map(drop),
            BuiltinChange::Regenerate => providers
                .prepare_regeneration(
                    &context,
                    ProviderRegenerationRequest::new(
                        ManagementRequestId::parse("15000000-0000-4000-8000-000000000611").unwrap(),
                        workspace.id().clone(),
                        builtin.id().clone(),
                        unrelated_setup.id().clone(),
                        1,
                    ),
                )
                .await
                .map(drop),
            BuiltinChange::ConnectAccount => harness
                .accounts
                .start_secret(
                    &context,
                    workspace.id(),
                    builtin.id(),
                    ManagementRequestId::parse("15000000-0000-4000-8000-000000000612").unwrap(),
                )
                .await
                .map(drop),
            BuiltinChange::RotateOAuthClientSecret => {
                harness
                    .accounts
                    .rotate_oauth_client_secret(
                        &context,
                        workspace.id(),
                        builtin.id(),
                        "client",
                        Secret::new("rotated-secret".into()).unwrap(),
                    )
                    .await
            }
            BuiltinChange::Delete => {
                providers
                    .delete_provider(&context, workspace.id(), builtin.id())
                    .await
            }
        };
        assert!(
            matches!(result, Err(Error::BuiltinProviderManaged)),
            "{change:?}: {result:?}"
        );
        assert_eq!(events().await.len(), before, "{change:?} records no event");
        let mut transaction = database.begin_read_snapshot().await.unwrap();
        let [current] = crate::active_providers_in_transaction(&mut transaction, workspace.id())
            .await
            .unwrap()
            .try_into()
            .expect("the Built-in Provider remains the Workspace's only Provider");
        assert_eq!(current.provider(), &builtin, "{change:?}");
        transaction.rollback().await.unwrap();
    }

    let before = events().await.len();
    let disabled = providers
        .disable_provider(
            &change_context(owner.clone(), 20, '2')
                .with_etag(&builtin.etag())
                .unwrap(),
            workspace.id(),
            builtin.id(),
        )
        .await
        .unwrap();
    assert_eq!(disabled.state(), ProviderState::Disabled);
    let restored = providers
        .restore_provider(
            &change_context(owner.clone(), 30, '3')
                .with_etag(&disabled.etag())
                .unwrap(),
            workspace.id(),
            builtin.id(),
        )
        .await
        .unwrap();
    assert_eq!(restored.state(), ProviderState::Active);
    let actions = events()
        .await
        .iter()
        .skip(before)
        .map(crate::AdministrativeLifecycleEvent::action)
        .collect::<Vec<_>>();
    assert_eq!(
        actions,
        [
            LifecycleAction::ProviderDisabled,
            LifecycleAction::ProviderRestored
        ]
    );
}
