//! The Built-in Provider: one per Workspace at `coral.admin`, discoverable and
//! callable only by Users with effective Workspace Owner authority.
use coral_workspace::{
    AgentDisplayName, AuthenticatedPrincipal, OrganizationRole, WorkspaceDisplayName, WorkspaceRole,
};

use coral_catalog::{BuiltinCatalog, OperationHints};
use coral_credentials::Secret;

use super::tests::{
    CREDENTIAL_FREE_OPENAPI, Harness, change_context, organization_user, publish_provider,
    test_modules,
};
use crate::access::CredentialRoute;
use crate::{
    Error, LifecycleAction, LifecycleActor, ManagementRequestId, OpenApiDocument, ProviderCallRoot,
    ProviderDisplayName, ProviderModule, ProviderRegenerationRequest, ProviderSetupInputKind,
    ProviderSetupSource, ProviderState,
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
        .install_builtin_in_transaction(&mut transaction, &context, workspace.id())
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
            "coral.admin.createProvider",
        )
        .await
        .unwrap();
        assert_eq!(resolved.is_some(), reaches, "call as {principal:?}");
        transaction.rollback().await.unwrap();
    }
}

#[tokio::test]
async fn a_workspace_has_one_builtin_provider_beside_an_owner_provider_named_admin() {
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
        .install_builtin_in_transaction(&mut transaction, &context, workspace.id())
        .await
        .unwrap();
    transaction.commit().await.unwrap();

    let admin = publish_provider(
        &providers,
        &context,
        workspace.id(),
        CREDENTIAL_FREE_OPENAPI,
        "admin",
        CredentialRoute::None,
    )
    .await;
    assert_eq!(builtin.call_root().as_str(), "coral.admin");
    assert_eq!(admin.call_root().as_str(), "coral.providers.admin");

    let mut transaction = database.begin().await.unwrap();
    assert!(matches!(
        providers
            .install_builtin_in_transaction(&mut transaction, &context, workspace.id(),)
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
    assert_eq!(roots, ["coral.admin", "coral.providers.admin"]);
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
        .install_builtin_in_transaction(&mut transaction, &context, workspace.id())
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

/// Coral defines the Built-in Provider's catalog in Rust, so it has no source
/// for an Owner to read, refresh or replace. A Provider an Owner adds keeps its
/// source beside it.
#[tokio::test]
async fn only_a_provider_an_owner_adds_has_a_source_to_inspect() {
    let (fixture, workspaces, providers) = test_modules().await;
    let database = fixture.database();
    let owner = organization_user(database.clone(), "user:owner", OrganizationRole::Member).await;
    let context = change_context(owner.clone(), 10, '1');
    let workspace = workspaces
        .create(&context, WorkspaceDisplayName::parse("Builtin").unwrap())
        .await
        .unwrap();
    let mut transaction = database.begin().await.unwrap();
    let builtin = providers
        .install_builtin_in_transaction(&mut transaction, &context, workspace.id())
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    let added = publish_provider(
        &providers,
        &context,
        workspace.id(),
        CREDENTIAL_FREE_OPENAPI,
        "added",
        CredentialRoute::None,
    )
    .await;

    let source = providers
        .inspect_provider_source(&owner, workspace.id(), builtin.id())
        .await;
    assert!(
        matches!(source, Err(Error::BuiltinProviderManaged)),
        "{source:?}"
    );
    let source = providers
        .inspect_provider_source(&owner, workspace.id(), added.id())
        .await
        .unwrap();
    assert_eq!(
        (source.url, source.kind),
        (None, ProviderSetupInputKind::OpenApi),
        "an uploaded OpenAPI document has no URL"
    );
}

/// Catalog B: this Coral version's Operations plus a read-only one it does
/// not perform, as a later Coral version could define.
fn catalog_b() -> BuiltinCatalog {
    BuiltinCatalog::with_unknown_operations([(
        "listThings",
        OperationHints::new(Some(true), None, None, None),
    )])
    .unwrap()
}

fn running() -> BuiltinCatalog {
    BuiltinCatalog::running().unwrap()
}

/// A Provider Module over the same database that serves `catalog`, as a
/// process of another Coral version would.
fn serving(providers: &ProviderModule, catalog: BuiltinCatalog) -> ProviderModule {
    ProviderModule::new(
        coral_outbound::DestinationPolicy::default(),
        providers.database.clone(),
        providers.workspaces.clone(),
        catalog,
    )
}

/// A Workspace created at time 10 by `owner` with this Coral version's catalog.
struct BuiltinWorkspace {
    fixture: coral_test_support::TestDatabase,
    workspaces: coral_workspace::WorkspaceModule,
    providers: ProviderModule,
    owner: coral_workspace::AuthenticatedUser,
    workspace: coral_workspace::Workspace,
    builtin: crate::Provider,
}

impl BuiltinWorkspace {
    async fn new() -> Self {
        let (fixture, workspaces, providers) = test_modules().await;
        let database = fixture.database();
        let owner =
            organization_user(database.clone(), "user:owner", OrganizationRole::Member).await;
        let context = change_context(owner.clone(), 10, '1');
        let workspace = workspaces
            .create(&context, WorkspaceDisplayName::parse("Builtin").unwrap())
            .await
            .unwrap();
        let mut transaction = database.begin().await.unwrap();
        let builtin = providers
            .install_builtin_in_transaction(&mut transaction, &context, workspace.id())
            .await
            .unwrap();
        transaction.commit().await.unwrap();
        Self {
            fixture,
            workspaces,
            providers,
            owner,
            workspace,
            builtin,
        }
    }

    async fn current(&self) -> crate::Provider {
        self.providers
            .inspect_provider(&self.owner, self.workspace.id(), self.builtin.id())
            .await
            .unwrap()
    }

    /// A process that serves `served` boots at `at`: it updates each outdated
    /// Built-in Provider in its own transaction.
    async fn boot(&self, served: &BuiltinCatalog, at: i64) -> Vec<crate::ActivatedGeneration> {
        let process = serving(&self.providers, served.clone());
        let database = self.fixture.database();
        let mut activated = Vec::new();
        for (workspace, provider) in process.outdated_builtin_providers().await.unwrap() {
            let mut transaction = database.begin().await.unwrap();
            activated.extend(
                process
                    .update_builtin_catalog_in_transaction(
                        &mut transaction,
                        &workspace,
                        &provider,
                        at,
                    )
                    .await
                    .unwrap(),
            );
            transaction.commit().await.unwrap();
        }
        activated
    }

    /// The Operation Keys the Built-in Provider serves.
    async fn keys(&self) -> Vec<String> {
        self.providers
            .inspect_catalog_page(
                &self.owner,
                self.workspace.id(),
                self.builtin.id(),
                None,
                100,
            )
            .await
            .unwrap()
            .into_iter()
            .map(|operation| operation.key.as_str().to_string())
            .collect()
    }

    async fn events(&self) -> Vec<crate::AdministrativeLifecycleEvent> {
        self.providers
            .list_lifecycle_events(&self.owner, self.workspace.id())
            .await
            .unwrap()
    }
}

#[derive(Debug, Clone, Copy)]
enum Arranged {
    Active,
    Disabled,
    Archived,
}

/// Each boot moves the Built-in Provider to the booting Coral's catalog once,
/// forward or back, in any Provider or Workspace state, and records one
/// `provider.updated` event with Coral as actor. A boot with the catalog the
/// Workspace already pins changes nothing, and an Owner-added Provider never
/// updates.
#[tokio::test]
async fn each_boot_moves_the_builtin_provider_to_the_booting_catalog_once() {
    for arranged_as in [Arranged::Active, Arranged::Disabled, Arranged::Archived] {
        let arranged = BuiltinWorkspace::new().await;
        let context = change_context(arranged.owner.clone(), 15, '2');
        publish_provider(
            &arranged.providers,
            &context,
            arranged.workspace.id(),
            CREDENTIAL_FREE_OPENAPI,
            "weather",
            CredentialRoute::None,
        )
        .await;
        match arranged_as {
            Arranged::Active => {}
            Arranged::Disabled => {
                arranged
                    .providers
                    .disable_provider(
                        &context.clone().with_etag(&arranged.builtin.etag()).unwrap(),
                        arranged.workspace.id(),
                        arranged.builtin.id(),
                    )
                    .await
                    .unwrap();
            }
            Arranged::Archived => {
                arranged
                    .workspaces
                    .archive(&context, arranged.workspace.id())
                    .await
                    .unwrap();
            }
        }
        let state = arranged.current().await.state();
        let before = arranged.events().await.len();
        assert!(
            arranged.boot(&running(), 20).await.is_empty(),
            "{arranged_as:?}: the Workspace already pins this catalog"
        );

        // A newer Coral boots, then a rollback boots this one again.
        for (served, at, keys) in [
            (catalog_b(), 30, vec!["createProvider", "listThings"]),
            (running(), 40, vec!["createProvider"]),
        ] {
            let activated = arranged.boot(&served, at).await;
            assert_eq!(
                activated
                    .iter()
                    .map(crate::ActivatedGeneration::provider)
                    .collect::<Vec<_>>(),
                [arranged.builtin.id()],
                "{arranged_as:?}"
            );
            assert_eq!(arranged.keys().await, keys, "{arranged_as:?}");
            assert!(
                arranged.boot(&served, at + 1).await.is_empty(),
                "{arranged_as:?}: the next boot finds the catalog pinned"
            );
        }

        assert_eq!(arranged.current().await.state(), state, "{arranged_as:?}");
        let events = arranged.events().await;
        let Some(updates @ [_, _]) = events.get(before..) else {
            panic!("{arranged_as:?}: each move records one event: {events:?}");
        };
        for event in updates {
            assert_eq!(event.action(), LifecycleAction::ProviderUpdated);
            assert!(matches!(event.actor(), LifecycleActor::Coral), "{event:?}");
            assert_eq!(event.target_id(), arranged.builtin.id().to_string());
            assert_eq!(
                event.changed_field_paths(),
                ["provider.active_generation", "catalog"]
            );
        }
    }
}

/// Two processes that boot together each try the update; one activates the
/// served catalog, and the other then finds it pinned.
#[tokio::test]
async fn two_processes_that_boot_together_activate_one_generation() {
    let arranged = BuiltinWorkspace::new().await;
    let before = arranged.events().await.len();
    let first = serving(&arranged.providers, catalog_b());
    let peer = std::sync::Arc::new(arranged.fixture.connect_peer().await.unwrap());
    let second = ProviderModule::new(
        coral_outbound::DestinationPolicy::default(),
        peer.clone(),
        coral_workspace::WorkspaceModule::new(peer.clone()),
        catalog_b(),
    );
    let update = |process: &ProviderModule, database: std::sync::Arc<coral_db::Database>, at| {
        let process = process.clone();
        let workspace = arranged.workspace.id().clone();
        let provider = arranged.builtin.id().clone();
        async move {
            let mut transaction = database.begin().await.unwrap();
            let updated = process
                .update_builtin_catalog_in_transaction(&mut transaction, &workspace, &provider, at)
                .await
                .unwrap();
            transaction.commit().await.unwrap();
            updated
        }
    };

    let (left, right) = tokio::join!(
        update(&first, arranged.fixture.database(), 20),
        update(&second, peer, 21),
    );

    assert_eq!(
        [&left, &right]
            .iter()
            .filter(|outcome| outcome.is_some())
            .count(),
        1,
        "{left:?} {right:?}"
    );
    assert_eq!(arranged.keys().await, ["createProvider", "listThings"]);
    assert_eq!(arranged.events().await.len(), before + 1);
}
