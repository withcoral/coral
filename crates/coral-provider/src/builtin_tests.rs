//! The Built-in Provider: one per Workspace at `coral.admin`, discoverable and
//! callable only by Users with effective Workspace Owner authority.
use coral_workspace::{
    AgentDisplayName, AuthenticatedPrincipal, OrganizationRole, WorkspaceDisplayName, WorkspaceRole,
};

use coral_catalog::{AuthenticationKind, BuiltinCatalog, OperationHints};
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
    with_list_things(OperationHints::new(Some(true), None, None, None))
}

/// This Coral version's Operations plus `listThings` with `hints`.
fn with_list_things(hints: OperationHints) -> BuiltinCatalog {
    BuiltinCatalog::with_unknown_operations([("listThings", hints)]).unwrap()
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

    /// `actor` reviews `served` and updates the Built-in Provider to it
    /// through a process that serves it, in its own transaction, with the
    /// Provider's current etag.
    async fn update(
        &self,
        served: &BuiltinCatalog,
        actor: &coral_workspace::AuthenticatedUser,
        at: i64,
    ) -> Result<Option<crate::ActivatedGeneration>, Error> {
        let etag = self.current().await.etag();
        self.update_with(
            served,
            actor,
            at,
            &etag,
            self.builtin.id(),
            served.identity(),
        )
        .await
    }

    /// `actor` updates `provider` with `etag` through a process that serves
    /// `served`, naming the `reviewed` catalog.
    async fn update_with(
        &self,
        served: &BuiltinCatalog,
        actor: &coral_workspace::AuthenticatedUser,
        at: i64,
        etag: &str,
        provider: &crate::ProviderId,
        reviewed: &str,
    ) -> Result<Option<crate::ActivatedGeneration>, Error> {
        let context = change_context(actor.clone(), at, '2')
            .with_etag(etag)
            .unwrap();
        let database = self.fixture.database();
        let mut transaction = database.begin().await.unwrap();
        let updated = serving(&self.providers, served.clone())
            .update_builtin_catalog_in_transaction(
                &mut transaction,
                &context,
                self.workspace.id(),
                provider,
                reviewed,
            )
            .await;
        if updated.is_ok() {
            transaction.commit().await.unwrap();
        } else {
            transaction.rollback().await.unwrap();
        }
        updated
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

    async fn member(&self, name: &str) -> coral_workspace::AuthenticatedUser {
        let member =
            organization_user(self.fixture.database(), name, OrganizationRole::Member).await;
        self.workspaces
            .set_membership(
                &change_context(self.owner.clone(), 11, '1'),
                self.workspace.id(),
                &member,
                WorkspaceRole::Member,
            )
            .await
            .unwrap();
        member
    }
}

/// The keys in each part of an offered update: added, removed, changed.
fn offered(changes: &coral_catalog::BuiltinCatalogChanges) -> [Vec<&str>; 3] {
    fn keys(operations: &[coral_catalog::OperationSummary]) -> Vec<&str> {
        operations
            .iter()
            .map(|operation| operation.key.as_str())
            .collect()
    }
    [
        keys(&changes.added),
        keys(&changes.removed),
        keys(&changes.changed),
    ]
}

/// An Owner sees an update only when the served catalog differs, and exactly
/// which Operations it adds, removes or changes. A Member sees none.
#[tokio::test]
async fn an_owner_sees_how_the_served_catalog_differs() {
    let arranged = BuiltinWorkspace::new().await;
    let workspace = arranged.workspace.id();
    let builtin = arranged.builtin.id();
    let inspect = |served: BuiltinCatalog| async {
        serving(&arranged.providers, served)
            .inspect_builtin_catalog_update(&arranged.owner, workspace, builtin)
            .await
            .unwrap()
    };

    assert_eq!(inspect(running()).await, None, "the same catalog");
    let to_b = inspect(catalog_b()).await.expect("B adds listThings");
    assert_eq!(offered(&to_b), [vec!["listThings"], vec![], vec![]]);
    assert_eq!(to_b.identity, catalog_b().identity(), "an update names B");

    arranged
        .update(&catalog_b(), &arranged.owner, 20)
        .await
        .unwrap();
    let to_a = inspect(running()).await.expect("A removes listThings");
    assert_eq!(offered(&to_a), [vec![], vec!["listThings"], vec![]]);
    let to_c = inspect(with_list_things(OperationHints::default()))
        .await
        .expect("C changes listThings");
    assert_eq!(offered(&to_c), [vec![], vec![], vec!["listThings"]]);

    let member = arranged.member("user:member").await;
    let denied = serving(&arranged.providers, running())
        .inspect_builtin_catalog_update(&member, workspace, builtin)
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
}

/// An Owner's update moves the Built-in Provider to the served catalog once,
/// in its current state, and records one `provider.updated` event with the
/// Owner as actor. An update to the catalog it already has changes nothing.
#[tokio::test]
async fn an_owner_updates_the_builtin_provider_to_the_served_catalog_once() {
    for state in [ProviderState::Active, ProviderState::Disabled] {
        let arranged = BuiltinWorkspace::new().await;
        if state == ProviderState::Disabled {
            arranged
                .providers
                .disable_provider(
                    &change_context(arranged.owner.clone(), 15, '2')
                        .with_etag(&arranged.builtin.etag())
                        .unwrap(),
                    arranged.workspace.id(),
                    arranged.builtin.id(),
                )
                .await
                .unwrap();
        }
        let before = arranged.events().await.len();
        let mut keys = arranged.keys().await;
        let served = catalog_b();

        let activated = arranged
            .update(&served, &arranged.owner, 20)
            .await
            .unwrap()
            .expect("the update activates catalog B");

        assert_eq!(activated.provider(), arranged.builtin.id(), "{state:?}");
        keys.push("listThings".to_string());
        keys.sort();
        assert_eq!(arranged.keys().await, keys);
        assert_eq!(arranged.current().await.state(), state);
        let events = arranged.events().await;
        let Some([event]) = events.get(before..) else {
            panic!("{state:?}: the update records one event: {events:?}");
        };
        assert_eq!(event.action(), LifecycleAction::ProviderUpdated);
        assert!(
            matches!(event.actor(), LifecycleActor::User { user_id, .. } if user_id == arranged.owner.user_id()),
            "{event:?}"
        );
        assert_eq!(event.target_id(), arranged.builtin.id().to_string());
        assert_eq!(
            event.changed_field_paths(),
            ["provider.active_generation", "catalog"]
        );

        assert_eq!(
            arranged.update(&served, &arranged.owner, 30).await.unwrap(),
            None,
            "the Workspace already has catalog B"
        );
        assert_eq!(arranged.events().await.len(), before + 1, "{state:?}");
    }
}

#[derive(Debug, Clone, Copy)]
enum Refusal {
    Member,
    StaleEtag,
    Archived,
    OwnerProvider,
    OtherCatalog,
}

/// An update by a Member, with a stale etag, in an archived Workspace, of an
/// Owner-added Provider, or that names a catalog other than the one the
/// serving Coral defines is refused and changes nothing.
#[tokio::test]
async fn a_refused_update_changes_nothing() {
    for refusal in [
        Refusal::Member,
        Refusal::StaleEtag,
        Refusal::Archived,
        Refusal::OwnerProvider,
        Refusal::OtherCatalog,
    ] {
        let arranged = BuiltinWorkspace::new().await;
        let served = catalog_b();
        let owner = arranged.owner.clone();
        let mut provider = arranged.builtin.id().clone();
        let mut etag = arranged.current().await.etag();
        let mut actor = owner.clone();
        let mut reviewed = served.identity().to_string();
        match refusal {
            Refusal::Member => actor = arranged.member("user:member").await,
            Refusal::StaleEtag => etag = uuid::Uuid::new_v4().to_string(),
            Refusal::Archived => {
                arranged
                    .workspaces
                    .archive(
                        &change_context(owner.clone(), 15, '2'),
                        arranged.workspace.id(),
                    )
                    .await
                    .unwrap();
            }
            Refusal::OwnerProvider => {
                let added = publish_provider(
                    &arranged.providers,
                    &change_context(owner.clone(), 15, '2'),
                    arranged.workspace.id(),
                    CREDENTIAL_FREE_OPENAPI,
                    "weather",
                    CredentialRoute::None,
                )
                .await;
                etag = added.etag();
                provider = added.id().clone();
            }
            // The Owner reviewed the update another Coral version offered.
            Refusal::OtherCatalog => reviewed = running().identity().to_string(),
        }
        let before = arranged.events().await.len();
        let keys = arranged.keys().await;

        let refused = arranged
            .update_with(&served, &actor, 20, &etag, &provider, &reviewed)
            .await;

        let expected = match refusal {
            Refusal::Member => matches!(
                refused,
                Err(Error::Workspace(coral_workspace::Error::PermissionDenied(
                    _
                )))
            ),
            Refusal::StaleEtag => matches!(refused, Err(Error::EtagMismatch)),
            Refusal::Archived => matches!(
                refused,
                Err(Error::Workspace(coral_workspace::Error::WorkspaceArchived))
            ),
            Refusal::OwnerProvider => matches!(refused, Err(Error::NotBuiltinProvider)),
            Refusal::OtherCatalog => matches!(refused, Err(Error::BuiltinCatalogMismatch)),
        };
        assert!(expected, "{refusal:?}: {refused:?}");
        assert_eq!(arranged.keys().await, keys, "{refusal:?}");
        assert_eq!(arranged.events().await.len(), before, "{refusal:?}");
    }
}

/// Two Owners who update with the same etag at once activate one Generation;
/// the other update fails on its stale etag.
#[tokio::test]
async fn two_concurrent_updates_activate_one_generation() {
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
    let etag = arranged.current().await.etag();
    let reviewed = catalog_b().identity().to_string();
    let update = |served: &ProviderModule, database: std::sync::Arc<coral_db::Database>, at| {
        let context = change_context(arranged.owner.clone(), at, '2')
            .with_etag(&etag)
            .unwrap();
        let served = served.clone();
        let reviewed = reviewed.clone();
        let workspace = arranged.workspace.id().clone();
        let provider = arranged.builtin.id().clone();
        async move {
            let mut transaction = database.begin().await.unwrap();
            let updated = served
                .update_builtin_catalog_in_transaction(
                    &mut transaction,
                    &context,
                    &workspace,
                    &provider,
                    &reviewed,
                )
                .await;
            if updated.is_ok() {
                transaction.commit().await.unwrap();
            }
            updated
        }
    };

    let (left, right) = tokio::join!(
        update(&first, arranged.fixture.database(), 20),
        update(&second, peer, 21),
    );

    let outcomes = [left, right];
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, Ok(Some(_))))
            .count(),
        1,
        "{outcomes:?}"
    );
    assert!(
        outcomes
            .iter()
            .any(|outcome| matches!(outcome, Err(Error::EtagMismatch))),
        "{outcomes:?}"
    );
    assert_eq!(arranged.events().await.len(), before + 1);
}

// Catches: a disabled Provider left out of the list though it still holds its
// namespace, or a list whose order or account facts a Program cannot rely on.
#[tokio::test]
async fn published_providers_lists_every_provider_in_creation_order() {
    let arranged = BuiltinWorkspace::new().await;
    let workspace = arranged.workspace.id();
    let weather = publish_provider(
        &arranged.providers,
        &change_context(arranged.owner.clone(), 15, '2'),
        workspace,
        CREDENTIAL_FREE_OPENAPI,
        "weather",
        CredentialRoute::None,
    )
    .await;
    publish_provider(
        &arranged.providers,
        &change_context(arranged.owner.clone(), 16, '3'),
        workspace,
        crate::account_tests::CREDENTIAL_REQUIRED_OPENAPI,
        "private",
        CredentialRoute::Caller,
    )
    .await;
    arranged
        .providers
        .disable_provider(
            &change_context(arranged.owner.clone(), 17, '4')
                .with_etag(&weather.etag())
                .unwrap(),
            workspace,
            weather.id(),
        )
        .await
        .unwrap();

    let listed = arranged
        .providers
        .published_providers(&arranged.owner, workspace)
        .await
        .unwrap();

    assert_eq!(
        listed
            .iter()
            .map(|published| (
                published.provider.call_root().as_str(),
                published.provider.state(),
                published.authentication,
                published.available_operations,
                published.account_required,
            ))
            .collect::<Vec<_>>(),
        [
            (
                "coral.admin",
                ProviderState::Active,
                AuthenticationKind::None,
                arranged.keys().await.len(),
                false,
            ),
            (
                "coral.providers.weather",
                ProviderState::Disabled,
                AuthenticationKind::None,
                1,
                false,
            ),
            // The Owner has not connected an Account yet.
            (
                "coral.providers.private",
                ProviderState::Active,
                AuthenticationKind::Bearer,
                1,
                true,
            ),
        ]
    );
}
