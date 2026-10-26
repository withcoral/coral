//! The Built-in Provider: one per Workspace at `coral.workspace`. Each of its
//! Operations is discoverable and callable by the Principals whose authority
//! permits the Workspace action it declares.
use coral_workspace::{
    AgentDisplayName, AuthenticatedPrincipal, OrganizationRole, WorkspaceAction,
    WorkspaceDisplayName, WorkspaceRole,
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

// Catches: discovery and Invocation disagreeing on who reaches a Built-in
// Operation, one Operation's rule applying to the whole Call Root, or an
// Operation another Coral version defines reaching anyone but an Owner.
#[tokio::test]
async fn each_builtin_operation_reaches_the_principals_its_workspace_action_permits() {
    let arranged = BuiltinWorkspace::new().await;
    let database = arranged.fixture.database();
    let workspace = arranged.workspace.id();
    arranged.boot(&catalog_b(), 11).await;
    let member = arranged.member("user:member").await;
    let agent = arranged.agent(12).await;

    let keys = ["addProvider", "listProviders", "listThings"];
    for (principal, reached) in [
        (
            AuthenticatedPrincipal::user(arranged.owner.clone()),
            &keys[..],
        ),
        (AuthenticatedPrincipal::user(member), &["listProviders"][..]),
        (agent, &["listProviders"][..]),
    ] {
        let mut transaction = database.begin_read_snapshot().await.unwrap();
        let authority = arranged
            .workspaces
            .check_discovery_in_transaction(&mut transaction, &principal, workspace, 13)
            .await
            .unwrap();
        let [builtin] = crate::active_providers_in_transaction(&mut transaction, workspace)
            .await
            .unwrap()
            .try_into()
            .expect("the Workspace has only its Built-in Provider");
        assert_eq!(builtin.provider().call_root(), &ProviderCallRoot::Builtin);
        assert!(
            crate::account::discoverable_in_transaction(
                &mut transaction,
                &builtin,
                principal.principal(),
                &authority,
            )
            .await
            .unwrap(),
            "discover as {principal:?}"
        );
        let unreached = crate::unreached_operations_in_transaction(
            &mut transaction,
            &builtin,
            authority.effective_user_role(),
        )
        .await
        .unwrap();
        transaction.rollback().await.unwrap();

        for key in keys {
            let reaches = reached.contains(&key);
            assert_eq!(
                unreached.iter().all(|unreached| unreached.as_str() != key),
                reaches,
                "discover {key} as {principal:?}"
            );
            let mut transaction = database.begin().await.unwrap();
            let resolved = crate::resolve_invocation_operation_in_transaction(
                &mut transaction,
                workspace,
                crate::InvocationCaller::new(&arranged.workspaces, &principal, 13),
                &format!("coral.workspace.{key}"),
            )
            .await
            .unwrap();
            assert_eq!(resolved.is_some(), reaches, "call {key} as {principal:?}");
            transaction.rollback().await.unwrap();
        }
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
        .install_builtin_in_transaction(&mut transaction, &context, workspace.id())
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
                .rename_provider(
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

/// `keys` plus catalog B's `listThings`, in Operation Key order.
fn with_list_things(keys: &[String]) -> Vec<String> {
    let mut keys = keys.to_vec();
    keys.push("listThings".to_string());
    keys.sort();
    keys
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

    /// A process that serves `served` boots at `at`: it replaces each outdated
    /// Generation in its own transaction. Returns the Providers it updated.
    async fn boot(&self, served: &BuiltinCatalog, at: i64) -> Vec<crate::ProviderId> {
        let process = serving(&self.providers, served.clone());
        let database = self.fixture.database();
        let mut updated = Vec::new();
        for outdated in process.outdated_builtin_generations().await.unwrap() {
            let mut transaction = database.begin().await.unwrap();
            if process
                .update_builtin_catalog_in_transaction(&mut transaction, &outdated, at)
                .await
                .unwrap()
            {
                updated.push(outdated.provider_id().clone());
            }
            transaction.commit().await.unwrap();
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

    /// An Agent the Owner registers at `at`, authenticated then.
    async fn agent(&self, at: i64) -> AuthenticatedPrincipal {
        let issued = self
            .workspaces
            .create_agent(
                &change_context(self.owner.clone(), at, '3'),
                self.workspace.id(),
                AgentDisplayName::parse("Worker").unwrap(),
                None,
            )
            .await
            .unwrap();
        self.workspaces
            .authenticate_agent(self.workspace.id(), issued.credential.expose(), at)
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
        let running_keys = arranged.keys().await;

        // A newer Coral boots, then a rollback boots this one again.
        for (served, at, keys) in [
            (catalog_b(), 30, with_list_things(&running_keys)),
            (running(), 40, running_keys.clone()),
        ] {
            assert_eq!(
                arranged.boot(&served, at).await,
                [arranged.builtin.id().clone()],
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

/// Two processes that boot together each try the update; one replaces the
/// Generation, and the other then finds another one active.
#[tokio::test]
async fn two_processes_that_boot_together_activate_one_generation() {
    let arranged = BuiltinWorkspace::new().await;
    let before = arranged.events().await.len();
    let keys = with_list_things(&arranged.keys().await);
    let first = serving(&arranged.providers, catalog_b());
    let peer = std::sync::Arc::new(arranged.fixture.connect_peer().await.unwrap());
    let second = ProviderModule::new(
        coral_outbound::DestinationPolicy::default(),
        peer.clone(),
        coral_workspace::WorkspaceModule::new(peer.clone()),
        catalog_b(),
    );
    let [outdated] = first
        .outdated_builtin_generations()
        .await
        .unwrap()
        .try_into()
        .expect("the Built-in Provider pins the running catalog");
    let update = |process: &ProviderModule, database: std::sync::Arc<coral_db::Database>, at| {
        let process = process.clone();
        let outdated = outdated.clone();
        async move {
            let mut transaction = database.begin().await.unwrap();
            let replaced = process
                .update_builtin_catalog_in_transaction(&mut transaction, &outdated, at)
                .await
                .unwrap();
            transaction.commit().await.unwrap();
            replaced
        }
    };

    let (left, right) = tokio::join!(
        update(&first, arranged.fixture.database(), 20),
        update(&second, peer, 21),
    );

    assert_eq!(
        [left, right].iter().filter(|replaced| **replaced).count(),
        1,
        "{left:?} {right:?}"
    );
    assert_eq!(arranged.keys().await, keys);
    assert_eq!(arranged.events().await.len(), before + 1);
}

// Catches: a disabled Provider left out of the list though it still holds its
// namespace, one that still offers Operations to call, or a list whose order
// or account facts a Program cannot rely on.
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

    let listed = crate::tests::published_providers(
        &arranged.providers,
        &AuthenticatedPrincipal::user(arranged.owner.clone()),
        workspace,
    )
    .await
    .unwrap();

    assert_eq!(
        listed
            .iter()
            .map(|published| (
                published.provider.call_root().as_str(),
                published.provider.state(),
                published.authentication,
                published.operations.len(),
                published.account_required,
            ))
            .collect::<Vec<_>>(),
        [
            (
                "coral.workspace",
                ProviderState::Active,
                AuthenticationKind::None,
                arranged.keys().await.len(),
                false,
            ),
            // A disabled Provider offers no Operation to call.
            (
                "coral.providers.weather",
                ProviderState::Disabled,
                AuthenticationKind::None,
                0,
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

// Catches: a management read that disagrees with discovery on what a
// Principal reaches: in the list, by its ID, or through its Operations; or
// listProviders refusing an Agent.
#[tokio::test]
async fn management_reads_show_each_principal_the_builtin_operations_it_reaches() {
    let arranged = BuiltinWorkspace::new().await;
    let workspace = arranged.workspace.id();
    publish_provider(
        &arranged.providers,
        &change_context(arranged.owner.clone(), 15, '2'),
        workspace,
        CREDENTIAL_FREE_OPENAPI,
        "added",
        CredentialRoute::None,
    )
    .await;
    let member = arranged.member("user:member").await;
    let agent = arranged.agent(16).await;
    let listed = ["coral.workspace", "coral.providers.added"];
    let roots = |providers: Vec<&crate::Provider>| {
        providers
            .into_iter()
            .map(|provider| provider.call_root().as_str().to_owned())
            .collect::<Vec<_>>()
    };

    for (principal, reached) in [
        (
            AuthenticatedPrincipal::user(arranged.owner.clone()),
            &[
                "addProvider",
                "getPolicy",
                "getProvider",
                "listProviders",
                "updatePolicy",
                "updateProvider",
            ][..],
        ),
        (
            AuthenticatedPrincipal::user(member),
            &["getPolicy", "listProviders"][..],
        ),
        (agent, &["getPolicy", "listProviders"][..]),
    ] {
        let providers = &arranged.providers;
        let published = crate::tests::published_providers(providers, &principal, workspace)
            .await
            .unwrap();
        assert_eq!(
            roots(
                published
                    .iter()
                    .map(|published| &published.provider)
                    .collect()
            ),
            listed,
            "{principal:?}"
        );
        let builtin_keys = published
            .first()
            .expect("the Built-in Provider comes first")
            .operations
            .iter()
            .map(|operation| operation.key().as_str())
            .collect::<Vec<_>>();
        assert_eq!(builtin_keys, reached, "{principal:?}");
        // Only a User inspects the Workspace's Providers in Reef.
        let Some(actor) = principal.authenticated_user() else {
            continue;
        };
        let inspected = providers.inspect_providers(actor, workspace).await.unwrap();
        assert_eq!(roots(inspected.iter().collect()), listed, "{actor:?}");
        let database = arranged.fixture.database();
        let mut tx = database.begin_read_snapshot().await.unwrap();
        let evidence = arranged
            .workspaces
            .check_in_transaction(&mut tx, actor, workspace, WorkspaceAction::Inspect)
            .await
            .unwrap();
        let readiness = crate::access::inspect_provider_readiness_in_transaction(
            &mut tx, actor, &evidence, None,
        )
        .await
        .unwrap();
        tx.rollback().await.unwrap();
        assert_eq!(
            roots(readiness.iter().map(|ready| &ready.provider).collect()),
            listed,
            "{actor:?}"
        );

        let builtin = arranged.builtin.id();
        let one = providers.inspect_provider(actor, workspace, builtin).await;
        assert_eq!(one.unwrap().id(), builtin, "{actor:?}");
        // One Operation a page: an Operation left out must not cost the
        // page its lookahead row, nor add one.
        for page_size in [1, 100] {
            let operations = providers
                .inspect_catalog_page(actor, workspace, builtin, None, page_size)
                .await
                .unwrap();
            let keys = operations
                .iter()
                .map(|operation| operation.key.as_str())
                .collect::<Vec<_>>();
            let lookahead = usize::try_from(page_size).unwrap() + 1;
            let expected = reached.iter().copied().take(lookahead).collect::<Vec<_>>();
            assert_eq!(keys, expected, "{actor:?}, page of {page_size}");
        }
    }
}

/// A boot installs the Built-in Provider in each Workspace that has none, such
/// as one created before Coral had it, in any Workspace state, with Coral as
/// actor. A Workspace that has one changes nothing.
#[tokio::test]
async fn a_boot_installs_the_builtin_provider_only_where_a_workspace_has_none() {
    let arranged = BuiltinWorkspace::new().await;
    let context = change_context(arranged.owner.clone(), 15, '2');
    let mut missing = Vec::new();
    for name in ["Earlier", "Archived"] {
        missing.push(
            arranged
                .workspaces
                .create(&context, WorkspaceDisplayName::parse(name).unwrap())
                .await
                .unwrap(),
        );
    }
    let [earlier, archived] = missing.as_slice() else {
        unreachable!("two Workspaces were created");
    };
    arranged
        .workspaces
        .archive(&context, archived.id())
        .await
        .unwrap();
    let mut expected = vec![earlier.id().clone(), archived.id().clone()];
    expected.sort();
    let mut without = arranged
        .providers
        .workspaces_without_builtin()
        .await
        .unwrap();
    without.sort();
    assert_eq!(without, expected);
    let before = arranged.events().await.len();

    let database = arranged.fixture.database();
    for workspace in [&arranged.workspace, earlier, archived] {
        let mut transaction = database.begin().await.unwrap();
        let installed = arranged
            .providers
            .install_missing_builtin_in_transaction(&mut transaction, workspace.id(), 20)
            .await
            .unwrap();
        transaction.commit().await.unwrap();
        let name = workspace.display_name().as_str();
        assert_eq!(
            installed.is_some(),
            workspace.id() != arranged.workspace.id(),
            "{name}"
        );
        if let Some(provider) = installed {
            assert_eq!(provider.call_root(), &ProviderCallRoot::Builtin, "{name}");
        }
    }

    assert!(
        arranged
            .providers
            .workspaces_without_builtin()
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(arranged.events().await.len(), before);
    let [event] = arranged
        .providers
        .list_lifecycle_events(&arranged.owner, earlier.id())
        .await
        .unwrap()
        .try_into()
        .expect("the install records one event");
    assert_eq!(event.action(), LifecycleAction::BuiltinProviderInstalled);
    assert!(matches!(event.actor(), LifecycleActor::Coral), "{event:?}");
}

/// Two processes that boot together each try the install; one installs, and
/// the other then finds the Workspace has one.
#[tokio::test]
async fn two_processes_that_boot_together_install_one_builtin_provider() {
    let (fixture, workspaces, first) = test_modules().await;
    let owner = organization_user(fixture.database(), "user:owner", OrganizationRole::Member).await;
    let workspace = workspaces
        .create(
            &change_context(owner, 10, '1'),
            WorkspaceDisplayName::parse("Earlier").unwrap(),
        )
        .await
        .unwrap();
    let peer = std::sync::Arc::new(fixture.connect_peer().await.unwrap());
    let second = ProviderModule::new(
        coral_outbound::DestinationPolicy::default(),
        peer.clone(),
        coral_workspace::WorkspaceModule::new(peer.clone()),
        running(),
    );
    let install = |process: &ProviderModule, database: std::sync::Arc<coral_db::Database>, at| {
        let process = process.clone();
        let workspace_id = workspace.id().clone();
        async move {
            let mut transaction = database.begin().await.unwrap();
            let installed = process
                .install_missing_builtin_in_transaction(&mut transaction, &workspace_id, at)
                .await
                .unwrap();
            transaction.commit().await.unwrap();
            installed.is_some()
        }
    };

    let (left, right) = tokio::join!(
        install(&first, fixture.database(), 20),
        install(&second, peer, 21),
    );

    assert_eq!(
        [left, right].iter().filter(|installed| **installed).count(),
        1,
        "{left:?} {right:?}"
    );
    assert!(first.workspaces_without_builtin().await.unwrap().is_empty());
}
