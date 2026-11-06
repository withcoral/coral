use super::{change, organization_user, test_module};
use crate::{
    AuthenticatedUser, AuthoritySource, Error, LifecycleAction, LifecycleTargetKind,
    OrganizationRole, Workspace, WorkspaceAction, WorkspaceDisplayName, WorkspaceModule,
    WorkspaceRole,
};
use coral_test_support::TestDatabase;

struct Fixture {
    _database: TestDatabase,
    module: WorkspaceModule,
    /// An Organization Owner with no Membership in the Workspace.
    governor: AuthenticatedUser,
    /// The Workspace's designated Owner, an Organization Member.
    owner: AuthenticatedUser,
    workspace: Workspace,
}

async fn fixture() -> Fixture {
    let (database, module) = test_module().await;
    let user = |id, role| organization_user(module.database.clone(), id, role);
    let governor = user("user:governor", OrganizationRole::Owner).await;
    let owner = user("user:owner", OrganizationRole::Member).await;
    let workspace = module
        .create(
            &change(owner.clone(), 2, '1'),
            WorkspaceDisplayName::parse("Engineering").unwrap(),
        )
        .await
        .unwrap();
    Fixture {
        _database: database,
        module,
        governor,
        owner,
        workspace,
    }
}

impl Fixture {
    async fn granted(&self) -> Vec<String> {
        self.module
            .list_workspace_secret_stores(&self.owner, self.workspace.id())
            .await
            .unwrap()
    }
}

// Catches: a grant that does not reach the Workspace, a revocation that keeps
// it, or a change with no audit evidence, or evidence carrying anything but
// the store's name.
#[tokio::test]
async fn an_organization_owner_grants_and_revokes_with_an_event_for_each() {
    let f = fixture().await;
    let id = f.workspace.id();
    let events_before = f.module.list_lifecycle_events(&f.owner, id).await.unwrap();

    f.module
        .grant_secret_store(&change(f.governor.clone(), 10, '2'), id, "platform")
        .await
        .unwrap();
    assert_eq!(f.granted().await, ["platform"]);

    f.module
        .revoke_secret_store(&change(f.governor.clone(), 11, '3'), id, "platform")
        .await
        .unwrap();
    assert!(f.granted().await.is_empty());

    let events = f.module.list_lifecycle_events(&f.owner, id).await.unwrap();
    let added: Vec<_> = events.iter().skip(events_before.len()).collect();
    let [granted, revoked] = added.as_slice() else {
        panic!("expected two new Events, got {added:?}");
    };
    for (event, action, at) in [
        (*granted, LifecycleAction::SecretStoreGranted, 10),
        (*revoked, LifecycleAction::SecretStoreRevoked, 11),
    ] {
        assert_eq!(event.action(), action);
        assert_eq!(event.actor_user_id(), f.governor.user_id());
        assert_eq!(event.authority_source(), AuthoritySource::OrganizationOwner);
        assert_eq!(event.target_kind(), LifecycleTargetKind::SecretStore);
        assert_eq!(event.target_id(), "platform");
        assert_eq!(event.occurred_at_unix_nanos(), at);
    }
    // The Organization Owner reads the same Events, as their own governance.
    let governance = f.module.list_governance_events(&f.governor).await.unwrap();
    assert!(added.iter().all(|event| governance.contains(event)));
}

#[derive(Clone, Copy, Debug)]
enum Actor {
    WorkspaceOwner,
    Outsider,
    OrganizationOwner,
}

// Catches: a Workspace Owner granting themselves a store (ADR 0032), a change to
// an archived Workspace, which permits none but restoration, or a refusal that
// writes a grant or an Event first.
#[tokio::test]
async fn a_refused_change_changes_nothing() {
    for (actor, archived) in [
        (Actor::WorkspaceOwner, false),
        (Actor::Outsider, false),
        (Actor::OrganizationOwner, true),
    ] {
        let f = fixture().await;
        let id = f.workspace.id();
        f.module
            .grant_secret_store(&change(f.governor.clone(), 10, '2'), id, "granted")
            .await
            .unwrap();
        if archived {
            f.module
                .archive_as_organization_owner(&change(f.governor.clone(), 11, '3'), id)
                .await
                .unwrap();
        }
        let outsider = organization_user(
            f.module.database.clone(),
            "user:outsider",
            OrganizationRole::Member,
        )
        .await;
        let user = match actor {
            Actor::WorkspaceOwner => &f.owner,
            Actor::Outsider => &outsider,
            Actor::OrganizationOwner => &f.governor,
        };
        let events = f.module.list_lifecycle_events(&f.owner, id).await.unwrap();

        let context = change(user.clone(), 20, '4');
        for result in [
            f.module.grant_secret_store(&context, id, "other").await,
            f.module.revoke_secret_store(&context, id, "granted").await,
        ] {
            let refused = if archived {
                matches!(result, Err(Error::WorkspaceArchived))
            } else {
                matches!(result, Err(Error::PermissionDenied(_)))
            };
            assert!(refused, "{actor:?}: {result:?}");
        }

        assert_eq!(f.granted().await, ["granted"]);
        assert_eq!(
            f.module.list_lifecycle_events(&f.owner, id).await.unwrap(),
            events
        );
    }
}

// Catches: a grant that gives the Organization Owner authority inside the
// Workspace, which ADR 0032 and ADR 0015 forbid.
#[tokio::test]
async fn a_grant_shows_the_organization_owner_nothing_inside_the_workspace() {
    let f = fixture().await;
    let id = f.workspace.id();
    f.module
        .grant_secret_store(&change(f.governor.clone(), 10, '2'), id, "platform")
        .await
        .unwrap();

    assert!(matches!(
        f.module
            .check(&f.governor, id, WorkspaceAction::Inspect)
            .await,
        Err(Error::WorkspaceNotFound)
    ));
    assert!(matches!(
        f.module.list_memberships(&f.governor, id).await,
        Err(Error::WorkspaceNotFound)
    ));
    assert!(matches!(
        f.module.list_lifecycle_events(&f.governor, id).await,
        Err(Error::WorkspaceNotFound)
    ));
    assert!(matches!(
        f.module.list_workspace_secret_stores(&f.governor, id).await,
        Err(Error::WorkspaceNotFound)
    ));
}

// Catches: a Member, who has no use for it, learning where the Workspace's
// credentials could come from.
#[tokio::test]
async fn only_a_workspace_owner_reads_the_workspace_grants() {
    let f = fixture().await;
    let id = f.workspace.id();
    let member = organization_user(
        f.module.database.clone(),
        "user:member",
        OrganizationRole::Member,
    )
    .await;
    f.module
        .set_membership(
            &change(f.owner.clone(), 3, '5'),
            id,
            &member,
            WorkspaceRole::Member,
        )
        .await
        .unwrap();

    assert!(matches!(
        f.module.list_workspace_secret_stores(&member, id).await,
        Err(Error::PermissionDenied(_))
    ));
}

// Catches: a second grant from a new form, or a revocation of nothing, adding
// false audit evidence.
#[tokio::test]
async fn a_change_that_changes_nothing_records_no_event() {
    let f = fixture().await;
    let id = f.workspace.id();
    let context = change(f.governor.clone(), 10, '2');
    f.module
        .grant_secret_store(&context, id, "platform")
        .await
        .unwrap();
    let events = f.module.list_lifecycle_events(&f.owner, id).await.unwrap();

    f.module
        .grant_secret_store(&context, id, "platform")
        .await
        .unwrap();
    f.module
        .revoke_secret_store(&context, id, "never-granted")
        .await
        .unwrap();

    assert_eq!(f.granted().await, ["platform"]);
    assert_eq!(
        f.module.list_lifecycle_events(&f.owner, id).await.unwrap(),
        events
    );
}

// Catches: a stale form granting a store again after its owner revoked it.
#[tokio::test]
async fn a_replayed_grant_does_not_undo_a_revocation() {
    let f = fixture().await;
    let id = f.workspace.id();
    let request = "00000000-0000-4000-8000-000000000001";
    let grant = change(f.governor.clone(), 10, '2')
        .with_request_id(request)
        .unwrap();
    f.module
        .grant_secret_store(&grant, id, "platform")
        .await
        .unwrap();
    f.module
        .revoke_secret_store(&change(f.governor.clone(), 11, '3'), id, "platform")
        .await
        .unwrap();
    let events = f.module.list_lifecycle_events(&f.owner, id).await.unwrap();

    assert!(matches!(
        f.module.grant_secret_store(&grant, id, "platform").await,
        Err(Error::RequestAlreadyApplied)
    ));
    assert!(f.granted().await.is_empty());
    assert_eq!(
        f.module.list_lifecycle_events(&f.owner, id).await.unwrap(),
        events
    );
}

// Catches: a grant whose name Coral rewrites or drops because config no longer
// defines it. This Module keeps it; the join with config sits above.
// Also catches: a listing that leaks to anyone but an Organization Owner.
#[tokio::test]
async fn an_organization_owner_lists_every_workspace_grant() {
    let f = fixture().await;
    let id = f.workspace.id();
    for store in ["zeta", "alpha"] {
        f.module
            .grant_secret_store(&change(f.governor.clone(), 10, '2'), id, store)
            .await
            .unwrap();
    }

    let grants = f
        .module
        .list_secret_store_grants(&f.governor)
        .await
        .unwrap();
    let listed: Vec<_> = grants
        .iter()
        .map(|grant| (grant.workspace.id(), grant.store.as_str()))
        .collect();
    assert_eq!(listed, [(id, "alpha"), (id, "zeta")]);
    assert!(
        grants
            .iter()
            .all(|grant| grant.workspace.display_name().as_str() == "Engineering")
    );

    assert!(matches!(
        f.module.list_secret_store_grants(&f.owner).await,
        Err(Error::PermissionDenied(_))
    ));
}
