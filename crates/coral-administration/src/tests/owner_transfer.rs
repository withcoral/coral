//! Organization Membership removal hands each Workspace the departing User
//! solely owns to a transfer target, in the removal's own transaction.
use super::*;
use coral_workspace::{AuthoritySource, LifecycleAction, OrganizationModule, WorkspaceRole};

/// One Workspace the departing User owns, with their connected Account, pending
/// reconnect, and Saved Function there.
struct Owned {
    workspace: coral_workspace::Workspace,
    connected: accounts::ConnectedScope,
}

/// An Organization Owner with no Workspace Membership removes a User who owns
/// three Workspaces: two alone and one with a second designated Owner.
struct Departure {
    db: coral_test_support::TestDatabase,
    workspaces: WorkspaceModule,
    organization: OrganizationModule,
    lifecycle: AccessLifecycleModule,
    functions: SavedFunctionModule,
    administrator: AuthenticatedUser,
    departing: AuthenticatedUser,
    target: AuthenticatedUser,
    co_owner: AuthenticatedUser,
    disabled: AuthenticatedUser,
    /// The target holds no Membership here.
    unjoined: Owned,
    /// The target is a `member` here.
    joined: Owned,
    /// `co_owner` is a second designated Owner here.
    shared: Owned,
}

impl Departure {
    async fn new() -> Self {
        let db = fixture().await;
        let database = db.database();
        let workspaces = WorkspaceModule::new(database.clone());
        let organization = OrganizationModule::new(database.clone());
        let user = |id: &'static str, role| provision_user(database.clone(), id, role);
        let administrator = user("user:administrator", OrganizationRole::Owner).await;
        let departing = user("user:departing", OrganizationRole::Member).await;
        let target = user("user:target", OrganizationRole::Member).await;
        let co_owner = user("user:co-owner", OrganizationRole::Member).await;
        let disabled = user("user:disabled", OrganizationRole::Member).await;
        organization
            .set_enabled(&change(&administrator), disabled.user_id(), 1, false)
            .await
            .unwrap();
        let functions = SavedFunctionModule::new(
            database.clone(),
            workspaces.clone(),
            coral_functions::IsolateLimits::default(),
        )
        .unwrap();
        let author = AuthenticatedPrincipal::user(departing.clone());
        let mut owned = Vec::new();
        for (name, other) in [
            ("Unjoined", None),
            ("Joined", Some((&target, WorkspaceRole::Member))),
            ("Shared", Some((&co_owner, WorkspaceRole::Owner))),
        ] {
            let workspace = workspaces
                .create(
                    &change(&departing),
                    WorkspaceDisplayName::parse(name).unwrap(),
                )
                .await
                .unwrap();
            if let Some((member, role)) = other {
                workspaces
                    .set_membership(&change(&departing), workspace.id(), member, role)
                    .await
                    .unwrap();
            }
            save_answer(&functions, &author, workspace.id()).await;
            let connected = accounts::ConnectedScope::new(
                database.clone(),
                workspaces.clone(),
                &departing,
                &departing,
                workspace.id(),
            )
            .await;
            owned.push(Owned {
                workspace,
                connected,
            });
        }
        let [unjoined, joined, shared] = <[Owned; 3]>::try_from(owned).ok().unwrap();
        // Removal ignores archive state, so an archived Workspace is transferred too.
        workspaces
            .archive_as_organization_owner(&change(&administrator), unjoined.workspace.id())
            .await
            .unwrap();
        Self {
            db,
            lifecycle: AccessLifecycleModule::new(database, workspaces.clone()),
            workspaces,
            organization,
            functions,
            administrator,
            departing,
            target,
            co_owner,
            disabled,
            unjoined,
            joined,
            shared,
        }
    }

    fn owned(&self) -> [&Owned; 3] {
        [&self.unjoined, &self.joined, &self.shared]
    }

    fn removal<'a>(&'a self, transfer_to: Option<&'a UserId>) -> AccessRemovalTarget<'a> {
        AccessRemovalTarget::OrganizationMember {
            user: self.departing.user_id(),
            revision: 1,
            transfer_to,
        }
    }

    /// Everything a denied or rolled-back removal must leave alone, read while
    /// the departing User still owns every Workspace.
    async fn state(&self) -> impl PartialEq + std::fmt::Debug + use<> {
        let author = AuthenticatedPrincipal::user(self.departing.clone());
        let path = SavedFunctionPath::parse("answer").unwrap();
        let mut scopes = Vec::new();
        for owned in self.owned() {
            let workspace = owned.workspace.id();
            owned.connected.assert_unchanged(&self.departing).await;
            scopes.push((
                self.workspaces
                    .list_memberships(&self.departing, workspace)
                    .await
                    .unwrap(),
                self.workspaces
                    .list_lifecycle_events(&self.departing, workspace)
                    .await
                    .unwrap(),
                self.functions.get(&author, workspace, &path).await.unwrap(),
                self.functions
                    .list_lifecycle_events(&author, workspace)
                    .await
                    .unwrap(),
            ));
        }
        (
            self.organization
                .resolve_user_authority(self.departing.user_id())
                .await
                .unwrap(),
            self.organization
                .list_lifecycle_events(&self.administrator)
                .await
                .unwrap()
                .into_iter()
                .map(|event| event.id)
                .collect::<Vec<_>>(),
            scopes,
        )
    }

    /// Current Memberships as `reader`, a designated Owner, sees them.
    async fn memberships(
        &self,
        reader: &AuthenticatedUser,
        owned: &Owned,
    ) -> Vec<(UserId, WorkspaceRole)> {
        self.workspaces
            .list_memberships(reader, owned.workspace.id())
            .await
            .unwrap()
            .iter()
            .map(|membership| (membership.user_id().clone(), membership.role()))
            .collect()
    }
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one departure proves each denial and rollback changes nothing before the transfer commits"
)]
async fn organization_removal_of_a_sole_workspace_owner_transfers_ownership_or_changes_nothing() {
    let f = Departure::new().await;
    let database = f.db.database();
    let absent = UserId::parse("user:absent").unwrap();
    let before = f.state().await;
    let mut events_before = Vec::new();
    for owned in f.owned() {
        events_before.push(
            f.workspaces
                .list_lifecycle_events(&f.departing, owned.workspace.id())
                .await
                .unwrap(),
        );
    }

    // (case, transfer target, whether the denial is the missing-target one)
    let denials = [
        ("no target", None, true),
        ("the departing User", Some(f.departing.user_id()), false),
        ("a disabled User", Some(f.disabled.user_id()), false),
        ("an unknown User", Some(&absent), false),
    ];
    for (case, transfer_to, missing) in denials {
        let denied = f
            .lifecycle
            .remove_organization_member(
                &change(&f.administrator),
                f.departing.user_id(),
                1,
                transfer_to,
            )
            .await;
        let exact = match &denied {
            Err(Error::Workspace(coral_workspace::Error::OwnerTransferRequired)) => missing,
            Err(Error::Workspace(coral_workspace::Error::OwnerTargetNotEnabled)) => !missing,
            _ => false,
        };
        assert!(exact, "{case}: {denied:?}");
        assert_eq!(f.state().await, before, "{case}: a denial changes nothing");
    }

    // The last two faults fire after the new Owner Memberships are written.
    for attempt in RemovalAttempt::cases(&[
        EventFault::AccountDeleted,
        EventFault::FunctionDeleted,
        EventFault::MembershipRemoved,
        EventFault::OrganizationMembershipRemoved,
    ]) {
        attempt
            .assert_rolled_back(
                &f.lifecycle,
                &database,
                &f.administrator,
                f.removal(Some(f.target.user_id())),
            )
            .await;
        assert_eq!(f.state().await, before, "a failed removal changes nothing");
    }

    f.lifecycle
        .remove_organization_member(
            &change(&f.administrator),
            f.departing.user_id(),
            1,
            Some(f.target.user_id()),
        )
        .await
        .unwrap();
    assert!(matches!(
        f.organization
            .resolve_user_authority(f.departing.user_id())
            .await,
        Err(coral_workspace::Error::PermissionDenied(_))
    ));

    // (Workspace, its Owner afterwards, the target's Owner Membership event)
    let outcomes = [
        (
            &f.unjoined,
            &f.target,
            Some(LifecycleAction::MembershipCreated),
        ),
        (
            &f.joined,
            &f.target,
            Some(LifecycleAction::MembershipRoleUpdated),
        ),
        // A second designated Owner remains, so the target gains nothing here.
        (&f.shared, &f.co_owner, None),
    ];
    let returned = provision_user(database, "user:departing", OrganizationRole::Member).await;
    for ((owned, owner, transfer), events_before) in outcomes.into_iter().zip(&events_before) {
        let workspace = owned.workspace.id();
        assert_eq!(
            f.memberships(owner, owned).await,
            [(owner.user_id().clone(), WorkspaceRole::Owner)]
        );
        let transfers = f
            .workspaces
            .list_lifecycle_events(owner, workspace)
            .await
            .unwrap()
            .into_iter()
            .filter(|event| {
                !events_before.contains(event) && event.target_id() == f.target.user_id().as_str()
            })
            .map(|event| {
                (
                    event.action(),
                    event.authority_source(),
                    event.actor_user_id().clone(),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            transfers,
            transfer
                .map(|action| (
                    action,
                    AuthoritySource::OrganizationOwner,
                    f.administrator.user_id().clone()
                ))
                .into_iter()
                .collect::<Vec<_>>()
        );

        // The new Owner of the archived Workspace restores it like any Owner.
        if workspace == f.unjoined.workspace.id() {
            f.workspaces
                .restore(&change(owner), workspace)
                .await
                .unwrap();
        }
        // A fresh login and grant cannot restore the departed User's work.
        f.workspaces
            .set_membership(&change(owner), workspace, &returned, WorkspaceRole::Member)
            .await
            .unwrap();
        assert!(matches!(
            f.functions
                .get(
                    &AuthenticatedPrincipal::user(returned.clone()),
                    workspace,
                    &SavedFunctionPath::parse("answer").unwrap()
                )
                .await,
            Err(coral_functions::Error::SavedFunctionNotFound)
        ));
        owned.connected.assert_retired(&returned).await;
    }
}
