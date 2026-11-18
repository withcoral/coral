use super::{at_version, change, organization_user, test_module};
use crate::{
    AgentDisplayName, AgentRegistrationId, AuthenticatedUser, Error, IfMatch, MembershipChange,
    OrganizationRole, RequestId, WorkspaceDisplayName, WorkspaceId, WorkspaceModule, WorkspaceRole,
};
use coral_db::UnixNanos;
use coral_test_support::TestDatabase;

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one grant is followed through duplicate creation, operation replay, and address reassignment"
)]
async fn exact_email_grant_resolves_one_verified_user_and_never_retargets_replay() {
    let (database, module) = test_module().await;
    let owner = organization_user(
        module.database.clone(),
        "user:email-owner",
        OrganizationRole::Owner,
    )
    .await;
    let alice = organization_user(
        module.database.clone(),
        "user:email-alice",
        OrganizationRole::Member,
    )
    .await;
    let bob = organization_user(
        module.database.clone(),
        "user:email-bob",
        OrganizationRole::Member,
    )
    .await;
    let workspace = module
        .create(
            &change(owner.clone(), 10, '1'),
            WorkspaceDisplayName::parse("Email grants").unwrap(),
        )
        .await
        .unwrap();
    let refresh = async |user: &crate::UserId, email: &str, time| {
        let mut tx = module.database.begin().await.unwrap();
        crate::OrganizationModule::hold_profile_updates_in_transaction(&mut tx)
            .await
            .unwrap();
        crate::OrganizationModule::refresh_profile_in_transaction(
            &mut tx,
            user,
            crate::VerifiedUserProfile::new(Some("Person"), Some(email), Some(true), None),
            time,
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
    };
    refresh(alice.user_id(), "Alice+tag@Example.test", UnixNanos::new(1)).await;
    let request = change(owner.clone(), 20, '2');
    let request_id = RequestId::generate();
    let granted = module
        .grant_membership_by_email(
            &request,
            &request_id,
            workspace.id(),
            "  ALICE+tag@example.test ",
            WorkspaceRole::Member,
        )
        .await
        .unwrap();
    assert_eq!(granted.user_id(), alice.user_id());
    assert!(matches!(
        module
            .grant_membership_by_email(
                &change(owner.clone(), 21, '3'),
                &RequestId::generate(),
                workspace.id(),
                "alice+tag@example.test",
                WorkspaceRole::Owner
            )
            .await,
        Err(Error::MembershipAlreadyExists)
    ));
    assert!(matches!(
        module
            .grant_membership_by_email(
                &request,
                &request_id,
                workspace.id(),
                "different-operation@example.test",
                WorkspaceRole::Owner
            )
            .await,
        Err(Error::RequestAlreadyApplied)
    ));
    refresh(alice.user_id(), "different@example.test", UnixNanos::new(2)).await;
    refresh(bob.user_id(), "alice+tag@example.test", UnixNanos::new(3)).await;
    assert!(matches!(
        module
            .grant_membership_by_email(
                &request,
                &request_id,
                workspace.id(),
                "  ALICE+tag@example.test ",
                WorkspaceRole::Member
            )
            .await,
        Err(Error::RequestAlreadyApplied)
    ));
    assert!(matches!(
        module
            .inspect_membership(&owner, workspace.id(), bob.user_id())
            .await,
        Err(Error::MembershipNotFound)
    ));
    assert_eq!(
        module
            .inspect_membership(&owner, workspace.id(), alice.user_id())
            .await
            .unwrap()
            .0
            .role(),
        WorkspaceRole::Member
    );
    drop(database);
}

#[tokio::test]
async fn email_grant_serializes_resolution_with_profile_ingestion() {
    let (_database, module) = test_module().await;
    let owner = organization_user(
        module.database.clone(),
        "user:race-owner",
        OrganizationRole::Owner,
    )
    .await;
    let alice = organization_user(
        module.database.clone(),
        "user:race-alice",
        OrganizationRole::Member,
    )
    .await;
    let bob = organization_user(
        module.database.clone(),
        "user:race-bob",
        OrganizationRole::Member,
    )
    .await;
    let workspace = module
        .create(
            &change(owner.clone(), 10, '1'),
            WorkspaceDisplayName::parse("Profile race").unwrap(),
        )
        .await
        .unwrap();
    let mut tx = module.database.begin().await.unwrap();
    crate::OrganizationModule::hold_profile_updates_in_transaction(&mut tx)
        .await
        .unwrap();
    crate::OrganizationModule::refresh_profile_in_transaction(
        &mut tx,
        alice.user_id(),
        crate::VerifiedUserProfile::new(None, Some("race@example.test"), Some(true), None),
        UnixNanos::new(1),
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();

    let mut tx = module.database.begin().await.unwrap();
    crate::OrganizationModule::hold_profile_updates_in_transaction(&mut tx)
        .await
        .unwrap();
    let request = change(owner.clone(), 20, '2');
    let request_id = RequestId::generate();
    let mut grant = Box::pin(module.grant_membership_by_email(
        &request,
        &request_id,
        workspace.id(),
        "race@example.test",
        WorkspaceRole::Member,
    ));
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), &mut grant)
            .await
            .is_err(),
        "grant must wait for profile ingestion"
    );
    crate::OrganizationModule::refresh_profile_in_transaction(
        &mut tx,
        bob.user_id(),
        crate::VerifiedUserProfile::new(None, Some("race@example.test"), Some(true), None),
        UnixNanos::new(2),
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    assert!(matches!(grant.await, Err(Error::AmbiguousEmail)));
    assert_eq!(
        module
            .list_memberships(&owner, workspace.id())
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn management_metadata_preserves_creation_time_and_scopes_membership_etags() {
    let (_database, module) = test_module().await;
    let owner = organization_user(
        module.database.clone(),
        "user:metadata-owner",
        OrganizationRole::Member,
    )
    .await;
    let mut workspaces = Vec::new();
    for name in ["First", "Second"] {
        workspaces.push(
            module
                .create(
                    &change(owner.clone(), 10, '1'),
                    WorkspaceDisplayName::parse(name).unwrap(),
                )
                .await
                .unwrap(),
        );
    }
    let first = workspaces.first().unwrap().id();
    let second = workspaces.last().unwrap().id();
    let (_, initial) = module.inspect_workspace(&owner, first).await.unwrap();
    let (_, first_member, _) = module
        .inspect_membership(&owner, first, owner.user_id())
        .await
        .unwrap();
    let (_, second_member, _) = module
        .inspect_membership(&owner, second, owner.user_id())
        .await
        .unwrap();
    assert_ne!(first_member.etag, second_member.etag);
    assert_eq!(
        (initial.created_at, initial.updated_at),
        (UnixNanos::new(10), UnixNanos::new(10))
    );

    module
        .update_display_name(
            &change(owner.clone(), 20, '2'),
            first,
            &IfMatch::parse(&initial.etag).unwrap(),
            WorkspaceDisplayName::parse("Renamed").unwrap(),
        )
        .await
        .unwrap();
    let (_, updated) = module.inspect_workspace(&owner, first).await.unwrap();
    assert_ne!(updated.etag, initial.etag);
    assert_eq!(
        (updated.created_at, updated.updated_at),
        (UnixNanos::new(10), UnixNanos::new(20))
    );
    assert!(matches!(
        module
            .update_display_name(
                &change(owner.clone(), 30, '3'),
                first,
                &IfMatch::parse(&initial.etag).unwrap(),
                WorkspaceDisplayName::parse("Stale update").unwrap(),
            )
            .await,
        Err(Error::EtagMismatch)
    ));
    let (evidence, unchanged) = module.inspect_workspace(&owner, first).await.unwrap();
    assert_eq!(evidence.workspace().display_name().as_str(), "Renamed");
    assert_eq!(unchanged.etag, updated.etag);
    assert_eq!(unchanged.updated_at, UnixNanos::new(20));
    for (workspace, original) in [(first, first_member), (second, second_member)] {
        let (_, member, _) = module
            .inspect_membership(&owner, workspace, owner.user_id())
            .await
            .unwrap();
        assert_eq!(member.etag, original.etag);
        assert_eq!(
            (member.created_at, member.updated_at),
            (UnixNanos::new(10), UnixNanos::new(10))
        );
    }
}

// Catches: a role change that checks its precondition before it finds the
// Membership. For a User who was never a Member that check finds no
// concurrency metadata and reports corrupt data, and for a removed Member it
// reports a stale etag. Either hides that the Membership is absent.
#[tokio::test]
async fn a_role_change_of_an_absent_membership_is_not_found_whatever_its_precondition() {
    let (_database, module) = test_module().await;
    let user =
        |id: &'static str| organization_user(module.database.clone(), id, OrganizationRole::Member);
    let owner = user("user:absent-owner").await;
    let never = user("user:absent-never").await;
    let removed = user("user:absent-removed").await;
    let workspace = module
        .create(
            &change(owner.clone(), 10, '1'),
            WorkspaceDisplayName::parse("Absent").unwrap(),
        )
        .await
        .unwrap();
    let workspace = workspace.id();
    module
        .set_membership(
            &change(owner.clone(), 20, '2'),
            workspace,
            &removed,
            WorkspaceRole::Member,
        )
        .await
        .unwrap();
    let (_, shown, _) = module
        .inspect_membership(&owner, workspace, removed.user_id())
        .await
        .unwrap();
    let shown = IfMatch::parse(&shown.etag).unwrap();
    super::remove_membership_authority(
        &module,
        &change(owner.clone(), 30, '3'),
        workspace,
        removed.user_id(),
    )
    .await
    .unwrap();
    let kept = || async {
        (
            module.list_memberships(&owner, workspace).await.unwrap(),
            module
                .list_lifecycle_events(&owner, workspace)
                .await
                .unwrap(),
        )
    };
    let before = kept().await;

    for (case, target, if_match) in [
        ("never a Member, a version", &never, at_version(1)),
        (
            "never a Member, a UUID",
            &never,
            IfMatch::token(uuid::Uuid::new_v4()),
        ),
        ("removed, the version it had", &removed, at_version(1)),
        ("removed, the etag it showed", &removed, shown),
    ] {
        let refused = module
            .manage_membership(
                &change(owner.clone(), 40, '4'),
                workspace,
                target,
                MembershipChange::UpdateRole {
                    role: WorkspaceRole::Owner,
                    if_match,
                },
            )
            .await;
        assert!(
            matches!(refused, Err(Error::MembershipNotFound)),
            "{case}: {refused:?}"
        );
        assert_eq!(kept().await, before, "{case}: a refusal changes nothing");
    }
}

/// One action that takes a request identity. Each takes one of two targets,
/// so that a repeat that ran again would show.
#[derive(Debug, Clone, Copy)]
enum Once {
    CreateWorkspace,
    CreateAgent,
    RotateCredential,
    GrantByEmail,
    AssignOwner,
    GrantSecretStore,
    RevokeSecretStore,
}

/// One Workspace with two designated Owners who are both Organization
/// Owners, so that either may take every action that takes a request
/// identity.
struct Requests {
    _database: TestDatabase,
    module: WorkspaceModule,
    first: AuthenticatedUser,
    second: AuthenticatedUser,
    workspace: WorkspaceId,
    /// Two Workspace Members, whom an assignment makes Owners.
    members: [AuthenticatedUser; 2],
    registration: AgentRegistrationId,
}

/// The verified emails of three Organization Members with no Membership.
const EMAILS: [&str; 3] = [
    "once-a@example.test",
    "once-b@example.test",
    "once-late@example.test",
];
/// The stores a grant names, and the granted stores a revocation names.
const GRANTED: [&str; 2] = ["granted-a", "granted-b"];
const REVOKED: [&str; 2] = ["revoked-a", "revoked-b"];

impl Requests {
    async fn new() -> Self {
        let (database, module) = test_module().await;
        let user = |id: &'static str, role| organization_user(module.database.clone(), id, role);
        let first = user("user:once-first", OrganizationRole::Owner).await;
        let second = user("user:once-second", OrganizationRole::Owner).await;
        let people = [
            user("user:once-person-a", OrganizationRole::Member).await,
            user("user:once-person-b", OrganizationRole::Member).await,
            user("user:once-person-late", OrganizationRole::Member).await,
        ];
        let members = [
            user("user:once-member-a", OrganizationRole::Member).await,
            user("user:once-member-b", OrganizationRole::Member).await,
        ];
        let workspace = module
            .create(
                &change(first.clone(), 10, '1'),
                WorkspaceDisplayName::parse("Requests").unwrap(),
            )
            .await
            .unwrap();
        let workspace = workspace.id().clone();
        let setup = change(first.clone(), 11, '1');
        module
            .set_membership(&setup, &workspace, &second, WorkspaceRole::Owner)
            .await
            .unwrap();
        for member in &members {
            module
                .set_membership(&setup, &workspace, member, WorkspaceRole::Member)
                .await
                .unwrap();
        }
        for (person, email) in people.iter().zip(EMAILS) {
            refresh_profile(&module, person.user_id(), email, true, 12).await;
        }
        let registration = module
            .create_agent(
                &setup,
                &RequestId::generate(),
                &workspace,
                AgentDisplayName::parse("Rotated").unwrap(),
                None,
            )
            .await
            .unwrap()
            .registration
            .id()
            .clone();
        // A revocation needs a grant to end.
        for store in REVOKED {
            module
                .grant_secret_store(&setup, &RequestId::generate(), &workspace, store)
                .await
                .unwrap();
        }
        Self {
            _database: database,
            module,
            first,
            second,
            workspace,
            members,
            registration,
        }
    }

    /// Takes `action` on its `target`th target as `actor`, for `request`.
    async fn take(
        &self,
        action: Once,
        actor: &AuthenticatedUser,
        request: &RequestId,
        target: usize,
    ) -> Result<(), Error> {
        let module = &self.module;
        let workspace = &self.workspace;
        let context = change(actor.clone(), 20, '2');
        match action {
            Once::CreateWorkspace => {
                let mut tx = module.database.begin().await?;
                module
                    .create_in_transaction(
                        &mut tx,
                        &context,
                        request,
                        WorkspaceDisplayName::parse(&format!("Created {target}"))?,
                    )
                    .await?;
                Ok(tx.commit().await?)
            }
            Once::CreateAgent => module
                .create_agent(
                    &context,
                    request,
                    workspace,
                    AgentDisplayName::parse(&format!("Created {target}"))?,
                    None,
                )
                .await
                .map(drop),
            Once::RotateCredential => module
                .rotate_agent_credential(
                    &context,
                    request,
                    workspace,
                    &self.registration,
                    &IfMatch::any(),
                )
                .await
                .map(drop),
            Once::GrantByEmail => module
                .grant_membership_by_email(
                    &context,
                    request,
                    workspace,
                    EMAILS.get(target).expect("a target"),
                    WorkspaceRole::Member,
                )
                .await
                .map(drop),
            Once::AssignOwner => {
                let target = self.members.get(target).expect("a target");
                module
                    .assign_owner(&context, request, workspace, target.user_id())
                    .await
            }
            Once::GrantSecretStore => {
                let store = GRANTED.get(target).expect("a target");
                module
                    .grant_secret_store(&context, request, workspace, store)
                    .await
            }
            Once::RevokeSecretStore => {
                let store = REVOKED.get(target).expect("a target");
                module
                    .revoke_secret_store(&context, request, workspace, store)
                    .await
            }
        }
    }

    /// Everything the actions change, as the first Owner reads it. A rotation
    /// shows in the Registration's etag and in the Events.
    async fn kept(&self) -> impl PartialEq + std::fmt::Debug + use<> {
        let module = &self.module;
        let (owner, workspace) = (&self.first, &self.workspace);
        let workspaces = module.list(owner).await.unwrap();
        let memberships = module.list_memberships(owner, workspace).await.unwrap();
        let agents: Vec<_> = module
            .inspect_agents(owner, workspace)
            .await
            .unwrap()
            .into_iter()
            .map(|agent| {
                (
                    agent.agent,
                    agent.agent_metadata.etag,
                    agent.registration_metadata.etag,
                )
            })
            .collect();
        let stores = module
            .list_workspace_secret_stores(owner, workspace)
            .await
            .unwrap();
        let events = module
            .list_lifecycle_events(owner, workspace)
            .await
            .unwrap();
        (workspaces, memberships, agents, stores, events)
    }
}

// Catches: an action that takes a request identity and does not claim it, so
// that a client's retry creates a second Workspace or Agent, rotates a
// credential again, or undoes a later change; a claim that one User's request
// takes from another's; and a claim that outlives the refusal of its action.
#[tokio::test]
async fn a_request_identity_runs_its_action_once_for_each_user() {
    let f = Requests::new().await;
    for action in [
        Once::CreateWorkspace,
        Once::CreateAgent,
        Once::RotateCredential,
        Once::GrantByEmail,
        Once::AssignOwner,
        Once::GrantSecretStore,
        Once::RevokeSecretStore,
    ] {
        let request = RequestId::generate();
        f.take(action, &f.first, &request, 0)
            .await
            .unwrap_or_else(|error| panic!("{action:?}: {error:?}"));
        let before = f.kept().await;

        let repeated = f.take(action, &f.first, &request, 1).await;
        assert!(
            matches!(repeated, Err(Error::RequestAlreadyApplied)),
            "{action:?}: {repeated:?}"
        );
        assert_eq!(
            f.kept().await,
            before,
            "{action:?}: a repeat changes nothing"
        );

        f.take(action, &f.second, &request, 1)
            .await
            .unwrap_or_else(|error| panic!("{action:?} by another User: {error:?}"));
    }

    // A refusal that follows the claim rolls it back: the same identity then
    // names an accepted grant.
    let request = RequestId::generate();
    let context = change(f.first.clone(), 30, '3');
    let grant = |email: &'static str| {
        f.module.grant_membership_by_email(
            &context,
            &request,
            &f.workspace,
            email,
            WorkspaceRole::Member,
        )
    };
    assert!(matches!(
        grant("nobody@example.test").await,
        Err(Error::MembershipNotFound)
    ));
    grant("once-late@example.test")
        .await
        .expect("a refused action does not use up its request identity");
}

/// Records `user`'s trusted profile as login ingestion does.
async fn refresh_profile(
    module: &crate::WorkspaceModule,
    user: &crate::UserId,
    email: &str,
    verified: bool,
    at: i64,
) {
    let mut tx = module.database.begin().await.unwrap();
    crate::OrganizationModule::hold_profile_updates_in_transaction(&mut tx)
        .await
        .unwrap();
    crate::OrganizationModule::refresh_profile_in_transaction(
        &mut tx,
        user,
        crate::VerifiedUserProfile::new(Some("Person"), Some(email), Some(verified), None),
        UnixNanos::new(at),
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
}

async fn grant_by_email(
    module: &crate::WorkspaceModule,
    actor: &crate::AuthenticatedUser,
    workspace: &crate::WorkspaceId,
    email: &str,
) -> Result<crate::WorkspaceMembership, Error> {
    module
        .grant_membership_by_email(
            &change(actor.clone(), 30, '3'),
            &RequestId::generate(),
            workspace,
            email,
            WorkspaceRole::Member,
        )
        .await
}

/// An address selects a User only through one current, verified, exact match:
/// tags are not stripped, an unverified claim neither matches nor competes,
/// and two verified claims select nobody.
#[tokio::test]
async fn email_grants_require_one_current_verified_exact_address() {
    let (_database, module) = test_module().await;
    let db = module.database.clone();
    let owner = organization_user(db.clone(), "user:verified-owner", OrganizationRole::Owner).await;
    let alice =
        organization_user(db.clone(), "user:verified-alice", OrganizationRole::Member).await;
    let bob = organization_user(db, "user:verified-bob", OrganizationRole::Member).await;
    let workspace = module
        .create(
            &change(owner.clone(), 10, '1'),
            WorkspaceDisplayName::parse("Verified grants").unwrap(),
        )
        .await
        .unwrap();
    refresh_profile(&module, alice.user_id(), "Alice+tag@Example.test", true, 1).await;
    refresh_profile(&module, bob.user_id(), "alice+tag@example.test", false, 2).await;

    assert!(matches!(
        grant_by_email(&module, &owner, workspace.id(), "alice@example.test").await,
        Err(Error::MembershipNotFound)
    ));
    assert_eq!(
        grant_by_email(&module, &owner, workspace.id(), "alice+tag@example.test")
            .await
            .unwrap()
            .user_id(),
        alice.user_id()
    );

    super::remove_membership_authority(
        &module,
        &change(owner.clone(), 40, '4'),
        workspace.id(),
        alice.user_id(),
    )
    .await
    .unwrap();
    refresh_profile(&module, bob.user_id(), "alice+tag@example.test", true, 3).await;
    assert!(matches!(
        grant_by_email(&module, &owner, workspace.id(), "alice+tag@example.test").await,
        Err(Error::AmbiguousEmail)
    ));
    refresh_profile(&module, bob.user_id(), "bob@example.test", true, 4).await;
    refresh_profile(&module, alice.user_id(), "alice+tag@example.test", false, 5).await;
    assert!(matches!(
        grant_by_email(&module, &owner, workspace.id(), "alice+tag@example.test").await,
        Err(Error::MembershipNotFound)
    ));
    assert!(
        module
            .list_memberships(&owner, workspace.id())
            .await
            .unwrap()
            .iter()
            .all(|membership| membership.user_id() == owner.user_id())
    );

    module
        .set_membership(
            &change(owner.clone(), 50, '5'),
            workspace.id(),
            &bob,
            WorkspaceRole::Member,
        )
        .await
        .unwrap();
    assert!(matches!(
        grant_by_email(&module, &bob, workspace.id(), "alice+tag@example.test").await,
        Err(Error::PermissionDenied(
            crate::Denial::WorkspaceOwnerRequired
        ))
    ));
    module
        .archive(
            &change(owner.clone(), 60, '6'),
            workspace.id(),
            &IfMatch::any(),
        )
        .await
        .unwrap();
    assert!(matches!(
        grant_by_email(&module, &owner, workspace.id(), "bob@example.test").await,
        Err(Error::WorkspaceArchived)
    ));
}

/// Profiles are visible only between Users who share a Workspace, and an
/// Agent's creator stays attributed after the creator loses all authority.
#[tokio::test]
async fn profiles_are_workspace_scoped_and_creator_attribution_survives_removal() {
    let (_database, module) = test_module().await;
    let db = module.database.clone();
    let owner = organization_user(db.clone(), "user:profile-owner", OrganizationRole::Owner).await;
    let alice = organization_user(db.clone(), "user:profile-alice", OrganizationRole::Member).await;
    let bob = organization_user(db.clone(), "user:profile-bob", OrganizationRole::Member).await;
    refresh_profile(&module, owner.user_id(), "owner@example.test", true, 1).await;
    let context = change(owner.clone(), 10, '1');
    let workspace = module
        .create(&context, WorkspaceDisplayName::parse("Profiles").unwrap())
        .await
        .unwrap();
    module
        .set_membership(&context, workspace.id(), &alice, WorkspaceRole::Owner)
        .await
        .unwrap();

    assert_eq!(
        module
            .inspect_user_profile(&alice, workspace.id(), owner.user_id())
            .await
            .unwrap()
            .email
            .as_deref(),
        Some("owner@example.test")
    );
    for (actor, user) in [
        (&alice, bob.user_id().clone()),
        (&owner, crate::UserId::parse("user:unknown").unwrap()),
    ] {
        assert!(matches!(
            module
                .inspect_user_profile(actor, workspace.id(), &user)
                .await,
            Err(Error::MembershipNotFound)
        ));
    }

    let issued = module
        .create_agent(
            &change(alice.clone(), 20, '2'),
            &RequestId::generate(),
            workspace.id(),
            crate::AgentDisplayName::parse("Retained creator").unwrap(),
            None,
        )
        .await
        .unwrap();
    super::remove_membership_authority(&module, &context, workspace.id(), alice.user_id())
        .await
        .unwrap();
    crate::OrganizationModule::new(db)
        .set_enabled(&context, alice.user_id(), &at_version(1), false)
        .await
        .unwrap();
    assert_eq!(
        &module
            .inspect_agent(&owner, workspace.id(), issued.agent.principal().id())
            .await
            .unwrap()
            .creator_profile
            .user_id,
        alice.user_id()
    );
}
