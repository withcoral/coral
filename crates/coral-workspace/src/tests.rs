#[path = "agent_tests.rs"]
mod agents;
#[path = "management_tests.rs"]
mod management;

use coral_test_support::TestDatabase;

use crate::{
    AuthoritySource, ChangeContext, Error, LifecycleAction, LifecycleOutcome, OrganizationRole,
    TraceId, UserId, WorkspaceAction, WorkspaceDisplayName, WorkspaceModule, WorkspaceRole,
    WorkspaceState, local_organization_owner,
};

#[tokio::test]
async fn organization_member_creates_one_active_owned_workspace_with_safe_event() {
    let (_database, module) = test_module().await;
    let creator = organization_user(
        module.database.clone(),
        "user:creator",
        OrganizationRole::Member,
    )
    .await;
    let context = change(creator.clone(), 10, '1');

    let workspace = module
        .create(
            &context,
            WorkspaceDisplayName::parse("Engineering").expect("display name"),
        )
        .await
        .expect("create Workspace");

    assert_eq!(workspace.display_name().as_str(), "Engineering");
    assert_eq!(workspace.state(), WorkspaceState::Active);
    assert_eq!(workspace.lifecycle_version(), 1);
    let evidence = module
        .check(&creator, workspace.id(), WorkspaceAction::Configure)
        .await
        .expect("creator configures Workspace");
    assert_eq!(evidence.effective_role(), WorkspaceRole::Owner);
    assert_eq!(
        evidence.authority_source(),
        AuthoritySource::WorkspaceMembership
    );
    let memberships = module
        .list_memberships(&creator, workspace.id())
        .await
        .expect("list Memberships");
    assert_eq!(memberships.len(), 1);
    let creator_membership = memberships.first().expect("creator Membership");
    assert_eq!(creator_membership.workspace_id(), workspace.id());
    assert_eq!(creator_membership.user_id(), creator.user_id());
    assert_eq!(creator_membership.role(), WorkspaceRole::Owner);

    let events = module
        .list_lifecycle_events(&creator, workspace.id())
        .await
        .expect("list lifecycle Events");
    assert_eq!(events.len(), 2);
    let workspace_event = events
        .iter()
        .find(|event| event.action() == LifecycleAction::WorkspaceCreated)
        .expect("Workspace creation Event");
    assert_eq!(workspace_event.action(), LifecycleAction::WorkspaceCreated);
    assert_eq!(
        workspace_event.authority_source(),
        AuthoritySource::OrganizationMembership
    );
    assert_eq!(workspace_event.actor_user_id(), creator.user_id());
    assert_eq!(workspace_event.outcome(), LifecycleOutcome::Accepted);
    assert_eq!(
        workspace_event.changed_field_paths(),
        ["display_name", "state"]
    );
    assert_eq!(workspace_event.trace_id(), context.trace_id());

    let membership_event = events
        .iter()
        .find(|event| event.action() == LifecycleAction::MembershipCreated)
        .expect("initial Membership Event");
    assert_eq!(
        membership_event.action(),
        LifecycleAction::MembershipCreated
    );
    assert_eq!(
        membership_event.authority_source(),
        AuthoritySource::OrganizationMembership
    );
    assert_eq!(membership_event.target_id(), creator.user_id().as_str());
    assert_eq!(membership_event.changed_field_paths(), ["role"]);
    assert_eq!(membership_event.trace_id(), context.trace_id());
}

#[tokio::test]
async fn organization_owner_without_a_membership_has_no_authority_inside_a_workspace() {
    let (_database, module) = test_module().await;
    let module = &module;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let creator = organization_user(
        module.database.clone(),
        &format!("user:creator:{suffix}"),
        OrganizationRole::Member,
    )
    .await;
    let organization_owner = organization_user(
        module.database.clone(),
        &format!("user:organization-owner:{suffix}"),
        OrganizationRole::Owner,
    )
    .await;
    let outsider = organization_user(
        module.database.clone(),
        &format!("user:outsider:{suffix}"),
        OrganizationRole::Member,
    )
    .await;
    let workspace = module
        .create(
            &change(creator.clone(), 10, '1'),
            WorkspaceDisplayName::parse("Members only").expect("display name"),
        )
        .await
        .expect("create Workspace");
    let events_before = module
        .list_lifecycle_events(&creator, workspace.id())
        .await
        .expect("list Events");

    // The Organization Owner gets exactly the denial a non-Member gets.
    for action in [
        WorkspaceAction::Inspect,
        WorkspaceAction::Configure,
        WorkspaceAction::Author,
        WorkspaceAction::StartRun,
        WorkspaceAction::Dispatch,
    ] {
        for actor in [&organization_owner, &outsider] {
            assert!(
                matches!(
                    module.check(actor, workspace.id(), action).await,
                    Err(Error::WorkspaceNotFound)
                ),
                "{action:?} by {}",
                actor.user_id()
            );
        }
    }
    assert!(matches!(
        module
            .update_display_name(
                &change(organization_owner.clone(), 20, '2'),
                workspace.id(),
                WorkspaceDisplayName::parse("Renamed").expect("display name"),
            )
            .await,
        Err(Error::WorkspaceNotFound)
    ));
    assert!(matches!(
        module
            .list_memberships(&organization_owner, workspace.id())
            .await,
        Err(Error::WorkspaceNotFound)
    ));
    assert!(matches!(
        module
            .list_lifecycle_events(&organization_owner, workspace.id())
            .await,
        Err(Error::WorkspaceNotFound)
    ));
    assert!(
        module
            .list(&organization_owner)
            .await
            .expect("list own Workspaces")
            .iter()
            .all(|found| found.id() != workspace.id()),
        "a Workspace without a Membership is not one of the Owner's Workspaces"
    );
    assert_eq!(
        module
            .list_lifecycle_events(&creator, workspace.id())
            .await
            .expect("list Events"),
        events_before,
        "denied actions record nothing"
    );
    assert_eq!(
        module
            .check(&creator, workspace.id(), WorkspaceAction::Inspect)
            .await
            .expect("creator inspects")
            .workspace()
            .display_name()
            .as_str(),
        "Members only"
    );
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one journey covers the Organization Owner's whole governance surface"
)]
async fn organization_owner_lists_workspaces_assigns_owners_archives_and_restores() {
    let (database, module) = test_module().await;
    let module = &module;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let user = |name: &str, role| {
        let id = format!("user:{name}:{suffix}");
        async move { organization_user(module.database.clone(), &id, role).await }
    };
    let creator = user("creator", OrganizationRole::Member).await;
    let member = user("member", OrganizationRole::Member).await;
    let disabled = user("disabled", OrganizationRole::Member).await;
    let organization_owner = user("organization-owner", OrganizationRole::Owner).await;
    let workspace = module
        .create(
            &change(creator.clone(), 10, '1'),
            WorkspaceDisplayName::parse("Governed").expect("display name"),
        )
        .await
        .expect("create Workspace");
    module
        .set_membership(
            &change(creator.clone(), 11, '2'),
            workspace.id(),
            &member,
            WorkspaceRole::Member,
        )
        .await
        .expect("add Member");
    crate::OrganizationModule::new(module.database.clone())
        .set_enabled(
            &change(organization_owner.clone(), 12, '3'),
            disabled.user_id(),
            1,
            false,
        )
        .await
        .expect("disable User");
    let owners = |listed: &[crate::OrganizationWorkspace]| {
        let mut owners = listed
            .iter()
            .find(|found| found.workspace.id() == workspace.id())
            .expect("Workspace is listed")
            .owners
            .iter()
            .map(|owner| (owner.profile.user_id.clone(), owner.enabled))
            .collect::<Vec<_>>();
        owners.sort();
        owners
    };
    let events = || async {
        module
            .list_lifecycle_events(&creator, workspace.id())
            .await
            .expect("list Events")
    };

    assert_eq!(
        owners(
            &module
                .list_organization_workspaces(&organization_owner)
                .await
                .expect("Organization Owner lists every Workspace")
        ),
        [(creator.user_id().clone(), true)]
    );
    // Listing and assignment need Organization Owner authority, not Workspace ownership.
    assert!(matches!(
        module.list_organization_workspaces(&creator).await,
        Err(Error::PermissionDenied(_))
    ));
    let before = events().await;
    let rejected = [
        (
            creator.clone(),
            member.user_id(),
            "Workspace Owner is not an Organization Owner",
        ),
        (
            organization_owner.clone(),
            disabled.user_id(),
            "target is disabled",
        ),
    ];
    for (actor, target, case) in rejected {
        let result = module
            .assign_owner(&change(actor, 20, '4'), workspace.id(), target)
            .await;
        assert!(
            matches!(
                (case, &result),
                (
                    "Workspace Owner is not an Organization Owner",
                    Err(Error::PermissionDenied(_))
                ) | ("target is disabled", Err(Error::OwnerTargetNotEnabled))
            ),
            "{case}: {result:?}"
        );
    }
    assert_eq!(
        events().await,
        before,
        "rejected assignments change nothing"
    );
    assert_eq!(
        owners(
            &module
                .list_organization_workspaces(&organization_owner)
                .await
                .expect("list Workspaces")
        ),
        [(creator.user_id().clone(), true)]
    );

    // Each assignment: a new Owner, a promoted Member, then an unchanged Owner.
    let assignments = [
        (
            organization_owner.user_id(),
            Some(LifecycleAction::MembershipCreated),
        ),
        (
            member.user_id(),
            Some(LifecycleAction::MembershipRoleUpdated),
        ),
        (member.user_id(), None),
    ];
    for (target, expected) in assignments {
        let before = events().await;
        module
            .assign_owner(
                &change(organization_owner.clone(), 30, '5'),
                workspace.id(),
                target,
            )
            .await
            .expect("assign Owner");
        let after = events().await;
        let recorded = after
            .iter()
            .filter(|event| !before.contains(event))
            .collect::<Vec<_>>();
        assert_eq!(
            recorded
                .iter()
                .map(|event| event.action())
                .collect::<Vec<_>>(),
            expected.into_iter().collect::<Vec<_>>()
        );
        for event in recorded {
            assert_eq!(event.authority_source(), AuthoritySource::OrganizationOwner);
            assert_eq!(event.actor_user_id(), organization_owner.user_id());
            assert_eq!(event.target_id(), target.as_str());
        }
    }
    let evidence = module
        .check(
            &organization_owner,
            workspace.id(),
            WorkspaceAction::Configure,
        )
        .await
        .expect("assigned Owner configures the Workspace");
    assert_eq!(
        evidence.authority_source(),
        AuthoritySource::WorkspaceMembership
    );

    // Archive and restore need no Membership; assignment needs an active Workspace.
    let governor = user("governor", OrganizationRole::Owner).await;
    let before = events().await;
    // The Member-only `archive` conceals the Workspace from a non-Member governor.
    assert!(matches!(
        module
            .archive(&change(governor.clone(), 39, '6'), workspace.id())
            .await,
        Err(Error::WorkspaceNotFound)
    ));
    // Governance is the Organization Owner's: a Workspace Owner uses `archive`.
    assert!(matches!(
        module
            .archive_as_organization_owner(&change(creator.clone(), 39, '6'), workspace.id())
            .await,
        Err(Error::PermissionDenied(_))
    ));
    assert_eq!(
        events().await,
        before,
        "a refused governance action changes nothing"
    );
    assert_eq!(
        module
            .check(&creator, workspace.id(), WorkspaceAction::Inspect)
            .await
            .expect("inspect")
            .workspace()
            .state(),
        WorkspaceState::Active
    );
    module
        .archive_as_organization_owner(&change(governor.clone(), 40, '6'), workspace.id())
        .await
        .expect("Organization Owner archives without a Membership");
    assert!(matches!(
        module
            .assign_owner(
                &change(governor.clone(), 41, '7'),
                workspace.id(),
                governor.user_id(),
            )
            .await,
        Err(Error::WorkspaceArchived)
    ));
    module
        .restore_as_organization_owner(&change(governor.clone(), 42, '8'), workspace.id())
        .await
        .expect("Organization Owner restores without a Membership");
    let after = events().await;
    let recorded = after
        .iter()
        .filter(|event| !before.contains(event))
        .map(|event| (event.action(), event.authority_source()))
        .collect::<Vec<_>>();
    assert_eq!(recorded.len(), 2);
    for expected in [
        LifecycleAction::WorkspaceArchived,
        LifecycleAction::WorkspaceRestored,
    ] {
        assert!(recorded.contains(&(expected, AuthoritySource::OrganizationOwner)));
    }
    assert!(matches!(
        module
            .check(&governor, workspace.id(), WorkspaceAction::Inspect)
            .await,
        Err(Error::WorkspaceNotFound)
    ));
    // An earlier version recorded this source for anything an Organization Owner
    // did inside a Workspace. Those Events are not governance.
    database
        .execute_fixture_sql(&format!(
            "INSERT INTO workspace_lifecycle_events (id, workspace_id, actor_user_id, \
             authority_source, action, target_kind, target_id, outcome, changed_field_paths, \
             occurred_at_unix_nanos, trace_id) VALUES ('legacy-{suffix}', '{}', '{}', \
             'organization_owner', 'agent.created', 'agent', 'agent:legacy', 'accepted', \
             '[\"state\"]', 5, '{}')",
            workspace.id(),
            organization_owner.user_id(),
            "1".repeat(32),
        ))
        .await
        .expect("insert a legacy Event");
    // The governor reads the governance Events without a Membership, and no others.
    let governance = module
        .list_governance_events(&governor)
        .await
        .expect("Organization Owner reads governance Events")
        .into_iter()
        .filter(|event| event.workspace_id() == workspace.id())
        .collect::<Vec<_>>();
    let expected = events()
        .await
        .into_iter()
        .filter(|event| {
            event.authority_source() == AuthoritySource::OrganizationOwner
                && event.action() != LifecycleAction::AgentCreated
        })
        .collect::<Vec<_>>();
    assert_eq!(governance, expected);
    assert!(
        governance
            .iter()
            .any(|event| event.action() == LifecycleAction::WorkspaceArchived)
            && governance
                .iter()
                .all(|event| event.action() != LifecycleAction::WorkspaceCreated)
    );
    assert!(matches!(
        module.list_governance_events(&creator).await,
        Err(Error::PermissionDenied(_))
    ));
    assert!(matches!(
        module
            .archive(
                &change(outsider_member(module, &suffix).await, 43, '9'),
                workspace.id()
            )
            .await,
        Err(Error::WorkspaceNotFound)
    ));
}

async fn outsider_member(module: &WorkspaceModule, suffix: &str) -> crate::AuthenticatedUser {
    organization_user(
        module.database.clone(),
        &format!("user:outsider:{suffix}"),
        OrganizationRole::Member,
    )
    .await
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one table covers leave, removal, and demotion of the last designated Owner"
)]
async fn the_last_designated_owner_cannot_leave_be_removed_or_be_demoted() {
    let (_database, module) = test_module().await;
    let module = &module;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let user = |name: &str, role| {
        let id = format!("user:{name}:{suffix}");
        async move { organization_user(module.database.clone(), &id, role).await }
    };
    let owner = user("owner", OrganizationRole::Member).await;
    let second = user("second", OrganizationRole::Member).await;
    let organization_owner = user("organization-owner", OrganizationRole::Owner).await;
    let workspace = module
        .create(
            &change(owner.clone(), 10, '1'),
            WorkspaceDisplayName::parse("Always owned").expect("display name"),
        )
        .await
        .expect("create Workspace");
    let snapshot = || async {
        (
            module
                .list_memberships(&owner, workspace.id())
                .await
                .expect("list Memberships"),
            module
                .list_lifecycle_events(&owner, workspace.id())
                .await
                .expect("list Events"),
        )
    };

    let before = snapshot().await;
    assert!(matches!(
        remove_membership_authority(
            module,
            &change(owner.clone(), 20, '2'),
            workspace.id(),
            owner.user_id(),
        )
        .await,
        Err(Error::LastWorkspaceOwner)
    ));
    assert!(matches!(
        module
            .set_membership(
                &change(owner.clone(), 21, '3'),
                workspace.id(),
                &owner,
                WorkspaceRole::Member,
            )
            .await,
        Err(Error::LastWorkspaceOwner)
    ));
    assert_eq!(snapshot().await, before, "denials change nothing");

    // A second Owner removing the first is the same guard seen from the other side.
    module
        .set_membership(
            &change(owner.clone(), 30, '4'),
            workspace.id(),
            &second,
            WorkspaceRole::Owner,
        )
        .await
        .expect("add a second Owner");
    // A disabled Owner is still an Owner on record, so the other Owner may leave.
    crate::OrganizationModule::new(module.database.clone())
        .set_enabled(
            &change(organization_owner.clone(), 31, '5'),
            second.user_id(),
            1,
            false,
        )
        .await
        .expect("disable the second Owner");
    assert_eq!(
        module
            .list_owners(&owner, workspace.id())
            .await
            .expect("list Owners")
            .iter()
            .filter(|listed| !listed.enabled)
            .map(|listed| listed.profile.user_id.clone())
            .collect::<Vec<_>>(),
        [second.user_id().clone()],
        "a disabled Owner stays listed, marked as disabled"
    );
    let (_, _, enabled) = module
        .inspect_membership(&owner, workspace.id(), second.user_id())
        .await
        .expect("inspect the disabled Owner's Membership");
    assert!(
        !enabled,
        "an exact Membership reports its holder as disabled"
    );
    // The guard counts the disabled Owner: the enabled one may step down. The
    // Workspace then has no enabled Owner until an Organization Owner assigns one.
    module
        .set_membership(
            &change(owner.clone(), 32, '6'),
            workspace.id(),
            &owner,
            WorkspaceRole::Member,
        )
        .await
        .expect("an Owner steps down while a disabled Owner is on record");
    // Disablement keeps the Membership, and other Principals keep working.
    module
        .check(&owner, workspace.id(), WorkspaceAction::StartRun)
        .await
        .expect("a Member still starts Runs while the only Owner is disabled");
    assert_eq!(
        module
            .list_organization_workspaces(&organization_owner)
            .await
            .expect("list Workspaces")
            .iter()
            .find(|found| found.workspace.id() == workspace.id())
            .expect("Workspace is listed")
            .owners
            .iter()
            .map(|listed| (listed.profile.user_id.clone(), listed.enabled))
            .collect::<Vec<_>>(),
        [(second.user_id().clone(), false)]
    );
    module
        .assign_owner(
            &change(organization_owner.clone(), 33, '6'),
            workspace.id(),
            owner.user_id(),
        )
        .await
        .expect("the Organization Owner assigns an Owner again");
    crate::OrganizationModule::new(module.database.clone())
        .set_enabled(
            &change(organization_owner.clone(), 34, '6'),
            second.user_id(),
            2,
            true,
        )
        .await
        .expect("enable the second Owner");

    // Two Owners leave at once: the guard lets exactly one go.
    let first_context = change(owner.clone(), 40, '7');
    let second_context = change(second.clone(), 40, '8');
    let (first_left, second_left) = tokio::join!(
        remove_membership_authority(module, &first_context, workspace.id(), owner.user_id()),
        remove_membership_authority(module, &second_context, workspace.id(), second.user_id()),
    );
    assert_ne!(first_left.is_ok(), second_left.is_ok());
    for result in [first_left, second_left] {
        assert!(matches!(result, Ok(()) | Err(Error::LastWorkspaceOwner)));
    }
    let listed = module
        .list_organization_workspaces(&organization_owner)
        .await
        .expect("list Workspaces");
    assert_eq!(
        listed
            .iter()
            .find(|found| found.workspace.id() == workspace.id())
            .expect("Workspace is listed")
            .owners
            .len(),
        1
    );
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "shared role matrix includes real Organization provisioning and disablement"
)]
async fn ordinary_workspace_roles_require_current_organization_membership() {
    let (_database, module) = test_module().await;
    let module = &module;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let owner = organization_user(
        module.database.clone(),
        &format!("user:owner:{suffix}"),
        OrganizationRole::Member,
    )
    .await;
    let member = organization_user(
        module.database.clone(),
        &format!("user:member:{suffix}"),
        OrganizationRole::Member,
    )
    .await;
    let outsider = organization_user(
        module.database.clone(),
        &format!("user:outsider:{suffix}"),
        OrganizationRole::Member,
    )
    .await;
    let workspace = module
        .create(
            &change(owner.clone(), 100, '3'),
            WorkspaceDisplayName::parse("Ordinary roles").expect("display name"),
        )
        .await
        .expect("create Workspace");
    module
        .set_membership(
            &change(owner.clone(), 110, '4'),
            workspace.id(),
            &member,
            WorkspaceRole::Member,
        )
        .await
        .expect("grant Member role");

    for action in [
        WorkspaceAction::Inspect,
        WorkspaceAction::StartRun,
        WorkspaceAction::Dispatch,
    ] {
        let evidence = module
            .check(&member, workspace.id(), action)
            .await
            .unwrap_or_else(|error| panic!("Member performs {action:?}: {error}"));
        assert_eq!(evidence.effective_role(), WorkspaceRole::Member);
        assert_eq!(
            evidence.authority_source(),
            AuthoritySource::WorkspaceMembership
        );
    }
    assert!(matches!(
        module
            .check(&member, workspace.id(), WorkspaceAction::Configure)
            .await,
        Err(Error::PermissionDenied(_))
    ));
    assert!(matches!(
        module
            .check(&outsider, workspace.id(), WorkspaceAction::Inspect)
            .await,
        Err(Error::WorkspaceNotFound)
    ));

    let departed =
        crate::AuthenticatedUser::without_organization_membership(member.user_id().clone());
    let administrator = organization_user(
        module.database.clone(),
        "user:administrator",
        OrganizationRole::Owner,
    )
    .await;
    crate::OrganizationModule::new(module.database.clone())
        .set_enabled(&change(administrator, 120, '5'), member.user_id(), 1, false)
        .await
        .unwrap();
    assert!(matches!(
        module
            .check(&departed, workspace.id(), WorkspaceAction::Inspect)
            .await,
        Err(Error::WorkspaceNotFound)
    ));
    assert!(matches!(
        module.list(&departed).await,
        Err(Error::PermissionDenied("User is disabled"))
    ));

    let target_without_organization = crate::AuthenticatedUser::without_organization_membership(
        UserId::parse(&format!("user:departed-target:{suffix}")).expect("User id"),
    );
    let event_count = module
        .list_lifecycle_events(&owner, workspace.id())
        .await
        .expect("list Events")
        .len();
    assert!(matches!(
        module
            .set_membership(
                &change(owner.clone(), 120, '5'),
                workspace.id(),
                &target_without_organization,
                WorkspaceRole::Member,
            )
            .await,
        Err(Error::PermissionDenied(_))
    ));
    assert_eq!(
        module
            .list_lifecycle_events(&owner, workspace.id())
            .await
            .expect("list Events after rejection")
            .len(),
        event_count,
        "authorization rejection must not write an Administrative Lifecycle Event"
    );
}

#[tokio::test]
async fn archive_blocks_configuration_runs_and_dispatch_until_restoration() {
    let (_database, module) = test_module().await;
    let module = &module;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let owner = organization_user(
        module.database.clone(),
        &format!("user:archive-owner:{suffix}"),
        OrganizationRole::Member,
    )
    .await;
    let member = organization_user(
        module.database.clone(),
        &format!("user:archive-member:{suffix}"),
        OrganizationRole::Member,
    )
    .await;
    let workspace = module
        .create(
            &change(owner.clone(), 200, '6'),
            WorkspaceDisplayName::parse("Archive contract").expect("display name"),
        )
        .await
        .expect("create Workspace");
    module
        .set_membership(
            &change(owner.clone(), 210, '7'),
            workspace.id(),
            &member,
            WorkspaceRole::Member,
        )
        .await
        .expect("grant Member");

    let archived = module
        .archive(&change(owner.clone(), 220, '8'), workspace.id())
        .await
        .expect("archive Workspace");
    assert_eq!(archived.state(), WorkspaceState::Archived);
    assert_eq!(archived.lifecycle_version(), 2);
    module
        .check(&member, workspace.id(), WorkspaceAction::Inspect)
        .await
        .expect("archived Workspace remains inspectable");
    for actor in [&owner, &member] {
        for action in [
            WorkspaceAction::Configure,
            WorkspaceAction::StartRun,
            WorkspaceAction::Dispatch,
        ] {
            let result = module.check(actor, workspace.id(), action).await;
            if action == WorkspaceAction::Configure && actor == &member {
                assert!(matches!(result, Err(Error::PermissionDenied(_))));
            } else {
                assert!(matches!(result, Err(Error::WorkspaceArchived)));
            }
        }
    }

    assert_archived_mutations_rejected(module, &owner, &member, workspace.id()).await;

    let restored = module
        .restore(&change(owner.clone(), 270, 'd'), workspace.id())
        .await
        .expect("restore Workspace");
    assert_eq!(restored.state(), WorkspaceState::Active);
    assert_eq!(restored.lifecycle_version(), 3);
    module
        .check(&member, workspace.id(), WorkspaceAction::StartRun)
        .await
        .expect("restoration permits new Runs");
    module
        .check(&member, workspace.id(), WorkspaceAction::Dispatch)
        .await
        .expect("restoration permits later dispatch");

    let actions = module
        .list_lifecycle_events(&owner, workspace.id())
        .await
        .expect("list Events")
        .into_iter()
        .map(|event| event.action())
        .collect::<Vec<_>>();
    assert_eq!(actions.len(), 5);
    assert_eq!(
        actions
            .iter()
            .filter(|action| **action == LifecycleAction::MembershipCreated)
            .count(),
        2,
        "creation records the initial Owner Membership and the later Member grant"
    );
    for expected in [
        LifecycleAction::WorkspaceCreated,
        LifecycleAction::WorkspaceArchived,
        LifecycleAction::WorkspaceRestored,
    ] {
        assert_eq!(
            actions.iter().filter(|action| **action == expected).count(),
            1,
            "expected one {expected:?} Event"
        );
    }
}

#[tokio::test]
async fn local_identity_owns_the_workspaces_it_creates_through_its_membership() {
    let (_database, module) = test_module().await;
    let local = local_organization_owner();
    crate::OrganizationModule::new(module.database.clone())
        .provision_login(&change(local.clone(), 1, '1'))
        .await
        .unwrap();
    let workspace = module
        .create(
            &change(local.clone(), 10, '1'),
            WorkspaceDisplayName::parse("Local fixture").expect("display name"),
        )
        .await
        .expect("create Workspace");

    let evidence = module
        .check(&local, workspace.id(), WorkspaceAction::Configure)
        .await
        .expect("local User configures its own Workspace");

    assert_eq!(
        evidence.authority_source(),
        AuthoritySource::WorkspaceMembership
    );
    assert_eq!(evidence.effective_role(), WorkspaceRole::Owner);
}

async fn assert_archived_mutations_rejected(
    module: &WorkspaceModule,
    owner: &crate::AuthenticatedUser,
    member: &crate::AuthenticatedUser,
    workspace_id: &crate::WorkspaceId,
) {
    let events_before_rejections = module
        .list_lifecycle_events(owner, workspace_id)
        .await
        .expect("list Events")
        .len();
    assert!(matches!(
        module
            .update_display_name(
                &change(owner.clone(), 230, '9'),
                workspace_id,
                WorkspaceDisplayName::parse("Blocked rename").expect("display name"),
            )
            .await,
        Err(Error::WorkspaceArchived)
    ));
    assert!(matches!(
        module
            .set_membership(
                &change(owner.clone(), 240, 'a'),
                workspace_id,
                member,
                WorkspaceRole::Owner,
            )
            .await,
        Err(Error::WorkspaceArchived)
    ));
    assert!(matches!(
        module
            .archive(&change(owner.clone(), 250, 'b'), workspace_id)
            .await,
        Err(Error::WorkspaceAlreadyArchived)
    ));
    assert!(matches!(
        module
            .restore(&change(member.clone(), 260, 'c'), workspace_id)
            .await,
        Err(Error::PermissionDenied(_))
    ));
    assert_eq!(
        module
            .list_lifecycle_events(owner, workspace_id)
            .await
            .expect("list Events after rejections")
            .len(),
        events_before_rejections,
        "rejected archived-state changes must write no Event"
    );
}

#[tokio::test]
async fn agent_creation_returns_one_credential_and_safe_durable_attribution() {
    let (_database, module) = test_module().await;
    let owner = organization_user(
        module.database.clone(),
        "user:agent-creator",
        OrganizationRole::Member,
    )
    .await;
    let context = change(owner.clone(), 10, '1');
    let workspace = module
        .create(&context, WorkspaceDisplayName::parse("Agents").unwrap())
        .await
        .unwrap();
    let before = module
        .list_lifecycle_events(&owner, workspace.id())
        .await
        .unwrap();
    let issued = module
        .create_agent(
            &context,
            workspace.id(),
            crate::AgentDisplayName::parse("Worker").unwrap(),
            None,
        )
        .await
        .expect("create Agent, Registration, and credential together");
    let actor = module
        .authenticate_agent(workspace.id(), issued.credential.expose(), 11)
        .await
        .unwrap();
    assert_eq!(actor.principal(), issued.agent.principal());
    assert_eq!(actor.agent_workspace_id(), Some(workspace.id()));
    assert_eq!(issued.agent.created_by(), owner.user_id());
    assert_eq!(issued.registration.agent_id(), actor.principal().id());
    assert_eq!(
        module
            .list_agents(&owner, workspace.id())
            .await
            .unwrap()
            .as_slice(),
        std::slice::from_ref(&issued.agent)
    );
    assert_eq!(
        module
            .get_agent_registration(&owner, workspace.id(), issued.agent.principal().id())
            .await
            .unwrap(),
        issued.registration
    );
    let events = module
        .list_lifecycle_events(&owner, workspace.id())
        .await
        .unwrap();
    assert_eq!(
        new_actions(&before, &events),
        sorted_actions([
            LifecycleAction::AgentCreated,
            LifecycleAction::AgentRegistrationCreated,
        ])
    );
    assert!(!format!("{events:?} {issued:?} {actor:?}").contains(issued.credential.expose()));
    assert!(matches!(
        authenticate_both(&module, workspace.id(), "invalid-secret-canary", 11).await,
        Err(Error::InvalidAgentCredential)
    ));
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one table covers what each User is offered and what each can pick"
)]
async fn sign_in_choices_are_active_memberships_with_their_role_and_only_they_can_be_picked() {
    let (_database, module) = test_module().await;
    let owner = organization_user(
        module.database.clone(),
        "user:choice-owner",
        OrganizationRole::Member,
    )
    .await;
    let member = organization_user(
        module.database.clone(),
        "user:choice-member",
        OrganizationRole::Member,
    )
    .await;
    let governor = organization_user(
        module.database.clone(),
        "user:choice-governor",
        OrganizationRole::Owner,
    )
    .await;
    let context = change(owner.clone(), 10, '1');
    let create = async |name| {
        module
            .create(&context, WorkspaceDisplayName::parse(name).unwrap())
            .await
            .unwrap()
    };
    let beta = create("Beta").await;
    let alpha = create("Alpha").await;
    create("alpha two").await;
    let archived = create("Archived").await;
    module.archive(&context, archived.id()).await.unwrap();
    module
        .set_membership(&context, beta.id(), &member, WorkspaceRole::Member)
        .await
        .unwrap();

    let choices = async |actor: &crate::AuthenticatedUser| {
        module
            .list_sign_in_choices(actor.user_id())
            .await
            .unwrap()
            .iter()
            .map(|choice| {
                (
                    choice.workspace().display_name().as_str().to_owned(),
                    choice.role(),
                )
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(
        choices(&owner).await,
        [
            ("Alpha".to_owned(), WorkspaceRole::Owner),
            ("alpha two".to_owned(), WorkspaceRole::Owner),
            ("Beta".to_owned(), WorkspaceRole::Owner),
        ],
        "archived Workspaces are not offered, and choices sort by name, whatever the case"
    );
    assert_eq!(
        module
            .list(&owner)
            .await
            .unwrap()
            .iter()
            .map(|workspace| workspace.display_name().as_str())
            .collect::<Vec<_>>(),
        ["Alpha", "alpha two", "Archived", "Beta"],
        "the Workspace list sorts the same way, archived ones included"
    );
    assert_eq!(
        choices(&member).await,
        [("Beta".to_owned(), WorkspaceRole::Member)]
    );
    assert_eq!(
        choices(&governor).await,
        [],
        "Organization Owner governance is not a Membership"
    );

    let picked = module
        .sign_in_choice(member.user_id(), beta.id())
        .await
        .unwrap();
    assert_eq!(
        (picked.workspace().id(), picked.role()),
        (beta.id(), WorkspaceRole::Member)
    );
    let absent = crate::WorkspaceId::generate();
    for (case, actor, workspace) in [
        ("no Membership", &member, alpha.id()),
        ("archived", &owner, archived.id()),
        ("governance only", &governor, alpha.id()),
        ("absent", &owner, &absent),
    ] {
        assert!(
            matches!(
                module.sign_in_choice(actor.user_id(), workspace).await,
                Err(Error::WorkspaceNotFound)
            ),
            "{case}"
        );
    }
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one lifecycle scenario follows rotation, expiry, restoration, and disablement"
)]
async fn rotation_preserves_identity_and_lifecycle_loss_blocks_stale_authority() {
    let (_database, module) = test_module().await;
    let owner = organization_user(
        module.database.clone(),
        "user:lifecycle-owner",
        OrganizationRole::Member,
    )
    .await;
    let context = change(owner.clone(), 10, '1');
    let workspace = module
        .create(&context, WorkspaceDisplayName::parse("Lifecycle").unwrap())
        .await
        .unwrap();
    let before = module
        .list_lifecycle_events(&owner, workspace.id())
        .await
        .unwrap();
    let issued = module
        .create_agent(
            &context,
            workspace.id(),
            crate::AgentDisplayName::parse("Worker").unwrap(),
            Some(100),
        )
        .await
        .unwrap();
    let actor = authenticate_both(&module, workspace.id(), issued.credential.expose(), 11)
        .await
        .unwrap();
    let rotated = module
        .rotate_agent_credential(
            &change(owner.clone(), 20, '2'),
            workspace.id(),
            issued.registration.id(),
        )
        .await
        .unwrap();
    assert_eq!(rotated.agent, issued.agent);
    assert_eq!(rotated.registration, issued.registration);
    assert!(matches!(
        authenticate_both(&module, workspace.id(), issued.credential.expose(), 21).await,
        Err(Error::InvalidAgentCredential)
    ));
    assert_eq!(
        authenticate_both(&module, workspace.id(), rotated.credential.expose(), 21)
            .await
            .unwrap(),
        actor
    );
    module
        .check_run_admission(&actor, workspace.id(), 99)
        .await
        .unwrap();
    assert!(matches!(
        module
            .check_run_admission(&actor, workspace.id(), 100)
            .await,
        Err(Error::AgentRegistrationExpired)
    ));
    assert!(matches!(
        authenticate_both(&module, workspace.id(), rotated.credential.expose(), 100).await,
        Err(Error::InvalidAgentCredential)
    ));
    module
        .update_agent_registration(
            &context,
            workspace.id(),
            issued.registration.id(),
            true,
            None,
        )
        .await
        .unwrap();
    module
        .check_run_admission(&actor, workspace.id(), 101)
        .await
        .unwrap();
    module
        .set_agent_state(
            &context,
            workspace.id(),
            actor.principal().id(),
            crate::AgentState::Disabled,
        )
        .await
        .unwrap();
    assert!(matches!(
        module
            .check_run_admission(&actor, workspace.id(), 101)
            .await,
        Err(Error::AgentDisabled)
    ));
    module
        .set_agent_state(
            &context,
            workspace.id(),
            actor.principal().id(),
            crate::AgentState::Enabled,
        )
        .await
        .unwrap();
    module
        .update_agent_registration(
            &context,
            workspace.id(),
            issued.registration.id(),
            false,
            None,
        )
        .await
        .unwrap();
    assert!(matches!(
        module
            .check_run_admission(&actor, workspace.id(), 101)
            .await,
        Err(Error::AgentDisabled)
    ));
    assert!(matches!(
        authenticate_both(&module, workspace.id(), rotated.credential.expose(), 101).await,
        Err(Error::InvalidAgentCredential)
    ));
    assert_eq!(
        module
            .list_agents(&owner, workspace.id())
            .await
            .unwrap()
            .len(),
        1
    );
    let events = module
        .list_lifecycle_events(&owner, workspace.id())
        .await
        .unwrap();
    assert_eq!(
        new_actions(&before, &events),
        sorted_actions([
            LifecycleAction::AgentCreated,
            LifecycleAction::AgentRegistrationCreated,
            LifecycleAction::AgentCredentialRotated,
            LifecycleAction::AgentRegistrationUpdated,
            LifecycleAction::AgentStateUpdated,
            LifecycleAction::AgentStateUpdated,
            LifecycleAction::AgentRegistrationUpdated,
        ]),
        "each accepted change records one Event; rejected credentials record none"
    );
    assert!(!format!("{events:?}").contains(rotated.credential.expose()));
}

/// Authenticates through the Workspace-scoped lookup and the credential-only
/// lookup of the Organization MCP Endpoint, which must give the same answer.
async fn authenticate_both(
    module: &WorkspaceModule,
    workspace: &crate::WorkspaceId,
    credential: &str,
    now_unix_nanos: i64,
) -> Result<crate::AuthenticatedPrincipal, Error> {
    let scoped = module
        .authenticate_agent(workspace, credential, now_unix_nanos)
        .await;
    let alone = module
        .authenticate_agent_credential(credential, now_unix_nanos)
        .await
        .map(|(principal, resolved)| {
            assert_eq!(&resolved, workspace);
            principal
        });
    assert_eq!(format!("{scoped:?}"), format!("{alone:?}"));
    scoped
}

/// Actions of the Events in `after` that `before` lacks, in a stable order.
/// Events that share a timestamp list in ID order, so compare them as a multiset.
fn new_actions(
    before: &[crate::AdministrativeLifecycleEvent],
    after: &[crate::AdministrativeLifecycleEvent],
) -> Vec<LifecycleAction> {
    sorted_actions(
        after
            .iter()
            .filter(|event| !before.contains(event))
            .map(crate::AdministrativeLifecycleEvent::action),
    )
}

fn sorted_actions(actions: impl IntoIterator<Item = LifecycleAction>) -> Vec<LifecycleAction> {
    let mut actions = actions.into_iter().collect::<Vec<_>>();
    actions.sort_by_key(|action| format!("{action:?}"));
    actions
}

async fn test_module() -> (TestDatabase, WorkspaceModule) {
    let fixture = TestDatabase::new(coral_db::migration_plan())
        .await
        .expect("create Workspace test database");
    let module = WorkspaceModule::new(fixture.database());
    (fixture, module)
}

async fn organization_user(
    database: std::sync::Arc<coral_db::Database>,
    id: &str,
    role: OrganizationRole,
) -> crate::AuthenticatedUser {
    let user =
        crate::AuthenticatedUser::organization_member(UserId::parse(id).expect("User id"), role);
    crate::OrganizationModule::new(database)
        .provision_login(&crate::ChangeContext::new(
            user.clone(),
            1,
            crate::TraceId::current_or_new(),
        ))
        .await
        .expect("provision test User");
    user
}

fn change(actor: crate::AuthenticatedUser, at: i64, trace_digit: char) -> ChangeContext {
    ChangeContext::new(
        actor,
        at,
        TraceId::parse(&trace_digit.to_string().repeat(32)).expect("trace id"),
    )
}

async fn remove_membership_authority(
    module: &WorkspaceModule,
    context: &ChangeContext,
    workspace: &crate::WorkspaceId,
    user: &UserId,
) -> Result<(), Error> {
    let mut tx = module.database.begin().await?;
    let prepared = module
        .prepare_access_removal_in_transaction(
            &mut tx,
            context,
            crate::AccessRemovalTarget::WorkspaceMember(workspace, user),
        )
        .await?;
    module
        .apply_access_removal_in_transaction(&mut tx, context, prepared)
        .await?;
    tx.commit().await?;
    Ok(())
}

/// A database from before Workspace Membership became the only authority:
/// the backfill gives each enabled Organization Owner the designated Owner
/// Membership that replaces the authority they held.
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one upgrade covers each Membership state the backfill meets"
)]
async fn upgrade_makes_enabled_organization_owners_designated_owners_everywhere() {
    let before_backfill = coral_test_support::edited_migration_plan(|migrations| {
        migrations
            .into_iter()
            .filter(|migration| migration.version < 22)
            .collect()
    });
    let fixture = TestDatabase::new(&before_backfill)
        .await
        .expect("create database from before the backfill");
    let module = WorkspaceModule::new(fixture.database());
    let module = &module;
    let user = |name: &str, role| {
        let id = format!("user:{name}");
        async move { organization_user(module.database.clone(), &id, role).await }
    };
    let creator = user("creator", OrganizationRole::Member).await;
    let organization_owner = user("organization-owner", OrganizationRole::Owner).await;
    let disabled_owner = user("disabled-owner", OrganizationRole::Owner).await;
    crate::OrganizationModule::new(module.database.clone())
        .set_enabled(
            &change(organization_owner.clone(), 2, '1'),
            disabled_owner.user_id(),
            1,
            false,
        )
        .await
        .expect("disable an Organization Owner");
    let mut workspaces = Vec::new();
    for name in ["No Membership", "Member", "Left earlier"] {
        workspaces.push(
            module
                .create(
                    &change(creator.clone(), 10, '2'),
                    WorkspaceDisplayName::parse(name).expect("display name"),
                )
                .await
                .expect("create Workspace"),
        );
    }
    let [none, member, left] = workspaces.as_slice() else {
        unreachable!("three Workspaces were created");
    };
    for workspace in [member, left] {
        module
            .set_membership(
                &change(creator.clone(), 11, '3'),
                workspace.id(),
                &organization_owner,
                WorkspaceRole::Member,
            )
            .await
            .expect("add the Organization Owner as a Member");
    }
    remove_membership_authority(
        module,
        &change(creator.clone(), 12, '4'),
        left.id(),
        organization_owner.user_id(),
    )
    .await
    .expect("the Organization Owner leaves one Workspace");
    let (_, member_etag_before, _, _) = module
        .inspect_memberships(&creator, member.id())
        .await
        .expect("inspect Memberships")
        .into_iter()
        .find(|(membership, ..)| membership.user_id() == organization_owner.user_id())
        .expect("Member row");
    let mut events_before = Vec::new();
    for workspace in &workspaces {
        events_before.push(
            module
                .list_lifecycle_events(&creator, workspace.id())
                .await
                .expect("list Events"),
        );
    }

    fixture
        .database()
        .migrate(coral_db::migration_plan())
        .await
        .expect("apply the backfill");

    let expected = [
        (none, LifecycleAction::MembershipCreated),
        (member, LifecycleAction::MembershipRoleUpdated),
        (left, LifecycleAction::MembershipCreated),
    ];
    for ((workspace, action), before) in expected.into_iter().zip(events_before) {
        let name = workspace.display_name().as_str();
        let evidence = module
            .check(
                &organization_owner,
                workspace.id(),
                WorkspaceAction::Configure,
            )
            .await
            .unwrap_or_else(|error| panic!("{name}: Organization Owner keeps access: {error}"));
        assert_eq!(
            evidence.authority_source(),
            AuthoritySource::WorkspaceMembership,
            "{name}"
        );
        let memberships = module
            .inspect_memberships(&organization_owner, workspace.id())
            .await
            .unwrap_or_else(|error| {
                panic!("{name}: backfilled Memberships have metadata: {error}")
            });
        let mut roles = memberships
            .iter()
            .map(|(membership, ..)| (membership.user_id().clone(), membership.role()))
            .collect::<Vec<_>>();
        roles.sort_by(|left, right| left.0.cmp(&right.0));
        assert_eq!(
            roles,
            [
                (creator.user_id().clone(), WorkspaceRole::Owner),
                (organization_owner.user_id().clone(), WorkspaceRole::Owner),
            ],
            "{name}: the disabled Organization Owner gets no Membership"
        );
        let after = module
            .list_lifecycle_events(&creator, workspace.id())
            .await
            .expect("list Events");
        let recorded = after
            .iter()
            .filter(|event| !before.contains(event))
            .collect::<Vec<_>>();
        assert_eq!(recorded.len(), 1, "{name}");
        let event = recorded.first().expect("backfill Event");
        assert_eq!(event.action(), action, "{name}");
        assert_eq!(event.authority_source(), AuthoritySource::OrganizationOwner);
        assert_eq!(event.target_id(), organization_owner.user_id().as_str());
    }
    let (_, member_etag_after, _, _) = module
        .inspect_memberships(&creator, member.id())
        .await
        .expect("inspect Memberships")
        .into_iter()
        .find(|(membership, ..)| membership.user_id() == organization_owner.user_id())
        .expect("promoted row");
    assert_ne!(
        member_etag_before.etag, member_etag_after.etag,
        "a promotion invalidates the Membership's earlier etag"
    );
    // The backfilled Membership is ordinary: its holder can leave again, with
    // the etag a client reads for it as the concurrency precondition.
    let (_, backfilled, _) = module
        .inspect_membership(&organization_owner, none.id(), organization_owner.user_id())
        .await
        .expect("inspect the backfilled Membership");
    remove_membership_authority(
        module,
        &change(organization_owner.clone(), 30, '5')
            .with_etag(&backfilled.etag)
            .expect("a backfilled etag is a valid precondition"),
        none.id(),
        organization_owner.user_id(),
    )
    .await
    .expect("a backfilled Owner leaves a Workspace they do not want");
}
