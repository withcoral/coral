use crate::{
    AuthenticatedUser, ChangeContext, OrganizationModule, OrganizationRole, TraceId, UserId,
};
use coral_test_support::TestDatabase;

fn context(id: &str, role: OrganizationRole) -> ChangeContext {
    ChangeContext::new(
        AuthenticatedUser::organization_member(UserId::parse(id).unwrap(), role),
        1,
        TraceId::parse(&"a".repeat(32)).unwrap(),
    )
}

#[tokio::test]
async fn only_current_organization_owners_can_inspect_members_for_removal() {
    let fixture = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
    let organization = OrganizationModule::new(fixture.database());
    let owner = context("owner", OrganizationRole::Owner);
    let member = context("member", OrganizationRole::Member);
    organization.provision_login(&owner).await.unwrap();
    organization.provision_login(&member).await.unwrap();
    organization
        .list_memberships(member.actor())
        .await
        .unwrap_err();
    let (own, profile) = organization
        .inspect_current_membership(member.actor())
        .await
        .unwrap();
    assert_eq!(own.user_id(), member.actor().user_id());
    assert_eq!(&profile.user_id, member.actor().user_id());
    assert_eq!(own.role(), OrganizationRole::Member);
    let disabled = organization
        .set_enabled(&owner, member.actor().user_id(), 1, false)
        .await
        .unwrap();
    let members = organization.list_memberships(owner.actor()).await.unwrap();
    assert_eq!(members.len(), 2);
    let (membership, profile) = members
        .iter()
        .find(|(membership, _)| membership.user_id() == member.actor().user_id())
        .unwrap();
    organization
        .inspect_current_membership(member.actor())
        .await
        .unwrap_err();
    assert_eq!(membership, &disabled);
    assert_eq!(&profile.user_id, member.actor().user_id());
    organization
        .set_role(
            &owner,
            member.actor().user_id(),
            disabled.revision(),
            OrganizationRole::Owner,
        )
        .await
        .unwrap();
    organization
        .list_memberships(member.actor())
        .await
        .unwrap_err();
}

#[tokio::test]
async fn concurrent_administration_cannot_remove_the_last_enabled_owner() {
    let fixture = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
    let organization = OrganizationModule::new(fixture.database());
    let first = context("first", OrganizationRole::Owner);
    let second = context("second", OrganizationRole::Owner);
    organization.provision_login(&first).await.unwrap();
    organization
        .set_role(&first, first.actor().user_id(), 1, OrganizationRole::Member)
        .await
        .unwrap_err();
    organization
        .set_enabled(&first, first.actor().user_id(), 1, false)
        .await
        .unwrap_err();
    organization.provision_login(&second).await.unwrap();
    let (a, b) = tokio::join!(
        organization.set_enabled(&first, first.actor().user_id(), 1, false),
        organization.set_enabled(&second, second.actor().user_id(), 1, false),
    );
    assert_ne!(a.is_ok(), b.is_ok());
    let first_current = organization
        .resolve_user_authority(first.actor().user_id())
        .await;
    let second_current = organization
        .resolve_user_authority(second.actor().user_id())
        .await;
    assert_ne!(first_current.is_ok(), second_current.is_ok());
    let survivor = first_current.or(second_current).unwrap();
    let events = organization.list_lifecycle_events(&survivor).await.unwrap();
    assert_eq!(events.len(), 3, "rejected changes must not write events");
}

#[tokio::test]
async fn disabled_user_cannot_use_retained_owner_context_or_dispatch() {
    let fixture = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
    let organization = OrganizationModule::new(fixture.database());
    let workspaces = crate::WorkspaceModule::new(fixture.database());
    let administrator = context("administrator", OrganizationRole::Owner);
    let owner = context("owner", OrganizationRole::Owner);
    organization.provision_login(&administrator).await.unwrap();
    let membership = organization.provision_login(&owner).await.unwrap();
    let workspace = workspaces
        .create(
            &owner,
            crate::WorkspaceDisplayName::parse("Private").unwrap(),
        )
        .await
        .unwrap();
    organization
        .set_enabled(
            &administrator,
            owner.actor().user_id(),
            membership.revision(),
            false,
        )
        .await
        .unwrap();
    workspaces
        .create(
            &owner,
            crate::WorkspaceDisplayName::parse("Denied").unwrap(),
        )
        .await
        .unwrap_err();
    workspaces.list(owner.actor()).await.unwrap_err();
    workspaces
        .check(
            owner.actor(),
            workspace.id(),
            crate::WorkspaceAction::Inspect,
        )
        .await
        .unwrap_err();
    let principal = crate::AuthenticatedPrincipal::user(owner.actor().clone());
    workspaces
        .check_run_admission(&principal, workspace.id(), 1)
        .await
        .unwrap_err();
    let database = fixture.database();
    let mut tx = database.begin().await.unwrap();
    workspaces
        .check_dispatch_in_transaction(
            &mut tx,
            &principal,
            workspace.id(),
            1,
            coral_db::RowLock::Shared,
        )
        .await
        .unwrap_err();
}

#[tokio::test]
async fn login_preserves_coral_role_and_disablement() {
    let fixture = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
    let organization = OrganizationModule::new(fixture.database());
    let owner = context("owner", OrganizationRole::Owner);
    let member = context("member", OrganizationRole::Member);
    organization.provision_login(&owner).await.unwrap();
    let initial = organization.provision_login(&member).await.unwrap();
    let disabled = organization
        .set_enabled(&owner, member.actor().user_id(), initial.revision(), false)
        .await
        .unwrap();
    let replay = organization
        .provision_login(&context("member", OrganizationRole::Owner))
        .await
        .unwrap();
    assert_eq!(replay, disabled);
    assert_eq!(replay.role(), OrganizationRole::Member);
    assert!(!replay.enabled());
    organization
        .resolve_user_authority(member.actor().user_id())
        .await
        .unwrap_err();
    assert_eq!(
        organization
            .list_lifecycle_events(owner.actor())
            .await
            .unwrap()
            .len(),
        3
    );
}
