use crate::{AccessLifecycleModule, Error};
use coral_functions::{
    SavedFunctionChangeContext, SavedFunctionDescription, SavedFunctionModule, SavedFunctionPath,
    SavedFunctionSave, SavedFunctionSource,
};
use coral_test_support::{TestDatabase, provision_user};
use coral_workspace::{
    AuthenticatedPrincipal, AuthenticatedUser, ChangeContext, OrganizationModule, OrganizationRole,
    WorkspaceDisplayName, WorkspaceModule, WorkspaceRole,
};

async fn fixture() -> TestDatabase {
    TestDatabase::new(coral_db::migration_plan()).await.unwrap()
}

fn change(user: &AuthenticatedUser) -> ChangeContext {
    coral_test_support::change(user.clone(), 100, '1')
}

enum StaleCommand {
    Remove,
    Role,
    Enabled,
}

#[expect(
    clippy::too_many_lines,
    reason = "one public offboarding and rejoin journey proves stale commands preserve fresh work"
)]
async fn stale_command_preserves_rejoined_membership_and_work(command: StaleCommand) {
    let db = fixture().await;
    let database = db.database();
    let organization = OrganizationModule::new(database.clone());
    let workspaces = WorkspaceModule::new(database.clone());
    let administration = AccessLifecycleModule::new(database.clone(), workspaces.clone());
    let owner = provision_user(database.clone(), "user:owner", OrganizationRole::Owner).await;
    let member = provision_user(database.clone(), "user:member", OrganizationRole::Member).await;
    let original = organization
        .provision_login(&change(&member))
        .await
        .unwrap();
    let workspace = workspaces
        .create(
            &change(&member),
            WorkspaceDisplayName::parse("Work").unwrap(),
        )
        .await
        .unwrap();
    // A second designated Owner, so removal needs no transfer target.
    workspaces
        .assign_owner(&change(&owner), workspace.id(), owner.user_id())
        .await
        .unwrap();
    administration
        .remove_organization_member(&change(&owner), member.user_id(), original.revision(), None)
        .await
        .unwrap();
    assert!(
        organization
            .resolve_user_authority(member.user_id())
            .await
            .is_err(),
        "removed Membership must have no current authority"
    );
    assert!(
        workspaces
            .list_memberships(&owner, workspace.id())
            .await
            .unwrap()
            .iter()
            .all(|membership| membership.user_id() != member.user_id())
    );
    assert!(matches!(
        administration
            .remove_organization_member(
                &change(&owner),
                member.user_id(),
                original.revision(),
                None
            )
            .await,
        Err(Error::Workspace(coral_workspace::Error::PermissionDenied(
            _
        )))
    ));
    let rejoined = organization
        .provision_login(&change(&member))
        .await
        .unwrap();
    let returned = organization
        .resolve_user_authority(member.user_id())
        .await
        .unwrap();
    workspaces
        .set_membership(
            &change(&owner),
            workspace.id(),
            &returned,
            WorkspaceRole::Member,
        )
        .await
        .unwrap();
    let author = AuthenticatedPrincipal::user(returned);
    let functions = SavedFunctionModule::new(
        database,
        workspaces.clone(),
        coral_functions::IsolateLimits::default(),
    )
    .unwrap();
    let path = SavedFunctionPath::parse("fresh").unwrap();
    let saved = functions
        .save(
            &SavedFunctionChangeContext::new(
                author.clone(),
                100,
                change(&owner).trace_id().clone(),
            ),
            workspace.id(),
            SavedFunctionSave::new(
                path.clone(),
                SavedFunctionDescription::parse("Fresh work after rejoining").unwrap(),
                SavedFunctionSource::parse("export default () => 42;").unwrap(),
            ),
        )
        .await
        .unwrap();
    let events_before = organization
        .list_lifecycle_events(&owner)
        .await
        .unwrap()
        .len();
    let result = match command {
        StaleCommand::Remove => {
            administration
                .remove_organization_member(
                    &change(&owner),
                    member.user_id(),
                    original.revision(),
                    None,
                )
                .await
        }
        StaleCommand::Role => organization
            .set_role(
                &change(&owner),
                member.user_id(),
                original.revision(),
                OrganizationRole::Owner,
            )
            .await
            .map(|_| ())
            .map_err(Error::Workspace),
        StaleCommand::Enabled => organization
            .set_enabled(
                &change(&owner),
                member.user_id(),
                original.revision(),
                false,
            )
            .await
            .map(|_| ())
            .map_err(Error::Workspace),
    };
    assert!(
        matches!(
            result,
            Err(Error::Workspace(
                coral_workspace::Error::OrganizationAuthorityConflict
            ))
        ),
        "old command must conflict after rejoin: {result:?}"
    );
    assert!(rejoined.revision() > original.revision());
    assert_eq!(
        organization
            .provision_login(&change(&member))
            .await
            .unwrap(),
        rejoined
    );
    assert_eq!(
        functions
            .get(&author, workspace.id(), &path)
            .await
            .unwrap()
            .id(),
        saved.id()
    );
    assert_eq!(
        functions
            .list_lifecycle_events(&author, workspace.id())
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        organization
            .list_lifecycle_events(&owner)
            .await
            .unwrap()
            .len(),
        events_before
    );
    assert!(
        functions
            .list_lifecycle_events(&AuthenticatedPrincipal::user(owner), workspace.id())
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn old_removal_cannot_delete_rejoined_membership_or_fresh_functions() {
    stale_command_preserves_rejoined_membership_and_work(StaleCommand::Remove).await;
}

#[tokio::test]
async fn old_role_change_cannot_promote_rejoined_membership() {
    stale_command_preserves_rejoined_membership_and_work(StaleCommand::Role).await;
}

#[tokio::test]
async fn old_disablement_cannot_disable_rejoined_membership() {
    stale_command_preserves_rejoined_membership_and_work(StaleCommand::Enabled).await;
}

#[tokio::test]
async fn removed_owner_is_absent_and_cannot_satisfy_last_owner_protection() {
    let db = fixture().await;
    let database = db.database();
    let organization = OrganizationModule::new(database.clone());
    let workspaces = WorkspaceModule::new(database.clone());
    let owner = provision_user(database.clone(), "user:owner", OrganizationRole::Owner).await;
    let departed = provision_user(database.clone(), "user:departed", OrganizationRole::Owner).await;
    let workspace = workspaces
        .create(
            &change(&owner),
            WorkspaceDisplayName::parse("Work").unwrap(),
        )
        .await
        .unwrap();
    let administration = AccessLifecycleModule::new(database, workspaces.clone());
    administration
        .remove_organization_member(&change(&owner), departed.user_id(), 1, None)
        .await
        .unwrap();
    assert!(organization.check_owner(&departed).await.is_err());
    let owners = workspaces
        .list_owners(&owner, workspace.id())
        .await
        .unwrap();
    assert_eq!(owners.len(), 1);
    assert_eq!(&owners.first().unwrap().profile.user_id, owner.user_id());
    assert!(matches!(
        organization
            .set_role(
                &change(&owner),
                departed.user_id(),
                1,
                OrganizationRole::Member
            )
            .await,
        Err(coral_workspace::Error::PermissionDenied(_))
    ));
    assert!(matches!(
        organization
            .set_enabled(&change(&owner), departed.user_id(), 1, true)
            .await,
        Err(coral_workspace::Error::PermissionDenied(_))
    ));
    assert!(matches!(
        organization
            .set_role(
                &change(&owner),
                owner.user_id(),
                1,
                OrganizationRole::Member
            )
            .await,
        Err(coral_workspace::Error::PermissionDenied(_))
    ));
    assert!(matches!(
        organization
            .set_enabled(&change(&owner), owner.user_id(), 1, false)
            .await,
        Err(coral_workspace::Error::PermissionDenied(_))
    ));
    assert!(matches!(
        administration
            .remove_organization_member(&change(&owner), owner.user_id(), 1, None)
            .await,
        Err(Error::Workspace(coral_workspace::Error::PermissionDenied(
            _
        )))
    ));
    let rejoined = organization
        .provision_login(&change(&AuthenticatedUser::organization_member(
            departed.user_id().clone(),
            OrganizationRole::Member,
        )))
        .await
        .unwrap();
    assert!(rejoined.revision() > 1);
    assert_eq!(rejoined.role(), OrganizationRole::Member);
    assert!(rejoined.enabled());
    assert!(organization.check_owner(&departed).await.is_err());
}
