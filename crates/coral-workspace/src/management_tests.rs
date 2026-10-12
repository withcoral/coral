use super::{change, organization_user, test_module};
use crate::{Error, MembershipChange, OrganizationRole, WorkspaceDisplayName, WorkspaceRole};

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
    refresh(alice.user_id(), "Alice+tag@Example.test", 1).await;
    let request = change(owner.clone(), 20, '2')
        .with_request_id(&uuid::Uuid::new_v4().to_string())
        .unwrap();
    let granted = module
        .grant_membership_by_email(
            &request,
            workspace.id(),
            "  ALICE+tag@example.test ",
            WorkspaceRole::Member,
        )
        .await
        .unwrap();
    assert_eq!(granted.user_id(), alice.user_id());
    let duplicate = change(owner.clone(), 21, '3')
        .with_request_id(&uuid::Uuid::new_v4().to_string())
        .unwrap();
    assert!(matches!(
        module
            .grant_membership_by_email(
                &duplicate,
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
                workspace.id(),
                "different-operation@example.test",
                WorkspaceRole::Owner
            )
            .await,
        Err(Error::RequestAlreadyApplied)
    ));
    refresh(alice.user_id(), "different@example.test", 2).await;
    refresh(bob.user_id(), "alice+tag@example.test", 3).await;
    assert!(matches!(
        module
            .grant_membership_by_email(
                &request,
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
        1,
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();

    let mut tx = module.database.begin().await.unwrap();
    crate::OrganizationModule::hold_profile_updates_in_transaction(&mut tx)
        .await
        .unwrap();
    let request = change(owner.clone(), 20, '2')
        .with_request_id(&uuid::Uuid::new_v4().to_string())
        .unwrap();
    let mut grant = Box::pin(module.grant_membership_by_email(
        &request,
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
        2,
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
    assert_eq!((initial.created_at, initial.updated_at), (10, 10));

    module
        .update_display_name(
            &change(owner.clone(), 20, '2')
                .with_etag(&initial.etag)
                .unwrap(),
            first,
            WorkspaceDisplayName::parse("Renamed").unwrap(),
        )
        .await
        .unwrap();
    let (_, updated) = module.inspect_workspace(&owner, first).await.unwrap();
    assert_ne!(updated.etag, initial.etag);
    assert_eq!((updated.created_at, updated.updated_at), (10, 20));
    assert!(matches!(
        module
            .update_display_name(
                &change(owner.clone(), 30, '3')
                    .with_etag(&initial.etag)
                    .unwrap(),
                first,
                WorkspaceDisplayName::parse("Stale update").unwrap(),
            )
            .await,
        Err(Error::EtagMismatch)
    ));
    let (evidence, unchanged) = module.inspect_workspace(&owner, first).await.unwrap();
    assert_eq!(evidence.workspace().display_name().as_str(), "Renamed");
    assert_eq!(unchanged.etag, updated.etag);
    assert_eq!(unchanged.updated_at, 20);
    for (workspace, original) in [(first, first_member), (second, second_member)] {
        let (_, member, _) = module
            .inspect_membership(&owner, workspace, owner.user_id())
            .await
            .unwrap();
        assert_eq!(member.etag, original.etag);
        assert_eq!((member.created_at, member.updated_at), (10, 10));
    }
}

#[tokio::test]
async fn management_request_claim_rolls_back_with_rejection_and_rejects_committed_replay() {
    let (_database, module) = test_module().await;
    let owner = organization_user(
        module.database.clone(),
        "user:request-owner",
        OrganizationRole::Member,
    )
    .await;
    let member = organization_user(
        module.database.clone(),
        "user:request-member",
        OrganizationRole::Member,
    )
    .await;
    let workspace = module
        .create(
            &change(owner.clone(), 10, '1'),
            WorkspaceDisplayName::parse("Requests").unwrap(),
        )
        .await
        .unwrap();
    let context = change(owner.clone(), 20, '2')
        .with_request_id(&uuid::Uuid::new_v4().to_string())
        .unwrap();

    // The claim is inserted before the missing Membership is rejected.
    assert!(matches!(
        module
            .manage_membership(
                &context,
                workspace.id(),
                &member,
                MembershipChange::UpdateRole(WorkspaceRole::Owner),
            )
            .await,
        Err(Error::MembershipNotFound)
    ));
    module
        .set_membership(&context, workspace.id(), &member, WorkspaceRole::Member)
        .await
        .expect("rejected mutation must not consume its request identity");
    let (_, original, _) = module
        .inspect_membership(&owner, workspace.id(), member.user_id())
        .await
        .unwrap();
    let events = module
        .list_lifecycle_events(&owner, workspace.id())
        .await
        .unwrap();
    assert!(matches!(
        module
            .set_membership(&context, workspace.id(), &member, WorkspaceRole::Owner)
            .await,
        Err(Error::RequestAlreadyApplied)
    ));
    let (membership, metadata, _) = module
        .inspect_membership(&owner, workspace.id(), member.user_id())
        .await
        .unwrap();
    assert_eq!(membership.role(), WorkspaceRole::Member);
    assert_eq!(metadata.etag, original.etag);
    assert_eq!(
        module
            .list_lifecycle_events(&owner, workspace.id())
            .await
            .unwrap()
            .len(),
        events.len()
    );
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
        at,
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
    let request = change(actor.clone(), 30, '3')
        .with_request_id(&uuid::Uuid::new_v4().to_string())
        .unwrap();
    module
        .grant_membership_by_email(&request, workspace, email, WorkspaceRole::Member)
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
        Err(Error::PermissionDenied(_))
    ));
    module
        .archive(&change(owner.clone(), 60, '6'), workspace.id())
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
        .set_enabled(&context, alice.user_id(), 1, false)
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
