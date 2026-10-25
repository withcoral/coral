use super::{change, organization_user, test_module};
use crate::*;
use sqlx::SqlSafeStr as _;

#[tokio::test]
async fn inspection_reports_all_coral_access_blockers_without_clearing_them() {
    let (_database, module) = test_module().await;
    let owner = organization_user(
        module.database.clone(),
        "user:owner",
        OrganizationRole::Owner,
    )
    .await;
    let context = change(owner.clone(), 10, '1');
    let w = module
        .create(&context, WorkspaceDisplayName::parse("Access").unwrap())
        .await
        .unwrap();
    let issued = module
        .create_agent(
            &context,
            w.id(),
            AgentDisplayName::parse("Worker").unwrap(),
            Some(100),
        )
        .await
        .unwrap();
    let id = issued.agent.principal().id();
    assert!(
        module
            .inspect_agent(&owner, w.id(), id)
            .await
            .unwrap()
            .access_blockers(99)
            .is_empty()
    );
    module
        .set_agent_state(&context, w.id(), id, AgentState::Disabled)
        .await
        .unwrap();
    module
        .update_agent_registration(&context, w.id(), issued.registration.id(), false, Some(100))
        .await
        .unwrap();
    module.archive(&context, w.id()).await.unwrap();
    assert_eq!(
        module
            .inspect_agent(&owner, w.id(), id)
            .await
            .unwrap()
            .access_blockers(100),
        vec![
            AgentAccessBlocker::WorkspaceArchived,
            AgentAccessBlocker::AgentDisabled,
            AgentAccessBlocker::ApiSuspended,
            AgentAccessBlocker::Expired
        ]
    );
    module.restore(&context, w.id()).await.unwrap();
    module
        .set_agent_state(&context, w.id(), id, AgentState::Enabled)
        .await
        .unwrap();
    assert_eq!(
        module
            .inspect_agent(&owner, w.id(), id)
            .await
            .unwrap()
            .access_blockers(100),
        vec![
            AgentAccessBlocker::ApiSuspended,
            AgentAccessBlocker::Expired
        ]
    );
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one shared matrix compares every actor at the same authority seams"
)]
async fn principal_authority_matrix_preserves_user_roles_and_agent_scope() {
    let (database, module) = test_module().await;
    let owner = organization_user(
        module.database.clone(),
        "user:owner",
        OrganizationRole::Member,
    )
    .await;
    let member = organization_user(
        module.database.clone(),
        "user:member",
        OrganizationRole::Member,
    )
    .await;
    let inherited = organization_user(
        module.database.clone(),
        "user:inherited",
        OrganizationRole::Owner,
    )
    .await;
    let outsider = organization_user(
        module.database.clone(),
        "user:outsider",
        OrganizationRole::Member,
    )
    .await;
    let workspace = module
        .create(
            &change(owner.clone(), 10, '1'),
            WorkspaceDisplayName::parse("Authority").unwrap(),
        )
        .await
        .unwrap();
    let other = module
        .create(
            &change(owner.clone(), 11, '2'),
            WorkspaceDisplayName::parse("Other").unwrap(),
        )
        .await
        .unwrap();
    module
        .set_membership(
            &change(owner.clone(), 12, '3'),
            workspace.id(),
            &member,
            WorkspaceRole::Member,
        )
        .await
        .unwrap();
    let issued = module
        .create_agent(
            &change(owner.clone(), 13, '4'),
            workspace.id(),
            AgentDisplayName::parse("Worker").unwrap(),
            Some(100),
        )
        .await
        .unwrap();
    let agent = module
        .authenticate_agent(workspace.id(), issued.credential.expose(), 14)
        .await
        .unwrap();
    let actors = [
        (
            AuthenticatedPrincipal::user(owner.clone()),
            true,
            Some(owner.user_id()),
        ),
        (
            AuthenticatedPrincipal::user(member.clone()),
            true,
            Some(member.user_id()),
        ),
        // An Organization Owner without a Membership is an outsider here.
        (AuthenticatedPrincipal::user(inherited.clone()), false, None),
        (agent.clone(), true, None),
        (AuthenticatedPrincipal::user(outsider), false, None),
        (
            AuthenticatedPrincipal::user(AuthenticatedUser::without_organization_membership(
                UserId::parse("user:absent").unwrap(),
            )),
            false,
            None,
        ),
    ];
    // Denied callers learn only that the Workspace does not exist for them.
    let expected = |permitted: bool, result: Result<(), Error>| {
        if permitted {
            result.is_ok()
        } else {
            matches!(result, Err(Error::WorkspaceNotFound))
        }
    };
    for (actor, permitted, membership) in actors {
        assert!(expected(
            permitted,
            module
                .check_run_admission(&actor, workspace.id(), 15)
                .await
                .map(drop)
        ));
        let authoring = module.check_authoring(&actor, workspace.id(), 15).await;
        if let Ok(evidence) = &authoring {
            assert_eq!(evidence.membership_user_id(), membership);
        }
        assert!(expected(permitted, authoring.map(drop)));
        let connection = database.database();
        let mut transaction = connection.begin().await.unwrap();
        assert!(expected(
            permitted,
            module
                .check_run_admission_in_transaction(&mut transaction, &actor, workspace.id(), 15)
                .await
                .map(drop)
        ));
        assert!(expected(
            permitted,
            module
                .check_authoring_in_transaction(&mut transaction, &actor, workspace.id(), 15)
                .await
                .map(drop)
        ));
        assert!(expected(
            permitted,
            module
                .check_dispatch_in_transaction(
                    &mut transaction,
                    &actor,
                    workspace.id(),
                    15,
                    coral_db::RowLock::Shared,
                )
                .await
                .map(drop)
        ));
        transaction.rollback().await.unwrap();
    }
    assert!(matches!(
        module.check_run_admission(&agent, other.id(), 15).await,
        Err(Error::WorkspaceNotFound)
    ));
    assert!(matches!(
        module
            .authenticate_agent(other.id(), issued.credential.expose(), 15)
            .await,
        Err(Error::InvalidAgentCredential)
    ));
    assert_eq!(
        module
            .authenticate_agent_credential(issued.credential.expose(), 15)
            .await
            .unwrap()
            .agent_workspace_id(),
        Some(workspace.id()),
        "the credential alone resolves its own Workspace, never another"
    );
    assert!(matches!(
        module.list_agents(&member, workspace.id()).await,
        Err(Error::PermissionDenied(_))
    ));
    assert!(matches!(
        module
            .get_agent_registration(&member, workspace.id(), issued.agent.principal().id())
            .await,
        Err(Error::AgentNotFound)
    ));
    assert!(matches!(
        module.list_agents(&inherited, workspace.id()).await,
        Err(Error::WorkspaceNotFound)
    ));
    module
        .archive(&change(owner.clone(), 20, '5'), workspace.id())
        .await
        .unwrap();
    for actor in [agent, AuthenticatedPrincipal::user(member)] {
        assert!(matches!(
            module.check_run_admission(&actor, workspace.id(), 21).await,
            Err(Error::WorkspaceArchived)
        ));
        assert!(matches!(
            module.check_authoring(&actor, workspace.id(), 21).await,
            Err(Error::WorkspaceArchived)
        ));
    }
    assert_eq!(
        module.list_agents(&owner, workspace.id()).await.unwrap(),
        [issued.agent]
    );
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one administration scenario checks rejection leaves state and evidence unchanged"
)]
async fn rejected_agent_administration_changes_neither_configuration_nor_events() {
    let (_database, module) = test_module().await;
    let owner = organization_user(
        module.database.clone(),
        "user:owner",
        OrganizationRole::Member,
    )
    .await;
    let member = organization_user(
        module.database.clone(),
        "user:member",
        OrganizationRole::Member,
    )
    .await;
    let context = change(owner.clone(), 10, '1');
    let workspace = module
        .create(
            &context,
            WorkspaceDisplayName::parse("Administration").unwrap(),
        )
        .await
        .unwrap();
    let other = module
        .create(&context, WorkspaceDisplayName::parse("Other").unwrap())
        .await
        .unwrap();
    module
        .set_membership(&context, workspace.id(), &member, WorkspaceRole::Member)
        .await
        .unwrap();
    let issued = module
        .create_agent(
            &context,
            workspace.id(),
            AgentDisplayName::parse("Worker").unwrap(),
            None,
        )
        .await
        .unwrap();
    let events = module
        .list_lifecycle_events(&owner, workspace.id())
        .await
        .unwrap();
    for (context, target) in [
        (change(member, 11, '2'), workspace.id()),
        (context.clone(), other.id()),
    ] {
        module
            .rotate_agent_credential(&context, target, issued.registration.id())
            .await
            .unwrap_err();
        module
            .set_agent_state(
                &context,
                target,
                issued.agent.principal().id(),
                AgentState::Disabled,
            )
            .await
            .unwrap_err();
        module
            .update_agent_registration(&context, target, issued.registration.id(), false, Some(0))
            .await
            .unwrap_err();
    }
    assert_eq!(
        module
            .list_lifecycle_events(&owner, workspace.id())
            .await
            .unwrap(),
        events
    );
    assert_eq!(
        module
            .list_agents(&owner, workspace.id())
            .await
            .unwrap()
            .as_slice(),
        std::slice::from_ref(&issued.agent)
    );
    module
        .authenticate_agent(workspace.id(), issued.credential.expose(), 12)
        .await
        .unwrap();
    module.archive(&context, workspace.id()).await.unwrap();
    assert!(matches!(
        module
            .create_agent(
                &context,
                workspace.id(),
                AgentDisplayName::parse("Blocked").unwrap(),
                None
            )
            .await,
        Err(Error::WorkspaceArchived)
    ));
    assert!(matches!(
        module
            .rotate_agent_credential(&context, workspace.id(), issued.registration.id())
            .await,
        Err(Error::WorkspaceArchived)
    ));
    assert!(matches!(
        module
            .set_agent_state(
                &context,
                workspace.id(),
                issued.agent.principal().id(),
                AgentState::Disabled
            )
            .await,
        Err(Error::WorkspaceArchived)
    ));
    assert!(matches!(
        module
            .update_agent_registration(
                &context,
                workspace.id(),
                issued.registration.id(),
                false,
                None
            )
            .await,
        Err(Error::WorkspaceArchived)
    ));
    assert_eq!(
        module
            .list_lifecycle_events(&owner, workspace.id())
            .await
            .unwrap()
            .len(),
        events.len() + 1
    );
}

#[tokio::test]
async fn event_failure_rolls_back_agent_registration_and_initial_credential_together() {
    let module = fault_module("target_kind IN ('agent', 'agent_registration')").await;
    let owner = organization_user(
        module.1.database.clone(),
        "user:owner",
        OrganizationRole::Owner,
    )
    .await;
    let context = change(owner.clone(), 10, '1');
    let workspace = module
        .1
        .create(
            &context,
            WorkspaceDisplayName::parse("Atomic creation").unwrap(),
        )
        .await
        .unwrap();
    assert!(matches!(
        module
            .1
            .create_agent(
                &context,
                workspace.id(),
                AgentDisplayName::parse("Worker").unwrap(),
                None
            )
            .await,
        Err(Error::Database(_))
    ));
    assert!(
        module
            .1
            .list_agents(&owner, workspace.id())
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        module
            .1
            .list_lifecycle_events(&owner, workspace.id())
            .await
            .unwrap()
            .len(),
        2
    );
}

#[tokio::test]
async fn event_failure_rolls_back_rotation_and_lifecycle_changes() {
    let (_database, module) = fault_module("target_kind = 'agent_registration'").await;
    let owner = organization_user(
        module.database.clone(),
        "user:owner",
        OrganizationRole::Owner,
    )
    .await;
    let context = change(owner.clone(), 10, '1');
    let workspace = module
        .create(
            &context,
            WorkspaceDisplayName::parse("Atomic changes").unwrap(),
        )
        .await
        .unwrap();
    let issued = module
        .create_agent(
            &context,
            workspace.id(),
            AgentDisplayName::parse("Worker").unwrap(),
            None,
        )
        .await
        .unwrap();
    assert!(matches!(
        module
            .rotate_agent_credential(&context, workspace.id(), issued.registration.id())
            .await,
        Err(Error::Database(_))
    ));
    assert!(matches!(
        module
            .update_agent_registration(
                &context,
                workspace.id(),
                issued.registration.id(),
                false,
                Some(0)
            )
            .await,
        Err(Error::Database(_))
    ));
    module
        .authenticate_agent(workspace.id(), issued.credential.expose(), 20)
        .await
        .unwrap();
    assert_eq!(
        module
            .get_agent_registration(&owner, workspace.id(), issued.agent.principal().id())
            .await
            .unwrap(),
        issued.registration
    );
    assert_eq!(
        module
            .list_lifecycle_events(&owner, workspace.id())
            .await
            .unwrap()
            .len(),
        4
    );
}

async fn fault_module(predicate: &str) -> (coral_test_support::TestDatabase, WorkspaceModule) {
    // Private persistence fault injection: reject the second matching Event.
    // Assertions still use the public lifecycle/credential Interface.
    let migrator = coral_test_support::migration_plan_with([sqlx::migrate::Migration::new(9000, "event fault".into(), sqlx::migrate::MigrationType::Simple,
        sqlx::AssertSqlSafe(format!("CREATE UNIQUE INDEX reject_second_event ON workspace_lifecycle_events(workspace_id) WHERE {predicate}")).into_sql_str(), false)]);
    let fixture = coral_test_support::TestDatabase::new(&migrator)
        .await
        .unwrap();
    let module = WorkspaceModule::new(fixture.database());
    (fixture, module)
}
