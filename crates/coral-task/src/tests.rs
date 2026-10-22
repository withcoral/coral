use crate::{Error, TASK_INTENT_MAX_CHARS, TaskIntent, TaskModule, TaskOutcome};
use coral_test_support::change;
use coral_test_support::{TestDatabase, provision_user as organization_user};
use coral_workspace::AuthenticatedPrincipal;
use coral_workspace::{OrganizationRole, WorkspaceDisplayName, WorkspaceModule, WorkspaceRole};

#[tokio::test]
async fn coral_generates_task_identity_and_persists_start_evidence() {
    let (_database, workspaces, tasks) = test_modules().await;
    let (workspaces, tasks) = (&workspaces, &tasks);
    let user = organization_user(
        tasks.database.clone(),
        "user:task-creator",
        OrganizationRole::Member,
    )
    .await;
    let workspace = workspaces
        .create(
            &change(user.clone(), 10, '1'),
            WorkspaceDisplayName::parse("Task fixture").expect("display name"),
        )
        .await
        .expect("create Workspace");
    let principal = AuthenticatedPrincipal::user(user);

    let task = tasks
        .start(
            &principal,
            workspace.id(),
            TaskIntent::parse("Prepare the provider rollout").expect("Task intent"),
            20,
        )
        .await
        .expect("start Task");
    let found = tasks
        .get(&principal, workspace.id(), task.id())
        .await
        .expect("get Task");

    assert_eq!(found, task);
    assert_eq!(found.workspace_id(), workspace.id());
    assert_eq!(found.principal(), principal.principal());
    assert_eq!(found.intent().as_str(), "Prepare the provider rollout");
    assert_eq!(found.started_at_unix_nanos(), 20);
    assert_eq!(found.outcome(), None);
    assert_eq!(found.ended_at_unix_nanos(), None);

    let maximum_intent = "é".repeat(TASK_INTENT_MAX_CHARS);
    tasks
        .start(
            &principal,
            workspace.id(),
            TaskIntent::parse(&maximum_intent).expect("maximum Task intent"),
            21,
        )
        .await
        .expect("persist maximum Task intent");
    for invalid in [
        " ".to_string(),
        "\0".to_string(),
        "a".repeat(TASK_INTENT_MAX_CHARS + 1),
    ] {
        assert!(matches!(
            TaskIntent::parse(&invalid),
            Err(Error::InvalidInput(_))
        ));
    }
    assert_eq!(
        tasks
            .list(&principal, workspace.id())
            .await
            .expect("list started Tasks")
            .len(),
        2,
        "invalid intent must create no Task"
    );
}

#[tokio::test]
async fn first_terminal_task_outcome_wins_without_changing_attribution() {
    let (_database, workspaces, tasks) = test_modules().await;
    let (workspaces, tasks) = (&workspaces, &tasks);
    let user = organization_user(
        tasks.database.clone(),
        "user:terminal-race",
        OrganizationRole::Member,
    )
    .await;
    let workspace = workspaces
        .create(
            &change(user.clone(), 10, '2'),
            WorkspaceDisplayName::parse("Terminal race").expect("display name"),
        )
        .await
        .expect("create Workspace");
    let principal = AuthenticatedPrincipal::user(user);
    let started = tasks
        .start(
            &principal,
            workspace.id(),
            TaskIntent::parse("Resolve one terminal race").expect("Task intent"),
            20,
        )
        .await
        .expect("start Task");

    let (success, failure) = tokio::join!(
        tasks.end(
            &principal,
            workspace.id(),
            started.id(),
            TaskOutcome::Success,
            Some("Succeeded"),
            30,
        ),
        tasks.end(
            &principal,
            workspace.id(),
            started.id(),
            TaskOutcome::Failure,
            Some("Failed"),
            40,
        ),
    );
    let success = success.expect("observe terminal winner");
    let failure = failure.expect("observe terminal winner");

    assert_eq!(success, failure);
    assert_eq!(
        (success.feedback(), success.ended_at_unix_nanos()),
        match success.outcome().unwrap() {
            TaskOutcome::Success => (Some("Succeeded"), Some(30)),
            TaskOutcome::Failure => (Some("Failed"), Some(40)),
        }
    );
    assert_eq!(success.workspace_id(), started.workspace_id());
    assert_eq!(success.principal(), started.principal());
    assert_eq!(success.intent(), started.intent());
    assert_eq!(success.started_at_unix_nanos(), 20);
    assert_eq!(
        tasks
            .get(&principal, workspace.id(), started.id())
            .await
            .expect("get ended Task"),
        success
    );
}

#[tokio::test]
async fn completion_feedback_is_validated_and_the_first_completion_wins() {
    let (_database, workspaces, tasks) = test_modules().await;
    let user = organization_user(
        tasks.database.clone(),
        "user:feedback",
        OrganizationRole::Member,
    )
    .await;
    let workspace = workspaces
        .create(
            &change(user.clone(), 10, '1'),
            WorkspaceDisplayName::parse("Completion").unwrap(),
        )
        .await
        .unwrap();
    let actor = AuthenticatedPrincipal::user(user);
    for outcome in [TaskOutcome::Success, TaskOutcome::Failure] {
        let task = start(&tasks, &actor, workspace.id(), "Finish work", 20).await;
        for invalid in [" ".to_string(), "bad\0text".to_string(), "é".repeat(4097)] {
            assert!(matches!(
                tasks
                    .end(
                        &actor,
                        workspace.id(),
                        task.id(),
                        outcome,
                        Some(&invalid),
                        30
                    )
                    .await,
                Err(Error::InvalidInput(_))
            ));
            assert_eq!(
                tasks
                    .get(&actor, workspace.id(), task.id())
                    .await
                    .unwrap()
                    .outcome(),
                None
            );
        }
        let feedback = "🪸".repeat(4096);
        let winner = tasks
            .end(
                &actor,
                workspace.id(),
                task.id(),
                outcome,
                Some(&feedback),
                30,
            )
            .await
            .unwrap();
        assert_eq!(winner.feedback(), Some(feedback.as_str()));
        assert_eq!(
            tasks.get(&actor, workspace.id(), task.id()).await.unwrap(),
            winner
        );
        assert_eq!(
            tasks
                .end(
                    &actor,
                    workspace.id(),
                    task.id(),
                    TaskOutcome::Failure,
                    Some("Changed"),
                    40
                )
                .await
                .unwrap(),
            winner
        );
    }
    let task = start(&tasks, &actor, workspace.id(), "No feedback", 20).await;
    let winner = tasks
        .end(
            &actor,
            workspace.id(),
            task.id(),
            TaskOutcome::Success,
            None,
            30,
        )
        .await
        .unwrap();
    assert_eq!(winner.feedback(), None);
    assert_eq!(
        tasks
            .end(
                &actor,
                workspace.id(),
                task.id(),
                TaskOutcome::Failure,
                Some("Late feedback"),
                40
            )
            .await
            .unwrap(),
        winner
    );
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one confidentiality matrix compares Users, Owners, and a registered Agent"
)]
async fn task_visibility_preserves_user_privacy_and_owner_agent_oversight() {
    let (_database, workspaces, tasks) = test_modules().await;
    let (workspaces, tasks) = (&workspaces, &tasks);
    let owner_user = organization_user(
        tasks.database.clone(),
        "user:workspace-owner",
        OrganizationRole::Member,
    )
    .await;
    let member_user = organization_user(
        tasks.database.clone(),
        "user:workspace-member",
        OrganizationRole::Member,
    )
    .await;
    let second_owner_user = organization_user(
        tasks.database.clone(),
        "user:second-owner",
        OrganizationRole::Member,
    )
    .await;
    // Organization Owner authority grants nothing without a Workspace Membership.
    let outsiders = [
        ("user:organization-owner", OrganizationRole::Owner),
        ("user:organization-member", OrganizationRole::Member),
    ];
    let mut non_members = Vec::new();
    for (id, role) in outsiders {
        non_members.push(AuthenticatedPrincipal::user(
            organization_user(tasks.database.clone(), id, role).await,
        ));
    }
    let workspace = workspaces
        .create(
            &change(owner_user.clone(), 10, '3'),
            WorkspaceDisplayName::parse("Visibility").expect("display name"),
        )
        .await
        .expect("create Workspace");
    workspaces
        .set_membership(
            &change(owner_user.clone(), 20, '4'),
            workspace.id(),
            &member_user,
            WorkspaceRole::Member,
        )
        .await
        .expect("add Workspace Member");
    workspaces
        .set_membership(
            &change(owner_user.clone(), 20, '4'),
            workspace.id(),
            &second_owner_user,
            WorkspaceRole::Owner,
        )
        .await
        .expect("add second Workspace Owner");
    let owner = AuthenticatedPrincipal::user(owner_user);
    let member = AuthenticatedPrincipal::user(member_user);
    let second_owner = AuthenticatedPrincipal::user(second_owner_user);
    let issued = workspaces
        .create_agent(
            &change(owner.authenticated_user().unwrap().clone(), 20, '4'),
            workspace.id(),
            coral_workspace::AgentDisplayName::parse("Worker").unwrap(),
            None,
        )
        .await
        .unwrap();
    let agent = workspaces
        .authenticate_agent(workspace.id(), issued.credential.expose(), 21)
        .await
        .unwrap();
    let owner_task = start(tasks, &owner, workspace.id(), "Owner work", 30).await;
    let member_task = start(tasks, &member, workspace.id(), "Private member work", 40).await;
    let second_owner_task = start(
        tasks,
        &second_owner,
        workspace.id(),
        "Second Owner work",
        45,
    )
    .await;
    let agent_task = start(tasks, &agent, workspace.id(), "Agent work", 50).await;
    let agent_task = tasks
        .end(
            &agent,
            workspace.id(),
            agent_task.id(),
            TaskOutcome::Success,
            Some("Agent completion"),
            60,
        )
        .await
        .expect("Agent ends its Task");

    assert_eq!(
        task_ids(
            tasks
                .list(&owner, workspace.id())
                .await
                .expect("Workspace Owner lists visible Tasks")
        ),
        task_ids(vec![owner_task.clone(), agent_task.clone()])
    );
    assert_eq!(
        task_ids(
            tasks
                .list(&second_owner, workspace.id())
                .await
                .expect("second Owner lists visible Tasks")
        ),
        task_ids(vec![second_owner_task.clone(), agent_task.clone()])
    );
    assert_eq!(
        tasks
            .list(&member, workspace.id())
            .await
            .expect("Member lists own Tasks"),
        vec![member_task.clone()]
    );
    assert!(matches!(
        tasks.list(&agent, workspace.id()).await,
        Err(Error::PermissionDenied(_))
    ));

    assert_task_concealed(tasks, &owner, workspace.id(), member_task.id()).await;
    assert_task_concealed(tasks, &second_owner, workspace.id(), owner_task.id()).await;
    assert_task_concealed(tasks, &second_owner, workspace.id(), member_task.id()).await;
    assert_task_concealed(tasks, &agent, workspace.id(), owner_task.id()).await;
    assert_task_concealed(tasks, &agent, workspace.id(), agent_task.id()).await;
    assert_task_visible(tasks, &second_owner, workspace.id(), &second_owner_task).await;
    assert_task_visible(tasks, &second_owner, workspace.id(), &agent_task).await;

    for non_member in &non_members {
        assert!(matches!(
            tasks
                .start(
                    non_member,
                    workspace.id(),
                    TaskIntent::parse("Non-Member work").expect("Task intent"),
                    70,
                )
                .await,
            Err(Error::Workspace(coral_workspace::Error::WorkspaceNotFound))
        ));
        assert!(matches!(
            tasks.list(non_member, workspace.id()).await,
            Err(Error::Workspace(coral_workspace::Error::WorkspaceNotFound))
        ));
        for task in [&owner_task, &member_task, &agent_task] {
            assert_task_concealed(tasks, non_member, workspace.id(), task.id()).await;
        }
    }
    assert_eq!(
        task_ids(
            tasks
                .list(&owner, workspace.id())
                .await
                .expect("Workspace Owner lists visible Tasks")
        ),
        task_ids(vec![owner_task, agent_task]),
        "denied starts created no Task"
    );
    // A Task is private to its User, so the Owner's listing cannot show one a
    // denied start left behind. Each non-Member joins and finds no Task.
    for non_member in &non_members {
        workspaces
            .set_membership(
                &change(owner.authenticated_user().unwrap().clone(), 80, '5'),
                workspace.id(),
                non_member.authenticated_user().unwrap(),
                WorkspaceRole::Member,
            )
            .await
            .expect("add the non-Member");
        assert!(
            tasks
                .list(non_member, workspace.id())
                .await
                .expect("the new Member lists their Tasks")
                .is_empty(),
            "a denied start created no Task for {non_member:?}"
        );
    }
}

async fn assert_task_visible(
    tasks: &TaskModule,
    principal: &AuthenticatedPrincipal,
    workspace_id: &coral_workspace::WorkspaceId,
    expected: &crate::Task,
) {
    assert_eq!(
        &tasks
            .get(principal, workspace_id, expected.id())
            .await
            .expect("inspect visible Task"),
        expected
    );
}

async fn assert_task_concealed(
    tasks: &TaskModule,
    principal: &AuthenticatedPrincipal,
    workspace_id: &coral_workspace::WorkspaceId,
    task_id: &crate::TaskId,
) {
    assert!(matches!(
        tasks.get(principal, workspace_id, task_id).await,
        Err(Error::TaskNotFound)
    ));
}

async fn start(
    tasks: &TaskModule,
    principal: &AuthenticatedPrincipal,
    workspace_id: &coral_workspace::WorkspaceId,
    intent: &str,
    at: i64,
) -> crate::Task {
    tasks
        .start(
            principal,
            workspace_id,
            TaskIntent::parse(intent).expect("Task intent"),
            at,
        )
        .await
        .expect("start Task")
}

fn task_ids(tasks: Vec<crate::Task>) -> Vec<crate::TaskId> {
    tasks.into_iter().map(|task| task.id().clone()).collect()
}

pub(crate) async fn test_modules() -> (TestDatabase, WorkspaceModule, TaskModule) {
    let fixture = TestDatabase::new(coral_db::migration_plan())
        .await
        .expect("create Workspace and Task test database");
    let database = fixture.database();
    let workspaces = WorkspaceModule::new(database.clone());
    let tasks = TaskModule::new(database, workspaces.clone());
    (fixture, workspaces, tasks)
}

/// A conflict on the identifier writes nothing and reports `false`; a fresh
/// identifier in the same transaction still inserts.
#[tokio::test]
async fn a_taken_task_identifier_is_reported_and_writes_nothing() {
    let (_database, workspaces, tasks) = test_modules().await;
    let user = organization_user(
        tasks.database.clone(),
        "user:task-identifier",
        OrganizationRole::Member,
    )
    .await;
    let workspace = workspaces
        .create(
            &change(user.clone(), 10, '1'),
            WorkspaceDisplayName::parse("Identifiers").expect("display name"),
        )
        .await
        .expect("create Workspace");
    let principal = AuthenticatedPrincipal::user(user);
    let task = |id: &str, intent: &str, started_at_unix_nanos: i64| {
        crate::Task::new(
            crate::TaskId::parse(id).expect("identifier"),
            workspace.id().clone(),
            principal.principal().clone(),
            TaskIntent::parse(intent).expect("Task intent"),
            None,
            None,
            started_at_unix_nanos,
            None,
        )
    };
    let taken = "random-big-business-store";
    let first = task(taken, "Check identifiers", 10);

    let mut transaction = tasks.database.begin().await.expect("transaction");
    assert!(
        crate::repository::insert(&mut transaction, &first)
            .await
            .expect("first insert")
    );
    // The conflicting Task differs in every column it can, so an insert that
    // overwrote the row would show.
    assert!(
        !crate::repository::insert(
            &mut transaction,
            &task(taken, "Overwrite the first Task", 20)
        )
        .await
        .expect("conflicting insert")
    );
    assert!(
        crate::repository::insert(
            &mut transaction,
            &task("height-word-database-library", "Check identifiers", 30)
        )
        .await
        .expect("fresh insert after the conflict")
    );
    transaction.commit().await.expect("commit");
    let found = tasks
        .get(
            &principal,
            workspace.id(),
            &crate::TaskId::parse(taken).expect("identifier"),
        )
        .await
        .expect("get Task");
    assert_eq!(
        found.intent(),
        first.intent(),
        "the conflict changed nothing"
    );
    assert_eq!(found.started_at_unix_nanos(), 10);
}
