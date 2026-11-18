use crate::{Error, TASK_INTENT_MAX_CHARS, TaskIntent, TaskModule, TaskOutcome};
use coral_db::UnixNanos;
use coral_test_support::change;
use coral_test_support::{TestDatabase, provision_user as organization_user};
use coral_workspace::AuthenticatedPrincipal;
use coral_workspace::{
    IfMatch, OrganizationRole, RequestId, WorkspaceDisplayName, WorkspaceModule, WorkspaceRole,
};

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
            UnixNanos::new(20),
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
    assert_eq!(found.started_at().get(), 20);
    assert_eq!(found.outcome(), None);
    assert_eq!(found.ended_at(), None);

    let maximum_intent = "é".repeat(TASK_INTENT_MAX_CHARS);
    tasks
        .start(
            &principal,
            workspace.id(),
            TaskIntent::parse(&maximum_intent).expect("maximum Task intent"),
            UnixNanos::new(21),
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
            UnixNanos::new(20),
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
            UnixNanos::new(30),
        ),
        tasks.end(
            &principal,
            workspace.id(),
            started.id(),
            TaskOutcome::Failure,
            Some("Failed"),
            UnixNanos::new(40),
        ),
    );
    let success = success.expect("observe terminal winner");
    let failure = failure.expect("observe terminal winner");

    assert_eq!(success, failure);
    assert_eq!(
        (success.feedback(), success.ended_at().map(UnixNanos::get)),
        match success.outcome().unwrap() {
            TaskOutcome::Success => (Some("Succeeded"), Some(30)),
            TaskOutcome::Failure => (Some("Failed"), Some(40)),
        }
    );
    assert_eq!(success.workspace_id(), started.workspace_id());
    assert_eq!(success.principal(), started.principal());
    assert_eq!(success.intent(), started.intent());
    assert_eq!(success.started_at().get(), 20);
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
                        UnixNanos::new(30)
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
                UnixNanos::new(30),
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
                    UnixNanos::new(40)
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
            UnixNanos::new(30),
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
                UnixNanos::new(40)
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
            &RequestId::generate(),
            workspace.id(),
            coral_workspace::AgentDisplayName::parse("Worker").unwrap(),
            None,
        )
        .await
        .unwrap();
    let agent = workspaces
        .authenticate_agent(
            workspace.id(),
            issued.credential.expose(),
            UnixNanos::new(21),
        )
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
            UnixNanos::new(60),
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
                    UnixNanos::new(70),
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

// Catches: a module above Task joining its records to a Task its caller did
// not start, to a Task addressed through another Workspace, or to an ended
// Task; a refusal that tells a caller outside the Workspace that the Task
// exists; and a hold that reads the Task as `get` does, which conceals an
// Agent's own Task from it.
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one table compares every caller and Task the hold accepts or refuses"
)]
async fn only_the_open_task_its_caller_started_is_held_open() {
    let (_database, workspaces, tasks) = test_modules().await;
    let (workspaces, tasks) = (&workspaces, &tasks);
    let mut users = Vec::new();
    for id in ["user:hold-owner", "user:hold-member", "user:hold-outsider"] {
        users.push(organization_user(tasks.database.clone(), id, OrganizationRole::Member).await);
    }
    let [owner_user, member_user, outsider_user] = users.try_into().expect("three Users");
    let workspace = workspaces
        .create(
            &change(owner_user.clone(), 10, '1'),
            WorkspaceDisplayName::parse("Held Tasks").expect("display name"),
        )
        .await
        .expect("create Workspace");
    let other_workspace = workspaces
        .create(
            &change(owner_user.clone(), 11, '2'),
            WorkspaceDisplayName::parse("Another Workspace").expect("display name"),
        )
        .await
        .expect("create the other Workspace");
    workspaces
        .set_membership(
            &change(owner_user.clone(), 20, '3'),
            workspace.id(),
            &member_user,
            WorkspaceRole::Member,
        )
        .await
        .expect("add Workspace Member");
    let issued = workspaces
        .create_agent(
            &change(owner_user.clone(), 20, '4'),
            &RequestId::generate(),
            workspace.id(),
            coral_workspace::AgentDisplayName::parse("Worker").expect("Agent name"),
            None,
        )
        .await
        .expect("create Agent");
    let agent = workspaces
        .authenticate_agent(
            workspace.id(),
            issued.credential.expose(),
            UnixNanos::new(21),
        )
        .await
        .expect("authenticate Agent");
    let owner = AuthenticatedPrincipal::user(owner_user);
    let member = AuthenticatedPrincipal::user(member_user);
    let outsider = AuthenticatedPrincipal::user(outsider_user);
    let owner_task = start(tasks, &owner, workspace.id(), "Owner work", 30).await;
    let member_task = start(tasks, &member, workspace.id(), "Member work", 31).await;
    let agent_task = start(tasks, &agent, workspace.id(), "Agent work", 32).await;
    let ended_task = start(tasks, &owner, workspace.id(), "Finished work", 33).await;
    let ended_task = tasks
        .end(
            &owner,
            workspace.id(),
            ended_task.id(),
            TaskOutcome::Success,
            None,
            UnixNanos::new(40),
        )
        .await
        .expect("end Task");
    let absent = crate::TaskId::parse("height-word-database-library").expect("identifier");
    let every_task = [&owner_task, &member_task, &agent_task, &ended_task];
    let before = retained(tasks, &every_task).await;

    for (actor, task) in [
        (&owner, &owner_task),
        (&member, &member_task),
        (&agent, &agent_task),
    ] {
        let mut transaction = tasks.database.begin().await.expect("transaction");
        let (held, authority) = tasks
            .hold_open_in_transaction(&mut transaction, actor, workspace.id(), task.id())
            .await
            .unwrap_or_else(|error| panic!("{actor:?} holds its own open Task: {error}"));
        assert_eq!(&held, task);
        assert_eq!(authority.workspace().id(), workspace.id(), "{actor:?}");
        transaction.commit().await.expect("commit");
    }

    let not_found: fn(&Error) -> bool = |error| matches!(error, Error::TaskNotFound);
    let closed: fn(&Error) -> bool = |error| matches!(error, Error::TaskClosed);
    for (case, actor, workspace_id, task_id, refused) in [
        (
            "a Member and an Owner's Task",
            &member,
            workspace.id(),
            owner_task.id(),
            not_found,
        ),
        (
            "an Owner and a Member's Task",
            &owner,
            workspace.id(),
            member_task.id(),
            not_found,
        ),
        // The Owner may read an Agent's Task, but did not start it.
        (
            "an Owner and an Agent's Task",
            &owner,
            workspace.id(),
            agent_task.id(),
            not_found,
        ),
        ("an absent Task", &owner, workspace.id(), &absent, not_found),
        (
            "a Task addressed through another Workspace",
            &owner,
            other_workspace.id(),
            owner_task.id(),
            not_found,
        ),
        (
            "an ended Task",
            &owner,
            workspace.id(),
            ended_task.id(),
            closed,
        ),
        // A caller outside the Workspace gets the answer an absent Task gets.
        (
            "a caller outside the Workspace",
            &outsider,
            workspace.id(),
            owner_task.id(),
            not_found,
        ),
    ] {
        let mut transaction = tasks.database.begin().await.expect("transaction");
        let result = tasks
            .hold_open_in_transaction(&mut transaction, actor, workspace_id, task_id)
            .await;
        assert!(result.as_ref().is_err_and(refused), "{case}: {result:?}");
        // A caller that commits after the refusal still commits nothing.
        transaction.commit().await.expect("commit");
    }
    assert_eq!(
        retained(tasks, &every_task).await,
        before,
        "neither a hold nor a refusal changed a Task"
    );
}

// Catches: a check that waits behind a writer that holds the Task, or that
// passes a caller who lost access to it.
#[tokio::test]
async fn check_open_holds_nothing_and_refuses_a_caller_who_lost_access() {
    let (_database, workspaces, tasks) = test_modules().await;
    let database = tasks.database.clone();
    let user = organization_user(database.clone(), "user:a", OrganizationRole::Owner).await;
    let other = organization_user(database.clone(), "user:b", OrganizationRole::Owner).await;
    let name = WorkspaceDisplayName::parse("Checked").expect("display name");
    let workspace = workspaces
        .create(&change(user.clone(), 10, '1'), name)
        .await;
    let workspace_id = workspace.expect("create Workspace").id().clone();
    let actor = AuthenticatedPrincipal::user(user.clone());
    let task = start(&tasks, &actor, &workspace_id, "Open", 20).await;

    let mut holder = database.begin().await.expect("transaction");
    let held = tasks.hold_open_in_transaction(&mut holder, &actor, &workspace_id, task.id());
    held.await.expect("hold the Task");
    let while_held = tasks.check_open(&actor, &workspace_id, task.id()).await;
    holder.commit().await.expect("commit");
    coral_workspace::OrganizationModule::new(database)
        .set_enabled(
            &change(other, 30, '2'),
            user.user_id(),
            &IfMatch::any(),
            false,
        )
        .await
        .expect("disable the User");
    let without_access = tasks.check_open(&actor, &workspace_id, task.id()).await;

    assert!(while_held.is_ok(), "{while_held:?}");
    assert!(
        matches!(without_access, Err(Error::TaskNotFound)),
        "{without_access:?}"
    );
}

/// Each Task as persistence retains it, with its last activity.
async fn retained(
    tasks: &TaskModule,
    every_task: &[&crate::Task],
) -> Vec<(crate::Task, UnixNanos)> {
    let mut transaction = tasks.database.begin().await.expect("transaction");
    let mut retained = Vec::new();
    for task in every_task {
        retained.push(
            crate::repository::retention_group(&mut transaction, task.workspace_id(), task.id())
                .await
                .expect("read the Task")
                .expect("the Task is retained"),
        );
    }
    transaction.rollback().await.expect("roll back");
    retained
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
            UnixNanos::new(at),
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
            UnixNanos::new(started_at_unix_nanos),
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
    assert_eq!(found.started_at().get(), 10);
}
