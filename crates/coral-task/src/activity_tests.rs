//! Current hierarchy contracts, replacing tests for superseded filters and sort modes.
use super::*;
use coral_workspace::{HistoryScope, WorkspaceRole};

#[tokio::test]
async fn activity_observation_wakes_for_task_creation_interactions_and_ending() {
    use std::time::Duration;
    let f = Fixture::new().await;
    let mut observation = f.runs.observe_activity(&f.actor).await.unwrap();
    let goal = f.goal().await;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), observation.next())
            .await
            .unwrap()
            .unwrap(),
        coral_run::ActivityWake::Changed
    );
    f.commands
        .search(
            &f.call(&goal, "Find operations"),
            coral_search::SearchInput::default(),
        )
        .await
        .unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), observation.next())
            .await
            .unwrap()
            .unwrap(),
        coral_run::ActivityWake::Changed
    );
    f.tasks
        .end(
            &f.actor,
            &f.workspace,
            goal.id(),
            TaskOutcome::Success,
            None,
            wall_time(),
        )
        .await
        .unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), observation.next())
            .await
            .unwrap()
            .unwrap(),
        coral_run::ActivityWake::Changed
    );
}

#[tokio::test]
async fn context_impact_task_total_includes_unloaded_measurements_and_ignores_non_applicable_commands()
 {
    let f = Fixture::new().await;
    let goal = f.goal().await;
    let call = f.call(&goal, "Find operations");
    for _ in 0..2 {
        f.commands
            .search(&call, coral_search::SearchInput::default())
            .await
            .unwrap();
    }
    let run = f
        .commands
        .submit(&call, "return 'é';", 30_000)
        .await
        .unwrap();
    f.wait_for_terminal(run.run_id()).await;
    f.commands
        .save_function(
            &call,
            "report",
            "Save computed work",
            "export default () => 42;",
        )
        .await
        .unwrap();
    f.commands.delete_function(&call, "report").await.unwrap();
    f.tasks
        .end(
            &f.actor,
            &f.workspace,
            goal.id(),
            TaskOutcome::Success,
            Some("Found an answer"),
            wall_time(),
        )
        .await
        .unwrap();
    let identity = TaskIdentity {
        workspace: f.workspace.clone(),
        task: goal.id().clone(),
    };
    let mut window = InteractionWindow::Start;
    let mut bytes = Vec::new();
    let mut task_totals = Vec::new();
    loop {
        let page = f
            .tasks
            .activity_interactions(&f.runs, &f.actor, &identity, &window, 1, None)
            .await
            .unwrap();
        task_totals.push(page.task.context_impact_bytes());
        bytes.extend(page.rows.iter().map(|row| {
            row.run
                .as_ref()
                .map_or(row.interaction.context_impact_bytes, |run| {
                    run.context_impact_bytes
                })
        }));
        let Some(after) = page.next_after else {
            break;
        };
        window = InteractionWindow::After(after);
    }
    // Searches and the Run's JSON result are measured; function authoring is not.
    let [
        Some(first_search),
        Some(second_search),
        Some(run_result),
        None,
        None,
    ] = bytes[..]
    else {
        panic!("unexpected child measurements: {bytes:?}");
    };
    assert_eq!(first_search, second_search);
    assert_eq!(
        usize::try_from(run_result).unwrap(),
        serde_json::to_string("é").unwrap().len()
    );
    let total = Some(first_search + second_search + run_result);
    assert!(
        task_totals.iter().all(|task_total| *task_total == total),
        "every page reports the Task total, including unloaded children: {task_totals:?}"
    );
    let roots = f
        .tasks
        .activity_roots(
            &f.runs,
            &f.actor,
            &HistoryScope::Personal(None),
            &ActivityWindow::Edge,
            1,
        )
        .await
        .unwrap();
    let ActivityRoot::Task(task) = roots.rows.first().unwrap() else {
        panic!("goal root")
    };
    assert_eq!(task.context_impact_bytes(), total);
}

/// The summary counts context as Activity does: discovery responses add to it, so
/// each caller's share matches their Task total, not their Runs alone.
#[tokio::test]
async fn activity_summary_counts_context_as_the_task_total_does() {
    let f = Fixture::new().await;
    let owner = f.actor.authenticated_user().unwrap().clone();
    let issued = f
        .tasks
        .workspaces
        .create_agent(
            &ChangeContext::new(owner, 1, coral_test_support::trace('1')),
            &f.workspace,
            coral_workspace::AgentDisplayName::parse("Worker").unwrap(),
            None,
        )
        .await
        .unwrap();
    let agent = f
        .tasks
        .workspaces
        .authenticate_agent(&f.workspace, issued.credential.expose(), 2)
        .await
        .unwrap();
    let goal = f.goal().await;
    let call = f.call(&goal, "Find operations");
    f.commands
        .search(&call, coral_search::SearchInput::default())
        .await
        .unwrap();
    let run = f
        .commands
        .submit(&call, "return 'é';", 30_000)
        .await
        .unwrap();
    f.wait_for_terminal(run.run_id()).await;
    let agent_goal = f
        .tasks
        .start(
            &agent,
            &f.workspace,
            TaskIntent::parse("Agent goal").unwrap(),
            3,
        )
        .await
        .unwrap();
    let agent_call = AttributedCommand::new(
        agent.clone(),
        f.workspace.clone(),
        agent_goal.id().clone(),
        TaskIntent::parse("Find agent operations").unwrap(),
        coral_test_support::trace('1'),
    );
    f.commands
        .search(&agent_call, coral_search::SearchInput::default())
        .await
        .unwrap();

    let (Some(owner_total), false) = task_impact(&f, &goal).await else {
        panic!("a measured, finished Task")
    };
    let summary = f
        .tasks
        .activity_summary(&f.runs, &f.actor, &f.workspace, 1)
        .await
        .unwrap();
    let share = |principal: &coral_workspace::Principal| {
        summary
            .actors
            .iter()
            .find(|share| share.principal == *principal)
            .map(|share| (share.bytes, share.agent_label.clone()))
    };
    assert_eq!(share(f.actor.principal()), Some((owner_total, None)));
    let Some((agent_total, Some(label))) = share(agent.principal()) else {
        panic!("the Agent's search is its own share: {:?}", summary.actors)
    };
    assert!(agent_total > 0 && label == "Worker");
    assert_eq!(summary.actors.len(), 2);
    assert_eq!(
        summary.context_impact_bytes,
        Some(owner_total + agent_total)
    );

    let denied = f
        .tasks
        .activity_summary(&f.runs, &agent, &f.workspace, 1)
        .await;
    assert!(
        matches!(
            denied,
            Err(Error::Workspace(coral_workspace::Error::PermissionDenied(
                _
            )))
        ),
        "an Agent cannot read the summary"
    );
}

#[tokio::test]
async fn child_viewports_seek_bounded_history_and_refresh_around_an_authorized_anchor() {
    let f = Fixture::new().await;
    let goal = f.goal().await;
    let call = f.call(&goal, "Read a continuous history");
    let identity = TaskIdentity {
        workspace: f.workspace.clone(),
        task: goal.id().clone(),
    };
    for _ in 0..8 {
        f.commands
            .search(&call, coral_search::SearchInput::default())
            .await
            .unwrap();
    }
    let window = InteractionWindow::Viewport {
        offset: 4,
        anchor: None,
        preceding: 1,
    };
    let page = f
        .tasks
        .activity_interactions(&f.runs, &f.actor, &identity, &window, 3, None)
        .await
        .unwrap();
    assert_eq!(page.offset, 4);
    assert_eq!(page.total, 8);
    assert_eq!(page.rows.len(), 3);
    let anchor = page
        .rows
        .get(1)
        .expect("middle child")
        .interaction
        .id
        .clone();
    f.commands
        .search(&call, coral_search::SearchInput::default())
        .await
        .unwrap();
    let refreshed = f
        .tasks
        .activity_interactions(
            &f.runs,
            &f.actor,
            &identity,
            &InteractionWindow::Viewport {
                offset: 0,
                anchor: Some(anchor.clone()),
                preceding: 1,
            },
            3,
            None,
        )
        .await
        .unwrap();
    assert_eq!(refreshed.offset, 4);
    assert_eq!(
        refreshed.rows.get(1).expect("middle child").interaction.id,
        anchor
    );
    assert_eq!(refreshed.total, 9);
    let beyond = f
        .tasks
        .activity_interactions(
            &f.runs,
            &f.actor,
            &identity,
            &InteractionWindow::Viewport {
                offset: 99,
                anchor: None,
                preceding: 0,
            },
            3,
            None,
        )
        .await
        .unwrap();
    assert_eq!(beyond.offset, 6);
    assert_eq!(beyond.rows.len(), 3);
}

#[tokio::test]
async fn context_impact_is_partial_while_a_run_runs_and_skips_an_unmeasured_child() {
    let f = Fixture::new().await;
    let goal = f.goal().await;
    assert_eq!(task_impact(&f, &goal).await, (Some(0), false), "empty goal");
    f.commands
        .search(
            &f.call(&goal, "Find operations"),
            coral_search::SearchInput::default(),
        )
        .await
        .unwrap();
    let (Some(finished), false) = task_impact(&f, &goal).await else {
        panic!("a finished search is measured")
    };
    let running = f
        .commands
        .submit(
            &f.call(&goal, "Wait for execution"),
            "await coral.emit('é'); while (true) {}",
            30_000,
        )
        .await
        .unwrap();
    // The emission is durable, but the running Run still withholds its bytes.
    f.runs
        .observe(
            &f.actor,
            &f.workspace,
            running.run_id(),
            &coral_run::RunObservation::immediate(Some(running.next_cursor().clone()), 1)
                .unwrap()
                .with_wait(std::time::Duration::from_secs(10))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        task_impact(&f, &goal).await,
        (Some(finished), true),
        "a running Run leaves the finished children's partial sum"
    );
    f.cancel(running.run_id()).await;
    let (Some(ended), false) = task_impact(&f, &goal).await else {
        panic!("the ended Run completes the total")
    };
    assert_eq!(
        usize::try_from(ended - finished).unwrap(),
        serde_json::to_string("é").unwrap().len(),
        "the total gains the ended Run's emission"
    );
    let waiting = f
        .commands
        .submit(&f.call(&goal, "Wait again"), "while (true) {}", 30_000)
        .await
        .unwrap();
    f.commands
        .search(
            &f.call(&goal, "Invalid search"),
            coral_search::SearchInput {
                limit: Some(0),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    let page = f
        .tasks
        .activity_interactions(
            &f.runs,
            &f.actor,
            &TaskIdentity {
                workspace: f.workspace.clone(),
                task: goal.id().clone(),
            },
            &InteractionWindow::Start,
            100,
            None,
        )
        .await
        .unwrap();
    let rejected = &page.rows.last().unwrap().interaction;
    assert_eq!(
        (rejected.kind, rejected.context_impact_bytes),
        (AttemptKind::Search, None),
        "a rejected search is an unmeasured child"
    );
    assert_eq!(
        task_impact(&f, &goal).await,
        (Some(ended), true),
        "an unmeasurable child adds nothing to the measured children's sum"
    );
    f.cancel(waiting.run_id()).await;
}

/// The Task's context impact and partial flag, which both Task reads must agree on.
async fn task_impact(f: &Fixture, goal: &Task) -> (Option<i64>, bool) {
    let roots = f
        .tasks
        .activity_roots(
            &f.runs,
            &f.actor,
            &HistoryScope::Personal(None),
            &ActivityWindow::Edge,
            1,
        )
        .await
        .unwrap();
    let ActivityRoot::Task(root) = roots.rows.first().unwrap() else {
        panic!("goal root")
    };
    let page = f
        .tasks
        .activity_interactions(
            &f.runs,
            &f.actor,
            &TaskIdentity {
                workspace: f.workspace.clone(),
                task: goal.id().clone(),
            },
            &InteractionWindow::Start,
            1,
            None,
        )
        .await
        .unwrap();
    let impact = (root.context_impact_bytes(), root.context_impact_partial());
    assert_eq!(
        (
            page.task.context_impact_bytes(),
            page.task.context_impact_partial()
        ),
        impact,
        "Task reads disagree"
    );
    impact
}

/// Six goals with tied timestamps in two Workspaces, newest-first by qualified identity.
struct TiedRoots {
    second: coral_workspace::WorkspaceId,
    goals: Vec<Task>,
}

impl TiedRoots {
    async fn new(f: &Fixture) -> Self {
        let user = f.actor.authenticated_user().unwrap().clone();
        let second = f
            .tasks
            .workspaces
            .create(
                &ChangeContext::new(user, 1, coral_test_support::trace('1')),
                WorkspaceDisplayName::parse("Second").unwrap(),
            )
            .await
            .unwrap();
        let mut goals = Vec::new();
        for workspace in [&f.workspace, second.id()] {
            for _ in 0..3 {
                goals.push(
                    f.tasks
                        .start(
                            &f.actor,
                            workspace,
                            TaskIntent::parse("Equal intent É_%").unwrap(),
                            10,
                        )
                        .await
                        .unwrap(),
                );
            }
        }
        // All timestamps tie: qualified identity, never intent text, determines the order.
        goals.sort_by_key(|task| (task.workspace_id().to_string(), task.id().to_string()));
        goals.reverse();
        Self {
            second: second.id().clone(),
            goals,
        }
    }

    fn names<'a>(
        tasks: impl IntoIterator<Item = &'a Task>,
    ) -> Vec<(coral_workspace::WorkspaceId, TaskId)> {
        tasks
            .into_iter()
            .map(|task| (task.workspace_id().clone(), task.id().clone()))
            .collect()
    }

    fn in_second(&self) -> impl Iterator<Item = &Task> {
        self.goals
            .iter()
            .filter(|goal| goal.workspace_id() == &self.second)
    }
}

fn root_names(rows: &[ActivityRoot]) -> Vec<(coral_workspace::WorkspaceId, TaskId)> {
    rows.iter()
        .map(|row| match row {
            ActivityRoot::Task(task) => (task.workspace_id().clone(), task.id().clone()),
            ActivityRoot::Run(_) => panic!("Task-only fixture"),
        })
        .collect()
}

#[tokio::test]
async fn root_windows_page_in_qualified_order_around_anchors() {
    let f = Fixture::new().await;
    let roots = TiedRoots::new(&f).await;
    let scope = HistoryScope::Personal(None);
    let window = |window: ActivityWindow| {
        let (tasks, runs, actor, scope) = (&f.tasks, &f.runs, &f.actor, &scope);
        async move {
            tasks
                .activity_roots(runs, actor, scope, &window, 2)
                .await
                .unwrap()
        }
    };
    let first = window(ActivityWindow::Edge).await;
    assert!(!first.anchor_unavailable);
    assert!(!first.has_before && first.has_after);
    assert_eq!(
        root_names(&first.rows),
        TiedRoots::names(roots.goals.iter().take(2))
    );
    let next = window(ActivityWindow::After(first.rows.last().unwrap().identity())).await;
    assert!(next.has_before && next.has_after);
    assert_eq!(
        root_names(&next.rows),
        TiedRoots::names(roots.goals.iter().skip(2).take(2))
    );
    let revealed = window(ActivityWindow::At(next.rows.first().unwrap().identity())).await;
    assert!(!revealed.anchor_unavailable);
    assert_eq!(root_names(&revealed.rows), root_names(&next.rows));
    assert!(revealed.has_before && revealed.has_after);
    let previous = window(ActivityWindow::Before(
        next.rows.first().unwrap().identity(),
    ))
    .await;
    assert_eq!(root_names(&previous.rows), root_names(&first.rows));
    assert!(!previous.has_before && previous.has_after);
    assert!(!previous.anchor_unavailable);
}

#[tokio::test]
async fn root_windows_keep_their_rows_when_new_roots_arrive() {
    let f = Fixture::new().await;
    TiedRoots::new(&f).await;
    let scope = HistoryScope::Personal(None);
    let first = f
        .tasks
        .activity_roots(&f.runs, &f.actor, &scope, &ActivityWindow::Edge, 2)
        .await
        .unwrap();
    let after = ActivityWindow::After(first.rows.last().unwrap().identity());
    let next = f
        .tasks
        .activity_roots(&f.runs, &f.actor, &scope, &after, 2)
        .await
        .unwrap();
    f.tasks
        .start(
            &f.actor,
            &f.workspace,
            TaskIntent::parse("New arrival").unwrap(),
            20,
        )
        .await
        .unwrap();
    let refreshed = f
        .tasks
        .activity_roots(&f.runs, &f.actor, &scope, &after, 2)
        .await
        .unwrap();
    assert!(refreshed.has_before && refreshed.has_after);
    assert!(!refreshed.anchor_unavailable);
    assert_eq!(root_names(&refreshed.rows), root_names(&next.rows));
    let retained = ActivityWindow::Records(next.rows.iter().map(ActivityRoot::identity).collect());
    let reading = f
        .tasks
        .activity_roots(&f.runs, &f.actor, &scope, &retained, 2)
        .await
        .unwrap();
    assert_eq!(
        root_names(&reading.rows),
        root_names(&next.rows),
        "incoming roots cannot evict retained readers"
    );
    assert!(reading.has_before && reading.has_after);
    assert!(!reading.anchor_unavailable);
    assert!(matches!(
        f.tasks
            .activity_roots(&f.runs, &f.actor, &scope, &retained, 1)
            .await,
        Err(Error::InvalidInput(_))
    ));
    for size in [0, 101] {
        assert!(matches!(
            f.tasks
                .activity_roots(&f.runs, &f.actor, &scope, &ActivityWindow::Edge, size)
                .await,
            Err(Error::InvalidInput(_))
        ));
    }
}

#[tokio::test]
async fn root_windows_conceal_another_users_private_anchor() {
    let f = Fixture::new().await;
    TiedRoots::new(&f).await;
    let scope = HistoryScope::Personal(None);
    let owner = f.workspace_user("root-owner", WorkspaceRole::Owner).await;
    let private = f
        .tasks
        .start(
            &AuthenticatedPrincipal::user(owner),
            &f.workspace,
            TaskIntent::parse("Other User's private goal").unwrap(),
            30,
        )
        .await
        .unwrap();
    let identity = ActivityIdentity::Task(f.workspace.clone(), private.id().clone());
    for window in [
        ActivityWindow::At(identity.clone()),
        ActivityWindow::Records(vec![identity]),
    ] {
        let concealed = f
            .tasks
            .activity_roots(&f.runs, &f.actor, &scope, &window, 2)
            .await
            .unwrap();
        assert!(concealed.anchor_unavailable && concealed.rows.is_empty());
        assert!(!concealed.has_before && !concealed.has_after);
    }
}

#[tokio::test]
async fn root_windows_reauthorize_rows_and_anchors_after_membership_loss() {
    let f = Fixture::new().await;
    let roots = TiedRoots::new(&f).await;
    let scope = HistoryScope::Personal(None);
    let owner = f.workspace_user("root-owner", WorkspaceRole::Owner).await;
    coral_administration::AccessLifecycleModule::new(
        f.tasks.database.clone(),
        f.tasks.workspaces.clone(),
    )
    .remove_workspace_member(
        &ChangeContext::new(owner, 40, coral_test_support::trace('1')),
        &f.workspace,
        f.actor.authenticated_user().unwrap().user_id(),
    )
    .await
    .unwrap();
    let lost_task = roots
        .goals
        .iter()
        .find(|goal| goal.workspace_id() == &f.workspace)
        .unwrap()
        .id()
        .clone();
    let lost = ActivityIdentity::Task(f.workspace.clone(), lost_task.clone());
    let surviving = roots.in_second().next().unwrap();
    let retained = f
        .tasks
        .activity_roots(
            &f.runs,
            &f.actor,
            &scope,
            &ActivityWindow::Records(vec![
                lost.clone(),
                ActivityIdentity::Task(surviving.workspace_id().clone(), surviving.id().clone()),
            ]),
            2,
        )
        .await
        .unwrap();
    assert_eq!(
        root_names(&retained.rows),
        TiedRoots::names([surviving]),
        "retained references do not authorize lost content or fill the gap"
    );
    assert!(!retained.anchor_unavailable);
    assert!(!retained.has_before && retained.has_after);
    for window in [
        ActivityWindow::At(lost.clone()),
        ActivityWindow::Records(vec![lost]),
    ] {
        let unavailable = f
            .tasks
            .activity_roots(&f.runs, &f.actor, &scope, &window, 2)
            .await
            .unwrap();
        assert!(unavailable.anchor_unavailable && unavailable.rows.is_empty());
        assert!(!unavailable.has_before && !unavailable.has_after);
    }
    assert!(matches!(
        f.tasks
            .activity_interactions(
                &f.runs,
                &f.actor,
                &TaskIdentity {
                    workspace: f.workspace.clone(),
                    task: lost_task,
                },
                &InteractionWindow::Start,
                2,
                None
            )
            .await,
        Err(Error::TaskNotFound)
    ));
    let remaining = f
        .tasks
        .activity_roots(&f.runs, &f.actor, &scope, &ActivityWindow::Edge, 100)
        .await
        .unwrap();
    assert_eq!(
        root_names(&remaining.rows),
        TiedRoots::names(roots.in_second())
    );
    assert!(!remaining.has_before && !remaining.has_after && !remaining.anchor_unavailable);
}

#[tokio::test]
async fn empty_root_windows_distinguish_boundaries_from_lost_retained_identities() {
    let f = Fixture::new().await;
    let scope = HistoryScope::Personal(None);
    for window in [ActivityWindow::Edge, ActivityWindow::Records(vec![])] {
        let empty = f
            .tasks
            .activity_roots(&f.runs, &f.actor, &scope, &window, 10)
            .await
            .unwrap();
        assert!(empty.rows.is_empty());
        assert!(!empty.anchor_unavailable && !empty.has_before && !empty.has_after);
    }
    let goal = f.goal().await;
    let identity = ActivityIdentity::Task(f.workspace.clone(), goal.id().clone());
    for window in [
        ActivityWindow::Before(identity.clone()),
        ActivityWindow::After(identity.clone()),
        ActivityWindow::Records(vec![]),
    ] {
        let empty = f
            .tasks
            .activity_roots(&f.runs, &f.actor, &scope, &window, 10)
            .await
            .unwrap();
        assert!(empty.rows.is_empty());
        assert!(!empty.anchor_unavailable && !empty.has_before && !empty.has_after);
    }
    let retained = f
        .tasks
        .activity_roots(
            &f.runs,
            &f.actor,
            &scope,
            &ActivityWindow::Records(vec![identity.clone()]),
            10,
        )
        .await
        .unwrap();
    assert!(
        matches!(retained.rows.as_slice(), [ActivityRoot::Task(task)] if task.id() == goal.id())
    );
    assert!(!retained.anchor_unavailable && !retained.has_before && !retained.has_after);
    assert!(
        f.tasks
            .expire_group(&f.workspace, goal.id(), 20 + HISTORY_RETENTION_NANOS)
            .await
            .unwrap()
    );
    for (window, unavailable) in [
        (ActivityWindow::Edge, false),
        (ActivityWindow::Records(vec![]), false),
        (ActivityWindow::Records(vec![identity.clone()]), true),
        (ActivityWindow::At(identity.clone()), true),
        (ActivityWindow::Before(identity.clone()), true),
        (ActivityWindow::After(identity), true),
    ] {
        let empty = f
            .tasks
            .activity_roots(&f.runs, &f.actor, &scope, &window, 10)
            .await
            .unwrap();
        assert!(empty.rows.is_empty());
        assert_eq!(empty.anchor_unavailable, unavailable);
        assert!(!empty.has_before && !empty.has_after);
    }
}

#[tokio::test]
async fn root_and_child_reads_do_not_renew_retention_and_expired_anchors_do_not_fall_back() {
    let f = Fixture::new().await;
    let goal = f.goal().await;
    let identity = TaskIdentity {
        workspace: f.workspace.clone(),
        task: goal.id().clone(),
    };
    let scope = HistoryScope::Personal(None);
    let roots = f
        .tasks
        .activity_roots(&f.runs, &f.actor, &scope, &ActivityWindow::Edge, 10)
        .await
        .unwrap();
    let children = f
        .tasks
        .activity_interactions(
            &f.runs,
            &f.actor,
            &identity,
            &InteractionWindow::Start,
            10,
            Some(&scope),
        )
        .await
        .unwrap();
    assert!(matches!(roots.rows.as_slice(), [ActivityRoot::Task(task)] if task.id() == goal.id()));
    assert!(!roots.has_before && !roots.has_after && !roots.anchor_unavailable);
    assert!(children.rows.is_empty() && children.next_after.is_none());
    assert!(
        f.tasks
            .expire_group(&f.workspace, goal.id(), 20 + HISTORY_RETENTION_NANOS)
            .await
            .unwrap()
    );
    let expired = f
        .tasks
        .activity_roots(
            &f.runs,
            &f.actor,
            &scope,
            &ActivityWindow::At(roots.rows.first().unwrap().identity()),
            10,
        )
        .await
        .unwrap();
    assert!(!expired.has_before && !expired.has_after);
    assert!(expired.anchor_unavailable && expired.rows.is_empty());
    assert!(matches!(
        f.tasks
            .activity_interactions(
                &f.runs,
                &f.actor,
                &identity,
                &InteractionWindow::Start,
                10,
                Some(&scope)
            )
            .await,
        Err(Error::TaskNotFound)
    ));
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one Agent hierarchy verifies Owner-only authority, exact scope, deletion and owner demotion"
)]
async fn agent_hierarchy_remains_owner_only_after_deletion_and_never_joins_personal_work() {
    let f = Fixture::new().await;
    let owner = f.actor.authenticated_user().unwrap().clone();
    let issued = f
        .tasks
        .workspaces
        .create_agent(
            &ChangeContext::new(owner.clone(), 1, coral_test_support::trace('1')),
            &f.workspace,
            coral_workspace::AgentDisplayName::parse("Worker").unwrap(),
            None,
        )
        .await
        .unwrap();
    let agent = f
        .tasks
        .workspaces
        .authenticate_agent(&f.workspace, issued.credential.expose(), 2)
        .await
        .unwrap();
    let goal = f
        .tasks
        .start(
            &agent,
            &f.workspace,
            TaskIntent::parse("Agent goal").unwrap(),
            3,
        )
        .await
        .unwrap();
    let call = AttributedCommand::new(
        agent.clone(),
        f.workspace.clone(),
        goal.id().clone(),
        TaskIntent::parse("Agent submission").unwrap(),
        coral_test_support::trace('1'),
    );
    let run = f
        .commands
        .submit(&call, "return 42;", 60_000)
        .await
        .unwrap();
    let second = f
        .workspace_user("second-history-owner", WorkspaceRole::Owner)
        .await;
    let second_owner = AuthenticatedPrincipal::user(second.clone());
    let scope = HistoryScope::Agent {
        workspace: f.workspace.clone(),
        agent: agent.principal().id().clone(),
    };
    let task = TaskIdentity {
        workspace: f.workspace.clone(),
        task: goal.id().clone(),
    };
    for actor in [&f.actor, &second_owner] {
        let roots = f
            .tasks
            .activity_roots(&f.runs, actor, &scope, &ActivityWindow::Edge, 10)
            .await
            .unwrap();
        assert!(
            matches!(roots.rows.as_slice(), [ActivityRoot::Task(task)] if task.id() == goal.id())
        );
        assert!(!roots.has_before && !roots.has_after && !roots.anchor_unavailable);
        let page = f
            .tasks
            .activity_interactions(
                &f.runs,
                actor,
                &task,
                &InteractionWindow::Start,
                1,
                Some(&scope),
            )
            .await
            .unwrap();
        assert_eq!(page.rows.len(), 1);
        assert!(page.next_after.is_none());
        assert_eq!(
            page.rows.first().unwrap().interaction.principal,
            *agent.principal()
        );
        assert!(
            f.tasks
                .activity_roots(
                    &f.runs,
                    actor,
                    &HistoryScope::Personal(None),
                    &ActivityWindow::Edge,
                    10
                )
                .await
                .unwrap()
                .rows
                .is_empty()
        );
        assert!(matches!(
            f.tasks
                .activity_interactions(
                    &f.runs,
                    actor,
                    &task,
                    &InteractionWindow::Start,
                    1,
                    Some(&HistoryScope::Personal(None))
                )
                .await,
            Err(Error::TaskNotFound)
        ));
    }
    // An Organization Owner without a Membership is denied like any non-Member.
    for (id, role) in [
        ("organization-owner", OrganizationRole::Owner),
        ("organization-member", OrganizationRole::Member),
    ] {
        let non_member =
            AuthenticatedPrincipal::user(provision_user(f.tasks.database.clone(), id, role).await);
        assert!(matches!(
            f.tasks
                .activity_roots(&f.runs, &non_member, &scope, &ActivityWindow::Edge, 10)
                .await,
            Err(Error::Workspace(coral_workspace::Error::WorkspaceNotFound))
        ));
        assert!(matches!(
            f.tasks
                .activity_interactions(
                    &f.runs,
                    &non_member,
                    &task,
                    &InteractionWindow::Start,
                    1,
                    Some(&scope),
                )
                .await,
            Err(Error::Workspace(coral_workspace::Error::WorkspaceNotFound))
        ));
    }
    assert!(matches!(
        f.tasks
            .activity_roots(&f.runs, &agent, &scope, &ActivityWindow::Edge, 10)
            .await,
        Err(Error::Workspace(coral_workspace::Error::PermissionDenied(
            _
        )))
    ));
    assert!(matches!(
        f.tasks
            .inspect_run(
                &f.runs,
                &agent,
                &f.workspace,
                run.run_id(),
                &coral_run::RunInspection {
                    event_limit: 1,
                    ..Default::default()
                }
            )
            .await,
        Err(Error::Run(coral_run::Error::RunNotFound))
    ));
    let wrong_agent = HistoryScope::Agent {
        workspace: f.workspace.clone(),
        agent: coral_workspace::PrincipalId::parse("other-agent").unwrap(),
    };
    assert!(matches!(
        f.tasks
            .activity_interactions(
                &f.runs,
                &f.actor,
                &task,
                &InteractionWindow::Start,
                1,
                Some(&wrong_agent)
            )
            .await,
        Err(Error::TaskNotFound)
    ));
    f.tasks
        .workspaces
        .set_membership(
            &ChangeContext::new(second.clone(), 10, coral_test_support::trace('1')),
            &f.workspace,
            &owner,
            WorkspaceRole::Member,
        )
        .await
        .unwrap();
    assert!(matches!(
        f.tasks
            .activity_roots(&f.runs, &f.actor, &scope, &ActivityWindow::Edge, 10)
            .await,
        Err(Error::Workspace(coral_workspace::Error::PermissionDenied(
            _
        )))
    ));
    assert!(matches!(
        f.tasks
            .inspect_run(
                &f.runs,
                &f.actor,
                &f.workspace,
                run.run_id(),
                &coral_run::RunInspection {
                    event_limit: 1,
                    ..Default::default()
                }
            )
            .await,
        Err(Error::Run(coral_run::Error::RunNotFound))
    ));
    coral_administration::AccessLifecycleModule::new(
        f.tasks.database.clone(),
        f.tasks.workspaces.clone(),
    )
    .delete_agent(
        &ChangeContext::new(second, 20, coral_test_support::trace('1')),
        &f.workspace,
        agent.principal().id(),
    )
    .await
    .unwrap();
    let roots = f
        .tasks
        .activity_roots(&f.runs, &second_owner, &scope, &ActivityWindow::Edge, 10)
        .await
        .unwrap();
    assert!(matches!(roots.rows.as_slice(), [ActivityRoot::Task(task)] if task.id() == goal.id()));
    assert!(!roots.has_before && !roots.has_after && !roots.anchor_unavailable);
    let retained = f
        .tasks
        .inspect_run(
            &f.runs,
            &second_owner,
            &f.workspace,
            run.run_id(),
            &coral_run::RunInspection {
                event_limit: 1,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(retained.execution.submitted_source, "return 42;");
    assert_eq!(retained.submission.unwrap().task.task, *goal.id());
}
