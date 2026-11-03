use super::*;

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one mixed-root journey checks ordering, both page directions, missing anchors and private-work isolation"
)]
async fn activity_roots_page_tasks_and_standalone_runs_without_duplicating_accepted_exec() {
    let fixture = Fixture::new().await;
    let goal = fixture.goal().await;
    fixture
        .commands
        .submit(&fixture.call(&goal, "accepted exec"), "return 42;", 60_000)
        .await
        .unwrap();
    let standalone = fixture
        .runs
        .start(
            &fixture.actor,
            &fixture.workspace,
            coral_run::RunSubmission::new(
                coral_run::ProgramSource::parse("return 1;").unwrap(),
                coral_run::RunTimeout::from_millis(60_000).unwrap(),
            ),
        )
        .await
        .unwrap();
    let scope = coral_workspace::HistoryScope::Personal(None);
    let page = fixture
        .tasks
        .activity_roots(
            &fixture.runs,
            &fixture.actor,
            &scope,
            &crate::ActivityWindow::Edge,
            1,
            None,
        )
        .await
        .unwrap();
    assert!(!page.has_before && page.has_after && !page.anchor_unavailable);
    assert_eq!(page.rows.len(), 1);
    let revealed = fixture
        .tasks
        .activity_roots(
            &fixture.runs,
            &fixture.actor,
            &scope,
            &crate::ActivityWindow::At(page.rows.first().unwrap().identity()),
            2,
            None,
        )
        .await
        .unwrap();
    assert!(!revealed.anchor_unavailable);
    assert!(
        matches!(revealed.rows.last().unwrap(), crate::ActivityRoot::Task(task) if task.id() == goal.id())
    );
    assert_eq!(revealed.rows.len(), 2);
    assert!(!revealed.has_before && !revealed.has_after);
    assert!(
        matches!(revealed.rows.first().unwrap(), crate::ActivityRoot::Run(run) if &run.id == standalone.run_id())
    );
    assert!(
        matches!(page.rows.first().unwrap(), crate::ActivityRoot::Run(run) if &run.id == standalone.run_id())
    );
    let next = fixture
        .tasks
        .activity_roots(
            &fixture.runs,
            &fixture.actor,
            &scope,
            &crate::ActivityWindow::After(page.rows.first().unwrap().identity()),
            1,
            None,
        )
        .await
        .unwrap();
    assert!(
        matches!(next.rows.first().unwrap(), crate::ActivityRoot::Task(task) if task.id() == goal.id())
    );
    assert!(next.has_before);
    assert!(!next.has_after);
    let previous = fixture
        .tasks
        .activity_roots(
            &fixture.runs,
            &fixture.actor,
            &scope,
            &crate::ActivityWindow::Before(next.rows.first().unwrap().identity()),
            1,
            None,
        )
        .await
        .unwrap();
    assert!(
        matches!(previous.rows.first().unwrap(), crate::ActivityRoot::Run(run) if &run.id == standalone.run_id())
    );
    let unavailable = fixture
        .tasks
        .activity_roots(
            &fixture.runs,
            &fixture.actor,
            &scope,
            &crate::ActivityWindow::After(crate::ActivityIdentity::Run(
                fixture.workspace.clone(),
                coral_run::RunId::parse("00000000-0000-4000-8000-000000000099").unwrap(),
            )),
            1,
            None,
        )
        .await
        .unwrap();
    assert!(unavailable.anchor_unavailable);
    assert!(unavailable.rows.is_empty());
    // Workspace Owner authority does not reveal another User's private work.
    let hidden = fixture
        .workspace_user("root-other-owner", coral_workspace::WorkspaceRole::Owner)
        .await;
    let hidden = fixture
        .tasks
        .activity_roots(
            &fixture.runs,
            &AuthenticatedPrincipal::user(hidden),
            &scope,
            &crate::ActivityWindow::Edge,
            10,
            None,
        )
        .await
        .unwrap();
    assert!(!hidden.has_before && !hidden.has_after && !hidden.anchor_unavailable);
    assert!(hidden.rows.is_empty());
}

#[tokio::test]
async fn a_failed_run_is_still_an_accepted_submission_child() {
    // coral-run owns the Run's entries, emissions, and exact-event privacy.
    let fixture = Fixture::new().await;
    let goal = fixture.goal().await;
    let run = fixture
        .commands
        .submit(
            &fixture.call(&goal, "emit then fail"),
            "coral.emit({kept: 24}); throw new Error('later failure');",
            60_000,
        )
        .await
        .unwrap();
    assert_eq!(
        fixture.wait_for_terminal(run.run_id()).await.state(),
        coral_run::RunState::Failed
    );
    let page = fixture
        .tasks
        .activity_interactions(
            &fixture.runs,
            &fixture.actor,
            &TaskIdentity {
                workspace: fixture.workspace.clone(),
                task: goal.id().clone(),
            },
            &InteractionWindow::Start,
            10,
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(page.rows.len(), 1);
    assert!(page.next_after.is_none());
    let child = page.rows.first().unwrap();
    assert_eq!(child.interaction.outcome, AttemptOutcome::Succeeded);
    assert_eq!(
        child.run.as_ref().unwrap().state,
        coral_run::RunState::Failed
    );
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one real Task and Run verify bounded child seeking beyond both first pages"
)]
async fn child_anchors_seek_later_history_without_loading_preceding_pages() {
    let fixture = Fixture::new().await;
    let goal = fixture.goal().await;
    for _ in 0..27 {
        fixture
            .commands
            .search(
                &fixture.call(&goal, "record discovery"),
                coral_search::SearchInput::default(),
            )
            .await
            .unwrap();
    }
    let accepted = fixture
        .commands
        .submit(
            &fixture.call(&goal, "later exec"),
            "for (let i = 0; i < 30; i++) coral.emit({i}); return 42;",
            60_000,
        )
        .await
        .unwrap();
    let identity = TaskIdentity {
        workspace: fixture.workspace.clone(),
        task: goal.id().clone(),
    };
    let first = fixture
        .tasks
        .activity_interactions(
            &fixture.runs,
            &fixture.actor,
            &identity,
            &InteractionWindow::Start,
            25,
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(first.rows.len(), 25);
    assert!(first.rows.iter().all(|entry| entry.run.is_none()));
    assert!(first.next_after.is_some());
    let anchored = fixture
        .tasks
        .activity_interactions(
            &fixture.runs,
            &fixture.actor,
            &identity,
            &InteractionWindow::Run(accepted.run_id().clone()),
            1,
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(anchored.rows.len(), 1);
    assert_eq!(anchored.offset, 27);
    assert!(anchored.next_after.is_none());
    let browse = fixture
        .tasks
        .activity_run_entries(
            &fixture.runs,
            &fixture.actor,
            &fixture.workspace,
            accepted.run_id(),
            &coral_run::RunEntryWindow::Start,
            1,
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        browse.submission_position,
        Some(27),
        "browse locates an authorized submission outside the loaded Task window"
    );
    let exact = fixture
        .tasks
        .inspect_run(
            &fixture.runs,
            &fixture.actor,
            &fixture.workspace,
            accepted.run_id(),
            &coral_run::RunInspection {
                event_limit: 1,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        browse.submission.unwrap().interaction.id,
        exact.submission.unwrap().interaction.id,
        "exact evidence keeps the same independently authorized association"
    );
    assert_eq!(
        &anchored.rows.first().unwrap().run.as_ref().unwrap().id,
        accepted.run_id()
    );
    assert_eq!(
        fixture.wait_for_terminal(accepted.run_id()).await.state(),
        coral_run::RunState::Succeeded
    );
    let output = fixture
        .runs
        .activity_entries(
            &fixture.actor,
            &fixture.workspace,
            accepted.run_id(),
            &coral_run::RunEntryWindow::Event(31),
            1,
            None,
        )
        .await
        .unwrap();
    assert_eq!(output.entries.len(), 1);
    assert!(
        matches!(output.entries.first().unwrap(), coral_run::RunEntry::Event { sequence: 31, kind, .. } if kind == "program.emitted")
    );
    let last = fixture
        .runs
        .activity_entries(
            &fixture.actor,
            &fixture.workspace,
            accepted.run_id(),
            &coral_run::RunEntryWindow::Final,
            1,
            None,
        )
        .await
        .unwrap();
    assert!(last.has_final_value);
    assert!(!last.has_more);
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one mixed standalone/attributed scenario verifies identity, Workspace qualification, and optional navigation privacy"
)]
async fn scoped_task_children_and_exact_execution_reads_keep_associations_private_and_optional() {
    let fixture = Fixture::new().await;
    let first = fixture.goal().await;
    let equal_intent = fixture.goal().await;
    let submission = || {
        coral_run::RunSubmission::new(
            coral_run::ProgramSource::parse("return 42;").unwrap(),
            coral_run::RunTimeout::from_millis(60_000).unwrap(),
        )
    };
    let attributed = fixture
        .commands
        .submit(&fixture.call(&first, "first attempt"), "return 42;", 60_000)
        .await
        .unwrap();
    fixture
        .commands
        .submit(
            &fixture.call(&equal_intent, "separate goal"),
            "return 42;",
            60_000,
        )
        .await
        .unwrap();
    let standalone = fixture
        .runs
        .start(&fixture.actor, &fixture.workspace, submission())
        .await
        .unwrap();
    let scope = coral_workspace::HistoryScope::Personal(None);
    let all = fixture
        .tasks
        .activity_roots(
            &fixture.runs,
            &fixture.actor,
            &scope,
            &ActivityWindow::Edge,
            25,
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        all.rows.len(),
        3,
        "two distinct goals and one standalone execution"
    );
    assert!(!all.has_before && !all.has_after && !all.anchor_unavailable);
    for goal in [&first, &equal_intent] {
        assert!(
            all.rows
                .iter()
                .any(|row| matches!(row, ActivityRoot::Task(task) if task.id() == goal.id()))
        );
    }
    assert!(
        all.rows
            .iter()
            .any(|row| matches!(row, ActivityRoot::Run(run) if &run.id == standalone.run_id()))
    );
    let exact = TaskIdentity {
        workspace: fixture.workspace.clone(),
        task: first.id().clone(),
    };
    let filtered = fixture
        .tasks
        .activity_interactions(
            &fixture.runs,
            &fixture.actor,
            &exact,
            &InteractionWindow::Start,
            25,
            Some(&scope),
            None,
        )
        .await
        .unwrap();
    assert_eq!(filtered.rows.len(), 1);
    assert_eq!(
        &filtered.rows.first().unwrap().run.as_ref().unwrap().id,
        attributed.run_id()
    );
    let history = fixture
        .tasks
        .inspect_run(
            &fixture.runs,
            &fixture.actor,
            &fixture.workspace,
            attributed.run_id(),
            &coral_run::RunInspection {
                event_limit: 1,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let association = history.submission.unwrap();
    assert_eq!(association.task.task, *first.id());
    assert_eq!(association.interaction.intent.as_str(), "first attempt");
    assert_eq!(history.execution.submitted_source, "return 42;");
    let event = fixture
        .tasks
        .inspect_run_event(
            &fixture.runs,
            &fixture.actor,
            &fixture.workspace,
            attributed.run_id(),
            1,
        )
        .await
        .unwrap();
    assert_eq!(event.execution.run.id, *attributed.run_id());
    assert_eq!(event.execution.event.sequence(), 1);
    assert_eq!(event.submission.unwrap().task.task, *first.id());
    let children = fixture
        .tasks
        .activity_run_entries(
            &fixture.runs,
            &fixture.actor,
            &fixture.workspace,
            attributed.run_id(),
            &coral_run::RunEntryWindow::Start,
            25,
            Some(&scope),
        )
        .await
        .unwrap();
    assert_eq!(children.submission_position, Some(0));
    assert_eq!(
        children.submission.unwrap().interaction.id,
        association.interaction.id
    );
    let standalone_children = fixture
        .tasks
        .activity_run_entries(
            &fixture.runs,
            &fixture.actor,
            &fixture.workspace,
            standalone.run_id(),
            &coral_run::RunEntryWindow::Start,
            1,
            None,
        )
        .await
        .unwrap();
    assert!(standalone_children.submission.is_none());
    assert!(standalone_children.submission_position.is_none());
    let standalone = fixture
        .tasks
        .inspect_run(
            &fixture.runs,
            &fixture.actor,
            &fixture.workspace,
            standalone.run_id(),
            &coral_run::RunInspection {
                event_limit: 1,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(standalone.submission.is_none());
    let other = fixture
        .tasks
        .workspaces
        .create(
            &ChangeContext::new(
                fixture.actor.authenticated_user().unwrap().clone(),
                30,
                coral_test_support::trace('1'),
            ),
            WorkspaceDisplayName::parse("other").unwrap(),
        )
        .await
        .unwrap();
    let mismatched = TaskIdentity {
        workspace: other.id().clone(),
        task: first.id().clone(),
    };
    assert!(matches!(
        fixture
            .tasks
            .activity_interactions(
                &fixture.runs,
                &fixture.actor,
                &mismatched,
                &InteractionWindow::Start,
                25,
                Some(&scope),
                None
            )
            .await,
        Err(Error::TaskNotFound)
    ));
    assert!(matches!(
        fixture
            .tasks
            .inspect_run(
                &fixture.runs,
                &fixture.actor,
                other.id(),
                attributed.run_id(),
                &coral_run::RunInspection {
                    event_limit: 1,
                    ..Default::default()
                }
            )
            .await,
        Err(Error::Run(coral_run::Error::RunNotFound))
    ));
    assert!(matches!(
        fixture
            .tasks
            .activity_interactions(
                &fixture.runs,
                &fixture.actor,
                &exact,
                &InteractionWindow::Start,
                25,
                Some(&coral_workspace::HistoryScope::Personal(Some(
                    other.id().clone()
                ))),
                None
            )
            .await,
        Err(Error::TaskNotFound)
    ));
    // Workspace Owner authority does not reveal another User's private work.
    let hidden = fixture
        .workspace_user("other-reader", coral_workspace::WorkspaceRole::Owner)
        .await;
    let hidden = AuthenticatedPrincipal::user(hidden);
    assert!(matches!(
        fixture
            .tasks
            .activity_run_entries(
                &fixture.runs,
                &hidden,
                &fixture.workspace,
                attributed.run_id(),
                &coral_run::RunEntryWindow::Start,
                1,
                None,
            )
            .await,
        Err(Error::Run(coral_run::Error::RunNotFound))
    ));
    assert!(matches!(
        fixture
            .tasks
            .inspect_run(
                &fixture.runs,
                &hidden,
                &fixture.workspace,
                attributed.run_id(),
                &coral_run::RunInspection {
                    event_limit: 1,
                    ..Default::default()
                }
            )
            .await,
        Err(Error::Run(coral_run::Error::RunNotFound))
    ));
}
