//! Intent search: which roots and children a query keeps, on both databases.
use super::*;
use coral_workspace::HistoryScope;

impl Fixture {
    /// Starts a goal one tick after the previous one, so roots order by start.
    async fn goal_with(&self, intent: &str) -> Task {
        static CLOCK: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(20);
        self.tasks
            .start(
                &self.actor,
                &self.workspace,
                TaskIntent::parse(intent).unwrap(),
                CLOCK.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            )
            .await
            .unwrap()
    }

    async fn standalone_run(&self) -> coral_run::RunId {
        self.runs
            .start(
                &self.actor,
                &self.workspace,
                coral_run::RunSubmission::new(
                    coral_run::ProgramSource::parse("return 1;").unwrap(),
                    coral_run::RunTimeout::from_millis(60_000).unwrap(),
                ),
            )
            .await
            .unwrap()
            .run_id()
            .clone()
    }

    async fn searched_roots(
        &self,
        query: &str,
        window: &ActivityWindow,
        limit: u16,
    ) -> ActivityRoots {
        self.tasks
            .activity_roots(
                &self.runs,
                &self.actor,
                &HistoryScope::Personal(None),
                window,
                limit,
                Some(&IntentQuery::parse(query).unwrap()),
            )
            .await
            .unwrap()
    }
}

fn task_matches(rows: &[ActivityRoot]) -> Vec<(TaskId, Option<bool>)> {
    rows.iter()
        .map(|row| match row {
            ActivityRoot::Task(task) => (task.id().clone(), task.intent_matches()),
            ActivityRoot::Run(run) => panic!("standalone Run {} matched a query", run.id),
        })
        .collect()
}

#[tokio::test]
async fn a_query_keeps_tasks_whose_intent_or_run_submission_intent_contains_it() {
    let f = Fixture::new().await;
    let by_task = f.goal_with("Deploy the BILLING service").await;
    let by_run = f.goal_with("Weekly chores").await;
    f.commands
        .submit(&f.call(&by_run, "Rotate billing keys"), "return 1;", 60_000)
        .await
        .unwrap();
    let by_discovery = f.goal_with("Unrelated goal").await;
    f.commands
        .search(
            &f.call(&by_discovery, "Find billing tools"),
            coral_search::SearchInput::default(),
        )
        .await
        .unwrap();
    f.goal_with("Nothing to see").await;
    f.standalone_run().await;

    let page = f
        .searched_roots("  BiLLing ", &ActivityWindow::Edge, 10)
        .await;

    assert_eq!(
        task_matches(&page.rows),
        [
            (by_run.id().clone(), Some(false)),
            (by_task.id().clone(), Some(true)),
        ],
        "a discovery call's intent, a standalone Run and other goals never match"
    );
    assert!(!page.has_before && !page.has_after && !page.anchor_unavailable);
}

#[tokio::test]
async fn queries_match_literally_and_fold_only_ascii_case() {
    let f = Fixture::new().await;
    let percent = f.goal_with("Raise quota to 50% now").await;
    let underscore = f.goal_with("Rename deploy_url").await;
    let accented = f.goal_with("Été report").await;
    f.goal_with("Raise quota to 500 now").await;
    f.goal_with("Rename deployXurl").await;
    for (query, expected) in [
        ("50% n", vec![percent.id().clone()]),
        ("DEPLOY_", vec![underscore.id().clone()]),
        ("été", vec![]),
        ("Été REPORT", vec![accented.id().clone()]),
    ] {
        let page = f.searched_roots(query, &ActivityWindow::Edge, 10).await;
        let ids: Vec<_> = task_matches(&page.rows)
            .into_iter()
            .map(|(id, _)| id)
            .collect();
        assert_eq!(ids, expected, "query {query:?}");
    }
}

#[test]
fn a_query_needs_three_to_1024_characters_after_trimming() {
    for (value, accepted) in [
        ("  ab  ", false),
        ("   ", false),
        ("a\0b", false),
        (&"x".repeat(1025), false),
        ("  abc  ", true),
        ("Été", true),
        (&"x".repeat(1024), true),
    ] {
        assert_eq!(
            IntentQuery::parse(value).is_ok(),
            accepted,
            "query {value:?}"
        );
        if !accepted {
            assert!(matches!(
                IntentQuery::parse(value),
                Err(Error::InvalidInput(_))
            ));
        }
    }
}

/// A goal whose intent matches "billing", with a discovery call and four Runs.
async fn billing_chores(f: &Fixture) -> (TaskIdentity, [coral_run::RunId; 4]) {
    let goal = f.goal_with("Weekly billing chores").await;
    f.commands
        .search(
            &f.call(&goal, "Find billing tools"),
            coral_search::SearchInput::default(),
        )
        .await
        .unwrap();
    let mut runs = Vec::new();
    for intent in [
        "Rotate keys",
        "Export INVOICES",
        "Archive invoices",
        "Tidy up",
    ] {
        runs.push(
            f.commands
                .submit(&f.call(&goal, intent), "return 1;", 60_000)
                .await
                .unwrap()
                .run_id()
                .clone(),
        );
    }
    let runs: [coral_run::RunId; 4] = runs.try_into().unwrap();
    let identity = TaskIdentity {
        workspace: f.workspace.clone(),
        task: goal.id().clone(),
    };
    (identity, runs)
}

async fn searched_children(
    f: &Fixture,
    identity: &TaskIdentity,
    query: &str,
    window: InteractionWindow,
    limit: u16,
) -> ActivityInteractions {
    f.tasks
        .activity_interactions(
            &f.runs,
            &f.actor,
            identity,
            &window,
            limit,
            None,
            Some(&IntentQuery::parse(query).unwrap()),
        )
        .await
        .unwrap()
}

fn intents(page: &ActivityInteractions) -> Vec<String> {
    page.rows
        .iter()
        .map(|row| row.interaction.intent.as_str().to_owned())
        .collect()
}

#[tokio::test]
async fn a_matching_task_keeps_every_child_and_a_run_match_keeps_only_matching_runs() {
    let f = Fixture::new().await;
    let (identity, runs) = billing_chores(&f).await;
    let read = |query: &'static str, window: InteractionWindow, limit: u16| {
        searched_children(&f, &identity, query, window, limit)
    };
    let whole = read("billing", InteractionWindow::Start, 100).await;
    assert_eq!(whole.task.intent_matches(), Some(true));
    assert_eq!(
        whole.total, 5,
        "a matching Task keeps its discovery call and every Run"
    );

    let first = read("invoices", InteractionWindow::Start, 1).await;
    assert_eq!(first.task.intent_matches(), Some(false));
    assert_eq!(intents(&first), ["Export INVOICES"]);
    assert_eq!((first.total, first.offset), (2, 0));
    let next = read(
        "invoices",
        InteractionWindow::After(first.next_after.clone().unwrap()),
        1,
    )
    .await;
    assert_eq!(intents(&next), ["Archive invoices"]);
    assert_eq!((next.offset, next.next_after.is_none()), (1, true));
    let revealed = read("invoices", InteractionWindow::Run(runs[2].clone()), 1).await;
    assert_eq!(
        (intents(&revealed), revealed.offset),
        (vec!["Archive invoices".to_owned()], 1)
    );
    let viewport = read(
        "invoices",
        InteractionWindow::Viewport {
            offset: 5,
            anchor: None,
            preceding: 0,
        },
        2,
    )
    .await;
    assert_eq!(intents(&viewport), ["Export INVOICES", "Archive invoices"]);

    let none = read("nothing here", InteractionWindow::Start, 10).await;
    assert_eq!((none.total, none.rows.len()), (0, 0));
}

#[tokio::test]
async fn a_child_the_query_leaves_out_anchors_no_viewport() {
    let f = Fixture::new().await;
    let (identity, _) = billing_chores(&f).await;
    let read = |query: &'static str, window: InteractionWindow, limit: u16| {
        searched_children(&f, &identity, query, window, limit)
    };
    let excluded = f
        .tasks
        .activity_interactions(
            &f.runs,
            &f.actor,
            &identity,
            &InteractionWindow::Start,
            100,
            None,
            None,
        )
        .await
        .unwrap()
        .rows
        .into_iter()
        .find(|row| row.interaction.intent.as_str() == "Rotate keys")
        .unwrap()
        .interaction
        .id;
    let unanchored = read(
        "invoices",
        InteractionWindow::Viewport {
            offset: 1,
            anchor: Some(excluded),
            preceding: 0,
        },
        1,
    )
    .await;
    assert!(
        unanchored.anchor_unavailable,
        "a child the query leaves out anchors nothing"
    );
    assert_eq!(intents(&unanchored), ["Archive invoices"]);
}

#[tokio::test]
async fn query_windows_page_only_through_matching_roots_the_caller_may_read() {
    let f = Fixture::new().await;
    let owner = f
        .workspace_user("search-owner", coral_workspace::WorkspaceRole::Owner)
        .await;
    let mut matching = Vec::new();
    let mut other = Vec::new();
    for index in 0..4 {
        matching.push(f.goal_with(&format!("Billing step {index}")).await);
        other.push(f.goal_with(&format!("Unrelated step {index}")).await);
        f.tasks
            .start(
                &AuthenticatedPrincipal::user(owner.clone()),
                &f.workspace,
                TaskIntent::parse("Another User's billing").unwrap(),
                100 + index,
            )
            .await
            .unwrap();
    }
    f.standalone_run().await;
    matching.reverse();
    let matching: [Task; 4] = matching.try_into().unwrap();
    let other: [Task; 4] = other.try_into().unwrap();
    let expected = |range: std::ops::Range<usize>| -> Vec<(TaskId, Option<bool>)> {
        matching
            .iter()
            .skip(range.start)
            .take(range.len())
            .map(|task| (task.id().clone(), Some(true)))
            .collect()
    };
    let identity = |task: &Task| ActivityIdentity::Task(f.workspace.clone(), task.id().clone());

    let first = f.searched_roots("billing", &ActivityWindow::Edge, 2).await;
    assert_eq!(task_matches(&first.rows), expected(0..2));
    assert!(!first.has_before && first.has_after);
    let next = f
        .searched_roots("billing", &ActivityWindow::After(identity(&matching[1])), 2)
        .await;
    assert_eq!(task_matches(&next.rows), expected(2..4));
    assert!(next.has_before && !next.has_after);
    let previous = f
        .searched_roots(
            "billing",
            &ActivityWindow::Before(identity(&matching[2])),
            2,
        )
        .await;
    assert_eq!(task_matches(&previous.rows), expected(0..2));
    let revealed = f
        .searched_roots("billing", &ActivityWindow::At(identity(&matching[1])), 2)
        .await;
    assert_eq!(task_matches(&revealed.rows), expected(1..3));

    let excluded = f
        .searched_roots("billing", &ActivityWindow::At(identity(&other[0])), 2)
        .await;
    assert!(excluded.anchor_unavailable && excluded.rows.is_empty());
    let retained = f
        .searched_roots(
            "billing",
            &ActivityWindow::Records(vec![identity(&other[1]), identity(&matching[3])]),
            2,
        )
        .await;
    assert_eq!(
        task_matches(&retained.rows),
        [(matching[3].id().clone(), Some(true))]
    );
}
