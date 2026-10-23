//! Agent observation and human inspection are distinct Run-owned interfaces.
use super::*;

#[tokio::test(flavor = "multi_thread")]
async fn internal_invocations_do_not_end_waits_and_initial_wait_batches_authored_output() {
    let upstream = SequencedUpstream::new(HttpOutcome::new(200, serde_json::json!({})));
    let fixture = TestModules::with_upstream(
        Arc::new(crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default()).unwrap()),
        upstream.clone(),
    )
    .await;
    let operation = fixture
        .user_operation("user:lean-wait", "Meaningful waits")
        .await;
    let actor = &operation.principal;
    let workspace = &operation.workspace_id;
    let accepted = fixture.runs.start(actor, workspace, submission(
        "for(let i=0;i<3;i++) { await coral.providers.weather.getForecast({}); if(i===1) { await coral.emit('ready'); console.log('ready'); } } return 7;"
    )).await.unwrap();
    upstream.until_sent(1).await;
    let observation = immediate(Some(accepted.next_cursor().clone()), 100)
        .with_wait(Duration::from_secs(2))
        .unwrap();
    let pending = fixture
        .runs
        .observe(actor, workspace, accepted.run_id(), &observation);
    tokio::pin!(pending);
    tokio::time::timeout(Duration::from_millis(20), &mut pending)
        .await
        .expect_err("Invocation start is not caller-visible output");
    upstream.release_one();
    upstream.until_sent(2).await;
    tokio::time::timeout(Duration::from_millis(100), &mut pending)
        .await
        .expect_err("Invocation end and next start must not end the wait");
    upstream.release_one();
    upstream.until_sent(3).await;
    let output = tokio::time::timeout(Duration::from_secs(1), pending)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(output.state(), RunState::Running);
    assert!(matches!(output.events(), [emitted, logged]
        if matches!(emitted.kind(), RunEventKind::ProgramEmitted { value } if value.as_json() == &serde_json::json!("ready"))
        && matches!(logged.kind(), RunEventKind::ProgramLogged { message, .. } if message == "ready")));
    assert!(!output.has_more());

    let deadline = immediate(Some(output.next_cursor().clone()), 100)
        .with_wait(Duration::from_millis(100))
        .unwrap();
    let started = tokio::time::Instant::now();
    let expired = fixture
        .runs
        .observe(actor, workspace, accepted.run_id(), &deadline)
        .await
        .unwrap();
    assert!(started.elapsed() >= Duration::from_millis(100));
    assert!(expired.events().is_empty());
    assert_eq!(expired.state(), RunState::Running);

    let initial = RunObservation::initial(Duration::from_secs(2)).unwrap();
    let batching = fixture
        .runs
        .observe(actor, workspace, accepted.run_id(), &initial);
    tokio::pin!(batching);
    tokio::time::timeout(Duration::from_millis(100), &mut batching)
        .await
        .expect_err("authored output alone does not end initial wait");
    upstream.release_one();
    let terminal = tokio::time::timeout(Duration::from_secs(1), batching)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(terminal.state(), RunState::Succeeded);
    assert_eq!(terminal.events(), output.events());
    assert!(
        matches!(terminal.terminal_outcome(), Some(RunTerminalOutcome::Succeeded {
        final_value: FinalValue::Present(value)
    }) if value.as_json() == &serde_json::json!(7))
    );
}

/// An agent that omits the cursor gets the earlier output back, but only later output ends its wait.
#[tokio::test(flavor = "multi_thread")]
async fn cursorless_wait_replays_earlier_output_and_ends_only_on_later_output() {
    let upstream = SequencedUpstream::new(HttpOutcome::new(200, serde_json::json!({})));
    let fixture = TestModules::with_upstream(
        Arc::new(crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default()).unwrap()),
        upstream.clone(),
    )
    .await;
    let operation = fixture
        .user_operation("user:cursorless-wait", "Cursorless waits")
        .await;
    let actor = &operation.principal;
    let workspace = &operation.workspace_id;
    let accepted = fixture.runs.start(actor, workspace, submission(
        "console.log('first'); await coral.providers.weather.getForecast({}); console.log('second'); await coral.providers.weather.getForecast({}); return 7;"
    )).await.unwrap();
    upstream.until_sent(1).await;
    let logged = |update: &crate::RunUpdate| {
        update
            .events()
            .iter()
            .map(|event| match event.kind() {
                RunEventKind::ProgramLogged { message, .. } => message.clone(),
                other => panic!("only Logs were authored: {other:?}"),
            })
            .collect::<Vec<_>>()
    };
    let first = fixture
        .runs
        .observe(
            actor,
            workspace,
            accepted.run_id(),
            &immediate(Some(accepted.next_cursor().clone()), 100)
                .with_wait(Duration::from_secs(2))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(logged(&first), ["first"]);

    let started = tokio::time::Instant::now();
    let replayed = fixture
        .runs
        .observe(
            actor,
            workspace,
            accepted.run_id(),
            &immediate(None, 100)
                .with_wait(Duration::from_millis(100))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(
        started.elapsed() >= Duration::from_millis(100),
        "earlier output does not end the wait"
    );
    assert_eq!(replayed.state(), RunState::Running);
    assert_eq!(replayed.events(), first.events());

    let cursorless = immediate(None, 100)
        .with_wait(Duration::from_secs(2))
        .unwrap();
    let pending = fixture
        .runs
        .observe(actor, workspace, accepted.run_id(), &cursorless);
    tokio::pin!(pending);
    tokio::time::timeout(Duration::from_millis(50), &mut pending)
        .await
        .expect_err("nothing new was authored yet");
    upstream.release_one();
    let later = tokio::time::timeout(Duration::from_secs(1), pending)
        .await
        .expect("later output ends the wait long before its deadline")
        .unwrap();
    assert_eq!(later.state(), RunState::Running);
    assert_eq!(logged(&later), ["first", "second"]);
    upstream.release_one();
}

#[tokio::test(flavor = "multi_thread")]
async fn authored_pages_skip_internal_records_without_losing_order_or_replay() {
    let fixture = TestModules::with_upstream(
        Arc::new(crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default()).unwrap()),
        Arc::new(ScriptedUpstream::json(200, serde_json::json!({}))),
    )
    .await;
    let operation = fixture
        .user_operation("user:lean-pages", "Authored output pages")
        .await;
    let actor = &operation.principal;
    let workspace = &operation.workspace_id;
    let accepted = fixture.runs.start(actor, workspace, submission(
        "for(let i=0;i<3;i++) { await coral.providers.weather.getForecast({}); await coral.emit(i); console.log(i); } return null;"
    )).await.unwrap();
    wait_for_terminal(&fixture.runs, actor, workspace, accepted.run_id()).await;
    let token = accepted.next_cursor().to_string();
    assert!(!token.contains(&accepted.run_id().to_string()));
    let decoded = crate::RunCursor::parse(&token).unwrap();
    assert_eq!(decoded, *accepted.next_cursor());
    for invalid in [
        "",
        &format!("{}.1.0", accepted.run_id()),
        &format!("{token}="),
        &"a".repeat(1000),
    ] {
        assert!(matches!(
            crate::RunCursor::parse(invalid),
            Err(Error::InvalidInput(_))
        ));
    }
    let mut cursor = Some(decoded);
    for (index, sequences) in [[4, 5], [8, 9], [12, 13]].into_iter().enumerate() {
        let observation = immediate(cursor, 2);
        let page = fixture
            .runs
            .observe(actor, workspace, accepted.run_id(), &observation)
            .await
            .unwrap();
        let replay = fixture
            .runs
            .observe(actor, workspace, accepted.run_id(), &observation)
            .await
            .unwrap();
        assert_eq!(page, replay);
        assert_eq!(
            page.events()
                .iter()
                .map(RunEvent::sequence)
                .collect::<Vec<_>>(),
            sequences
        );
        assert!(matches!(page.events(), [emitted, logged]
            if matches!(emitted.kind(), RunEventKind::ProgramEmitted { value } if value.as_json() == &serde_json::json!(index))
            && matches!(logged.kind(), RunEventKind::ProgramLogged { message, .. } if message == &index.to_string())));
        assert_eq!(page.has_more(), index < 2);
        assert_eq!(page.terminal_outcome().is_some(), index == 2);
        cursor = Some(page.next_cursor().clone());
    }
    let finished = fixture
        .runs
        .observe(actor, workspace, accepted.run_id(), &immediate(cursor, 1))
        .await
        .unwrap();
    assert!(finished.events().is_empty());
    assert!(!finished.has_more());
    assert!(
        matches!(finished.terminal_outcome(), Some(RunTerminalOutcome::Succeeded {
        final_value: FinalValue::Present(value)
    }) if value.as_json().is_null())
    );
    assert_eq!(
        retained_events(&fixture.runs, actor, workspace, accepted.run_id())
            .await
            .len(),
        14
    );
}

/// With polling gone, only a change notification ends this wait before its deadline.
#[tokio::test]
async fn waiting_observation_wakes_on_a_committed_change() {
    let held = HeldApproval::new("user:observation").await;
    let pending = held.observe_changes(Duration::from_secs(10));
    tokio::pin!(pending);
    tokio::time::timeout(Duration::from_millis(50), &mut pending)
        .await
        .expect_err("nothing changed yet");
    held.approve().await;
    let update = tokio::time::timeout(Duration::from_secs(2), pending)
        .await
        .expect("the decision wakes the wait long before its deadline")
        .unwrap();
    assert!(update.required_actions().is_empty());
}

/// An action's expiry is a commit of the Run Owner's timer: the wait wakes
/// on it like on any change, and derives nothing from the clock. Domain time
/// never reaches the deadline here; the timer fires on the host clock.
#[tokio::test]
async fn waiting_observation_wakes_when_the_owner_records_an_actions_expiry() {
    // The expiry is a host-clock timer from the request's creation: one
    // second keeps the "still pending" check clear of it on a slow host.
    let held = HeldApproval::expiring("user:observation", Duration::from_secs(1)).await;
    let pending = held.observe_changes(Duration::from_secs(10));
    tokio::pin!(pending);
    tokio::time::timeout(Duration::from_millis(50), &mut pending)
        .await
        .expect_err("the action is still pending");
    let update = tokio::time::timeout(Duration::from_secs(3), pending)
        .await
        .expect("the owner's expiry chunk ends the wait long before its deadline")
        .unwrap();
    assert!(update.required_actions().is_empty());
}

/// Notifications are only hints. Losing them degrades the wait to polling, not an error.
#[tokio::test]
async fn waiting_observation_falls_back_to_polling_during_a_notification_outage() {
    coral_test_support::database_only!(Postgres);
    let held = HeldApproval::new("user:observation").await;
    let pending = held.observe_changes(Duration::from_secs(10));
    tokio::pin!(pending);
    tokio::time::timeout(Duration::from_millis(50), &mut pending)
        .await
        .expect_err("nothing changed yet");
    held.fixture
        .database
        .interrupt_change_notifications()
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_millis(200), &mut pending)
        .await
        .expect_err("an outage alone is not a change");
    held.approve().await;
    let update = tokio::time::timeout(Duration::from_secs(3), pending)
        .await
        .expect("polling observes the decision")
        .expect("an outage does not fail the wait");
    assert!(update.required_actions().is_empty());
}
