//! The Run Owner under uncertain commits, lost leases and task failure (ADR
//! 0018, slice 3): a commit the database does not answer is resolved by a
//! reread and recommitted as built when it did not land; a corrupt ledger
//! stops the owner loudly; past one lease without a commit the owner answers
//! at Recorded; past three it ends the Program and records Interrupted once
//! the database answers; and an owner task that panics has its supervisor
//! record the interruption under a new claim.
use super::*;

use crate::execution::ExecutionPolicy;

/// A fixture whose owner never ticks or trips a liveness bound during the
/// test: the only chunk commits are the ones the test provokes.
async fn quiet_fixture(engine: Arc<BurstEngine>, probe: Arc<CommitProbe>) -> TestModules {
    let mut fixture = TestModules::with_execution_adapters(
        engine,
        Arc::new(RejectingUpstream),
        Arc::new(NoopInvocationObserver),
        probe,
    )
    .await;
    fixture.runs.execution_policy =
        ExecutionPolicy::testing_with_claim_lease(Duration::from_secs(60));
    fixture
}

/// Starts a one-Emission Program, held before its burst, and waits for its
/// start mark.
async fn started_run(
    fixture: &TestModules,
    label: &str,
) -> (
    coral_workspace::WorkspaceId,
    AuthenticatedPrincipal,
    crate::RunId,
) {
    let (workspace_id, principal) = fixture
        .user_scope(&format!("user:{label}"), "Survive the database")
        .await;
    let accepted = fixture
        .runs
        .start(&principal, &workspace_id, submission(label))
        .await
        .expect("accept the Program");
    let run_id = accepted.run_id().clone();
    wait_until_started(fixture, &workspace_id, &run_id).await;
    (workspace_id, principal, run_id)
}

fn emitted_values(events: &[RunEvent]) -> Vec<serde_json::Value> {
    events
        .iter()
        .filter_map(|event| match event.kind() {
            RunEventKind::ProgramEmitted { value } => Some(value.as_json().clone()),
            _ => None,
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_commit_whose_answer_is_lost_after_it_landed_is_recorded_once_by_the_reread() {
    let (engine, gates) = BurstEngine::new(vec!["\"one\"".to_string()]);
    let probe = CommitProbe::new();
    let fixture = quiet_fixture(engine.clone(), probe.clone()).await;
    let (workspace_id, principal, run_id) = started_run(&fixture, "lost-after").await;
    let before = fixture.run_record(&workspace_id, &run_id).await;
    let counter_before = fixture.compatibility_counter(&workspace_id, &run_id).await;
    let commits_before = probe.commits();

    let release_answer = probe.lose_next_answer();
    gates.go.send(()).expect("start the burst");
    wait_until(|| engine.acknowledged.load(Ordering::SeqCst) == 1).await;
    wait_until(|| probe.lost()).await;
    // The chunk is on the row; its answer has not reached the owner.
    let landed = fixture.run_record(&workspace_id, &run_id).await;
    assert_eq!(landed.chunk_sequence - before.chunk_sequence, 1);
    assert_eq!(last_sequence(&landed) - last_sequence(&before), 1);
    release_answer.send(()).expect("lose the answer");

    gates.release.send(()).expect("release the Program");
    let terminal = wait_for_terminal(&fixture.runs, &principal, &workspace_id, &run_id).await;
    assert_eq!(terminal.state(), RunState::Succeeded);
    assert_eq!(
        probe.commits() - commits_before,
        1,
        "the reread resolved the chunk as recorded: no recommit"
    );
    let after = fixture.run_record(&workspace_id, &run_id).await;
    assert_eq!(
        after.chunk_sequence - before.chunk_sequence,
        2,
        "the Emission's chunk and the terminal chunk"
    );
    let events = fixture
        .events_after(&workspace_id, &run_id, last_sequence(&before))
        .await;
    assert_eq!(emitted_values(&events), vec![serde_json::json!("one")]);
    assert_eq!(events.len(), 2, "{events:?}");
    assert_eq!(
        fixture.compatibility_counter(&workspace_id, &run_id).await - counter_before,
        2
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_commit_whose_answer_is_lost_before_it_landed_is_recommitted_as_built_and_lands_once() {
    let (engine, gates) = BurstEngine::new(vec!["\"one\"".to_string()]);
    let probe = CommitProbe::new();
    let fixture = quiet_fixture(engine.clone(), probe.clone()).await;
    let (workspace_id, principal, run_id) = started_run(&fixture, "lost-before").await;
    let before = fixture.run_record(&workspace_id, &run_id).await;
    let commits_before = probe.commits();
    // The Run has its one `run.started` Event: the next Event insert fails.
    fixture
        .inject_fault("CREATE UNIQUE INDEX reject_run_events ON program_run_events(run_id)")
        .await;

    gates.go.send(()).expect("start the burst");
    wait_until(|| engine.acknowledged.load(Ordering::SeqCst) == 1).await;
    // Each attempt fails at the Event insert; the reread finds the head where
    // it was, and the chunk is recommitted as built.
    wait_until(|| probe.commits() >= commits_before + 2).await;
    assert_eq!(
        fixture.run_record(&workspace_id, &run_id).await,
        before,
        "nothing landed while the database refused the chunk"
    );

    fixture.inject_fault("DROP INDEX reject_run_events").await;
    wait_until_async_within(Duration::from_secs(10), async || {
        fixture
            .run_record(&workspace_id, &run_id)
            .await
            .chunk_sequence
            > before.chunk_sequence
    })
    .await;
    let landed = fixture.run_record(&workspace_id, &run_id).await;
    assert_eq!(landed.chunk_sequence - before.chunk_sequence, 1);
    assert_eq!(
        emitted_values(
            &fixture
                .events_after(&workspace_id, &run_id, last_sequence(&before))
                .await
        ),
        vec![serde_json::json!("one")]
    );

    gates.release.send(()).expect("release the Program");
    let terminal = wait_for_terminal(&fixture.runs, &principal, &workspace_id, &run_id).await;
    assert_eq!(terminal.state(), RunState::Succeeded);
    assert_eq!(
        emitted_values(&fixture.events_after(&workspace_id, &run_id, 0).await),
        vec![serde_json::json!("one")],
        "the recommit landed the Emission once"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reread_that_shows_a_foreign_head_fails_loudly_and_changes_nothing() {
    let (engine, gates) = BurstEngine::new(vec!["\"one\"".to_string()]);
    let probe = CommitProbe::new();
    let fixture = quiet_fixture(engine.clone(), probe.clone()).await;
    let (workspace_id, principal, run_id) = started_run(&fixture, "foreign-head").await;
    let claim_id = fixture
        .run_record(&workspace_id, &run_id)
        .await
        .execution_claim_id
        .expect("started Run");
    let owner = fixture
        .runs
        .owners
        .find(&run_id, &claim_id)
        .expect("the owner is live");

    let release_answer = probe.lose_next_answer();
    gates.go.send(()).expect("start the burst");
    wait_until(|| engine.acknowledged.load(Ordering::SeqCst) == 1).await;
    wait_until(|| probe.lost()).await;
    // While the answer is lost, the head moves below the one the chunk was
    // built on: nothing the owner did can explain it.
    fixture
        .inject_fault(&format!(
            "UPDATE program_run_records SET chunk_sequence = 0 WHERE id = '{run_id}'"
        ))
        .await;
    let tampered = fixture.run_record(&workspace_id, &run_id).await;
    let counter = fixture.compatibility_counter(&workspace_id, &run_id).await;
    let events = fixture.events_after(&workspace_id, &run_id, 0).await;
    let (work, settled) = probe_work();
    owner.work(work).expect("the owner takes the work");
    release_answer.send(()).expect("lose the answer");

    // The reread stops the owner: the pending work learns the ledger is
    // corrupt, the Program is terminated, and nothing more is written.
    wait_until(|| settled.lock().expect("settle lock").is_some()).await;
    assert!(
        matches!(
            settled.lock().expect("settle lock").as_ref(),
            Some(Err(Error::CorruptData(_)))
        ),
        "{:?}",
        settled.lock().expect("settle lock")
    );
    wait_until(|| owner.stop().is_requested()).await;
    gates.release.send(()).expect("release the Program");
    wait_until(|| !fixture.runs.has_owner(&run_id)).await;

    assert_eq!(fixture.run_record(&workspace_id, &run_id).await, tampered);
    assert_eq!(
        fixture.compatibility_counter(&workspace_id, &run_id).await,
        counter
    );
    assert_eq!(
        fixture.events_after(&workspace_id, &run_id, 0).await,
        events
    );
    let observed = fixture
        .runs
        .observe(&principal, &workspace_id, &run_id, &immediate(None, 100))
        .await
        .expect("observe the Run");
    assert_eq!(observed.state(), RunState::Running, "no terminal write");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_owner_past_one_lease_without_a_commit_answers_program_requests_at_recorded() {
    let (engine, gates) = BurstEngine::new(vec!["\"one\"".to_string()]);
    let probe = CommitProbe::new();
    let fixture = TestModules::with_execution_adapters(
        engine.clone(),
        Arc::new(RejectingUpstream),
        Arc::new(NoopInvocationObserver),
        probe.clone(),
    )
    .await;
    let (workspace_id, principal, run_id) = started_run(&fixture, "suspect").await;
    // The idle tick's chunk is held: no commit succeeds from here on.
    let release_commit = probe.hold_next_commit();
    wait_until(|| probe.held()).await;
    let accepted_at = fixture
        .run_record(&workspace_id, &run_id)
        .await
        .accepted_at_unix_nanos;
    let lease = fixture.runs.execution_policy.claim_lease_nanos();
    fixture
        .clock
        .next
        .store(accepted_at + 2 * lease, Ordering::SeqCst);

    // Past one lease since the start mark, the Emission is applied but
    // answered only once its chunk is recorded, which waits for the held one.
    gates.go.send(()).expect("start the burst");
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(engine.acknowledged.load(Ordering::SeqCst), 0);
    assert_eq!(engine.rejected.load(Ordering::SeqCst), 0);

    release_commit.send(()).expect("release the held commit");
    wait_until(|| engine.acknowledged.load(Ordering::SeqCst) == 1).await;
    gates.release.send(()).expect("release the Program");
    let terminal = wait_for_terminal(&fixture.runs, &principal, &workspace_id, &run_id).await;
    assert_eq!(terminal.state(), RunState::Succeeded);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_owner_past_three_leases_ends_v8_ends_dispatches_indeterminate_and_records_interrupted()
{
    let upstream = Arc::new(StopAwareUpstream::new());
    let probe = CommitProbe::new();
    let fixture = TestModules::with_execution_adapters(
        Arc::new(
            crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default())
                .expect("V8 engine"),
        ),
        upstream.clone(),
        Arc::new(NoopInvocationObserver),
        probe.clone(),
    )
    .await;
    let operation = fixture
        .user_operation("user:self-interrupt", "Give the Run up")
        .await;
    let accepted = fixture
        .runs
        .start(
            &operation.principal,
            &operation.workspace_id,
            submission("await coral.providers.weather.getForecast();"),
        )
        .await
        .expect("accept the Program");
    let run_id = accepted.run_id().clone();
    wait_until(|| upstream.entered.load(Ordering::SeqCst)).await;
    // The idle tick's chunk is held: no commit succeeds from here on.
    let release_commit = probe.hold_next_commit();
    wait_until(|| probe.held()).await;
    let before = fixture.run_record(&operation.workspace_id, &run_id).await;
    // The Interrupted chunk's two Events share one instant: the database
    // refuses it until the fault goes.
    fixture
        .inject_fault(
            "CREATE UNIQUE INDEX reject_same_instant_events \
             ON program_run_events(run_id, occurred_at_unix_nanos)",
        )
        .await;
    let lease = fixture.runs.execution_policy.claim_lease_nanos();
    fixture
        .clock
        .next
        .store(before.accepted_at_unix_nanos + 4 * lease, Ordering::SeqCst);

    // Past three leases the owner gives up: the dispatch in flight is
    // canceled and V8 ends, while the record waits for the database.
    wait_until(|| upstream.exited.load(Ordering::SeqCst)).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        fixture
            .run_record(&operation.workspace_id, &run_id)
            .await
            .state,
        RunState::Running,
        "the Interrupted record is refused until the database answers"
    );

    fixture
        .inject_fault("DROP INDEX reject_same_instant_events")
        .await;
    wait_until_async_within(Duration::from_secs(10), async || {
        fixture
            .run_record(&operation.workspace_id, &run_id)
            .await
            .state
            .is_terminal()
    })
    .await;
    let after = fixture.run_record(&operation.workspace_id, &run_id).await;
    assert_eq!(after.state, RunState::Interrupted);
    assert_eq!(
        after.chunk_sequence - before.chunk_sequence,
        1,
        "one Interrupted chunk"
    );
    let events = fixture
        .events_after(&operation.workspace_id, &run_id, last_sequence(&before))
        .await;
    assert!(
        matches!(
            events.as_slice(),
            [ended, interrupted]
                if matches!(
                    ended.kind(),
                    RunEventKind::InvocationEnded {
                        result_kind: crate::InvocationResultKind::Indeterminate,
                        ..
                    }
                ) && matches!(
                    interrupted.kind(),
                    RunEventKind::Ended { state: RunState::Interrupted, .. }
                )
        ),
        "{events:?}"
    );

    // The chunk held all along answers late and changes nothing.
    release_commit.send(()).expect("release the held commit");
    wait_until(|| !fixture.runs.has_owner(&run_id)).await;
    assert_eq!(
        fixture.run_record(&operation.workspace_id, &run_id).await,
        after
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_owner_task_that_panics_mid_run_has_the_supervisor_record_interrupted_once() {
    let (engine, gates) = BurstEngine::new(vec!["\"one\"".to_string()]);
    let probe = CommitProbe::new();
    let fixture = quiet_fixture(engine.clone(), probe.clone()).await;
    let (workspace_id, principal, run_id) = started_run(&fixture, "panic").await;
    let before = fixture.run_record(&workspace_id, &run_id).await;
    let claim_id = before.execution_claim_id.clone().expect("started Run");

    // The Emission's message is the next one the owner's task dequeues.
    probe.panic_on_next_message();
    gates.go.send(()).expect("start the burst");
    wait_until_async_within(Duration::from_secs(10), async || {
        fixture
            .run_record(&workspace_id, &run_id)
            .await
            .state
            .is_terminal()
    })
    .await;
    let after = fixture.run_record(&workspace_id, &run_id).await;
    assert_eq!(after.state, RunState::Interrupted);
    assert!(
        after
            .execution_claim_id
            .as_deref()
            .is_some_and(|claim| claim != claim_id),
        "the supervisor took the claim over: {:?}",
        after.execution_claim_id
    );
    assert_eq!(after.worker_claim_id, None);
    assert_eq!(
        after.chunk_sequence - before.chunk_sequence,
        1,
        "head advanced once"
    );
    let events = fixture
        .events_after(&workspace_id, &run_id, last_sequence(&before))
        .await;
    assert!(
        matches!(
            events.as_slice(),
            [ended] if matches!(ended.kind(), RunEventKind::Ended { state: RunState::Interrupted, .. })
        ),
        "{events:?}"
    );
    assert!(!fixture.runs.has_owner(&run_id));
    assert_eq!(
        engine.rejected.load(Ordering::SeqCst),
        1,
        "the Emission's answer was lost with the owner"
    );
    gates.release.send(()).expect("release the Program");

    // A request sent afterwards receives the recorded answer; nothing is
    // written twice.
    let update = fixture
        .runs
        .cancel(&principal, &workspace_id, &run_id, &immediate(None, 100))
        .await
        .expect("cancel after the interruption");
    assert_eq!(update.state(), RunState::Interrupted);
    assert_eq!(fixture.run_record(&workspace_id, &run_id).await, after);
}
