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
    // The database answers neither of the next two attempts before they
    // write; the reread finds the head where it was each time, and the
    // chunk is recommitted as built until it lands.
    probe.unanswer_next_attempts(2);

    gates.go.send(()).expect("start the burst");
    wait_until(|| engine.acknowledged.load(Ordering::SeqCst) == 1).await;
    wait_until_async_within(Duration::from_secs(10), async || {
        fixture
            .run_record(&workspace_id, &run_id)
            .await
            .chunk_sequence
            > before.chunk_sequence
    })
    .await;
    assert_eq!(
        probe.commits() - commits_before,
        3,
        "two unanswered attempts, one landed"
    );
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

/// A rejection the database answers with is definite: the chunk is not
/// retried, the owner stops, and the Program's end records Interrupted.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_definite_database_rejection_stops_the_owner_without_a_retry() {
    let (engine, gates) = BurstEngine::new(vec!["\"one\"".to_string()]);
    let probe = CommitProbe::new();
    let fixture = quiet_fixture(engine.clone(), probe.clone()).await;
    let (workspace_id, principal, run_id) = started_run(&fixture, "rejected").await;
    let before = fixture.run_record(&workspace_id, &run_id).await;
    let claim_id = before.execution_claim_id.clone().expect("started Run");
    let owner = fixture
        .runs
        .owners
        .find(&run_id, &claim_id)
        .expect("the owner is live");
    let commits_before = probe.commits();
    // The Run has its one `run.started` Event: the next Event insert is
    // rejected.
    fixture
        .inject_fault("CREATE UNIQUE INDEX reject_run_events ON program_run_events(run_id)")
        .await;

    gates.go.send(()).expect("start the burst");
    wait_until(|| engine.acknowledged.load(Ordering::SeqCst) == 1).await;
    wait_until(|| owner.stop().is_requested()).await;
    assert_eq!(
        probe.commits() - commits_before,
        1,
        "a rejection is not retried"
    );
    assert_eq!(fixture.run_record(&workspace_id, &run_id).await, before);

    fixture.inject_fault("DROP INDEX reject_run_events").await;
    gates.release.send(()).expect("release the Program");
    let terminal = wait_for_terminal(&fixture.runs, &principal, &workspace_id, &run_id).await;
    assert_eq!(terminal.state(), RunState::Interrupted);
    let events = fixture
        .events_after(&workspace_id, &run_id, last_sequence(&before))
        .await;
    assert!(
        matches!(
            events.as_slice(),
            [ended] if matches!(ended.kind(), RunEventKind::Ended { state: RunState::Interrupted, .. })
        ),
        "the rejected Emission is not on the ledger: {events:?}"
    );
}

/// An owner whose settle of the interruption it recorded fails retires and
/// leaves its Run `running` with the termination recorded and its worker
/// claim released: no timer of its own fires after it retired, and nothing
/// renews a lease. Once its lease lapsed, the expired-lease sweep recreates
/// its owner, which ends the Run in one chunk.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_run_whose_owner_retired_on_a_failed_settle_is_ended_by_the_next_sweep() {
    let (engine, gates) = BurstEngine::new(vec!["\"one\"".to_string()]);
    let probe = CommitProbe::new();
    let fixture = quiet_fixture(engine.clone(), probe.clone()).await;
    let (workspace_id, principal, run_id) = started_run(&fixture, "sweep-after-retire").await;
    let before = fixture.run_record(&workspace_id, &run_id).await;
    let claim_id = before.execution_claim_id.clone().expect("started Run");
    let owner = fixture
        .runs
        .owners
        .find(&run_id, &claim_id)
        .expect("the owner is live");
    // Every Event insert after `run.started` is rejected: the Emission's
    // chunk stops the owner, its terminal write records the interruption
    // (an intent, no Event), and the settle's `run.ended` fails.
    fixture
        .inject_fault("CREATE UNIQUE INDEX reject_run_events ON program_run_events(run_id)")
        .await;
    gates.go.send(()).expect("start the burst");
    wait_until(|| engine.acknowledged.load(Ordering::SeqCst) == 1).await;
    wait_until(|| owner.stop().is_requested()).await;
    gates.release.send(()).expect("release the Program");
    wait_until(|| owner.is_retired()).await;
    let left = fixture.run_record(&workspace_id, &run_id).await;
    assert_eq!(
        left.state,
        RunState::Running,
        "the owner left the Run running"
    );
    assert_eq!(
        left.termination.map(|termination| termination.kind),
        Some(crate::lifecycle::RunTerminationKind::Interrupted)
    );
    assert_eq!(left.worker_claim_id, None, "the interruption released it");
    fixture.inject_fault("DROP INDEX reject_run_events").await;

    fixture.clock.next.store(
        before.accepted_at_unix_nanos + 2 * fixture.runs.execution_policy.claim_lease_nanos(),
        Ordering::SeqCst,
    );
    assert_eq!(fixture.runs.sweep_expired_leases().await.unwrap(), 1);
    let terminal = wait_for_terminal(&fixture.runs, &principal, &workspace_id, &run_id).await;
    assert_eq!(terminal.state(), RunState::Interrupted);
    let after = fixture.run_record(&workspace_id, &run_id).await;
    assert_eq!(after.chunk_sequence, left.chunk_sequence + 1, "one chunk");
    wait_until(|| !fixture.runs.has_owner(&run_id)).await;
}

/// The row read before a chunk's write is judged the same way as a reread:
/// a head below the one the owner tracks stops the owner before it writes,
/// with one attempt and nothing on the ledger.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_foreign_head_read_before_the_write_stops_the_owner_without_a_write() {
    let (engine, gates) = BurstEngine::new(vec!["\"one\"".to_string()]);
    let probe = CommitProbe::new();
    let fixture = quiet_fixture(engine.clone(), probe.clone()).await;
    let (workspace_id, principal, run_id) = started_run(&fixture, "foreign-head-read").await;
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
    let commits_before = probe.commits();

    let release_commit = probe.hold_next_commit();
    gates.go.send(()).expect("start the burst");
    wait_until(|| engine.acknowledged.load(Ordering::SeqCst) == 1).await;
    wait_until(|| probe.held()).await;
    // Before the chunk's transaction opens, the head moves below the one it
    // was built on.
    fixture
        .inject_fault(&format!(
            "UPDATE program_run_records SET chunk_sequence = 0 WHERE id = '{run_id}'"
        ))
        .await;
    let tampered = fixture.run_record(&workspace_id, &run_id).await;
    let events = fixture.events_after(&workspace_id, &run_id, 0).await;
    let (work, settled) = probe_work();
    owner.work(work).expect("the owner takes the work");
    release_commit.send(()).expect("release the commit");

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
    assert_eq!(
        probe.commits() - commits_before,
        1,
        "the read refuses the chunk; nothing is retried"
    );
    gates.release.send(()).expect("release the Program");
    wait_until(|| !fixture.runs.has_owner(&run_id)).await;
    assert_eq!(fixture.run_record(&workspace_id, &run_id).await, tampered);
    assert_eq!(
        fixture.events_after(&workspace_id, &run_id, 0).await,
        events
    );
    let _ = principal;
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

/// The one chunk that ends a Run its owner can no longer run records the
/// deadline when it has passed: a takeover (or a self-interrupt) after the
/// deadline ends the Run `timed_out`, as every deadline path does.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_takeover_after_the_run_deadline_records_timed_out() {
    let (engine, gates) = BurstEngine::new(vec!["\"one\"".to_string()]);
    let probe = CommitProbe::new();
    let fixture = quiet_fixture(engine.clone(), probe.clone()).await;
    let (workspace_id, principal, run_id) = started_run(&fixture, "late-panic").await;
    let before = fixture.run_record(&workspace_id, &run_id).await;
    let claim_id = before.execution_claim_id.clone().expect("started Run");
    wait_until(|| engine.started.load(Ordering::SeqCst)).await;

    // The deadline passes before the owner's task ends.
    fixture
        .clock
        .next
        .store(before.deadline_at_unix_nanos + 1, Ordering::SeqCst);
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
    gates.release.send(()).expect("release the Program");

    let after = fixture.run_record(&workspace_id, &run_id).await;
    assert_eq!(after.state, RunState::TimedOut);
    assert!(
        after
            .execution_claim_id
            .as_deref()
            .is_some_and(|claim| claim != claim_id),
        "the supervisor took the claim over: {:?}",
        after.execution_claim_id
    );
    assert_eq!(after.chunk_sequence - before.chunk_sequence, 1, "one chunk");
    let events = fixture
        .events_after(&workspace_id, &run_id, last_sequence(&before))
        .await;
    assert!(
        matches!(
            events.as_slice(),
            [ended] if matches!(ended.kind(), RunEventKind::Ended { state: RunState::TimedOut, .. })
        ),
        "{events:?}"
    );
    let update = fixture
        .runs
        .observe(&principal, &workspace_id, &run_id, &immediate(None, 100))
        .await
        .expect("observe after the timeout");
    assert_eq!(update.state(), RunState::TimedOut);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_owner_task_that_panics_mid_run_has_the_supervisor_record_interrupted_once() {
    let (engine, gates) = BurstEngine::new(vec!["\"one\"".to_string()]);
    let probe = CommitProbe::new();
    let fixture = quiet_fixture(engine.clone(), probe.clone()).await;
    let (workspace_id, principal, run_id) = started_run(&fixture, "panic").await;
    let before = fixture.run_record(&workspace_id, &run_id).await;
    let claim_id = before.execution_claim_id.clone().expect("started Run");

    // Once the Program executes, the owner has consumed its start: the
    // Emission's message is the next one its task dequeues.
    wait_until(|| engine.started.load(Ordering::SeqCst)).await;
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
