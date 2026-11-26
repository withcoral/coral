//! A Run stream's loop under uncertain commits (ADR 0018, slice 3): a
//! commit the database does not answer is resolved by a reread and
//! recommitted as built when it did not land; a definite rejection and a
//! corrupt ledger stop the loop loudly.
use super::*;

use super::run_stream_reconnect::finish_with;
use super::run_streams::{Scripted, emit, ended, finish_terminated, take};
use coral_run_executor::wire::v1::RunEnd as WireEnd;

/// A fixture whose only chunk commits are the ones the test provokes.
async fn quiet_fixture(probe: Arc<CommitProbe>) -> TestModules {
    TestModules::with_execution_adapters(
        Arc::new(ImmediateEngine),
        Arc::new(RejectingUpstream),
        Arc::new(NoopInvocationObserver),
        probe,
    )
    .await
}

/// A Run started on a scripted stream, and its loop.
struct Started {
    workspace_id: coral_workspace::WorkspaceId,
    principal: AuthenticatedPrincipal,
    run_id: crate::RunId,
    stream: Scripted,
    run_loop: crate::run_stream::run_loop::LoopHandle,
}

async fn started_run(fixture: &TestModules, label: &str) -> Started {
    let (workspace_id, principal) = fixture
        .user_scope(&format!("user:{label}"), "Survive the database")
        .await;
    let accepted = fixture
        .runs
        .start(&principal, &workspace_id, submission(label))
        .await
        .expect("accept the Program");
    let run_id = accepted.run_id().clone();
    let claim = take(&fixture.runs).await;
    let mut stream = Scripted::open(&fixture.runs, &run_id.to_string(), &claim);
    stream.start().await;
    let run_loop = fixture
        .runs
        .loops
        .find(&run_id, &claim)
        .expect("the loop is live");
    Started {
        workspace_id,
        principal,
        run_id,
        stream,
        run_loop,
    }
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

/// Ends the Program with a Final Value and waits for the Run's end.
async fn succeed(run: &mut Started, number: u64) {
    run.stream.request(number, finish_with("null"));
    run.stream
        .until(|read| read.contains(&ended(WireEnd::Succeeded)))
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_commit_whose_answer_is_lost_after_it_landed_is_recorded_once_by_the_reread() {
    let probe = CommitProbe::new();
    let fixture = quiet_fixture(probe.clone()).await;
    let mut run = started_run(&fixture, "lost-after").await;
    let (workspace_id, run_id) = (run.workspace_id.clone(), run.run_id.clone());
    let before = fixture.run_record(&workspace_id, &run_id).await;
    let counter_before = fixture.compatibility_counter(&workspace_id, &run_id).await;
    let commits_before = probe.commits();

    let release_answer = probe.lose_next_answer();
    run.stream.request(1, emit(&serde_json::json!("one")));
    wait_until(|| probe.lost()).await;
    // The chunk is on the row; its answer has not reached the loop.
    let landed = fixture.run_record(&workspace_id, &run_id).await;
    assert_eq!(landed.chunk_sequence - before.chunk_sequence, 1);
    assert_eq!(last_sequence(&landed) - last_sequence(&before), 1);
    release_answer.send(()).expect("lose the answer");

    succeed(&mut run, 2).await;
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
    let probe = CommitProbe::new();
    let fixture = quiet_fixture(probe.clone()).await;
    let mut run = started_run(&fixture, "lost-before").await;
    let (workspace_id, run_id) = (run.workspace_id.clone(), run.run_id.clone());
    let before = fixture.run_record(&workspace_id, &run_id).await;
    let commits_before = probe.commits();
    // The database answers neither of the next two attempts before they
    // write; the reread finds the head where it was each time, and the
    // chunk is recommitted as built until it lands.
    probe.unanswer_next_attempts(2);

    run.stream.request(1, emit(&serde_json::json!("one")));
    wait_until_async(async || {
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

    succeed(&mut run, 2).await;
    assert_eq!(
        emitted_values(&fixture.events_after(&workspace_id, &run_id, 0).await),
        vec![serde_json::json!("one")],
        "the recommit landed the Emission once"
    );
}

/// A rejection the database answers with is definite: the chunk is not
/// retried, the loop stops, and the Program's end records Interrupted.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_definite_database_rejection_stops_the_loop_without_a_retry() {
    let probe = CommitProbe::new();
    let fixture = quiet_fixture(probe.clone()).await;
    let mut run = started_run(&fixture, "rejected").await;
    let (workspace_id, run_id) = (run.workspace_id.clone(), run.run_id.clone());
    let before = fixture.run_record(&workspace_id, &run_id).await;
    let commits_before = probe.commits();
    // The Run has its one `run.started` Event: the next Event insert is
    // rejected.
    fixture
        .inject_fault("CREATE UNIQUE INDEX reject_run_events ON program_run_events(run_id)")
        .await;

    run.stream.request(1, emit(&serde_json::json!("one")));
    wait_until(|| run.run_loop.stop().is_requested()).await;
    assert_eq!(
        probe.commits() - commits_before,
        1,
        "a rejection is not retried"
    );
    assert_eq!(fixture.run_record(&workspace_id, &run_id).await, before);

    fixture.inject_fault("DROP INDEX reject_run_events").await;
    finish_terminated(&mut run.stream, 2).await;
    let terminal = wait_for_terminal(&fixture.runs, &run.principal, &workspace_id, &run_id).await;
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

/// How a commit the test holds first sees a head no writer could have
/// left: in the row read before its write, or in the reread after an answer
/// it lost.
#[derive(Debug, Clone, Copy)]
enum ForeignHead {
    ReadBeforeTheWrite,
    Reread,
}

/// A head below the one the loop tracks stops the loop loudly, whether the
/// row read before a chunk's write or the reread after a lost answer shows
/// it: the pending work learns the ledger is corrupt, nothing is retried,
/// and nothing more is written.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_foreign_head_stops_the_loop_loudly_and_changes_nothing() {
    for case in [ForeignHead::ReadBeforeTheWrite, ForeignHead::Reread] {
        let probe = CommitProbe::new();
        let fixture = quiet_fixture(probe.clone()).await;
        let mut run = started_run(&fixture, "foreign-head").await;
        let (workspace_id, run_id) = (run.workspace_id.clone(), run.run_id.clone());
        let commits_before = probe.commits();

        let release = match case {
            ForeignHead::ReadBeforeTheWrite => probe.hold_next_commit(),
            ForeignHead::Reread => probe.lose_next_answer(),
        };
        run.stream.request(1, emit(&serde_json::json!("one")));
        wait_until(|| match case {
            ForeignHead::ReadBeforeTheWrite => probe.held(),
            ForeignHead::Reread => probe.lost(),
        })
        .await;
        // The head moves below the one the chunk was built on: nothing the
        // loop did can explain it.
        fixture
            .inject_fault(&format!(
                "UPDATE program_run_records SET chunk_sequence = 0 WHERE id = '{run_id}'"
            ))
            .await;
        let tampered = fixture.run_record(&workspace_id, &run_id).await;
        let counter = fixture.compatibility_counter(&workspace_id, &run_id).await;
        let events = fixture.events_after(&workspace_id, &run_id, 0).await;
        let (work, settled) = probe_work();
        run.run_loop.work(work).expect("the loop takes the work");
        release.send(()).expect("release the commit");

        wait_until(|| settled.lock().expect("settle lock").is_some()).await;
        assert!(
            matches!(
                settled.lock().expect("settle lock").as_ref(),
                Some(Err(Error::CorruptData(_)))
            ),
            "{case:?}: {:?}",
            settled.lock().expect("settle lock")
        );
        wait_until(|| run.run_loop.stop().is_requested()).await;
        assert_eq!(probe.commits() - commits_before, 1, "{case:?}: no retry");
        finish_terminated(&mut run.stream, 2).await;
        wait_until(|| !fixture.runs.has_loop(&run_id)).await;
        assert_eq!(
            fixture.run_record(&workspace_id, &run_id).await,
            tampered,
            "{case:?}"
        );
        assert_eq!(
            fixture.compatibility_counter(&workspace_id, &run_id).await,
            counter,
            "{case:?}"
        );
        assert_eq!(
            fixture.events_after(&workspace_id, &run_id, 0).await,
            events,
            "{case:?}"
        );
    }
}
