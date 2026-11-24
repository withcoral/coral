//! The Run Ledger head and the chunk-commit predicate (ADR 0018, contract
//! lines 1, 2, 4 and 5), on a Run no worker touches.
use super::*;

use super::run_streams::take;
use crate::run_repository::{Chunk, NewEvent, commit_chunk, find_exact_in_transaction};

/// A staged Run, and the claim of its take when `taken`.
async fn arranged_run(
    fixture: &TestModules,
    label: &str,
    taken: bool,
) -> (coral_workspace::WorkspaceId, crate::RunId, Option<String>) {
    let (workspace_id, principal) = fixture
        .user_scope(&format!("user:ledger-{label}"), "Run Ledger")
        .await;
    let run_id = fixture
        .staged_run(&principal, &workspace_id, "ledger")
        .await;
    let claim = if taken {
        Some(take(&fixture.runs).await)
    } else {
        None
    };
    (workspace_id, run_id, claim)
}

#[tokio::test]
async fn a_chunk_built_on_a_stale_claim_matches_no_row_and_changes_nothing() {
    let fixture = TestModules::new(Arc::new(ImmediateEngine)).await;
    // (whether a take claimed the Run, the claim the chunk was built on)
    let cases: [(&str, bool, Option<&str>); 3] = [
        ("other-claim", true, Some("claim-b")),
        ("claim-lost", true, None),
        ("claim-not-yet", false, Some("claim-a")),
    ];
    for (label, taken, chunk_claim) in cases {
        let (workspace_id, run_id, _claim) = arranged_run(&fixture, label, taken).await;
        let before = fixture.run_record(&workspace_id, &run_id).await;
        let counter_before = fixture.compatibility_counter(&workspace_id, &run_id).await;
        let events_before = fixture.events_after(&workspace_id, &run_id, 0).await;

        let mut transaction = fixture.database.begin().await.expect("begin");
        let run = find_exact_in_transaction(&mut transaction, &workspace_id, &run_id)
            .await
            .expect("read")
            .expect("Run exists");
        let chunk = Chunk::new(&run, chunk_claim, 2_000)
            .event(
                NewEvent::Logged(ProgramLogLevel::Info, "stale", None),
                2_000,
            )
            .output_bytes(5);
        let rejected = commit_chunk(&mut transaction, chunk).await;
        assert!(
            matches!(rejected, Err(Error::ClaimMismatch)),
            "{label}: {rejected:?}"
        );
        transaction
            .commit()
            .await
            .expect("commit the rejected write");

        assert_eq!(
            fixture.run_record(&workspace_id, &run_id).await,
            before,
            "{label}: the Run row is unchanged"
        );
        assert_eq!(
            fixture.events_after(&workspace_id, &run_id, 0).await,
            events_before,
            "{label}: no Event landed"
        );
        assert_eq!(
            fixture.compatibility_counter(&workspace_id, &run_id).await,
            counter_before,
            "{label}: the previous release's counter is unchanged"
        );
    }
}

#[tokio::test]
async fn a_chunk_built_on_a_stale_head_matches_no_row_and_changes_nothing() {
    let fixture = TestModules::new(Arc::new(ImmediateEngine)).await;
    let (workspace_id, run_id, claim) = arranged_run(&fixture, "head", true).await;
    let claim = claim.as_deref();
    // Move the head once, so a chunk can be stale in either direction.
    let mut transaction = fixture.database.begin().await.expect("begin");
    let run = find_exact_in_transaction(&mut transaction, &workspace_id, &run_id)
        .await
        .expect("read")
        .expect("Run exists");
    commit_chunk(
        &mut transaction,
        Chunk::new(&run, claim, 2_000)
            .event(NewEvent::Logged(ProgramLogLevel::Info, "one", None), 2_000),
    )
    .await
    .expect("the first chunk commits");
    transaction.commit().await.expect("commit");
    let before = fixture.run_record(&workspace_id, &run_id).await;
    let counter_before = fixture.compatibility_counter(&workspace_id, &run_id).await;
    let events_before = fixture.events_after(&workspace_id, &run_id, 0).await;
    assert_eq!(before.chunk_sequence, 1);

    for stale_head in [0, 2] {
        let mut transaction = fixture.database.begin().await.expect("begin");
        let run = find_exact_in_transaction(&mut transaction, &workspace_id, &run_id)
            .await
            .expect("read")
            .expect("Run exists");
        let chunk = Chunk::new(&run, claim, 2_000)
            .built_on_head(stale_head)
            .event(
                NewEvent::Logged(ProgramLogLevel::Info, "stale", None),
                3_000,
            )
            .output_bytes(before.accumulated_output_bytes + 5);
        let rejected = commit_chunk(&mut transaction, chunk).await;
        assert!(
            matches!(rejected, Err(Error::HeadMismatch)),
            "head {stale_head}: {rejected:?}"
        );
        transaction
            .commit()
            .await
            .expect("commit the rejected write");

        assert_eq!(
            fixture.run_record(&workspace_id, &run_id).await,
            before,
            "head {stale_head}: the Run row is unchanged"
        );
        assert_eq!(
            fixture.events_after(&workspace_id, &run_id, 0).await,
            events_before,
            "head {stale_head}: no Event landed"
        );
        assert_eq!(
            fixture.compatibility_counter(&workspace_id, &run_id).await,
            counter_before,
            "head {stale_head}: the previous release's counter is unchanged"
        );
    }
}

#[tokio::test]
async fn a_chunk_of_n_events_moves_the_head_once_and_lands_prefix_complete() {
    const EVENTS: usize = 5;
    let events = u64::try_from(EVENTS).expect("a small chunk");
    let fixture = TestModules::new(Arc::new(ImmediateEngine)).await;
    let (workspace_id, run_id, claim) = arranged_run(&fixture, "chunk", true).await;
    let before = fixture.run_record(&workspace_id, &run_id).await;
    let counter_before = fixture.compatibility_counter(&workspace_id, &run_id).await;
    let last_commit_at = async || {
        crate::run_repository::last_commit_at(&fixture.database, &workspace_id, &run_id)
            .await
            .expect("read the last commit time")
    };
    assert_eq!(last_commit_at().await, None, "no chunk committed yet");

    let mut transaction = fixture.database.begin().await.expect("begin");
    let run = find_exact_in_transaction(&mut transaction, &workspace_id, &run_id)
        .await
        .expect("read")
        .expect("Run exists");
    let mut chunk = Chunk::new(&run, claim.as_deref(), 2_000)
        .output_bytes(run.accumulated_output_bytes + 5 * EVENTS);
    for index in 0..events {
        chunk = chunk.event(
            NewEvent::Logged(ProgramLogLevel::Info, "chunk", None),
            2_000 + i64::try_from(index).expect("index"),
        );
    }
    commit_chunk(&mut transaction, chunk)
        .await
        .expect("the chunk commits");

    // A snapshot taken while the chunk's transaction is open sees none of it:
    // the head, the output total and the Events are still the ones read before.
    let mid_commit = fixture.run_record(&workspace_id, &run_id).await;
    assert_eq!(
        mid_commit, before,
        "a snapshot mid-commit sees no part of the chunk"
    );
    assert_eq!(
        fixture
            .events_after(&workspace_id, &run_id, last_sequence(&before))
            .await,
        [] as [RunEvent; 0]
    );
    transaction.commit().await.expect("commit the chunk");

    let after = fixture.run_record(&workspace_id, &run_id).await;
    assert_eq!(after.chunk_sequence - before.chunk_sequence, 1);
    assert_eq!(last_sequence(&after) - last_sequence(&before), events);
    assert_eq!(
        after.accumulated_output_bytes - before.accumulated_output_bytes,
        5 * EVENTS
    );
    assert_eq!(
        last_commit_at().await,
        Some(2_000),
        "the chunk records its commit time"
    );
    // The previous release numbers its Events from this counter during a
    // rolling update, so the chunk moves it by exactly its Event count.
    assert_eq!(
        fixture.compatibility_counter(&workspace_id, &run_id).await - counter_before,
        i64::try_from(EVENTS).expect("a small chunk")
    );
    let landed = fixture
        .events_after(&workspace_id, &run_id, last_sequence(&before))
        .await;
    assert_eq!(
        landed.iter().map(RunEvent::sequence).collect::<Vec<_>>(),
        (last_sequence(&before) + 1..=last_sequence(&before) + events).collect::<Vec<_>>(),
        "the chunk's Events follow the last one without a gap and in order"
    );
    assert!(landed.iter().all(|event| matches!(
        event.kind(),
        RunEventKind::ProgramLogged { message, .. } if message == "chunk"
    )));
}

/// No Run row hold orders writers: the head predicate does. Writers that
/// race with chunks built on the same head land once; every other one
/// matches no row and writes nothing, and the one that landed numbers its
/// Event after the Run's last one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn writers_racing_on_one_head_land_once_and_the_rest_write_nothing() {
    const WRITERS: usize = 4;
    let fixture = Arc::new(TestModules::new(Arc::new(ImmediateEngine)).await);
    let (workspace_id, run_id, claim) = arranged_run(&fixture, "race", true).await;
    let before = Arc::new(fixture.run_record(&workspace_id, &run_id).await);
    let claim = Arc::new(claim);

    let barrier = Arc::new(tokio::sync::Barrier::new(WRITERS));
    let writers = (0..WRITERS).map(|_| {
        let fixture = Arc::clone(&fixture);
        let barrier = barrier.clone();
        let before = Arc::clone(&before);
        let claim = Arc::clone(&claim);
        tokio::spawn(async move {
            barrier.wait().await;
            let mut transaction = fixture.database.begin().await.expect("begin");
            let written = commit_chunk(
                &mut transaction,
                Chunk::new(&before, claim.as_deref(), 2_000)
                    .event(NewEvent::Logged(ProgramLogLevel::Info, "race", None), 2_000),
            )
            .await;
            if written.is_ok() {
                transaction.commit().await.expect("commit");
            }
            written
        })
    });
    // Spawn every writer before awaiting one: they meet at the barrier.
    let mut outcomes = Vec::new();
    for writer in writers.collect::<Vec<_>>() {
        outcomes.push(writer.await.expect("writer task"));
    }

    assert_eq!(outcomes.iter().filter(|outcome| outcome.is_ok()).count(), 1);
    assert!(
        outcomes
            .iter()
            .filter_map(|outcome| outcome.as_ref().err())
            .all(|error| matches!(error, Error::HeadMismatch)),
        "{outcomes:?}"
    );
    let after = fixture.run_record(&workspace_id, &run_id).await;
    assert_eq!(after.chunk_sequence, before.chunk_sequence + 1);
    let landed = fixture
        .events_after(&workspace_id, &run_id, last_sequence(&before))
        .await;
    assert_eq!(
        landed.iter().map(RunEvent::sequence).collect::<Vec<_>>(),
        [last_sequence(&before) + 1]
    );
}

#[tokio::test]
async fn no_chunk_lands_after_run_ended() {
    let fixture = TestModules::new(Arc::new(ImmediateEngine)).await;
    let (workspace_id, run_id, claim) = arranged_run(&fixture, "ended", true).await;
    let claim = claim.expect("the take's claim");
    let mut transaction = fixture.database.begin().await.expect("begin");
    let run = find_exact_in_transaction(&mut transaction, &workspace_id, &run_id)
        .await
        .expect("read")
        .expect("Run exists");
    crate::run_repository::start_taken(
        &mut transaction,
        &run,
        &claim,
        1_500,
        1_000_000,
        &crate::CoralInstanceId::generate(),
    )
    .await
    .expect("the start chunk commits");
    let run = find_exact_in_transaction(&mut transaction, &workspace_id, &run_id)
        .await
        .expect("read")
        .expect("Run exists");
    // A started Run ends under the take's claim and keeps it.
    crate::run_repository::finish_run(
        &mut transaction,
        &run,
        &claim,
        &RunTerminalOutcome::Succeeded {
            final_value: FinalValue::Absent,
        },
        2_000,
    )
    .await
    .expect("the terminal chunk commits");
    transaction.commit().await.expect("commit");
    let ended = fixture.run_record(&workspace_id, &run_id).await;
    assert_eq!(ended.state, RunState::Succeeded);
    let counter_ended = fixture.compatibility_counter(&workspace_id, &run_id).await;
    let events_before = fixture.events_after(&workspace_id, &run_id, 0).await;

    // A chunk built on the ended Run's own head and claim: nothing stale about
    // it, and still no Event may follow `run.ended`.
    let mut transaction = fixture.database.begin().await.expect("begin");
    let run = find_exact_in_transaction(&mut transaction, &workspace_id, &run_id)
        .await
        .expect("read")
        .expect("Run exists");
    let rejected = commit_chunk(
        &mut transaction,
        Chunk::new(&run, run.execution_claim_id.as_deref(), 2_000)
            .event(NewEvent::Logged(ProgramLogLevel::Info, "late", None), 3_000),
    )
    .await;
    assert!(matches!(rejected, Err(Error::RunEnded)), "{rejected:?}");
    transaction
        .commit()
        .await
        .expect("commit the rejected write");

    assert_eq!(fixture.run_record(&workspace_id, &run_id).await, ended);
    assert_eq!(
        fixture.events_after(&workspace_id, &run_id, 0).await,
        events_before
    );
    assert_eq!(
        fixture.compatibility_counter(&workspace_id, &run_id).await,
        counter_ended
    );
}
