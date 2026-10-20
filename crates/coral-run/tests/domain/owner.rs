//! The Run Owner (ADR 0018): Program requests answer at Applied (at Recorded
//! while the owner is suspect), the Run Ledger lands in chunks, the lease
//! rides every commit, and a claim mismatch stops the owner. The last test
//! drives `apply` without a database.
use super::*;

use crate::owner::{
    CHUNK_WORK_CAP, CommitRejection, Effect, Message, OwnerLimits, OwnerState, Phase, apply,
};
use crate::termination::ShutdownAction;
use crate::{AppliedOutputCap, CHUNK_BYTES_CAP, CHUNK_EVENTS_CAP, CoralFailure};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_burst_of_emits_commits_in_chunks_and_stays_prefix_complete() {
    const EMITS: usize = 200;
    let values = (0..EMITS).map(|index| index.to_string()).collect();
    let (engine, gates) = BurstEngine::new(values);
    let (commit_gate, release_commit) = CommitGate::blocking();
    let fixture = TestModules::with_execution_adapters(
        engine.clone(),
        Arc::new(RejectingUpstream),
        Arc::new(NoopInvocationObserver),
        commit_gate.clone(),
    )
    .await;
    let (workspace_id, principal) = fixture.user_scope("user:burst", "Emit a burst").await;
    let accepted = fixture
        .runs
        .start(&principal, &workspace_id, submission("burst"))
        .await
        .expect("accept the Program");
    let run_id = accepted.run_id().clone();
    wait_until_started(&fixture, &workspace_id, &run_id).await;
    let started = fixture.run_record(&workspace_id, &run_id).await;

    // The first chunk is held at its commit; every emit still answers at
    // Applied, and nothing lands.
    gates.go.send(()).expect("start the burst");
    wait_until(|| engine.acknowledged.load(Ordering::SeqCst) == EMITS).await;
    assert_eq!(engine.rejected.load(Ordering::SeqCst), 0);
    assert!(commit_gate.entered());
    assert_eq!(fixture.run_record(&workspace_id, &run_id).await, started);
    assert!(
        fixture
            .events_after(&workspace_id, &run_id, 1)
            .await
            .is_empty()
    );

    release_commit.send(()).expect("release the commit");
    gates.release.send(()).expect("release the Program");
    let terminal = wait_for_terminal(&fixture.runs, &principal, &workspace_id, &run_id).await;
    assert_eq!(terminal.state(), RunState::Succeeded);
    let after = fixture.run_record(&workspace_id, &run_id).await;
    // The held chunk, the one chunk that queued behind it, and the terminal
    // chunk (an idle tick may add one): far fewer chunks than emits.
    assert!(
        after.chunk_sequence - started.chunk_sequence <= 4,
        "{} chunks for {EMITS} emits",
        after.chunk_sequence - started.chunk_sequence
    );
    let emitted = retained_events(&fixture.runs, &principal, &workspace_id, &run_id)
        .await
        .into_iter()
        .filter_map(|event| match event.kind() {
            RunEventKind::ProgramEmitted { value } => {
                Some((event.sequence(), value.as_json().clone()))
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        emitted,
        (0..EMITS)
            .map(|index| (index as u64 + 2, serde_json::json!(index)))
            .collect::<Vec<_>>()
    );
    assert_eq!(last_sequence(&after), EMITS as u64 + 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn above_the_applied_cap_a_program_request_answers_only_after_its_chunk_commits() {
    // Three Emissions of one chunk each: the third crosses the smallest cap.
    let cap = AppliedOutputCap::new(2 * CHUNK_BYTES_CAP).expect("the smallest cap");
    let value = format!("\"{}\"", "x".repeat(CHUNK_BYTES_CAP - 2));
    let (engine, gates) = BurstEngine::new(vec![value; 3]);
    gates.go.send(()).expect("no gate before the burst");
    let (commit_gate, release_commit) = CommitGate::blocking();
    let fixture = TestModules::with_execution_adapters(
        engine.clone(),
        Arc::new(RejectingUpstream),
        Arc::new(NoopInvocationObserver),
        commit_gate.clone(),
    )
    .await;
    let runs = fixture.runs.clone().with_applied_output_cap(cap);
    let (workspace_id, principal) = fixture.user_scope("user:cap", "Cross the cap").await;
    let accepted = runs
        .start(&principal, &workspace_id, submission("cap"))
        .await
        .expect("accept the Program");
    let run_id = accepted.run_id().clone();

    // The first Emission's chunk is held at its commit. The second answers at
    // Applied, at the cap; the third is above it and waits for its own chunk.
    wait_until(|| engine.acknowledged.load(Ordering::SeqCst) == 2).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        engine.acknowledged.load(Ordering::SeqCst),
        2,
        "the third Emission waits for its chunk while the cap is exceeded"
    );
    assert!(
        fixture
            .events_after(&workspace_id, &run_id, 1)
            .await
            .is_empty(),
        "nothing landed while the first chunk was held"
    );

    release_commit.send(()).expect("release the commit");
    wait_until(|| engine.acknowledged.load(Ordering::SeqCst) == 3).await;
    gates.release.send(()).expect("release the Program");
    let terminal = wait_for_terminal(&runs, &principal, &workspace_id, &run_id).await;
    assert_eq!(terminal.state(), RunState::Succeeded);
    let after = fixture.run_record(&workspace_id, &run_id).await;
    assert_eq!(after.accumulated_output_bytes, 3 * CHUNK_BYTES_CAP);
    assert_eq!(last_sequence(&after), 2 + 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_claim_mismatch_on_commit_stops_the_owner_ends_v8_and_changes_nothing_further() {
    let (engine, gates) = BurstEngine::new(vec!["\"one\"".to_string()]);
    gates.go.send(()).expect("no gate before the Emission");
    let fixture = TestModules::new(engine.clone()).await;
    let (workspace_id, principal) = fixture
        .user_scope("user:claim-mismatch", "Lose the claim")
        .await;
    let accepted = fixture
        .runs
        .start(&principal, &workspace_id, submission("mismatch"))
        .await
        .expect("accept the Program");
    let run_id = accepted.run_id().clone();
    wait_until(|| engine.acknowledged.load(Ordering::SeqCst) == 1).await;
    wait_until_recorded(&fixture, &workspace_id, &run_id, 2).await;
    let claimed = fixture.run_record(&workspace_id, &run_id).await;
    let claim_id = claimed.execution_claim_id.clone().expect("started Run");
    let owner = fixture
        .runs
        .owners
        .find(&run_id, &claim_id)
        .expect("the owner is live");

    // Another holder takes the Run: both claim columns name a new claim id.
    fixture
        .inject_fault(&format!(
            "UPDATE program_run_records SET execution_claim_id = 'taken-over', worker_claim_id = 'taken-over' WHERE id = '{run_id}'"
        ))
        .await;
    let taken = fixture.run_record(&workspace_id, &run_id).await;
    let counter = fixture.compatibility_counter(&workspace_id, &run_id).await;
    let events = fixture.events_after(&workspace_id, &run_id, 0).await;

    // The next chunk commit finds the mismatch: the work waiting on it learns
    // the owner is lost, the Program is terminated, and nothing else is written.
    let (work, settled) = probe_work();
    owner.work(work).expect("the owner takes the work");
    wait_until(|| settled.lock().expect("settle lock").is_some()).await;
    assert!(
        matches!(
            settled.lock().expect("settle lock").as_ref(),
            Some(Err(Error::OwnerLost))
        ),
        "{:?}",
        settled.lock().expect("settle lock")
    );
    // The work settles from the commit task; the owner terminates the Program
    // when the rejection reaches its loop.
    wait_until(|| owner.stop().is_requested()).await;
    gates.release.send(()).expect("release the Program");
    wait_until(|| !fixture.runs.has_owner(&run_id)).await;

    assert_eq!(fixture.run_record(&workspace_id, &run_id).await, taken);
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
async fn the_lease_is_renewed_by_a_chunk_commit_and_by_the_idle_tick() {
    let (engine, gates) = BurstEngine::new(vec!["\"one\"".to_string()]);
    gates.go.send(()).expect("no gate before the Emission");
    let fixture = TestModules::new(engine.clone()).await;
    let (workspace_id, principal) = fixture.user_scope("user:lease", "Renew the lease").await;
    let accepted = fixture
        .runs
        .start(&principal, &workspace_id, submission("lease"))
        .await
        .expect("accept the Program");
    let run_id = accepted.run_id().clone();
    wait_until_recorded(&fixture, &workspace_id, &run_id, 2).await;
    let after_chunk = fixture.run_record(&workspace_id, &run_id).await;
    let lease = fixture.runs.execution_policy.claim_lease_nanos();
    let expires_at = after_chunk
        .worker_claim_expires_at_unix_nanos
        .expect("leased Run");
    // Acceptance leased the Run from its acceptance; the chunk's commit, later
    // on the clock, leased it again from its own time.
    assert!(
        expires_at > after_chunk.accepted_at_unix_nanos + lease,
        "{expires_at} vs {}",
        after_chunk.accepted_at_unix_nanos + lease
    );
    assert_eq!(
        after_chunk.worker_claim_id.as_deref(),
        after_chunk.execution_claim_id.as_deref()
    );

    // Nothing is pending now. Within a lease the idle tick renews again, with
    // one empty chunk and no Event.
    let events = fixture.events_after(&workspace_id, &run_id, 0).await;
    let renewed = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let run = fixture.run_record(&workspace_id, &run_id).await;
            if run.worker_claim_expires_at_unix_nanos > Some(expires_at) {
                return run;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the idle tick renews the lease within a lease");
    assert!(renewed.chunk_sequence > after_chunk.chunk_sequence);
    assert_eq!(
        fixture.events_after(&workspace_id, &run_id, 0).await,
        events
    );
    assert_eq!(
        renewed.accumulated_output_bytes,
        after_chunk.accumulated_output_bytes
    );

    gates.release.send(()).expect("release the Program");
    let terminal = wait_for_terminal(&fixture.runs, &principal, &workspace_id, &run_id).await;
    assert_eq!(terminal.state(), RunState::Succeeded);
}

/// A clock the host cannot read stops the owner like a failed commit; the
/// Program's end that follows still reaches the terminal write, which fails
/// on the same clock, and the owner retires with the Run's end unrecorded.
/// Without that, the end was discarded with the failed clock read and the
/// owner stayed registered forever.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failing_clock_stops_the_owner_and_the_programs_end_still_retires_it() {
    let (engine, gates) = BurstEngine::new(Vec::new());
    gates.go.send(()).expect("no gate before the Program runs");
    let fixture = TestModules::new(engine.clone()).await;
    let (workspace_id, principal) = fixture.user_scope("user:clock", "Lose the clock").await;
    let accepted = fixture
        .runs
        .start(&principal, &workspace_id, submission("clock"))
        .await
        .expect("accept the Program");
    let run_id = accepted.run_id().clone();
    // The start mark is the owner's first chunk and the Program runs after
    // it; the clock fails once the Program is running.
    wait_until_started(&fixture, &workspace_id, &run_id).await;
    let events = fixture.events_after(&workspace_id, &run_id, 0).await;
    fixture.clock.fail();

    gates.release.send(()).expect("release the Program");
    tokio::time::timeout(Duration::from_secs(5), async {
        while fixture.runs.has_owner(&run_id) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the owner retires after the Program's end");
    let abandoned = fixture.run_record(&workspace_id, &run_id).await;
    assert_eq!(abandoned.state, RunState::Running);
    assert!(abandoned.termination.is_none());
    assert_eq!(
        fixture.events_after(&workspace_id, &run_id, 0).await,
        events,
        "no Event is written on a clock the host cannot read"
    );
}

/// A reply the table test can read back.
fn reply() -> (crate::owner::Reply, Answer) {
    tokio::sync::oneshot::channel()
}

fn answered(receiver: &mut Answer) -> Option<Result<(), Error>> {
    receiver.try_recv().ok()
}

fn emit(state: &mut OwnerState, bytes: usize, now: i64) -> (Vec<Effect>, Answer) {
    let (reply, receiver) = reply();
    let value = ProgramValue::from_encoded_json(format!("\"{}\"", "x".repeat(bytes - 2)))
        .expect("a string value");
    (apply(state, Message::Emit { value, reply }, now), receiver)
}

type Answer = tokio::sync::oneshot::Receiver<Result<(), Error>>;

fn result(state: &mut OwnerState, bytes: usize, now: i64) -> (Vec<Effect>, Answer, Settled) {
    let (reply, receiver) = reply();
    let (work, settled) = probe_work();
    (
        apply(state, Message::InvocationResult { bytes, work, reply }, now),
        receiver,
        settled,
    )
}

/// Runs the effects an `apply` returned that a host would answer at once,
/// and returns the rest.
fn deliver(effects: Vec<Effect>) -> Vec<Effect> {
    effects
        .into_iter()
        .filter_map(|effect| match effect {
            Effect::Reply(reply, result) => {
                let _delivered = reply.send(result);
                None
            }
            Effect::Abandon(work, error) => {
                work.settle(Err(error));
                None
            }
            other => Some(other),
        })
        .collect()
}

fn started(limits: OwnerLimits) -> OwnerState {
    let mut state = OwnerState::new(limits, 1);
    assert!(matches!(
        apply(&mut state, Message::Launch, 1).as_slice(),
        [Effect::Start]
    ));
    assert!(matches!(
        apply(
            &mut state,
            Message::Started(Ok(Some(ProgramPackage::scripted("x")))),
            2
        )
        .as_slice(),
        [Effect::StartProgram(_)]
    ));
    assert_eq!(state.phase(), Phase::Running);
    state
}

const LEASE: i64 = 100_000;

const LIMITS: OwnerLimits = OwnerLimits {
    run_output_bytes: 64 << 20,
    applied_cap_bytes: AppliedOutputCap::DEFAULT_BYTES,
    deadline_at_unix_nanos: 1_000_000,
    suspect_after_nanos: LEASE,
    self_interrupt_after_nanos: 3 * LEASE,
};

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one table of message orders, each case a block"
)]
fn apply_decides_bursts_budgets_the_applied_cap_and_a_lost_claim_in_memory() {
    // A burst: every emit answers at Applied; nothing commits before the
    // mailbox drains; the burst lands as chunks of at most the Events cap.
    {
        let mut state = started(LIMITS);
        let mut receivers = Vec::new();
        for index in 0..CHUNK_EVENTS_CAP + 5 {
            let now = 3 + i64::try_from(index).expect("a small index");
            let (effects, receiver) = emit(&mut state, 10, now);
            assert!(
                deliver(effects).is_empty(),
                "an emit commits nothing by itself"
            );
            receivers.push(receiver);
        }
        assert!(
            receivers
                .iter_mut()
                .all(|receiver| matches!(answered(receiver), Some(Ok(()))))
        );
        assert_eq!(state.pending_chunks(), 2);
        assert_eq!(state.output_total(), 10 * (CHUNK_EVENTS_CAP + 5));
        let effects = apply(&mut state, Message::Drained, 2_000);
        let [
            Effect::Commit {
                chunk,
                output_total,
            },
        ] = effects.as_slice()
        else {
            panic!("{effects:?}");
        };
        assert_eq!(chunk.events, CHUNK_EVENTS_CAP);
        assert_eq!(*output_total, 10 * CHUNK_EVENTS_CAP);
        assert!(state.has_commit_in_flight());
        // Nothing else commits while one is in flight; the next drain is a no-op.
        assert!(apply(&mut state, Message::Drained, 2_001).is_empty());
        let effects = apply(&mut state, Message::Committed, 2_002);
        let [
            Effect::Commit {
                chunk,
                output_total,
            },
        ] = effects.as_slice()
        else {
            panic!("{effects:?}");
        };
        assert_eq!(chunk.events, 5);
        assert_eq!(*output_total, 10 * (CHUNK_EVENTS_CAP + 5));
        assert!(apply(&mut state, Message::Committed, 2_003).is_empty());
        assert_eq!(state.unrecorded_bytes(), 0);
    }

    // Row work closes a chunk at its own cap, so one commit never holds the
    // Run row through more than that many held rechecks.
    {
        let mut state = started(LIMITS);
        for _ in 0..=CHUNK_WORK_CAP {
            let (effects, _, _) = result(&mut state, 1, 5);
            assert!(deliver(effects).is_empty());
        }
        assert_eq!(state.pending_chunks(), 2);
        let effects = apply(&mut state, Message::Drained, 6);
        let [Effect::Commit { chunk, .. }] = effects.as_slice() else {
            panic!("{effects:?}");
        };
        assert_eq!(chunk.works, CHUNK_WORK_CAP);
        assert_eq!(chunk.items.len(), CHUNK_WORK_CAP);
    }

    // Two results against the budget: the first fits, the second is refused
    // by the byte, and the total records only what was admitted.
    {
        let mut state = started(OwnerLimits {
            run_output_bytes: 300,
            ..LIMITS
        });
        let (effects, mut first, first_work) = result(&mut state, 150, 10);
        assert!(deliver(effects).is_empty());
        assert!(matches!(answered(&mut first), Some(Ok(()))));
        let (effects, mut second, second_work) = result(&mut state, 151, 11);
        assert!(deliver(effects).is_empty());
        assert!(matches!(
            answered(&mut second),
            Some(Err(Error::InvocationRejected(rejection)))
                if rejection.failure() == CoralFailure::RunOutputTooLarge
        ));
        assert!(matches!(
            second_work.lock().expect("settle").as_ref(),
            Some(Err(Error::InvocationRejected(rejection)))
                if rejection.failure() == CoralFailure::RunOutputTooLarge
        ));
        assert!(
            first_work.lock().expect("settle").is_none(),
            "the admitted work rides the chunk"
        );
        assert_eq!(state.output_total(), 150);
        let (effects, mut third, _) = result(&mut state, 150, 12);
        assert!(deliver(effects).is_empty());
        assert!(
            matches!(answered(&mut third), Some(Ok(()))),
            "exactly the budget fits"
        );
        assert_eq!(state.output_total(), 300);
        let effects = apply(&mut state, Message::Drained, 13);
        let [
            Effect::Commit {
                chunk,
                output_total,
            },
        ] = effects.as_slice()
        else {
            panic!("{effects:?}");
        };
        assert_eq!(chunk.items.len(), 2);
        assert_eq!(*output_total, 300);
    }

    // The Applied cap: once unrecorded output exceeds it, a request answers
    // only when the chunk that carries it is recorded.
    {
        let mut state = started(OwnerLimits {
            applied_cap_bytes: 2 * CHUNK_BYTES_CAP,
            ..LIMITS
        });
        let (effects, mut first) = emit(&mut state, CHUNK_BYTES_CAP, 20);
        assert!(deliver(effects).is_empty());
        assert!(matches!(answered(&mut first), Some(Ok(()))));
        let (effects, mut second) = emit(&mut state, CHUNK_BYTES_CAP, 21);
        assert!(deliver(effects).is_empty());
        assert!(
            matches!(answered(&mut second), Some(Ok(()))),
            "at the cap is not above it"
        );
        let (effects, mut third) = emit(&mut state, CHUNK_BYTES_CAP, 22);
        assert!(deliver(effects).is_empty());
        assert!(
            answered(&mut third).is_none(),
            "above the cap the request waits"
        );
        assert_eq!(
            state.pending_chunks(),
            3,
            "one Event at the bytes cap fills a chunk"
        );
        assert!(matches!(
            apply(&mut state, Message::Drained, 23).as_slice(),
            [Effect::Commit { .. }]
        ));
        for now in [24, 25] {
            let effects = deliver(apply(&mut state, Message::Committed, now));
            assert!(
                matches!(effects.as_slice(), [Effect::Commit { .. }]),
                "{effects:?}"
            );
            assert!(
                answered(&mut third).is_none(),
                "its own chunk has not landed"
            );
        }
        let effects = deliver(apply(&mut state, Message::Committed, 26));
        assert!(effects.is_empty(), "{effects:?}");
        assert!(matches!(answered(&mut third), Some(Ok(()))));
        assert_eq!(state.unrecorded_bytes(), 0);
    }

    // A claim mismatch: the request waiting on the lost chunk learns the owner
    // is lost, pending work is abandoned, the Program is terminated, later
    // requests are refused, and the Program's end writes nothing.
    {
        let mut state = started(OwnerLimits {
            applied_cap_bytes: 2 * CHUNK_BYTES_CAP,
            ..LIMITS
        });
        for _ in 0..3 {
            let (effects, _) = emit(&mut state, CHUNK_BYTES_CAP, 30);
            deliver(effects);
        }
        let (effects, mut waiting) = emit(&mut state, 10, 31);
        assert!(deliver(effects).is_empty());
        assert!(
            answered(&mut waiting).is_none(),
            "above the cap: waits for its chunk"
        );
        // Land the three full chunks; the fourth, carrying the waiting request,
        // is then in flight.
        assert!(matches!(
            apply(&mut state, Message::Drained, 32).as_slice(),
            [Effect::Commit { .. }]
        ));
        for now in [33, 34, 35] {
            // The third full chunk also answers the Emission that crossed the cap.
            let rest = deliver(apply(&mut state, Message::Committed, now));
            assert!(
                matches!(rest.as_slice(), [Effect::Commit { .. }]),
                "{rest:?}"
            );
        }
        assert!(answered(&mut waiting).is_none());
        let (effects, mut applied, pending_work) = result(&mut state, 10, 36);
        assert!(deliver(effects).is_empty());
        assert!(
            matches!(answered(&mut applied), Some(Ok(()))),
            "under the cap again"
        );
        let effects = apply(
            &mut state,
            Message::CommitRejected(CommitRejection::ClaimMismatch),
            37,
        );
        assert!(
            effects
                .iter()
                .any(|effect| matches!(effect, Effect::TerminateProgram))
        );
        assert!(
            !effects
                .iter()
                .any(|effect| matches!(effect, Effect::Commit { .. }))
        );
        let rest = deliver(effects);
        assert!(
            rest.iter()
                .all(|effect| matches!(effect, Effect::TerminateProgram)),
            "{rest:?}"
        );
        assert!(matches!(
            answered(&mut waiting),
            Some(Err(Error::OwnerLost))
        ));
        assert!(matches!(
            pending_work.lock().expect("settle").as_ref(),
            Some(Err(Error::OwnerLost))
        ));
        assert_eq!(
            state.phase(),
            Phase::Stopped(CommitRejection::ClaimMismatch)
        );
        assert_eq!(state.pending_chunks(), 0);
        let (effects, mut refused) = emit(&mut state, 4, 38);
        deliver(effects);
        assert!(matches!(
            answered(&mut refused),
            Some(Err(Error::RunEventPersistence(_)))
        ));
        let effects = apply(
            &mut state,
            Message::ProgramEnded(ExecutionCompletion::ProgramHostTerminated),
            39,
        );
        assert!(
            matches!(effects.as_slice(), [Effect::Retire]),
            "{effects:?}"
        );
        assert_eq!(state.phase(), Phase::Retired);
    }

    // A refused commit also stops the owner, but the Program's end still gets
    // its terminal write: the deadline or a released claim is for it to record.
    {
        let mut state = started(LIMITS);
        let (effects, _) = emit(&mut state, 10, 40);
        deliver(effects);
        apply(&mut state, Message::Drained, 41);
        deliver(apply(
            &mut state,
            Message::CommitRejected(CommitRejection::Refused),
            42,
        ));
        let effects = apply(
            &mut state,
            Message::ProgramEnded(ExecutionCompletion::ProgramHostTerminated),
            43,
        );
        assert!(
            matches!(
                effects.as_slice(),
                [Effect::CompleteExecution(
                    ExecutionCompletion::ProgramHostTerminated
                )]
            ),
            "{effects:?}"
        );
    }

    // The idle tick commits an empty chunk only while nothing else is pending
    // or in flight; it renews the lease and nothing else.
    {
        let mut state = started(LIMITS);
        let effects = apply(&mut state, Message::IdleTick, 50);
        let [
            Effect::Commit {
                chunk,
                output_total,
            },
        ] = effects.as_slice()
        else {
            panic!("{effects:?}");
        };
        assert!(chunk.is_empty());
        assert_eq!(*output_total, 0);
        assert!(apply(&mut state, Message::IdleTick, 51).is_empty());
        apply(&mut state, Message::Committed, 52);
        let (effects, _) = emit(&mut state, 10, 53);
        deliver(effects);
        assert!(apply(&mut state, Message::IdleTick, 54).is_empty());
    }

    // The Program's end drains what is pending before the terminal write.
    {
        let mut state = started(LIMITS);
        let (effects, _) = emit(&mut state, 10, 60);
        deliver(effects);
        let effects = apply(
            &mut state,
            Message::ProgramEnded(ExecutionCompletion::Succeeded(FinalValue::Absent)),
            61,
        );
        assert!(
            matches!(
                effects.as_slice(),
                [Effect::TerminateProgram, Effect::Commit { .. }]
            ),
            "{effects:?}"
        );
        assert_eq!(state.phase(), Phase::Ending);
        let effects = apply(&mut state, Message::Committed, 62);
        assert!(
            matches!(
                effects.as_slice(),
                [Effect::CompleteExecution(ExecutionCompletion::Succeeded(
                    FinalValue::Absent
                ))]
            ),
            "{effects:?}"
        );
        assert_eq!(state.phase(), Phase::Completing);
        assert!(matches!(
            apply(&mut state, Message::Completed(Ok(None)), 63).as_slice(),
            [Effect::Retire]
        ));
    }

    // Row work reserves one Event: the Events cap counts it with the Emissions.
    {
        let mut state = started(LIMITS);
        for index in 0..CHUNK_EVENTS_CAP - 1 {
            let (effects, _) = emit(&mut state, 10, 70 + i64::try_from(index).expect("small"));
            deliver(effects);
        }
        assert!(apply(&mut state, Message::Work(probe_work().0), 2_000).is_empty());
        assert_eq!(state.pending_chunks(), 1, "the work closed the chunk");
        let effects = apply(&mut state, Message::Drained, 2_001);
        let [Effect::Commit { chunk, .. }] = effects.as_slice() else {
            panic!("{effects:?}");
        };
        assert_eq!(chunk.events, CHUNK_EVENTS_CAP);
        assert_eq!(chunk.items.len(), CHUNK_EVENTS_CAP);
    }

    // A refused terminal write (a nonterminal Invocation) with an Invocation
    // end waiting: the end commits first, then the write runs again; refused
    // with nothing pending, the owner retires and leaves the Run to recovery.
    {
        let mut state = started(LIMITS);
        let effects = apply(
            &mut state,
            Message::ProgramEnded(ExecutionCompletion::Succeeded(FinalValue::Absent)),
            80,
        );
        assert!(
            matches!(
                effects.as_slice(),
                [Effect::TerminateProgram, Effect::CompleteExecution(_)]
            ),
            "{effects:?}"
        );
        let (effects, mut late, late_work) = result(&mut state, 10, 81);
        assert!(deliver(effects).is_empty());
        assert!(matches!(answered(&mut late), Some(Ok(()))));
        let refused = || Message::Completed(Err(Error::InvocationPersistence("open".into())));
        let effects = apply(&mut state, refused(), 82);
        assert!(
            matches!(effects.as_slice(), [Effect::Commit { .. }]),
            "{effects:?}"
        );
        assert_eq!(state.phase(), Phase::Ending);
        let effects = apply(&mut state, Message::Committed, 83);
        assert!(
            matches!(
                effects.as_slice(),
                [Effect::CompleteExecution(ExecutionCompletion::Succeeded(
                    FinalValue::Absent
                ))]
            ),
            "{effects:?}"
        );
        assert!(late_work.lock().expect("settle").is_none());
        assert!(matches!(
            apply(&mut state, refused(), 84).as_slice(),
            [Effect::Retire]
        ));
        assert_eq!(state.phase(), Phase::Retired);
    }

    // A settled termination with a chunk in flight: the owner retires only
    // once that chunk has answered, and accepts nothing more meanwhile.
    {
        let mut state = started(LIMITS);
        deliver(apply(
            &mut state,
            Message::ProgramEnded(ExecutionCompletion::ProgramHostTerminated),
            90,
        ));
        let (effects, _, _) = result(&mut state, 10, 91);
        deliver(effects);
        let effects = apply(
            &mut state,
            Message::Completed(Ok(Some(ShutdownAction::for_apply_test()))),
            92,
        );
        assert!(
            matches!(
                effects.as_slice(),
                [Effect::Settle(_), Effect::Commit { .. }]
            ),
            "{effects:?}"
        );
        assert!(apply(&mut state, Message::Settled, 93).is_empty());
        assert_eq!(state.phase(), Phase::Settling);
        let (effects, mut refused, refused_work) = result(&mut state, 10, 94);
        assert!(deliver(effects).is_empty());
        assert!(matches!(
            answered(&mut refused),
            Some(Err(Error::InvocationPersistence(_)))
        ));
        assert!(matches!(
            refused_work.lock().expect("settle").as_ref(),
            Some(Err(Error::ProgramHostTerminated))
        ));
        assert!(matches!(
            apply(&mut state, Message::Committed, 95).as_slice(),
            [Effect::Retire]
        ));
        assert_eq!(state.phase(), Phase::Retired);
    }

    // Suspect: past one lease without a successful commit, a Program request
    // answers only once its chunk is recorded; the next commit clears it.
    {
        let mut state = started(LIMITS);
        let (effects, mut prompt) = emit(&mut state, 10, 100);
        assert!(deliver(effects).is_empty());
        assert!(matches!(answered(&mut prompt), Some(Ok(()))));
        assert!(matches!(
            apply(&mut state, Message::Drained, 101).as_slice(),
            [Effect::Commit { .. }]
        ));
        let (effects, mut late) = emit(&mut state, 10, 2 + LEASE + 1);
        assert!(
            matches!(
                effects.as_slice(),
                [Effect::Suspect {
                    last_commit_age_nanos
                }] if *last_commit_age_nanos == LEASE + 1
            ),
            "{effects:?}"
        );
        assert!(state.is_suspect());
        assert!(
            answered(&mut late).is_none(),
            "suspect: answers at Recorded"
        );
        // The chunk in flight lands: the owner is trusted again, and the
        // late Emission's chunk commits and answers it.
        let effects = deliver(apply(&mut state, Message::Committed, 2 + LEASE + 2));
        assert!(
            matches!(effects.as_slice(), [Effect::Commit { .. }]),
            "{effects:?}"
        );
        assert!(!state.is_suspect());
        assert!(answered(&mut late).is_none());
        assert!(deliver(apply(&mut state, Message::Committed, 2 + LEASE + 3)).is_empty());
        assert!(matches!(answered(&mut late), Some(Ok(()))));
        let (effects, mut prompt) = emit(&mut state, 10, 2 + LEASE + 4);
        assert!(deliver(effects).is_empty());
        assert!(
            matches!(answered(&mut prompt), Some(Ok(()))),
            "Applied again"
        );
    }

    // Self-interrupt: past three leases without a successful commit the
    // owner gives its Run up. The request on the chunk in flight and the
    // pending work learn the host ended, the Program is terminated, the
    // Interrupted chunk is written; the late outcome of the chunk in flight
    // and the Program's end change nothing, and the record retires the owner.
    {
        // Every request rides its chunk, so each one is observable.
        let mut state = started(OwnerLimits {
            applied_cap_bytes: 0,
            ..LIMITS
        });
        let (effects, mut waiting) = emit(&mut state, 10, 200);
        assert!(deliver(effects).is_empty());
        assert!(answered(&mut waiting).is_none());
        assert!(matches!(
            apply(&mut state, Message::Drained, 201).as_slice(),
            [Effect::Commit { .. }]
        ));
        let (effects, mut pending, pending_work) = result(&mut state, 10, 202);
        assert!(deliver(effects).is_empty());
        assert!(answered(&mut pending).is_none());
        let now = 2 + 3 * LEASE + 1;
        let effects = apply(&mut state, Message::IdleTick, now);
        assert!(
            matches!(
                effects.as_slice(),
                [.., Effect::TerminateProgram, Effect::Interrupt {
                    last_commit_age_nanos
                }] if *last_commit_age_nanos == 3 * LEASE + 1
            ),
            "{effects:?}"
        );
        let rest = deliver(effects);
        assert_eq!(rest.len(), 2, "{rest:?}");
        assert!(matches!(
            answered(&mut waiting),
            Some(Err(Error::ProgramHostTerminated))
        ));
        assert!(matches!(
            answered(&mut pending),
            Some(Err(Error::ProgramHostTerminated))
        ));
        assert!(matches!(
            pending_work.lock().expect("settle").as_ref(),
            Some(Err(Error::ProgramHostTerminated))
        ));
        assert_eq!(state.phase(), Phase::Interrupting);
        assert_eq!(state.pending_chunks(), 0);
        assert!(apply(&mut state, Message::Committed, now + 1).is_empty());
        assert!(
            apply(
                &mut state,
                Message::ProgramEnded(ExecutionCompletion::ProgramHostTerminated),
                now + 2,
            )
            .is_empty()
        );
        let (effects, mut refused) = emit(&mut state, 4, now + 3);
        deliver(effects);
        assert!(matches!(
            answered(&mut refused),
            Some(Err(Error::RunEventPersistence(_)))
        ));
        assert!(matches!(
            apply(&mut state, Message::Interrupted(Ok(())), now + 4).as_slice(),
            [Effect::Retire]
        ));
        assert_eq!(state.phase(), Phase::Retired);
    }

    // A commit that succeeded counts before the liveness judgement: its
    // message may reach the owner past the self-interrupt bound.
    {
        let mut state = started(LIMITS);
        let (effects, _) = emit(&mut state, 10, 100);
        deliver(effects);
        apply(&mut state, Message::Drained, 101);
        assert!(state.has_commit_in_flight());
        let effects = apply(&mut state, Message::Committed, 2 + 3 * LEASE + 5);
        assert!(
            effects
                .iter()
                .all(|effect| matches!(effect, Effect::Commit { .. })),
            "{effects:?}"
        );
        assert_eq!(state.phase(), Phase::Running);
        assert!(!state.is_suspect());
    }

    // Past the Run deadline the deadline ends the Run, not the owner's
    // liveness bound.
    {
        let mut state = started(LIMITS);
        let past_deadline = LIMITS.deadline_at_unix_nanos + 3 * LEASE;
        let effects = apply(&mut state, Message::IdleTick, past_deadline);
        assert!(
            effects
                .iter()
                .all(|effect| matches!(effect, Effect::Commit { .. })),
            "{effects:?}"
        );
        assert_eq!(state.phase(), Phase::Running);
    }

    // A corrupt ledger: the owner stops loudly, pending work learns it, and
    // the Program's end writes nothing.
    {
        let mut state = started(LIMITS);
        let (effects, _) = emit(&mut state, 10, 300);
        deliver(effects);
        apply(&mut state, Message::Drained, 301);
        let (work_effects, _, pending_work) = result(&mut state, 10, 302);
        assert!(deliver(work_effects).is_empty());
        let effects = apply(
            &mut state,
            Message::CommitRejected(CommitRejection::Corrupt),
            303,
        );
        let rest = deliver(effects);
        assert!(
            rest.iter()
                .all(|effect| matches!(effect, Effect::TerminateProgram)),
            "{rest:?}"
        );
        assert!(matches!(
            pending_work.lock().expect("settle").as_ref(),
            Some(Err(Error::CorruptData(_)))
        ));
        assert_eq!(state.phase(), Phase::Stopped(CommitRejection::Corrupt));
        let effects = apply(
            &mut state,
            Message::ProgramEnded(ExecutionCompletion::ProgramHostTerminated),
            304,
        );
        assert!(
            matches!(effects.as_slice(), [Effect::Retire]),
            "{effects:?}"
        );
    }
}

/// The fence rows of the `apply` table: fence requests gather in one chunk's
/// batch and commit when free; a stop answers every batch not yet committed
/// at once and nothing else; a request past the deadline or after a stop is
/// answered at once; the terminal write waits for a batch in flight.
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one table of fence message orders, each case a block"
)]
async fn apply_batches_fence_requests_and_a_stop_answers_the_uncommitted_ones_at_once() {
    use crate::invocation::{FenceOutcome, FenceRequest};
    use crate::owner::FENCE_BATCH_CAP;

    type FenceAnswer = tokio::sync::oneshot::Receiver<Result<FenceOutcome, Error>>;

    let (engine, _release) = BlockingEngine::new();
    let fixture = TestModules::new(Arc::new(engine)).await;
    let operation = fixture
        .user_operation("user:apply-fence", "Fence rows")
        .await;
    let run = fixture
        .runs
        .start(
            &operation.principal,
            &operation.workspace_id,
            submission("return 1;"),
        )
        .await
        .unwrap();
    wait_until_started(&fixture, &operation.workspace_id, run.run_id()).await;
    let record = fixture
        .run_record(&operation.workspace_id, run.run_id())
        .await;
    let admitted = admitted_invocation(&fixture, &operation, run.run_id()).await;
    let invoker = test_invoker(&fixture, &operation, &record);
    let fence = |state: &mut OwnerState, now: i64| -> (Vec<Effect>, FenceAnswer) {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let request = FenceRequest::for_apply_test(invoker.clone(), admitted.clone(), sender);
        (
            apply(state, Message::Fence(Box::new(request)), now),
            receiver,
        )
    };
    let answered = |receiver: &mut FenceAnswer| receiver.try_recv().ok();

    // Three requests, one batch, one chunk; nothing commits before the drain.
    {
        let mut state = started(LIMITS);
        let mut answers = Vec::new();
        for now in [3, 4, 5] {
            let (effects, answer) = fence(&mut state, now);
            assert!(
                deliver(effects).is_empty(),
                "a request commits nothing by itself"
            );
            answers.push(answer);
        }
        assert_eq!(state.pending_chunks(), 1);
        assert!(answers.iter_mut().all(|answer| answered(answer).is_none()));
        let effects = apply(&mut state, Message::Drained, 6);
        let [Effect::Commit { chunk, .. }] = effects.as_slice() else {
            panic!("{effects:?}");
        };
        assert!(
            chunk.items.is_empty(),
            "the fence writes no Event and is no item"
        );
        assert_eq!(chunk.works, 1, "one batch counts as one row work");
        assert_eq!(
            chunk
                .fence
                .as_ref()
                .map(|batch| batch.invocation_ids().len()),
            Some(3)
        );
    }

    // A batch closes its chunk at the cap, like the row work cap.
    {
        let mut state = started(LIMITS);
        for now in 0..=FENCE_BATCH_CAP {
            let (effects, _) = fence(&mut state, 10 + i64::try_from(now).unwrap());
            deliver(effects);
        }
        assert_eq!(state.pending_chunks(), 2);
        let effects = apply(&mut state, Message::Drained, 100);
        let [Effect::Commit { chunk, .. }] = effects.as_slice() else {
            panic!("{effects:?}");
        };
        assert_eq!(
            chunk
                .fence
                .as_ref()
                .map(|batch| batch.invocation_ids().len()),
            Some(FENCE_BATCH_CAP)
        );
    }

    // A fence-only chunk is no empty chunk for an Event above the bytes cap:
    // that Event still commits alone, after the batch.
    {
        let mut state = started(LIMITS);
        let (effects, _) = fence(&mut state, 10);
        deliver(effects);
        let (effects, _) = emit(&mut state, CHUNK_BYTES_CAP + 1, 11);
        deliver(effects);
        assert_eq!(state.pending_chunks(), 2);
        let effects = apply(&mut state, Message::Drained, 12);
        let [Effect::Commit { chunk, .. }] = effects.as_slice() else {
            panic!("{effects:?}");
        };
        assert!(
            chunk.fence.is_some() && chunk.items.is_empty(),
            "the batch commits first, without the Event"
        );
    }

    // A stop: the batch not yet committed answers terminated at once, the
    // Program is terminated, and the chunk that held only the batch is gone.
    {
        let mut state = started(LIMITS);
        let (effects, mut first) = fence(&mut state, 20);
        deliver(effects);
        let (effects, mut second) = fence(&mut state, 21);
        deliver(effects);
        let effects = apply(&mut state, Message::Stop, 22);
        assert!(
            effects
                .iter()
                .any(|effect| matches!(effect, Effect::TerminateProgram)),
            "{effects:?}"
        );
        let rest = deliver(effects);
        assert!(
            rest.iter()
                .all(|effect| matches!(effect, Effect::TerminateProgram)),
            "{rest:?}"
        );
        for answer in [&mut first, &mut second] {
            assert!(
                matches!(answered(answer), Some(Err(Error::ProgramHostTerminated))),
                "a fence request not yet committed learns the stop at once"
            );
        }
        assert_eq!(state.pending_chunks(), 0);
        assert!(apply(&mut state, Message::Drained, 23).is_empty());
        // A request after the stop answers at once too.
        let (effects, mut late) = fence(&mut state, 24);
        assert!(deliver(effects).is_empty());
        assert!(matches!(
            answered(&mut late),
            Some(Err(Error::ProgramHostTerminated))
        ));
    }

    // A stop leaves the chunk's other pending work: an Emission beside the
    // abandoned batch still commits, without the batch.
    {
        let mut state = started(LIMITS);
        let (effects, _) = emit(&mut state, 10, 20);
        deliver(effects);
        let (effects, mut pending) = fence(&mut state, 21);
        deliver(effects);
        deliver(apply(&mut state, Message::Stop, 22));
        assert!(matches!(
            answered(&mut pending),
            Some(Err(Error::ProgramHostTerminated))
        ));
        assert_eq!(state.pending_chunks(), 1);
        let effects = apply(&mut state, Message::Drained, 23);
        let [Effect::Commit { chunk, .. }] = effects.as_slice() else {
            panic!("{effects:?}");
        };
        assert!(chunk.fence.is_none(), "the abandoned batch left the chunk");
        assert_eq!(chunk.items.len(), 1, "the Emission commits");
    }

    // Past the deadline in owner memory a request answers at once.
    {
        let mut state = started(LIMITS);
        let (effects, mut late) = fence(&mut state, LIMITS.deadline_at_unix_nanos);
        assert!(deliver(effects).is_empty());
        assert!(matches!(
            answered(&mut late),
            Some(Err(Error::ProgramHostTerminated))
        ));
        assert_eq!(state.pending_chunks(), 0);
    }

    // The terminal chunk waits for a fence in flight: the Program's end
    // writes nothing until the batch's commit has answered.
    {
        let mut state = started(LIMITS);
        let (effects, mut answer) = fence(&mut state, 30);
        deliver(effects);
        assert!(matches!(
            apply(&mut state, Message::Drained, 31).as_slice(),
            [Effect::Commit { .. }]
        ));
        let effects = apply(
            &mut state,
            Message::ProgramEnded(ExecutionCompletion::Succeeded(FinalValue::Absent)),
            32,
        );
        assert!(
            matches!(effects.as_slice(), [Effect::TerminateProgram]),
            "{effects:?}"
        );
        assert!(
            answered(&mut answer).is_none(),
            "the batch is still in flight"
        );
        let effects = apply(&mut state, Message::Committed, 33);
        assert!(
            matches!(effects.as_slice(), [Effect::CompleteExecution(_)]),
            "{effects:?}"
        );
    }
}
