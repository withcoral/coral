//! The Run Owner (ADR 0018): Program requests answer at Applied (at Recorded
//! while the owner is suspect), the Run Ledger lands in chunks, the lease
//! rides every commit, and a claim mismatch stops the owner. The `apply`
//! tables drive the owner's loop without a database.
use super::*;

use crate::owner::{
    CHUNK_WORK_CAP, Check, CommitRejection, Effect, Message, OwnerLimits, OwnerState, ParkedWait,
    Phase, StartedExecution, Timer,
};

/// [`crate::owner::apply`] without the idle tick's re-arming, which every
/// commit returns for the host to run; the cases below read the rest. The
/// cases on the tick itself call the owner's `apply` directly.
fn apply_without_tick(
    state: &mut OwnerState,
    message: Message,
    now_unix_nanos: i64,
) -> Vec<Effect> {
    crate::owner::apply(state, message, now_unix_nanos)
        .into_iter()
        .filter(|effect| {
            !matches!(
                effect,
                Effect::ArmTimer {
                    timer: Timer::IdleTick,
                    ..
                }
            )
        })
        .collect()
}
use crate::owner::RecoveryStep;
use crate::termination::ShutdownAction;
use crate::{AppliedOutputCap, CHUNK_BYTES_CAP, CHUNK_EVENTS_CAP, CoralFailure, FanOutLimit};

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
    let message = Message::Emit {
        value,
        location: None,
        reply,
    };
    (apply_without_tick(state, message, now), receiver)
}

type Answer = tokio::sync::oneshot::Receiver<Result<(), Error>>;

fn result(state: &mut OwnerState, bytes: usize, now: i64) -> (Vec<Effect>, Answer, Settled) {
    let (reply, receiver) = reply();
    let (work, settled) = probe_work();
    (
        apply_without_tick(state, Message::InvocationResult { bytes, work, reply }, now),
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
            Effect::Wake(parked) => {
                parked.wake();
                None
            }
            other => Some(other),
        })
        .collect()
}

/// An owner launched at 1 and started at 2: the launch arms the Run
/// deadline and the idle tick (the owner's liveness is judged from the
/// start mark on), the start mark arms the idle tick again.
fn started(limits: OwnerLimits) -> OwnerState {
    let mut state = OwnerState::new(limits, 1);
    assert!(matches!(
        crate::owner::apply(&mut state, Message::Launch, 1).as_slice(),
        [
            Effect::Start,
            Effect::ArmTimer {
                timer: Timer::RunDeadline,
                after_nanos,
            },
            Effect::ArmTimer {
                timer: Timer::IdleTick,
                after_nanos: tick,
            }
        ] if *after_nanos == limits.deadline_at_unix_nanos - 1 && *tick == limits.idle_tick_nanos
    ));
    assert!(matches!(
        crate::owner::apply(
            &mut state,
            Message::Started(Ok(Some(StartedExecution {
                package: ProgramPackage::scripted("x"),
                head: 1,
            }))),
            2
        )
        .as_slice(),
        [
            Effect::ArmTimer {
                timer: Timer::IdleTick,
                after_nanos,
            },
            Effect::StartProgram(_)
        ] if *after_nanos == limits.idle_tick_nanos
    ));
    assert_eq!(state.phase(), Phase::Running);
    assert_eq!(state.head(), 1);
    state
}

/// The effects of a stop: the shutdown grace armed, then the Program
/// terminated.
fn stops(effects: &[Effect]) -> bool {
    matches!(
        effects,
        [
            Effect::ArmTimer {
                timer: Timer::Grace,
                after_nanos
            },
            Effect::TerminateProgram
        ] if *after_nanos == LIMITS.shutdown_grace_nanos
    )
}

const LEASE: i64 = 100_000;

const LIMITS: OwnerLimits = OwnerLimits {
    run_output_bytes: 64 << 20,
    applied_cap_bytes: AppliedOutputCap::DEFAULT_BYTES,
    fan_out_invocations: FanOutLimit::DEFAULT_INVOCATIONS,
    deadline_at_unix_nanos: 1_000_000,
    shutdown_grace_nanos: 1_000,
    idle_tick_nanos: LEASE / 2,
    suspect_after_nanos: LEASE,
    self_interrupt_after_nanos: 3 * LEASE,
    precheck_interval_nanos: PRECHECK,
    wake_gap_nanos: WAKE_GAP,
};

/// The apply table's periodic precheck interval and wake gap.
const PRECHECK: i64 = 15_000;
const WAKE_GAP: i64 = 1_000;

/// [`LIMITS`] under a Fan-out Limit of two.
const LIMITED: OwnerLimits = OwnerLimits {
    fan_out_invocations: 2,
    ..LIMITS
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
        let effects = apply_without_tick(&mut state, Message::Drained, 2_000);
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
        assert!(apply_without_tick(&mut state, Message::Drained, 2_001).is_empty());
        let effects = apply_without_tick(&mut state, Message::Committed, 2_002);
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
        assert!(apply_without_tick(&mut state, Message::Committed, 2_003).is_empty());
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
        let effects = apply_without_tick(&mut state, Message::Drained, 6);
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
        let effects = apply_without_tick(&mut state, Message::Drained, 13);
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
            apply_without_tick(&mut state, Message::Drained, 23).as_slice(),
            [Effect::Commit { .. }]
        ));
        for now in [24, 25] {
            let effects = deliver(apply_without_tick(&mut state, Message::Committed, now));
            assert!(
                matches!(effects.as_slice(), [Effect::Commit { .. }]),
                "{effects:?}"
            );
            assert!(
                answered(&mut third).is_none(),
                "its own chunk has not landed"
            );
        }
        let effects = deliver(apply_without_tick(&mut state, Message::Committed, 26));
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
            apply_without_tick(&mut state, Message::Drained, 32).as_slice(),
            [Effect::Commit { .. }]
        ));
        for now in [33, 34, 35] {
            // The third full chunk also answers the Emission that crossed the cap.
            let rest = deliver(apply_without_tick(&mut state, Message::Committed, now));
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
        let effects = apply_without_tick(
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
            "a rejected commit terminates the Program; the grace starts with a stop, not here: {rest:?}"
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
        let effects = apply_without_tick(
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
        apply_without_tick(&mut state, Message::Drained, 41);
        deliver(apply_without_tick(
            &mut state,
            Message::CommitRejected(CommitRejection::Refused),
            42,
        ));
        let effects = apply_without_tick(
            &mut state,
            Message::ProgramEnded(ExecutionCompletion::ProgramHostTerminated),
            43,
        );
        assert!(
            matches!(
                effects.as_slice(),
                [Effect::CompleteExecution {
                    completion: ExecutionCompletion::ProgramHostTerminated,
                    ..
                }]
            ),
            "{effects:?}"
        );
    }

    // The idle tick commits an empty chunk only while nothing else is pending
    // or in flight, and arms itself again either way (every commit does, so
    // a busy owner renews its lease by its chunks alone); it renews the
    // lease and nothing else.
    {
        let mut state = started(LIMITS);
        let tick = |state: &mut OwnerState, now: i64| -> Vec<Effect> {
            let mut effects = crate::owner::apply(state, Message::Timer(Timer::IdleTick), now);
            assert!(
                matches!(
                    effects.first(),
                    Some(Effect::ArmTimer {
                        timer: Timer::IdleTick,
                        after_nanos
                    }) if *after_nanos == LIMITS.idle_tick_nanos
                ),
                "{effects:?}"
            );
            effects.remove(0);
            effects
        };
        let effects = tick(&mut state, 50);
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
        assert!(tick(&mut state, 51).is_empty());
        apply_without_tick(&mut state, Message::Committed, 52);
        let (effects, _) = emit(&mut state, 10, 53);
        deliver(effects);
        assert!(tick(&mut state, 54).is_empty());
        let effects = crate::owner::apply(&mut state, Message::Drained, 55);
        assert!(
            matches!(
                effects.as_slice(),
                [
                    Effect::ArmTimer {
                        timer: Timer::IdleTick,
                        ..
                    },
                    Effect::Commit { .. }
                ]
            ),
            "every commit arms the tick again: {effects:?}"
        );
    }

    // The Program's end drains what is pending before the terminal write.
    {
        let mut state = started(LIMITS);
        let (effects, _) = emit(&mut state, 10, 60);
        deliver(effects);
        let effects = apply_without_tick(
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
        let effects = apply_without_tick(&mut state, Message::Committed, 62);
        assert!(
            matches!(
                effects.as_slice(),
                [Effect::CompleteExecution {
                    completion: ExecutionCompletion::Succeeded(FinalValue::Absent),
                    ..
                }]
            ),
            "{effects:?}"
        );
        assert_eq!(state.phase(), Phase::Completing);
        assert!(matches!(
            apply_without_tick(&mut state, Message::Completed(Ok(None)), 63).as_slice(),
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
        assert!(apply_without_tick(&mut state, Message::Work(probe_work().0), 2_000).is_empty());
        assert_eq!(state.pending_chunks(), 1, "the work closed the chunk");
        let effects = apply_without_tick(&mut state, Message::Drained, 2_001);
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
        let effects = apply_without_tick(
            &mut state,
            Message::ProgramEnded(ExecutionCompletion::Succeeded(FinalValue::Absent)),
            80,
        );
        assert!(
            matches!(
                effects.as_slice(),
                [Effect::TerminateProgram, Effect::CompleteExecution { .. }]
            ),
            "{effects:?}"
        );
        let (effects, mut late, late_work) = result(&mut state, 10, 81);
        assert!(deliver(effects).is_empty());
        assert!(matches!(answered(&mut late), Some(Ok(()))));
        let refused = || Message::Completed(Err(Error::InvocationPersistence("open".into())));
        let effects = apply_without_tick(&mut state, refused(), 82);
        assert!(
            matches!(effects.as_slice(), [Effect::Commit { .. }]),
            "{effects:?}"
        );
        assert_eq!(state.phase(), Phase::Ending);
        let effects = apply_without_tick(&mut state, Message::Committed, 83);
        assert!(
            matches!(
                effects.as_slice(),
                [Effect::CompleteExecution {
                    completion: ExecutionCompletion::Succeeded(FinalValue::Absent),
                    ..
                }]
            ),
            "{effects:?}"
        );
        assert!(late_work.lock().expect("settle").is_none());
        assert!(matches!(
            apply_without_tick(&mut state, refused(), 84).as_slice(),
            [Effect::Retire]
        ));
        assert_eq!(state.phase(), Phase::Retired);
    }

    // A recorded termination is settled between the owner's commits, never
    // beside one: a result pending at the terminal write commits first, a
    // settle attempt follows each commit, a result that arrives during an
    // attempt commits before the next attempt, and the attempt that ends
    // the Run retires the owner, which accepts nothing more.
    {
        let mut state = started(LIMITS);
        deliver(apply_without_tick(
            &mut state,
            Message::ProgramEnded(ExecutionCompletion::ProgramHostTerminated),
            90,
        ));
        let (effects, _, _) = result(&mut state, 10, 91);
        deliver(effects);
        let effects = apply_without_tick(
            &mut state,
            Message::Completed(Ok(Some(ShutdownAction::for_apply_test()))),
            92,
        );
        assert!(
            matches!(effects.as_slice(), [Effect::Commit { .. }]),
            "{effects:?}"
        );
        assert_eq!(state.phase(), Phase::Settling);
        assert!(
            matches!(
                apply_without_tick(&mut state, Message::Committed, 93).as_slice(),
                [Effect::Settle { again: false, .. }]
            ),
            "the settle is attempted once the pending result is recorded"
        );
        let (effects, mut late, late_work) = result(&mut state, 10, 94);
        assert!(deliver(effects).is_empty());
        assert!(matches!(answered(&mut late), Some(Ok(()))));
        assert!(
            apply_without_tick(&mut state, Message::Drained, 95).is_empty(),
            "nothing commits during a settle attempt"
        );
        let effects = apply_without_tick(&mut state, Message::SettleAttempted(Ok(false)), 96);
        assert!(
            matches!(effects.as_slice(), [Effect::Commit { .. }]),
            "the late result commits before the next attempt: {effects:?}"
        );
        assert!(matches!(
            apply_without_tick(&mut state, Message::Committed, 97).as_slice(),
            [Effect::Settle { again: false, .. }]
        ));
        assert!(late_work.lock().expect("settle").is_none());
        assert!(
            matches!(
                apply_without_tick(&mut state, Message::SettleAttempted(Ok(false)), 98).as_slice(),
                [Effect::Settle { again: true, .. }]
            ),
            "with nothing to commit the next attempt waits a moment"
        );
        assert!(matches!(
            apply_without_tick(&mut state, Message::SettleAttempted(Ok(true)), 99).as_slice(),
            [Effect::Retire]
        ));
        assert_eq!(state.phase(), Phase::Retired);
        let (effects, mut refused, refused_work) = result(&mut state, 10, 100);
        assert!(deliver(effects).is_empty());
        assert!(matches!(
            answered(&mut refused),
            Some(Err(Error::InvocationPersistence(_)))
        ));
        assert!(matches!(
            refused_work.lock().expect("settle").as_ref(),
            Some(Err(Error::ProgramHostTerminated))
        ));
    }

    // Suspect: past one lease without a successful commit, a Program request
    // answers only once its chunk is recorded; the next commit clears it.
    {
        let mut state = started(LIMITS);
        let (effects, mut prompt) = emit(&mut state, 10, 100);
        assert!(deliver(effects).is_empty());
        assert!(matches!(answered(&mut prompt), Some(Ok(()))));
        assert!(matches!(
            apply_without_tick(&mut state, Message::Drained, 101).as_slice(),
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
        let effects = deliver(apply_without_tick(
            &mut state,
            Message::Committed,
            2 + LEASE + 2,
        ));
        assert!(
            matches!(effects.as_slice(), [Effect::Commit { .. }]),
            "{effects:?}"
        );
        assert!(!state.is_suspect());
        assert!(answered(&mut late).is_none());
        assert!(
            deliver(apply_without_tick(
                &mut state,
                Message::Committed,
                2 + LEASE + 3
            ))
            .is_empty()
        );
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
            apply_without_tick(&mut state, Message::Drained, 201).as_slice(),
            [Effect::Commit { .. }]
        ));
        let (effects, mut pending, pending_work) = result(&mut state, 10, 202);
        assert!(deliver(effects).is_empty());
        assert!(answered(&mut pending).is_none());
        let now = 2 + 3 * LEASE + 1;
        let effects = apply_without_tick(&mut state, Message::Timer(Timer::IdleTick), now);
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
        assert!(apply_without_tick(&mut state, Message::Committed, now + 1).is_empty());
        assert!(
            apply_without_tick(
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
            apply_without_tick(&mut state, Message::Interrupted(Ok(())), now + 4).as_slice(),
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
        apply_without_tick(&mut state, Message::Drained, 101);
        assert!(state.has_commit_in_flight());
        let effects = apply_without_tick(&mut state, Message::Committed, 2 + 3 * LEASE + 5);
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
        let effects =
            apply_without_tick(&mut state, Message::Timer(Timer::IdleTick), past_deadline);
        assert!(
            effects
                .iter()
                .all(|effect| matches!(effect, Effect::Commit { .. } | Effect::ArmTimer { .. })),
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
        apply_without_tick(&mut state, Message::Drained, 301);
        let (work_effects, _, pending_work) = result(&mut state, 10, 302);
        assert!(deliver(work_effects).is_empty());
        let effects = apply_without_tick(
            &mut state,
            Message::CommitRejected(CommitRejection::Corrupt),
            303,
        );
        let rest = deliver(effects);
        assert!(
            rest.iter()
                .all(|effect| matches!(effect, Effect::TerminateProgram)),
            "a rejected commit terminates the Program; the grace starts with a stop, not here: {rest:?}"
        );
        assert!(matches!(
            pending_work.lock().expect("settle").as_ref(),
            Some(Err(Error::CorruptData(_)))
        ));
        assert_eq!(state.phase(), Phase::Stopped(CommitRejection::Corrupt));
        let effects = apply_without_tick(
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
    let tool = fixture.user_tool("user:apply-fence", "Fence rows").await;
    let run = fixture
        .runs
        .start(&tool.principal, &tool.workspace_id, submission("return 1;"))
        .await
        .unwrap();
    wait_until_started(&fixture, &tool.workspace_id, run.run_id()).await;
    let record = fixture.run_record(&tool.workspace_id, run.run_id()).await;
    // Four Invocations: a request names one of them, so a batch says which
    // ones it holds.
    let mut invocations = Vec::new();
    for _ in 0..4 {
        invocations.push(admitted_invocation(&fixture, &tool, run.run_id()).await);
    }
    let ids: [crate::InvocationId; 4] = invocations
        .iter()
        .map(|admitted| admitted.id.clone())
        .collect::<Vec<_>>()
        .try_into()
        .expect("four Invocations");
    let invoker = test_invoker(&fixture, &tool, &record);
    let fence_as = |state: &mut OwnerState, index: usize, now: i64| -> (Vec<Effect>, FenceAnswer) {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let admitted = invocations.get(index).expect("one of four").clone();
        let request = FenceRequest::for_apply_test(invoker.clone(), admitted, sender);
        (
            apply_without_tick(state, Message::Fence(Box::new(request)), now),
            receiver,
        )
    };
    let fence = |state: &mut OwnerState, now: i64| fence_as(state, 0, now);
    // One request per Invocation, in order, at the instants of `nows`;
    // the effects delivered.
    let fence_each = |state: &mut OwnerState, nows: std::ops::Range<i64>| -> Vec<FenceAnswer> {
        nows.enumerate()
            .map(|(index, now)| {
                let (effects, answer) = fence_as(state, index, now);
                deliver(effects);
                answer
            })
            .collect()
    };
    let answered = |receiver: &mut FenceAnswer| receiver.try_recv().ok();
    // The Invocations one commit's batch holds.
    let batched = |effects: &[Effect]| -> Vec<crate::InvocationId> {
        let [Effect::Commit { chunk, .. }] = effects else {
            panic!("{effects:?}");
        };
        chunk
            .fence
            .as_ref()
            .map(|batch| batch.invocation_ids())
            .unwrap_or_default()
    };

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
        let effects = apply_without_tick(&mut state, Message::Drained, 6);
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

    // A batch closes its chunk at the cap, like the row work cap. Under a
    // Fan-out Limit above the cap the request past it opens a second chunk;
    // at the default limit it would wait for a slot instead, since every
    // request in a batch holds one.
    {
        let mut state = started(OwnerLimits {
            fan_out_invocations: FENCE_BATCH_CAP + 1,
            ..LIMITS
        });
        for now in 0..=FENCE_BATCH_CAP {
            let (effects, _) = fence(&mut state, 10 + i64::try_from(now).unwrap());
            deliver(effects);
        }
        assert_eq!(state.pending_chunks(), 2);
        let effects = apply_without_tick(&mut state, Message::Drained, 100);
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
        let effects = apply_without_tick(&mut state, Message::Drained, 12);
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
        let effects = apply_without_tick(&mut state, Message::Stop, 22);
        assert!(
            effects
                .iter()
                .any(|effect| matches!(effect, Effect::TerminateProgram)),
            "{effects:?}"
        );
        let rest = deliver(effects);
        assert!(
            rest.iter().all(|effect| matches!(
                effect,
                Effect::TerminateProgram
                    | Effect::ArmTimer {
                        timer: Timer::Grace,
                        ..
                    }
            )),
            "a stop terminates the Program and starts the shutdown grace: {rest:?}"
        );
        for answer in [&mut first, &mut second] {
            assert!(
                matches!(answered(answer), Some(Err(Error::ProgramHostTerminated))),
                "a fence request not yet committed learns the stop at once"
            );
        }
        assert_eq!(state.pending_chunks(), 0);
        assert!(apply_without_tick(&mut state, Message::Drained, 23).is_empty());
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
        deliver(apply_without_tick(&mut state, Message::Stop, 22));
        assert!(matches!(
            answered(&mut pending),
            Some(Err(Error::ProgramHostTerminated))
        ));
        assert_eq!(state.pending_chunks(), 1);
        let effects = apply_without_tick(&mut state, Message::Drained, 23);
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
            apply_without_tick(&mut state, Message::Drained, 31).as_slice(),
            [Effect::Commit { .. }]
        ));
        let effects = apply_without_tick(
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
        let effects = apply_without_tick(&mut state, Message::Committed, 33);
        assert!(
            matches!(effects.as_slice(), [Effect::CompleteExecution { .. }]),
            "{effects:?}"
        );
    }

    // The Fan-out Limit, at two. Four ready requests: the first two take
    // the slots and batch; the third and fourth wait in memory, in readiness
    // order, with no chunk of their own and no answer. An upstream result
    // frees its slot as it is applied, so the request that has waited
    // longest joins the chunk after the result's row; a dispatch that ends
    // without a result frees its slot through `DispatchEnded`, and a slot
    // freed while nothing waits stays free.
    {
        let mut state = started(LIMITED);
        let mut answers = fence_each(&mut state, 3..7);
        assert_eq!(state.slots_held(), 2);
        assert_eq!(state.waiting_for_a_slot(), 2);
        assert_eq!(state.pending_chunks(), 1);
        let effects = apply_without_tick(&mut state, Message::Drained, 7);
        assert_eq!(batched(&effects), [ids[0].clone(), ids[1].clone()]);
        assert!(deliver(apply_without_tick(&mut state, Message::Committed, 8)).is_empty());
        assert_eq!(state.pending_chunks(), 0, "a slot wait builds no chunk");
        assert!(answers.iter_mut().all(|answer| answered(answer).is_none()));

        let (effects, mut applied, _work) = result(&mut state, 10, 9);
        assert!(deliver(effects).is_empty());
        assert!(
            matches!(applied.try_recv(), Ok(Ok(()))),
            "answered at Applied"
        );
        assert_eq!(state.slots_held(), 2);
        assert_eq!(state.waiting_for_a_slot(), 1);
        let effects = apply_without_tick(&mut state, Message::Drained, 10);
        let [Effect::Commit { chunk, .. }] = effects.as_slice() else {
            panic!("{effects:?}");
        };
        assert_eq!(chunk.items.len(), 1, "the result's row rides the chunk");
        assert_eq!(
            batched(&effects),
            [ids[2].clone()],
            "the third, not the fourth"
        );
        deliver(apply_without_tick(&mut state, Message::Committed, 11));

        deliver(apply_without_tick(&mut state, Message::DispatchEnded, 12));
        deliver(apply_without_tick(&mut state, Message::DispatchEnded, 13));
        assert_eq!(
            state.slots_held(),
            1,
            "the fourth took the first slot freed"
        );
        assert_eq!(state.waiting_for_a_slot(), 0);
        let effects = apply_without_tick(&mut state, Message::Drained, 14);
        assert_eq!(batched(&effects), [ids[3].clone()]);
        deliver(apply_without_tick(&mut state, Message::Committed, 15));
        let (effects, _) = fence(&mut state, 16);
        deliver(effects);
        assert_eq!(state.slots_held(), 2);
        assert_eq!(state.waiting_for_a_slot(), 0);
        assert_eq!(state.pending_chunks(), 1, "the free slot batches at once");
    }

    // An over-budget result frees no slot: the owner applies nothing, the
    // row stays dispatching, and the Invocation task frees the slot once
    // the row's end is recorded (through `DispatchEnded`).
    {
        let mut state = started(OwnerLimits {
            run_output_bytes: 10,
            ..LIMITED
        });
        let mut answers = fence_each(&mut state, 20..23);
        deliver(apply_without_tick(&mut state, Message::Drained, 23));
        deliver(apply_without_tick(&mut state, Message::Committed, 24));
        let (effects, mut refused, refused_work) = result(&mut state, 11, 25);
        assert!(deliver(effects).is_empty());
        assert!(matches!(
            refused.try_recv(),
            Ok(Err(Error::InvocationRejected(rejection)))
                if rejection.failure() == CoralFailure::RunOutputTooLarge
        ));
        assert!(matches!(
            refused_work.lock().expect("settle").as_ref(),
            Some(Err(Error::InvocationRejected(_)))
        ));
        assert_eq!(state.slots_held(), 2, "a refused result frees no slot");
        assert_eq!(state.waiting_for_a_slot(), 1);
        assert_eq!(state.pending_chunks(), 0);
        assert!(answered(answers.last_mut().expect("the waiting request")).is_none());
        deliver(apply_without_tick(&mut state, Message::DispatchEnded, 26));
        assert_eq!(state.slots_held(), 2);
        assert_eq!(state.waiting_for_a_slot(), 0);
        assert_eq!(state.pending_chunks(), 1, "the row's end freed the slot");
    }

    // Once the Run takes no more Program requests, the request waiting for
    // a slot is answered at once with the error the ending makes, while the
    // batch in flight answers on its own: a stop, the Program's end and a
    // lost claim each end a slot wait.
    for (ending, message, lost) in [
        (
            "a stop",
            Message::Stop,
            (|error: &Error| matches!(error, Error::ProgramHostTerminated)) as fn(&Error) -> bool,
        ),
        (
            "the Program's end",
            Message::ProgramEnded(ExecutionCompletion::Succeeded(FinalValue::Absent)),
            |error: &Error| matches!(error, Error::ProgramHostTerminated),
        ),
        (
            "a lost claim",
            Message::CommitRejected(CommitRejection::ClaimMismatch),
            |error: &Error| matches!(error, Error::OwnerLost),
        ),
    ] {
        let mut state = started(LIMITED);
        let mut answers = fence_each(&mut state, 30..33);
        deliver(apply_without_tick(&mut state, Message::Drained, 33));
        assert!(state.has_commit_in_flight(), "{ending}");
        deliver(apply_without_tick(&mut state, message, 34));
        assert!(
            matches!(
                answered(answers.last_mut().expect("the waiting request")),
                Some(Err(error)) if lost(&error)
            ),
            "{ending} answers the waiting request"
        );
        assert_eq!(state.waiting_for_a_slot(), 0, "{ending}");
        assert!(
            answered(answers.first_mut().expect("a batched request")).is_none(),
            "{ending}: the batch in flight answers on its own"
        );
    }

    // The Run deadline covers a slot wait: a slot freed past it answers
    // every waiting request at once, like a request that arrives past it.
    {
        let mut state = started(LIMITED);
        let mut answers = fence_each(&mut state, 40..44);
        deliver(apply_without_tick(&mut state, Message::Drained, 44));
        deliver(apply_without_tick(&mut state, Message::Committed, 45));
        let effects = apply_without_tick(
            &mut state,
            Message::DispatchEnded,
            LIMITS.deadline_at_unix_nanos,
        );
        assert!(deliver(effects).is_empty());
        for answer in answers.iter_mut().skip(2) {
            assert!(matches!(
                answered(answer),
                Some(Err(Error::ProgramHostTerminated))
            ));
        }
        assert_eq!(state.waiting_for_a_slot(), 0);
        assert_eq!(state.pending_chunks(), 0);
    }
}

/// The Fan-out Limit admits at least one Invocation: at zero no dispatch
/// could ever start.
#[test]
fn a_fan_out_limit_below_one_invocation_is_rejected() {
    assert!(matches!(FanOutLimit::new(0), Err(Error::InvalidInput(_))));
    assert_eq!(
        FanOutLimit::new(1).expect("one Invocation").invocations(),
        1
    );
    assert_eq!(
        FanOutLimit::default().invocations(),
        FanOutLimit::DEFAULT_INVOCATIONS
    );
}

/// The deadline of every Approval Request in the apply table.
const APPROVAL_DEADLINE: i64 = 100;

/// A decision request for the apply table, on a fresh Approval Request id.
fn decision(approval: &crate::approval::ApprovalId) -> crate::owner::DecisionRequest {
    crate::owner::DecisionRequest {
        approval: approval.clone(),
        deadline_at_unix_nanos: APPROVAL_DEADLINE,
        decision: crate::approval::ApprovalDecision::Approve,
        actor: requester(),
        comment: None,
        trace: TraceId::parse(&"e".repeat(32)).expect("a trace id"),
    }
}

fn requester() -> coral_workspace::Principal {
    coral_workspace::Principal::from_parts(
        coral_workspace::PrincipalId::parse("user:apply").expect("a principal id"),
        coral_workspace::PrincipalKind::User,
    )
}

fn approval_id() -> crate::approval::ApprovalId {
    crate::approval::ApprovalId::parse(&uuid::Uuid::new_v4().to_string()).expect("an id")
}

/// A decision on `approval` the Run Inbox delivers in `row`.
fn decide(
    state: &mut OwnerState,
    approval: &crate::approval::ApprovalId,
    row: &str,
    now: i64,
) -> Vec<Effect> {
    apply_without_tick(
        state,
        Message::Decide {
            request: decision(approval),
            row: crate::run_inbox::InboxRow(row.to_string()),
        },
        now,
    )
}

/// A cancellation the Run Inbox delivers in `row`.
fn cancel(state: &mut OwnerState, row: &str, now: i64) -> Vec<Effect> {
    apply_without_tick(
        state,
        Message::Cancel {
            requester: requester(),
            row: crate::run_inbox::InboxRow(row.to_string()),
        },
        now,
    )
}

/// What a parked Invocation reads once its owner wakes it (`Ok`) or lets
/// it go (`Err`).
type Woken = tokio::sync::oneshot::Receiver<()>;

/// Parks `admitted` on `wait`, as a landed held recheck reports it.
fn park(
    state: &mut OwnerState,
    invoker: &crate::invocation::ToolInvoker,
    admitted: &Arc<crate::invocation::AdmittedInvocation>,
    wait: ParkedWait,
    now: i64,
) -> (Vec<Effect>, Woken) {
    let (wake, woken) = tokio::sync::oneshot::channel();
    let parked = crate::invocation::ParkedInvocation::for_apply_test(invoker, admitted, wait, wake);
    (
        apply_without_tick(state, Message::Parked(Box::new(parked)), now),
        woken,
    )
}

fn approval_wait(approval: &crate::approval::ApprovalId) -> ParkedWait {
    ParkedWait::Approval {
        approval: approval.clone(),
        deadline_at_unix_nanos: APPROVAL_DEADLINE,
    }
}

fn connection_wait(
    request: coral_provider::account::ConnectionRequestId,
    expires_at_unix_nanos: i64,
) -> ParkedWait {
    ParkedWait::Connection {
        request,
        expires_at_unix_nanos,
    }
}

fn connection_request() -> coral_provider::account::ConnectionRequestId {
    coral_provider::account::ConnectionRequestId::parse(&uuid::Uuid::new_v4().to_string())
        .expect("a request id")
}

/// Whether `effects` are exactly one check of the parks `parks` reads.
fn prechecks(effects: &[Effect], check: Check, parks: &[u64]) -> bool {
    match effects {
        [Effect::Precheck(batch)] => {
            check == Check::Periodic && batch.check() == check && batch.parks() == parks
        }
        [
            Effect::Precheck(batch),
            Effect::ArmTimer {
                timer: Timer::WakeGap,
                after_nanos: WAKE_GAP,
            },
        ] => check == Check::Connections && batch.check() == check && batch.parks() == parks,
        _ => false,
    }
}

/// The one pending chunk a commit effect carries.
fn committed_chunk(effects: Vec<Effect>) -> crate::owner::ChunkBuildView {
    let effects = deliver(effects);
    let [Effect::Commit { chunk, .. }] = effects.as_slice() else {
        panic!("{effects:?}");
    };
    crate::owner::ChunkBuildView::of(chunk)
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one table of outside-request message orders, each case a block"
)]
async fn apply_records_outside_requests_once_in_mailbox_order_and_applies_a_cancel_at_once() {
    use crate::invocation::FenceRequest;
    use crate::lifecycle::RunTerminationKind;

    let (engine, _release) = BlockingEngine::new();
    let fixture = TestModules::new(Arc::new(engine)).await;
    let tool = fixture
        .user_tool("user:apply-requests", "Request rows")
        .await;
    let run = fixture
        .runs
        .start(&tool.principal, &tool.workspace_id, submission("return 1;"))
        .await
        .unwrap();
    wait_until_started(&fixture, &tool.workspace_id, run.run_id()).await;
    let record = fixture.run_record(&tool.workspace_id, run.run_id()).await;
    let admitted = admitted_invocation(&fixture, &tool, run.run_id()).await;
    let invoker = test_invoker(&fixture, &tool, &record);

    // Two decisions on one request: the first rides the next chunk with the
    // deletion of its row; the second, a repeat, has its row deleted by the
    // same chunk, and a third, after the record, by the next one. A row
    // delivered again changes nothing, before its deletion lands and after.
    {
        let mut state = started(LIMITS);
        let approval = approval_id();
        assert!(deliver(decide(&mut state, &approval, "first", 3)).is_empty());
        assert!(deliver(decide(&mut state, &approval, "first", 4)).is_empty());
        assert!(deliver(decide(&mut state, &approval, "second", 4)).is_empty());
        assert_eq!(state.parked_requests(), 0);
        assert_eq!(state.pending_chunks(), 1);
        let chunk = committed_chunk(apply_without_tick(&mut state, Message::Drained, 5));
        assert_eq!((chunk.works, chunk.inbox_rows), (1, 2));
        assert!(chunk.termination.is_none());
        let effects = deliver(apply_without_tick(&mut state, Message::Committed, 6));
        assert!(effects.is_empty(), "{effects:?}");
        assert!(deliver(decide(&mut state, &approval, "second", 7)).is_empty());
        assert_eq!(state.pending_chunks(), 0);
        assert!(deliver(decide(&mut state, &approval, "third", 7)).is_empty());
        let repeat = committed_chunk(apply_without_tick(&mut state, Message::Drained, 8));
        assert_eq!(
            (repeat.works, repeat.inbox_rows),
            (0, 1),
            "a recorded request joins no work"
        );
        assert_eq!(state.head(), 2);
    }

    // A cancel while a fence is in flight: applied at once (the Program is
    // terminated, the batch in flight is left to return on its own), and
    // the intent, `cancellation.requested` and the deletion of its Run Inbox
    // row ride the next chunk. The row delivered again changes nothing,
    // before its deletion lands and after; a later row, a repeat, changes
    // nothing but has a chunk delete it.
    {
        let mut state = started(LIMITS);
        let (sender, _receiver) = tokio::sync::oneshot::channel();
        let request = FenceRequest::for_apply_test(invoker.clone(), admitted.clone(), sender);
        deliver(apply_without_tick(
            &mut state,
            Message::Fence(Box::new(request)),
            3,
        ));
        let batch = committed_chunk(apply_without_tick(&mut state, Message::Drained, 4));
        assert_eq!(batch.fenced, 1);
        let effects = deliver(cancel(&mut state, "first", 5));
        assert!(stops(&effects), "{effects:?}");
        assert_eq!(
            state.termination().map(|intent| intent.termination.kind),
            Some(RunTerminationKind::Canceled)
        );
        assert_eq!(state.pending_chunks(), 1);
        assert!(deliver(cancel(&mut state, "first", 6)).is_empty());
        assert_eq!(state.parked_requests(), 0);
        assert!(
            deliver(apply_without_tick(&mut state, Message::Drained, 7)).is_empty(),
            "the intent waits behind the fence in flight"
        );
        let intent = committed_chunk(apply_without_tick(&mut state, Message::Committed, 8));
        assert_eq!(intent.events, 1, "`cancellation.requested`");
        assert_eq!(intent.inbox_rows, 1);
        assert_eq!(
            intent.termination.map(|intent| intent.termination.kind),
            Some(RunTerminationKind::Canceled)
        );
        let effects = deliver(apply_without_tick(&mut state, Message::Committed, 9));
        assert!(effects.is_empty(), "{effects:?}");
        assert!(deliver(cancel(&mut state, "first", 10)).is_empty());
        assert_eq!(state.pending_chunks(), 0);
        assert!(deliver(cancel(&mut state, "repeat", 11)).is_empty());
        let repeat = committed_chunk(apply_without_tick(&mut state, Message::Drained, 12));
        assert_eq!((repeat.events, repeat.inbox_rows), (0, 1));
        assert!(repeat.termination.is_none());
        assert_eq!(state.head(), 3);
    }

    // The deadline against a decision: a decision applied before the
    // deadline rides a chunk; the deadline terminates the Program and rides
    // the next chunk's head update like a cancellation, with no Event; a
    // decision applied after it closes the request in that same chunk, and
    // the terminal write finds the timeout recorded.
    {
        let mut state = started(LIMITS);
        let approval = approval_id();
        assert!(deliver(decide(&mut state, &approval, "first", 3)).is_empty());
        let effects = deliver(apply_without_tick(
            &mut state,
            Message::Timer(Timer::RunDeadline),
            LIMITS.deadline_at_unix_nanos,
        ));
        assert!(stops(&effects), "{effects:?}");
        assert_eq!(
            state.termination().map(|intent| intent.termination.kind),
            Some(RunTerminationKind::TimedOut)
        );
        assert!(
            deliver(decide(
                &mut state,
                &approval_id(),
                "late",
                LIMITS.deadline_at_unix_nanos + 1,
            ))
            .is_empty()
        );
        let chunk = committed_chunk(apply_without_tick(
            &mut state,
            Message::Drained,
            LIMITS.deadline_at_unix_nanos + 2,
        ));
        assert_eq!((chunk.works, chunk.inbox_rows), (2, 2));
        assert_eq!(chunk.events, 0, "a timeout writes no Event");
        assert_eq!(
            chunk.termination.map(|intent| intent.termination.kind),
            Some(RunTerminationKind::TimedOut),
            "the timeout rides the chunk's head update"
        );
        deliver(apply_without_tick(
            &mut state,
            Message::Committed,
            LIMITS.deadline_at_unix_nanos + 3,
        ));
        let effects = apply_without_tick(
            &mut state,
            Message::ProgramEnded(ExecutionCompletion::ProgramHostTerminated),
            LIMITS.deadline_at_unix_nanos + 4,
        );
        assert!(
            matches!(
                effects.as_slice(),
                [
                    Effect::TerminateProgram,
                    Effect::CompleteExecution {
                        completion: ExecutionCompletion::ProgramHostTerminated,
                        intent: None,
                    }
                ]
            ),
            "the timeout is recorded already: {effects:?}"
        );
    }

    // A cancel past the deadline times the Run out: the Program is
    // terminated, and the timeout rides the next chunk with the deletion of
    // the cancel's row, a repeat of the timeout.
    {
        let mut state = started(LIMITS);
        let effects = cancel(&mut state, "late", LIMITS.deadline_at_unix_nanos);
        assert!(stops(&deliver(effects)));
        assert_eq!(state.pending_chunks(), 1);
        assert_eq!(state.parked_requests(), 0);
        let chunk = committed_chunk(apply_without_tick(
            &mut state,
            Message::Drained,
            LIMITS.deadline_at_unix_nanos + 1,
        ));
        assert_eq!(
            chunk.events, 0,
            "a cancel past the deadline writes no Event"
        );
        assert_eq!(chunk.inbox_rows, 1);
        assert_eq!(
            chunk.termination.map(|intent| intent.termination.kind),
            Some(RunTerminationKind::TimedOut)
        );
        let effects = deliver(apply_without_tick(
            &mut state,
            Message::Committed,
            LIMITS.deadline_at_unix_nanos + 2,
        ));
        assert!(effects.is_empty(), "{effects:?}");
        let effects = apply_without_tick(
            &mut state,
            Message::ProgramEnded(ExecutionCompletion::ProgramHostTerminated),
            LIMITS.deadline_at_unix_nanos + 3,
        );
        assert!(
            matches!(
                effects.as_slice(),
                [
                    Effect::TerminateProgram,
                    Effect::CompleteExecution { intent: None, .. }
                ]
            ),
            "{effects:?}"
        );
    }

    // A Program that never reports its end after the cancel: once the
    // shutdown grace is out the owner ends the Run without it, and a late
    // end changes nothing.
    {
        let mut state = started(LIMITS);
        deliver(cancel(&mut state, "cancel", 3));
        committed_chunk(apply_without_tick(&mut state, Message::Drained, 4));
        deliver(apply_without_tick(&mut state, Message::Committed, 5));
        let effects = apply_without_tick(&mut state, Message::Timer(Timer::Grace), 6);
        assert!(
            matches!(
                effects.as_slice(),
                [
                    Effect::TerminateProgram,
                    Effect::CompleteExecution { intent: None, .. }
                ]
            ),
            "{effects:?}"
        );
        assert_eq!(state.phase(), Phase::Completing);
        assert!(
            apply_without_tick(
                &mut state,
                Message::ProgramEnded(ExecutionCompletion::Succeeded(FinalValue::Absent)),
                7,
            )
            .is_empty()
        );
    }

    // A lost claim commits nothing more: the rows of the decision on the
    // chunk in flight, of its repeat and of a cancellation on a chunk not
    // yet committed stay in the Run Inbox. A row delivered again, or a new
    // one, changes nothing.
    {
        let mut state = started(LIMITS);
        let approval = approval_id();
        assert!(deliver(decide(&mut state, &approval, "first", 3)).is_empty());
        committed_chunk(apply_without_tick(&mut state, Message::Drained, 4));
        assert!(deliver(decide(&mut state, &approval, "repeat", 5)).is_empty());
        deliver(cancel(&mut state, "pending", 6));
        let effects = deliver(apply_without_tick(
            &mut state,
            Message::CommitRejected(CommitRejection::ClaimMismatch),
            7,
        ));
        assert!(
            matches!(effects.as_slice(), [Effect::TerminateProgram]),
            "{effects:?}"
        );
        assert_eq!(
            state.phase(),
            Phase::Stopped(CommitRejection::ClaimMismatch)
        );
        assert!(deliver(cancel(&mut state, "pending", 8)).is_empty());
        assert!(deliver(decide(&mut state, &approval, "first", 8)).is_empty());
        assert!(deliver(decide(&mut state, &approval_id(), "late", 8)).is_empty());
        assert_eq!(state.pending_chunks(), 0);
    }

    // A cancel before the Run started: the Program never starts, the intent
    // commits once the start mark has, and the terminal write ends the Run.
    {
        let mut state = OwnerState::new(LIMITS, 1);
        deliver(apply_without_tick(&mut state, Message::Launch, 1));
        assert!(stops(&deliver(cancel(&mut state, "early", 2))));
        let chunk = committed_chunk(apply_without_tick(
            &mut state,
            Message::Started(Ok(Some(StartedExecution {
                package: ProgramPackage::scripted("x"),
                head: 1,
            }))),
            3,
        ));
        assert_eq!(state.phase(), Phase::Ending);
        assert_eq!((chunk.events, chunk.inbox_rows), (1, 1));
        let effects = apply_without_tick(&mut state, Message::Committed, 4);
        assert!(
            matches!(
                effects.as_slice(),
                [Effect::CompleteExecution {
                    completion: ExecutionCompletion::ProgramHostTerminated,
                    ..
                }]
            ),
            "{effects:?}"
        );
        let effects = apply_without_tick(
            &mut state,
            Message::Completed(Ok(Some(ShutdownAction::for_apply_test()))),
            5,
        );
        assert!(matches!(
            effects.as_slice(),
            [Effect::Settle { again: false, .. }]
        ));
    }

    // During the terminal write a decision joins the next chunk, which
    // closes its request, and a cancel waits for that write. The Run ended:
    // nothing more commits, and both rows stay for the record to answer. A
    // termination to settle: a chunk of the settle closes the request and
    // deletes both rows, the cancel's a repeat.
    {
        let mut state = started(LIMITS);
        deliver(apply_without_tick(
            &mut state,
            Message::ProgramEnded(ExecutionCompletion::Succeeded(FinalValue::Absent)),
            3,
        ));
        assert_eq!(state.phase(), Phase::Completing);
        assert!(deliver(decide(&mut state, &approval_id(), "decision", 4)).is_empty());
        assert!(deliver(cancel(&mut state, "cancel", 4)).is_empty());
        assert!(deliver(cancel(&mut state, "cancel", 4)).is_empty());
        assert_eq!(
            state.parked_requests(),
            1,
            "a row delivered again parks once"
        );
        assert_eq!(state.pending_chunks(), 1);
        let effects = deliver(apply_without_tick(
            &mut state,
            Message::Completed(Ok(None)),
            5,
        ));
        assert!(
            matches!(effects.as_slice(), [Effect::Retire]),
            "{effects:?}"
        );

        let mut state = started(LIMITS);
        deliver(apply_without_tick(
            &mut state,
            Message::ProgramEnded(ExecutionCompletion::ProgramHostTerminated),
            3,
        ));
        assert!(deliver(decide(&mut state, &approval_id(), "decision", 4)).is_empty());
        assert!(deliver(cancel(&mut state, "settled", 4)).is_empty());
        let chunk = committed_chunk(apply_without_tick(
            &mut state,
            Message::Completed(Ok(Some(ShutdownAction::for_apply_test()))),
            5,
        ));
        assert_eq!((chunk.works, chunk.inbox_rows), (1, 2));
        assert!(chunk.termination.is_none());
        assert_eq!(state.phase(), Phase::Settling);
        let effects = apply_without_tick(&mut state, Message::Committed, 6);
        assert!(
            matches!(effects.as_slice(), [Effect::Settle { again: false, .. }]),
            "{effects:?}"
        );
    }

    // A cancel and the Run deadline in one drain: the first applied decides
    // the kind. The cancel first: the intent is the cancellation and rides
    // the next chunk; the deadline's timer changes nothing.
    {
        let mut state = started(LIMITS);
        assert!(stops(&deliver(cancel(&mut state, "first", 3))));
        assert!(
            apply_without_tick(
                &mut state,
                Message::Timer(Timer::RunDeadline),
                LIMITS.deadline_at_unix_nanos
            )
            .is_empty()
        );
        assert_eq!(
            state.termination().map(|intent| intent.termination.kind),
            Some(RunTerminationKind::Canceled)
        );
        let chunk = committed_chunk(apply_without_tick(
            &mut state,
            Message::Drained,
            LIMITS.deadline_at_unix_nanos + 1,
        ));
        assert_eq!(
            (chunk.events, chunk.inbox_rows),
            (1, 1),
            "`cancellation.requested`"
        );
        assert_eq!(
            chunk.termination.map(|intent| intent.termination.kind),
            Some(RunTerminationKind::Canceled)
        );
    }

    // The deadline's timer first, both before the deadline on the owner's
    // clock (the timer fires on the host's): the timeout is the intent, and
    // the cancel, a repeat, has its row deleted by the chunk that carries
    // the timeout.
    {
        let mut state = started(LIMITS);
        let effects = deliver(apply_without_tick(
            &mut state,
            Message::Timer(Timer::RunDeadline),
            10,
        ));
        assert!(stops(&effects), "{effects:?}");
        assert!(deliver(cancel(&mut state, "after", 11)).is_empty());
        assert_eq!(
            state.termination().map(|intent| intent.termination.kind),
            Some(RunTerminationKind::TimedOut)
        );
        assert_eq!(state.parked_requests(), 0);
        assert_eq!(
            state.pending_chunks(),
            1,
            "the timeout and the cancel's row ride one chunk"
        );
        let chunk = committed_chunk(apply_without_tick(&mut state, Message::Drained, 12));
        assert_eq!((chunk.events, chunk.inbox_rows), (0, 1));
        assert_eq!(
            chunk.termination.map(|intent| intent.termination.kind),
            Some(RunTerminationKind::TimedOut)
        );
        deliver(apply_without_tick(&mut state, Message::Committed, 13));
        let effects = apply_without_tick(
            &mut state,
            Message::ProgramEnded(ExecutionCompletion::ProgramHostTerminated),
            14,
        );
        assert!(
            matches!(
                effects.as_slice(),
                [
                    Effect::TerminateProgram,
                    Effect::CompleteExecution { intent: None, .. }
                ]
            ),
            "{effects:?}"
        );
    }

    // An Approval Request's deadline: the chunk whose gate parked the
    // Invocation on the request reports the park, and the owner arms the
    // request's timer and the periodic precheck. The timer first: the expiry
    // rides the next chunk with no requester, and once it is recorded the
    // parked Invocation goes back to its held recheck with no read; a
    // decision applied after the timer is a repeat, whose row rides the
    // expiry's chunk. A park on a request already decided goes back at once
    // and arms nothing.
    {
        let mut state = started(LIMITS);
        let approval = approval_id();
        let timer = Timer::Approval(approval.clone());
        let (effects, mut woken) =
            park(&mut state, &invoker, &admitted, approval_wait(&approval), 3);
        assert!(
            matches!(
                effects.as_slice(),
                [
                    Effect::ArmTimer { timer: Timer::Approval(id), after_nanos: 97 },
                    Effect::ArmTimer { timer: Timer::Precheck, after_nanos: PRECHECK },
                ] if *id == approval
            ),
            "an approval wait subscribes to nothing: {effects:?}"
        );
        assert!(
            apply_without_tick(&mut state, Message::Timer(timer.clone()), APPROVAL_DEADLINE)
                .is_empty()
        );
        assert_eq!(state.pending_chunks(), 1);
        assert!(deliver(decide(&mut state, &approval, "late", APPROVAL_DEADLINE + 1)).is_empty());
        assert_eq!(state.parked_requests(), 0);
        let chunk = committed_chunk(apply_without_tick(&mut state, Message::Drained, 102));
        assert_eq!(
            (chunk.works, chunk.inbox_rows),
            (1, 1),
            "the expiry, and the decision's row"
        );
        woken.try_recv().expect_err("parked until the expiry lands");
        let effects = deliver(apply_without_tick(&mut state, Message::Committed, 104));
        assert!(effects.is_empty(), "{effects:?}");
        woken.try_recv().expect("the recorded expiry wakes it");
        assert_eq!(state.parked_invocations(), 0);
        assert_eq!(state.pending_chunks(), 0);
        let (effects, mut again) = park(
            &mut state,
            &invoker,
            &admitted,
            approval_wait(&approval),
            105,
        );
        assert!(
            deliver(effects).is_empty(),
            "a decided request arms no timer"
        );
        again.try_recv().expect("and goes back at once");
        assert_eq!(state.parked_invocations(), 0);
    }

    // The decision first: it rides the chunk and wakes the parked
    // Invocation once recorded, and the timer that fires after it builds no
    // expiry.
    {
        let mut state = started(LIMITS);
        let approval = approval_id();
        let timer = Timer::Approval(approval.clone());
        let (effects, mut woken) =
            park(&mut state, &invoker, &admitted, approval_wait(&approval), 3);
        assert_eq!(effects.len(), 2, "{effects:?}");
        assert!(deliver(decide(&mut state, &approval, "first", 4)).is_empty());
        assert!(apply_without_tick(&mut state, Message::Timer(timer.clone()), 100).is_empty());
        let chunk = committed_chunk(apply_without_tick(&mut state, Message::Drained, 101));
        assert_eq!(
            (chunk.works, chunk.inbox_rows),
            (1, 1),
            "the decision alone"
        );
        deliver(apply_without_tick(&mut state, Message::Committed, 103));
        woken.try_recv().expect("woken");
        assert!(
            apply_without_tick(&mut state, Message::Timer(timer), 104).is_empty(),
            "a late timer on a decided request"
        );
        assert_eq!(state.pending_chunks(), 0);
    }

    // A decision applied past the request's deadline on the owner's clock,
    // before its timer's message arrived: the expiry is applied first, as
    // the timer would have been, and the decision is a repeat whose row
    // rides the expiry's chunk.
    {
        let mut state = started(LIMITS);
        let approval = approval_id();
        let (effects, mut woken) =
            park(&mut state, &invoker, &admitted, approval_wait(&approval), 3);
        assert_eq!(effects.len(), 2, "{effects:?}");
        assert!(deliver(decide(&mut state, &approval, "late", APPROVAL_DEADLINE)).is_empty());
        let chunk = committed_chunk(apply_without_tick(&mut state, Message::Drained, 101));
        assert_eq!(
            (chunk.works, chunk.inbox_rows),
            (1, 1),
            "the expiry, and the decision's row"
        );
        deliver(apply_without_tick(&mut state, Message::Committed, 103));
        woken.try_recv().expect("woken");
        assert_eq!(state.pending_chunks(), 0);
    }

    // A connection request's deadline wakes the Invocations parked on that
    // request alone while the Run takes Program requests, and nothing once
    // it stops; the stop lets every parked Invocation go.
    {
        let mut state = started(LIMITS);
        let request = connection_request();
        let other = connection_request();
        let timer = Timer::Connection(request);
        let (effects, mut woken) = park(
            &mut state,
            &invoker,
            &admitted,
            connection_wait(request, 50),
            3,
        );
        assert!(
            matches!(
                effects.as_slice(),
                [
                    Effect::ArmTimer {
                        timer: Timer::Connection(_),
                        after_nanos: 47
                    },
                    Effect::Subscribe,
                    Effect::ArmTimer {
                        timer: Timer::Precheck,
                        ..
                    },
                ]
            ),
            "{effects:?}"
        );
        let (effects, mut bystander) = park(
            &mut state,
            &invoker,
            &admitted,
            connection_wait(other, 70),
            4,
        );
        let [
            Effect::ArmTimer {
                timer: Timer::Connection(_),
                after_nanos: 66,
            },
            read @ ..,
        ] = effects.as_slice()
        else {
            panic!("{effects:?}");
        };
        assert!(
            prechecks(read, Check::Connections, &[1, 2]),
            "one subscription and one precheck timer; the park under the held \
             subscription reads the requests: {effects:?}"
        );
        let effects = deliver(apply_without_tick(
            &mut state,
            Message::Timer(timer.clone()),
            50,
        ));
        assert!(effects.is_empty(), "{effects:?}");
        woken.try_recv().expect("woken");
        bystander
            .try_recv()
            .expect_err("another request's wait stays parked");
        let (_, mut again) = park(
            &mut state,
            &invoker,
            &admitted,
            connection_wait(request, 50),
            51,
        );
        let effects = deliver(apply_without_tick(&mut state, Message::Stop, 52));
        assert!(
            matches!(
                effects.as_slice(),
                [
                    Effect::ArmTimer {
                        timer: Timer::Grace,
                        ..
                    },
                    Effect::Unsubscribe,
                    Effect::TerminateProgram,
                ]
            ),
            "{effects:?}"
        );
        assert!(
            matches!(
                again.try_recv(),
                Err(tokio::sync::oneshot::error::TryRecvError::Closed)
            ),
            "a stop lets the parked Invocations go"
        );
        assert!(matches!(
            bystander.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Closed)
        ));
        assert!(apply_without_tick(&mut state, Message::Timer(timer), 53).is_empty());
        let (effects, mut late) = park(
            &mut state,
            &invoker,
            &admitted,
            connection_wait(request, 90),
            54,
        );
        assert!(
            effects.is_empty(),
            "a stopped Run parks nothing: {effects:?}"
        );
        assert!(matches!(
            late.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Closed)
        ));
    }
}

/// The owner's wake-ups and prechecks of its parked Invocations, in mailbox
/// order: an approval wait subscribes to nothing, so a change rings nothing
/// and reads nothing; a connection wait holds the one subscription, and the
/// rings it brings are coalesced into one read of the connection requests
/// per wake gap; the periodic precheck reads every parked wait, wakes only
/// the parks it found still parked under the serial it read, and is armed
/// again while waits remain; a failed check finds nothing.
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one table of wake-up and precheck message orders, each case a block"
)]
async fn apply_wakes_parked_invocations_on_their_own_evidence_and_prechecks_the_rest() {
    let (engine, _release) = BlockingEngine::new();
    let fixture = TestModules::new(Arc::new(engine)).await;
    let tool = fixture
        .user_tool("user:apply-wakeups", "Parked waits")
        .await;
    let run = fixture
        .runs
        .start(&tool.principal, &tool.workspace_id, submission("return 1;"))
        .await
        .unwrap();
    wait_until_started(&fixture, &tool.workspace_id, run.run_id()).await;
    let record = fixture.run_record(&tool.workspace_id, run.run_id()).await;
    let admitted = admitted_invocation(&fixture, &tool, run.run_id()).await;
    let invoker = test_invoker(&fixture, &tool, &record);

    // An approval wait, then a connection wait: the first arms the periodic
    // precheck, the second the subscription. A ring before the subscription
    // reads nothing; once subscribed, a ring reads the connection requests
    // and starts the wake gap, rings meanwhile coalesce, and the next read
    // starts when the gap is out. A read that finds the request ended wakes
    // its Invocation and drops the subscription.
    {
        let mut state = started(LIMITS);
        let approval = approval_id();
        let request = connection_request();
        let (effects, mut approval_woken) =
            park(&mut state, &invoker, &admitted, approval_wait(&approval), 3);
        assert_eq!(effects.len(), 2, "{effects:?}");
        assert!(
            apply_without_tick(&mut state, Message::Changed, 4).is_empty(),
            "no subscription: an unrelated commit reads nothing"
        );
        let (effects, mut connection_woken) = park(
            &mut state,
            &invoker,
            &admitted,
            connection_wait(request, 500),
            5,
        );
        assert!(
            matches!(
                effects.as_slice(),
                [
                    Effect::ArmTimer {
                        timer: Timer::Connection(_),
                        ..
                    },
                    Effect::Subscribe
                ]
            ),
            "{effects:?}"
        );
        let effects = apply_without_tick(&mut state, Message::Changed, 6);
        assert!(prechecks(&effects, Check::Connections, &[2]), "{effects:?}");
        assert!(apply_without_tick(&mut state, Message::Changed, 7).is_empty());
        let effects = apply_without_tick(
            &mut state,
            Message::Prechecked {
                check: Check::Connections,
                found: Ok(Vec::new()),
            },
            8,
        );
        assert!(effects.is_empty(), "the gap is not out: {effects:?}");
        assert!(apply_without_tick(&mut state, Message::Changed, 9).is_empty());
        let effects = apply_without_tick(&mut state, Message::Timer(Timer::WakeGap), 10);
        assert!(
            prechecks(&effects, Check::Connections, &[2]),
            "the rings coalesce into one read: {effects:?}"
        );
        let effects = apply_without_tick(&mut state, Message::Timer(Timer::WakeGap), 11);
        assert!(effects.is_empty(), "nothing rang since: {effects:?}");
        let effects = deliver(apply_without_tick(
            &mut state,
            Message::Prechecked {
                check: Check::Connections,
                found: Ok(vec![2]),
            },
            12,
        ));
        assert!(
            matches!(effects.as_slice(), [Effect::Unsubscribe]),
            "{effects:?}"
        );
        connection_woken.try_recv().expect("woken");
        approval_woken.try_recv().expect_err("still parked");
        assert!(apply_without_tick(&mut state, Message::Changed, 13).is_empty());
        assert_eq!(state.parked_invocations(), 1);
    }

    // A connection wait that parks under a subscription already held counts
    // as a ring: a ring that came before its message read a batch without
    // it, and its request may have ended since its held recheck read it.
    // Inside the wake gap the read waits for the gap, and then reads both.
    {
        let mut state = started(LIMITS);
        let (effects, _first) = park(
            &mut state,
            &invoker,
            &admitted,
            connection_wait(connection_request(), 500),
            3,
        );
        assert!(
            effects
                .iter()
                .any(|effect| matches!(effect, Effect::Subscribe))
        );
        let effects = apply_without_tick(&mut state, Message::Changed, 4);
        assert!(prechecks(&effects, Check::Connections, &[1]), "{effects:?}");
        let (effects, _second) = park(
            &mut state,
            &invoker,
            &admitted,
            connection_wait(connection_request(), 500),
            5,
        );
        assert!(
            matches!(
                effects.as_slice(),
                [Effect::ArmTimer {
                    timer: Timer::Connection(_),
                    ..
                }]
            ),
            "the first read still runs: {effects:?}"
        );
        assert!(
            apply_without_tick(
                &mut state,
                Message::Prechecked {
                    check: Check::Connections,
                    found: Ok(Vec::new()),
                },
                6,
            )
            .is_empty(),
            "the gap is not out"
        );
        let effects = apply_without_tick(&mut state, Message::Timer(Timer::WakeGap), 7);
        assert!(
            prechecks(&effects, Check::Connections, &[1, 2]),
            "the second park is read without a ring of its own: {effects:?}"
        );
    }

    // The periodic precheck reads every parked wait. A finding wakes the
    // parks it names; a park woken meanwhile by the owner's own record is
    // not woken twice; the precheck is armed again while waits remain and
    // not once none does. A failed precheck wakes nothing.
    {
        let mut state = started(LIMITS);
        let approval = approval_id();
        let request = connection_request();
        let (_, mut approval_woken) =
            park(&mut state, &invoker, &admitted, approval_wait(&approval), 3);
        let (_, mut connection_woken) = park(
            &mut state,
            &invoker,
            &admitted,
            connection_wait(request, 500),
            4,
        );
        let effects = apply_without_tick(&mut state, Message::Timer(Timer::Precheck), 5);
        assert!(prechecks(&effects, Check::Periodic, &[1, 2]), "{effects:?}");
        let effects = apply_without_tick(
            &mut state,
            Message::Prechecked {
                check: Check::Periodic,
                found: Err(Error::OwnerLost),
            },
            6,
        );
        assert!(
            matches!(
                effects.as_slice(),
                [Effect::ArmTimer {
                    timer: Timer::Precheck,
                    after_nanos: PRECHECK
                }]
            ),
            "a failed precheck wakes nothing and is read again: {effects:?}"
        );
        let effects = apply_without_tick(&mut state, Message::Timer(Timer::Precheck), 7);
        assert!(prechecks(&effects, Check::Periodic, &[1, 2]), "{effects:?}");
        let effects = deliver(apply_without_tick(
            &mut state,
            Message::Prechecked {
                check: Check::Periodic,
                found: Ok(vec![2]),
            },
            8,
        ));
        assert!(
            matches!(
                effects.as_slice(),
                [
                    Effect::Unsubscribe,
                    Effect::ArmTimer {
                        timer: Timer::Precheck,
                        ..
                    },
                ]
            ),
            "{effects:?}"
        );
        connection_woken.try_recv().expect("woken");
        approval_woken.try_recv().expect_err("still parked");
        let effects = apply_without_tick(&mut state, Message::Timer(Timer::Precheck), 9);
        assert!(prechecks(&effects, Check::Periodic, &[1]), "{effects:?}");
        // The decision lands before the precheck answers: the owner's
        // record wakes the Invocation, and the finding finds it gone.
        assert!(deliver(decide(&mut state, &approval, "decision", 10)).is_empty());
        committed_chunk(apply_without_tick(&mut state, Message::Drained, 11));
        deliver(apply_without_tick(&mut state, Message::Committed, 12));
        approval_woken.try_recv().expect("woken");
        let effects = apply_without_tick(
            &mut state,
            Message::Prechecked {
                check: Check::Periodic,
                found: Ok(vec![1]),
            },
            13,
        );
        assert!(
            effects.is_empty(),
            "a stale finding wakes nothing and nothing is left to precheck: {effects:?}"
        );
        assert_eq!(state.parked_invocations(), 0);
        let effects = apply_without_tick(&mut state, Message::Timer(Timer::Precheck), 14);
        assert!(effects.is_empty(), "{effects:?}");
    }

    // A precheck due past the Run deadline on the owner's clock, before the
    // deadline's timer arrived: the timeout is applied first, as the timer
    // would have been, the parked Invocation is let go, and nothing is read.
    {
        let mut state = started(LIMITS);
        let (_, mut woken) = park(
            &mut state,
            &invoker,
            &admitted,
            approval_wait(&approval_id()),
            3,
        );
        let effects = deliver(apply_without_tick(
            &mut state,
            Message::Timer(Timer::Precheck),
            LIMITS.deadline_at_unix_nanos,
        ));
        assert!(stops(&effects), "{effects:?}");
        assert_eq!(
            state.termination().map(|intent| intent.termination.kind),
            Some(crate::lifecycle::RunTerminationKind::TimedOut)
        );
        assert!(matches!(
            woken.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Closed)
        ));
    }

    // A precheck due once the Run takes no more Program requests reads
    // nothing, and its timer is not armed again.
    {
        let mut state = started(LIMITS);
        let (_, _woken) = park(
            &mut state,
            &invoker,
            &admitted,
            approval_wait(&approval_id()),
            3,
        );
        deliver(apply_without_tick(&mut state, Message::Stop, 4));
        assert_eq!(state.parked_invocations(), 0);
        let effects = apply_without_tick(&mut state, Message::Timer(Timer::Precheck), 5);
        assert!(effects.is_empty(), "{effects:?}");
    }
}

/// A recovered owner applies the Run Inbox requests the sweep queued after
/// the timers, as a live owner applies them: a decision past its request's
/// deadline is a repeat of its expiry; a cancellation is the first
/// termination unless the Run deadline passed, whose timeout comes first; a
/// decision after a termination closes its request. Its chunk deletes every
/// row it applied.
#[test]
fn a_recovered_owner_applies_its_run_inbox_requests_in_insert_order() {
    use crate::run_inbox::InboxRow;
    #[derive(Debug, PartialEq)]
    enum Step {
        Expire,
        Decide(bool),
        Cancel,
    }
    let run_deadline = LIMITS.deadline_at_unix_nanos;
    // (the rows delivered: (row, at), a cancel or a decision; what the
    // chunk writes, the cancellation last)
    let cases = [
        (vec![("decision", 3)], vec![Step::Decide(true)]),
        (vec![("decision", APPROVAL_DEADLINE)], vec![Step::Expire]),
        (
            vec![("decision", 3), ("cancel", 4)],
            vec![Step::Decide(true), Step::Cancel],
        ),
        (
            vec![("cancel", 3), ("decision", 4)],
            vec![Step::Decide(false), Step::Cancel],
        ),
        (vec![("cancel", run_deadline)], vec![]),
    ];
    for (delivered, written) in cases {
        let approval = approval_id();
        let mut state = OwnerState::recovered(LIMITS, 1);
        for &(row, at) in &delivered {
            let effects = if row == "cancel" {
                cancel(&mut state, row, at)
            } else {
                decide(&mut state, &approval, row, at)
            };
            assert!(effects.is_empty(), "{delivered:?}: {effects:?}");
        }
        let effects = apply_without_tick(&mut state, Message::Recover, run_deadline + 1);
        let [Effect::Recover(recovered)] = effects.as_slice() else {
            panic!("{delivered:?}: one chunk, nothing else: {effects:?}");
        };
        let steps = recovered
            .steps
            .iter()
            .map(|step| match step {
                RecoveryStep::Expire(expired) => {
                    assert_eq!(*expired, approval);
                    Step::Expire
                }
                RecoveryStep::Decide { decide, .. } => Step::Decide(*decide),
            })
            .chain(recovered.termination.iter().map(|intent| {
                assert_eq!(intent.requester, Some(requester()));
                Step::Cancel
            }))
            .collect::<Vec<_>>();
        assert_eq!(steps, written, "{delivered:?}");
        assert_eq!(
            recovered.rows,
            delivered
                .iter()
                .map(|(row, _)| InboxRow((*row).to_string()))
                .collect::<Vec<_>>(),
            "{delivered:?}"
        );
    }
    // A cancellation whose intent the row cannot hold is not applied, and
    // its row stays.
    let mut state = OwnerState::recovered(
        OwnerLimits {
            shutdown_grace_nanos: i64::MAX,
            ..LIMITS
        },
        1,
    );
    assert!(cancel(&mut state, "cancel", 3).is_empty());
    let effects = apply_without_tick(&mut state, Message::Recover, 4);
    let [Effect::Recover(recovered)] = effects.as_slice() else {
        panic!("one chunk, nothing else: {effects:?}");
    };
    assert!(recovered.termination.is_none() && recovered.rows.is_empty());
}

/// A recovered owner gathers the Approval Request timers the sweep queued,
/// in order, and its end writes them in one chunk. The write's answer, or a
/// stop (the Shutdown Drain is out) whether the chunk is queued or in
/// flight, retires it.
#[test]
fn a_recovered_owner_writes_its_queued_expiries_in_one_chunk_and_retires() {
    let (first, second) = (approval_id(), approval_id());
    let mut state = OwnerState::recovered(LIMITS, 1);
    let mut effects = Vec::new();
    for message in [
        Message::Timer(Timer::Approval(first.clone())),
        Message::Timer(Timer::Approval(second.clone())),
        Message::Recover,
    ] {
        effects.extend(apply_without_tick(&mut state, message, 3));
    }
    let [Effect::Recover(recovered)] = effects.as_slice() else {
        panic!("one chunk, nothing else: {effects:?}");
    };
    assert!(
        matches!(
            recovered.steps.as_slice(),
            [RecoveryStep::Expire(one), RecoveryStep::Expire(two)] if *one == first && *two == second
        ),
        "{recovered:?}"
    );
    assert!(apply_without_tick(&mut state, Message::Drained, 4).is_empty());
    assert!(matches!(
        apply_without_tick(
            &mut state,
            Message::Interrupted(Err(CommitRejection::ClaimMismatch)),
            5
        )
        .as_slice(),
        [Effect::Retire]
    ));
    assert_eq!(state.phase(), Phase::Retired);
    for writing in [false, true] {
        let mut state = OwnerState::recovered(LIMITS, 1);
        if writing {
            apply_without_tick(&mut state, Message::Recover, 3);
        }
        let effects = apply_without_tick(&mut state, Message::Stop, 4);
        assert!(
            matches!(effects.as_slice(), [Effect::Retire]),
            "writing {writing}: {effects:?}"
        );
        assert_eq!(state.phase(), Phase::Retired);
    }
}
