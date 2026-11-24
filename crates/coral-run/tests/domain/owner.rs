//! A Run stream's loop (ADR 0018, ADR 0027): Program requests answer at
//! Applied (at Recorded above the Applied cap), the Run Ledger lands in
//! chunks, and a claim mismatch stops the loop. The `apply` tables drive the
//! loop without a database.
use super::*;

use super::run_stream_reconnect::finish_with;
use super::run_streams::{ended, started as started_on_a_stream};
use crate::lifecycle::RunTerminationKind;
use crate::owner::{
    CHUNK_WORK_CAP, Check, CommitRejection, DrainStep, Effect, Message, OwnerLimits, OwnerState,
    ParkedWait, Phase, StartedExecution, Timer, apply,
};
use crate::{AppliedOutputCap, CHUNK_BYTES_CAP, CHUNK_EVENTS_CAP, CoralFailure, FanOutLimit};
use coral_run_executor::wire::v1::RunEnd as WireEnd;

/// Catches: a burst of Emissions committed one chunk each, or a chunk that
/// lands out of order or in part. While the first chunk waits to commit,
/// the rest queue behind it; the burst then lands in a few chunks,
/// prefix-complete, under the default Applied cap and above the smallest.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_burst_of_emits_commits_in_chunks_and_stays_prefix_complete() {
    let full = serde_json::Value::String("x".repeat(CHUNK_BYTES_CAP - 2));
    let smallest = AppliedOutputCap::new(2 * CHUNK_BYTES_CAP).expect("the smallest cap");
    let cases: [(AppliedOutputCap, Vec<serde_json::Value>, u64); 2] = [
        (
            AppliedOutputCap::default(),
            (0..200).map(|index| serde_json::json!(index)).collect(),
            4,
        ),
        // One chunk each: the third crosses the cap.
        (smallest, vec![full; 3], 4),
    ];
    for (cap, values, most_chunks) in cases {
        let (commit_gate, release_commit) = CommitGate::blocking();
        let fixture = TestModules::with_execution_adapters(
            Arc::new(ImmediateEngine),
            Arc::new(RejectingUpstream),
            Arc::new(NoopInvocationObserver),
            commit_gate.clone(),
        )
        .await;
        let runs = fixture.runs.clone().with_applied_output_cap(cap);
        let tool = fixture.user_tool("user:burst", "Emit a burst").await;
        let (run_id, mut stream) = started_on_a_stream(&runs, &tool).await;
        let started = fixture.run_record(&tool.workspace_id, &run_id).await;
        let emits = u64::try_from(values.len()).expect("a small burst");
        let dequeued = commit_gate.dequeued();

        for (number, value) in (1..).zip(&values) {
            stream.request(number, super::run_streams::emit(value));
        }
        // The first chunk is held at its commit while the loop applies the
        // burst: nothing lands.
        wait_until(|| commit_gate.dequeued() - dequeued == values.len()).await;
        assert!(commit_gate.entered());
        assert_eq!(
            fixture.run_record(&tool.workspace_id, &run_id).await,
            started
        );
        assert_eq!(
            fixture.events_after(&tool.workspace_id, &run_id, 1).await,
            [] as [RunEvent; 0]
        );

        release_commit.send(()).expect("release the commit");
        stream.request(emits + 1, finish_with("null"));
        stream
            .until(|read| read.contains(&ended(WireEnd::Succeeded)))
            .await;
        let after = fixture.run_record(&tool.workspace_id, &run_id).await;
        // The held chunk, the chunks that queued behind it, and the
        // terminal chunk: far fewer chunks than Emissions.
        assert!(
            after.chunk_sequence - started.chunk_sequence <= most_chunks,
            "{} chunks for {emits} Emissions",
            after.chunk_sequence - started.chunk_sequence
        );
        let emitted = retained_events(&runs, &tool.principal, &tool.workspace_id, &run_id)
            .await
            .into_iter()
            .filter_map(|event| match event.kind() {
                RunEventKind::ProgramEmitted { value } => {
                    Some((event.sequence(), value.as_json().clone()))
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(emitted, (2..).zip(values).collect::<Vec<_>>());
        assert_eq!(last_sequence(&after), emits + 2);
    }
}

/// A clock the host cannot read stops the loop like a failed commit; the
/// Program's end that follows still reaches the terminal write, which fails
/// on the same clock, and the loop retires with the Run's end unrecorded.
/// Without that, the end was discarded with the failed clock read and the
/// loop stayed registered forever.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failing_clock_stops_the_owner_and_the_programs_end_still_retires_it() {
    let fixture = TestModules::new(Arc::new(ImmediateEngine)).await;
    let tool = fixture.user_tool("user:clock", "Lose the clock").await;
    let (run_id, stream) = started_on_a_stream(&fixture.runs, &tool).await;
    let events = fixture.events_after(&tool.workspace_id, &run_id, 0).await;
    fixture.clock.fail();

    stream.request(1, finish_with("null"));
    wait_until(|| !fixture.runs.has_owner(&run_id)).await;
    let abandoned = fixture.run_record(&tool.workspace_id, &run_id).await;
    assert_eq!(abandoned.state, RunState::Running);
    assert!(abandoned.termination.is_none());
    assert_eq!(
        fixture.events_after(&tool.workspace_id, &run_id, 0).await,
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
    (apply(state, message, now), receiver)
}

type Answer = tokio::sync::oneshot::Receiver<Result<(), Error>>;

fn result(state: &mut OwnerState, bytes: usize, now: i64) -> (Vec<Effect>, Answer, Settled) {
    result_freeing(state, bytes, true, now)
}

/// [`result`] of an Invocation that frees a slot as it is applied when
/// `frees_slot`.
fn result_freeing(
    state: &mut OwnerState,
    bytes: usize,
    frees_slot: bool,
    now: i64,
) -> (Vec<Effect>, Answer, Settled) {
    let (reply, receiver) = reply();
    let (work, settled) = probe_work();
    (
        apply(
            state,
            Message::InvocationResult {
                bytes,
                work,
                frees_slot,
                reply,
            },
            now,
        ),
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

/// A Run stream's loop started at 2 with the deadline `limits` names: the
/// start arms the Run deadline and starts the Program.
fn started(limits: OwnerLimits) -> OwnerState {
    let mut state = OwnerState::for_run_stream(limits, (0, 0));
    let deadline = limits.deadline_at_unix_nanos;
    assert!(matches!(
        apply(
            &mut state,
            Message::Started(Ok(Some(StartedExecution {
                head: 1,
                deadline_at_unix_nanos: deadline,
            }))),
            2
        )
        .as_slice(),
        [
            Effect::ArmTimer {
                timer: Timer::RunDeadline,
                after_nanos: until_deadline,
            },
            Effect::StartProgram { deadline_at_unix_nanos }
        ] if *until_deadline == deadline - 2 && *deadline_at_unix_nanos == deadline
    ));
    assert_eq!(state.phase(), Phase::Running);
    assert_eq!(state.head(), 1);
    state
}

/// The termination a Run stream's end writes, when `effects` end with it.
fn ended_as(effects: &[Effect]) -> Option<RunTerminationKind> {
    match effects {
        [.., Effect::EndStreamRun(end)] => Some(end.termination.kind),
        _ => None,
    }
}

const LIMITS: OwnerLimits = OwnerLimits {
    run_output_bytes: 64 << 20,
    applied_cap_bytes: AppliedOutputCap::DEFAULT_BYTES,
    fan_out_invocations: FanOutLimit::DEFAULT_INVOCATIONS,
    deadline_at_unix_nanos: 1_000_000,
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
            "a rejected commit terminates the Program: {rest:?}"
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
                [Effect::CompleteExecution {
                    completion: ExecutionCompletion::ProgramHostTerminated,
                    ..
                }]
            ),
            "{effects:?}"
        );
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
                [Effect::CompleteExecution {
                    completion: ExecutionCompletion::Succeeded(FinalValue::Absent),
                    ..
                }]
            ),
            "{effects:?}"
        );
        assert_eq!(state.phase(), Phase::Completing);
        assert!(matches!(
            apply(&mut state, Message::Completed(Ok(())), 63).as_slice(),
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
                [Effect::TerminateProgram, Effect::CompleteExecution { .. }]
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
                [Effect::CompleteExecution {
                    completion: ExecutionCompletion::Succeeded(FinalValue::Absent),
                    ..
                }]
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

    // The Run Executor's own end of the Program (a limit, a lost Program
    // host) is written first, as the deadline is: what is pending commits,
    // then one chunk ends the Run and its open Invocations, with no grace
    // and no `Terminate` before it. Past the deadline the Run times out. A
    // result that arrives after the end is refused.
    for (completion, now, kind) in [
        (
            ExecutionCompletion::CpuLimitExceeded,
            90,
            RunTerminationKind::CpuLimitExceeded,
        ),
        (
            ExecutionCompletion::MemoryLimitExceeded,
            90,
            RunTerminationKind::MemoryLimitExceeded,
        ),
        (
            ExecutionCompletion::ProgramHostTerminated,
            90,
            RunTerminationKind::Interrupted,
        ),
        (
            ExecutionCompletion::CpuLimitExceeded,
            LIMITS.deadline_at_unix_nanos,
            RunTerminationKind::TimedOut,
        ),
    ] {
        let mut state = started(LIMITS);
        let (effects, _, _) = result(&mut state, 10, now - 1);
        deliver(effects);
        let effects = apply(&mut state, Message::ProgramEnded(completion.clone()), now);
        assert!(
            matches!(
                effects.as_slice(),
                [Effect::TerminateProgram, Effect::Commit { .. }]
            ),
            "{completion:?}: the pending result commits first: {effects:?}"
        );
        assert!(state.ends_stream_first(), "{completion:?}");
        let effects = apply(&mut state, Message::Committed, now + 1);
        assert_eq!(
            ended_as(&effects),
            Some(kind),
            "{completion:?}: {effects:?}"
        );
        assert_eq!(state.phase(), Phase::Completing);
        let (effects, _, late_work) = result(&mut state, 10, now + 2);
        assert!(deliver(effects).is_empty());
        let rest = deliver(apply(&mut state, Message::Completed(Ok(())), now + 3));
        assert!(matches!(rest.as_slice(), [Effect::Retire]), "{rest:?}");
        assert!(
            matches!(
                late_work.lock().expect("settle").as_ref(),
                Some(Err(Error::ProgramHostTerminated))
            ),
            "{completion:?}: a result after the end is never recorded"
        );
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
            "a rejected commit terminates the Program: {rest:?}"
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

/// The enforcement rows of the `apply` table: enforcement requests gather in
/// one chunk's batch and commit when free; a stop answers every batch not yet
/// committed at once and nothing else; a request past the deadline or after a
/// stop is answered at once; the terminal write waits for a batch in flight.
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one table of enforcement message orders, each case a block"
)]
async fn apply_batches_enforcement_requests_and_a_stop_answers_the_uncommitted_ones_at_once() {
    use crate::invocation::{EnforcementOutcome, EnforcementRequest};
    use crate::owner::ENFORCEMENT_BATCH_CAP;

    type EnforcementAnswer = tokio::sync::oneshot::Receiver<Result<EnforcementOutcome, Error>>;

    let fixture = TestModules::new(Arc::new(ImmediateEngine)).await;
    let tool = fixture
        .user_tool("user:apply-enforcement", "Enforcement rows")
        .await;
    let (run_id, _stream) = started_on_a_stream(&fixture.runs, &tool).await;
    let record = fixture.run_record(&tool.workspace_id, &run_id).await;
    // Four Invocations: a request names one of them, so a batch says which
    // ones it holds.
    let mut invocations = Vec::new();
    for _ in 0..4 {
        invocations.push(admitted_invocation(&fixture, &tool, &run_id).await);
    }
    // A call to a Private Tool, which Coral performs in the Program.
    let performed = admitted_private_invocation(&fixture, &tool, &run_id, "weather.spin").await;
    let ids: [crate::InvocationId; 4] = invocations
        .iter()
        .map(|admitted| admitted.id.clone())
        .collect::<Vec<_>>()
        .try_into()
        .expect("four Invocations");
    let invoker = test_invoker(&fixture, &tool, &record);
    let enforce_as =
        |state: &mut OwnerState, index: usize, now: i64| -> (Vec<Effect>, EnforcementAnswer) {
            let (sender, receiver) = tokio::sync::oneshot::channel();
            let admitted = invocations.get(index).expect("one of four").clone();
            let request = EnforcementRequest::for_apply_test(invoker.clone(), admitted, sender);
            (
                apply(state, Message::Enforce(Box::new(request)), now),
                receiver,
            )
        };
    let enforce = |state: &mut OwnerState, now: i64| enforce_as(state, 0, now);
    // One request per Invocation, in order, at the instants of `nows`;
    // the effects delivered.
    let enforce_each =
        |state: &mut OwnerState, nows: std::ops::Range<i64>| -> Vec<EnforcementAnswer> {
            nows.enumerate()
                .map(|(index, now)| {
                    let (effects, answer) = enforce_as(state, index, now);
                    deliver(effects);
                    answer
                })
                .collect()
        };
    let answered = |receiver: &mut EnforcementAnswer| receiver.try_recv().ok();
    // The Invocations one commit's batch holds.
    let batched = |effects: &[Effect]| -> Vec<crate::InvocationId> {
        let [Effect::Commit { chunk, .. }] = effects else {
            panic!("{effects:?}");
        };
        chunk
            .enforcement
            .as_ref()
            .map_or_default(|batch| batch.invocation_ids())
    };

    // Three requests, one batch, one chunk; nothing commits before the drain.
    {
        let mut state = started(LIMITS);
        let mut answers = Vec::new();
        for now in [3, 4, 5] {
            let (effects, answer) = enforce(&mut state, now);
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
            "the enforcement point writes no Event and is no item"
        );
        assert_eq!(chunk.works, 1, "one batch counts as one row work");
        assert_eq!(
            chunk
                .enforcement
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
            fan_out_invocations: ENFORCEMENT_BATCH_CAP + 1,
            ..LIMITS
        });
        for now in 0..=ENFORCEMENT_BATCH_CAP {
            let (effects, _) = enforce(&mut state, 10 + i64::try_from(now).unwrap());
            deliver(effects);
        }
        assert_eq!(state.pending_chunks(), 2);
        let effects = apply(&mut state, Message::Drained, 100);
        let [Effect::Commit { chunk, .. }] = effects.as_slice() else {
            panic!("{effects:?}");
        };
        assert_eq!(
            chunk
                .enforcement
                .as_ref()
                .map(|batch| batch.invocation_ids().len()),
            Some(ENFORCEMENT_BATCH_CAP)
        );
    }

    // An enforcement-only chunk is no empty chunk for an Event above the bytes
    // cap: that Event still commits alone, after the batch.
    {
        let mut state = started(LIMITS);
        let (effects, _) = enforce(&mut state, 10);
        deliver(effects);
        let (effects, _) = emit(&mut state, CHUNK_BYTES_CAP + 1, 11);
        deliver(effects);
        assert_eq!(state.pending_chunks(), 2);
        let effects = apply(&mut state, Message::Drained, 12);
        let [Effect::Commit { chunk, .. }] = effects.as_slice() else {
            panic!("{effects:?}");
        };
        assert!(
            chunk.enforcement.is_some() && chunk.items.is_empty(),
            "the batch commits first, without the Event"
        );
    }

    // The deadline's stop: the batch not yet committed answers terminated
    // at once, the Program is terminated, and the chunk that held only the
    // batch is gone, so the end is written at once.
    {
        let mut state = started(LIMITS);
        let (effects, mut first) = enforce(&mut state, 20);
        deliver(effects);
        let (effects, mut second) = enforce(&mut state, 21);
        deliver(effects);
        let rest = deliver(apply(&mut state, Message::Timer(Timer::RunDeadline), 22));
        assert!(
            matches!(
                rest.as_slice(),
                [Effect::TerminateProgram, Effect::EndStreamRun(_)]
            ),
            "the stop terminates the Program and writes the end: {rest:?}"
        );
        for answer in [&mut first, &mut second] {
            assert!(
                matches!(answered(answer), Some(Err(Error::ProgramHostTerminated))),
                "an enforcement request not yet committed learns the stop at once"
            );
        }
        assert_eq!(state.pending_chunks(), 0);
        assert!(apply(&mut state, Message::Drained, 23).is_empty());
        // A request after the stop answers at once too.
        let (effects, mut late) = enforce(&mut state, 24);
        assert!(deliver(effects).is_empty());
        assert!(matches!(
            answered(&mut late),
            Some(Err(Error::ProgramHostTerminated))
        ));
    }

    // A stop leaves the chunk's other pending work: an Emission beside the
    // abandoned batch still commits, without the batch, before the end.
    {
        let mut state = started(LIMITS);
        let (effects, _) = emit(&mut state, 10, 20);
        deliver(effects);
        let (effects, mut pending) = enforce(&mut state, 21);
        deliver(effects);
        let effects = deliver(apply(&mut state, Message::Timer(Timer::RunDeadline), 22));
        assert!(matches!(
            answered(&mut pending),
            Some(Err(Error::ProgramHostTerminated))
        ));
        let [Effect::TerminateProgram, Effect::Commit { chunk, .. }] = effects.as_slice() else {
            panic!("{effects:?}");
        };
        assert!(
            chunk.enforcement.is_none(),
            "the abandoned batch left the chunk"
        );
        assert_eq!(chunk.items.len(), 1, "the Emission commits");
    }

    // Past the deadline in owner memory a request answers at once.
    {
        let mut state = started(LIMITS);
        let (effects, mut late) = enforce(&mut state, LIMITS.deadline_at_unix_nanos);
        assert!(deliver(effects).is_empty());
        assert!(matches!(
            answered(&mut late),
            Some(Err(Error::ProgramHostTerminated))
        ));
        assert_eq!(state.pending_chunks(), 0);
    }

    // The terminal chunk waits for an enforcement check in flight: the
    // Program's end writes nothing until the batch's commit has answered.
    {
        let mut state = started(LIMITS);
        let (effects, mut answer) = enforce(&mut state, 30);
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
        let mut answers = enforce_each(&mut state, 3..7);
        assert_eq!(state.slots_held(), 2);
        assert_eq!(state.waiting_for_a_slot(), 2);
        assert_eq!(state.pending_chunks(), 1);
        let effects = apply(&mut state, Message::Drained, 7);
        assert_eq!(batched(&effects), [ids[0].clone(), ids[1].clone()]);
        assert!(deliver(apply(&mut state, Message::Committed, 8)).is_empty());
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
        let effects = apply(&mut state, Message::Drained, 10);
        let [Effect::Commit { chunk, .. }] = effects.as_slice() else {
            panic!("{effects:?}");
        };
        assert_eq!(chunk.items.len(), 1, "the result's row rides the chunk");
        assert_eq!(
            batched(&effects),
            [ids[2].clone()],
            "the third, not the fourth"
        );
        deliver(apply(&mut state, Message::Committed, 11));

        deliver(apply(&mut state, Message::DispatchEnded, 12));
        deliver(apply(&mut state, Message::DispatchEnded, 13));
        assert_eq!(
            state.slots_held(),
            1,
            "the fourth took the first slot freed"
        );
        assert_eq!(state.waiting_for_a_slot(), 0);
        let effects = apply(&mut state, Message::Drained, 14);
        assert_eq!(batched(&effects), [ids[3].clone()]);
        deliver(apply(&mut state, Message::Committed, 15));
        let (effects, _) = enforce(&mut state, 16);
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
        let mut answers = enforce_each(&mut state, 20..23);
        deliver(apply(&mut state, Message::Drained, 23));
        deliver(apply(&mut state, Message::Committed, 24));
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
        deliver(apply(&mut state, Message::DispatchEnded, 26));
        assert_eq!(state.slots_held(), 2);
        assert_eq!(state.waiting_for_a_slot(), 0);
        assert_eq!(state.pending_chunks(), 1, "the row's end freed the slot");
    }

    // A call Coral performs in the Program holds no slot, so the calls its
    // body makes never wait for its own: at the limit it batches at once,
    // ahead of a request that waits for a slot, and its result frees none.
    {
        let mut state = started(LIMITED);
        let mut answers = enforce_each(&mut state, 50..53);
        deliver(apply(&mut state, Message::Drained, 53));
        deliver(apply(&mut state, Message::Committed, 54));
        let (sender, _answer) = tokio::sync::oneshot::channel();
        let request =
            EnforcementRequest::for_apply_test(invoker.clone(), performed.clone(), sender);
        assert!(deliver(apply(&mut state, Message::Enforce(Box::new(request)), 55)).is_empty());
        assert_eq!(state.slots_held(), 2);
        assert_eq!(state.waiting_for_a_slot(), 1);
        let effects = apply(&mut state, Message::Drained, 56);
        assert_eq!(batched(&effects), std::slice::from_ref(&performed.id));
        deliver(apply(&mut state, Message::Committed, 57));

        let (effects, mut applied, _work) = result_freeing(&mut state, 10, false, 58);
        assert!(deliver(effects).is_empty());
        assert!(matches!(applied.try_recv(), Ok(Ok(()))));
        assert_eq!(state.slots_held(), 2, "its result frees no slot");
        assert_eq!(state.waiting_for_a_slot(), 1);
        assert!(answered(answers.last_mut().expect("the waiting request")).is_none());
    }

    // Once the Run takes no more Program requests, the request waiting for
    // a slot is answered at once with the error the ending makes, while the
    // batch in flight answers on its own: a stop, the Program's end and a
    // lost claim each end a slot wait.
    for (ending, message, lost) in [
        (
            "the deadline's stop",
            Message::Timer(Timer::RunDeadline),
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
        let mut answers = enforce_each(&mut state, 30..33);
        deliver(apply(&mut state, Message::Drained, 33));
        assert!(state.has_commit_in_flight(), "{ending}");
        deliver(apply(&mut state, message, 34));
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
        let mut answers = enforce_each(&mut state, 40..44);
        deliver(apply(&mut state, Message::Drained, 44));
        deliver(apply(&mut state, Message::Committed, 45));
        let effects = apply(
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

    // The Shutdown Drain on a Run stream's owner: a request after `Hold`
    // starts no dispatch and stays in memory; `Move` waits for the
    // dispatch in flight, and its result frees the stream to move. The
    // held request is answered as the Program's host ending, its result's
    // row commits with no dispatch, and nothing writes the Run's end.
    {
        let mut state = started(LIMITED);
        let mut answers = enforce_each(&mut state, 50..51);
        deliver(apply(&mut state, Message::Drained, 51));
        let effects = apply(&mut state, Message::Drain(DrainStep::Hold), 52);
        assert!(deliver(effects).is_empty());
        let (effects, mut held) = enforce_as(&mut state, 1, 53);
        assert!(deliver(effects).is_empty());
        assert_eq!(state.waiting_for_a_slot(), 1, "held, with a slot free");
        assert_eq!(state.slots_held(), 1);
        let effects = apply(&mut state, Message::Drain(DrainStep::Move), 54);
        assert!(effects.is_empty(), "the batch is in flight: {effects:?}");
        let effects = deliver(apply(&mut state, Message::Committed, 55));
        assert!(effects.is_empty(), "its dispatch is in flight: {effects:?}");
        assert!(answered(answers.first_mut().expect("the batched request")).is_none());
        let (effects, _applied, _work) = result(&mut state, 10, 56);
        let effects = deliver(effects);
        assert!(
            matches!(effects.as_slice(), [Effect::MoveStream]),
            "the result frees the stream: {effects:?}"
        );
        assert!(matches!(
            answered(&mut held),
            Some(Err(Error::ProgramHostTerminated))
        ));
        let effects = deliver(apply(&mut state, Message::Detached, 57));
        assert!(
            matches!(effects.as_slice(), [Effect::Commit { chunk, .. }] if chunk.enforcement.is_none()),
            "the result's row commits, with no dispatch: {effects:?}"
        );
        let effects = apply(&mut state, Message::Committed, 58);
        assert!(
            matches!(effects.as_slice(), [Effect::Retire]),
            "{effects:?}"
        );
    }

    // At the drain's length a stream moves with its dispatch in flight,
    // and a batch not yet committed starts no dispatch. A request that its
    // mailbox held behind the move is held too, and the detach answers it.
    {
        let mut state = started(LIMITS);
        let mut answers = enforce_each(&mut state, 60..61);
        deliver(apply(&mut state, Message::Drained, 61));
        let (effects, mut unsent) = enforce_as(&mut state, 1, 62);
        assert!(deliver(effects).is_empty());
        let effects = apply(&mut state, Message::Drain(DrainStep::MoveNow), 63);
        assert!(
            matches!(
                effects.as_slice(),
                [Effect::MoveStream, Effect::Abandon(..)]
            ),
            "{effects:?}"
        );
        deliver(effects);
        assert!(matches!(
            answered(&mut unsent),
            Some(Err(Error::ProgramHostTerminated))
        ));
        assert!(answered(answers.first_mut().expect("in flight")).is_none());
        let (effects, mut behind) = enforce_as(&mut state, 2, 64);
        assert!(deliver(effects).is_empty());
        assert_eq!(state.waiting_for_a_slot(), 1);
        let effects = apply(&mut state, Message::Drain(DrainStep::MoveNow), 65);
        assert!(deliver(effects).is_empty(), "it moves once");
        deliver(apply(&mut state, Message::Detached, 66));
        assert!(matches!(
            answered(&mut behind),
            Some(Err(Error::ProgramHostTerminated))
        ));
    }
}

/// Catches: the drain's interruption of a local child's Run that records
/// Interrupted for a Run past its deadline, or that a `Started` still on its
/// way loses, starting the Program after the drain.
#[test]
fn the_drains_interruption_times_out_a_run_past_its_deadline_and_waits_for_its_start() {
    let limits = OwnerLimits {
        deadline_at_unix_nanos: 10,
        ..LIMITS
    };
    let deadline = limits.deadline_at_unix_nanos;
    for (at, kind) in [
        (
            deadline - 1,
            crate::lifecycle::RunTerminationKind::Interrupted,
        ),
        (deadline, crate::lifecycle::RunTerminationKind::TimedOut),
    ] {
        let mut state = started(limits);
        let effects = deliver(apply(&mut state, Message::Drain(DrainStep::Interrupt), at));
        assert_eq!(ended_as(&effects), Some(kind), "{at}: {effects:?}");
    }

    let mut state = OwnerState::for_run_stream(limits, (0, 0));
    let effects = apply(&mut state, Message::Drain(DrainStep::Interrupt), 2);
    assert!(deliver(effects).is_empty(), "it waits for the start");
    let effects = deliver(apply(
        &mut state,
        Message::Started(Ok(Some(StartedExecution {
            head: 1,
            deadline_at_unix_nanos: deadline,
        }))),
        3,
    ));
    assert!(
        !effects
            .iter()
            .any(|effect| matches!(effect, Effect::StartProgram { .. })),
        "the Program never starts: {effects:?}"
    );
    assert_eq!(
        ended_as(&effects),
        Some(crate::lifecycle::RunTerminationKind::Interrupted),
        "{effects:?}"
    );
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

fn approval_id() -> crate::approval::ApprovalId {
    crate::approval::ApprovalId::parse(&uuid::Uuid::new_v4().to_string()).expect("an id")
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
    (apply(state, Message::Parked(Box::new(parked)), now), woken)
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
            check != Check::Connections && batch.check() == check && batch.parks() == parks
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
    let fixture = TestModules::new(Arc::new(ImmediateEngine)).await;
    let tool = fixture
        .user_tool("user:apply-wakeups", "Parked waits")
        .await;
    let (run_id, _stream) = started_on_a_stream(&fixture.runs, &tool).await;
    let record = fixture.run_record(&tool.workspace_id, &run_id).await;
    let admitted = admitted_invocation(&fixture, &tool, &run_id).await;
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
            apply(&mut state, Message::Changed, 4).is_empty(),
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
        let effects = apply(&mut state, Message::Changed, 6);
        assert!(prechecks(&effects, Check::Connections, &[2]), "{effects:?}");
        assert!(apply(&mut state, Message::Changed, 7).is_empty());
        let effects = apply(
            &mut state,
            Message::Prechecked {
                check: Check::Connections,
                found: Ok(Vec::new()),
            },
            8,
        );
        assert!(effects.is_empty(), "the gap is not out: {effects:?}");
        assert!(apply(&mut state, Message::Changed, 9).is_empty());
        let effects = apply(&mut state, Message::Timer(Timer::WakeGap), 10);
        assert!(
            prechecks(&effects, Check::Connections, &[2]),
            "the rings coalesce into one read: {effects:?}"
        );
        let effects = apply(&mut state, Message::Timer(Timer::WakeGap), 11);
        assert!(effects.is_empty(), "nothing rang since: {effects:?}");
        let effects = deliver(apply(
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
        assert!(apply(&mut state, Message::Changed, 13).is_empty());
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
        let effects = apply(&mut state, Message::Changed, 4);
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
            apply(
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
        let effects = apply(&mut state, Message::Timer(Timer::WakeGap), 7);
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
        let effects = apply(&mut state, Message::Timer(Timer::Precheck), 5);
        assert!(prechecks(&effects, Check::Periodic, &[1, 2]), "{effects:?}");
        let effects = apply(
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
        let effects = apply(&mut state, Message::Timer(Timer::Precheck), 7);
        assert!(prechecks(&effects, Check::Periodic, &[1, 2]), "{effects:?}");
        let effects = deliver(apply(
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
        let effects = apply(&mut state, Message::Timer(Timer::Precheck), 9);
        assert!(prechecks(&effects, Check::Periodic, &[1]), "{effects:?}");
        // Another writer decides before the precheck answers: the moved
        // head reads the Approval Requests, which wakes the Invocation, and
        // the finding finds it gone.
        let moved = crate::run_repository::RunHead {
            head: state.head() + 1,
            recorded_total: 0,
            running: true,
            ended_as: None,
        };
        let effects = apply(&mut state, Message::Woken(moved), 10);
        assert!(prechecks(&effects, Check::Approvals, &[1]), "{effects:?}");
        let found = Message::Prechecked {
            check: Check::Approvals,
            found: Ok(vec![1]),
        };
        assert!(deliver(apply(&mut state, found, 11)).is_empty());
        approval_woken.try_recv().expect("woken");
        let effects = apply(
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
        let effects = apply(&mut state, Message::Timer(Timer::Precheck), 14);
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
        let effects = deliver(apply(
            &mut state,
            Message::Timer(Timer::Precheck),
            LIMITS.deadline_at_unix_nanos,
        ));
        assert_eq!(
            ended_as(&effects),
            Some(crate::lifecycle::RunTerminationKind::TimedOut),
            "{effects:?}"
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
        deliver(apply(&mut state, Message::Timer(Timer::RunDeadline), 4));
        assert_eq!(state.parked_invocations(), 0);
        let effects = apply(&mut state, Message::Timer(Timer::Precheck), 5);
        assert!(effects.is_empty(), "{effects:?}");
    }
}

/// Catches: a Run stream's deadline that rides an intent with a shutdown
/// grace, or that is written before the chunks applied ahead of it.
#[test]
fn a_run_streams_deadline_is_written_after_its_chunks() {
    use crate::lifecycle::RunTerminationKind;

    let mut state = started(LIMITS);
    // One chunk in flight, one applied behind it.
    let (effects, _first) = emit(&mut state, 8, 3);
    deliver(effects);
    committed_chunk(apply(&mut state, Message::Drained, 3));
    let (effects, _second) = emit(&mut state, 8, 4);
    deliver(effects);

    let past = LIMITS.deadline_at_unix_nanos + 1;
    let effects = deliver(apply(&mut state, Message::Timer(Timer::RunDeadline), past));
    assert!(
        matches!(effects.as_slice(), [Effect::TerminateProgram]),
        "the stop at once, no grace and no intent: {effects:?}"
    );
    let chunk = committed_chunk(apply(&mut state, Message::Committed, past));
    assert_eq!(chunk.events, 1, "the applied Emission commits");
    let effects = apply(&mut state, Message::Committed, past);
    assert!(
        matches!(
            effects.as_slice(),
            [Effect::EndStreamRun(end)]
                if end.termination.kind == RunTerminationKind::TimedOut
                    && end.termination.started_at_unix_nanos == past
        ),
        "{effects:?}"
    );
}

/// Catches: a Run stream's `Terminate` that names the loop's own decided
/// end once another writer ended the Run with another termination: the end
/// the Run records comes first.
#[test]
fn a_run_streams_terminate_names_the_end_its_run_records() {
    use crate::lifecycle::RunTerminationKind;

    let mut state = started(LIMITS);
    // The loop decides the deadline's end on its own.
    deliver(apply(&mut state, Message::Timer(Timer::RunDeadline), 3));
    assert_eq!(state.terminated_as(), None);

    // A cancellation ended the Run first.
    deliver(apply(
        &mut state,
        Message::EndedOutside(Some(RunTerminationKind::Canceled)),
        4,
    ));

    assert_eq!(state.terminated_as(), Some(RunTerminationKind::Canceled));
}
