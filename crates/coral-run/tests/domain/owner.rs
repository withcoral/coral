//! The Run Owner (ADR 0018, slice 2): Program requests answer at Applied, the
//! Run Ledger lands in chunks, the lease rides every commit, and a claim
//! mismatch stops the owner. The last test drives `apply` without a database.
use super::*;

use crate::owner::{
    CHUNK_WORK_CAP, ChunkWork, CommitRejection, Effect, Message, OwnerLimits, OwnerState, Phase,
    apply,
};
use crate::termination::ShutdownAction;
use crate::{AppliedOutputCap, CHUNK_BYTES_CAP, CHUNK_EVENTS_CAP, CoralFailure};
use coral_db::Transaction;

/// Waits for `go`, emits `values` in one burst recording each acknowledgement,
/// then holds the Program until `release`, so the test can look at the ledger
/// while the Program is still running.
struct BurstEngine {
    values: Vec<String>,
    acknowledged: AtomicUsize,
    rejected: AtomicUsize,
    go: Mutex<Option<mpsc::Receiver<()>>>,
    release: Mutex<Option<mpsc::Receiver<()>>>,
}

struct Gates {
    go: mpsc::Sender<()>,
    release: mpsc::Sender<()>,
}

impl BurstEngine {
    fn new(values: Vec<String>) -> (Arc<Self>, Gates) {
        let (go, go_receiver) = mpsc::channel();
        let (release, release_receiver) = mpsc::channel();
        (
            Arc::new(Self {
                values,
                acknowledged: AtomicUsize::new(0),
                rejected: AtomicUsize::new(0),
                go: Mutex::new(Some(go_receiver)),
                release: Mutex::new(Some(release_receiver)),
            }),
            Gates { go, release },
        )
    }

    fn wait(gate: &Mutex<Option<mpsc::Receiver<()>>>) -> bool {
        let receiver = gate.lock().expect("gate lock").take();
        receiver.is_none_or(|receiver| receiver.recv().is_ok())
    }
}

impl ProgramEngine for BurstEngine {
    fn compile(
        &self,
        _deadline: std::time::Instant,
        source: &str,
    ) -> Result<ProgramPackage, String> {
        Ok(ProgramPackage::scripted(source))
    }

    fn execute(
        &self,
        _package: ProgramPackage,
        host: Arc<dyn coral_v8::ProgramHost>,
        stop: Arc<ExecutionStop>,
    ) -> ExecutionCompletion {
        if !Self::wait(&self.go) {
            return ExecutionCompletion::ProgramHostTerminated;
        }
        for value in &self.values {
            match host.emit(value) {
                Ok(()) => self.acknowledged.fetch_add(1, Ordering::SeqCst),
                Err(_rejected) => self.rejected.fetch_add(1, Ordering::SeqCst),
            };
        }
        if !Self::wait(&self.release) || stop.is_requested() {
            return ExecutionCompletion::ProgramHostTerminated;
        }
        ExecutionCompletion::Succeeded(FinalValue::Absent)
    }
}

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

async fn wait_until_started(
    fixture: &TestModules,
    workspace_id: &coral_workspace::WorkspaceId,
    run_id: &crate::RunId,
) {
    wait_until_async(async || {
        fixture
            .run_record(workspace_id, run_id)
            .await
            .execution_started()
    })
    .await;
}

/// How a [`ProbeWork`] was settled, once it was.
type Settled = Arc<Mutex<Option<Result<(), Error>>>>;

/// Row work that only records how it was settled.
#[derive(Debug)]
struct ProbeWork {
    settled: Settled,
}

#[async_trait]
impl ChunkWork for ProbeWork {
    async fn run(
        &mut self,
        _transaction: &mut Transaction<'_>,
        _run: &crate::lifecycle::RunRecord,
        _now_unix_nanos: i64,
    ) -> Result<Vec<crate::run_repository::OwnedEvent>, Error> {
        Ok(Vec::new())
    }

    fn settle(self: Box<Self>, recorded: Result<(), Error>) {
        *self.settled.lock().expect("settle lock") = Some(recorded);
    }
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
    let settled = Arc::new(Mutex::new(None));
    owner
        .work(Box::new(ProbeWork {
            settled: settled.clone(),
        }))
        .expect("the owner takes the work");
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

async fn wait_until_recorded(
    fixture: &TestModules,
    workspace_id: &coral_workspace::WorkspaceId,
    run_id: &crate::RunId,
    sequence: u64,
) {
    wait_until_async(async || {
        last_sequence(&fixture.run_record(workspace_id, run_id).await) >= sequence
    })
    .await;
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
    wait_until_async(async || {
        fixture
            .run_record(&workspace_id, &run_id)
            .await
            .execution_started()
    })
    .await;
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
    let mut state = OwnerState::new(limits);
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

const LIMITS: OwnerLimits = OwnerLimits {
    run_output_bytes: 64 << 20,
    applied_cap_bytes: AppliedOutputCap::DEFAULT_BYTES,
    deadline_at_unix_nanos: 1_000_000,
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
            Some(Err(Error::InvocationRejected(
                CoralFailure::RunOutputTooLarge
            )))
        ));
        assert!(matches!(
            second_work.lock().expect("settle").as_ref(),
            Some(Err(Error::InvocationRejected(
                CoralFailure::RunOutputTooLarge
            )))
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
}

fn probe_work() -> (Box<dyn ChunkWork>, Settled) {
    let settled = Arc::new(Mutex::new(None));
    (
        Box::new(ProbeWork {
            settled: settled.clone(),
        }),
        settled,
    )
}
