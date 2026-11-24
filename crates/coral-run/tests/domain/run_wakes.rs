//! Outside requests for a Run a Run Executor holds: the Coral
//! Instance that receives a decision or a cancellation commits it under the
//! Run's claim and head, and the Run's stream loop, wherever its stream is,
//! sees the head move and acts on it. On Postgres the request goes to
//! another Coral Instance; on SQLite, which has one, to the same.
use super::*;
use coral_run_executor::wire::v1::{RunEnd as WireEnd, TerminateReason, run_response};

use super::run_stream_reconnect::{answers, failed_with, finish_with, forecast, only_answer};
use super::run_streams::{Scripted, emit, ended, take, take_any};
use crate::approval::{ApprovalDecision, ApprovalState};
use crate::observation::Turn;

/// A Run started on a scripted stream from `runs`, and `other`, the module
/// an outside request goes to.
struct Held {
    fixture: TestModules,
    runs: RunModule,
    other: RunModule,
    tool: ToolFixture,
    run_id: crate::RunId,
    stream: Scripted,
}

impl Held {
    /// A Run whose weather Tool waits for approval, answering through
    /// `upstream`, with `observer` on the stream's module. Only the wake-up
    /// reader tells the loop of an outside decision: the periodic precheck stays
    /// out, and no real timer fires before a test's wait for an answer runs
    /// out (an Approval Request's deadline is 120 s away, the Run's 150 s);
    /// tests move the test clock instead.
    async fn new(
        upstream: Arc<dyn UpstreamSender>,
        observer: Arc<dyn ExecutionObserver>,
        approval: bool,
    ) -> Self {
        let mut fixture = TestModules::with_execution_adapters(
            Arc::new(ImmediateEngine),
            upstream,
            Arc::new(NoopInvocationObserver),
            observer,
        )
        .await;
        fixture.runs.execution_policy =
            crate::execution::ExecutionPolicy::testing_with_approval_wait(Duration::from_secs(120))
                .with_wakeups(Duration::from_secs(3600), Duration::from_millis(50));
        let tool = fixture.user_tool("user:wakes", "Wake rows").await;
        if approval {
            fixture.require_approval(&tool).await;
        }
        let runs = fixture
            .runs
            .clone()
            .with_fan_out_limit(crate::FanOutLimit::new(1).expect("a limit"));
        let other = if fixture.database.system() == coral_db::DatabaseSystem::Postgres {
            fixture.next_process()
        } else {
            runs.clone()
        };
        let accepted = runs
            .start(
                &tool.principal,
                &tool.workspace_id,
                RunSubmission::new(
                    ProgramSource::parse("return 1;").expect("Program source"),
                    RunTimeout::from_millis(150_000).expect("Run timeout"),
                ),
            )
            .await
            .expect("accept a Run");
        let run_id = accepted.run_id().clone();
        let claim = take(&runs).await;
        let mut stream = Scripted::open(&runs, &run_id.to_string(), &claim);
        stream.start().await;
        Self {
            fixture,
            runs,
            other,
            tool,
            run_id,
            stream,
        }
    }

    /// The Approval Request of the call the stream made, once there is one.
    async fn approval_request(&self) -> crate::approval::ApprovalRequest {
        wait_for_approval(
            &self.fixture.approvals(),
            &self.tool.principal,
            &self.tool.workspace_id,
            &self.run_id,
        )
        .await
    }

    async fn decide(
        &self,
        runs: &RunModule,
        id: &crate::approval::ApprovalId,
        decision: ApprovalDecision,
    ) -> crate::approval::ApprovalRequest {
        runs.approvals()
            .decide(
                &self.tool.principal,
                &self.tool.workspace_id,
                id,
                decision,
                None,
                &TraceId::parse(&"e".repeat(32)).expect("a trace id"),
            )
            .await
            .expect("the decision is recorded at once")
    }

    /// Reads to the answer to request `number`. No real timer answers it
    /// during a test: only a wake does.
    async fn answered(&mut self, number: u64) -> Vec<run_response::Message> {
        self.stream
            .until(|read| !answers(read, number).is_empty())
            .await
    }

    /// Reads the stream to its `Terminate`, with the Run written before it.
    async fn terminated(&mut self) -> TerminateReason {
        loop {
            match self.stream.next().await {
                run_response::Message::Terminate(terminate) => return terminate.reason(),
                run_response::Message::Ended(end) => panic!("Ended before Terminate: {end:?}"),
                _ => {}
            }
        }
    }
}

/// Whether the approved call went upstream: an expiry answers too.
fn dispatched(answer: &coral_run_executor::wire::v1::InvocationResult) -> bool {
    matches!(
        answer.outcome,
        Some(coral_run_executor::wire::v1::invocation_result::Outcome::ResultJson(_))
    )
}

fn answering() -> Arc<SequencedUpstream> {
    let upstream = SequencedUpstream::new(HttpOutcome::new(
        200,
        serde_json::json!({"temperature": 18}),
    ));
    upstream.release_all();
    upstream
}

/// Catches: a decision on a Run a Run Executor holds that waits for a Run
/// Owner, answers "not yet recorded", or reaches the Program only through
/// the periodic precheck; a receiver that judges a late approval by its own
/// clock.
#[tokio::test(flavor = "multi_thread")]
async fn a_decision_received_elsewhere_reaches_the_program_through_the_streams_loop() {
    use ApprovalDecision::{Approve, Decline};
    // The decision, whether it comes past the request's deadline, the state
    // it is recorded in, and the answer the Program gets ("result": the
    // call went upstream).
    let cases = [
        (Approve, false, ApprovalState::Approved, "result"),
        (Decline, false, ApprovalState::Declined, "approval_declined"),
        // Recorded as made; the enforcement point refuses it.
        (Approve, true, ApprovalState::Approved, "approval_expired"),
    ];
    for (decision, late, recorded, answer) in cases {
        let mut held = Held::new(answering(), Arc::new(NoopExecutionObserver), true).await;
        held.stream.request(1, forecast(None));
        let request = held.approval_request().await;
        if late {
            held.fixture
                .clock
                .next
                .store(request.deadline_at_unix_nanos + 1, Ordering::SeqCst);
        }

        let decided = held.decide(&held.other, &request.id, decision).await;

        assert_eq!(decided.state, recorded);
        let read = held.answered(1).await;
        let answered = only_answer(&read, 1);
        if answer == "result" {
            assert!(dispatched(answered), "{read:?}");
        } else {
            assert!(failed_with(answered, answer), "{read:?}");
        }
        held.stream.request(2, finish_with("1"));
        held.stream
            .until(|read| read.contains(&ended(WireEnd::Succeeded)))
            .await;
    }
}

/// Catches: a decision received on the Coral Instance whose loop holds the
/// stream that waits for the reader's backstop. The local wake and a ring
/// race for the read; either reads at once.
#[tokio::test(flavor = "multi_thread")]
async fn a_decision_on_the_instance_with_the_stream_wakes_its_reader_at_once() {
    let probe = WakeProbe::new();
    let mut held = Held::new(answering(), probe.clone(), true).await;
    held.stream.request(1, forecast(None));
    let request = held.approval_request().await;

    held.decide(&held.runs, &request.id, ApprovalDecision::Approve)
        .await;

    let read = held.answered(1).await;
    assert!(dispatched(only_answer(&read, 1)), "{read:?}");
    assert!(
        matches!(probe.turns().first(), Some(Turn::Woken | Turn::Rang)),
        "{:?}",
        probe.turns()
    );
}

/// Catches: a decision that lands while the loop's chunk is being written,
/// whose waiter learns of it only from a wake-up the reader has not sent.
#[tokio::test(flavor = "multi_thread")]
async fn a_decision_under_the_loops_chunk_reaches_its_waiter_through_the_rebuild() {
    let probe = WakeProbe::new();
    let mut held = Held::new(answering(), probe.clone(), true).await;
    held.stream.request(1, forecast(None));
    let request = held.approval_request().await;
    probe.hold_reads();
    let release = probe.hold_next_commit();
    held.stream
        .request(2, emit(&serde_json::json!({"step": 2})));
    wait_until(|| probe.commit_held()).await;

    held.decide(&held.other, &request.id, ApprovalDecision::Approve)
        .await;
    release.send(()).expect("release the chunk");

    let read = held.answered(1).await;
    assert!(dispatched(only_answer(&read, 1)), "{read:?}");
    assert!(probe.turns().is_empty(), "the reader handed nothing");
}

/// Catches: a cancellation of a Run a Run Executor holds that waits for a
/// Run Owner, leaves an Invocation open, or never reaches the Program; and
/// a receiver that judges one past the Run's deadline by its own clock: it
/// ends Canceled, as the first write under the head decides.
#[tokio::test(flavor = "multi_thread")]
async fn a_cancellation_received_elsewhere_ends_the_run_with_its_invocations() {
    for past_deadline in [false, true] {
        let upstream = Arc::new(BlockingUpstream::new(HttpOutcome::new(
            200,
            serde_json::json!({}),
        )));
        let mut held = Held::new(upstream.clone(), Arc::new(NoopExecutionObserver), false).await;
        // One Invocation dispatches and holds the one slot; the next waits.
        held.stream.request(1, forecast(None));
        wait_until(|| upstream.entered.load(Ordering::SeqCst)).await;
        held.stream.request(2, forecast(None));
        wait_until_async(async || {
            held.fixture
                .invocation_rows(&held.tool.workspace_id, &held.run_id)
                .await
                .len()
                == 2
        })
        .await;
        if past_deadline {
            let run = held
                .fixture
                .run_record(&held.tool.workspace_id, &held.run_id)
                .await;
            let deadline = run
                .deadline_at_unix_nanos
                .expect("a started Run's deadline");
            held.fixture.clock.next.store(deadline, Ordering::SeqCst);
        }

        let canceled = held
            .other
            .cancel(
                &held.tool.principal,
                &held.tool.workspace_id,
                &held.run_id,
                &immediate(None, 100),
            )
            .await
            .expect("the cancellation is recorded at once");

        // The receiver compares no clock: past the deadline too, the
        // cancellation that lands first decides.
        assert_eq!(canceled.state(), RunState::Canceled);
        assert_eq!(held.terminated().await, TerminateReason::Canceled);
        let rows = held
            .fixture
            .invocation_rows(&held.tool.workspace_id, &held.run_id)
            .await;
        assert_eq!(
            rows.iter()
                .map(|row| (row.result_kind, row.failure_code.as_deref()))
                .collect::<Vec<_>>(),
            [
                (Some(crate::InvocationResultKind::Indeterminate), None),
                (Some(crate::InvocationResultKind::RunTerminated), None),
            ],
            "{rows:?}"
        );
        let cancellations = retained_events(
            &held.fixture.runs,
            &held.tool.principal,
            &held.tool.workspace_id,
            &held.run_id,
        )
        .await
        .iter()
        .filter(|event| matches!(event.kind(), RunEventKind::CancellationRequested { .. }))
        .count();
        assert_eq!(cancellations, 1);
    }
}

/// Catches: a loop whose Run another writer ended, and whose stream then
/// closes without the Program's `Finish` (its Run Executor is gone), staying
/// registered on its Coral Instance until the process exits.
#[tokio::test]
async fn a_loop_whose_run_ended_elsewhere_retires_when_its_stream_closes() {
    let upstream = Arc::new(BlockingUpstream::new(HttpOutcome::new(
        200,
        serde_json::json!({}),
    )));
    let mut held = Held::new(upstream.clone(), Arc::new(NoopExecutionObserver), false).await;
    held.stream.request(1, forecast(None));
    wait_until(|| upstream.entered.load(Ordering::SeqCst)).await;
    held.other
        .cancel(
            &held.tool.principal,
            &held.tool.workspace_id,
            &held.run_id,
            &immediate(None, 100),
        )
        .await
        .expect("the cancellation is recorded at once");
    assert_eq!(held.terminated().await, TerminateReason::Canceled);
    assert!(held.runs.has_owner(&held.run_id));

    let Held {
        runs,
        run_id,
        stream,
        ..
    } = held;
    drop(stream);

    wait_until(|| !runs.has_owner(&run_id)).await;
}

/// Catches: a cancellation that writes a Run an older release's Run Owner
/// holds, which only that owner writes: the receiver answers that it is not
/// recorded, and the Run is unchanged.
#[tokio::test]
async fn a_cancellation_of_an_older_releases_owned_run_writes_nothing() {
    let fixture = TestModules::new(Arc::new(ImmediateEngine)).await;
    let tool = fixture.user_tool("user:legacy-owner", "Legacy owner").await;
    let run_id = fixture
        .staged_run(&tool.principal, &tool.workspace_id, "return 1;")
        .await;
    fixture
        .inject_fault(&format!(
            "UPDATE program_run_records SET worker_claim_id = 'owner-claim', \
             worker_claim_expires_at_unix_nanos = accepted_at_unix_nanos WHERE id = '{run_id}'"
        ))
        .await;
    let before = fixture.run_record(&tool.workspace_id, &run_id).await;
    let events_before = fixture.events_after(&tool.workspace_id, &run_id, 0).await;

    let refused = fixture
        .runs
        .cancel(
            &tool.principal,
            &tool.workspace_id,
            &run_id,
            &immediate(None, 100),
        )
        .await;

    assert!(
        matches!(refused, Err(Error::RunRequestNotYetRecorded)),
        "{refused:?}"
    );
    assert_eq!(
        fixture.run_record(&tool.workspace_id, &run_id).await,
        before
    );
    assert_eq!(
        fixture.events_after(&tool.workspace_id, &run_id, 0).await,
        events_before
    );
}

/// Catches: a cancellation of a Run a Run Executor has not started that
/// waits for a loop no Run will get: a waiting Run, and a taken one.
#[tokio::test]
async fn a_cancellation_ends_a_run_executors_run_that_has_not_started_at_once() {
    for taken in [false, true] {
        let fixture = TestModules::new(Arc::new(ImmediateEngine)).await;
        let tool = fixture.user_tool("user:unstarted", "Unstarted").await;
        let runs = fixture.runs.clone();
        let accepted = runs
            .start(&tool.principal, &tool.workspace_id, submission("return 1;"))
            .await
            .expect("accept a Run");
        let run_id = accepted.run_id().clone();
        let claim = if taken { Some(take(&runs).await) } else { None };

        let canceled = runs
            .cancel(
                &tool.principal,
                &tool.workspace_id,
                &run_id,
                &immediate(None, 100),
            )
            .await
            .expect("the cancellation is recorded at once");

        assert_eq!(canceled.state(), RunState::Canceled);
        let run = fixture.run_record(&tool.workspace_id, &run_id).await;
        assert_eq!(run.execution_claim_id, None);
        if let Some(claim) = claim {
            // The end released the take's claim: no stream opens on it.
            let mut late = Scripted::open(&runs, &run_id.to_string(), &claim);
            assert_eq!(late.next().await, ended(WireEnd::NotHeld));
        } else {
            assert_eq!(take_any(&runs).await, None, "the canceled Run is not taken");
        }
    }
}

/// Catches: a sweeper write on a running stream Run that its loop learns of
/// only from its own next commit or the periodic precheck.
#[tokio::test(flavor = "multi_thread")]
async fn the_sweepers_writes_reach_the_streams_loop() {
    for run_ends in [false, true] {
        let mut held = Held::new(answering(), Arc::new(NoopExecutionObserver), true).await;
        held.stream.request(1, forecast(None));
        let request = held.approval_request().await;
        let run = held
            .fixture
            .run_record(&held.tool.workspace_id, &held.run_id)
            .await;
        let past = if run_ends {
            run.deadline_at_unix_nanos
                .expect("a started Run's deadline")
        } else {
            request.deadline_at_unix_nanos
        };
        held.fixture.clock.next.store(past, Ordering::SeqCst);

        let swept = held
            .other
            .sweep_runs()
            .await
            .expect("a sweep")
            .expect("the sweeper is elected");

        if run_ends {
            assert_eq!(swept.ended, 1);
            assert_eq!(held.terminated().await, TerminateReason::TimedOut);
        } else {
            assert_eq!(swept.expired, 1);
            let read = held.stream.until(|read| !answers(read, 1).is_empty()).await;
            assert!(
                failed_with(only_answer(&read, 1), "approval_expired"),
                "{read:?}"
            );
            held.stream.request(2, finish_with("1"));
            held.stream
                .until(|read| read.contains(&ended(WireEnd::Succeeded)))
                .await;
        }
    }
}

/// Catches: a Program that runs on while its loop's chunk is in flight after
/// another writer ended the Run, and a second `Terminate` once that chunk
/// finds the end.
#[tokio::test(flavor = "multi_thread")]
async fn a_run_ended_under_the_loops_chunk_terminates_its_program_once() {
    let probe = WakeProbe::new();
    let mut held = Held::new(answering(), probe.clone(), false).await;
    let release = probe.hold_next_commit();
    held.stream
        .request(1, emit(&serde_json::json!({"step": 1})));
    wait_until(|| probe.commit_held()).await;

    let canceled = held
        .other
        .cancel(
            &held.tool.principal,
            &held.tool.workspace_id,
            &held.run_id,
            &immediate(None, 100),
        )
        .await
        .expect("the cancellation is recorded at once");

    assert_eq!(canceled.state(), RunState::Canceled);
    // The chunk is still held: the wake stopped the Program.
    assert_eq!(held.terminated().await, TerminateReason::Canceled);
    release.send(()).expect("release the chunk");
    held.stream.request(2, finish_with("1"));
    let rest = held
        .stream
        .until(|read| read.contains(&ended(WireEnd::Canceled)))
        .await;
    assert!(
        !rest
            .iter()
            .any(|message| matches!(message, run_response::Message::Terminate(_))),
        "one Terminate: {rest:?}"
    );
}

/// Catches: a wake-up read that sends a statement for each Run stream's
/// loop on its Coral Instance.
#[tokio::test]
async fn a_wake_up_read_of_many_loops_sends_the_statements_of_one() {
    let fixture = TestModules::new(Arc::new(ImmediateEngine)).await;
    // A peer handle counts only its own statements.
    let reads = fixture.test_database.connect_peer().await.unwrap();
    let workspace = coral_workspace::WorkspaceId::parse(&uuid::Uuid::new_v4().to_string())
        .expect("a Workspace id");
    let runs = |count: usize| {
        (0..count)
            .map(|_| {
                (
                    workspace.clone(),
                    crate::RunId::generate().expect("a Run id"),
                )
            })
            .collect::<Vec<_>>()
    };
    let mut sent = Vec::new();
    for count in [1, 150] {
        let before = reads.statements_sent();
        crate::run_repository::heads(&reads, &runs(count))
            .await
            .expect("read the heads");
        sent.push(reads.statements_sent() - before);
    }
    assert!(
        matches!(sent.as_slice(), [one, many] if one == many),
        "{sent:?}"
    );
}
