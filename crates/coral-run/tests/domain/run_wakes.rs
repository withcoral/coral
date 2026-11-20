//! Outside requests for a Run a Run Executor holds (ADR 0027): the Coral
//! Instance that receives a decision or a cancellation commits it under the
//! Run's claim and head and raises the Run's wake row, and the Run's stream
//! loop, wherever its stream is, acts on it. On Postgres the request goes to
//! another Coral Instance; on SQLite, which has one, to the same.
use super::*;
use coral_run_executor::wire::v1::{RunEnd as WireEnd, TerminateReason, run_response};

use super::run_stream_reconnect::{answers, failed_with, finish_with, forecast, only_answer};
use super::run_streams::{Scripted, emit, ended, take};
use crate::approval::{ApprovalDecision, ApprovalState};
use crate::observation::Turn;
use crate::run_repository;

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
    /// `upstream`, with `observer` on the stream's module. Only a wake row
    /// tells the loop of an outside decision: the periodic precheck stays
    /// out, and an Approval Request's deadline comes before the Run's.
    async fn new(
        upstream: Arc<dyn UpstreamSender>,
        observer: Arc<dyn ExecutionObserver>,
        approval: bool,
    ) -> Self {
        let mut fixture = TestModules::with_execution_adapters(
            Arc::new(ImmediateEngine::default()),
            upstream,
            Arc::new(NoopInvocationObserver),
            observer,
        )
        .await;
        fixture.runs.execution_policy =
            crate::execution::ExecutionPolicy::testing_with_approval_wait(Duration::from_secs(30))
                .with_wakeups(Duration::from_secs(3600), Duration::from_millis(50));
        let tool = fixture.user_tool("user:wakes", "Wake rows").await;
        if approval {
            fixture.require_approval(&tool).await;
        }
        let runs = fixture
            .runs
            .clone()
            .with_fan_out_limit(crate::FanOutLimit::new(1).expect("a limit"))
            .with_run_streams();
        let other = if fixture.database.system() == coral_db::DatabaseSystem::Postgres {
            fixture.next_process().with_run_streams()
        } else {
            runs.clone()
        };
        let accepted = runs
            .start(&tool.principal, &tool.workspace_id, submission("return 1;"))
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

    /// Reads to the answer to request `number`, well before its Approval
    /// Request's own timer (30 s) would answer it.
    async fn answered_before_its_timer(&mut self, number: u64) -> Vec<run_response::Message> {
        tokio::time::timeout(
            Duration::from_secs(10),
            self.stream.until(|read| !answers(read, number).is_empty()),
        )
        .await
        .expect("answered before the request's timer")
    }

    async fn wake_rows(&self) -> usize {
        self.fixture
            .wake_rows(&self.tool.workspace_id, &self.run_id)
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
/// the periodic precheck; and a wake row left after the Run ended.
#[tokio::test(flavor = "multi_thread")]
async fn a_decision_received_elsewhere_reaches_the_program_through_the_streams_loop() {
    for decision in [ApprovalDecision::Approve, ApprovalDecision::Decline] {
        let mut held = Held::new(answering(), Arc::new(NoopExecutionObserver), true).await;
        held.stream.request(1, forecast(None));
        let request = held.approval_request().await;

        let decided = held.decide(&held.other, &request.id, decision).await;

        let answer = match decision {
            ApprovalDecision::Approve => {
                assert_eq!(decided.state, ApprovalState::Approved);
                "result"
            }
            ApprovalDecision::Decline => {
                assert_eq!(decided.state, ApprovalState::Declined);
                "approval_declined"
            }
        };
        let read = held.answered_before_its_timer(1).await;
        let answered = only_answer(&read, 1);
        if answer == "result" {
            assert!(dispatched(answered), "{read:?}");
        } else {
            assert!(failed_with(answered, answer), "{read:?}");
        }
        assert_eq!(
            held.fixture
                .inbox_rows(&held.tool.workspace_id, &held.run_id)
                .await,
            0
        );
        held.stream.request(2, finish_with("1"));
        held.stream
            .until(|read| read.contains(&ended(WireEnd::Succeeded)))
            .await;
        wait_until_async(async || held.wake_rows().await == 0).await;
    }
}

/// Catches: a decision received on the Coral Instance whose loop holds the
/// stream that reaches the loop only after the change signal's read gap.
#[tokio::test(flavor = "multi_thread")]
async fn a_decision_on_the_instance_with_the_stream_wakes_its_reader_at_once() {
    let probe = WakeProbe::new();
    let mut held = Held::new(answering(), probe.clone(), true).await;
    held.stream.request(1, forecast(None));
    let request = held.approval_request().await;

    held.decide(&held.runs, &request.id, ApprovalDecision::Approve)
        .await;

    let read = held.answered_before_its_timer(1).await;
    assert!(dispatched(only_answer(&read, 1)), "{read:?}");
    assert_eq!(probe.turns().first(), Some(&Turn::Woken));
}

/// Catches: a decision that lands while the loop's chunk is being written,
/// whose waiter learns of it only from a wake row the reader has not read.
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

    let read = held.answered_before_its_timer(1).await;
    assert!(dispatched(only_answer(&read, 1)), "{read:?}");
    assert!(probe.turns().is_empty(), "the reader handed nothing");
}

/// Catches: a cancellation of a Run a Run Executor holds that waits for a
/// Run Owner, leaves an Invocation open, or never reaches the Program; and
/// one past the Run's deadline that ends it anything but timed out.
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

        let (state, reason) = if past_deadline {
            (RunState::TimedOut, TerminateReason::TimedOut)
        } else {
            (RunState::Canceled, TerminateReason::Canceled)
        };
        assert_eq!(canceled.state(), state);
        assert_eq!(held.terminated().await, reason);
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
        assert_eq!(cancellations, usize::from(!past_deadline));
        assert_eq!(
            held.fixture
                .inbox_rows(&held.tool.workspace_id, &held.run_id)
                .await,
            0
        );
        wait_until_async(async || held.wake_rows().await == 0).await;
    }
}

/// Catches: a cancellation of a Run a Run Executor has not started that
/// waits for a loop no Run will get: a waiting Run, and a taken one.
#[tokio::test]
async fn a_cancellation_ends_a_run_executors_run_that_has_not_started_at_once() {
    for taken in [false, true] {
        let fixture = TestModules::new(Arc::new(ImmediateEngine::default())).await;
        let tool = fixture.user_tool("user:unstarted", "Unstarted").await;
        let runs = fixture.runs.clone().with_run_streams();
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
        assert_eq!(fixture.wake_rows(&tool.workspace_id, &run_id).await, 0);
        if let Some(claim) = claim {
            // The end released the take's claim: no stream opens on it.
            let mut late = Scripted::open(&runs, &run_id.to_string(), &claim);
            assert_eq!(late.next().await, ended(WireEnd::NotHeld));
        } else {
            let mut transaction = runs.database.begin().await.expect("begin a take");
            let claimed = run_repository::take_oldest(
                &mut transaction,
                1,
                &run_repository::Taker {
                    run_executor_id: "6f0c2a59-55a7-4e0b-9a2e-7c1d4f3b8e21",
                    run_executor_kind: coral_run_executor::Kind::Dialed,
                },
                1,
            )
            .await
            .expect("a take");
            assert!(claimed.is_empty(), "the canceled Run is not taken");
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
        wait_until_async(async || held.wake_rows().await == 0).await;
    }
}
