//! Coral's side of a Run stream: `Open` is answered by the Run row,
//! `Loaded` rechecks authority and starts the Run, and the Program's
//! requests commit in number order under the claim and the head.
use super::*;
use crate::approval::{ApprovalDecision, ApprovalState};
use coral_run_executor::wire::v1::{
    Emit, Finish, FinishEnd, Loaded, Open, Request as WireRequest, RunEnd as WireEnd, RunRequest,
    RunResponse, TerminateReason, finish, request, run_request, run_response,
};
use coral_run_executor::{Closing, Kind, PROTOCOL_LEVEL, ServedRunStream};
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;

use super::agents::{AgentLoss, agent_scope};
use crate::run_repository::{self, Chunk, OwnedEvent, OwnedEventKind, Taker};

/// One Run stream served by `runs`, driven as a scripted Run Executor.
pub(super) struct Scripted {
    requests: mpsc::UnboundedSender<RunRequest>,
    responses: mpsc::UnboundedReceiver<Result<RunResponse, Closing>>,
}

impl Scripted {
    pub(super) fn open(runs: &RunModule, run_id: &str, claim_id: &str) -> Self {
        Self::open_with(
            runs,
            Open {
                run_id: run_id.to_owned(),
                claim_id: claim_id.to_owned(),
                protocol_level: PROTOCOL_LEVEL,
                awaited: Vec::new(),
            },
        )
    }

    /// Opens the stream with an `Open` the test builds, such as a reopen's,
    /// which names the requests it waits on.
    pub(super) fn open_with(runs: &RunModule, open: Open) -> Self {
        let (requests, received) = mpsc::unbounded_channel();
        let (responses, sent) = mpsc::unbounded_channel();
        let server = runs.run_stream_server();
        tokio::spawn(async move {
            server
                .serve(ServedRunStream {
                    requests: Box::pin(UnboundedReceiverStream::new(received)),
                    responses,
                })
                .await;
        });
        let stream = Self {
            requests,
            responses: sent,
        };
        stream.send(run_request::Message::Open(open));
        stream
    }

    pub(super) fn send(&self, message: run_request::Message) {
        self.requests
            .send(RunRequest {
                message: Some(message),
            })
            .expect("the stream is served");
    }

    pub(super) fn request(&self, number: u64, kind: request::Kind) {
        self.send(run_request::Message::Request(WireRequest {
            number,
            kind: Some(kind),
        }));
    }

    pub(super) async fn recv(&mut self) -> Result<RunResponse, Closing> {
        tokio::time::timeout(EVENTUALLY, self.responses.recv())
            .await
            .expect("Coral answers in time")
            .expect("the stream is open")
    }

    pub(super) async fn next(&mut self) -> run_response::Message {
        self.recv()
            .await
            .expect("a message, not a close")
            .message
            .expect("a message")
    }

    /// How Coral closed the stream without `Ended`.
    pub(super) async fn closing(&mut self) -> Closing {
        self.recv().await.expect_err("a close, not a message")
    }

    /// Opens, loads and expects `Started`.
    /// Reads the package, sends `Loaded` and returns the `Started` that
    /// answers it.
    pub(super) async fn start(&mut self) -> coral_run_executor::wire::v1::Started {
        assert!(matches!(
            self.next().await,
            run_response::Message::Package(_)
        ));
        self.send(run_request::Message::Loaded(Loaded {}));
        match self.next().await {
            run_response::Message::Started(started) => started,
            other => panic!("expected Started, got {other:?}"),
        }
    }
}

/// Answers the stream's `Terminate` as the Run Executor does, once its
/// Program has ended.
pub(super) async fn finish_terminated(stream: &mut Scripted, number: u64) {
    stream
        .until(|read| matches!(read.last(), Some(run_response::Message::Terminate(_))))
        .await;
    stream.request(
        number,
        request::Kind::Finish(Finish {
            completion: Some(finish::Completion::End(FinishEnd::HostTerminated.into())),
        }),
    );
}

pub(super) fn emit(value: &serde_json::Value) -> request::Kind {
    request::Kind::Emit(Emit {
        value_json: value.to_string(),
        location: None,
    })
}

pub(super) fn ended(end: WireEnd) -> run_response::Message {
    run_response::Message::Ended(coral_run_executor::wire::v1::Ended { end: end.into() })
}

pub(super) fn committed(number: u64) -> run_response::Message {
    run_response::Message::Committed(coral_run_executor::wire::v1::Committed {
        request_number: number,
    })
}

/// Takes the oldest waiting Run as a Run Executor would, and returns its claim.
pub(super) async fn take(runs: &RunModule) -> String {
    take_any(runs).await.expect("a waiting Run")
}

/// Takes the oldest waiting Run for `taker` at `taken_at_unix_nanos`, and
/// returns its claim.
pub(super) async fn take_for(
    runs: &RunModule,
    taker: &Taker<'_>,
    taken_at_unix_nanos: i64,
) -> String {
    try_take_for(runs, taker, taken_at_unix_nanos)
        .await
        .expect("a waiting Run")
}

/// [`take`], or `None` when no Run waits.
pub(super) async fn take_any(runs: &RunModule) -> Option<String> {
    try_take_for(
        runs,
        &Taker {
            run_executor_id: "6f0c2a59-55a7-4e0b-9a2e-7c1d4f3b8e21",
            run_executor_kind: Kind::Dialed,
        },
        1,
    )
    .await
}

async fn try_take_for(
    runs: &RunModule,
    taker: &Taker<'_>,
    taken_at_unix_nanos: i64,
) -> Option<String> {
    let mut transaction = runs.database.begin().await.expect("begin the take");
    let claimed = run_repository::take_oldest(&mut transaction, 1, taker, taken_at_unix_nanos)
        .await
        .expect("take a waiting Run");
    transaction.commit().await.expect("commit the take");
    claimed.into_iter().next().map(|claimed| claimed.claim_id)
}

/// What a Run's Events say, without identities and times.
pub(super) fn shape(events: &[RunEvent]) -> Vec<String> {
    events
        .iter()
        .map(|event| match event.kind() {
            RunEventKind::Started => "started".to_owned(),
            RunEventKind::InvocationStarted { .. } => "invocation started".to_owned(),
            RunEventKind::InvocationEnded { result_kind, .. } => {
                format!("invocation ended {result_kind:?}")
            }
            RunEventKind::ProgramEmitted { value } => format!("emitted {}", value.as_json()),
            RunEventKind::ProgramLogged { level, message } => format!("{level:?} {message}"),
            RunEventKind::CancellationRequested { .. } => "cancellation".to_owned(),
            RunEventKind::Ended { state } => format!("ended {state:?}"),
        })
        .collect()
}

#[tokio::test]
async fn an_open_with_another_claim_is_told_the_run_is_not_held() {
    let fixture = TestModules::new(Arc::new(ImmediateEngine)).await;
    let runs = fixture.runs.clone();
    let (workspace_id, principal) = fixture.user_scope("user:claim", "Claims").await;
    let accepted = runs
        .start(&principal, &workspace_id, submission("return 1;"))
        .await
        .expect("accept a Run");
    let claim = take(&runs).await;

    let mut stream = Scripted::open(&runs, &accepted.run_id().to_string(), "not-the-claim");
    assert_eq!(stream.next().await, ended(WireEnd::NotHeld));
    let run = fixture.run_record(&workspace_id, accepted.run_id()).await;
    assert_eq!(
        (run.state, run.execution_started(), run.execution_claim_id),
        (RunState::Running, false, Some(claim))
    );
}

/// Catches: a Run summary that reads the acceptance as the start, so a Run
/// waiting for a Run Executor reads as executing.
#[tokio::test]
async fn a_run_summary_reads_no_start_mark_until_its_run_stream_starts_it() {
    let fixture = TestModules::new(Arc::new(ImmediateEngine)).await;
    let runs = fixture.runs.clone();
    let (workspace_id, principal) = fixture.user_scope("user:start", "Start mark").await;
    let accepted = runs
        .start(&principal, &workspace_id, submission("return 1;"))
        .await
        .expect("accept a Run");
    let started_at = || async {
        runs.inspect_run(
            &principal,
            &workspace_id,
            accepted.run_id(),
            &crate::RunInspection {
                event_limit: 1,
                ..Default::default()
            },
        )
        .await
        .expect("inspect the Run")
        .summary
        .execution_started_at
    };
    assert_eq!(started_at().await, None, "a waiting Run has not started");
    let claim = take(&runs).await;
    assert_eq!(started_at().await, None, "a taken Run has not started");

    let mut stream = Scripted::open(&runs, &accepted.run_id().to_string(), &claim);
    stream.start().await;
    let run = fixture.run_record(&workspace_id, accepted.run_id()).await;
    assert_eq!(
        started_at().await,
        run.execution_started_at_unix_nanos
            .map(coral_db::UnixNanos::new)
    );
    assert!(run.execution_started());
}

#[tokio::test]
async fn a_broken_message_before_started_interrupts_the_run_and_closes_the_stream() {
    // A request where Loaded belongs, and a message with nothing in it.
    for empty in [false, true] {
        let fixture = TestModules::new(Arc::new(ImmediateEngine)).await;
        let runs = fixture.runs.clone();
        let (workspace_id, principal) = fixture.user_scope("user:early", "Early request").await;
        let accepted = runs
            .start(&principal, &workspace_id, submission("return 1;"))
            .await
            .expect("accept a Run");
        let claim = take(&runs).await;
        let mut stream = Scripted::open(&runs, &accepted.run_id().to_string(), &claim);
        assert!(matches!(
            stream.next().await,
            run_response::Message::Package(_)
        ));
        if empty {
            stream
                .requests
                .send(RunRequest { message: None })
                .expect("the stream is served");
        } else {
            stream.request(1, emit(&serde_json::json!({"early": true})));
        }

        assert!(
            matches!(stream.closing().await, Closing::InvalidArgument(_)),
            "{empty}"
        );
        let run = fixture.run_record(&workspace_id, accepted.run_id()).await;
        assert_eq!(
            (run.state, run.execution_started(), run.execution_claim_id),
            (RunState::Interrupted, false, None),
            "{empty}"
        );
    }
}

#[tokio::test]
async fn a_lost_authority_at_the_start_fails_the_run_before_any_program_code() {
    for loss in AgentLoss::ALL {
        let fixture = TestModules::new(Arc::new(ImmediateEngine)).await;
        let runs = fixture.runs.clone();
        let (workspace_id, owner) = fixture.user_scope("user:owner", "Stream start").await;
        let (agent, registration) = agent_scope(&fixture, &owner, &workspace_id).await;
        let accepted = runs
            .start(&agent, &workspace_id, submission("return 1;"))
            .await
            .expect("accept the Agent's Run");
        let claim = take(&runs).await;
        let mut stream = Scripted::open(&runs, &accepted.run_id().to_string(), &claim);
        assert!(matches!(
            stream.next().await,
            run_response::Message::Package(_)
        ));
        loss.apply(&fixture, &owner, &workspace_id, &agent, &registration)
            .await;
        stream.send(run_request::Message::Loaded(Loaded {}));

        assert_eq!(stream.next().await, ended(WireEnd::Failed), "{loss:?}");
        let run = fixture.run_record(&workspace_id, accepted.run_id()).await;
        assert!(
            matches!(&run.terminal_outcome, Some(RunTerminalOutcome::Failed(failure)) if failure.code() == RunFailureCode::RunAuthorityLost),
            "{loss:?}"
        );
        assert_eq!(
            (run.execution_started(), run.execution_claim_id),
            (false, None),
            "{loss:?}"
        );
    }
}

/// Catches: a loop that treats a head another writer moved as corrupt, or
/// that commits again a request that writer already committed, in the
/// chunk in flight or in one built after it.
#[tokio::test]
async fn a_head_race_with_a_direct_write_lands_each_request_once() {
    let (gate, release) = CommitGate::blocking();
    let fixture = TestModules::with_execution_adapters(
        Arc::new(ImmediateEngine),
        Arc::new(RejectingUpstream),
        Arc::new(NoopInvocationObserver),
        gate.clone(),
    )
    .await;
    let runs = fixture.runs.clone();
    let (workspace_id, principal) = fixture.user_scope("user:race", "Head race").await;
    let accepted = runs
        .start(&principal, &workspace_id, submission("return 3;"))
        .await
        .expect("accept a Run");
    let claim = take(&runs).await;
    let mut stream = Scripted::open(&runs, &accepted.run_id().to_string(), &claim);
    stream.start().await;

    // Request 1's chunk waits at the gate, request 2 waits behind it, and
    // another writer commits both and moves the head.
    stream.request(1, emit(&serde_json::json!({"n": 1})));
    wait_until(|| gate.entered()).await;
    stream.request(2, emit(&serde_json::json!({"n": 2})));
    let mut transaction = fixture.database.begin().await.expect("begin the write");
    let run = run_repository::find_exact_in_transaction(
        &mut transaction,
        &workspace_id,
        accepted.run_id(),
    )
    .await
    .expect("read the Run")
    .expect("the Run");
    let others = [1, 2].map(|n| {
        OwnedEvent::new(
            OwnedEventKind::Emitted(serde_json::json!({"n": n}).to_string(), None),
            run.accepted_at_unix_nanos,
        )
    });
    run_repository::commit_chunk(
        &mut transaction,
        Chunk::new(&run, Some(&claim), run.accepted_at_unix_nanos)
            .owned_events(&others)
            .output_bytes(
                run.accumulated_output_bytes
                    + others.iter().map(OwnedEvent::output_bytes).sum::<usize>(),
            )
            .request_number(2),
    )
    .await
    .expect("the other writer's chunk");
    transaction.commit().await.expect("commit it");
    release.send(()).expect("release the loop's commit");

    assert_eq!(stream.next().await, committed(2));
    assert_eq!(stream.next().await, committed(2));
    stream.request(3, emit(&serde_json::json!({"n": 3})));
    assert_eq!(stream.next().await, committed(3));
    stream.request(
        4,
        request::Kind::Finish(Finish {
            completion: Some(finish::Completion::ValueJson("3".to_owned())),
        }),
    );
    assert_eq!(stream.next().await, ended(WireEnd::Succeeded));

    let terminal = wait_for_terminal(&runs, &principal, &workspace_id, accepted.run_id()).await;
    assert_eq!(
        shape(terminal.events()),
        [
            r#"emitted {"n":1}"#,
            r#"emitted {"n":2}"#,
            r#"emitted {"n":3}"#
        ]
    );
    let run = fixture.run_record(&workspace_id, accepted.run_id()).await;
    assert_eq!(run.committed_request_number, Some(3));
}

pub(super) fn invoke_forecast() -> request::Kind {
    request::Kind::Invoke(coral_run_executor::wire::v1::Invoke {
        call_path: "coral.providers.weather.getForecast".to_owned(),
        arguments_json: "{}".to_owned(),
        location: None,
        parent: None,
    })
}

/// Fires one of the stream Run owner's timers now, as its sleep would.
pub(super) fn fire(runs: &RunModule, run_id: &crate::RunId, timer: crate::owner::Timer) {
    runs.owners
        .get(run_id)
        .expect("the stream's owner")
        .send(crate::owner::Message::Timer(timer));
}

/// An accepted Run of `tool` taken and started on a scripted stream.
pub(super) async fn started(runs: &RunModule, tool: &ToolFixture) -> (crate::RunId, Scripted) {
    let accepted = runs
        .start(
            &tool.principal,
            &tool.workspace_id,
            submission("await coral.providers.weather.getForecast({});"),
        )
        .await
        .expect("accept a Run");
    let claim = take(runs).await;
    let mut stream = Scripted::open(runs, &accepted.run_id().to_string(), &claim);
    stream.start().await;
    (accepted.run_id().clone(), stream)
}

/// Catches: a deadline that terminates the Program before the end is
/// written, leaves an Invocation open, or lets a dispatch result that lands
/// afterwards record anything.
#[tokio::test]
async fn a_run_past_its_deadline_ends_timed_out_first_and_drops_a_late_dispatch_result() {
    let upstream = Arc::new(BlockingUpstream::new(HttpOutcome::new(
        200,
        serde_json::json!({}),
    )));
    let fixture = TestModules::with_execution_adapters(
        Arc::new(ImmediateEngine),
        upstream.clone(),
        Arc::new(NoopInvocationObserver),
        Arc::new(NoopExecutionObserver),
    )
    .await;
    let runs = fixture
        .runs
        .clone()
        .with_fan_out_limit(crate::FanOutLimit::new(1).expect("a limit"));
    let tool = fixture
        .user_tool("user:deadline", "Past the deadline")
        .await;
    let (run_id, mut stream) = started(&runs, &tool).await;
    // One Invocation dispatches and holds the one slot; the next waits.
    stream.request(1, invoke_forecast());
    wait_until(|| upstream.entered.load(Ordering::SeqCst)).await;
    stream.request(2, invoke_forecast());
    wait_until_async(async || {
        fixture
            .invocation_rows(&tool.workspace_id, &run_id)
            .await
            .len()
            == 2
    })
    .await;

    fire(&runs, &run_id, crate::owner::Timer::RunDeadline);
    loop {
        match stream.next().await {
            run_response::Message::Terminate(terminate) => {
                assert_eq!(terminate.reason(), TerminateReason::TimedOut);
                let run = fixture.run_record(&tool.workspace_id, &run_id).await;
                assert_eq!(run.state, RunState::TimedOut, "written before Terminate");
                assert_eq!(run.termination, None, "no termination intent");
                break;
            }
            run_response::Message::Ended(_) => panic!("Ended before Terminate"),
            _ => {}
        }
    }
    // The answer to the call refused at the deadline may still come.
    let mut next = stream.next().await;
    if let run_response::Message::InvocationResult(refused) = &next {
        assert_eq!(refused.number, 2);
        next = stream.next().await;
    }
    assert_eq!(next, ended(WireEnd::TimedOut));
    let rows = fixture.invocation_rows(&tool.workspace_id, &run_id).await;
    // The dispatching Invocation ends indeterminate, the waiting one
    // run_terminated, in the terminal chunk itself.
    assert_eq!(
        rows.iter()
            .map(|row| (
                row.result_kind,
                row.dispatch_started_at_unix_nanos.is_some()
            ))
            .collect::<Vec<_>>(),
        [
            (Some(crate::InvocationResultKind::Indeterminate), true),
            (Some(crate::InvocationResultKind::RunTerminated), false),
        ]
    );
    let events = fixture.events_after(&tool.workspace_id, &run_id, 0).await;

    // Only the owner records a dispatch result: once it retired, the
    // released dispatch's result has nowhere to land.
    wait_until(|| runs.owners.get(&run_id).is_none()).await;
    upstream.release();
    wait_until(|| upstream.exited.load(Ordering::SeqCst)).await;
    assert_eq!(
        fixture.invocation_rows(&tool.workspace_id, &run_id).await,
        rows,
        "the late result is dropped"
    );
    assert_eq!(
        fixture.events_after(&tool.workspace_id, &run_id, 0).await,
        events
    );
}

/// Catches: a `Started` whose time left is rounded down, so the Run
/// Executor ends the Program before Coral's deadline and the Run records
/// Interrupted. The test clock moves a nanosecond each read, so the time
/// left when `Started` goes out is just under the time limit.
#[tokio::test]
async fn started_rounds_the_time_left_up_to_a_whole_millisecond() {
    let fixture = TestModules::new(Arc::new(ImmediateEngine)).await;
    let runs = fixture.runs.clone();
    let tool = fixture.user_tool("user:round", "Round up").await;
    let accepted = runs
        .start(&tool.principal, &tool.workspace_id, submission("return 1;"))
        .await
        .expect("accept a Run");
    let claim = take(&runs).await;
    let mut stream = Scripted::open(&runs, &accepted.run_id().to_string(), &claim);
    let started = stream.start().await;
    let run = fixture
        .run_record(&tool.workspace_id, accepted.run_id())
        .await;
    let limit = run.time_limit_nanos.expect("a time limit");
    assert_eq!(
        i64::try_from(started.deadline_ms).unwrap(),
        limit / 1_000_000
    );
}

/// Catches: a `Finish` at the Run Executor's deadline recorded as anything
/// but the owner's own deadline rule.
#[tokio::test]
async fn a_finish_at_the_deadline_times_out_once_corals_clock_passed_it() {
    for (passed, end, state) in [
        (true, WireEnd::TimedOut, RunState::TimedOut),
        (false, WireEnd::Interrupted, RunState::Interrupted),
    ] {
        let fixture = TestModules::new(Arc::new(ImmediateEngine)).await;
        let runs = fixture.runs.clone();
        let tool = fixture
            .user_tool("user:finish", "Finish at the deadline")
            .await;
        let (run_id, mut stream) = started(&runs, &tool).await;
        let run = fixture.run_record(&tool.workspace_id, &run_id).await;
        let deadline = run
            .deadline_at_unix_nanos
            .expect("a started Run's deadline");
        if passed {
            fixture.clock.next.store(deadline, Ordering::SeqCst);
        }

        stream.request(
            1,
            request::Kind::Finish(Finish {
                completion: Some(finish::Completion::End(FinishEnd::DeadlineReached.into())),
            }),
        );
        assert_eq!(stream.next().await, ended(end), "{passed}");
        let run = fixture.run_record(&tool.workspace_id, &run_id).await;
        assert_eq!(run.state, state, "{passed}");
        assert_eq!(
            run.terminal_at_unix_nanos.expect("ended") >= deadline,
            passed,
            "dated at or after the deadline only when it passed"
        );
    }
}

/// Catches: a Run Executor's own end of a Program (a CPU or memory limit, a
/// lost Program host) recorded as a termination intent that waits for its
/// dispatches, written in more than one chunk, or leaving an Invocation
/// open; or a late dispatch result recorded after the end. Past the Run
/// deadline the Run times out instead (ADR 0027: a termination is written
/// first, with no grace).
#[tokio::test]
async fn a_programs_own_end_ends_its_run_in_one_chunk_with_no_grace() {
    for (end, past_deadline, state, wire, failure) in [
        (
            FinishEnd::CpuBudget,
            false,
            RunState::Failed,
            WireEnd::Failed,
            Some(RunFailureCode::CpuLimitExceeded),
        ),
        (
            FinishEnd::MemoryLimit,
            false,
            RunState::Failed,
            WireEnd::Failed,
            Some(RunFailureCode::MemoryLimitExceeded),
        ),
        (
            FinishEnd::HostTerminated,
            false,
            RunState::Interrupted,
            WireEnd::Interrupted,
            None,
        ),
        (
            FinishEnd::CpuBudget,
            true,
            RunState::TimedOut,
            WireEnd::TimedOut,
            None,
        ),
    ] {
        let case = format!("{end:?}, past the deadline: {past_deadline}");
        let upstream = Arc::new(BlockingUpstream::new(HttpOutcome::new(
            200,
            serde_json::json!({}),
        )));
        let fixture = TestModules::with_execution_adapters(
            Arc::new(ImmediateEngine),
            upstream.clone(),
            Arc::new(NoopInvocationObserver),
            Arc::new(NoopExecutionObserver),
        )
        .await;
        let runs = fixture.runs.clone();
        let tool = fixture
            .user_tool("user:own-end", "A Program's own end")
            .await;
        let (run_id, mut stream) = started(&runs, &tool).await;
        stream.request(1, invoke_forecast());
        wait_until(|| upstream.entered.load(Ordering::SeqCst)).await;
        stream.until(|read| read.contains(&committed(1))).await;
        let before = fixture.run_record(&tool.workspace_id, &run_id).await;
        if past_deadline {
            let deadline = before
                .deadline_at_unix_nanos
                .expect("a started Run's deadline");
            fixture.clock.next.store(deadline, Ordering::SeqCst);
        }

        stream.request(
            2,
            request::Kind::Finish(Finish {
                completion: Some(finish::Completion::End(end.into())),
            }),
        );
        let read = stream
            .until(|read| {
                read.iter()
                    .any(|message| matches!(message, run_response::Message::Ended(_)))
            })
            .await;
        assert_eq!(read.last(), Some(&ended(wire)), "{case}");
        let run = fixture.run_record(&tool.workspace_id, &run_id).await;
        assert_eq!(run.state, state, "{case}");
        let code = match &run.terminal_outcome {
            Some(crate::RunTerminalOutcome::Failed(failure)) => Some(failure.code()),
            _ => None,
        };
        assert_eq!(code, failure, "{case}");
        assert_eq!(
            run.chunk_sequence,
            before.chunk_sequence + 1,
            "{case}: one chunk"
        );
        assert_eq!(run.termination, None, "{case}: no termination intent");
        let rows = fixture.invocation_rows(&tool.workspace_id, &run_id).await;
        assert_eq!(
            rows.iter().map(|row| row.result_kind).collect::<Vec<_>>(),
            [Some(crate::InvocationResultKind::Indeterminate)],
            "{case}"
        );

        wait_until(|| runs.owners.get(&run_id).is_none()).await;
        upstream.release();
        wait_until(|| upstream.exited.load(Ordering::SeqCst)).await;
        assert_eq!(
            fixture.invocation_rows(&tool.workspace_id, &run_id).await,
            rows,
            "{case}: the late result is dropped"
        );
    }
}

/// Catches: an Approval Request's deadline the loop does not record, or
/// one that overrides a decision committed before it.
#[tokio::test]
async fn an_approval_expires_in_the_loop_unless_a_decision_committed_first() {
    for decided_first in [false, true] {
        let fixture = TestModules::new(Arc::new(ImmediateEngine)).await;
        let runs = fixture.runs.clone();
        let tool = fixture.user_tool("user:review", "Expire in the loop").await;
        fixture.require_approval(&tool).await;
        let approvals = fixture.approvals();
        let (run_id, mut stream) = started(&runs, &tool).await;
        stream.request(1, invoke_forecast());
        let request =
            wait_for_approval(&approvals, &tool.principal, &tool.workspace_id, &run_id).await;
        if decided_first {
            let decided = approvals
                .decide(
                    &tool.principal,
                    &tool.workspace_id,
                    &request.id,
                    ApprovalDecision::Approve,
                    None,
                    &TraceId::parse(&"a".repeat(32)).unwrap(),
                )
                .await
                .expect("decide first");
            assert_eq!(decided.state, ApprovalState::Approved);
        }

        fire(
            &runs,
            &run_id,
            crate::owner::Timer::Approval(request.id.clone()),
        );
        wait_until_async(async || {
            fixture
                .invocation_rows(&tool.workspace_id, &run_id)
                .await
                .iter()
                .all(|row| row.ended_at_unix_nanos.is_some())
        })
        .await;
        // The owner applies its mailbox in order: once the Finish sent after
        // the timer is answered, the timer was applied.
        stream.request(
            2,
            request::Kind::Finish(Finish {
                completion: Some(finish::Completion::ValueJson("1".to_owned())),
            }),
        );
        let end = loop {
            if let message @ run_response::Message::Ended(_) = stream.next().await {
                break message;
            }
        };
        assert_eq!(end, ended(WireEnd::Succeeded));
        let settled = approvals
            .get(&tool.principal, &tool.workspace_id, &request.id)
            .await
            .expect("read the request");
        let rows = fixture.invocation_rows(&tool.workspace_id, &run_id).await;
        if decided_first {
            assert_eq!(settled.state, ApprovalState::Approved);
            assert!(
                matches!(rows.as_slice(), [row] if row.dispatch_started_at_unix_nanos.is_some()
                    && row.failure_code.as_deref() != Some("approval_expired")),
                "the approved call dispatched: {rows:?}"
            );
        } else {
            assert_eq!(settled.state, ApprovalState::Expired);
            assert_eq!(settled.approver, None);
            assert_eq!(
                settled.decided_at_unix_nanos,
                Some(request.deadline_at_unix_nanos)
            );
            assert!(
                matches!(rows.as_slice(), [row] if row.failure_code.as_deref() == Some("approval_expired")),
                "{rows:?}"
            );
        }
    }
}

/// Catches: a stale stream owner whose interruption lands on a Run whose
/// stream another Coral Instance now holds, under the claim the reopen kept.
#[tokio::test]
async fn a_stream_owner_writes_no_interruption_once_another_instance_holds_the_stream() {
    let fixture = TestModules::new(Arc::new(ImmediateEngine)).await;
    let runs = fixture.runs.clone();
    let tool = fixture.user_tool("user:holder", "Stream holder").await;
    let (run_id, stream) = started(&runs, &tool).await;
    drop(stream);
    wait_until(|| runs.owners.get(&run_id).is_none()).await;
    let elsewhere = crate::CoralInstanceId::generate();
    assert_eq!(
        fixture
            .inject_fault(&format!(
                "UPDATE program_run_records SET holder_instance_id = '{elsewhere}' \
                 WHERE id = '{run_id}'"
            ))
            .await,
        1
    );
    let before = (
        fixture.run_record(&tool.workspace_id, &run_id).await,
        fixture.events_after(&tool.workspace_id, &run_id, 0).await,
    );
    let claim = before
        .0
        .execution_claim_id
        .clone()
        .expect("the take's claim");

    let interrupted = runs
        .interrupt_owned_run(
            &tool.workspace_id,
            &run_id,
            &claim,
            None,
            crate::termination::Interrupter::Owner,
        )
        .await;

    assert!(
        matches!(interrupted, Err(Error::ClaimMismatch)),
        "{interrupted:?}"
    );
    assert_eq!(
        (
            fixture.run_record(&tool.workspace_id, &run_id).await,
            fixture.events_after(&tool.workspace_id, &run_id, 0).await,
        ),
        before
    );
}
