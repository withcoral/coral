//! Coral's side of a Run stream: `Open` is answered by the Run row,
//! `Loaded` rechecks authority and starts the Run, and the Program's
//! requests commit in number order under the claim and the head.
use super::*;
use crate::approval::{ApprovalDecision, ApprovalState};
use coral_run_executor::wire::v1::{
    Emit, Finish, FinishEnd, Loaded, Open, Request as WireRequest, RunEnd as WireEnd, RunRequest,
    RunResponse, TerminateReason, finish, request, run_request, run_response,
};
use coral_run_executor::{Closing, Credentials, Kind, LinkServer, PROTOCOL_LEVEL, ServedRunStream};
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;

use super::agents::{AgentLoss, agent_scope};
use crate::run_repository::{self, Chunk, OwnedEvent, OwnedEventKind, Taker};

/// One Run stream served by `runs`, driven as a scripted Run Executor.
struct Scripted {
    requests: mpsc::UnboundedSender<RunRequest>,
    responses: mpsc::UnboundedReceiver<Result<RunResponse, Closing>>,
}

impl Scripted {
    fn open(runs: &RunModule, run_id: &str, claim_id: &str) -> Self {
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
        stream.send(run_request::Message::Open(Open {
            run_id: run_id.to_owned(),
            claim_id: claim_id.to_owned(),
            protocol_level: PROTOCOL_LEVEL,
        }));
        stream
    }

    fn send(&self, message: run_request::Message) {
        self.requests
            .send(RunRequest {
                message: Some(message),
            })
            .expect("the stream is served");
    }

    fn request(&self, number: u64, kind: request::Kind) {
        self.send(run_request::Message::Request(WireRequest {
            number,
            kind: Some(kind),
        }));
    }

    async fn recv(&mut self) -> Result<RunResponse, Closing> {
        tokio::time::timeout(EVENTUALLY, self.responses.recv())
            .await
            .expect("Coral answers in time")
            .expect("the stream is open")
    }

    async fn next(&mut self) -> run_response::Message {
        self.recv()
            .await
            .expect("a message, not a close")
            .message
            .expect("a message")
    }

    /// How Coral closed the stream without `Ended`.
    async fn closing(&mut self) -> Closing {
        self.recv().await.expect_err("a close, not a message")
    }

    /// Opens, loads and expects `Started`.
    async fn start(&mut self) {
        assert!(matches!(
            self.next().await,
            run_response::Message::Package(_)
        ));
        self.send(run_request::Message::Loaded(Loaded {}));
        assert!(matches!(
            self.next().await,
            run_response::Message::Started(_)
        ));
    }
}

fn emit(value: &serde_json::Value) -> request::Kind {
    request::Kind::Emit(Emit {
        value_json: value.to_string(),
        location: None,
    })
}

fn ended(end: WireEnd) -> run_response::Message {
    run_response::Message::Ended(coral_run_executor::wire::v1::Ended { end: end.into() })
}

fn committed(number: u64) -> run_response::Message {
    run_response::Message::Committed(coral_run_executor::wire::v1::Committed {
        request_number: number,
    })
}

/// Takes the oldest waiting Run as a Run Executor would, and returns its claim.
async fn take(runs: &RunModule) -> String {
    let mut transaction = runs.database.begin().await.expect("begin the take");
    let claimed = run_repository::take_oldest(
        &mut transaction,
        1,
        &Taker {
            run_executor_id: "6f0c2a59-55a7-4e0b-9a2e-7c1d4f3b8e21",
            run_executor_kind: Kind::Dialed,
        },
        1,
    )
    .await
    .expect("take a waiting Run");
    transaction.commit().await.expect("commit the take");
    claimed.into_iter().next().expect("a waiting Run").claim_id
}

/// What a Run's Events say, without identities and times.
fn shape(events: &[RunEvent]) -> Vec<String> {
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
async fn a_programs_emissions_logs_and_invocations_land_as_they_do_with_the_run_owner() {
    let server = wiremock::MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::path("/forecast"))
        .respond_with(
            wiremock::ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"temperature": 18})),
        )
        .mount(&server)
        .await;
    let fixture = TestModules::new(Arc::new(ImmediateEngine::default())).await;
    let tool = fixture
        .user_tool_at("user:stream", "Stream parity", &server.uri())
        .await;
    let source = r#"
        await coral.emit({ step: 1 });
        console.log("between");
        const forecast = await coral.providers.weather.getForecast();
        await coral.emit({ temperature: forecast.data.temperature });
        return forecast.data.temperature;
    "#;
    let owned = fixture.live_runs(None);
    let on_streams = fixture.live_runs(None).with_run_streams();
    let link = LinkServer::new(
        Credentials::new([coral_run_executor::Token::new(
            coral_run_executor::testing::RUN_EXECUTOR_TOKEN.to_owned(),
        )
        .expect("a token")]),
        fixture.database.clone(),
        on_streams.instance.as_str().to_owned(),
    )
    .with_run_streams(on_streams.run_stream_server());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind the link");
    let address = format!("http://{}", listener.local_addr().expect("its address"));
    let streams = link.streams();
    tokio::spawn(link.serve(listener, std::future::pending()));
    let taker = on_streams.clone();
    tokio::spawn(async move { taker.take_runs(streams).await });
    let (_retire, _run_executor) = coral_run_executor::testing::spawn_run_executor_at(
        &address,
        4,
        coral_run_executor::RunLimits::default(),
        None,
    );

    let mut shapes = Vec::new();
    for runs in [&owned, &on_streams] {
        let accepted = runs
            .start(&tool.principal, &tool.workspace_id, submission(source))
            .await
            .expect("accept the Program");
        let terminal =
            wait_for_terminal(runs, &tool.principal, &tool.workspace_id, accepted.run_id()).await;
        assert!(
            matches!(
                terminal.terminal_outcome(),
                Some(RunTerminalOutcome::Succeeded { .. })
            ),
            "{:?}",
            terminal.terminal_outcome()
        );
        shapes.push((shape(terminal.events()), accepted.run_id().clone()));
    }
    let [(owned, _), (streamed, run_id)] = shapes.as_slice() else {
        panic!("one Run on each path");
    };
    assert_eq!(owned, streamed);
    // The stream's Run committed its requests in number order: the two
    // Emissions, the Log and the Invocation; the terminal write took Finish.
    let run = fixture.run_record(&tool.workspace_id, run_id).await;
    assert_eq!(run.committed_request_number, Some(4));
    let numbers: Vec<(Option<i64>,)> = fixture
        .database
        .fetch_all(
            Query::select()
                .column(Alias::new("request_number"))
                .from(Alias::new("tool_invocations"))
                .and_where(Expr::col(Alias::new("run_id")).eq(run_id.to_string()))
                .to_owned(),
        )
        .await
        .expect("read the Invocations");
    assert_eq!(numbers, vec![(Some(3),)]);
}

#[tokio::test]
async fn an_open_with_another_claim_is_told_the_run_is_not_held() {
    let fixture = TestModules::new(Arc::new(ImmediateEngine::default())).await;
    let runs = fixture.runs.clone().with_run_streams();
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

#[tokio::test]
async fn a_broken_message_before_started_interrupts_the_run_and_closes_the_stream() {
    // A request where Loaded belongs, and a message with nothing in it.
    for empty in [false, true] {
        let fixture = TestModules::new(Arc::new(ImmediateEngine::default())).await;
        let runs = fixture.runs.clone().with_run_streams();
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
        let fixture = TestModules::new(Arc::new(ImmediateEngine::default())).await;
        let runs = fixture.runs.clone().with_run_streams();
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
        Arc::new(ImmediateEngine::default()),
        Arc::new(RejectingUpstream),
        Arc::new(NoopInvocationObserver),
        gate.clone(),
    )
    .await;
    let runs = fixture.runs.clone().with_run_streams();
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

fn invoke_forecast() -> request::Kind {
    request::Kind::Invoke(coral_run_executor::wire::v1::Invoke {
        call_path: "coral.providers.weather.getForecast".to_owned(),
        arguments_json: "{}".to_owned(),
        location: None,
        parent: None,
    })
}

/// Fires one of the stream Run owner's timers now, as its sleep would.
fn fire(runs: &RunModule, run_id: &crate::RunId, timer: crate::owner::Timer) {
    runs.owners
        .get(run_id)
        .expect("the stream's owner")
        .send(crate::owner::Message::Timer(timer));
}

/// An accepted Run of `tool` taken and started on a scripted stream.
async fn started(runs: &RunModule, tool: &ToolFixture) -> (crate::RunId, Scripted) {
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
        Arc::new(ImmediateEngine::default()),
        upstream.clone(),
        Arc::new(NoopInvocationObserver),
        Arc::new(NoopExecutionObserver),
    )
    .await;
    let runs = fixture
        .runs
        .clone()
        .with_fan_out_limit(crate::FanOutLimit::new(1).expect("a limit"))
        .with_run_streams();
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

/// Catches: a `Finish` at the Run Executor's deadline recorded as anything
/// but the owner's own deadline rule.
#[tokio::test]
async fn a_finish_at_the_deadline_times_out_once_corals_clock_passed_it() {
    for (passed, end, state) in [
        (true, WireEnd::TimedOut, RunState::TimedOut),
        (false, WireEnd::Interrupted, RunState::Interrupted),
    ] {
        let fixture = TestModules::new(Arc::new(ImmediateEngine::default())).await;
        let runs = fixture.runs.clone().with_run_streams();
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

/// Catches: an approval deadline the loop does not record, or one that
/// overrides a decision committed before it.
#[tokio::test]
async fn an_approval_expires_in_the_loop_unless_a_decision_committed_first() {
    for decided_first in [false, true] {
        let fixture = TestModules::new(Arc::new(ImmediateEngine::default())).await;
        let runs = fixture.runs.clone().with_run_streams();
        let tool = fixture.user_tool("user:review", "Expire in the loop").await;
        fixture.require_approval(&tool).await;
        let approvals = fixture.approvals();
        let (run_id, stream) = started(&runs, &tool).await;
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
