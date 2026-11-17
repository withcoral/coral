//! Coral's side of a Run stream (ADR 0027): `Open` is answered by the Run
//! row, `Loaded` rechecks authority and starts the Run, and the Program's
//! requests commit in number order under the claim and the head.
use super::*;
use coral_run_executor::wire::v1::{
    Emit, Finish, Loaded, Open, Request as WireRequest, RunEnd as WireEnd, RunRequest, RunResponse,
    finish, request, run_request, run_response,
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

    async fn next(&mut self) -> run_response::Message {
        tokio::time::timeout(EVENTUALLY, self.responses.recv())
            .await
            .expect("Coral answers in time")
            .expect("the stream is open")
            .expect("a message, not a close")
            .message
            .expect("a message")
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
    let _claim = take(&runs).await;

    let mut stream = Scripted::open(&runs, &accepted.run_id().to_string(), "not-the-claim");
    assert_eq!(stream.next().await, ended(WireEnd::NotHeld));
    let run = fixture.run_record(&workspace_id, accepted.run_id()).await;
    assert_eq!(
        (run.state, run.execution_started()),
        (RunState::Running, false)
    );
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
/// that commits again a request that writer already committed.
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

    // Request 1's chunk waits at the gate while another writer commits
    // request 1 itself and moves the head.
    stream.request(1, emit(&serde_json::json!({"n": 1})));
    wait_until(|| gate.entered()).await;
    let mut transaction = fixture.database.begin().await.expect("begin the write");
    let run = run_repository::find_exact_in_transaction(
        &mut transaction,
        &workspace_id,
        accepted.run_id(),
    )
    .await
    .expect("read the Run")
    .expect("the Run");
    let other = OwnedEvent::new(
        OwnedEventKind::Emitted(serde_json::json!({"n": 1}).to_string(), None),
        run.accepted_at_unix_nanos,
    );
    run_repository::commit_chunk(
        &mut transaction,
        Chunk::new(&run, Some(&claim), run.accepted_at_unix_nanos)
            .owned_events([&other])
            .output_bytes(run.accumulated_output_bytes + other.output_bytes())
            .request_number(1),
    )
    .await
    .expect("the other writer's chunk");
    transaction.commit().await.expect("commit it");
    release.send(()).expect("release the loop's commit");

    assert_eq!(stream.next().await, committed(1));
    stream.request(2, emit(&serde_json::json!({"n": 2})));
    assert_eq!(stream.next().await, committed(2));
    stream.request(
        3,
        request::Kind::Finish(Finish {
            completion: Some(finish::Completion::ValueJson("3".to_owned())),
        }),
    );
    assert_eq!(stream.next().await, ended(WireEnd::Succeeded));

    let terminal = wait_for_terminal(&runs, &principal, &workspace_id, accepted.run_id()).await;
    assert_eq!(
        shape(terminal.events()),
        [r#"emitted {"n":1}"#, r#"emitted {"n":2}"#]
    );
    let run = fixture.run_record(&workspace_id, accepted.run_id()).await;
    assert_eq!(run.committed_request_number, Some(2));
}
