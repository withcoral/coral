//! A Run's stream reopens on any Coral Instance: the reopen settles the call
//! in flight on the loop before as `indeterminate`, takes back the call that
//! waits for its approval, answers again what the Run Executor waits on, and
//! lands each resent request once.
use super::*;
use coral_run_executor::PROTOCOL_LEVEL;
use coral_run_executor::wire::v1::{
    Awaited, Complete, Finish, InvocationResult, Invoke, Open, Perform, RunEnd as WireEnd,
    complete, finish, invocation_result, request, run_response,
};

use super::agents::{AgentLoss, agent_scope};
use super::run_streams::{Scripted, committed, emit, ended, shape, take, take_for};
use crate::InvocationResultKind;
use crate::approval::ApprovalDecision;

/// A call to the weather Tool, from the body of the Perform `parent` when
/// there is one.
pub(super) fn forecast(parent: Option<u64>) -> request::Kind {
    request::Kind::Invoke(Invoke {
        call_path: "coral.providers.weather.getForecast".to_owned(),
        arguments_json: "{}".to_owned(),
        location: None,
        parent,
    })
}

/// The answers to request `number` in `messages`.
pub(super) fn answers(messages: &[run_response::Message], number: u64) -> Vec<&InvocationResult> {
    messages
        .iter()
        .filter_map(|message| match message {
            run_response::Message::InvocationResult(result) if result.number == number => {
                Some(result)
            }
            _ => None,
        })
        .collect()
}

impl Scripted {
    /// Reads until `done` holds for every message read so far.
    pub(super) async fn until(
        &mut self,
        done: impl Fn(&[run_response::Message]) -> bool,
    ) -> Vec<run_response::Message> {
        let mut read = Vec::new();
        while !done(&read) {
            read.push(self.next().await);
        }
        read
    }

    /// Reads past any messages to how Coral closed the stream.
    async fn closed(&mut self) -> coral_run_executor::Closing {
        self.read_to_close().await.1
    }

    /// Reads to how Coral closed the stream, with every message before it.
    pub(super) async fn read_to_close(
        &mut self,
    ) -> (Vec<run_response::Message>, coral_run_executor::Closing) {
        let mut read = Vec::new();
        loop {
            match self.recv().await {
                Ok(response) => read.push(response.message.expect("a message")),
                Err(closing) => return (read, closing),
            }
        }
    }
}

/// The Approval Requests of `run`, once there are `count`.
async fn approval_requests(
    fixture: &TestModules,
    tool: &ToolFixture,
    run: &crate::RunId,
    count: usize,
) -> Vec<crate::approval::ApprovalRequest> {
    let approvals = fixture.approvals();
    let list = async || {
        approvals
            .list_for_run(&tool.principal, &tool.workspace_id, run)
            .await
            .expect("list the Approval Requests")
    };
    wait_until_async(async || list().await.len() == count).await;
    list().await
}

/// `principal` approves `id` in `workspace_id` through `runs`.
async fn approve(
    runs: RunModule,
    principal: AuthenticatedPrincipal,
    workspace_id: coral_workspace::WorkspaceId,
    id: crate::approval::ApprovalId,
) -> Result<crate::approval::ApprovalRequest, Error> {
    runs.approvals()
        .decide(
            &principal,
            &workspace_id,
            &id,
            ApprovalDecision::Approve,
            None,
            &TraceId::parse(&"d".repeat(32)).expect("a trace id"),
        )
        .await
}

/// The one answer to request `number` in `read`.
pub(super) fn only_answer(read: &[run_response::Message], number: u64) -> &InvocationResult {
    let [answer] = answers(read, number)[..] else {
        panic!("one answer to request {number}: {read:?}");
    };
    answer
}

/// Approves `waiting` from another Coral Instance while no loop holds its
/// Run's stream: the decision is recorded at once, and the reopened loop
/// finds it in the rows.
async fn decide_without_a_loop(
    fixture: &TestModules,
    tool: &ToolFixture,
    waiting: crate::approval::ApprovalId,
) {
    let decided = approve(
        fixture.next_process(),
        tool.principal.clone(),
        tool.workspace_id.clone(),
        waiting,
    )
    .await
    .expect("the decision is recorded");
    assert_eq!(decided.state, crate::approval::ApprovalState::Approved);
}

/// Each Invocation of `run_id`, in admission order, by its result kind.
pub(super) async fn result_kinds(
    fixture: &TestModules,
    tool: &ToolFixture,
    run_id: &crate::RunId,
) -> Vec<Option<InvocationResultKind>> {
    fixture
        .invocation_rows(&tool.workspace_id, run_id)
        .await
        .into_iter()
        .map(|row| row.result_kind)
        .collect()
}

/// A started Run on its stream from `first`, and `second`, the module its
/// stream moves to: another Coral Instance on Postgres; on SQLite, which has
/// one, the same module, whose stream drops.
pub(super) struct Moving {
    pub(super) fixture: TestModules,
    pub(super) first: RunModule,
    pub(super) second: RunModule,
    pub(super) tool: ToolFixture,
    pub(super) run_id: crate::RunId,
    pub(super) claim: String,
}

/// The two modules over `fixture` a Run's stream moves between, each as
/// `serving` makes it.
fn modules(
    fixture: &TestModules,
    serving: impl Fn(RunModule) -> RunModule,
) -> (RunModule, RunModule) {
    let first = serving(fixture.runs.clone());
    let second = if fixture.database.system() == coral_db::DatabaseSystem::Postgres {
        serving(fixture.next_process())
    } else {
        first.clone()
    };
    (first, second)
}

/// Starts a Run of `source` as `tool`'s User on a stream from `first`.
pub(super) async fn start_moving(
    fixture: TestModules,
    instances: (RunModule, RunModule),
    tool: ToolFixture,
    source: &str,
) -> (Moving, Scripted) {
    Box::pin(start_moving_for(fixture, instances, tool, source, None)).await
}

/// [`start_moving`], with the Run taken for `taker` when one is given.
pub(super) async fn start_moving_for(
    fixture: TestModules,
    (first, second): (RunModule, RunModule),
    tool: ToolFixture,
    source: &str,
    taker: Option<&crate::run_repository::Taker<'_>>,
) -> (Moving, Scripted) {
    let accepted = first
        .start(&tool.principal, &tool.workspace_id, submission(source))
        .await
        .expect("accept a Run");
    let run_id = accepted.run_id().clone();
    let claim = match taker {
        Some(taker) => take_for(&first, taker, 1).await,
        None => take(&first).await,
    };
    let mut stream = Scripted::open(&first, &run_id.to_string(), &claim);
    stream.start().await;
    let moving = Moving {
        fixture,
        first,
        second,
        tool,
        run_id,
        claim,
    };
    (moving, stream)
}

/// A started Run whose weather Tool waits for approval, sending through
/// `upstream`.
async fn moving(upstream: Arc<SequencedUpstream>) -> (Moving, Scripted) {
    let fixture = TestModules::with_upstream(Arc::new(ImmediateEngine), upstream).await;
    let tool = fixture.user_tool("user:reconnect", "Reconnect").await;
    fixture.require_approval(&tool).await;
    let modules = modules(&fixture, |runs| runs);
    start_moving(fixture, modules, tool, "return 1;").await
}

/// Reopens the moved stream on `second`, waiting on `awaited`.
pub(super) fn reopen(moving: &Moving, awaited: &[(u64, u64)]) -> Scripted {
    Scripted::open_with(
        &moving.second,
        Open {
            run_id: moving.run_id.to_string(),
            claim_id: moving.claim.clone(),
            protocol_level: PROTOCOL_LEVEL,
            awaited: awaited
                .iter()
                .map(|&(number, perform)| Awaited { number, perform })
                .collect(),
        },
    )
}

pub(super) fn finish_with(value: &str) -> request::Kind {
    request::Kind::Finish(Finish {
        completion: Some(finish::Completion::ValueJson(value.to_owned())),
    })
}

pub(super) fn failed_with(answer: &InvocationResult, code: &str) -> bool {
    matches!(&answer.outcome,
        Some(invocation_result::Outcome::CoralFailure(failure)) if failure.code == code)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_run_stream_that_moves_mid_run_lands_each_request_and_result_once() {
    let upstream = SequencedUpstream::new(HttpOutcome::new(
        200,
        serde_json::json!({"temperature": 18}),
    ));
    let (moving, mut stream) = moving(upstream.clone()).await;
    let (fixture, tool, run_id) = (&moving.fixture, &moving.tool, &moving.run_id);

    // Request 2 is approved and its call is in flight on the first loop;
    // request 3 waits for its approval.
    stream.request(1, emit(&serde_json::json!({"step": 1})));
    stream.request(2, forecast(None));
    let requests = approval_requests(fixture, tool, run_id, 1).await;
    let [in_flight] = requests.as_slice() else {
        panic!("one Approval Request: {requests:?}");
    };
    approve(
        fixture.runs.clone(),
        tool.principal.clone(),
        tool.workspace_id.clone(),
        in_flight.id.clone(),
    )
    .await
    .expect("approve request 2");
    upstream.until_sent(1).await;
    stream.request(3, forecast(None));
    let waiting = approval_requests(fixture, tool, run_id, 2).await;
    let waiting = waiting
        .iter()
        .find(|request| request.id != in_flight.id)
        .expect("request 3's Approval Request")
        .id
        .clone();
    stream.until(|read| read.contains(&committed(3))).await;
    // Request 4 may land before the stream drops, or not.
    stream.request(4, emit(&serde_json::json!({"step": 4})));
    drop(stream);

    // Decided while no loop holds the Run's stream.
    decide_without_a_loop(fixture, tool, waiting).await;

    // The Run Executor saw `Committed(3)`, waits on 2 and 3, and resends 4.
    let mut stream = reopen(&moving, &[(2, 0), (3, 0)]);
    let reopened = stream.next().await;
    assert!(
        reopened == committed(3) || reopened == committed(4),
        "{reopened:?}"
    );
    stream.request(4, emit(&serde_json::json!({"step": 4})));
    upstream.release_all();
    let read = stream
        .until(|read| !answers(read, 2).is_empty() && !answers(read, 3).is_empty())
        .await;
    assert!(
        read.iter()
            .any(|message| matches!(message, run_response::Message::Started(_)))
    );
    // The call in flight on the first loop ended with it, and the call
    // decided meanwhile went on after the reopen.
    assert!(
        failed_with(only_answer(&read, 2), "upstream_incomplete"),
        "{read:?}"
    );
    assert!(
        matches!(
            &only_answer(&read, 3).outcome,
            Some(invocation_result::Outcome::ResultJson(_))
        ),
        "{read:?}"
    );
    stream.request(5, finish_with("1"));
    let rest = stream
        .until(|read| read.contains(&ended(WireEnd::Succeeded)))
        .await;
    assert!(
        answers(&rest, 2).is_empty() && answers(&rest, 3).is_empty(),
        "{rest:?}"
    );

    let terminal =
        wait_for_terminal(&moving.second, &tool.principal, &tool.workspace_id, run_id).await;
    assert_eq!(
        shape(terminal.events()),
        [r#"emitted {"step":1}"#, r#"emitted {"step":4}"#]
    );
    // The first loop's call was let go after the move: its result changed
    // nothing, and its Invocation stays `indeterminate`.
    assert_eq!(
        result_kinds(fixture, tool, run_id).await,
        [
            Some(InvocationResultKind::Indeterminate),
            Some(InvocationResultKind::UpstreamOutcome)
        ]
    );
    let run = fixture.run_record(&tool.workspace_id, run_id).await;
    assert_eq!(run.committed_request_number, Some(4));
}

#[tokio::test]
async fn a_reopen_answers_a_waited_request_that_left_no_row_as_not_admitted() {
    let fixture = TestModules::new(Arc::new(ImmediateEngine)).await;
    let runs = fixture.runs.clone();
    let (workspace_id, principal) = fixture.user_scope("user:unadmitted", "Not admitted").await;
    let accepted = runs
        .start(&principal, &workspace_id, submission("return 1;"))
        .await
        .expect("accept a Run");
    let claim = take(&runs).await;
    let mut stream = Scripted::open(&runs, &accepted.run_id().to_string(), &claim);
    stream.start().await;
    // A Call Path that names nothing is refused before admission.
    stream.request(
        1,
        request::Kind::Invoke(Invoke {
            call_path: "coral.providers.nothing.here".to_owned(),
            arguments_json: "{}".to_owned(),
            location: None,
            parent: None,
        }),
    );
    stream.request(2, emit(&serde_json::json!({"step": 2})));
    stream.until(|read| read.contains(&committed(2))).await;
    drop(stream);

    let mut stream = Scripted::open_with(
        &runs,
        Open {
            run_id: accepted.run_id().to_string(),
            claim_id: claim,
            protocol_level: PROTOCOL_LEVEL,
            awaited: vec![Awaited {
                number: 1,
                perform: 0,
            }],
        },
    );
    assert_eq!(stream.next().await, committed(2));
    let read = stream.until(|read| !answers(read, 1).is_empty()).await;
    assert!(
        failed_with(only_answer(&read, 1), "invocation_not_admitted"),
        "{read:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_loop_whose_stream_moved_commits_nothing_more() {
    let (gate, release) = CommitGate::blocking();
    let upstream = SequencedUpstream::new(HttpOutcome::new(
        200,
        serde_json::json!({"temperature": 18}),
    ));
    upstream.release_all();
    let fixture = TestModules::with_execution_adapters(
        Arc::new(ImmediateEngine),
        upstream,
        Arc::new(NoopInvocationObserver),
        gate.clone(),
    )
    .await;
    let tool = fixture.user_tool("user:fenced", "Fence").await;
    let modules = modules(&fixture, |runs| runs);
    let (moving, stream) = start_moving(fixture, modules, tool, "return 1;").await;
    // Request 1's admission waits at the gate in the first loop's chunk.
    stream.request(1, forecast(None));
    wait_until(|| gate.entered()).await;
    let first_loop = moving
        .first
        .owners
        .find(&moving.run_id, &moving.claim)
        .expect("the first loop's owner");
    drop(stream);
    let mut stream = reopen(&moving, &[(1, 0)]);
    assert_eq!(stream.next().await, committed(0));
    // The first loop's chunk finds the stream moved: it lands nothing, so
    // the resent request is admitted once, on the new loop.
    release.send(()).expect("release the first loop's chunk");
    wait_until(|| first_loop.is_retired()).await;
    stream.request(1, forecast(None));
    let read = stream.until(|read| !answers(read, 1).is_empty()).await;
    assert!(
        matches!(
            &only_answer(&read, 1).outcome,
            Some(invocation_result::Outcome::ResultJson(_))
        ),
        "{read:?}"
    );
    stream.request(2, finish_with("1"));
    stream
        .until(|read| read.contains(&ended(WireEnd::Succeeded)))
        .await;
    assert_eq!(
        result_kinds(&moving.fixture, &moving.tool, &moving.run_id).await,
        [Some(InvocationResultKind::UpstreamOutcome)]
    );
}

#[tokio::test]
async fn a_reopen_rebuilds_the_runs_actor_and_interrupts_a_run_whose_principal_is_gone() {
    // A disabled Agent keeps its Run, whose calls the enforcement point
    // refuses; a deleted one leaves no actor to run it as. The reopen lands
    // on the Coral Instance that held the stream, or on another, which the
    // row does not name as the holder yet.
    for (loss, reopened, elsewhere) in [
        (AgentLoss::AgentDisabled, committed(0), false),
        (AgentLoss::AgentDisabled, committed(0), true),
        (AgentLoss::Deleted, ended(WireEnd::Interrupted), false),
        (AgentLoss::Deleted, ended(WireEnd::Interrupted), true),
    ] {
        let fixture = TestModules::new(Arc::new(ImmediateEngine)).await;
        let runs = fixture.runs.clone();
        let (workspace_id, owner) = fixture.user_scope("user:owner", "Reopen actor").await;
        let (agent, registration) = agent_scope(&fixture, &owner, &workspace_id).await;
        let accepted = runs
            .start(&agent, &workspace_id, submission("return 1;"))
            .await
            .expect("accept the Agent's Run");
        let run_id = accepted.run_id().to_string();
        let claim = take(&runs).await;
        let mut stream = Scripted::open(&runs, &run_id, &claim);
        stream.start().await;
        drop(stream);
        loss.apply(&fixture, &owner, &workspace_id, &agent, &registration)
            .await;
        let reopening = if elsewhere {
            fixture.next_process()
        } else {
            runs.clone()
        };
        let mut stream = Scripted::open(&reopening, &run_id, &claim);
        assert_eq!(stream.next().await, reopened, "{loss:?} {elsewhere}");
    }
}

#[tokio::test]
async fn a_waiting_call_whose_provider_is_gone_ends_as_the_enforcement_point_would() {
    let upstream = SequencedUpstream::new(HttpOutcome::new(200, serde_json::json!({})));
    let (moving, mut stream) = moving(upstream).await;
    stream.request(1, forecast(None));
    approval_requests(&moving.fixture, &moving.tool, &moving.run_id, 1).await;
    stream.until(|read| read.contains(&committed(1))).await;
    drop(stream);
    moving
        .fixture
        .providers
        .delete_provider(
            &change(moving.tool.user.clone(), 100, 'd'),
            &moving.tool.workspace_id,
            &moving.tool.provider_id,
            &IfMatch::any(),
        )
        .await
        .expect("remove the Provider")
        .skip_search();
    let mut stream = reopen(&moving, &[(1, 0)]);
    assert_eq!(stream.next().await, committed(1));
    let read = stream.until(|read| !answers(read, 1).is_empty()).await;
    assert!(
        failed_with(only_answer(&read, 1), "dispatch_authority_lost"),
        "{read:?}"
    );
    assert_eq!(
        result_kinds(&moving.fixture, &moving.tool, &moving.run_id).await,
        [Some(InvocationResultKind::CoralFailure)]
    );
}

/// A started Run whose Program calls the User's Private Tool `notes.title`,
/// whose body calls the weather Tool through `upstream`; the Private Tool
/// declares `default`.
async fn moving_bodies(
    upstream: Arc<SequencedUpstream>,
    default: Option<ToolAccessState>,
) -> (Moving, Scripted) {
    let engine =
        crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default()).expect("V8 engine");
    let fixture = TestModules::with_upstream(Arc::new(engine), upstream).await;
    let tool = fixture.user_tool("user:bodies", "Reconnect bodies").await;
    let private_tools = || {
        PrivateToolModule::new(
            fixture.database.clone(),
            fixture.workspaces.clone(),
            crate::IsolateLimits::default(),
            coral_test_support::TEST_COMPILATION_TIMEOUT,
        )
        .expect("the Private Tool module")
    };
    private_tools()
        .save(
            &PrivateToolChangeContext::new(
                tool.principal.clone(),
                coral_db::UnixNanos::new(60),
                coral_workspace::TraceId::current_or_new(),
            ),
            &tool.workspace_id,
            PrivateToolSave::new(
                PrivateToolPath::parse("notes.title").expect("a path"),
                AuthoredToolDescription::parse("A tool body").expect("a description"),
                AuthoredToolSource::parse(
                    "export default async function title() {
  return (await coral.providers.weather.getForecast()).data.temperature;
}",
                )
                .expect("a source"),
            )
            .declaring_default(default),
        )
        .await
        .expect("save the Private Tool");
    let modules = modules(&fixture, |runs| {
        let index =
            coral_search::SearchIndex::new(fixture.database.clone(), Duration::from_secs(3_600));
        runs.with_private_tools(crate::PrivateTools::new(private_tools(), index))
    });
    start_moving(
        fixture,
        modules,
        tool,
        "return await coral.private.notes.title({});",
    )
    .await
}

fn title() -> request::Kind {
    request::Kind::Perform(Perform {
        call_path: "coral.private.notes.title".to_owned(),
        arguments_json: "{}".to_owned(),
        location: None,
        parent: None,
    })
}

fn returned(perform: u64, json: &str) -> request::Kind {
    request::Kind::Complete(Complete {
        perform,
        outcome: Some(complete::Outcome::ReturnedJson(json.to_owned())),
    })
}

fn permitted(answer: &InvocationResult) -> bool {
    matches!(
        answer.outcome,
        Some(invocation_result::Outcome::Permitted(_))
    )
}

/// Reads request `number`'s answer from `stream`.
async fn answer(stream: &mut Scripted, number: u64) -> InvocationResult {
    let read = stream.until(|read| !answers(read, number).is_empty()).await;
    only_answer(&read, number).clone()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_performs_body_keeps_running_across_a_reopen_unless_its_end_was_lost() {
    // Whether the body's `Complete` committed on the loop before.
    for lost in [false, true] {
        let upstream = SequencedUpstream::new(HttpOutcome::new(
            200,
            serde_json::json!({"temperature": 21}),
        ));
        let (moving, mut stream) = moving_bodies(upstream.clone(), None).await;
        stream.request(1, title());
        assert!(permitted(&answer(&mut stream, 1).await), "lost: {lost}");
        // The body's call is in flight when the stream drops.
        stream.request(2, forecast(Some(1)));
        upstream.until_sent(1).await;
        let (watermark, awaited) = if lost {
            stream.request(3, returned(1, "21"));
            (3, vec![(2, 0), (3, 1)])
        } else {
            (2, vec![(2, 0)])
        };
        stream
            .until(|read| read.contains(&committed(watermark)))
            .await;
        drop(stream);
        let mut stream = reopen(&moving, &awaited);
        assert_eq!(stream.next().await, committed(watermark), "lost: {lost}");
        assert!(
            failed_with(&answer(&mut stream, 2).await, "upstream_incomplete"),
            "lost: {lost}"
        );
        if !lost {
            stream.request(3, returned(1, "21"));
        }
        let completed = answer(&mut stream, 3).await;
        let perform_kind = if lost {
            assert!(
                failed_with(&completed, "upstream_incomplete"),
                "{completed:?}"
            );
            InvocationResultKind::Indeterminate
        } else {
            assert_eq!(
                completed.outcome,
                Some(invocation_result::Outcome::ResultJson("21".to_owned()))
            );
            InvocationResultKind::UpstreamOutcome
        };
        stream.request(4, finish_with("21"));
        stream
            .until(|read| read.contains(&ended(WireEnd::Succeeded)))
            .await;
        assert_eq!(
            result_kinds(&moving.fixture, &moving.tool, &moving.run_id).await,
            [
                Some(perform_kind),
                Some(InvocationResultKind::Indeterminate)
            ],
            "lost: {lost}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_reopened_stream_refuses_a_call_under_an_ended_perform_and_breaks_on_a_wrong_number() {
    // A request that breaks the protocol after the reopen, by its number:
    // a call under request 2, which was an Emission, and a request
    // numbered 0.
    for (number, broken) in [
        (5, forecast(Some(2))),
        (0, emit(&serde_json::json!({"late": true}))),
    ] {
        let upstream = SequencedUpstream::new(HttpOutcome::new(
            200,
            serde_json::json!({"temperature": 21}),
        ));
        let (moving, mut stream) = moving_bodies(upstream, None).await;
        stream.request(1, title());
        assert!(permitted(&answer(&mut stream, 1).await), "{number}");
        stream.request(2, emit(&serde_json::json!({"early": true})));
        stream.request(3, returned(1, "21"));
        // The watermark can pass request 3 before the body's end is
        // durable, so wait for request 3's answer too: it comes once the
        // end is durable, and the reopen then awaits no answer.
        stream
            .until(|read| !answers(read, 3).is_empty() && read.contains(&committed(3)))
            .await;
        drop(stream);
        let mut stream = reopen(&moving, &[]);
        assert_eq!(stream.next().await, committed(3), "{number}");
        stream.request(4, forecast(Some(1)));
        assert!(
            failed_with(&answer(&mut stream, 4).await, "parent_invocation_ended"),
            "{number}"
        );
        stream.request(number, broken);
        let closing = stream.closed().await;
        assert!(
            matches!(closing, coral_run_executor::Closing::InvalidArgument(_)),
            "{number}: {closing:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_perform_waiting_for_its_approval_is_permitted_after_a_reopen() {
    let upstream = SequencedUpstream::new(HttpOutcome::new(
        200,
        serde_json::json!({"temperature": 21}),
    ));
    upstream.release_all();
    let (moving, mut stream) =
        moving_bodies(upstream, Some(ToolAccessState::ApprovalRequired)).await;
    stream.request(1, title());
    let requests = approval_requests(&moving.fixture, &moving.tool, &moving.run_id, 1).await;
    let [waiting] = requests.as_slice() else {
        panic!("one Approval Request: {requests:?}");
    };
    stream.until(|read| read.contains(&committed(1))).await;
    drop(stream);
    decide_without_a_loop(&moving.fixture, &moving.tool, waiting.id.clone()).await;
    let mut stream = reopen(&moving, &[(1, 0)]);
    assert_eq!(stream.next().await, committed(1));
    assert!(permitted(&answer(&mut stream, 1).await));
    stream.request(2, forecast(Some(1)));
    assert!(matches!(
        answer(&mut stream, 2).await.outcome,
        Some(invocation_result::Outcome::ResultJson(_))
    ));
    stream.request(3, returned(1, "21"));
    assert_eq!(
        answer(&mut stream, 3).await.outcome,
        Some(invocation_result::Outcome::ResultJson("21".to_owned()))
    );
    stream.request(4, finish_with("21"));
    stream
        .until(|read| read.contains(&ended(WireEnd::Succeeded)))
        .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_reopen_ends_a_run_whose_termination_the_loop_before_recorded() {
    let fixture = TestModules::new(Arc::new(ImmediateEngine)).await;
    let runs = fixture.runs.clone();
    let (workspace_id, principal) = fixture.user_scope("user:ending", "Reopen end").await;
    let accepted = runs
        .start(&principal, &workspace_id, submission("return 1;"))
        .await
        .expect("accept a Run");
    let run_id = accepted.run_id().clone();
    let claim = take(&runs).await;
    let mut stream = Scripted::open(&runs, &run_id.to_string(), &claim);
    stream.start().await;
    let canceled = {
        let (runs, principal) = (runs.clone(), principal.clone());
        let (workspace_id, run_id) = (workspace_id.clone(), run_id.clone());
        tokio::spawn(async move {
            runs.cancel(&principal, &workspace_id, &run_id, &immediate(None, 100))
                .await
        })
    };
    // The stream drops once `Terminate` arrives and the cancellation is
    // recorded, before the Program's `Finish`. `Terminate` goes out before
    // the chunk that records it commits.
    stream
        .until(|read| {
            read.iter()
                .any(|message| matches!(message, run_response::Message::Terminate(_)))
        })
        .await;
    wait_until_async(async || {
        retained_events(&runs, &principal, &workspace_id, &run_id)
            .await
            .iter()
            .any(|event| matches!(event.kind(), RunEventKind::CancellationRequested { .. }))
    })
    .await;
    drop(stream);
    let mut stream = Scripted::open(&runs, &run_id.to_string(), &claim);
    assert_eq!(stream.next().await, ended(WireEnd::Canceled));
    canceled
        .await
        .expect("the cancellation's task")
        .expect("the Run is canceled");
}
