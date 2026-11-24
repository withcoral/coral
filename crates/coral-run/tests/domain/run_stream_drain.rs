//! The Shutdown Drain moves a Coral Instance's Run streams, not its Runs
//! (ADR 0027, Reconnect): a draining Coral Instance opens no stream, holds
//! new dispatches, ends each open stream cleanly once no dispatch is in
//! flight on it, and records no Run's end; the Run goes on on another. A
//! local child's stream can reopen only on its parent: its Run keeps
//! dispatching, and ends Interrupted at the drain's length.
use super::*;
use coral_run_executor::wire::v1::{RunEnd as WireEnd, invocation_result, run_response};
use coral_run_executor::{Closing, PullStreams};

use super::run_stream_reconnect::{
    Moving, answers, failed_with, finish_with, forecast, only_answer, reopen, result_kinds,
    start_moving, start_moving_for,
};
use super::run_streams::{Scripted, committed, emit, ended, shape, take};
use crate::InvocationResultKind;
use crate::run_repository::Taker;
use coral_run_executor::Kind;
use coral_run_executor::wire::v1::TerminateReason;

/// A drain far longer than any wait in these tests: a shutdown that returns
/// within one returned before its drain was out.
const LONG_DRAIN: Duration = Duration::from_secs(600);

/// The Coral Instance that drains, with `drain` as its Shutdown Drain, and
/// the one its streams move to.
fn two_instances(fixture: &TestModules, drain: Duration) -> (RunModule, RunModule) {
    let mut first = fixture.runs.clone();
    first.shutdown_drain = crate::ShutdownDrain::new(drain);
    (first, fixture.next_process())
}

/// A started Run on a stream of the first of two Coral Instances, whose
/// weather Tool sends through `upstream`.
async fn streaming(upstream: Arc<SequencedUpstream>, drain: Duration) -> (Moving, Scripted) {
    streaming_for(upstream, drain, None).await
}

/// [`streaming`], with the Run taken for `taker`, such as a local child.
async fn streaming_for(
    upstream: Arc<SequencedUpstream>,
    drain: Duration,
    taker: Option<&Taker<'_>>,
) -> (Moving, Scripted) {
    let fixture = TestModules::with_upstream(Arc::new(ImmediateEngine), upstream).await;
    let tool = fixture.user_tool("user:drain", "Drain").await;
    let instances = two_instances(&fixture, drain);
    Box::pin(start_moving_for(
        fixture,
        instances,
        tool,
        "return 1;",
        taker,
    ))
    .await
}

fn held_upstream() -> Arc<SequencedUpstream> {
    SequencedUpstream::new(HttpOutcome::new(
        200,
        serde_json::json!({"temperature": 18}),
    ))
}

/// Starts `runs`' Shutdown Drain; the rest runs on its own task.
fn shut_down(runs: &RunModule) -> tokio::task::JoinHandle<()> {
    tokio::spawn(runs.shutdown())
}

/// Finishes the Run on its reopened `stream` with request `number`, and
/// returns its Events once it succeeded.
async fn finish_on(moving: &Moving, stream: &mut Scripted, number: u64) -> Vec<RunEvent> {
    stream.request(number, finish_with("1"));
    stream
        .until(|read| read.contains(&ended(WireEnd::Succeeded)))
        .await;
    let terminal = wait_for_terminal(
        &moving.second,
        &moving.tool.principal,
        &moving.tool.workspace_id,
        &moving.run_id,
    )
    .await;
    terminal.events().to_vec()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_draining_stream_waits_for_its_dispatch_then_moves_and_the_run_finishes_elsewhere() {
    let upstream = held_upstream();
    let (moving, mut stream) = streaming(upstream.clone(), LONG_DRAIN).await;
    stream.request(1, emit(&serde_json::json!({"step": 1})));
    stream.request(2, forecast(None));
    upstream.until_sent(1).await;
    stream.until(|read| read.contains(&committed(2))).await;

    let shutdown = shut_down(&moving.first);
    // The drain keeps committing the stream's requests; request 4's call
    // waits, `pending`, for the Coral Instance the stream moves to.
    stream.request(3, emit(&serde_json::json!({"step": 3})));
    stream.request(4, forecast(None));
    stream.until(|read| read.contains(&committed(4))).await;
    assert!(moving.first.owners.contains(&moving.run_id), "still held");

    upstream.release_all();
    let (before, closing) = stream.read_to_close().await;
    assert_eq!(closing, Closing::Moved);
    assert!(answers(&before, 4).is_empty(), "{before:?}");
    tokio::time::timeout(EVENTUALLY, shutdown)
        .await
        .expect("the drain ends once its stream moved")
        .expect("the shutdown task");

    // The answer to 2 goes out as its result is applied, and may cross the
    // move: then the Run Executor waits on it still.
    let awaited: &[(u64, u64)] = if answers(&before, 2).is_empty() {
        &[(2, 0), (4, 0)]
    } else {
        &[(4, 0)]
    };
    let mut stream = reopen(&moving, awaited);
    assert_eq!(stream.next().await, committed(4));
    let after = stream.until(|read| !answers(read, 4).is_empty()).await;
    let returned = |read: &[run_response::Message], number| {
        matches!(
            &only_answer(read, number).outcome,
            Some(invocation_result::Outcome::ResultJson(_))
        )
    };
    let read = [before, after].concat();
    assert!(
        returned(&read, 2),
        "the dispatch in flight returned: {read:?}"
    );
    assert!(
        returned(&read, 4),
        "the held call went on after the reopen: {read:?}"
    );
    let events = finish_on(&moving, &mut stream, 5).await;
    assert_eq!(
        shape(&events),
        [r#"emitted {"step":1}"#, r#"emitted {"step":3}"#]
    );
    assert_eq!(
        result_kinds(&moving.fixture, &moving.tool, &moving.run_id).await,
        [
            Some(InvocationResultKind::UpstreamOutcome),
            Some(InvocationResultKind::UpstreamOutcome)
        ]
    );
    assert_eq!(upstream.sends.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn a_draining_stream_with_nothing_in_flight_moves_at_once() {
    let (moving, mut stream) = streaming(held_upstream(), LONG_DRAIN).await;
    stream.request(1, emit(&serde_json::json!({"step": 1})));
    stream.until(|read| read.contains(&committed(1))).await;

    let shutdown = shut_down(&moving.first);
    assert_eq!(stream.closing().await, Closing::Moved);
    tokio::time::timeout(EVENTUALLY, shutdown)
        .await
        .expect("the drain ends once its stream moved")
        .expect("the shutdown task");
    let run = moving
        .fixture
        .run_record(&moving.tool.workspace_id, &moving.run_id)
        .await;
    assert_eq!(run.state, RunState::Running, "the drain ends no Run");

    let mut stream = reopen(&moving, &[]);
    assert_eq!(stream.next().await, committed(1));
    let events = finish_on(&moving, &mut stream, 2).await;
    assert_eq!(shape(&events), [r#"emitted {"step":1}"#]);
}

#[tokio::test]
async fn a_taken_run_whose_first_open_hits_a_draining_instance_starts_on_another() {
    let fixture = TestModules::new(Arc::new(ImmediateEngine)).await;
    let (first, second) = two_instances(&fixture, LONG_DRAIN);
    let (workspace_id, principal) = fixture.user_scope("user:drain-take", "Drain take").await;
    let accepted = first
        .start(&principal, &workspace_id, submission("return 1;"))
        .await
        .expect("accept a Run");
    let run_id = accepted.run_id().clone();
    let claim = take(&first).await;
    let taken = fixture.run_record(&workspace_id, &run_id).await;

    first.shutdown().await;
    let mut refused = Scripted::open(&first, &run_id.to_string(), &claim);
    assert!(matches!(refused.closing().await, Closing::Unavailable(_)));
    assert_eq!(
        fixture.run_record(&workspace_id, &run_id).await,
        taken,
        "the refused open changed nothing; the take stands"
    );

    let mut stream = Scripted::open(&second, &run_id.to_string(), &claim);
    stream.start().await;
    stream.request(1, finish_with("1"));
    stream
        .until(|read| read.contains(&ended(WireEnd::Succeeded)))
        .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn at_the_drains_length_a_stream_moves_with_its_dispatch_in_flight() {
    let upstream = held_upstream();
    let (moving, mut stream) = streaming(upstream.clone(), Duration::ZERO).await;
    stream.request(1, forecast(None));
    upstream.until_sent(1).await;
    stream.until(|read| read.contains(&committed(1))).await;

    let shutdown = shut_down(&moving.first);
    let (read, closing) = stream.read_to_close().await;
    assert_eq!(closing, Closing::Moved);
    assert!(answers(&read, 1).is_empty(), "{read:?}");
    shutdown.await.expect("the shutdown task");

    // The Coral Instance it reopens on settles the call it lost.
    let mut stream = reopen(&moving, &[(1, 0)]);
    let read = stream.until(|read| !answers(read, 1).is_empty()).await;
    assert!(
        failed_with(only_answer(&read, 1), "upstream_incomplete"),
        "{read:?}"
    );
    upstream.release_all();
    finish_on(&moving, &mut stream, 2).await;
    assert_eq!(
        result_kinds(&moving.fixture, &moving.tool, &moving.run_id).await,
        [Some(InvocationResultKind::Indeterminate)]
    );
}

#[tokio::test]
async fn a_stream_moves_only_once_its_instance_holds_no_pull_stream() {
    let streams = PullStreams::default();
    let (pull, _sent) = streams.scripted("6f0c2a59-55a7-4e0b-9a2e-7c1d4f3b8e21", 1);
    let fixture = TestModules::new(Arc::new(ImmediateEngine)).await;
    let tool = fixture.user_tool("user:drain-pull", "Drain pull").await;
    let (first, second) = two_instances(&fixture, LONG_DRAIN);
    let first = first.with_pull_streams(streams.clone());
    let (moving, mut stream) = start_moving(fixture, (first, second), tool, "return 1;").await;

    let shutdown = shut_down(&moving.first);
    wait_until(|| !streams.has_room()).await;
    // The stream is idle, but a reopen now would ride the pull stream's
    // connection back here.
    stream.request(1, emit(&serde_json::json!({"step": 1})));
    stream.until(|read| read.contains(&committed(1))).await;
    assert!(moving.first.owners.contains(&moving.run_id), "still held");

    streams.end_scripted(pull);
    assert_eq!(stream.closing().await, Closing::Moved);
    tokio::time::timeout(EVENTUALLY, shutdown)
        .await
        .expect("the drain ends once its stream moved")
        .expect("the shutdown task");
}

/// A local child of the first Coral Instance.
fn local_child() -> Taker<'static> {
    Taker {
        run_executor_id: "0b7e3c4d-2a51-4f68-9d3e-5c8a1b2f7e90",
        run_executor_kind: Kind::Local,
    }
}

/// Catches: a drain that moves a local child's stream, which can reopen
/// only on its parent, or holds its new calls.
#[tokio::test(flavor = "multi_thread")]
async fn a_local_childs_run_goes_on_through_the_drain_and_finishes_here() {
    let upstream = held_upstream();
    let (moving, mut stream) =
        streaming_for(upstream.clone(), LONG_DRAIN, Some(&local_child())).await;
    stream.request(1, forecast(None));
    upstream.until_sent(1).await;

    let shutdown = shut_down(&moving.first);
    // A call made after the drain started still dispatches.
    stream.request(2, forecast(None));
    upstream.until_sent(2).await;
    upstream.release_all();
    stream
        .until(|read| !answers(read, 1).is_empty() && !answers(read, 2).is_empty())
        .await;
    stream.request(3, finish_with("1"));
    stream
        .until(|read| read.contains(&ended(WireEnd::Succeeded)))
        .await;
    tokio::time::timeout(EVENTUALLY, shutdown)
        .await
        .expect("the drain ends once the Run ended")
        .expect("the shutdown task");
    assert_eq!(
        result_kinds(&moving.fixture, &moving.tool, &moving.run_id).await,
        [
            Some(InvocationResultKind::UpstreamOutcome),
            Some(InvocationResultKind::UpstreamOutcome),
        ]
    );
}

/// Catches: a local child's Run left running past the drain's length, to
/// wait for its child's lease to lapse.
#[tokio::test(flavor = "multi_thread")]
async fn at_the_drains_length_a_local_childs_run_ends_interrupted() {
    let upstream = held_upstream();
    let (moving, mut stream) =
        streaming_for(upstream.clone(), Duration::ZERO, Some(&local_child())).await;
    stream.request(1, forecast(None));
    upstream.until_sent(1).await;

    let shutdown = shut_down(&moving.first);
    loop {
        match stream.next().await {
            run_response::Message::Terminate(terminate) => {
                assert_eq!(terminate.reason(), TerminateReason::Interrupted);
                break;
            }
            run_response::Message::Ended(_) => panic!("Ended before Terminate"),
            _ => {}
        }
    }
    shutdown.await.expect("the shutdown task");
    let run = moving
        .fixture
        .run_record(&moving.tool.workspace_id, &moving.run_id)
        .await;
    assert_eq!(run.state, RunState::Interrupted);
    upstream.release_all();
    assert_eq!(
        result_kinds(&moving.fixture, &moving.tool, &moving.run_id).await,
        [Some(InvocationResultKind::Indeterminate)]
    );
}

/// Catches: a Run submitted once the Shutdown Drain started, which this
/// Coral Instance would never serve: it is refused with `ShuttingDown`, and
/// nothing is written.
#[tokio::test]
async fn a_new_run_is_refused_once_the_shutdown_drain_started() {
    let fixture = TestModules::new(Arc::new(ImmediateEngine)).await;
    let (workspace_id, principal) = fixture.user_scope("user:refused", "Refused").await;
    let before = fixture.runs.count_runs().await.expect("count Runs");

    let drain = shut_down(&fixture.runs);
    let refused = fixture
        .runs
        .start(&principal, &workspace_id, submission("return 1;"))
        .await;

    assert!(matches!(refused, Err(Error::ShuttingDown)), "{refused:?}");
    assert_eq!(fixture.runs.count_runs().await.expect("count Runs"), before);
    drain.await.expect("the drain ends");
}
