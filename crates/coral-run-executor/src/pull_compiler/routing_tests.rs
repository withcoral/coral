//! The adapter's routing: which pull stream serves a call, and how a call
//! waits when none has room.

use std::sync::Arc;
use std::thread;
use std::time::Duration;

use coral_test_support::TEST_COMPILATION_TIMEOUT;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use super::{COMPILE_WAIT, PullStreamCompiler, ROOM_WAIT_RESERVE};
use crate::local::tests::FakeStarter;
use crate::pull::tests::{compiled, up};
use crate::pull::{Credentials, dialed_admission};
use crate::wire::v1::{PullRequest, PullResponse, Room, pull_request, pull_response};
use crate::{CompileError, CompilerClient, LocalRunExecutors, PullStreams, StreamId, Unavailable};

type Sent = mpsc::UnboundedReceiver<Result<PullResponse, tonic::Status>>;

/// A dialed stream with `free` units of room, and what Coral sends on it.
fn open(streams: &PullStreams, free: u32) -> (StreamId, Sent) {
    let (outbound, sent) = mpsc::unbounded_channel();
    let stream = streams.insert(
        "a-run-executor".to_owned(),
        dialed_admission(),
        free,
        outbound,
    );
    (stream, sent)
}

/// Answers the next `Compile` sent on `stream`, then reports `free` when
/// there is any to report.
fn answer(streams: &PullStreams, stream: StreamId, sent: &mut Sent, free: Option<u32>) {
    let Some(Ok(PullResponse {
        message: Some(pull_response::Message::Compile(compile)),
    })) = sent.blocking_recv()
    else {
        panic!("a Compile");
    };
    streams
        .receive(stream, up(compiled(compile.number)))
        .unwrap();
    if let Some(free) = free {
        let room = PullRequest {
            message: Some(pull_request::Message::Room(Room { free })),
        };
        streams.receive(stream, room).unwrap();
    }
}

fn compile(caller: &dyn CompilerClient) -> Result<String, CompileError> {
    let program = caller.compile(
        TEST_COMPILATION_TIMEOUT,
        "return 42;",
        &[],
        &coral_v8::BuiltinTools::default(),
        None,
    )?;
    Ok(program.javascript().to_owned())
}

/// Answers the next `Compile` sent on `stream` from a blocking worker, as
/// a Run Executor does, then reports `free` when there is any to report.
/// Returns what was sent.
async fn answered(
    streams: &PullStreams,
    stream: StreamId,
    mut sent: Sent,
    free: Option<u32>,
) -> Sent {
    let streams = streams.clone();
    tokio::task::spawn_blocking(move || {
        answer(&streams, stream, &mut sent, free);
        sent
    })
    .await
    .expect("the answer")
}

/// Compiles through `caller` on a blocking worker, as admission does.
fn compiling(caller: Arc<dyn CompilerClient>) -> JoinHandle<Result<String, CompileError>> {
    tokio::task::spawn_blocking(move || compile(caller.as_ref()))
}

/// Waits for a compiler as admission does: the held client.
async fn held(caller: &PullStreamCompiler) -> Arc<dyn CompilerClient> {
    caller
        .ready()
        .await
        .expect("a compiler")
        .expect("a held unit")
}

/// [`held`] in a task of its own, so the test acts while it waits.
fn held_later(caller: &PullStreamCompiler) -> JoinHandle<Arc<dyn CompilerClient>> {
    let caller = caller.clone();
    tokio::spawn(async move { held(&caller).await })
}

fn free(streams: &PullStreams, stream: StreamId) -> u32 {
    let held = streams.held();
    held.iter().find(|held| held.stream == stream).unwrap().free
}

#[tokio::test(start_paused = true)]
async fn a_call_takes_a_unit_of_the_stream_with_the_most_room_until_its_answer() {
    let streams = PullStreams::default();
    let (low, _low_sent) = open(&streams, 1);
    let (most, sent) = open(&streams, 3);
    let (mid, _mid_sent) = open(&streams, 2);
    let caller = PullStreamCompiler::new(streams.clone(), None);
    let first = held(&caller).await;
    assert_eq!(
        [
            free(&streams, low),
            free(&streams, most),
            free(&streams, mid)
        ],
        [1, 2, 2],
        "one unit of the stream with the most room held"
    );
    let calling = compiling(first);
    answered(&streams, most, sent, None).await;
    assert_eq!(calling.await.unwrap().as_deref(), Ok("42"));
    assert_eq!(
        free(&streams, most),
        2,
        "an answer alone frees nothing: the Run Executor's room report does"
    );
}

#[tokio::test(start_paused = true)]
async fn a_call_waits_for_a_pull_stream_and_fails_past_the_bound() {
    // No stream yet: the call is served by the one that opens.
    let streams = PullStreams::default();
    let caller = PullStreamCompiler::new(streams.clone(), None);
    let waiting = held_later(&caller);
    tokio::time::sleep(COMPILE_WAIT / 2).await;
    assert!(!waiting.is_finished(), "it waits for a stream");
    let (stream, sent) = open(&streams, 1);
    let calling = compiling(waiting.await.unwrap());
    answered(&streams, stream, sent, Some(1)).await;
    assert_eq!(calling.await.unwrap().as_deref(), Ok("42"));

    // A stream with no room is no compiler: past the bound, none is.
    let streams = PullStreams::default();
    let (_full, _sent) = open(&streams, 0);
    let caller = PullStreamCompiler::new(streams, None);
    let error = caller
        .ready()
        .await
        .map(|held| held.is_some())
        .expect_err("no compiler");
    assert_eq!(error, CompileError::Unavailable(Unavailable::NoCompiler));
    assert_eq!(error.code(), "compiler_unavailable");
    assert_eq!(
        coral_v8::CompileFailure::from(error),
        coral_v8::CompileFailure::NoCompiler
    );
}

// Catches: a call no stream took that fails as a compile timeout, which
// rejects the source, though no compiler judged it.
#[test]
fn a_call_no_stream_takes_in_its_time_fails_as_no_compiler() {
    let streams = PullStreams::default();
    let (_full, _sent) = open(&streams, 0);
    let caller = PullStreamCompiler::new(streams, None);
    // Less time left than a call keeps when it waits for room: it looks once.
    let result = caller.compile(
        ROOM_WAIT_RESERVE / 2,
        "return 42;",
        &[],
        &coral_v8::BuiltinTools::default(),
        None,
    );
    assert_eq!(
        result.map(|_compiled| ()),
        Err(CompileError::Unavailable(Unavailable::NoCompiler))
    );
}

#[test]
fn a_stream_that_ends_fails_its_calls_in_flight() {
    let streams = PullStreams::default();
    let (stream, mut sent) = open(&streams, 1);
    let caller = PullStreamCompiler::new(streams.clone(), None);
    let calling = thread::spawn(move || compile(&caller));
    assert!(sent.blocking_recv().is_some(), "a Compile");
    streams.end(stream, None);
    assert_eq!(
        calling.join().unwrap(),
        Err(CompileError::Unavailable(Unavailable::Stopped))
    );
}

#[tokio::test(start_paused = true)]
async fn a_call_with_no_room_starts_a_local_child_that_serves_it() {
    let streams = PullStreams::default();
    let credentials = Credentials::default();
    let starter = FakeStarter::default();
    let children =
        LocalRunExecutors::start(starter.clone(), 1, credentials.clone(), streams.clone());
    let caller = PullStreamCompiler::new(streams.clone(), Some(children));
    let waiting = held_later(&caller);

    let bearer = loop {
        if let Some(bearer) = starter.bearers().first().cloned() {
            break bearer;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    let (outbound, sent) = mpsc::unbounded_channel();
    let admitted = credentials.admit(Some(&bearer)).expect("the child's token");
    let child = streams.insert(uuid::Uuid::new_v4().to_string(), admitted, 1, outbound);
    let calling = compiling(waiting.await.unwrap());
    answered(&streams, child, sent, Some(1)).await;
    assert_eq!(calling.await.unwrap().as_deref(), Ok("42"));
    assert_eq!(starter.bearers().len(), 1, "one child started");
}

// Catches: callers that all see one free unit, so the losers wait for room
// inside their compile time instead of the wait for a compiler.
#[tokio::test(start_paused = true)]
async fn ready_holds_the_unit_for_its_caller_until_its_first_call() {
    let streams = PullStreams::default();
    let (stream, sent) = open(&streams, 1);
    let caller = PullStreamCompiler::new(streams.clone(), None);
    let first = held(&caller).await;
    assert_eq!(free(&streams, stream), 0, "held for the first caller");

    // A second caller waits for room: the held unit is not its.
    let waiting = held_later(&caller);
    tokio::time::sleep(COMPILE_WAIT / 2).await;
    assert!(!waiting.is_finished(), "the second caller waits");

    // The first call spends the held unit; its room report frees it for
    // the second.
    let calling = compiling(first);
    let sent = answered(&streams, stream, sent, Some(1)).await;
    assert_eq!(calling.await.unwrap().as_deref(), Ok("42"));
    let calling = compiling(waiting.await.unwrap());
    answered(&streams, stream, sent, Some(1)).await;
    assert_eq!(calling.await.unwrap().as_deref(), Ok("42"));

    // A held unit no call spends is freed when its client drops.
    let unused = held(&caller).await;
    assert_eq!(free(&streams, stream), 0);
    drop(unused);
    assert_eq!(free(&streams, stream), 1);
}

// Catches: a wait for a compiler that blocks its thread, so nothing else on
// it runs until the wait ends.
#[tokio::test(flavor = "current_thread")]
async fn ready_waits_for_room_without_holding_its_thread() {
    let streams = PullStreams::default();
    let caller = PullStreamCompiler::new(streams.clone(), None);
    // The stream opens on the thread the wait runs on.
    let opening = async {
        tokio::task::yield_now().await;
        open(&streams, 1)
    };
    let (held, (stream, _sent)) = tokio::join!(caller.ready(), opening);
    let _held = held.expect("a compiler").expect("a held unit");
    assert_eq!(free(&streams, stream), 0);
}
