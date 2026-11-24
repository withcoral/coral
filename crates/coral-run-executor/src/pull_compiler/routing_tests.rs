//! The adapter's routing: which pull stream serves a call, and how a call
//! waits when none has room.

use std::sync::Arc;
use std::time::Duration;

use coral_test_support::TEST_COMPILATION_TIMEOUT;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use super::{COMPILE_WAIT, PullStreamCompiler, ROOM_WAIT_RESERVE};
use crate::local::tests::FakeStarter;
use crate::pull::tests::{Sent, busy_stream, compiled, dialed_stream as open, up};
use crate::pull::{COMPILE_ROOM, Credentials};
use crate::wire::v1::{PullRequest, PullResponse, Room, pull_request, pull_response};
use crate::{CompileError, CompilerClient, LocalRunExecutors, PullStreams, StreamId, Unavailable};

/// Answers the next `Compile` sent on `stream`, as a Run Executor does,
/// then reports `free` when there is any to report.
async fn answer(streams: &PullStreams, stream: StreamId, sent: &mut Sent, free: Option<u32>) {
    let Some(Ok(PullResponse {
        message: Some(pull_response::Message::Compile(compile)),
    })) = sent.recv().await
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

async fn compile(caller: &dyn CompilerClient) -> Result<String, CompileError> {
    let program = caller
        .compile(
            TEST_COMPILATION_TIMEOUT,
            "return 42;",
            &[],
            &coral_v8::BuiltinTools::default(),
            None,
        )
        .await?;
    Ok(program.javascript().to_owned())
}

/// Compiles through `caller` in a task of its own, as admission does.
fn compiling(caller: Arc<dyn CompilerClient>) -> JoinHandle<Result<String, CompileError>> {
    tokio::spawn(async move { compile(caller.as_ref()).await })
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

fn rooms(streams: &PullStreams, stream: StreamId) -> (u32, u32) {
    let held = streams.held();
    let held = held.iter().find(|held| held.stream == stream).unwrap();
    (held.free, streams.compile_room(stream))
}

// Catches: a compile that spends room for Runs, so a full set of Runs stops
// it; compile room that its answer does not free; or a tie that ignores room
// for Runs.
#[tokio::test(start_paused = true)]
async fn a_call_takes_a_unit_of_compile_room_until_its_answer_and_no_room_for_runs() {
    let streams = PullStreams::default();
    let (full, mut sent) = open(&streams, 0);
    let (low, _low_sent) = open(&streams, 1);
    streams.hold_compile(low).unwrap();
    let caller = PullStreamCompiler::new(streams.clone(), None);
    let first = held(&caller).await;
    let free = |stream| rooms(&streams, stream);
    let room = COMPILE_ROOM;
    // The most compile room wins over room for Runs.
    assert_eq!(free(full), (0, room - 1), "a full Run Executor compiles");
    assert_eq!(free(low), (1, room - 1));
    let calling = compiling(first);
    answer(&streams, full, &mut sent, None).await;
    assert_eq!(calling.await.unwrap().as_deref(), Ok("42"));
    assert_eq!(free(full), (0, room), "the answer frees its unit");

    // A tie in compile room goes to the stream with more room for Runs:
    // `full` reports none, as a full or a retiring Run Executor does.
    streams.let_go(low);
    let _second = held(&caller).await;
    assert_eq!(free(low), (1, room - 1), "the tie goes to room for Runs");
    assert_eq!(free(full), (0, room));
}

/// What the stream after the ones that end does with the call.
#[derive(Clone, Copy)]
enum Next {
    /// It is sent nothing.
    Untouched,
    /// It answers.
    Answers,
    /// It takes the call and never answers.
    Silent,
}

/// Catches: a call that fails when its stream ends before it answers,
/// although another stream has room; one routed more than once, or past the
/// room bound; or one that then fails as anything but a stopped compiler, a
/// timeout on the next stream included.
#[tokio::test(start_paused = true)]
async fn a_call_whose_stream_ends_first_is_routed_once_more() {
    // A name, each stream's room, how many end before they answer, how long
    // after its `Compile` the first ends, the next stream, and the result.
    type Case = (
        &'static str,
        &'static [u32],
        usize,
        Duration,
        Next,
        Result<&'static str, CompileError>,
    );
    let stopped = Err(CompileError::Unavailable(Unavailable::Stopped));
    let bound = TEST_COMPILATION_TIMEOUT.saturating_sub(ROOM_WAIT_RESERVE);
    let cases: [Case; 5] = [
        (
            "another stream",
            &[2, 1],
            1,
            Duration::ZERO,
            Next::Answers,
            Ok("42"),
        ),
        (
            "a silent next stream",
            &[2, 1],
            1,
            Duration::ZERO,
            Next::Silent,
            stopped.clone(),
        ),
        (
            "past the room bound",
            &[2, 1],
            1,
            bound,
            Next::Untouched,
            stopped.clone(),
        ),
        (
            "no other stream",
            &[1],
            1,
            Duration::ZERO,
            Next::Untouched,
            stopped.clone(),
        ),
        (
            "two that end",
            &[3, 2, 1],
            2,
            Duration::ZERO,
            Next::Untouched,
            stopped,
        ),
    ];
    for (name, rooms, ending, after, next, expected) in cases {
        let streams = PullStreams::default();
        let mut opened: Vec<_> = rooms.iter().map(|&free| open(&streams, free)).collect();
        let calling = compiling(Arc::new(PullStreamCompiler::new(streams.clone(), None)));
        // Each goes to the stream with the most room left.
        for (index, (stream, sent)) in opened.iter_mut().take(ending).enumerate() {
            assert!(sent.recv().await.is_some(), "{name}: a Compile");
            if index == 0 {
                tokio::time::advance(after).await;
            }
            streams.end(*stream, None);
        }
        let mut rest: Vec<_> = opened.iter_mut().skip(ending).collect();
        match (next, rest.first_mut()) {
            (Next::Answers, Some((stream, sent))) => {
                let answered = answer(&streams, *stream, sent, None);
                let answered = tokio::time::timeout(TEST_COMPILATION_TIMEOUT, answered).await;
                answered.expect("the next stream takes the call");
            }
            (Next::Silent, Some((_, sent))) => {
                let taken = tokio::time::timeout(TEST_COMPILATION_TIMEOUT, sent.recv()).await;
                assert!(
                    matches!(taken, Ok(Some(_))),
                    "{name}: the next stream takes it"
                );
            }
            _ => {}
        }
        let returned = calling.await.unwrap();
        let returned = returned.as_deref().map_err(Clone::clone);
        assert_eq!(returned, expected, "{name}");
        for (_, sent) in rest {
            assert!(sent.try_recv().is_err(), "{name}: sent no more");
        }
    }
}

#[tokio::test(start_paused = true)]
async fn a_call_waits_for_a_pull_stream_and_fails_past_the_bound() {
    // No stream yet: the call is served by the one that opens.
    let streams = PullStreams::default();
    let caller = PullStreamCompiler::new(streams.clone(), None);
    let waiting = held_later(&caller);
    tokio::time::sleep(COMPILE_WAIT / 2).await;
    assert!(!waiting.is_finished(), "it waits for a stream");
    let (stream, mut sent) = open(&streams, 1);
    let calling = compiling(waiting.await.unwrap());
    answer(&streams, stream, &mut sent, Some(1)).await;
    assert_eq!(calling.await.unwrap().as_deref(), Ok("42"));

    // A busy compiler is no compiler: past the bound, none is.
    let streams = PullStreams::default();
    let (_busy, _sent) = busy_stream(&streams);
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
// rejects the source, though no compiler judged it; or that says so only
// once its caller's own deadline has ended the wait.
#[tokio::test(start_paused = true)]
async fn a_call_no_stream_takes_in_its_time_fails_as_no_compiler_before_its_deadline() {
    let streams = PullStreams::default();
    let (_busy, _sent) = busy_stream(&streams);
    let caller = PullStreamCompiler::new(streams, None);
    let time_left = Duration::from_secs(2);
    let started = tokio::time::Instant::now();
    let result = caller
        .compile(
            time_left,
            "return 42;",
            &[],
            &coral_v8::BuiltinTools::default(),
            None,
        )
        .await;
    assert_eq!(
        result.map(|_compiled| ()),
        Err(CompileError::Unavailable(Unavailable::NoCompiler))
    );
    assert_eq!(
        Some(started.elapsed()),
        time_left.checked_sub(ROOM_WAIT_RESERVE),
        "it stops waiting a reserve before its deadline"
    );
}

#[tokio::test(start_paused = true)]
async fn a_call_with_no_room_starts_a_local_child_that_serves_it() {
    let streams = PullStreams::default();
    let credentials = Credentials::default();
    let starter = FakeStarter::default();
    let children = LocalRunExecutors::start(
        starter.clone(),
        1,
        credentials.clone(),
        streams.clone(),
        None,
    );
    let caller = PullStreamCompiler::new(streams.clone(), Some(children));
    let waiting = held_later(&caller);

    let bearer = loop {
        if let Some(bearer) = starter.bearers().first().cloned() {
            break bearer;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    let (outbound, mut sent) = mpsc::unbounded_channel();
    let admitted = credentials.admit(Some(&bearer)).expect("the child's token");
    let child = streams.insert(uuid::Uuid::new_v4().to_string(), admitted, 1, outbound);
    let calling = compiling(waiting.await.unwrap());
    answer(&streams, child, &mut sent, Some(1)).await;
    assert_eq!(calling.await.unwrap().as_deref(), Ok("42"));
    assert_eq!(starter.bearers().len(), 1, "one child started");
}

// Catches: callers that all see one free unit, so the losers wait for room
// inside their compile time instead of the wait for a compiler.
#[tokio::test(start_paused = true)]
async fn ready_holds_the_unit_for_its_caller_until_its_first_call() {
    let streams = PullStreams::default();
    let (stream, mut sent) = open(&streams, 1);
    let caller = PullStreamCompiler::new(streams.clone(), None);
    let mut firsts = Vec::new();
    for _unit in 0..COMPILE_ROOM {
        firsts.push(held(&caller).await);
    }
    assert_eq!(
        streams.compile_room(stream),
        0,
        "held for the first callers"
    );

    // Another caller waits for room: the held units are not its.
    let waiting = held_later(&caller);
    tokio::time::sleep(COMPILE_WAIT / 2).await;
    assert!(!waiting.is_finished(), "the next caller waits");

    // A first call spends its held unit, and its answer frees it for the
    // caller that waits.
    let calling = compiling(firsts.pop().unwrap());
    answer(&streams, stream, &mut sent, None).await;
    assert_eq!(calling.await.unwrap().as_deref(), Ok("42"));
    let calling = compiling(waiting.await.unwrap());
    answer(&streams, stream, &mut sent, None).await;
    assert_eq!(calling.await.unwrap().as_deref(), Ok("42"));

    // A held unit no call spends is freed when its client drops.
    assert_eq!(streams.compile_room(stream), 1);
    drop(firsts);
    assert_eq!(streams.compile_room(stream), COMPILE_ROOM);
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
    assert_eq!(streams.compile_room(stream), COMPILE_ROOM - 1);
}
