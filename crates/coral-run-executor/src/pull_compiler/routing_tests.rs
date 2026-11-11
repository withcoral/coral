//! The adapter's routing: which pull stream serves a call, and how a call
//! waits when none has room.

use std::io;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use coral_test_support::TEST_COMPILATION_TIMEOUT;
use tokio::sync::mpsc;

use super::PullStreamCompiler;
use crate::link::Token;
use crate::pull::tests::{compiled, up};
use crate::pull::{Credentials, dialed_admission};
use crate::wire::v1::{PullRequest, PullResponse, Room, pull_request, pull_response};
use crate::{
    CompileError, CompilerClient, LocalChild, LocalRunExecutors, ProcessStarter, PullStreams,
    StreamId, Unavailable,
};

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

/// Answers the next `Compile` sent on `stream`, then reports `free`.
fn answer(streams: &PullStreams, stream: StreamId, sent: &mut Sent, free: u32) {
    let Some(Ok(PullResponse {
        message: Some(pull_response::Message::Compile(compile)),
    })) = sent.blocking_recv()
    else {
        panic!("a Compile");
    };
    streams
        .receive(stream, up(compiled(compile.number)))
        .unwrap();
    let room = PullRequest {
        message: Some(pull_request::Message::Room(Room { free })),
    };
    streams.receive(stream, room).unwrap();
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

/// Waits for a compiler as admission does, then compiles through the
/// client `ready` returns.
fn ready_then_compile(caller: &PullStreamCompiler) -> Result<String, CompileError> {
    match caller.ready()? {
        Some(held) => compile(held.as_ref()),
        None => compile(caller),
    }
}

fn free(streams: &PullStreams, stream: StreamId) -> u32 {
    let held = streams.held();
    held.iter().find(|held| held.stream == stream).unwrap().free
}

#[test]
fn a_call_takes_a_unit_of_the_stream_with_the_most_room_until_its_answer() {
    let streams = PullStreams::default();
    let (low, _low_sent) = open(&streams, 1);
    let (most, mut sent) = open(&streams, 3);
    let (mid, _mid_sent) = open(&streams, 2);
    let caller = PullStreamCompiler::new(streams.clone(), None);
    let calling = thread::spawn(move || ready_then_compile(&caller));
    let Some(Ok(PullResponse {
        message: Some(pull_response::Message::Compile(sent_compile)),
    })) = sent.blocking_recv()
    else {
        panic!("a Compile on the stream with the most room");
    };
    assert_eq!(
        [
            free(&streams, low),
            free(&streams, most),
            free(&streams, mid)
        ],
        [1, 2, 2],
        "one unit spent"
    );
    streams
        .receive(most, up(compiled(sent_compile.number)))
        .unwrap();
    let room = PullRequest {
        message: Some(pull_request::Message::Room(Room { free: 3 })),
    };
    streams.receive(most, room).unwrap();
    assert_eq!(calling.join().unwrap().as_deref(), Ok("42"));
    assert_eq!(free(&streams, most), 3, "the room report frees it");
}

#[test]
fn a_call_waits_for_a_pull_stream_and_fails_past_the_bound() {
    // No stream yet: the call is served by the one that opens.
    let streams = PullStreams::default();
    let caller = PullStreamCompiler::new(streams.clone(), None);
    let calling = thread::spawn(move || ready_then_compile(&caller));
    thread::sleep(Duration::from_millis(100));
    let (stream, mut sent) = open(&streams, 1);
    answer(&streams, stream, &mut sent, 1);
    assert_eq!(calling.join().unwrap().as_deref(), Ok("42"));

    // A stream with no room is no compiler: past the bound, none is.
    let streams = PullStreams::default();
    let (_full, _sent) = open(&streams, 0);
    let caller = PullStreamCompiler::waiting(streams, None, Duration::from_millis(200));
    let none = caller.ready().map(|held| held.is_some());
    assert_eq!(
        none,
        Err(CompileError::Unavailable(Unavailable::NoCompiler))
    );
    assert_eq!(none.unwrap_err().code(), "compiler_unavailable");
    assert_eq!(
        coral_v8::CompileFailure::from(CompileError::Unavailable(Unavailable::NoCompiler)),
        coral_v8::CompileFailure::NoCompiler
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

/// Starts children that never exit, keeping each one's bearer header.
#[derive(Clone, Default)]
struct Starter(Arc<Mutex<Vec<String>>>);

struct Child;

impl ProcessStarter for Starter {
    fn start(&mut self, token: &Token) -> io::Result<Box<dyn LocalChild>> {
        let bearer = format!("Bearer {}", token.expose());
        self.0
            .lock()
            .map_err(|_poisoned| io::Error::other("the fake's state"))?
            .push(bearer);
        Ok(Box::new(Child))
    }
}

impl LocalChild for Child {
    fn close_stdin(&mut self) {}
    fn kill(&mut self) {}
    fn exited(&mut self) -> Option<Option<i32>> {
        None
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_call_with_no_room_starts_a_local_child_that_serves_it() {
    let streams = PullStreams::default();
    let credentials = Credentials::default();
    let starter = Starter::default();
    let children =
        LocalRunExecutors::start(starter.clone(), 1, credentials.clone(), streams.clone());
    let caller = PullStreamCompiler::new(streams.clone(), Some(children));
    let calling = tokio::task::spawn_blocking(move || ready_then_compile(&caller));

    let bearer = loop {
        if let Some(bearer) = starter.0.lock().unwrap().first().cloned() {
            break bearer;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    let (outbound, mut sent) = mpsc::unbounded_channel();
    let admitted = credentials.admit(Some(&bearer)).expect("the child's token");
    let child = streams.insert(uuid::Uuid::new_v4().to_string(), admitted, 1, outbound);
    let answering = streams.clone();
    tokio::task::spawn_blocking(move || answer(&answering, child, &mut sent, 1))
        .await
        .unwrap();
    assert_eq!(calling.await.unwrap().as_deref(), Ok("42"));
    assert_eq!(starter.0.lock().unwrap().len(), 1, "one child started");
}

// Catches: callers that all see one free unit, so the losers wait for room
// inside their compile time instead of the wait for a compiler.
#[test]
fn ready_holds_the_unit_for_its_caller_until_its_first_call() {
    let streams = PullStreams::default();
    let (stream, mut sent) = open(&streams, 1);
    let caller = PullStreamCompiler::new(streams.clone(), None);
    let first = caller.ready().unwrap().expect("a held unit");
    assert_eq!(free(&streams, stream), 0, "held for the first caller");

    // A second caller waits for room: the held unit is not its.
    let second = caller.clone();
    let waiting = thread::spawn(move || {
        let held = second.ready()?.expect("a held unit");
        compile(held.as_ref())
    });
    thread::sleep(Duration::from_millis(100));
    assert!(!waiting.is_finished(), "the second caller waits");

    // The first call spends the held unit; its room report frees it for
    // the second.
    let calling = thread::spawn(move || compile(first.as_ref()));
    answer(&streams, stream, &mut sent, 1);
    assert_eq!(calling.join().unwrap().as_deref(), Ok("42"));
    answer(&streams, stream, &mut sent, 1);
    assert_eq!(waiting.join().unwrap().as_deref(), Ok("42"));

    // A held unit no call spends is freed when its client drops.
    let unused = caller.ready().unwrap().expect("a held unit");
    assert_eq!(free(&streams, stream), 0);
    drop(unused);
    assert_eq!(free(&streams, stream), 1);
}
