//! The relay through the database. On Postgres the rows cross Coral
//! Instances, each with its own connection; on SQLite the one Coral
//! Instance relays its own calls while its Run Executors are full.

use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use coral_db::{Database, DatabaseSystem};
use coral_test_support::TestDatabase;
use tokio::task::JoinHandle;

use super::CompileRelay;
use crate::pull::tests::{Sent, compiled, dialed_stream as open, up};
use crate::relay_repository::count;
use crate::wire::v1::{PullRequest, PullResponse, Room, pull_request, pull_response};
use crate::{
    BuiltinTools, CompileError, CompilerClient, PullStreamCompiler, PullStreams, StreamId,
    Unavailable,
};

/// One Coral Instance: its pull streams, its compiler client, and its
/// pickup, stopped when it drops.
struct Instance {
    streams: PullStreams,
    compiler: PullStreamCompiler,
    pickup: JoinHandle<Infallible>,
}

impl Drop for Instance {
    fn drop(&mut self) {
        self.pickup.abort();
    }
}

fn instance(database: Arc<Database>, id: &str) -> Arc<Instance> {
    let streams = PullStreams::default();
    let relay = CompileRelay::new(database, id);
    let pickup = tokio::spawn(relay.clone().serve(streams.clone()));
    Arc::new(Instance {
        compiler: PullStreamCompiler::relayed(streams.clone(), None, relay),
        streams,
        pickup,
    })
}

/// The requester, and `N` Coral Instances that serve its rows: on
/// Postgres each one its own, on SQLite the requester itself.
async fn instances<const N: usize>(database: &TestDatabase) -> (Arc<Instance>, [Arc<Instance>; N]) {
    let requester = instance(database.database(), "requester");
    let mut others = Vec::new();
    for index in 0..N {
        others.push(match database.database().system() {
            DatabaseSystem::Postgres => {
                let peer = database.connect_peer().await.expect("a peer connection");
                instance(Arc::new(peer), &format!("server-{index}"))
            }
            DatabaseSystem::Sqlite => requester.clone(),
        });
    }
    let Ok(others) = others.try_into() else {
        unreachable!("{N} servers");
    };
    (requester, others)
}

/// `stream`'s Run Executor reports `free` units of room.
fn report(streams: &PullStreams, stream: StreamId, free: u32) {
    let room = PullRequest {
        message: Some(pull_request::Message::Room(Room { free })),
    };
    streams.receive(stream, room).expect("a room report");
}

/// Answers the next `Compile` sent on `stream` with the package `42`.
async fn answer(streams: &PullStreams, stream: StreamId, sent: &mut Sent) {
    let next = tokio::time::timeout(Duration::from_secs(5), sent.recv()).await;
    let Ok(Some(Ok(PullResponse {
        message: Some(pull_response::Message::Compile(compile)),
    }))) = next
    else {
        panic!("a Compile within 5 s");
    };
    streams
        .receive(stream, up(compiled(compile.number)))
        .expect("the answer");
}

/// Compiles `return 42;` on `compiler` within `time_left`.
fn compiling(
    compiler: &PullStreamCompiler,
    time_left: Duration,
) -> JoinHandle<Result<String, CompileError>> {
    let compiler = compiler.clone();
    tokio::spawn(async move {
        let builtin_tools = BuiltinTools::default();
        let program = compiler
            .compile(time_left, "return 42;", &[], &builtin_tools, None)
            .await?;
        Ok(program.javascript().to_owned())
    })
}

/// Waits until `rows` rows are waiting.
async fn rows_reach(database: &Database, rows: i64) {
    for _ in 0..500 {
        if count(database).await.expect("the rows") == rows {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("{rows} rows within 5 s");
}

#[tokio::test]
async fn a_compile_with_no_room_here_is_served_where_there_is_room() {
    let database = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
    let (requester, [server]) = instances(&database).await;
    let (stream, mut sent) = open(&server.streams, 0);

    // Ready waits on a probe until a Coral Instance has room. Room
    // elsewhere holds nothing here; room that showed here is held, and
    // freed as the held client drops.
    let compiler = requester.compiler.clone();
    let ready = tokio::spawn(async move { compiler.ready().await.map(|held| held.is_some()) });
    rows_reach(&database.database(), 1).await;
    report(&server.streams, stream, 1);
    assert_eq!(ready.await.unwrap(), Ok(Arc::ptr_eq(&requester, &server)));
    rows_reach(&database.database(), 0).await;

    report(&server.streams, stream, 0);
    let call = compiling(&requester.compiler, Duration::from_secs(5));
    rows_reach(&database.database(), 1).await;
    report(&server.streams, stream, 1);
    answer(&server.streams, stream, &mut sent).await;
    assert_eq!(call.await.unwrap().as_deref(), Ok("42"));
    assert_eq!(
        count(&database.database()).await.unwrap(),
        0,
        "the requester deletes its row"
    );
}

#[tokio::test]
async fn two_coral_instances_with_room_serve_two_rows_once_each() {
    let database = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
    let (requester, servers) = instances::<2>(&database).await;
    let mut opened: Vec<_> = servers
        .iter()
        .map(|server| (server, open(&server.streams, 0)))
        .collect();
    let calls = [
        compiling(&requester.compiler, Duration::from_secs(5)),
        compiling(&requester.compiler, Duration::from_secs(5)),
    ];
    rows_reach(&database.database(), 2).await;

    // One unit each, and no room report after a send: a stream that served
    // a row cannot take the other.
    for (server, (stream, _)) in &opened {
        report(&server.streams, *stream, 1);
    }
    for (server, (stream, sent)) in &mut opened {
        answer(&server.streams, *stream, sent).await;
    }
    for call in calls {
        assert_eq!(call.await.unwrap().as_deref(), Ok("42"));
    }
    for (_, (_, sent)) in &mut opened {
        assert!(sent.try_recv().is_err(), "each row is served once");
    }
    assert_eq!(count(&database.database()).await.unwrap(), 0);
}

#[tokio::test]
async fn a_row_nobody_serves_fails_at_the_requesters_bound_and_is_gone() {
    let database = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
    let (requester, [server]) = instances(&database).await;
    let (_stream, mut sent) = open(&server.streams, 0);

    let call = compiling(&requester.compiler, Duration::from_millis(500));
    rows_reach(&database.database(), 1).await;
    assert_eq!(
        call.await.unwrap(),
        Err(CompileError::Unavailable(Unavailable::NoCompiler))
    );
    assert_eq!(
        count(&database.database()).await.unwrap(),
        0,
        "the row is gone"
    );
    assert!(sent.try_recv().is_err(), "nothing was sent");
}

#[tokio::test]
async fn a_row_served_and_never_answered_fails_as_timed_out_and_is_gone() {
    let database = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
    let (requester, [server]) = instances(&database).await;
    let (stream, mut sent) = open(&server.streams, 0);

    let call = compiling(&requester.compiler, Duration::from_millis(800));
    rows_reach(&database.database(), 1).await;
    report(&server.streams, stream, 1);
    let sent = tokio::time::timeout(Duration::from_secs(5), sent.recv()).await;
    assert!(
        matches!(sent, Ok(Some(Ok(_)))),
        "the call went down the stream"
    );
    assert_eq!(call.await.unwrap(), Err(CompileError::TimedOut));
    assert_eq!(
        count(&database.database()).await.unwrap(),
        0,
        "the row is gone"
    );
}

#[tokio::test]
async fn a_row_written_while_no_run_executor_is_linked_is_served_once_one_dials_in() {
    let database = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
    let (requester, [server]) = instances(&database).await;

    let call = compiling(&requester.compiler, Duration::from_secs(5));
    rows_reach(&database.database(), 1).await;
    let (stream, mut sent) = open(&server.streams, 1);
    answer(&server.streams, stream, &mut sent).await;
    assert_eq!(call.await.unwrap().as_deref(), Ok("42"));
    assert_eq!(count(&database.database()).await.unwrap(), 0);
}
