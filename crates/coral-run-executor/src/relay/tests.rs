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
use crate::relay_repository::{Take, count, take_oldest};
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
    relaying(database, id, super::PROMISE_KEPT, super::ANSWER_RESERVE)
}

/// A Coral Instance whose pickup holds a unit for a probe it answered for
/// another one for `kept`.
fn promising(database: Arc<Database>, id: &str, kept: Duration) -> Arc<Instance> {
    relaying(database, id, kept, super::ANSWER_RESERVE)
}

/// A Coral Instance whose pickup holds a unit for a probe it answered for
/// another one for `kept`, and ends a compile it serves `answer_reserve`
/// before the call's deadline.
fn relaying(
    database: Arc<Database>,
    id: &str,
    kept: Duration,
    answer_reserve: Duration,
) -> Arc<Instance> {
    let streams = PullStreams::default();
    let relay = CompileRelay::new(database, id)
        .with_promise_kept(kept)
        .with_answer_reserve(answer_reserve);
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
    instances_answering(database, super::ANSWER_RESERVE).await
}

/// [`instances`], each ending a compile it serves `answer_reserve` before
/// the call's deadline.
async fn instances_answering<const N: usize>(
    database: &TestDatabase,
    answer_reserve: Duration,
) -> (Arc<Instance>, [Arc<Instance>; N]) {
    let instance = |database, id: &str| relaying(database, id, super::PROMISE_KEPT, answer_reserve);
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

/// The units the streams can still take.
fn free(streams: &PullStreams) -> u32 {
    streams.held().iter().map(|held| held.free).sum()
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

/// Waits until `rows` rows are waiting, looking again on each change
/// signal: each row's write raises one.
async fn rows_reach(database: &Database, rows: i64) {
    let mut changes = database.subscribe_changes().expect("the change signal");
    let reached = async {
        while count(database).await.expect("the rows") != rows {
            changes.changed().await.expect("the change signal");
        }
    };
    tokio::time::timeout(Duration::from_secs(5), reached)
        .await
        .unwrap_or_else(|_elapsed| panic!("{rows} rows within 5 s"));
}

/// Waits for a compiler on `compiler`, as admission does.
fn readying(
    compiler: &PullStreamCompiler,
) -> JoinHandle<Result<Option<Arc<dyn CompilerClient>>, CompileError>> {
    let compiler = compiler.clone();
    tokio::spawn(async move { compiler.ready().await })
}

#[tokio::test]
async fn a_compile_with_no_room_here_is_served_where_there_is_room() {
    let database = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
    let (requester, [server]) = instances(&database).await;
    let (stream, mut sent) = open(&server.streams, 0);

    // Ready waits on a probe until a Coral Instance has room. Room
    // elsewhere holds nothing here; room that showed here is held, and
    // freed as the held client drops.
    let ready = readying(&requester.compiler);
    rows_reach(&database.database(), 1).await;
    report(&server.streams, stream, 1);
    let held = ready.await.unwrap().map(|held| held.is_some());
    assert_eq!(held, Ok(Arc::ptr_eq(&requester, &server)));
    assert_eq!(
        count(&database.database()).await.unwrap(),
        0,
        "the probe is gone"
    );

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

    let call = compiling(&requester.compiler, Duration::from_secs(2));
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
async fn a_row_served_whose_compile_runs_out_of_time_fails_as_timed_out_and_is_gone() {
    let database = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
    // The served compile ends a second early, so its timed-out answer is
    // read well before the requester gives up on the taker.
    let (requester, [server]) = instances_answering(&database, Duration::from_secs(1)).await;
    let (stream, mut sent) = open(&server.streams, 0);

    let call = compiling(&requester.compiler, Duration::from_secs(3));
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
async fn a_row_whose_taker_stopped_fails_as_a_stopped_compiler_and_is_gone() {
    let database = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
    // No stream with room anywhere, so only the stopped taker takes it.
    let (requester, [_server]) = instances(&database).await;
    let call = compiling(&requester.compiler, Duration::from_secs(3));
    rows_reach(&database.database(), 1).await;

    // A Coral Instance takes the row, then stops before it answers.
    let db = database.database();
    let mut transaction = db.begin().await.unwrap();
    let take = Take {
        taker: "stopped",
        deadline_after: 0,
        from: None,
    };
    let taken = take_oldest(&mut transaction, take).await.unwrap();
    transaction.commit().await.unwrap();
    assert!(taken.is_some(), "the row is taken");

    assert_eq!(
        call.await.unwrap(),
        Err(CompileError::Unavailable(Unavailable::Stopped)),
        "not a compile timeout: nothing compiled it"
    );
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

/// The requester, and another Coral Instance whose pickup holds a unit for
/// a probe it answered for `kept`: on SQLite they share the one database
/// handle.
async fn requester_and_promising_server(
    database: &TestDatabase,
    kept: Duration,
) -> (Arc<Instance>, Arc<Instance>) {
    let requester = instance(database.database(), "requester");
    let server = match database.database().system() {
        DatabaseSystem::Postgres => {
            let peer = database.connect_peer().await.expect("a peer connection");
            promising(Arc::new(peer), "server", kept)
        }
        DatabaseSystem::Sqlite => promising(database.database(), "server", kept),
    };
    (requester, server)
}

#[tokio::test]
async fn one_unit_of_room_elsewhere_readies_one_waiting_caller_at_a_time() {
    let database = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
    // A promise that outlasts the test, so only its call or new room frees
    // the unit.
    let (requester, server) =
        requester_and_promising_server(&database, Duration::from_secs(600)).await;
    let (stream, mut sent) = open(&server.streams, 0);
    let first = readying(&requester.compiler);
    rows_reach(&database.database(), 1).await;
    let second = readying(&requester.compiler);
    rows_reach(&database.database(), 2).await;

    report(&server.streams, stream, 1);
    let ready = first.await.unwrap().map(|held| held.is_some());
    assert_eq!(ready, Ok(false), "the oldest caller relays its calls");
    // The server holds its one unit for the first caller's call, so the
    // other's probe finds no room.
    assert_eq!(free(&server.streams), 0, "the unit is held");
    assert!(!second.is_finished(), "the other waits for room");

    // The first caller's call spends the held unit.
    let call = compiling(&requester.compiler, Duration::from_secs(5));
    answer(&server.streams, stream, &mut sent).await;
    assert_eq!(call.await.unwrap().as_deref(), Ok("42"));
    assert!(!second.is_finished(), "the other still waits for room");

    report(&server.streams, stream, 1);
    let ready = second.await.unwrap().map(|held| held.is_some());
    assert_eq!(ready, Ok(false), "new room readies the other");
}

#[tokio::test]
async fn a_unit_held_for_a_call_that_never_comes_readies_the_next_caller() {
    let database = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
    let (requester, server) =
        requester_and_promising_server(&database, Duration::from_millis(200)).await;
    let (stream, _sent) = open(&server.streams, 0);
    let first = readying(&requester.compiler);
    rows_reach(&database.database(), 1).await;
    let second = readying(&requester.compiler);
    rows_reach(&database.database(), 2).await;

    report(&server.streams, stream, 1);
    let ready = first.await.unwrap().map(|held| held.is_some());
    assert_eq!(ready, Ok(false));
    // No call comes: the promise lapses, and the unit readies the other.
    let ready = second.await.unwrap().map(|held| held.is_some());
    assert_eq!(ready, Ok(false));
}
