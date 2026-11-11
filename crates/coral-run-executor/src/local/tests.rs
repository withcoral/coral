use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use coral_telemetry::testing::ExportCapture;
use tokio::sync::mpsc;
use tokio::time::{Instant, sleep};
use tracing_subscriber::layer::SubscriberExt as _;

use super::{LocalChild, LocalRunExecutors, ProcessStarter};
use crate::link::Token;
use crate::pull::{Credentials, PullStreams, StreamId};
use crate::wire::v1::{PullRequest, Room, TakenRun, pull_request};

const LIMIT: usize = 4;
const MAX_RUNS_HELD: u32 = 8;
const SECOND: Duration = Duration::from_secs(1);
const MINUTE: Duration = Duration::from_mins(1);

/// What the fake starter keeps of one child.
struct Started {
    at: Instant,
    bearer: String,
    stdin_closed: bool,
    killed: bool,
}

/// A process starter that starts nothing. A child it starts exits when
/// its stdin closes or it is killed, as a Run Executor does.
#[derive(Clone, Default)]
struct FakeStarter {
    started: Arc<Mutex<Vec<Arc<Mutex<Started>>>>>,
    failures_left: Arc<AtomicUsize>,
}

struct FakeChild(Arc<Mutex<Started>>);

impl ProcessStarter for FakeStarter {
    fn start(&mut self, token: &Token) -> io::Result<Box<dyn LocalChild>> {
        if self.failures_left.load(Ordering::Relaxed) > 0 {
            self.failures_left.fetch_sub(1, Ordering::Relaxed);
            return Err(io::Error::other("no process"));
        }
        let child = Arc::new(Mutex::new(Started {
            at: Instant::now(),
            bearer: format!("Bearer {}", token.expose()),
            stdin_closed: false,
            killed: false,
        }));
        lock(&self.started).push(child.clone());
        Ok(Box::new(FakeChild(child)))
    }
}

impl LocalChild for FakeChild {
    fn close_stdin(&mut self) {
        lock(&self.0).stdin_closed = true;
    }

    fn kill(&mut self) {
        lock(&self.0).killed = true;
    }

    fn exited(&mut self) -> Option<Option<i32>> {
        let started = lock(&self.0);
        match (started.killed, started.stdin_closed) {
            (true, _) => Some(None),
            (false, true) => Some(Some(0)),
            (false, false) => None,
        }
    }
}

impl Drop for FakeChild {
    /// Dropping a real child closes the pipe to its stdin.
    fn drop(&mut self) {
        self.close_stdin();
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().expect("the fake's state")
}

struct Harness {
    supervisor: LocalRunExecutors,
    starter: FakeStarter,
    credentials: Credentials,
    streams: PullStreams,
    began: Instant,
}

impl Harness {
    fn start() -> Self {
        let starter = FakeStarter::default();
        let credentials = Credentials::default();
        let streams = PullStreams::default();
        let supervisor =
            LocalRunExecutors::start(starter.clone(), LIMIT, credentials.clone(), streams.clone());
        Self {
            supervisor,
            starter,
            credentials,
            streams,
            began: Instant::now(),
        }
    }

    /// Asks for room and lets the supervisor act on it.
    async fn request_room(&self) {
        self.supervisor.request_room();
        sleep(Duration::from_millis(1)).await;
    }

    /// When each child started, in whole seconds since the harness began.
    fn starts(&self) -> Vec<u64> {
        let since = |child: &Arc<Mutex<Started>>| lock(child).at.duration_since(self.began);
        let started = lock(&self.starter.started);
        started.iter().map(|child| since(child).as_secs()).collect()
    }

    fn child<T>(&self, index: usize, read: impl Fn(&Started) -> T) -> T {
        let started = lock(&self.starter.started);
        read(&lock(started.get(index).expect("the child started")))
    }

    /// Whether the listener still admits child `index`'s token.
    fn admits(&self, index: usize) -> bool {
        let bearer = self.child(index, |child| child.bearer.clone());
        self.credentials.admit(Some(&bearer)).is_some()
    }

    /// Opens child `index`'s pull stream with `free` units of room.
    fn hello(&self, index: usize, free: u32) -> StreamId {
        let bearer = self.child(index, |child| child.bearer.clone());
        let admitted = self.credentials.admit(Some(&bearer)).unwrap();
        let (outbound, _responses) = mpsc::unbounded_channel();
        let id = uuid::Uuid::new_v4().to_string();
        self.streams.insert(id, admitted, free, outbound)
    }
}

#[tokio::test(start_paused = true)]
async fn a_start_that_fails_or_says_no_hello_in_time_backs_off_until_a_hello() {
    let harness = Harness::start();
    harness.starter.failures_left.store(1, Ordering::Relaxed);

    // The process does not start at 0 s: the next start waits 1 s. Each
    // child then says no Hello: Coral kills it 10 s after its start and
    // waits 2 s, 4 s and 8 s before the next.
    harness.request_room().await;
    sleep(45 * SECOND).await;
    assert_eq!(harness.starts(), [1, 13, 27, 45]);
    for index in 0..3 {
        assert!(harness.child(index, |child| child.killed), "child {index}");
        assert!(!harness.admits(index), "child {index}'s verifier");
    }
    assert!(harness.admits(3));

    // A Hello resets the wait. That child has no room, so the next starts
    // at once; it says no Hello, and the start after it waits 1 s again.
    harness.hello(3, 0);
    sleep(13 * SECOND).await;
    assert_eq!(harness.starts(), [1, 13, 27, 45, 46, 57]);
}

#[tokio::test(start_paused = true)]
async fn a_child_idle_for_five_minutes_retires_and_its_verifier_is_forgotten() {
    let harness = Harness::start();
    harness.request_room().await;
    // Work that waited takes a unit before the supervisor sees the stream.
    let stream = harness.hello(0, MAX_RUNS_HELD);
    let run = || vec![TakenRun::default()];
    harness.streams.take(stream, run()).unwrap();

    // A request while a pull stream has room starts nothing.
    harness.request_room().await;
    sleep(2 * SECOND).await;
    assert_eq!(harness.starts(), [0]);

    // The Runs it holds keep it, however long.
    sleep(4 * MINUTE).await;
    harness.streams.take(stream, run()).unwrap();
    sleep(7 * MINUTE).await;
    assert!(!harness.child(0, |child| child.stdin_closed));

    // One Run ends; the one it took first still keeps it.
    let report = |free| PullRequest {
        message: Some(pull_request::Message::Room(Room { free })),
    };
    harness
        .streams
        .receive(stream, report(MAX_RUNS_HELD - 1))
        .unwrap();
    sleep(6 * MINUTE).await;
    assert!(!harness.child(0, |child| child.stdin_closed));

    // The idle period starts when its room is whole again.
    harness
        .streams
        .receive(stream, report(MAX_RUNS_HELD))
        .unwrap();

    // Its pull stream moves at the connection age. While it is away, a
    // request for room starts nothing, and the move is no work.
    sleep(2 * MINUTE).await;
    harness.streams.close(stream);
    harness.request_room().await;
    sleep(2 * SECOND).await;
    harness.hello(0, MAX_RUNS_HELD);
    sleep(176 * SECOND).await;
    assert!(!harness.child(0, |child| child.stdin_closed));
    assert!(harness.admits(0));

    sleep(4 * SECOND).await;
    assert!(harness.child(0, |child| child.stdin_closed && !child.killed));
    assert!(!harness.admits(0), "the verifier is forgotten at exit");
    assert_eq!(harness.starts(), [0], "an idle retire starts nothing");
}

#[tokio::test(start_paused = true)]
async fn a_stop_closes_each_childs_stdin_and_wins_over_a_request_that_waits_beside_it() {
    let harness = Harness::start();
    harness.request_room().await;
    harness.hello(0, 0);
    assert!(!harness.child(0, |child| child.stdin_closed));

    harness.supervisor.request_room();
    harness.supervisor.stop();
    sleep(3 * SECOND).await;
    assert!(harness.child(0, |child| child.stdin_closed && !child.killed));
    assert_eq!(harness.starts(), [0], "the request starts nothing");
}

#[tokio::test(start_paused = true)]
async fn a_request_for_room_at_the_limit_starts_nothing_and_is_logged_once_a_minute() {
    let logs = ExportCapture::default();
    let _subscriber =
        tracing::subscriber::set_default(tracing_subscriber::registry().with(logs.log_layer()));
    let warnings = || {
        logs.text()
            .matches("local_run_executor_limit is reached")
            .count()
    };
    let harness = Harness::start();

    // A request while a child is starting, or while one has room, starts
    // nothing; each child here opens its pull stream full.
    for index in 0..LIMIT {
        harness.request_room().await;
        harness.request_room().await;
        assert_eq!(harness.starts().len(), index + 1);
        harness.hello(index, 0);
    }
    assert_eq!(warnings(), 0);

    harness.request_room().await;
    sleep(30 * SECOND).await;
    assert_eq!(
        harness.starts().len(),
        LIMIT,
        "a fifth request starts nothing"
    );
    assert_eq!(warnings(), 1);
    sleep(MINUTE).await;
    assert_eq!(harness.starts().len(), LIMIT);
    assert_eq!(warnings(), 2);
}
