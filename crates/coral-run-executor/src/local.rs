//! The local child supervisor: the Run Executors that Coral starts as its
//! own child processes, and the only code that knows a Run Executor's
//! process.
//!
//! [`LocalRunExecutors::request_room`] is its interface: work waits and no
//! pull stream has room. The supervisor then starts a child, below its
//! limit, through a [`ProcessStarter`]. It hands each child a random token
//! and admits that token's verifier until the child exits. It kills a child
//! that opens no pull stream in time, backs off after a failed start,
//! retires an idle child by closing its stdin, and reaps every exit.

use std::fmt::Write as _;
use std::io;
use std::sync::Arc;
use std::time::Duration;

use coral_telemetry::safe;
use ring::rand::{SecureRandom as _, SystemRandom};
use tokio::sync::{Notify, mpsc};
use tokio::time::{Instant, MissedTickBehavior, interval};
use uuid::Uuid;

use crate::link::Token;
use crate::pull::{Credentials, HeldStream, LocalCredential, PullStreams};

mod process;

pub use process::ChildCommand;

/// How long a child has to open its pull stream before Coral kills it.
const HELLO_TIMEOUT: Duration = Duration::from_secs(10);
/// The wait before the start after a failed one, which doubles each time.
const FIRST_BACKOFF: Duration = Duration::from_secs(1);
/// The longest wait before a start.
const MAX_BACKOFF: Duration = Duration::from_secs(30);
/// How long a child holds no Run and no compile before Coral retires it.
const IDLE: Duration = Duration::from_mins(5);
/// How often the supervisor logs that its limit holds work back.
const LIMIT_LOG_PERIOD: Duration = Duration::from_mins(1);
/// How often the supervisor looks at its children and their pull streams.
const TICK: Duration = Duration::from_secs(1);

/// Starts one Run Executor process.
pub trait ProcessStarter: Send + 'static {
    /// Starts a child and writes `token` and a newline to its stdin, which
    /// stays open.
    ///
    /// # Errors
    ///
    /// Returns the failure to start the process or to write its token.
    fn start(&mut self, token: &Token) -> io::Result<Box<dyn LocalChild>>;
}

/// One child process.
pub trait LocalChild: Send {
    /// Closes the child's stdin: its order to retire.
    fn close_stdin(&mut self);
    /// Kills the child.
    fn kill(&mut self);
    /// Reaps the child once it exited: its exit code, or none when a signal
    /// ended it. `None` while it runs.
    fn exited(&mut self) -> Option<Option<i32>>;
}

/// The local child supervisor's handle. The supervisor stops at
/// [`LocalRunExecutors::stop`], or when the last clone drops, and its
/// children's stdin closes with it.
#[derive(Clone)]
pub struct LocalRunExecutors {
    requests: mpsc::Sender<()>,
    stopped: Arc<Notify>,
}

impl LocalRunExecutors {
    /// Starts the supervisor, which runs at most `limit` children at once.
    /// Each child opens its pull stream in `streams`, and is admitted
    /// through `credentials`.
    #[must_use]
    pub fn start(
        starter: impl ProcessStarter,
        limit: usize,
        credentials: Credentials,
        streams: PullStreams,
    ) -> Self {
        let (requests, requested) = mpsc::channel(1);
        let stopped = Arc::new(Notify::new());
        let supervisor = Supervisor {
            starter,
            limit,
            credentials,
            streams,
            children: Vec::new(),
            wanted: false,
            backoff: FIRST_BACKOFF,
            start_after: Instant::now(),
            limit_logged: None,
        };
        tokio::spawn(supervisor.run(requested, stopped.clone()));
        Self { requests, stopped }
    }

    /// Stops the supervisor: every child's stdin closes, and nothing waits
    /// for the children. Later requests for room start nothing.
    pub fn stop(&self) {
        self.stopped.notify_one();
    }

    /// Work waits and no pull stream has room: starts a child, below the
    /// limit. It returns at once, and the child's pull stream appears in
    /// the [`PullStreams`] when it opens.
    pub fn request_room(&self) {
        // A full channel already carries the request.
        let _carried = self.requests.try_send(());
    }
}

struct Child {
    process: Box<dyn LocalChild>,
    /// Admits the child's token until the child is reaped.
    credential: LocalCredential,
    started: Instant,
    /// What the child's first `Hello` declared: its id and its whole room.
    hello: Option<(Option<Uuid>, u32)>,
    /// When its pull stream was last seen. A stream moves at each connection
    /// age, so it is away for a moment.
    last_seen: Instant,
    /// When it last held a Run or a compile, or said `Hello`.
    last_work: Instant,
    /// Coral closed its stdin, or killed it.
    retiring: bool,
}

struct Supervisor<S> {
    starter: S,
    limit: usize,
    credentials: Credentials,
    streams: PullStreams,
    children: Vec<Child>,
    /// Work waited when it last asked, and no room has shown since.
    wanted: bool,
    /// The wait after the next failed start.
    backoff: Duration,
    /// No child starts before this.
    start_after: Instant,
    limit_logged: Option<Instant>,
}

impl<S: ProcessStarter> Supervisor<S> {
    /// Runs until it is stopped or every handle drops. Dropping the children
    /// then closes each one's stdin, and nothing waits for them.
    async fn run(mut self, mut requested: mpsc::Receiver<()>, stopped: Arc<Notify>) {
        let mut ticks = interval(TICK);
        ticks.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                // A stop wins over a request that waits beside it.
                biased;
                () = stopped.notified() => return,
                request = requested.recv() => match request {
                    Some(()) => self.wanted = true,
                    None => return,
                },
                _ = ticks.tick() => {}
            }
            let now = Instant::now();
            let held = self.streams.held();
            self.observe(&held, now);
            self.provide_room(&held, now);
        }
    }

    /// Notes each child's `Hello`, reaps the children that exited, kills
    /// the ones whose `Hello` is late, and retires the idle ones.
    fn observe(&mut self, held: &[HeldStream], now: Instant) {
        let mut children = std::mem::take(&mut self.children);
        children.retain_mut(|child| {
            let stream = held
                .iter()
                .find(|stream| stream.credential == child.credential.id());
            if let Some(stream) = stream {
                // Its first Hello carries its whole room: it holds nothing.
                let (_, room) = *child.hello.get_or_insert_with(|| {
                    self.backoff = FIRST_BACKOFF;
                    child.last_work = now;
                    (
                        Uuid::try_parse(&stream.run_executor_id).ok(),
                        stream.hello_free,
                    )
                });
                child.last_seen = now;
                if stream.free < room {
                    child.last_work = now;
                } else if let Some(worked) = stream.last_work {
                    child.last_work = child.last_work.max(worked);
                }
            }
            if let Some(code) = child.process.exited() {
                tracing::info!(
                    coral.run_executor.id =
                        child.hello.and_then(|(id, _)| id).as_ref().map(safe::id),
                    coral.run_executor.exit_code = code.map(safe::number),
                    "a local Run Executor exited"
                );
                if child.hello.is_none() && !child.retiring {
                    self.start_failed(now);
                }
                // Dropping the child forgets its verifier.
                return false;
            }
            if child.retiring {
                return true;
            }
            if child.hello.is_none() {
                if now.duration_since(child.started) >= HELLO_TIMEOUT {
                    tracing::warn!(
                        "a local Run Executor opened no pull stream in time; killing it"
                    );
                    child.process.kill();
                    child.retiring = true;
                    self.start_failed(now);
                }
            } else if now.duration_since(child.last_work) >= IDLE {
                // A child whose pull stream stays away that long retires
                // too. Retiring ends no Run: the child exits when it holds
                // none.
                child.process.close_stdin();
                child.retiring = true;
            }
            true
        });
        self.children = children;
    }

    /// Starts a child when work waits, no pull stream has room and none is
    /// about to open, the back-off is over and the limit allows it.
    fn provide_room(&mut self, held: &[HeldStream], now: Instant) {
        if !self.wanted {
            return;
        }
        let retiring = |stream: &HeldStream| {
            self.children
                .iter()
                .any(|child| child.retiring && child.credential.id() == stream.credential)
        };
        if held
            .iter()
            .any(|stream| stream.free > 0 && !retiring(stream))
        {
            self.wanted = false;
            return;
        }
        // A child that has said no Hello yet, or whose pull stream is away
        // while it moves, is room about to open.
        let opening = |child: &Child| {
            !child.retiring
                && (child.hello.is_none() || now.duration_since(child.last_seen) < HELLO_TIMEOUT)
                && !held
                    .iter()
                    .any(|stream| stream.credential == child.credential.id())
        };
        if self.children.iter().any(opening) || now < self.start_after {
            return;
        }
        if self.children.len() >= self.limit {
            if self
                .limit_logged
                .is_none_or(|logged| now.duration_since(logged) >= LIMIT_LOG_PERIOD)
            {
                self.limit_logged = Some(now);
                tracing::warn!(
                    coral.run_executor.local_limit = safe::number(self.limit),
                    "work waits for a local Run Executor: runtime.local_run_executor_limit is reached"
                );
            }
            return;
        }
        let started = random_token().and_then(|token| {
            let credential = self.credentials.admit_local(&token);
            let process = self.starter.start(&token)?;
            Ok((process, credential))
        });
        match started {
            Ok((process, credential)) => self.children.push(Child {
                process,
                credential,
                started: now,
                hello: None,
                last_seen: now,
                last_work: now,
                retiring: false,
            }),
            Err(error) => {
                tracing::warn!(
                    error.type = safe::kind(io_error_kind(&error)),
                    "a local Run Executor did not start"
                );
                self.start_failed(now);
            }
        }
    }

    /// The next start waits the back-off, which doubles.
    fn start_failed(&mut self, now: Instant) {
        self.start_after = now + self.backoff;
        self.backoff = (self.backoff * 2).min(MAX_BACKOFF);
    }
}

/// A random 256-bit token, in hexadecimal.
fn random_token() -> io::Result<Token> {
    let mut bytes = [0_u8; 32];
    SystemRandom::new()
        .fill(&mut bytes)
        .map_err(|_unspecified| io::Error::other("the system has no randomness"))?;
    let mut token = String::with_capacity(64);
    for byte in bytes {
        // Writing to a String cannot fail.
        let _written = write!(token, "{byte:02x}");
    }
    Token::new(token).map_err(io::Error::other)
}

fn io_error_kind(error: &io::Error) -> &'static str {
    match error.kind() {
        io::ErrorKind::NotFound => "not_found",
        io::ErrorKind::PermissionDenied => "permission_denied",
        io::ErrorKind::OutOfMemory => "out_of_memory",
        io::ErrorKind::BrokenPipe => "broken_pipe",
        _ => "io",
    }
}

#[cfg(test)]
pub(crate) mod tests;
