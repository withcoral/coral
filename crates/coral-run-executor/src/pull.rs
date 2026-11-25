//! Coral's side of the link: the listener that Run Executors dial, the
//! credentials it admits, and the pull streams this Coral Instance holds.

use std::collections::{HashMap, VecDeque};
use std::future::{Future, pending};
use std::hash::{BuildHasher as _, RandomState};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use coral_db::Database;
use coral_telemetry::safe;
use sha2::{Digest as _, Sha256};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::{Instant, MissedTickBehavior, Sleep, interval_at, sleep_until, timeout};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::transport::Server;
use tonic::transport::server::TcpIncoming;
use tonic::{Code, Request, Response, Status, Streaming};

use crate::compile_wire::{ProtocolError, Read, Takes};
use crate::link::{KEEPALIVE, Token};
use crate::relay::CompileRelay;
use crate::time;
use crate::wire::v1::compile::Call;
use crate::wire::v1::link_service_server::{LinkService, LinkServiceServer};
use crate::wire::v1::{
    Compile, Compiled, PullRequest, PullResponse, RunRequest, RunResponse, Taken, TakenRun,
    Welcome, pull_request, pull_response,
};
use crate::{
    LocalRunExecutors, MESSAGE_MAX_BYTES, PROTOCOL_LEVEL, PullStreamCompiler, RunStreamServer,
    SHUTTING_DOWN, ServedRunStream, lease_repository,
};

/// How long the listener keeps a connection, and a pull stream its open
/// time: then a GOAWAY, and the pull stream ends cleanly (the move).
pub(crate) const CONNECTION_AGE: Duration = Duration::from_mins(5);
/// How far past each renewal a Run Executor's lease lasts.
pub(crate) const LEASE: Duration = Duration::from_secs(60);
/// How often the holder of a pull stream renews its Run Executor's lease.
pub(crate) const RENEWAL: Duration = Duration::from_secs(20);
/// HTTP/2 `SETTINGS_MAX_HEADER_LIST_SIZE`, as Coral's private listener sets
/// it: room for a token as long as [`Token`] admits.
const HTTP2_MAX_HEADER_LIST_SIZE: u32 = 128 * 1024;
/// The compiles Coral sends one Run Executor at once: one on its one warm
/// compiler and three queued behind it, so that a short call queued there,
/// such as an analysis, never leaves the compiler idle while the next call
/// is on its way. A compile spends this compile room, never the room the
/// Run Executor reports for Runs, so a full set of Runs does not stop one.
pub(crate) const COMPILE_ROOM: u32 = 4;

/// The link's periods: [`CONNECTION_AGE`], [`LEASE`] and [`RENEWAL`] by
/// default.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Timing {
    /// How long a connection and a pull stream stay open.
    pub(crate) connection_age: Duration,
    /// How far past each renewal a lease lasts.
    pub(crate) lease: Duration,
    /// How often a held pull stream renews its lease.
    pub(crate) renewal: Duration,
}

impl Default for Timing {
    fn default() -> Self {
        Self {
            connection_age: CONNECTION_AGE,
            lease: LEASE,
            renewal: RENEWAL,
        }
    }
}

/// Which credential admitted a Run Executor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    /// A local child's token, which Coral wrote to its stdin.
    Local,
    /// An operator token: a Run Executor that Coral did not start.
    Dialed,
}

impl Kind {
    /// How a row records the kind.
    #[must_use]
    pub const fn as_storage_str(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Dialed => "dialed",
        }
    }
}

type Verifier = [u8; 32];

pub(crate) fn verifier(token: &str) -> Verifier {
    Sha256::digest(token.as_bytes()).into()
}

/// The credentials the listener admits, each kept as its SHA-256 verifier:
/// the operator's tokens, and one per live local child.
#[derive(Clone, Default)]
pub struct Credentials(Arc<Mutex<HashMap<Verifier, Admitted>>>);

/// What one admitted credential is: its kind and, for a local child, the
/// Run Executor id its first `Hello` gave.
#[derive(Clone)]
struct Admitted {
    kind: Kind,
    hello: Option<String>,
}

impl Admitted {
    const fn of(kind: Kind) -> Self {
        Self { kind, hello: None }
    }
}

impl Credentials {
    /// Admits each of the operator's `tokens`, two during a rotation, as
    /// [`Kind::Dialed`].
    #[must_use]
    pub fn new(tokens: impl IntoIterator<Item = Token>) -> Self {
        let credentials = Self::default();
        lock(&credentials.0).extend(
            tokens
                .into_iter()
                .map(|token| (token.verifier(), Admitted::of(Kind::Dialed))),
        );
        credentials
    }

    /// Admits a local child's `token` as [`Kind::Local`] until the returned
    /// credential drops, when the child exits.
    #[must_use]
    pub fn admit_local(&self, token: &Token) -> LocalCredential {
        let verifier = token.verifier();
        lock(&self.0).insert(verifier, Admitted::of(Kind::Local));
        LocalCredential {
            credentials: self.clone(),
            verifier,
        }
    }

    /// The kind `authorization`, a request's header value, admits, and the
    /// credential that admits it.
    pub(crate) fn admit(&self, authorization: Option<&str>) -> Option<(Kind, CredentialId)> {
        let (scheme, token) = authorization?.split_once(' ')?;
        // An authentication scheme's name ignores case.
        if !scheme.eq_ignore_ascii_case("Bearer") {
            return None;
        }
        let verifier = verifier(token);
        let kind = lock(&self.0).get(&verifier)?.kind;
        Some((kind, CredentialId(verifier)))
    }

    /// Records the Run Executor id of a local child's first `Hello` on its
    /// credential, so the supervisor can name the child once it exits, even
    /// when its pull stream went before the supervisor saw it.
    pub(crate) fn said_hello(&self, credential: CredentialId, run_executor_id: &str) {
        if let Some(admitted) = lock(&self.0).get_mut(&credential.0)
            && admitted.kind == Kind::Local
            && admitted.hello.is_none()
        {
            admitted.hello = Some(run_executor_id.to_owned());
        }
    }
}

/// Which admitted credential opened a pull stream: how the local child
/// supervisor finds its child's stream.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct CredentialId(Verifier);

/// A dialed admission for a stream a test inserts without the listener.
#[cfg(any(test, feature = "test-support"))]
pub(crate) fn dialed_admission() -> (Kind, CredentialId) {
    (Kind::Dialed, CredentialId(verifier("dialed")))
}

impl std::fmt::Debug for CredentialId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("CredentialId")
    }
}

/// A local child's credential, admitted until this drops.
pub struct LocalCredential {
    credentials: Credentials,
    verifier: Verifier,
}

impl LocalCredential {
    /// The id its pull streams carry.
    pub(crate) fn id(&self) -> CredentialId {
        CredentialId(self.verifier)
    }

    /// The Run Executor id of the child's first `Hello`; none before it.
    pub(crate) fn run_executor_id(&self) -> Option<String> {
        lock(&self.credentials.0)
            .get(&self.verifier)
            .and_then(|admitted| admitted.hello.clone())
    }
}

impl Drop for LocalCredential {
    fn drop(&mut self) {
        lock(&self.credentials.0).remove(&self.verifier);
    }
}

/// One pull stream this Coral Instance holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct StreamId(u64);

/// A pull stream that takes sends, with its room.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HeldStream {
    /// The stream.
    pub stream: StreamId,
    /// The id its Run Executor declared, for operators only.
    pub run_executor_id: String,
    /// The kind of credential that admitted it.
    pub kind: Kind,
    /// The credential that admitted it.
    pub(crate) credential: CredentialId,
    /// The Runs Coral may still send it.
    pub free: u32,
    /// The room its `Hello` declared, which no send or report changes.
    pub(crate) hello_free: u32,
    /// When it last took a unit or freed one: Coral sent it a Run or a
    /// compile, it answered a compile, or it reported its room. None since
    /// it opened, which is no work: a stream reopens at each connection age.
    pub(crate) last_work: Option<Instant>,
}

/// Why a send went nowhere.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum Refused {
    /// The Run Executor's room is less than the send's units.
    #[error("the Run Executor has no room for it")]
    NoRoom,
    /// The pull stream ended, or is ending.
    #[error("the pull stream is closed")]
    Closed,
    /// The message is past [`MESSAGE_MAX_BYTES`]. Sent, it would end the
    /// stream and every compile in flight on it.
    #[error("the message is too large for the pull stream")]
    TooLarge,
}

/// The pull streams this Coral Instance holds, for the compiler client and
/// the take loop. Each Run sent spends a unit of its stream's room, which a
/// `Room` report replaces. Each compile spends a unit of the stream's
/// compile room, a count Coral keeps, until its answer. Callers that find
/// no compile room wait in one line, and each unit that frees goes to the
/// oldest of them.
#[derive(Clone, Default)]
pub struct PullStreams {
    held: Arc<Mutex<HashMap<StreamId, Held>>>,
    /// The callers waiting for compile room, oldest first. Locked only
    /// while `held` is.
    compile_line: Arc<Mutex<VecDeque<oneshot::Sender<StreamId>>>>,
    /// Signalled when a stream opens or reports its room.
    room_signal: Arc<tokio::sync::Notify>,
    next: Arc<AtomicU64>,
    /// Set once this Coral Instance drains ([`Self::drain`]).
    draining: Arc<watch::Sender<bool>>,
    /// How many streams it holds, for [`Self::ended`].
    count: Arc<watch::Sender<usize>>,
}

struct Held {
    /// What the take loop and the compiler client see.
    view: HeldStream,
    /// Set at the connection age, the drain or shutdown: nothing more is
    /// sent.
    closing: bool,
    outbound: mpsc::UnboundedSender<Result<PullResponse, Status>>,
    compiles_sent: u64,
    /// Units of room held for a take while it claims Runs. `free` leaves
    /// them out.
    kept_for_takes: u32,
    /// Units of compile room held for callers that wait for room, and for
    /// the calls a relay pickup promised another Coral Instance, until each
    /// sends its compile or lets the unit go.
    kept_for_compiles: u32,
    /// Each compile in flight, by number.
    compiles: HashMap<u64, InFlight>,
}

/// A compile sent and not yet answered.
struct InFlight {
    /// When its time runs out.
    due: Instant,
    /// The answers it accepts.
    takes: Takes,
    /// Where its answer goes: a channel of one, sent on once.
    answered: oneshot::Sender<Read>,
}

impl Held {
    fn spend(&mut self, units: u32) -> Result<(), Refused> {
        if self.closing {
            return Err(Refused::Closed);
        }
        self.view.free = self.view.free.checked_sub(units).ok_or(Refused::NoRoom)?;
        self.view.last_work = Some(Instant::now());
        Ok(())
    }

    /// The compiles Coral may still send: [`COMPILE_ROOM`] less the
    /// compiles in flight and the units held for them. None once it closes.
    fn compile_room(&self) -> u32 {
        if self.closing {
            return 0;
        }
        let in_flight = u32::try_from(self.compiles.len()).unwrap_or(u32::MAX);
        COMPILE_ROOM.saturating_sub(in_flight.saturating_add(self.kept_for_compiles))
    }

    /// Checks that a compile, sent or held, fits its compile room. The
    /// compile in flight or the held unit then takes it.
    fn fit_compile(&mut self) -> Result<(), Refused> {
        if self.closing {
            return Err(Refused::Closed);
        }
        if self.compile_room() == 0 {
            return Err(Refused::NoRoom);
        }
        self.view.last_work = Some(Instant::now());
        Ok(())
    }

    /// Holds one unit of its compile room for a caller.
    fn hold_compile(&mut self) -> Result<(), Refused> {
        self.fit_compile()?;
        self.kept_for_compiles += 1;
        Ok(())
    }

    fn send(&self, response: PullResponse) {
        // A closed receiver means the stream is ending; its holder removes it.
        drop(self.outbound.send(Ok(response)));
    }
}

/// Whether a stream that takes sends has room for a Run.
fn has_room(held: &HashMap<StreamId, Held>) -> bool {
    held.values()
        .any(|held| !held.closing && held.view.free > 0)
}

/// Whether a stream that takes sends has room for a compile.
fn has_compile_room(held: &HashMap<StreamId, Held>) -> bool {
    held.values().any(|held| held.compile_room() > 0)
}

/// The stream with the most compile room, ties as
/// [`PullStreams::most_compile_room`] breaks them.
fn most_compile_room(held: &HashMap<StreamId, Held>) -> Option<StreamId> {
    let random = RandomState::new();
    held.values()
        .filter(|held| held.compile_room() > 0)
        .max_by_key(|held| {
            let (stream, free) = (held.view.stream, held.view.free);
            (held.compile_room(), free, random.hash_one(stream))
        })
        .map(|held| held.view.stream)
}

/// Hands each unit of compile room free to the oldest caller in `line`,
/// held for it on the stream with the most. A caller that left the line
/// takes none. So while a caller waits in line, no stream has compile room
/// free.
fn hand_off(held: &mut HashMap<StreamId, Held>, line: &mut VecDeque<oneshot::Sender<StreamId>>) {
    while let Some(stream) = most_compile_room(held)
        && let Some(next) = line.pop_front()
    {
        // Held before `held` unlocks, so the caller never finds it unheld.
        if next.send(stream).is_ok()
            && let Some(chosen) = held.get_mut(&stream)
        {
            let _found_above = chosen.hold_compile();
        }
    }
}

/// A caller's place in the line for compile room. Dropped, it leaves the
/// line, and frees a unit handed to it that it never read.
pub(crate) struct CompileTurn {
    streams: PullStreams,
    /// None once it read its unit.
    handed: Option<oneshot::Receiver<StreamId>>,
}

impl CompileTurn {
    /// Waits up to `wait` for its unit: the stream that holds it for the
    /// caller. None when `wait` ran out first; it keeps its place.
    pub(crate) async fn handed(&mut self, wait: Duration) -> Option<StreamId> {
        let Some(handed) = self.handed.as_mut() else {
            tokio::time::sleep(wait).await;
            return None;
        };
        match timeout(wait, handed).await {
            Ok(Ok(stream)) => {
                self.handed = None;
                Some(stream)
            }
            // Not while this turn holds its streams: the line keeps the
            // sender until it hands a unit.
            Ok(Err(_gone)) => {
                self.handed = None;
                None
            }
            Err(_elapsed) => None,
        }
    }
}

impl Drop for CompileTurn {
    fn drop(&mut self) {
        if let Some(mut handed) = self.handed.take() {
            handed.close();
            if let Ok(stream) = handed.try_recv() {
                self.streams.let_go(stream);
            }
        }
    }
}

impl PullStreams {
    /// The pull streams that take sends now, each with its room.
    #[must_use]
    pub fn held(&self) -> Vec<HeldStream> {
        lock(&self.held)
            .values()
            .filter(|held| !held.closing)
            .map(|held| held.view.clone())
            .collect()
    }

    /// Whether a stream that takes sends has room for a Run.
    #[must_use]
    pub fn has_room(&self) -> bool {
        has_room(&lock(&self.held))
    }

    /// Whether a stream that takes sends has room for a compile.
    pub(crate) fn has_compile_room(&self) -> bool {
        has_compile_room(&lock(&self.held))
    }

    /// The stream that takes sends with the most compile room, ties to the
    /// most room for Runs and then at random, or none when no stream has
    /// compile room. A retiring Run Executor reports no room for Runs, so it
    /// loses a tie, but it still takes a compile when it has more compile
    /// room; it serves the compile before it retires.
    pub(crate) fn most_compile_room(&self) -> Option<StreamId> {
        most_compile_room(&lock(&self.held))
    }

    /// How many callers wait in line for compile room.
    #[cfg(test)]
    pub(crate) fn waiting_for_compile_room(&self) -> usize {
        let _held = lock(&self.held);
        let line = lock(&self.compile_line);
        line.iter().filter(|waiting| !waiting.is_closed()).count()
    }

    /// Whether this Coral Instance holds a stream that takes sends: a
    /// caller here can wait for its compile room.
    pub(crate) fn has_open_stream(&self) -> bool {
        lock(&self.held).values().any(|held| !held.closing)
    }

    /// One unit of compile room held for the caller now, on the stream with
    /// the most, when one is free. Otherwise its place in the line: at the
    /// end, or at the front for a caller `again`, whose unit or stream went
    /// before its call was answered. A caller in line means no room is free.
    pub(crate) fn compile_turn(&self, again: bool) -> Result<StreamId, CompileTurn> {
        let mut held = lock(&self.held);
        let mut line = lock(&self.compile_line);
        line.retain(|waiting| !waiting.is_closed());
        if let Some(stream) = most_compile_room(&held)
            && let Some(chosen) = held.get_mut(&stream)
            && chosen.hold_compile().is_ok()
        {
            return Ok(stream);
        }
        let (waiting, handed) = oneshot::channel();
        if again {
            line.push_front(waiting);
        } else {
            line.push_back(waiting);
        }
        Err(CompileTurn {
            streams: self.clone(),
            handed: Some(handed),
        })
    }

    /// The units of compile room `stream` has free, none once it is gone.
    #[cfg(test)]
    pub(crate) fn compile_room(&self, stream: StreamId) -> u32 {
        lock(&self.held).get(&stream).map_or(0, Held::compile_room)
    }

    /// The stream that takes sends with the most room for Runs, ties at
    /// random, with its room and its Run Executor, or none when no stream
    /// has room.
    #[must_use]
    pub fn with_most_room(&self) -> Option<HeldStream> {
        let random = RandomState::new();
        lock(&self.held)
            .values()
            .filter(|held| !held.closing && held.view.free > 0)
            .max_by_key(|held| (held.view.free, random.hash_one(held.view.stream)))
            .map(|held| held.view.clone())
    }

    /// Waits until a stream opens or frees room, or for `wait`, holding no
    /// thread. It returns at once while a stream has room for a Run.
    pub async fn room_signalled(&self, wait: Duration) {
        self.signalled(wait, has_room).await;
    }

    /// [`Self::room_signalled`] for room for a compile.
    pub(crate) async fn compile_room_signalled(&self, wait: Duration) {
        self.signalled(wait, has_compile_room).await;
    }

    async fn signalled(&self, wait: Duration, room: fn(&HashMap<StreamId, Held>) -> bool) {
        let signalled = self.room_signal.notified();
        tokio::pin!(signalled);
        // Registered before the look, so a signal after it is not missed.
        signalled.as_mut().enable();
        if room(&lock(&self.held)) {
            return;
        }
        drop(tokio::time::timeout(wait, signalled).await);
    }

    /// Opens a pull stream that a test scripts, for a dialed Run Executor
    /// with `free` room. What Coral sends it arrives on the receiver.
    #[cfg(any(test, feature = "test-support"))]
    #[must_use]
    pub fn scripted(
        &self,
        run_executor_id: &str,
        free: u32,
    ) -> (
        StreamId,
        mpsc::UnboundedReceiver<Result<PullResponse, Status>>,
    ) {
        let (outbound, sent) = mpsc::unbounded_channel();
        let admission = dialed_admission();
        let stream = self.insert(run_executor_id.to_string(), admission, free, outbound);
        (stream, sent)
    }

    /// Wakes every caller that waits for room.
    fn signal_room(&self) {
        self.room_signal.notify_waiters();
    }

    /// Sends `runs` on `stream`, one unit each; none sends nothing.
    ///
    /// # Errors
    ///
    /// Returns [`Refused`] when the stream is gone or has too little room.
    pub fn take(&self, stream: StreamId, runs: Vec<TakenRun>) -> Result<(), Refused> {
        let mut held = lock(&self.held);
        let held = held.get_mut(&stream).ok_or(Refused::Closed)?;
        if runs.is_empty() {
            return Ok(());
        }
        held.spend(u32::try_from(runs.len()).unwrap_or(u32::MAX))?;
        held.send(response(pull_response::Message::Taken(Taken { runs })));
        Ok(())
    }

    /// Holds `units` of `stream`'s room for a take while it claims Runs: no
    /// other take spends them until [`Self::take_held`] spends or frees
    /// them.
    ///
    /// # Errors
    ///
    /// Returns [`Refused`] when the stream is gone or has too little room.
    pub fn hold_units(&self, stream: StreamId, units: u32) -> Result<(), Refused> {
        let mut held = lock(&self.held);
        let held = held.get_mut(&stream).ok_or(Refused::Closed)?;
        held.spend(units)?;
        held.kept_for_takes += units;
        Ok(())
    }

    /// Sends `runs` on `stream` as one `Taken`, on the `units` that
    /// [`Self::hold_units`] held, one each, and frees the rest. No runs
    /// send nothing.
    ///
    /// # Errors
    ///
    /// Returns [`Refused::Closed`] when the stream is gone or ending, and
    /// [`Refused::NoRoom`] for more runs than units. Either sends nothing and
    /// frees every unit.
    pub fn take_held(
        &self,
        stream: StreamId,
        units: u32,
        runs: Vec<TakenRun>,
    ) -> Result<(), Refused> {
        let mut held = lock(&self.held);
        let held = held.get_mut(&stream).ok_or(Refused::Closed)?;
        let fits = u32::try_from(runs.len()).ok().filter(|sent| *sent <= units);
        let sent = match fits {
            Some(sent) if !held.closing => sent,
            _ => 0,
        };
        held.kept_for_takes = held.kept_for_takes.saturating_sub(units);
        held.view.free += units - sent;
        if units > sent {
            self.signal_room();
        }
        if held.closing {
            return Err(Refused::Closed);
        }
        if fits.is_none() {
            return Err(Refused::NoRoom);
        }
        if sent > 0 {
            held.view.last_work = Some(Instant::now());
            held.send(response(pull_response::Message::Taken(Taken { runs })));
        }
        Ok(())
    }

    /// Sends `call` on `stream` as its next numbered `Compile`, with
    /// `time_left_ms` to answer. It spends one unit of compile room until
    /// the answer, and no room for Runs. The answer, read and checked
    /// against the call, arrives on the receiver, which disconnects
    /// unanswered if the stream ends first.
    ///
    /// # Errors
    ///
    /// Returns [`Refused`] when the stream is gone or has no compile room,
    /// or the `Compile` is too large to send. A refused call sends and
    /// spends nothing.
    pub(crate) fn compile(
        &self,
        stream: StreamId,
        time_left_ms: u32,
        call: Call,
    ) -> Result<oneshot::Receiver<Read>, Refused> {
        self.send_compile(stream, time_left_ms, call, false)
    }

    /// Holds one unit of `stream`'s compile room for a caller: no other
    /// compile takes it until [`Self::compile_held`] spends it or
    /// [`Self::let_go`] frees it.
    ///
    /// # Errors
    ///
    /// Returns [`Refused`] when the stream is gone or has no compile room.
    pub(crate) fn hold_compile(&self, stream: StreamId) -> Result<(), Refused> {
        lock(&self.held)
            .get_mut(&stream)
            .ok_or(Refused::Closed)?
            .hold_compile()
    }

    /// Frees a unit [`Self::hold_compile`] or [`Self::compile_turn`] held
    /// and no compile spent. The oldest caller in line takes it.
    pub(crate) fn let_go(&self, stream: StreamId) {
        let mut held = lock(&self.held);
        if let Some(chosen) = held.get_mut(&stream)
            && chosen.kept_for_compiles > 0
        {
            chosen.kept_for_compiles -= 1;
            hand_off(&mut held, &mut lock(&self.compile_line));
            // Room a caller in line took wakes nobody.
            let room_left = has_compile_room(&held);
            drop(held);
            if room_left {
                self.signal_room();
            }
        }
    }

    /// [`Self::compile`] on a unit [`Self::hold_compile`] or
    /// [`Self::compile_turn`] held, which it spends. A refused call frees
    /// the unit.
    pub(crate) fn compile_held(
        &self,
        stream: StreamId,
        time_left_ms: u32,
        call: Call,
    ) -> Result<oneshot::Receiver<Read>, Refused> {
        let sent = self.send_compile(stream, time_left_ms, call, true);
        if sent.is_err() {
            self.let_go(stream);
        }
        sent
    }

    fn send_compile(
        &self,
        stream: StreamId,
        time_left_ms: u32,
        call: Call,
        on_held_unit: bool,
    ) -> Result<oneshot::Receiver<Read>, Refused> {
        if too_large(&call) {
            return Err(Refused::TooLarge);
        }
        let takes = Takes::of(&call);
        let mut message = pull_response::Message::Compile(Compile {
            number: 0,
            time_left_ms,
            call: Some(call),
        });
        let mut held = lock(&self.held);
        let held = held.get_mut(&stream).ok_or(Refused::Closed)?;
        if on_held_unit {
            if held.closing || held.kept_for_compiles == 0 {
                return Err(Refused::Closed);
            }
            held.kept_for_compiles -= 1;
            held.view.last_work = Some(Instant::now());
        } else {
            held.fit_compile()?;
        }
        held.compiles_sent += 1;
        let number = held.compiles_sent;
        if let pull_response::Message::Compile(compile) = &mut message {
            compile.number = number;
        }
        let (answered, answer) = oneshot::channel();
        let in_flight = InFlight {
            due: Instant::now() + Duration::from_millis(time_left_ms.into()),
            takes,
            answered,
        };
        held.compiles.insert(number, in_flight);
        held.send(response(message));
        Ok(answer)
    }

    pub(crate) fn insert(
        &self,
        run_executor_id: String,
        (kind, credential): (Kind, CredentialId),
        free: u32,
        outbound: mpsc::UnboundedSender<Result<PullResponse, Status>>,
    ) -> StreamId {
        let stream = StreamId(self.next.fetch_add(1, Ordering::Relaxed));
        let held = Held {
            view: HeldStream {
                stream,
                run_executor_id,
                kind,
                credential,
                free,
                hello_free: free,
                last_work: None,
            },
            closing: false,
            outbound,
            compiles_sent: 0,
            kept_for_takes: 0,
            kept_for_compiles: 0,
            compiles: HashMap::new(),
        };
        let mut held_streams = lock(&self.held);
        // Read under the lock `drain` walks the streams in: a stream that
        // opens while the Coral Instance drains takes no send, and its
        // holder ends it.
        let draining = *self.draining.borrow();
        held_streams.insert(
            stream,
            Held {
                closing: draining,
                ..held
            },
        );
        self.count.send_replace(held_streams.len());
        if !draining {
            hand_off(&mut held_streams, &mut lock(&self.compile_line));
        }
        drop(held_streams);
        if !draining {
            self.signal_room();
        }
        stream
    }

    /// Applies one message from `stream`'s Run Executor. A message that
    /// breaks the protocol changes nothing; its holder ends the stream. Only
    /// that holder calls it, one message at a time.
    pub(crate) fn receive(&self, stream: StreamId, request: PullRequest) -> Result<(), Status> {
        match request.message {
            Some(pull_request::Message::Room(room)) => {
                if let Some(held) = lock(&self.held).get_mut(&stream) {
                    held.view.free = room.free.saturating_sub(held.kept_for_takes);
                    held.view.last_work = Some(Instant::now());
                }
                // Only room wakes the calls that wait for it.
                if room.free > 0 {
                    self.signal_room();
                }
                Ok(())
            }
            Some(pull_request::Message::Compiled(compiled)) => self.answer(stream, compiled),
            Some(pull_request::Message::Hello(_)) => {
                Err(Status::invalid_argument("Hello opens the pull stream once"))
            }
            None => Err(Status::invalid_argument("the message is empty")),
        }
    }

    /// Hands `compiled` to the compile in flight it answers, once it reads
    /// as an answer to that call.
    fn answer(&self, stream: StreamId, compiled: Compiled) -> Result<(), Status> {
        let Some(in_flight) = lock(&self.held)
            .get(&stream)
            .map(|held| held.compiles.get(&compiled.number).map(|call| call.takes))
        else {
            return Ok(());
        };
        let takes = in_flight
            .ok_or_else(|| Status::invalid_argument("Compiled answers no Compile in flight"))?;
        // Read outside the lock: a package's digest covers megabytes. Only
        // this stream's holder answers its compiles, so the call still waits.
        let read = compiled
            .answer
            .ok_or(ProtocolError)
            .and_then(Read::answer)
            .ok()
            .filter(|read| read.answers(takes))
            .ok_or_else(|| Status::invalid_argument("Compiled breaks the protocol"))?;
        let mut held = lock(&self.held);
        let answered = held.get_mut(&stream).and_then(|chosen| {
            // An answer is work, and frees its unit of compile room for the
            // oldest caller in line.
            chosen.view.last_work = Some(Instant::now());
            chosen.compiles.remove(&compiled.number)
        });
        if answered.is_some() {
            hand_off(&mut held, &mut lock(&self.compile_line));
        }
        // Room a caller in line took wakes nobody.
        let room_left = has_compile_room(&held);
        drop(held);
        if let Some(in_flight) = answered {
            if room_left {
                self.signal_room();
            }
            // An answer nobody waits for any more is dropped.
            drop(in_flight.answered.send(read));
        }
        Ok(())
    }

    /// Stops sends on `stream`. Returns when its compiles in flight are
    /// due: the last deadline among them, or now.
    pub(crate) fn close(&self, stream: StreamId) -> Instant {
        let mut held = lock(&self.held);
        let now = Instant::now();
        held.get_mut(&stream).map_or(now, |held| {
            held.closing = true;
            held.compiles
                .values()
                .map(|in_flight| in_flight.due)
                .fold(now, Instant::max)
        })
    }

    fn compiles_in_flight(&self, stream: StreamId) -> bool {
        lock(&self.held)
            .get(&stream)
            .is_some_and(|held| !held.compiles.is_empty())
    }

    /// Forgets `stream`, which fails its compiles in flight, and ends
    /// Coral's side with `violation`, or cleanly without one: the move.
    pub(crate) fn end(&self, stream: StreamId, violation: Option<Status>) {
        let mut held_streams = lock(&self.held);
        let held = held_streams.remove(&stream);
        self.count.send_replace(held_streams.len());
        drop(held_streams);
        if let (Some(held), Some(violation)) = (held, violation) {
            drop(held.outbound.send(Err(violation)));
        }
    }

    /// Drains this Coral Instance's link: every stream takes no send from
    /// now on and ends cleanly once its compiles in flight are answered or
    /// due, as at its connection age, and the listener refuses a new pull
    /// stream or Run stream with `UNAVAILABLE`. The Run Executor redials and
    /// lands on another Coral Instance.
    pub fn drain(&self) {
        self.draining.send_replace(true);
        for held in lock(&self.held).values_mut() {
            held.closing = true;
        }
    }

    /// Whether [`Self::drain`] ran.
    pub(crate) fn is_draining(&self) -> bool {
        *self.draining.borrow()
    }

    /// Watches [`Self::drain`].
    fn draining(&self) -> watch::Receiver<bool> {
        self.draining.subscribe()
    }

    /// Returns once this Coral Instance holds no pull stream.
    pub async fn ended(&self) {
        let mut count = self.count.subscribe();
        // The sender lives as long as `self`.
        let _none_held = count.wait_for(|count| *count == 0).await;
    }

    /// Ends a stream [`Self::scripted`] opened, as its holder would.
    #[cfg(any(test, feature = "test-support"))]
    pub fn end_scripted(&self, stream: StreamId) {
        self.end(stream, None);
    }
}

/// Whether `call`, sent as a `Compile`, passes [`MESSAGE_MAX_BYTES`]. Sent,
/// such a message would end the stream and every compile on it.
pub(crate) fn too_large(call: &Call) -> bool {
    use prost::encoding::encoded_len_varint;
    // The `Compile`: its call, its longest number and its longest time left,
    // each a key byte and a varint.
    let compile = call.encoded_len() + 1 + 10 + 1 + 5;
    // The `PullResponse` around it: a key byte and a length.
    let response = 1 + encoded_len_varint(compile as u64) + compile;
    response > MESSAGE_MAX_BYTES
}

/// Coral's side of the link: the pull streams it holds, the credentials it
/// admits, and the lease rows it writes.
pub struct LinkServer {
    pub(crate) credentials: Credentials,
    pub(crate) streams: PullStreams,
    pub(crate) database: Arc<Database>,
    /// This Coral Instance's id, which Welcome carries.
    pub(crate) coral_instance_id: String,
    pub(crate) timing: Timing,
    /// What serves the Run streams; `None` refuses them.
    pub(crate) run_streams: Option<Arc<dyn RunStreamServer>>,
}

impl LinkServer {
    /// The link of the Coral Instance `coral_instance_id`, which admits
    /// `credentials` and writes lease rows in `database`.
    #[must_use]
    pub fn new(
        credentials: Credentials,
        database: Arc<Database>,
        coral_instance_id: String,
    ) -> Self {
        Self {
            credentials,
            streams: PullStreams::default(),
            database,
            coral_instance_id,
            timing: Timing::default(),
            run_streams: None,
        }
    }

    /// Serves Run streams with `server`.
    #[must_use]
    pub fn with_run_streams(mut self, server: Arc<dyn RunStreamServer>) -> Self {
        self.run_streams = Some(server);
        self
    }

    /// The pull streams this link holds.
    #[must_use]
    pub fn streams(&self) -> PullStreams {
        self.streams.clone()
    }

    /// The compiler client over these pull streams. A call that finds no
    /// stream with room relays through the database to any Coral Instance
    /// with room; it asks `children`, when there are local children, for
    /// room.
    #[must_use]
    pub fn compiler(&self, children: Option<LocalRunExecutors>) -> PullStreamCompiler {
        PullStreamCompiler::relayed(self.streams(), children, self.relay())
    }

    fn relay(&self) -> CompileRelay {
        CompileRelay::new(self.database.clone(), &self.coral_instance_id)
    }

    /// Serves the link over plaintext HTTP/2 on `listener` until `shutdown`
    /// completes, and serves relayed compiles on its streams. Shutdown ends
    /// each pull stream cleanly, after its compiles in flight, as the
    /// connection age does.
    ///
    /// # Errors
    ///
    /// Returns the listener's failure.
    pub async fn serve(
        self,
        listener: TcpListener,
        shutdown: impl Future<Output = ()>,
    ) -> Result<(), tonic::transport::Error> {
        let (close, closing) = watch::channel(false);
        let connection_age = self.timing.connection_age;
        let pickup = self.relay().serve(self.streams());
        let service = PullService {
            link: Arc::new(self),
            closing,
        };
        let served = Server::builder()
            .max_connection_age(connection_age)
            .http2_keepalive_interval(Some(KEEPALIVE))
            .http2_keepalive_timeout(Some(KEEPALIVE))
            .http2_max_header_list_size(HTTP2_MAX_HEADER_LIST_SIZE)
            .add_service(
                LinkServiceServer::new(service)
                    .max_decoding_message_size(MESSAGE_MAX_BYTES)
                    .max_encoding_message_size(MESSAGE_MAX_BYTES),
            )
            .serve_with_incoming_shutdown(
                TcpIncoming::from(listener).with_nodelay(Some(true)),
                async move {
                    shutdown.await;
                    close.send_replace(true);
                },
            );
        tokio::select! {
            served = served => served,
            never = pickup => match never {},
        }
    }
}

#[derive(Clone)]
struct PullService {
    link: Arc<LinkServer>,
    closing: watch::Receiver<bool>,
}

#[tonic::async_trait]
impl LinkService for PullService {
    type PullStream = UnboundedReceiverStream<Result<PullResponse, Status>>;
    type RunStream = Pin<Box<dyn tokio_stream::Stream<Item = Result<RunResponse, Status>> + Send>>;

    /// Hands an admitted Run Executor's Run stream to the Run stream server.
    async fn run(
        &self,
        request: Request<Streaming<RunRequest>>,
    ) -> Result<Response<Self::RunStream>, Status> {
        let authorization = request
            .metadata()
            .get("authorization")
            .and_then(|value| value.to_str().ok());
        self.link.credentials.admit(authorization).ok_or_else(|| {
            Status::unauthenticated("the Run Executor credential is not accepted")
        })?;
        let server =
            self.link.run_streams.clone().ok_or_else(|| {
                Status::unimplemented("this Coral Instance serves no Run streams")
            })?;
        let (responses, outbound) = mpsc::unbounded_channel();
        // A message the link cannot decode ends the requests; the server
        // sees the Run Executor's side end.
        let requests = Box::pin(tokio_stream::StreamExt::map_while(
            request.into_inner(),
            Result::ok,
        ));
        tokio::spawn(async move {
            server
                .serve(ServedRunStream {
                    requests,
                    responses,
                })
                .await;
        });
        // A move ends the stream with OK and no message.
        Ok(Response::new(Box::pin(tokio_stream::StreamExt::map_while(
            UnboundedReceiverStream::new(outbound),
            |message| match message {
                Ok(response) => Some(Ok(response)),
                Err(closing) => closing.status().map(Err),
            },
        ))))
    }

    async fn pull(
        &self,
        request: Request<Streaming<PullRequest>>,
    ) -> Result<Response<Self::PullStream>, Status> {
        let authorization = request
            .metadata()
            .get("authorization")
            .and_then(|value| value.to_str().ok());
        let admitted = self.link.credentials.admit(authorization).ok_or_else(|| {
            Status::unauthenticated("the Run Executor credential is not accepted")
        })?;
        self.refuse_while_draining()?;
        let mut inbound = request.into_inner();
        // A Run Executor sends Hello with the call; one that sends none
        // within a keepalive holds nothing open, shutdown included.
        let first = timeout(KEEPALIVE, inbound.message()).await.ok();
        let Some(pull_request::Message::Hello(hello)) = first
            .transpose()
            .map_err(|status| undecodable(&status).unwrap_or(status))?
            .flatten()
            .and_then(|request| request.message)
        else {
            return Err(Status::invalid_argument("the pull stream opens with Hello"));
        };
        if hello.protocol_level.abs_diff(PROTOCOL_LEVEL) > 1 {
            tracing::warn!(
                coral.run_executor.protocol_level = safe::number(PROTOCOL_LEVEL),
                coral.run_executor.peer_protocol_level = safe::number(hello.protocol_level),
                "the Run Executor's protocol level is more than one apart; refusing its pull stream"
            );
            return Err(Status::failed_precondition(
                "the protocol levels are more than one apart",
            ));
        }
        let run_executor_id = uuid::Uuid::try_parse(&hello.run_executor_id)
            .map_err(|_invalid| Status::invalid_argument("the Run Executor id is not a UUID"))?
            .to_string();
        let opened = Instant::now();
        let kind = admitted.0;
        self.renew(&run_executor_id, kind).await.map_err(|error| {
            tracing::warn!(
                error.type = safe::kind(error.code()),
                "Run Executor lease write failed; refusing its pull stream"
            );
            Status::unavailable("the Run Executor lease is not written")
        })?;
        // The drain may have started while the Hello or the lease was due.
        self.refuse_while_draining()?;
        let (outbound, responses) = mpsc::unbounded_channel();
        let welcome = response(pull_response::Message::Welcome(Welcome {
            protocol_level: PROTOCOL_LEVEL,
            coral_instance_id: self.link.coral_instance_id.clone(),
        }));
        // The receiver is still here, so the send lands.
        drop(outbound.send(Ok(welcome)));
        self.link
            .credentials
            .said_hello(admitted.1, &run_executor_id);
        let stream =
            self.link
                .streams
                .insert(run_executor_id.clone(), admitted, hello.free, outbound);
        tokio::spawn(
            self.clone()
                .hold(stream, inbound, opened, run_executor_id, kind),
        );
        Ok(Response::new(UnboundedReceiverStream::new(responses)))
    }
}

impl PullService {
    /// A draining Coral Instance opens no pull stream: the Run Executor
    /// opens it on another. A stream that opens past this check is born
    /// closing. The Run module refuses a new Run stream.
    fn refuse_while_draining(&self) -> Result<(), Status> {
        if self.link.streams.is_draining() {
            return Err(Status::unavailable(SHUTTING_DOWN));
        }
        Ok(())
    }

    /// Holds `stream` until it ends: applies its messages, renews its lease,
    /// and ends it cleanly at its age or shutdown once its compiles in
    /// flight are answered or due. A protocol violation ends it with
    /// `INVALID_ARGUMENT`. Nothing is written once it ends.
    async fn hold(
        self,
        stream: StreamId,
        mut inbound: Streaming<PullRequest>,
        opened: Instant,
        run_executor_id: String,
        kind: Kind,
    ) {
        let streams = &self.link.streams;
        let timing = self.link.timing;
        let mut renewals = interval_at(opened + timing.renewal, timing.renewal);
        renewals.set_missed_tick_behavior(MissedTickBehavior::Skip);
        // The renewal in flight, polled beside the stream, so a slow write
        // holds back no message.
        let mut renewal = None;
        // The connection age, then the end of the drain.
        let deadline = sleep_until(opened + timing.connection_age);
        tokio::pin!(deadline);
        let mut closing = self.closing.clone();
        let mut draining = streams.draining();
        let mut draining_until = None;
        let end = loop {
            if let Some(until) = draining_until
                && (!streams.compiles_in_flight(stream) || Instant::now() >= until)
            {
                break Ok(());
            }
            tokio::select! {
                message = inbound.message() => match message {
                    Ok(Some(request)) => {
                        if let Err(status) = streams.receive(stream, request) {
                            break Err(status);
                        }
                    }
                    // A message that does not decode is a violation; any
                    // other error is the connection ending.
                    Err(status) => match undecodable(&status) {
                        Some(violation) => break Err(violation),
                        None => break Ok(()),
                    },
                    // The Run Executor's side ended.
                    Ok(None) => break Ok(()),
                },
                _ = renewals.tick(), if renewal.is_none() => {
                    renewal = Some(Box::pin(self.renew(&run_executor_id, kind)));
                }
                written = async {
                    match renewal.as_mut() {
                        Some(renewal) => renewal.await,
                        None => pending().await,
                    }
                } => {
                    renewal = None;
                    renewed(written);
                }
                () = &mut deadline => {
                    if draining_until.is_none() {
                        draining_until = Some(drain(streams, stream, deadline.as_mut()));
                    }
                }
                // Its only change is to closing; a dropped sender closes too.
                _ = closing.changed(), if draining_until.is_none() => {
                    draining_until = Some(drain(streams, stream, deadline.as_mut()));
                }
                // The Coral Instance drains, now or before this stream opened.
                _ = draining.wait_for(|draining| *draining), if draining_until.is_none() => {
                    draining_until = Some(drain(streams, stream, deadline.as_mut()));
                }
            }
        };
        // A renewal in flight lands before the stream goes; none starts after.
        if let Some(renewal) = renewal {
            renewed(renewal.await);
        }
        streams.end(stream, end.err());
    }

    async fn renew(&self, run_executor_id: &str, kind: Kind) -> Result<(), coral_db::Error> {
        let lease_until = time::unix_now().saturating_add(self.link.timing.lease);
        lease_repository::renew(&self.link.database, run_executor_id, kind, lease_until).await
    }
}

/// Stops sends on `stream` and moves `deadline` to when its compiles in
/// flight are due, which it returns.
fn drain(streams: &PullStreams, stream: StreamId, deadline: Pin<&mut Sleep>) -> Instant {
    let until = streams.close(stream);
    deadline.reset(until);
    until
}

/// The violation that ends a pull stream when `status`, an inbound error,
/// says a message did not decode: tonic answers `INTERNAL` for bytes that are
/// not a `PullRequest` and `OUT_OF_RANGE` for one over the size limit.
fn undecodable(status: &Status) -> Option<Status> {
    matches!(status.code(), Code::Internal | Code::OutOfRange)
        .then(|| Status::invalid_argument("the message is not a PullRequest"))
}

fn renewed(written: Result<(), coral_db::Error>) {
    if let Err(error) = written {
        tracing::warn!(
            error.type = safe::kind(error.code()),
            "Run Executor lease renewal failed; the next one retries"
        );
    }
}

pub(crate) fn response(message: pull_response::Message) -> PullResponse {
    PullResponse {
        message: Some(message),
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
pub(crate) mod tests;
